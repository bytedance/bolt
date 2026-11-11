# Copyright (c) ByteDance Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Maintain and prove the worker-only Debian iptables isolation rules."""

import ipaddress
import json
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.parse import urlsplit

from bolt_ci.config import NETWORK
from bolt_ci.docker import DOCKER_BIN, DOCKER_ENV
from bolt_ci.policy import HASH

BRIDGE = "bglut-worker"
CONTROL_BRIDGE = "bglut-control"
CONTROL_NETWORK = "bolt-gluten-ci-control"
OWNER = "io.bolt.gluten-ci.worker-network"
BRIDGE_NAME = "com.docker.network.bridge.name"
ICC = "com.docker.network.bridge.enable_icc"
BOOT_ID = Path("/proc/sys/kernel/random/boot_id")
IP = "/usr/sbin/ip"
IPTABLES = "/usr/sbin/iptables"
IP6TABLES = "/usr/sbin/ip6tables"
INPUT = "BGLUT-W-IN"
OUT = "BGLUT-W-OUT"
BACK = "BGLUT-W-BACK"
BLOCKED = (
    "0.0.0.0/8",
    "10.0.0.0/8",
    "100.64.0.0/10",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "172.16.0.0/12",
    "192.0.0.0/24",
    "192.168.0.0/16",
    "198.18.0.0/15",
    "224.0.0.0/3",
)
METADATA = ("169.254.0.0/16", "100.100.100.200/32", "192.0.0.192/32")
IFACE = re.compile(r"[A-Za-z0-9_.-]{1,15}")


def command(args, failure, *, timeout=60, data=None):
    try:
        result = subprocess.run(
            args,
            env=DOCKER_ENV,
            input=data,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError):
        raise ValueError(failure) from None
    if result.returncode:
        raise ValueError(failure)
    return result.stdout


def docker(*args, timeout=60):
    return command(
        [DOCKER_BIN, *args], "Local Docker operation failed", timeout=timeout
    )


def atomic(path, data):
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temp = Path(stream.name)
        try:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
            temp.replace(path)
        finally:
            temp.unlink(missing_ok=True)


def atomic_json(path, data):
    atomic(path, (json.dumps(data, indent=2, sort_keys=True) + "\n").encode())


def valid_network(name):
    return NETWORK.fullmatch(name) and name not in {
        "bridge",
        "host",
        "none",
        CONTROL_NETWORK,
    }


def network(name, *, create=False):
    names = docker("network", "ls", "--format", "{{.Name}}").decode().splitlines()
    if name not in names and create:
        if (Path("/sys/class/net") / BRIDGE).exists():
            raise ValueError("The worker bridge name is already in use")
        docker(
            "network",
            "create",
            "--driver=bridge",
            "--ipv6=false",
            f"--label={OWNER}=1",
            f"--opt={BRIDGE_NAME}={BRIDGE}",
            f"--opt={ICC}=false",
            name,
        )
    (record,) = json.loads(docker("network", "inspect", name))
    if (
        not HASH.fullmatch(record["Id"])
        or record["Name"] != name
        or record["Driver"] != "bridge"
        or record["Scope"] != "local"
        or record["EnableIPv6"] is not False
        or record.get("EnableIPv4", True) is not True
        or record["Internal"] is not False
        or record["Ingress"] is not False
        or (record.get("Labels") or {}).get(OWNER) != "1"
        or record["Options"] != {BRIDGE_NAME: BRIDGE, ICC: "false"}
        or not (Path("/sys/class/net") / BRIDGE / "bridge").is_dir()
    ):
        raise ValueError("Worker bridge differs from the owned IPv4-only configuration")
    return record


def cache_address(raw):
    address = ipaddress.IPv4Address(raw)
    if (
        address.is_loopback
        or address.is_link_local
        or address.is_unspecified
        or address.is_multicast
        or any(address in ipaddress.ip_network(cidr) for cidr in METADATA)
    ):
        raise ValueError("Invalid explicit Conan cache destination")
    return str(address)


def endpoint(plan):
    if not plan["conan_remote"]:
        return None, None
    url = urlsplit(plan["conan_remote"])
    if url.hostname != "host.docker.internal":
        return (cache_address(url.hostname), url.port), None
    gateway = cache_address(plan["host_gateway"])
    (container,) = json.loads(docker("container", "inspect", plan["conan_container"]))
    if container["Id"] != plan["conan_container"] or not container["State"]["Running"]:
        raise ValueError("The pinned Conan cache container is unavailable")
    settings = container["NetworkSettings"]
    ((network_name, cache_net),) = settings["Networks"].items()
    bindings = [
        int(port.split("/")[0])
        for port, published in settings["Ports"].items()
        if port.endswith("/tcp")
        and any(
            entry["HostPort"] == str(url.port)
            and entry["HostIp"] in {gateway, "0.0.0.0"}
            for entry in published or ()
        )
    ]
    if (
        len(bindings) != 1
        or network_name == CONTROL_NETWORK
        or cache_net["NetworkID"] == plan["network_id"]
    ):
        raise ValueError("The Conan cache port differs from the prepared binding")
    actual = (cache_address(cache_net["IPAddress"]), bindings[0])
    return (gateway, url.port), actual


def rules(wan, cache=None, translated=None):
    incoming = [f"-A {INPUT} -j DROP"]
    forwarded = [
        f"-A {OUT} -o {BRIDGE} -j DROP",
        f"-A {OUT} -o {CONTROL_BRIDGE} -j DROP",
    ]
    if cache:
        address, port = cache
        incoming.insert(
            0, f"-A {INPUT} -d {address}/32 -p tcp -m tcp --dport {port} -j ACCEPT"
        )
        target, target_port = translated or cache
        forwarded.append(
            f"-A {OUT} -d {target}/32 -p tcp -m tcp --dport {target_port} "
            f"-m conntrack --ctorigdst {address} --ctorigdstport {port} --ctdir ORIGINAL -j ACCEPT"
        )
    forwarded.extend(f"-A {OUT} -d {cidr} -j DROP" for cidr in BLOCKED)
    forwarded.extend([f"-A {OUT} -o {wan} -j RETURN", f"-A {OUT} -j DROP"])
    return tuple(incoming), tuple(forwarded)


def table(tool):
    return command(
        [tool, "-w", "5", "-S"], "Kernel firewall rules unavailable"
    ).decode()


def rule_identity(rule):
    # iptables-save may canonicalize match option order and /32 masks.
    tokens = rule.split()
    if len(tokens) % 2:
        return ()
    pairs = []
    for key, value in zip(tokens[::2], tokens[1::2], strict=True):
        if key in {"-d", "--ctorigdst"}:
            value = str(ipaddress.ip_network(value, strict=False))
        if key == "--ctstate":
            value = ",".join(sorted(value.split(",")))
        pairs.append((key, value))
    return tuple(sorted(pairs))


def verify_chain(contents, parent, owned, exact, *, position=0):
    chain = contents.splitlines()
    hook, *expected = exact
    parent_rules = [rule for rule in chain if rule.startswith(f"-A {parent} ")]
    installed = [rule for rule in chain if rule.startswith(f"-A {owned} ")]
    if (
        parent_rules[position : position + 1] != [hook]
        or parent_rules.count(hook) != 1
        or [rule_identity(rule) for rule in installed]
        != [rule_identity(rule) for rule in expected]
    ):
        raise ValueError(
            "Worker firewall hook or chain differs from the verified policy"
        )


def hook_rules(wan, cache, translated, *, ipv6=False):
    in_rules, out_rules = rules(wan, cache, translated)
    forward = "FORWARD" if ipv6 else "DOCKER-USER"
    return (
        (
            "INPUT",
            INPUT,
            (
                f"-A INPUT -i {BRIDGE} -j {INPUT}",
                *((f"-A {INPUT} -j DROP",) if ipv6 else in_rules),
            ),
        ),
        (
            forward,
            OUT,
            (
                f"-A {forward} -i {BRIDGE} -j {OUT}",
                *((f"-A {OUT} -j DROP",) if ipv6 else out_rules),
            ),
        ),
        (
            forward,
            BACK,
            (
                f"-A {forward} -o {BRIDGE} -j {BACK}",
                *(
                    (f"-A {BACK} -m conntrack --ctstate RELATED,ESTABLISHED -j RETURN",)
                    if not ipv6
                    else ()
                ),
                f"-A {BACK} -j DROP",
            ),
        ),
    )


def apply(tool, policies, contents):
    # Rebuild only the owned chains and hooks in one filter-table transaction.
    lines = contents.splitlines()
    script = ["*filter"]
    positions = {}
    for parent, name, (hook, *owned) in policies:
        if f"-N {name}" in lines:
            if any(line.endswith(f" -j {name}") and line != hook for line in lines):
                raise ValueError(
                    "A worker firewall chain is referenced outside its owned bridge hook"
                )
            script.append(f"-F {name}")
        else:
            script.append(f":{name} - [0:0]")
        script.extend(hook.replace("-A ", "-D ", 1) for _ in range(lines.count(hook)))
        positions[parent] = positions.get(parent, 0) + 1
        script.append(
            hook.replace(f"-A {parent} ", f"-I {parent} {positions[parent]} ", 1)
        )
        script.extend(owned)
    script.append("COMMIT")
    command(
        [f"{tool}-restore", "--wait", "5", "--noflush"],
        "Could not install the owned worker firewall policy",
        data=("\n".join(script) + "\n").encode(),
    )


def verify_policies(contents, policies, *, ipv6=False):
    if not ipv6:
        first = next(
            (line for line in contents.splitlines() if line.startswith("-A FORWARD ")),
            "",
        )
        if first != "-A FORWARD -j DOCKER-USER":
            raise ValueError(
                "Docker must use its iptables backend with DOCKER-USER first"
            )
    positions = {}
    for parent, name, exact in policies:
        verify_chain(contents, parent, name, exact, position=positions.get(parent, 0))
        positions[parent] = positions.get(parent, 0) + 1


def uplink():
    def route(*args):
        return json.loads(
            command([IP, "-j", "-4", "route", *args], "Cannot read host IPv4 routes")
        )

    (external,) = route("get", "1.1.1.1")
    interface = external["dev"]
    if (
        not IFACE.fullmatch(interface)
        or interface in {"lo", BRIDGE, CONTROL_BRIDGE}
        or (Path("/sys/class/net") / interface / "bridge").exists()
        or interface not in {row.get("dev") for row in route("show", "default")}
    ):
        raise ValueError("Worker policy requires an external IPv4 default uplink")
    return interface


def verify_backend():
    info = json.loads(docker("info", "--format", "{{json .}}"))
    backend = info.get("FirewallBackend")
    version = re.match(r"([0-9]+)\.", info["ServerVersion"])
    # Older Docker versions do not report the driver and only support the
    # iptables backend. They still have to pass the live kernel hook checks.
    if backend is None and version and int(version[1]) < 29:
        return
    if not isinstance(backend, dict) or backend.get("Driver") not in {
        "iptables",
        "iptables+firewalld",
    }:
        raise ValueError("Docker must use its iptables firewall backend")


def enforce(plan, *, install=False):
    verify_backend()
    live = network(plan["network"])
    if live["Id"] != plan["network_id"]:
        raise ValueError("Worker bridge identity changed; run prepare again")
    for file in (
        "/proc/sys/net/ipv4/ip_forward",
        "/proc/sys/net/bridge/bridge-nf-call-iptables",
    ):
        if Path(file).read_text().strip() != "1":
            raise ValueError(
                "IPv4 forwarding and Linux bridge netfilter must be enabled"
            )
    cache, translated = endpoint(plan)
    wan = uplink()
    for tool, ipv6 in ((IPTABLES, False), (IP6TABLES, True)):
        expected = hook_rules(wan, cache, translated, ipv6=ipv6)
        existing = table(tool)
        if install:
            if not ipv6 and "-A FORWARD -j DOCKER-USER" not in existing.splitlines():
                raise ValueError("Docker's iptables DOCKER-USER hook is missing")
            apply(tool, expected, existing)
        verify_policies(table(tool), expected, ipv6=ipv6)


def renew(plan, receipt, boot_stamp, *, install=False):
    try:
        boot = BOOT_ID.read_text().strip()
        reinstall = not boot_stamp.is_file() or boot_stamp.read_text() != boot
        enforce(plan, install=install or reinstall)
        if reinstall:
            atomic(boot_stamp, boot.encode())
        atomic_json(
            receipt,
            {
                "schema": 1,
                "network": plan["network"],
                "network_id": plan["network_id"],
                "image": plan["image"],
                "boot_id": boot,
                "verified_at": int(time.time()),
                "metadata_blocked": True,
                "control_blocked": True,
            },
        )
    except (OSError, ValueError, KeyError, TypeError):
        receipt.unlink(missing_ok=True)
        raise
