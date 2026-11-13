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

"""Prepare the standalone Bolt CI project and operate its trusted services."""

import argparse
import fcntl
import hashlib
import hmac
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path
from urllib.parse import urlsplit

import bundle
import network_guard as guard
from bolt_ci.__main__ import _revision
from bolt_ci.config import CACHE_KEYS, IMAGE, JDKS, Config, absolute, bundle_digest
from bolt_ci.docker import DOCKER_BIN, DOCKER_ENV, WORKER_PATH, Docker, checkout
from bolt_ci.policy import HASH, LOGIN, repository, valid_sha
from bolt_ci.state import State

HERE = Path(__file__).resolve().parent
PROJECT = "bolt-gluten-ci"
ENGINE_URL = "https://github.com/bytedance/bolt.git"
CONFIG_TARGET = "/run/bolt-gluten-ci"
SERVICE = "bolt-gluten-ci-network.service"
TIMER = "bolt-gluten-ci-network.timer"
SYSTEMD = Path("/etc/systemd/system")
TAGGED_IMAGE = re.compile(r"[a-z0-9][a-z0-9._:/-]*:[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")
PORTABLE_ENV = {
    "WATCH_REPOSITORY": "apache/gluten",
    "DOCKER_NETWORK": "bolt-gluten-ci-workers",
    "MAINTAINER_LOGINS": "",
    "CONAN_REMOTE": "",
    "CONAN_CACHE_CONTAINER": "",
    "BOLT_CHECKOUT_SEED": "",
}
NUMBERS = {
    "POLL_SECONDS": (60, 1, 3600),
    "MAX_PARALLEL": (4, 1, 128),
    "QUEUE_TIMEOUT_SECONDS": (1800, 1, 86400),
    "TASK_TIMEOUT_SECONDS": (21600, 1, 86400),
    "CPUS": (16, 1, 256),
    "MEMORY_GIB": (50, 1, 2048),
}
CACHE_LAYOUT = {"conan": "seeds/conan", "spark": "seeds/spark", "pip": "seeds/pip"}
TOOL_PATH = "/opt/worker-tools"
TOOL_LIBRARY_PATH = (
    f"{TOOL_PATH}/usr/lib/x86_64-linux-gnu:{TOOL_PATH}/lib/x86_64-linux-gnu"
)
CONTROL_SERVICES = ("listener", "runner", "commenter")


def host_root(env):
    root = absolute(env.get("HOST_DATA_DIR", ""))
    if root == Path("/") or not root.is_dir():
        raise ValueError("HOST_DATA_DIR must be an existing canonical data directory")
    return root


def settings(env):
    root = host_root(env)
    values = {key: env.get(key, default) for key, default in PORTABLE_ENV.items()}
    for key in ("CI_IMAGE", "BOLT_REF", "READ_TOKEN_FILE", "BOT_TOKEN_FILE"):
        if not env.get(key):
            raise ValueError(f"Required deployment setting is missing ({key})")
        values[key] = env[key]
    image = values["CI_IMAGE"]
    if not (IMAGE.fullmatch(image) or TAGGED_IMAGE.fullmatch(image)):
        raise ValueError("CI_IMAGE must have an explicit registry tag or digest")
    if not valid_sha(values["BOLT_REF"]):
        raise ValueError("BOLT_REF must be a full lowercase commit SHA")
    if not guard.valid_network(values["DOCKER_NETWORK"]):
        raise ValueError("DOCKER_NETWORK must name a dedicated worker network")
    if not repository(values["WATCH_REPOSITORY"]):
        raise ValueError("Invalid WATCH_REPOSITORY")
    maintainers = (
        values["MAINTAINER_LOGINS"].split(",") if values["MAINTAINER_LOGINS"] else []
    )
    maintainers = [name.strip() for name in maintainers]
    if any(not LOGIN.fullmatch(name) for name in maintainers):
        raise ValueError("Invalid MAINTAINER_LOGINS")
    if env.get("STATUS_PUBLISHING", "false") != "false":
        raise ValueError("Only comment publishing is enabled")
    paths = {
        "READ_TOKEN_FILE": values["READ_TOKEN_FILE"],
        "BOT_TOKEN_FILE": values["BOT_TOKEN_FILE"],
        "ARROW_CACHE_SCRIPT": env.get(
            "ARROW_CACHE_SCRIPT", str(root / "conan-source-cache/arrow-cache.py")
        ),
        "ARROW_SOURCE_DIR": env.get(
            "ARROW_SOURCE_DIR", str(root / "conan-source-cache/s")
        ),
        "CCACHE_SEED_DIR": env.get("CCACHE_SEED_DIR", str(root / "ccache-data")),
    }
    if values["BOLT_CHECKOUT_SEED"]:
        paths["BOLT_CHECKOUT_SEED"] = values["BOLT_CHECKOUT_SEED"]
    values.update({key: str(absolute(path)) for key, path in paths.items()})
    remote = values["CONAN_REMOTE"]
    if remote:
        url = urlsplit(remote)
        if (
            not re.fullmatch(
                r"https?://[a-zA-Z0-9.-]+:[0-9]+(?:/[a-zA-Z0-9_./-]*)?", remote
            )
            or not url.port
        ):
            raise ValueError(
                "CONAN_REMOTE must be a credential-free HTTP(S) endpoint and port"
            )
        if url.hostname == "host.docker.internal":
            if not HASH.fullmatch(values["CONAN_CACHE_CONTAINER"]):
                raise ValueError(
                    "The published Conan alias requires the exact container ID"
                )
        else:
            guard.cache_address(url.hostname)
    if values["CONAN_CACHE_CONTAINER"] and not (
        remote and urlsplit(remote).hostname == "host.docker.internal"
    ):
        raise ValueError("CONAN_CACHE_CONTAINER is only used with host.docker.internal")

    def integer(key, default, minimum, maximum):
        raw = env.get(key, str(default))
        if not re.fullmatch(r"[0-9]{1,19}", raw) or not minimum <= int(raw) <= maximum:
            raise ValueError(f"Invalid deployment limit ({key})")
        return int(raw)

    values.update(
        {key.lower(): integer(key, *limits) for key, limits in NUMBERS.items()}
    )
    values["min_free_gib"] = integer("MIN_FREE_GIB", 80, 5, 65536)
    values["only_pr"] = (
        integer("ONLY_PR", 1, 1, 2**63 - 1) if env.get("ONLY_PR") else None
    )
    values["maintainer_logins"] = maintainers
    values["HOST_DATA_DIR"] = str(root)
    return values


def secure_directory(path):
    if path.exists():
        info = path.stat()
        if (
            path.is_symlink()
            or not path.is_dir()
            or info.st_uid != 0
            or info.st_mode & 0o077
        ):
            raise ValueError("Controller directory must be root-owned and mode 0700")
    else:
        path.mkdir(mode=0o700)


def token(path):
    info = path.stat()
    if (
        path.is_symlink()
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid != 0
        or info.st_mode & 0o077
        or info.st_nlink != 1
        or info.st_size > 4096
    ):
        raise ValueError("Credential files must be root-owned private regular files")
    value = path.read_bytes().strip()
    if not re.fullmatch(rb"[A-Za-z0-9_.-]{1,4096}", value):
        raise ValueError("Invalid GitHub credential file format")
    return value


def check_host_paths(values, runtime, assets):
    private = runtime / "control"
    jobs = runtime / "jobs"
    root = Path(values["HOST_DATA_DIR"])
    if root.stat().st_uid != 0 or root.stat().st_mode & 0o022:
        raise ValueError(
            "The data directory must be root-owned and not group/world writable"
        )
    reader, writer = (
        Path(values[key]) for key in ("READ_TOKEN_FILE", "BOT_TOKEN_FILE")
    )
    trusted = (
        assets,
        runtime / "assets",
        Path(values["ARROW_SOURCE_DIR"]),
        Path(values["ARROW_CACHE_SCRIPT"]),
        Path(values["CCACHE_SEED_DIR"]),
        runtime / "cache",
    )
    if values["BOLT_CHECKOUT_SEED"]:
        trusted += (Path(values["BOLT_CHECKOUT_SEED"]),)
    for asset in trusted:
        for protected in (reader, writer, private, jobs):
            if protected.is_relative_to(asset) or asset.is_relative_to(protected):
                raise ValueError("Worker assets overlap a private controller path")
    if any(
        secret.is_relative_to(private) or secret.is_relative_to(jobs)
        for secret in (reader, writer)
    ):
        raise ValueError("Credential files must be outside controller state and jobs")
    if reader == writer or hmac.compare_digest(token(reader), token(writer)):
        raise ValueError("Use two distinct GitHub credentials")
    if (
        not Path(values["ARROW_CACHE_SCRIPT"]).is_file()
        or not Path(values["ARROW_SOURCE_DIR"]).is_dir()
    ):
        raise ValueError(
            "The existing Arrow cache source directory and script are required"
        )


def compose_env(private, configured=None):
    if configured is not None:
        return {key: str(value) for key, value in configured.items()}
    # A complete render-only fixture for tests and static Compose validation.
    keys = (
        "CONFIG",
        "HELPERS",
        "STATE",
        "READER",
        "WRITER",
        "ARROW_SCRIPT",
        "JOBS",
        "ENGINE",
        "RECEIPTS",
        "MAVEN",
        "SEEDS",
        "CCACHE",
        "DRIVER",
        "JDKS",
        "TOOLS",
        "ARROW_SOURCES",
    )
    return {f"CI_{key}": str(private / key.lower()) for key in keys} | {
        "CI_CONTROL_IMAGE": "bolt-gluten-ci-controller:fixture"
    }


def compose_command(private, args, configured=None):
    if configured is None:
        configured = (
            json.loads((private / "compose.json").read_bytes())
            if (private / "compose.json").is_file()
            else {}
        )
    cmd = [
        DOCKER_BIN,
        "compose",
        "--env-file",
        "/dev/null",
        "--project-name",
        PROJECT,
        "--project-directory",
        str(HERE),
        "--file",
        str(HERE / "compose.yml"),
        *args,
    ]
    return cmd, DOCKER_ENV | compose_env(private, configured)


def compose(private, *args, capture=False):
    cmd, env = compose_command(private, args)
    result = subprocess.run(
        cmd,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE,
        timeout=300,
    )
    if result.returncode:
        raise ValueError(
            "Bolt Compose operation failed; see service logs or Docker daemon logs"
        )
    return result.stdout


def image_record(image):
    result = subprocess.run(
        [DOCKER_BIN, "image", "inspect", image],
        env=DOCKER_ENV,
        capture_output=True,
        timeout=60,
        check=False,
    )
    if result.returncode:
        if any(
            message in result.stderr.lower()
            for message in (b"no such image", b"no such object")
        ):
            return None
        raise ValueError("Docker could not inspect a local image")
    (image_record,) = json.loads(result.stdout)
    if image_record["Os"] != "linux" or image_record["Architecture"] != "amd64":
        raise ValueError("The worker and control images must be Linux/amd64")
    return image_record


def worker_image(reference):
    record = image_record(reference)
    if record is None or not record.get("RepoDigests"):
        guard.docker("pull", "--platform=linux/amd64", reference, timeout=1800)
        record = image_record(reference)
    if "@" in reference:
        pin = reference
    else:
        repo = reference.rsplit(":", 1)[0]
        matches = [
            digest
            for digest in (record or {}).get("RepoDigests", ())
            if digest.split("@")[0]
            in {repo, f"docker.io/{repo}", f"{repo.removeprefix('docker.io/')}"}
            and IMAGE.fullmatch(digest)
        ]
        if len(matches) != 1:
            raise ValueError(
                "CI_IMAGE did not resolve to one registry digest; supply an explicit digest"
            )
        (pin,) = matches
    pinned = image_record(pin)
    if not pinned or pinned["Id"] != record["Id"] or pin not in record["RepoDigests"]:
        raise ValueError("The immutable worker image is not available locally")
    return pin


def controller_image(revision):
    tag = f"bolt-gluten-ci-controller:{revision}"
    current = image_record(tag)
    if not current:
        guard.docker(
            "build",
            "--platform=linux/amd64",
            "--tag",
            tag,
            "--label",
            f"io.bolt.gluten-ci.release={revision}",
            str(HERE),
            timeout=1200,
        )
        current = image_record(tag)
    if (
        not current
        or (current["Config"]["Labels"] or {}).get("io.bolt.gluten-ci.release")
        != revision
    ):
        raise ValueError("The local controller tag does not match the trusted release")
    return tag, current["Id"]


def immutable_checkout(destination, sha, seed=""):
    if not destination.exists():
        url = Path(seed).as_uri() if seed else ENGINE_URL
        with tempfile.TemporaryDirectory(dir=destination.parent) as temp:
            candidate = Path(temp) / "checkout"
            checkout(candidate, url, sha, local=bool(seed))
            candidate.rename(destination)
    _revision(destination, sha)


def jdk_asset(major):
    request = (
        f"https://api.adoptium.net/v3/assets/feature_releases/{major}/ga"
        "?architecture=x64&heap_size=normal&image_type=jdk&jvm_impl=hotspot"
        "&os=linux&page_size=1&project=jdk&sort_order=DESC&vendor=eclipse"
    )
    with urllib.request.urlopen(request, timeout=60) as stream:
        releases = json.loads(stream.read(2_000_001))
    asset = releases[0]["binaries"][0]["package"]
    link, digest, size = asset["link"], asset["checksum"], asset["size"]
    if (
        not link.startswith(
            f"https://github.com/adoptium/temurin{major}-binaries/releases/download/"
        )
        or not link.endswith(".tar.gz")
        or not HASH.fullmatch(digest)
        or type(size) is not int
        or not 0 < size <= 500_000_000
    ):
        raise ValueError("Adoptium returned an unexpected JDK archive descriptor")
    return link, digest, size


def download_jdk_archive(link, digest, size, target):
    # Verify before extracting. Read through EOF so an oversized response fails.
    hasher = hashlib.sha256()
    remaining = size
    with (
        urllib.request.urlopen(link, timeout=120) as incoming,
        target.open("xb") as output,
    ):
        if urlsplit(incoming.geturl()).scheme != "https":
            raise ValueError("The JDK download did not use HTTPS")
        while block := incoming.read(min(remaining + 1, 1024 * 1024)):
            remaining -= len(block)
            if remaining < 0:
                raise ValueError("The JDK download exceeds its expected size")
            hasher.update(block)
            output.write(block)
    if remaining or not hmac.compare_digest(hasher.hexdigest(), digest):
        raise ValueError("The JDK archive checksum or size did not match")


def prepare_jdk(parent, major):
    marker = parent / f"temurin-{major}.json"
    if not marker.is_file():
        link, digest, size = jdk_asset(major)
        destination = parent / f"temurin-{major}-{digest}"
        if not destination.exists():
            with tempfile.TemporaryDirectory(dir=parent) as temp:
                candidate = Path(temp)
                archive_file = candidate / "jdk.tar.gz"
                download_jdk_archive(link, digest, size, archive_file)
                extracted = candidate / "tree"
                with tarfile.open(archive_file, mode="r:gz") as archive:
                    archive.extractall(extracted, filter="data")
                (home,) = extracted.iterdir()
                if not (home / "bin/javac").is_file():
                    raise ValueError("The verified JDK archive has no javac")
                home.rename(destination)
        guard.atomic_json(marker, {"sha256": digest, "source": link})
    digest = json.loads(marker.read_bytes())["sha256"]
    if not HASH.fullmatch(digest):
        raise ValueError("Invalid cached JDK checksum")
    destination = parent / f"temurin-{major}-{digest}"
    if (
        absolute(str(destination)) != destination
        or not (destination / "bin/javac").is_file()
    ):
        raise ValueError("Prepared JDK cache is incomplete")
    return destination


def mount(source, target, readonly=True):
    return f"type=bind,source={source},target={target}" + (
        ",readonly" if readonly else ""
    )


def prepare_tools(parent, image):
    destination = parent / image.split("@sha256:")[1]
    if destination.is_dir():
        return destination
    with tempfile.TemporaryDirectory(dir=parent) as temp:
        output = Path(temp) / "prefix"
        output.mkdir(mode=0o755)
        script = """
set -euo pipefail
apt-get update
apt-get -y --no-install-recommends --download-only install jq libxml2-utils unzip
for package in /var/cache/apt/archives/*.deb; do
  test -f "$package" || continue
  name=$(dpkg-deb -f "$package" Package)
  case "$name" in
    jq|libjq1|libonig5|libxml2-utils|libxml2|libxml2-16|unzip)
      dpkg-deb --extract "$package" /out
      ;;
  esac
done
"""
        guard.docker(
            "run",
            "--rm",
            "--pull=never",
            "--platform=linux/amd64",
            "--user=0:0",
            "--security-opt=no-new-privileges",
            "--mount",
            mount(output, "/out", False),
            "--entrypoint=/bin/bash",
            image,
            "-c",
            script,
            timeout=600,
        )
        probe_tools(image, output)
        output.rename(destination)
    return destination


def worker_probe(image, network, entry, *args, options=()):
    return guard.docker(
        "run",
        "--rm",
        "--pull=never",
        "--platform=linux/amd64",
        f"--network={network}",
        "--user=1000:1000",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--sysctl=net.ipv6.conf.all.disable_ipv6=1",
        "--pids-limit=128",
        "--entrypoint",
        entry,
        *options,
        image,
        *args,
        timeout=90,
    )


def probe_tools(image, tools):
    worker_probe(
        image,
        "none",
        "/bin/bash",
        "-c",
        "set -eu; jq --version; xmllint --version; unzip -v; shasum --version; "
        "flock --version; clang-22 --version; /opt/venv/bin/conan --version",
        options=(
            "--mount",
            mount(tools, TOOL_PATH),
            "--env",
            f"PATH={TOOL_PATH}/usr/bin:{TOOL_PATH}/bin:{WORKER_PATH}",
            "--env",
            f"LD_LIBRARY_PATH={TOOL_LIBRARY_PATH}",
        ),
    )


def probe_jdks(image, jdk_dirs):
    for major, path in jdk_dirs.items():
        output = worker_probe(
            image,
            "none",
            "/bin/bash",
            "-c",
            "/opt/job-jdk/bin/javac -version 2>&1",
            options=(
                "--mount",
                mount(path, "/opt/job-jdk"),
            ),
        )
        expected = b"1.8" if major == 8 else str(major).encode()
        if not re.fullmatch(
            rb"javac " + re.escape(expected) + rb"(?:[._+\-][^\n]*)?\s*", output
        ):
            raise ValueError("A cached JDK returned the wrong javac version")


def cache_gateway(image):
    result = worker_probe(
        image,
        "none",
        "/bin/cat",
        "/etc/hosts",
        options=("--add-host", "host.docker.internal:host-gateway"),
    )
    candidates = [
        columns[0]
        for line in result.decode().splitlines()
        if (columns := line.split())
        and "host.docker.internal" in columns[1:]
        and ":" not in columns[0]
    ]
    (gateway,) = candidates
    return guard.cache_address(gateway)


def probe_worker(
    plan, arrow_script, arrow_sources, helper_dir, driver_dir, control_address
):
    # Current kernel rules are the proof. These checks also detect an unusable
    # resolver, a missing cache, or exposed metadata in the real job image.
    script = """
import socket
import sys
from pathlib import Path
from urllib.parse import urlsplit
assert not Path("/var/run/docker.sock").exists()
assert not Path("/run/bolt-gluten-ci/reader").exists()
assert not Path("/run/bolt-gluten-ci/writer").exists()
assert Path("/work/helpers/scripts/gluten-ci/run.py").is_file()
assert Path("/work/driver/.github/scripts/DiscoverGlutenSuites.java").is_file()
assert Path("/opt/bolt-arrow-cache.py").is_file()
assert Path("/opt/bolt-arrow-sources").is_dir()
for address, port in (("169.254.169.254", 80), ("100.100.100.200", 80), (sys.argv[2], 39127)):
    try:
        sock = socket.create_connection((address, port), timeout=2)
    except OSError:
        continue
    sock.close()
    raise SystemExit("protected destination reachable")
with socket.create_connection(("api.github.com", 443), timeout=12):
    pass
if sys.argv[1]:
    remote = urlsplit(sys.argv[1])
    with socket.create_connection((remote.hostname, remote.port), timeout=8):
        pass
"""
    options = (
        "--mount",
        mount(arrow_script, "/opt/bolt-arrow-cache.py"),
        "--mount",
        mount(arrow_sources, "/opt/bolt-arrow-sources"),
        "--mount",
        mount(helper_dir, "/work/helpers"),
        "--mount",
        mount(driver_dir, "/work/driver"),
    )
    if plan["host_gateway"]:
        options += ("--add-host", "host.docker.internal:host-gateway")
    worker_probe(
        plan["image"],
        plan["network"],
        "/opt/venv/bin/python3",
        "-c",
        script,
        plan["conan_remote"],
        control_address,
        options=options,
    )


def systemd(*args):
    return guard.command(
        ["/usr/bin/systemctl", *args],
        "Could not manage the Bolt network guard timer",
    )


def install_timer(private):
    # Both paths are canonical absolute paths with no systemd specifiers.
    service = f"""[Unit]
Description=Verify Bolt Gluten CI worker isolation
After=docker.service
Requisite=docker.service

[Service]
Type=oneshot
TimeoutStartSec=60
ExecStart=/usr/bin/python3 -B "{HERE / "prepare.py"}" _guard "{private}"
"""
    timer = """[Unit]
Description=Renew Bolt Gluten CI worker isolation proof

[Timer]
OnBootSec=20s
OnUnitActiveSec=20s
AccuracySec=1s

[Install]
WantedBy=timers.target
"""
    for name, unit in ((SERVICE, service), (TIMER, timer)):
        path = SYSTEMD / name
        guard.atomic(path, unit.encode())
        path.chmod(0o644)
    systemd("daemon-reload")
    systemd("enable", "--now", TIMER)


def control_canary(image):
    identifier = (
        guard.docker(
            "run",
            "--detach",
            "--rm",
            "--pull=never",
            "--network",
            guard.CONTROL_NETWORK,
            "--entrypoint=/usr/local/bin/python3",
            image,
            "-m",
            "http.server",
            "39127",
        )
        .decode()
        .strip()
    )
    if not HASH.fullmatch(identifier):
        raise ValueError("Invalid control isolation probe container")
    return identifier


def prepared_files(values, runtime, private, manifest):
    assets = runtime / "assets"
    helper = HERE.parent / "helpers"
    driver = HERE.parent / "driver"
    _revision(driver, bundle.DRIVER_SHA)
    if not (driver / bundle.DRIVER_SCRIPTS / "DiscoverGlutenSuites.java").is_file():
        raise ValueError("The pinned driver has no suite discovery script")
    helper_digest = bundle_digest(helper)
    if set(CACHE_LAYOUT) | {"maven", "ccache"} != CACHE_KEYS:
        raise ValueError("Deployment cache layout and controller do not agree")
    for directory in (
        assets,
        assets / "jdks",
        assets / "tools",
        assets / "engines",
        runtime / "cache",
    ):
        if absolute(str(directory)) != directory:
            raise ValueError("Deployment cache directory is not canonical")
        directory.mkdir(mode=0o755, exist_ok=True)
    for directory in ("state", "receipts"):
        secure_directory(private / directory)
    secure_directory(runtime / "jobs")
    for entry in ("maven", *CACHE_LAYOUT.values(), "ccache"):
        directory = runtime / "cache" / entry
        absolute(str(directory))
        directory.mkdir(mode=0o755, parents=True, exist_ok=True)
    ccache = Path(values["CCACHE_SEED_DIR"])
    if not ccache.is_dir():
        ccache = runtime / "cache/ccache"
    engine = assets / "engines" / values["BOLT_REF"]
    immutable_checkout(engine, values["BOLT_REF"], values["BOLT_CHECKOUT_SEED"])
    worker = worker_image(values["CI_IMAGE"])
    image, control_id = controller_image(manifest["bolt_commit"])
    jdk_dirs = {
        version: prepare_jdk(assets / "jdks", version) for version in sorted(JDKS)
    }
    probe_jdks(worker, jdk_dirs)
    tools = prepare_tools(assets / "tools", worker)
    probe_tools(worker, tools)
    arrow_script, arrow_sources = (
        Path(values[key]) for key in ("ARROW_CACHE_SCRIPT", "ARROW_SOURCE_DIR")
    )
    config = {
        "repository": values["WATCH_REPOSITORY"],
        "host_data_dir": values["HOST_DATA_DIR"],
        "state_dir": str(private / "state"),
        "helper_dir": str(helper),
        "bolt_checkout": str(engine),
        "driver_dir": str(driver),
        "jdk_dirs": {str(version): str(path) for version, path in jdk_dirs.items()},
        "image": worker,
        "bolt_sha": values["BOLT_REF"],
        "driver_sha": bundle.DRIVER_SHA,
        "helper_sha": helper_digest,
        "network": values["DOCKER_NETWORK"],
        "network_preflight_file": str(private / "receipts/network.json"),
        "network_preflight_max_age_seconds": 90,
        "read_token_file": f"{CONFIG_TARGET}/reader",
        "bot_token_file": f"{CONFIG_TARGET}/writer",
        "maintainer_logins": values["maintainer_logins"],
        "only_pr": values["only_pr"],
        "cache_seed_dirs": {
            "maven": str(runtime / "cache/maven"),
            "ccache": str(ccache),
            **{
                key: str(runtime / "cache" / path) for key, path in CACHE_LAYOUT.items()
            },
        },
        "worker_tools_dir": str(tools),
        "arrow_cache_script": str(arrow_script),
        "arrow_source_dir": str(arrow_sources),
        "conan_remote": values["CONAN_REMOTE"],
        "status_publishing": False,
        **{key.lower(): values[key.lower()] for key in NUMBERS},
    }
    alias = bool(
        config["conan_remote"]
        and urlsplit(config["conan_remote"]).hostname == "host.docker.internal"
    )
    if alias:
        config["conan_extra_host"] = "host.docker.internal:host-gateway"
    c = Config.from_mapping(config)
    network = guard.network(c.network, create=True)
    network_plan = {
        "network": c.network,
        "network_id": network["Id"],
        "image": c.image,
        "conan_remote": c.conan_remote,
        "conan_container": values["CONAN_CACHE_CONTAINER"],
        "host_gateway": cache_gateway(c.image) if alias else "",
    }
    compose_variables = {
        "CI_CONFIG": str(private / "config.json"),
        "CI_HELPERS": c.helper_dir,
        "CI_STATE": c.state_dir,
        "CI_READER": values["READ_TOKEN_FILE"],
        "CI_WRITER": values["BOT_TOKEN_FILE"],
        "CI_ARROW_SCRIPT": c.arrow_cache_script,
        "CI_JOBS": c.jobs_dir,
        "CI_ENGINE": c.bolt_checkout,
        "CI_RECEIPTS": c.network_preflight_file.parent,
        "CI_MAVEN": c.cache_seed_dirs["maven"],
        "CI_SEEDS": runtime / "cache/seeds",
        "CI_CCACHE": c.cache_seed_dirs["ccache"],
        "CI_DRIVER": c.driver_dir,
        "CI_JDKS": assets / "jdks",
        "CI_TOOLS": c.worker_tools_dir,
        "CI_ARROW_SOURCES": c.arrow_source_dir,
        "CI_CONTROL_IMAGE": image,
    }
    guard.atomic_json(private / "config.json", config)
    guard.atomic_json(private / "compose.json", compose_env(private, compose_variables))
    return c, network_plan, image, control_id


def runtime_dir(env):
    directory = host_root(env) / "gluten-external-ci"
    return absolute(str(directory))


def require_local_host():
    if os.geteuid() != 0:
        raise ValueError("Run ci.sh as root on the trusted Docker host")
    if os.uname().sysname != "Linux" or os.uname().machine != "x86_64":
        raise ValueError("Deployment requires a Linux x86_64 Docker host")
    if not Path("/var/run/docker.sock").is_socket():
        raise ValueError("The local Docker socket is unavailable")


def load_plan(private):
    return json.loads((private / "network.json").read_bytes())


def ready_file(private):
    return private / "ready.json"


def receipt_file(private):
    return private / "receipts/network.json"


def deployment_identity(values, private):
    return hashlib.sha256(
        json.dumps(
            {
                "settings": values,
                "source": str(HERE),
                "manifest": bundle.digest_file(HERE.parent / bundle.MANIFEST),
                "config": bundle.digest_file(private / "config.json"),
                "compose": bundle.digest_file(private / "compose.json"),
                "network": bundle.digest_file(private / "network.json"),
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()


def prepare(values, runtime, private):
    check_host_paths(values, runtime, HERE.parent)
    manifest = bundle.verify_manifest(HERE.parent)
    guard.docker("compose", "version", "--short")
    if shutil.disk_usage(runtime).free < values["min_free_gib"] * 1024**3:
        raise ValueError(
            "Insufficient free space for prepared assets and private worker caches"
        )
    if manifest["driver_commit"] != bundle.DRIVER_SHA:
        raise ValueError("The bundle does not contain the approved driver revision")
    if (
        guard.docker(
            "ps",
            "--quiet",
            "--filter",
            f"label=com.docker.compose.project={PROJECT}",
        ).strip()
        or guard.docker(
            "ps",
            "--quiet",
            "--filter",
            f"network={values['DOCKER_NETWORK']}",
            "--filter",
            "label=io.bolt.gluten-ci.instance",
        ).strip()
    ):
        raise ValueError(
            "Stop the Bolt CI services and workers before preparing a release"
        )
    ready_file(private).unlink(missing_ok=True)
    receipt_file(private).unlink(missing_ok=True)
    try:
        c, plan, control_image, control_id = prepared_files(
            values, runtime, private, manifest
        )
        guard.enforce(plan, install=True)
        guard.renew(plan, c.network_preflight_file, private / "guard-boot")
        compose(private, "run", "--rm", "--no-deps", "-T", "preflight")
        canary = control_canary(control_image)
        try:
            guard.docker(
                "exec",
                canary,
                "/usr/local/bin/python3",
                "-c",
                "import socket; import time; time.sleep(1); "
                "socket.create_connection(('127.0.0.1', 39127), 5).close()",
            )
            (data,) = json.loads(guard.docker("container", "inspect", canary))
            address = data["NetworkSettings"]["Networks"][guard.CONTROL_NETWORK][
                "IPAddress"
            ]
            probe_worker(
                plan,
                c.arrow_cache_script,
                c.arrow_source_dir,
                c.helper_dir,
                c.driver_dir,
                address,
            )
        finally:
            guard.docker("rm", "--force", canary)
        guard.atomic_json(private / "network.json", plan)
        install_timer(private)
        guard.atomic_json(
            ready_file(private),
            {
                "identity": deployment_identity(values, private),
                "control_id": control_id,
            },
        )
    except BaseException:
        receipt_file(private).unlink(missing_ok=True)
        raise
    print("Bolt CI prepared. Start with enqueue --dry-run for the validation PR.")


def check_ready(values, runtime, private):
    bundle.verify_manifest(HERE.parent)
    marker = json.loads(ready_file(private).read_bytes())
    if marker["identity"] != deployment_identity(values, private):
        raise ValueError("Deployment settings changed; stop and run ci.sh prepare")
    check_host_paths(values, runtime, HERE.parent)
    current = image_record(
        json.loads((private / "compose.json").read_bytes())["CI_CONTROL_IMAGE"]
    )
    if not current or current["Id"] != marker["control_id"]:
        raise ValueError("The controller image changed; run ci.sh prepare")
    c = Config.load(private / "config.json")

    if not image_record(c.image) or not Docker(c).verify_network():
        raise ValueError(
            "Worker image or current firewall proof is missing; run ci.sh prepare"
        )
    try:
        guard.enforce(load_plan(private))
    except (OSError, ValueError, KeyError, TypeError):
        c.network_preflight_file.unlink(missing_ok=True)
        raise
    systemd("is-active", TIMER)


def stop(private):
    # Freeze claiming before touching workers. The kernel policy and host
    # renewal timer stay active throughout the stop and while services are off.
    compose(private, "stop", "listener", "runner")
    cfg = Config.load(private / "config.json")
    state = State(cfg.state_dir, cfg.repository, cfg.matrix, cfg.fingerprint)
    docker = Docker(cfg)
    active = {task["container"]: task for task in state.active()}
    for name in sorted(docker.names(state.instance)):
        container = docker.inspect(name)
        if container and container.status in {"running", "restarting", "created"}:
            if name in active:
                state.mark_stopping(active[name], "interrupted")
            if container.status != "created":
                docker.stop(name)
    compose(private, "stop", "commenter")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("prepare", "up", "status", "logs", "stop"):
        commands.add_parser(name)
    enqueue = commands.add_parser("enqueue")
    enqueue.add_argument("--pr", required=True, type=int)
    enqueue.add_argument("--dry-run", action="store_true")
    timer = commands.add_parser("_guard", help=argparse.SUPPRESS)
    timer.add_argument("private", type=Path)
    return parser.parse_args()


def main():
    args = parse_args()
    # Validate the exported host settings before contacting Docker.
    if args.command == "_guard":
        private = absolute(str(args.private))
    else:
        runtime = runtime_dir(os.environ)
        private = runtime / "control"
        values = settings(os.environ) if args.command in {"prepare", "up"} else None
    require_local_host()
    os.umask(0o022)
    if args.command == "prepare":
        runtime.mkdir(mode=0o755, exist_ok=True)
        secure_directory(private)
    if not private.is_dir():
        raise ValueError("Run ci.sh prepare first")
    secure_directory(private)
    with (private / "operation.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if args.command == "_guard":
            if not ready_file(private).is_file():
                receipt_file(private).unlink(missing_ok=True)
                raise ValueError("Deployment has no completed preparation record")
            try:
                guard.renew(
                    load_plan(private), receipt_file(private), private / "guard-boot"
                )
            except (OSError, ValueError, KeyError, TypeError):
                receipt_file(private).unlink(missing_ok=True)
                raise
        elif args.command == "prepare":
            prepare(values, runtime, private)
        elif args.command == "up":
            check_ready(values, runtime, private)
            compose(
                private, "up", "-d", "--no-build", "--pull", "never", *CONTROL_SERVICES
            )
        elif args.command == "status":
            compose(private, "ps", "--all", *CONTROL_SERVICES)
            compose(private, "run", "--rm", "--no-deps", "-T", "status")
        elif args.command == "logs":
            compose(private, "logs", "--no-color", "--tail", "100", *CONTROL_SERVICES)
        elif args.command == "stop":
            stop(private)
        elif args.command == "enqueue":
            if not 0 < args.pr < 2**63:
                raise ValueError("Invalid PR number")
            command = ("enqueue", "--pr", str(args.pr))
            if args.dry_run:
                compose(
                    private,
                    "run",
                    "--rm",
                    "--no-deps",
                    "-T",
                    "preview",
                    *command,
                    "--dry-run",
                )
            else:
                # Use the running listener, keeping its state and reader access.
                compose(
                    private,
                    "exec",
                    "-T",
                    "listener",
                    "/usr/local/bin/python3",
                    "-m",
                    "bolt_ci",
                    *command,
                )


if __name__ == "__main__":
    try:
        main()
    except ValueError as error:
        # Errors from validated boundaries are deliberately independent of
        # credential values. Do not dump command output, env, or the config.
        if type(error) is ValueError:
            print(str(error), file=sys.stderr)
        else:
            print("Invalid deployment data or response", file=sys.stderr)
        sys.exit(1)
    except (OSError, KeyError, TypeError, RuntimeError, subprocess.SubprocessError):
        print(
            "Bolt CI operation failed; check local prerequisites and service logs",
            file=sys.stderr,
        )
        sys.exit(1)
