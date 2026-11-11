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

"""Worker sandbox arguments, network identity, and private Git checkouts."""

import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from .policy import HASH, repository, valid_sha

PROCESS_ENV = {"PATH": "/usr/bin:/bin", "HOME": "/nonexistent", "LC_ALL": "C"}
DOCKER_ENV = PROCESS_ENV | {"DOCKER_HOST": "unix:///var/run/docker.sock"}
DOCKER_BIN = "/usr/bin/docker"
GIT_BIN = "/usr/bin/git"
NAME = re.compile(r"bolt-gluten-[0-9a-f]{12}-[0-9]+-[0-9]+")
INSTANCE = "io.bolt.gluten-ci.instance"
WORKER_UID = 1000
WORKER_GID = 1000
HELPER_PATH = "/work/helpers/scripts/gluten-ci/run.py"
WORKER_PATH = (
    "/opt/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
)


class DockerError(RuntimeError):
    def __init__(self):
        super().__init__("Docker worker operation failed")


@dataclass(frozen=True)
class Container:
    name: str
    labels: dict
    status: str
    exit_code: int
    oom: bool
    id: str = ""


def checkout(target, url, sha, *, local=False):
    if not valid_sha(sha):
        raise ValueError("Invalid checkout pin")
    if local:
        if not url.startswith("file:///"):
            raise ValueError("Invalid local prepared checkout")
    else:
        if (
            not url.startswith("https://github.com/")
            or not url.endswith(".git")
            or not repository(
                url.removeprefix("https://github.com/").removesuffix(".git")
            )
        ):
            raise ValueError("Invalid public source checkout")
    target.mkdir(exist_ok=False)
    env = PROCESS_ENV | {
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ALLOW_PROTOCOL": "file" if local else "https",
        "GIT_LFS_SKIP_SMUDGE": "1",
    }

    def git(*args):
        try:
            return subprocess.run(
                [
                    GIT_BIN,
                    "-c",
                    "core.hooksPath=/dev/null",
                    "-c",
                    "credential.helper=",
                    "-c",
                    "http.followRedirects=false",
                    "-C",
                    str(target),
                    *args,
                ],
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                check=True,
                timeout=150,
            ).stdout
        except (OSError, subprocess.SubprocessError):
            raise RuntimeError("Pinned checkout could not be staged") from None

    git("init", "--template=")
    git("fetch", "--no-tags", "--no-recurse-submodules", "--depth=1", url, sha)
    if (
        git("rev-parse", "--verify", "FETCH_HEAD^{commit}").strip().decode("ascii")
        != sha
    ):
        raise ValueError("Staged checkout does not match its pin")
    git("checkout", "--detach", "--no-recurse-submodules", sha)


class Workspaces:
    def __init__(self, config):
        self.config = config

    def job(self, task):
        request, ordinal = task["request_id"], task["ordinal"]
        if (
            type(request) is not int
            or type(ordinal) is not int
            or request < 1
            or ordinal < 0
        ):
            raise ValueError("Invalid worker identity")
        return self.config.jobs_dir / str(request) / str(ordinal)

    def artifact(self, task):
        return self.job(task).parent / "native"

    def stage(self, task):
        job = self.job(task)
        job.mkdir(mode=0o700, parents=True)
        for path in (job, job.parent, job.parent.parent):
            if path.resolve() != path:
                raise ValueError("Worker directory must be canonical")
            path.chmod(0o700)
        checkout(
            job / "source",
            f"https://github.com/{task['head_repo']}.git",
            task["head_sha"],
        )
        checkout(
            job / "bolt",
            self.config.bolt_checkout.as_uri(),
            self.config.bolt_sha,
            local=True,
        )
        (job / "reports").mkdir()
        (job / "cache").mkdir()
        (job / "native-out").mkdir()
        for key in ("maven", "conan", "spark", "pip", "ccache"):
            destination = job / "cache" / key
            seed = self.config.cache_seed_dirs.get(key)
            if seed:
                shutil.copytree(seed, destination, symlinks=True)
            else:
                destination.mkdir()
        for child in job.iterdir():
            for parent, dirs, files in os.walk(child, followlinks=False):
                os.chown(parent, WORKER_UID, WORKER_GID, follow_symlinks=False)
                for name in dirs + files:
                    os.chown(
                        Path(parent) / name,
                        WORKER_UID,
                        WORKER_GID,
                        follow_symlinks=False,
                    )

    def clean(self, task):
        if self.config.keep_workdirs:
            return
        job = self.job(task)
        if job.resolve() != job:
            raise ValueError("Unsafe worker cleanup root")
        for name in ("source", "bolt", "cache", "native-out"):
            child = job / name
            if child.is_symlink():
                child.unlink()
            elif child.is_dir():
                shutil.rmtree(child)


class Docker:
    def __init__(
        self,
        config,
        *,
        run=subprocess.run,
        clock=time.time,
        boot_id_path=Path("/proc/sys/kernel/random/boot_id"),
    ):
        self.config = config
        self.run = run
        self.clock = clock
        self.boot_id_path = boot_id_path

    def _command(self, args, *, allow_missing=False, output=None, timeout=45):
        try:
            result = self.run(
                [DOCKER_BIN, *args],
                env=DOCKER_ENV,
                cwd="/",
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE if output is None else output,
                stderr=subprocess.PIPE if output is None else output,
                timeout=timeout,
            )
        except (OSError, subprocess.SubprocessError):
            raise DockerError() from None
        if result.returncode:
            if allow_missing and any(
                text in result.stderr.lower()
                for text in (b"no such object", b"no such container")
            ):
                return None
            raise DockerError()
        return result.stdout

    def verify_network(self):
        try:
            path = self.config.network_preflight_file
            if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o022:
                return False
            with path.open("rb") as stream:
                raw = stream.read(8193)
            if len(raw) > 8192:
                return False
            receipt = json.loads(raw)
            now = int(self.clock())
            verified = receipt["verified_at"]
            if (
                type(receipt["schema"]) is not int
                or receipt["schema"] != 1
                or receipt["network"] != self.config.network
                or receipt["image"] != self.config.image
                or receipt["metadata_blocked"] is not True
                or receipt["control_blocked"] is not True
                or receipt["boot_id"] != self.boot_id_path.read_text().strip()
                or type(verified) is not int
                or not 0
                <= now - verified
                <= self.config.network_preflight_max_age_seconds
                or not HASH.fullmatch(receipt["network_id"])
            ):
                return False
            (network,) = json.loads(
                self._command(["network", "inspect", self.config.network])
            )
            return (
                network["Id"] == receipt["network_id"] and network["Driver"] == "bridge"
            )
        except (OSError, ValueError, TypeError, KeyError, DockerError):
            return False

    @staticmethod
    def _name(name):
        if not NAME.fullmatch(name):
            raise ValueError("Invalid worker container name")
        return name

    def names(self, instance):
        output = self._command(
            [
                "ps",
                "-a",
                "--filter",
                f"label={INSTANCE}={instance}",
                "--format",
                "{{.Names}}",
            ]
        )
        return {
            name for name in output.decode("ascii").splitlines() if NAME.fullmatch(name)
        }

    def inspect(self, name):
        raw = self._command(
            ["container", "inspect", self._name(name)], allow_missing=True
        )
        if raw is None:
            return None
        try:
            (data,) = json.loads(raw)
            state = data["State"]
            if not HASH.fullmatch(data["Id"]) or type(state["ExitCode"]) is not int:
                raise ValueError()
            return Container(
                data["Name"].removeprefix("/"),
                data["Config"]["Labels"] or {},
                state["Status"],
                state["ExitCode"],
                state["OOMKilled"],
                data["Id"],
            )
        except (KeyError, ValueError, TypeError, AttributeError):
            raise DockerError() from None

    def create_args(self, task, workspaces, labels):
        c = self.config
        name = self._name(task["container"])
        job = workspaces.job(task)
        entry = c.tasks[task["task"]]
        mounts = [
            (job / "source", "/work/source", False),
            (job / "bolt", "/work/bolt", False),
            (job / "reports", "/work/reports", False),
            (job / "cache", "/work/cache", False),
            (job / "native-out", "/work/artifacts", False),
            (c.helper_dir, "/work/helpers", True),
            (c.driver_dir, "/work/driver", True),
            (c.jdk_dirs[entry["jdk"]], "/opt/job-jdk", True),
        ]
        env = {
            "HOME": "/home/code",
            "PATH": WORKER_PATH,
            "JAVA_HOME": "/opt/job-jdk",
            "CI_NUM_THREADS": str(c.cpus),
            "GLUTEN_MAVEN_REPO": "/work/cache/maven",
            "CONAN_HOME": "/work/cache/conan",
            "GLUTEN_CACHE_ROOT": "/work/cache/spark",
            "PIP_CACHE_DIR": "/work/cache/pip",
            "CCACHE_DIR": "/work/cache/ccache",
            "GLUTEN_CI_DRIVER_SHA": c.driver_sha,
            "GLUTEN_CI_HELPER_SHA": c.helper_sha,
            "GLUTEN_CI_CONFIG_SHA256": c.fingerprint,
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": "safe.directory",
            "GIT_CONFIG_VALUE_0": "/work/driver",
        }
        if c.arrow_cache_script:
            mounts.extend(
                [
                    (c.arrow_cache_script, "/opt/bolt-arrow-cache.py", True),
                    (c.arrow_source_dir, "/opt/bolt-arrow-sources", True),
                ]
            )
            env["BOLT_CI_ARROW_CACHE_SCRIPT"] = "/opt/bolt-arrow-cache.py"
        if c.conan_remote:
            env["BOLT_CI_CONAN_REMOTE"] = c.conan_remote
        if c.worker_tools_dir:
            mounts.append((c.worker_tools_dir, "/opt/worker-tools", True))
            env["PATH"] = (
                "/opt/worker-tools/usr/bin:/opt/worker-tools/bin:" + env["PATH"]
            )
            env["LD_LIBRARY_PATH"] = (
                "/opt/worker-tools/usr/lib/x86_64-linux-gnu:/opt/worker-tools/lib/x86_64-linux-gnu"
            )
        args = [
            "create",
            "--name",
            name,
            "--platform=linux/amd64",
            "--pull=never",
            "--init",
            "--restart=no",
            f"--network={c.network}",
            "--user=1000:1000",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--sysctl=net.ipv6.conf.all.disable_ipv6=1",
            f"--cpus={c.cpus}",
            f"--memory={c.memory_gib}g",
            f"--memory-swap={c.memory_gib}g",
            "--pids-limit=4096",
            "--shm-size=1g",
            "--workdir=/work/source",
            "--log-driver=local",
            "--log-opt=max-size=25m",
            "--log-opt=max-file=2",
            "--entrypoint=/opt/venv/bin/python3",
        ]
        if c.conan_extra_host:
            args.extend(["--add-host", c.conan_extra_host])
        for key, value in labels.items():
            args.extend(["--label", f"{key}={value}"])
        for key, value in env.items():
            args.extend(["--env", f"{key}={value}"])
        for source, target, readonly in mounts:
            mode = ",readonly" if readonly else ""
            args.extend(["--mount", f"type=bind,source={source},target={target}{mode}"])
        args.extend(
            [
                c.image,
                HELPER_PATH,
                "--task",
                entry["id"],
                "--source",
                "/work/source",
                "--bolt",
                "/work/bolt",
                "--driver",
                "/work/driver",
                "--reports",
                "/work/reports",
                "--artifacts",
                "/work/artifacts",
                "--compile-only",
            ]
        )
        return args

    def create(self, task, workspaces, labels):
        self._command(self.create_args(task, workspaces, labels))

    def start(self, name):
        self._command(["start", self._name(name)])

    def stop(self, name):
        self._command(["stop", "--time", "5", self._name(name)], allow_missing=True)

    def logs(self, name, destination):
        with destination.open("wb") as output:
            self._command(["logs", self._name(name)], output=output)

    def remove(self, name):
        self._command(["rm", self._name(name)], allow_missing=True)
