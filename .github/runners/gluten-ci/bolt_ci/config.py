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

"""Deployment contract and trusted worker bundle identity."""

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from .policy import LOGIN, repository, valid_sha

HELPERS = (
    "scripts/gluten-ci/run.py",
    "scripts/gluten-ci/run-task.sh",
    "scripts/gluten-ci/matrix.json",
    "scripts/gluten-ci/ci-bolt-build.sh",
    "scripts/gluten-ci/ci-bolt-spark-env.sh",
)
CACHE_KEYS = {"maven", "conan", "spark", "pip", "ccache"}
NETWORK = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}")
IMAGE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}")
COMPILE_MODE = "compile"
# Exact matrix reviewed with the Bolt helper implementation. The digest binds
# every ID, profile and selector, including Spark's Scala 2.13 variant.
MATRIX_SHA256 = "59bca224f0953f39abdfdaeb38d34ab511fff89fd05aca0473bb7a87e67cfd59"
JDKS = {8, 11, 17, 21, 25}


def absolute(value):
    if not isinstance(value, str) or not re.fullmatch(r"/[A-Za-z0-9_./+ -]+", value):
        raise ValueError("Invalid deployment path")
    path = Path(value)
    if path.as_posix() != value or ".." in path.parts or path.resolve() != path:
        raise ValueError("Deployment path must be absolute and canonical")
    return path


def digest_file(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def bundle_digest(directory):
    bundle = hashlib.sha256()
    for asset in HELPERS:
        path = directory / asset
        if path.resolve() != path or not path.is_file():
            raise ValueError("Trusted worker helper is missing or linked")
        bundle.update(bytes.fromhex(digest_file(path)))
    return bundle.hexdigest()


def load_matrix(directory):
    with (directory / "scripts/gluten-ci/matrix.json").open("rb") as stream:
        raw = stream.read(65537)
    if len(raw) > 65536 or hashlib.sha256(raw).hexdigest() != MATRIX_SHA256:
        raise ValueError("Worker matrix does not match the reviewed Gluten matrix")
    matrix = json.loads(raw)
    native = [entry for entry in matrix if entry["kind"] == "native"]
    if (
        len(native) != 1
        or matrix[0] != native[0]
        or native[0]["id"] != "native"
        or native[0]["stage"] != "native"
        or native[0]["profiles"]
        or type(native[0]["jdk"]) is not int
        or native[0]["jdk"] not in JDKS
    ):
        raise ValueError("Invalid native worker task")
    return matrix


@dataclass(frozen=True)
class Config:
    repository: str
    host_data_dir: Path
    state_dir: Path
    helper_dir: Path
    bolt_checkout: Path
    driver_dir: Path
    jdk_dirs: dict
    image: str
    bolt_sha: str
    driver_sha: str
    helper_sha: str
    network: str
    network_preflight_file: Path
    read_token_file: Path
    bot_token_file: Path
    matrix: list
    fingerprint: str
    maintainer_logins: tuple = ()
    only_pr: int | None = None
    poll_seconds: int = 60
    runner_poll_seconds: int = 5
    commenter_poll_seconds: int = 10
    max_parallel: int = 4
    queue_timeout_seconds: int = 1800
    task_timeout_seconds: int = 21600
    cpus: int = 16
    memory_gib: int = 50
    network_preflight_max_age_seconds: int = 300
    cache_seed_dirs: dict | None = None
    arrow_source_dir: Path | None = None
    arrow_cache_script: Path | None = None
    conan_remote: str = ""
    worker_tools_dir: Path | None = None
    conan_extra_host: str = ""
    keep_workdirs: bool = False

    @classmethod
    def load(cls, path):
        with Path(path).open("rb") as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError("Configuration exceeds size limit")
        return cls.from_mapping(json.loads(raw))

    @classmethod
    def from_mapping(cls, raw):
        options = dict(raw)
        if options.pop("status_publishing", False) is not False:
            raise ValueError(
                "Commit status publishing requires a separate upstream policy"
            )
        for key in (
            "host_data_dir",
            "state_dir",
            "helper_dir",
            "bolt_checkout",
            "driver_dir",
            "network_preflight_file",
            "read_token_file",
            "bot_token_file",
        ):
            options[key] = absolute(options[key])
        for key in ("arrow_cache_script", "arrow_source_dir", "worker_tools_dir"):
            if options.get(key):
                options[key] = absolute(options[key])
            else:
                options.pop(key, None)
        if bool(options.get("arrow_cache_script")) != bool(
            options.get("arrow_source_dir")
        ):
            raise ValueError("Arrow script and seed must be configured together")
        if not repository(options["repository"]):
            raise ValueError("Invalid watched repository")
        if not valid_sha(options["bolt_sha"]) or not valid_sha(options["driver_sha"]):
            raise ValueError("Invalid engine or driver pin")
        network = options["network"]
        if not NETWORK.fullmatch(network) or network in {"none", "host", "bridge"}:
            raise ValueError("Expected a dedicated Docker network")
        if not IMAGE.fullmatch(options["image"]):
            raise ValueError("Expected an immutable worker image digest")
        jdk = options["jdk_dirs"]
        if set(jdk) != {str(number) for number in JDKS}:
            raise ValueError("Expected all five prepared JDKs")
        options["jdk_dirs"] = {
            int(number): absolute(path) for number, path in jdk.items()
        }
        seeds = options.get("cache_seed_dirs") or {}
        if not set(seeds) <= CACHE_KEYS:
            raise ValueError("Invalid cache seed")
        options["cache_seed_dirs"] = {
            key: absolute(path) for key, path in seeds.items()
        }
        maintainers = options.get("maintainer_logins", [])
        if not isinstance(maintainers, list) or any(
            not LOGIN.fullmatch(login) for login in maintainers
        ):
            raise ValueError("Invalid maintainer allowlist")
        options["maintainer_logins"] = tuple(login.lower() for login in maintainers)
        only_pr = options.get("only_pr")
        if only_pr is not None and (
            type(only_pr) is not int or not 0 < only_pr < 2**63
        ):
            raise ValueError("Invalid validation PR")
        bounds = {
            "poll_seconds": (1, 3600),
            "runner_poll_seconds": (1, 60),
            "commenter_poll_seconds": (1, 3600),
            "max_parallel": (1, 128),
            "queue_timeout_seconds": (1, 86400),
            "task_timeout_seconds": (1, 86400),
            "cpus": (1, 256),
            "memory_gib": (1, 2048),
            "network_preflight_max_age_seconds": (1, 600),
        }
        for key, (minimum, maximum) in bounds.items():
            if key in options and (
                type(options[key]) is not int or not minimum <= options[key] <= maximum
            ):
                raise ValueError("Invalid scheduling limit")
        if type(options.get("keep_workdirs", False)) is not bool:
            raise ValueError("Invalid workspace retention policy")
        remote = options.get("conan_remote", "")
        if remote:
            url = urlsplit(remote)
            if (
                url.scheme not in {"https", "http"}
                or not url.hostname
                or url.username
                or url.password
                or url.query
                or url.fragment
                or not url.port
                or not re.fullmatch(
                    r"https?://[a-zA-Z0-9.-]+:[0-9]+(?:/[a-zA-Z0-9_./-]*)?", remote
                )
            ):
                raise ValueError(
                    "Conan remote must be an explicit credential-free endpoint"
                )
        extra = options.get("conan_extra_host", "")
        if extra and (
            extra != "host.docker.internal:host-gateway"
            or urlsplit(remote).hostname != "host.docker.internal"
        ):
            raise ValueError("Invalid Conan cache alias")
        for key in ("matrix", "fingerprint"):
            if key in options:
                raise ValueError("Computed configuration key cannot be overridden")
        helper = options["helper_dir"]
        contents = bundle_digest(helper)
        pin = options["helper_sha"]
        if not valid_sha(pin) and pin != contents:
            raise ValueError("Worker helper does not match the trusted pin")
        options["matrix"] = load_matrix(helper)[:1]
        fingerprint = {
            key: raw.get(key)
            for key in (
                "image",
                "bolt_sha",
                "driver_sha",
                "helper_sha",
                "network",
                "worker_tools_dir",
                "arrow_source_dir",
                "conan_remote",
                "cache_seed_dirs",
                "jdk_dirs",
                "arrow_cache_script",
                "conan_extra_host",
            )
        }
        fingerprint["contents"] = contents
        fingerprint["mode"] = COMPILE_MODE
        fingerprint["graph"] = options["matrix"]
        if options.get("arrow_cache_script"):
            fingerprint["arrow_digest"] = digest_file(options["arrow_cache_script"])
        options["fingerprint"] = hashlib.sha256(
            json.dumps(fingerprint, sort_keys=True).encode()
        ).hexdigest()
        config = cls(**options)
        config.check_paths()
        return config

    def check_paths(self):
        writable = [self.jobs_dir, self.state_dir]
        sensitive = [
            self.state_dir,
            self.read_token_file,
            self.bot_token_file,
            self.jobs_dir,
            self.network_preflight_file,
        ]
        trusted = [
            self.helper_dir,
            self.bolt_checkout,
            self.driver_dir,
            *self.jdk_dirs.values(),
            *self.cache_seed_dirs.values(),
            self.arrow_cache_script,
            self.arrow_source_dir,
            self.worker_tools_dir,
        ]

        def overlap(first, second):
            return first.is_relative_to(second) or second.is_relative_to(first)

        secrets = (self.read_token_file, self.bot_token_file)
        if (
            overlap(*writable)
            or self.read_token_file == self.bot_token_file
            or any(
                overlap(directory, secret)
                for directory in writable
                for secret in secrets
            )
        ):
            raise ValueError("Controller and worker storage overlap")
        for asset in filter(None, trusted):
            if any(overlap(asset, private) for private in sensitive):
                raise ValueError("Worker asset overlaps a private controller path")

    @property
    def jobs_dir(self):
        return self.host_data_dir / "gluten-external-ci/jobs"

    @property
    def tasks(self):
        return {entry["id"]: entry for entry in self.matrix}

    def identity(self, head_sha):
        return dict(
            source_sha=head_sha,
            bolt_sha=self.bolt_sha,
            driver_sha=self.driver_sha,
            helper_sha=self.helper_sha,
            config_sha256=self.fingerprint,
        )

    def read_token(self, writer=False, optional=False):
        path = self.bot_token_file if writer else self.read_token_file
        if optional and not path.is_file():
            return ""
        with path.open("rb") as stream:
            if path.stat().st_mode & 0o077:
                raise ValueError("GitHub credential file must be owner-readable only")
            value = stream.read(4097).decode("ascii").strip()
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,4096}", value):
            raise ValueError("Invalid GitHub credential file")
        return value
