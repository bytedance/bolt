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

"""Reconcile durable worker claims and validate bounded report evidence."""

import hashlib
import json
import logging
import os
import re
import stat
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

from .config import COMPILE_MODE
from .docker import DockerError, INSTANCE
from .github import ApiError
from .policy import HASH
from .state import COUNTS

MAX_JSON = 4 * 1024 * 1024
MAX_GRAPH = 64 * 1024 * 1024
MAX_ARTIFACT = 16 * 1024**3
ZERO = dict.fromkeys(COUNTS, 0)
NATIVE = "native.tar.gz"
MANIFEST = "native-manifest.json"
BACKEND = "cpp/build/package/bolt/linux/amd64/libbolt_backend.so"
VERIFICATION = "compile-verification.json"
BUILD_LOG = "native-build.log"
LINKAGE = "runtime-linkage.txt"
PACKAGE = "bolt-package.json"
GRAPHS = ("bolt-export.json", "gluten-install.json")
PACKAGE_KEYS = ("ref", "package_id", "prev")
LABEL = "io.bolt.gluten-ci."


def relative_path(path):
    if not isinstance(path, str) or not path or len(path) > 4096 or "\x00" in path:
        raise ValueError("Invalid worker evidence path")
    rel = PurePosixPath(path)
    if (
        rel.is_absolute()
        or not rel.parts
        or rel.as_posix() != path
        or ".." in rel.parts
    ):
        raise ValueError("Invalid worker evidence path")
    return rel


@contextmanager
def regular(root, relative, maximum):
    """Open each directory component by descriptor, without following symlinks."""
    rel = relative_path(relative)
    if not root.is_absolute() or ".." in root.parts:
        raise ValueError("Invalid worker output directory")
    dirs = os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY
    directory = os.open("/", dirs)
    try:
        for part in (*root.parts[1:], *rel.parts[:-1]):
            child = os.open(part, dirs, dir_fd=directory)
            os.close(directory)
            directory = child
        fd = os.open(
            rel.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
        )
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or info.st_size > maximum
            ):
                raise ValueError("Invalid worker output file")
            yield stream
    finally:
        os.close(directory)


def contents(root, relative, maximum):
    with regular(root, relative, maximum) as stream:
        data = stream.read(maximum + 1)
    if len(data) > maximum:
        raise ValueError("Worker output exceeded size limit")
    return data


def json_report(root, name, maximum=MAX_JSON):
    data = json.loads(contents(root, name, maximum))
    if not isinstance(data, dict):
        raise ValueError("Invalid worker JSON")
    return data


def package_identity(record, bolt_sha):
    if not isinstance(record, dict):
        raise ValueError("Invalid Bolt package record")
    identity = {key: record.get(key) for key in PACKAGE_KEYS}
    if (
        any(
            not isinstance(value, str) or not value.strip()
            for value in identity.values()
        )
        or identity["ref"].partition("#")[0] != f"bolt/ci-{bolt_sha}"
    ):
        raise ValueError("Invalid selected Bolt package")
    return identity


def graph_package(reports, name, bolt_sha):
    graph = json_report(reports, name, MAX_GRAPH).get("graph")
    if not isinstance(graph, dict) or not isinstance(graph.get("nodes"), dict):
        raise ValueError("Invalid Conan graph")
    nodes = list(graph["nodes"].values())
    if any(not isinstance(node, dict) for node in nodes):
        raise ValueError("Invalid Conan graph node")
    hosts = [
        node
        for node in nodes
        if node.get("name") == "bolt" and node.get("context") == "host"
    ]
    if len(hosts) != 1:
        raise ValueError("Expected one Bolt host package")
    return package_identity(hosts[0], bolt_sha)


def verify_compile(reports, source, bolt_sha, paths):
    if (
        not {"sources.json", BUILD_LOG, LINKAGE, PACKAGE, VERIFICATION, *GRAPHS}
        <= paths
    ):
        raise ValueError("Missing required compilation evidence")
    with regular(reports, BUILD_LOG, MAX_ARTIFACT):
        pass
    linkage = contents(reports, LINKAGE, MAX_JSON).decode("utf-8")
    backend_links = re.search(
        rf"^/work/source/{re.escape(BACKEND)}:\n((?:[ \t].*\n?)*)",
        linkage,
        re.MULTILINE,
    )
    if (
        not backend_links
        or "=> not found" in linkage
        or not re.search(
            r"^[ \t]+\S+ => /\S+ \(0x[0-9a-f]+\)$",
            backend_links[1],
            re.MULTILINE,
        )
    ):
        raise ValueError("Invalid Bolt runtime linkage")

    report = json_report(reports, VERIFICATION)
    archive_hash = report.get("archive_sha256")
    backend = report.get("backend")
    if (
        type(report.get("schema")) is not int
        or report["schema"] != 1
        or report.get("bolt_sha") != bolt_sha
        or not isinstance(archive_hash, str)
        or not HASH.fullmatch(archive_hash)
        or not isinstance(backend, dict)
        or backend.get("path") != BACKEND
        or type(backend.get("size")) is not int
        or not 0 < backend["size"] <= MAX_ARTIFACT
        or not isinstance(backend.get("sha256"), str)
        or not HASH.fullmatch(backend["sha256"])
    ):
        raise ValueError("Invalid compilation verification")
    with regular(source, BACKEND, MAX_ARTIFACT) as library:
        if (
            os.fstat(library.fileno()).st_size != backend["size"]
            or hashlib.file_digest(library, "sha256").hexdigest() != backend["sha256"]
        ):
            raise ValueError("Compiled Bolt backend checksum mismatch")

    package = package_identity(json_report(reports, PACKAGE), bolt_sha)
    if package != report.get("bolt_package") or any(
        graph_package(reports, name, bolt_sha) != package for name in GRAPHS
    ):
        raise ValueError("Bolt package mismatch between export and consumption")
    return archive_hash


def collect_result(config, task, job, exit_code):
    reports = job / "reports"
    result = json_report(reports, "result.json")
    if json_report(reports, "sources.json") != config.identity(task["head_sha"]):
        raise ValueError("Worker report identity mismatch")
    counts = result.get("tests")
    evidence = result.get("evidence")
    if (
        type(result.get("schema")) is not int
        or result["schema"] != 1
        or result.get("task") != "native"
        or task["task"] != "native"
        or result.get("mode") != COMPILE_MODE
        or type(result.get("exit_code")) is not int
        or result["exit_code"] != exit_code
        or result.get("status") != ("failure" if exit_code else "success")
        or not isinstance(counts, dict)
        or counts != ZERO
        or any(type(value) is not int for value in counts.values())
        or not isinstance(evidence, list)
        or len(evidence) > 20000
    ):
        raise ValueError("Invalid compilation result")
    paths = {relative_path(path).as_posix() for path in evidence}
    if len(paths) != len(evidence):
        raise ValueError("Duplicate worker evidence")
    if exit_code:
        return "failure", "build", ZERO, None
    archive_hash = verify_compile(reports, job / "source", config.bolt_sha, paths)
    return "success", None, ZERO, archive_hash


def seal_native(config, task, source, destination, archive_hash):
    expected = config.identity(task["head_sha"]) | {
        "schema": 1,
        "mode": COMPILE_MODE,
        "archive_sha256": archive_hash,
    }
    manifest = json_report(source, MANIFEST)
    if type(manifest.get("schema")) is not int or manifest != expected:
        raise ValueError("Native compilation artifact identity mismatch")
    if destination.exists():
        with regular(destination, NATIVE, MAX_ARTIFACT) as archive:
            if (
                json_report(destination, MANIFEST) == manifest
                and os.fstat(archive.fileno()).st_size > 0
                and hashlib.file_digest(archive, "sha256").hexdigest() == archive_hash
            ):
                return
        raise ValueError("Previously sealed native artifact differs")
    with tempfile.TemporaryDirectory(dir=destination.parent, prefix=".native-") as temp:
        staging = Path(temp) / "artifact"
        staging.mkdir(mode=0o755)
        staging.chmod(0o755)
        checksum = hashlib.sha256()
        size = 0
        with (
            regular(source, NATIVE, MAX_ARTIFACT) as archive,
            (staging / NATIVE).open("xb") as output,
        ):
            while block := archive.read(1024 * 1024):
                size += len(block)
                if size > MAX_ARTIFACT:
                    raise ValueError("Native artifact exceeded size limit")
                checksum.update(block)
                output.write(block)
            output.flush()
            os.fsync(output.fileno())
        if size == 0 or checksum.hexdigest() != archive_hash:
            raise ValueError("Native artifact checksum mismatch")
        with (staging / MANIFEST).open("x") as output:
            json.dump(manifest, output)
            output.flush()
            os.fsync(output.fileno())
        for path in staging.iterdir():
            path.chmod(0o444)
        staging.rename(destination)
        directory = os.open(destination.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


class Runner:
    def __init__(self, config, state, api, docker, workspaces, *, clock=time.time):
        self.config = config
        self.state = state
        self.api = api
        self.docker = docker
        self.workspaces = workspaces
        self.clock = clock

    def _labels(self, task):
        return {
            INSTANCE: self.state.instance,
            LABEL + "request": str(task["request_id"]),
            LABEL + "ordinal": str(task["ordinal"]),
            LABEL + "source": task["head_sha"],
            LABEL + "bundle": task["bundle"],
        }

    def _owns(self, task, container):
        return (
            container.name == task["container"]
            and (task["docker_id"] is None or task["docker_id"] == container.id)
            and all(
                container.labels.get(key) == value
                for key, value in self._labels(task).items()
            )
        )

    def _verified(self, task):
        if not self.state.current_task(task):
            return False
        pr = self.api.pull(task["pr"])
        current = pr.open and pr.matches_execution(task)
        if not current:
            self.state.invalidate(task)
        return current and self.state.current_task(task)

    def _fail(self, task, now, category):
        self.state.finish(task, "failure", category, None, now)

    def _collect(self, task, container, now):
        name = task["container"]
        job = self.workspaces.job(task)
        self.docker.logs(name, job / "worker.log")
        if container.oom:
            self._fail(task, now, "infrastructure")
            return
        try:
            status, category, counts, archive_hash = collect_result(
                self.config, task, job, container.exit_code
            )
            if status == "success":
                seal_native(
                    self.config,
                    task,
                    job / "native-out",
                    self.workspaces.artifact(task),
                    archive_hash,
                )
        except (OSError, ValueError, RecursionError):
            self._fail(task, now, "evidence")
            return
        self.state.finish(task, status, category, counts, now)

    def _reconcile(self, task, now, safe):
        name = task["container"]
        container = self.docker.inspect(name)
        if not container or not self._owns(task, container):
            self._fail(task, now, task["stop_reason"] or "interrupted")
            return
        if task["docker_id"] is None and not self.state.remember_container(
            task, container.id
        ):
            self._fail(task, now, "interrupted")
            return
        task = task | {"docker_id": container.id}
        out_of_scope = (
            self.config.only_pr is not None and self.config.only_pr != task["pr"]
        )
        invalid = (
            task["request_state"] == "superseded"
            or not safe
            or out_of_scope
            or task["bundle"] != self.config.fingerprint
        )
        expired = now - task["claimed"] >= self.config.task_timeout_seconds
        stop = task["stop_reason"]
        if not stop and invalid:
            stop = "interrupted"
        if not stop and expired and container.status != "exited":
            stop = "timeout"
        if stop:
            self.state.mark_stopping(task, stop)
            if container.status not in {"created", "exited", "dead"}:
                self.docker.stop(name)
                remaining = self.docker.inspect(name)
                if remaining and remaining.status not in {"created", "exited", "dead"}:
                    return
            self._fail(task, now, stop)
            return
        if container.status == "created":
            if not self._verified(task):
                self._fail(task, now, "interrupted")
                return
            try:
                self.docker.start(name)
            except DockerError:
                # A lost start response is ambiguous, so inspect before failing.
                after = self.docker.inspect(name)
                if (
                    not after
                    or not self._owns(task, after)
                    or after.status == "created"
                ):
                    self._fail(task, now, "infrastructure")
                    return
                container = after
            else:
                container = self.docker.inspect(name)
            if not container or not self._owns(task, container):
                self._fail(task, now, "interrupted")
                return
        if container.status == "running":
            self.state.mark_started(task, now)
        elif container.status == "exited":
            if self._verified(task):
                self.state.mark_started(task, now)
                self._collect(task, container, now)
            else:
                self._fail(task, now, "interrupted")
        else:
            if container.status not in {"created", "dead"}:
                self.docker.stop(name)
            self._fail(task, now, "interrupted")

    def _launch(self, task, now):
        if task["bundle"] != self.config.fingerprint:
            self._fail(task, now, "interrupted")
            return
        try:
            if not self._verified(task):
                self._fail(task, now, "interrupted")
                return
            self.workspaces.stage(task)
        except (ApiError, OSError, ValueError, RuntimeError):
            self._fail(task, int(self.clock()), "infrastructure")
            return
        if not self.docker.verify_network():
            self._fail(task, int(self.clock()), "interrupted")
            return
        try:
            self.docker.create(task, self.workspaces, self._labels(task))
        except DockerError:
            # The durable deterministic name survives a lost create response.
            pass
        self._reconcile(task, int(self.clock()), self.docker.verify_network())

    def _cleanup(self, names):
        for task in self.state.cleanup_ready(names):
            name = task["container"]
            container = self.docker.inspect(name) if name in names else None
            if container and self._owns(task, container):
                if container.status not in {"created", "exited", "dead"}:
                    self.docker.stop(name)
                    continue
                job = self.workspaces.job(task)
                if not (job / "worker.log").exists():
                    self.docker.logs(name, job / "worker.log")
                self.docker.remove(name)
            self.workspaces.clean(task)
            self.state.cleaned(task)

    def tick(self):
        with self.state.lock("scheduler") as locked:
            if not locked:
                return
            config = self.config
            safe = self.docker.verify_network()
            for task in self.state.active():
                try:
                    self._reconcile(task, int(self.clock()), safe)
                except (ApiError, DockerError) as error:
                    logging.warning("Worker reconciliation deferred (%s)", error)
            self._cleanup(self.docker.names(self.state.instance))
            now = int(self.clock())
            self.state.expire(now, config.queue_timeout_seconds, config.only_pr)
            if not safe:
                logging.warning(
                    "Worker scheduling paused pending verified network isolation"
                )
                return
            for _ in range(config.max_parallel):
                if not self.docker.verify_network():
                    break
                now = int(self.clock())
                task = self.state.claim(
                    now,
                    config.queue_timeout_seconds,
                    config.max_parallel,
                    config.only_pr,
                )
                if not task:
                    break
                self._launch(task, now)
