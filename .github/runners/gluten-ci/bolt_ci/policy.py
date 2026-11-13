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

"""Trusted PR path and command policy."""

import re
from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import PurePosixPath

RULE_VERSION = 1
SHA = re.compile(r"[0-9a-f]{40}")
HASH = re.compile(r"[0-9a-f]{64}")
LOGIN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})")
REPOSITORY = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]{0,38}/[A-Za-z0-9_.-]{1,100}")
RELEVANT = (
    "backends-bolt/*",
    "cpp/bolt/*",
    "cpp/bolt.CMakeLists.cmake",
    "ep/build-bolt/*",
    "gluten-core/*",
    "gluten-substrait/*",
    "gluten-arrow/*",
    "gluten-ui/*",
    "gluten-ut/*",
    "shims/*",
    "package/*",
    "gluten-celeborn/*",
    "gluten-uniffle/*",
    "gluten-delta/*",
    "gluten-iceberg/*",
    "gluten-paimon/*",
    "gluten-hudi/*",
    "gluten-kafka/*",
    "cpp/core/*",
    "cpp/CMake/*",
    "cpp/CMakeLists.txt",
    "cpp/conanfile.py",
    "pom.xml",
    "build/*",
    ".mvn/*",
    ".gitmodules",
    "dev/build_bolt_arrow.sh",
    "dev/run-scala-test.sh",
    "dev/package.sh",
    "dev/install-conan.sh",
    "dev/Dockerfile*bolt*",
    ".github/workflows/bolt*.yml",
    ".github/workflows/util/bolt-ci/*",
    ".github/actions/restore-bolt-native/*",
    ".github/workflows/util/install-spark-resources.sh",
    "tools/gluten-it/*",
)
UNRELATED = (
    "docs/*",
    "backends-clickhouse/*",
    "cpp-ch/*",
    "ep/build-clickhouse/*",
    "backends-velox/*",
    "cpp/velox/*",
    "ep/build-velox/*",
    "gluten-flink/*",
    "LICENSE",
    "NOTICE",
    "CONTRIBUTING.md",
    "SECURITY.md",
    ".github/workflows/clickhouse_be_trigger.yml",
    "dev/buildbundle-veloxbe.sh",
    "dev/builddeps-veloxbe.sh",
)


def repository(value):
    return (
        isinstance(value, str)
        and REPOSITORY.fullmatch(value)
        and value.split("/")[1] not in {".", ".."}
    )


def valid_sha(value):
    return isinstance(value, str) and SHA.fullmatch(value) is not None


def unrelated(path):
    if not isinstance(path, str) or not path or len(path) > 4096 or "\x00" in path:
        return False
    parsed = PurePosixPath(path)
    if parsed.is_absolute() or parsed.as_posix() != path or ".." in parsed.parts:
        return False
    if any(fnmatchcase(path, pattern) for pattern in RELEVANT):
        return False
    return ("/" not in path and path.startswith("README")) or any(
        fnmatchcase(path, pattern) for pattern in UNRELATED
    )


def affected(files, total):
    if (
        not isinstance(files, list)
        or not files
        or len(files) != total
        or len(files) >= 3000
    ):
        return True
    names = set()
    for entry in files:
        if not isinstance(entry, dict) or not unrelated(entry.get("filename")):
            return True
        name = entry["filename"]
        if name in names:
            return True
        names.add(name)
        if entry.get("status") == "renamed" or "previous_filename" in entry:
            if not unrelated(entry.get("previous_filename")):
                return True
    return False


def command_sha(body):
    if not isinstance(body, str):
        return None
    command = body.strip()
    if command in {"Run Gluten Bolt CI", "Run Gluten Bolt CI on x86"}:
        return ""
    match = re.fullmatch(r"/bolt ci(?: ([0-9a-f]{40}))?", command)
    return (match[1] or "") if match else None


@dataclass(frozen=True)
class Pull:
    number: int
    head_sha: str
    head_repo: str
    base_sha: str
    base_repo: str
    base_ref: str
    author: str
    changed: int | None
    updated: int
    open: bool

    @property
    def diff_identity(self):
        return (
            self.head_sha,
            self.head_repo,
            self.base_sha,
            self.base_repo,
            self.base_ref,
        )

    def matches_execution(self, row):
        # Workers build the PR head. Advancing the target tip only changes
        # the identity of a diff being classified.
        return bool(
            row
            and self.head_sha == row["head_sha"]
            and self.head_repo == row["head_repo"]
            and self.base_repo == row["base_repo"]
            and self.base_ref == row["base_ref"]
        )
