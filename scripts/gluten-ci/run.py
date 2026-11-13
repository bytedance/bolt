# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run one Bolt CI task against explicit checkouts and collect its evidence."""

import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
from xml.etree import ElementTree
from zipfile import BadZipFile, ZipFile

HERE = Path(__file__).resolve().parent
DEFAULT_DRIVER_SHA = "72f74589df524b2f4a7695b4f10c5b13a95812e8"
NATIVE_PACKAGE = PurePosixPath("cpp/build/package/bolt/linux/amd64")
NATIVE_RUNTIME = PurePosixPath("bolt-runtime")
UDF_EXAMPLES = PurePosixPath("cpp/build/Release/bolt/udf/examples")
PACKAGE_LIBRARIES = ("libgluten.so", "libbolt_backend.so", "libglutenlibloader.so")
NATIVE_LIBRARIES = {
    *(NATIVE_PACKAGE / name for name in PACKAGE_LIBRARIES),
    UDF_EXAMPLES / "libmyudf.so",
    UDF_EXAMPLES / "libmyudaf.so",
}
NATIVE_ARCHIVE = "native.tar.gz"
NATIVE_MANIFEST = "native-manifest.json"
BACKEND_LIBRARY = NATIVE_PACKAGE / "libbolt_backend.so"
COMPILE_VERIFICATION = "compile-verification.json"
LOADER_SUITE = "org.apache.gluten.jni.BoltNativeLoaderSuite"
LIBRARY_NAME = re.compile(r"lib[A-Za-z0-9_.+-]+\.so(?:\.[A-Za-z0-9_.+-]+)?")
SUITE_NAME = re.compile(r"[A-Za-z_$][A-Za-z0-9_.$]*")
TPC_SUMMARY = re.compile(r"Summary: ([0-9]+) out of ([0-9]+) queries passed\.")
ZERO_COUNTS = {"total": 0, "passed": 0, "failed": 0, "skipped": 0}
MAX_XML_BYTES = 64 * 1024 * 1024
MAX_TPC_LOG_BYTES = 128 * 1024 * 1024


def load_matrix():
    entries = json.loads((HERE / "matrix.json").read_text(encoding="utf-8"))
    tasks = {entry["id"]: entry for entry in entries}
    if len(tasks) != len(entries):
        raise ValueError("Duplicate Bolt CI task ID")
    return tasks


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path, value):
    with tempfile.NamedTemporaryFile(
        dir=path.parent,
        prefix=f".{path.name}.",
        mode="w",
        encoding="utf-8",
        delete=False,
    ) as stream:
        temporary = Path(stream.name)
        json.dump(value, stream, indent=2)
        stream.write("\n")
    temporary.replace(path)


def git_sha(checkout):
    commit = subprocess.check_output(
        ["git", "-C", checkout, "rev-parse", "--verify", "HEAD^{commit}"],
        text=True,
    ).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError(f"Invalid checkout revision at {checkout}")
    return commit


def identity_for(args):
    expected_driver = os.environ.get("GLUTEN_CI_DRIVER_SHA", DEFAULT_DRIVER_SHA)
    if not re.fullmatch(r"[0-9a-f]{40}", expected_driver):
        raise ValueError("Invalid GLUTEN_CI_DRIVER_SHA")
    driver = git_sha(args.driver)
    if driver != expected_driver:
        raise ValueError("Bolt CI driver revision does not match the trusted pin")
    helper = os.environ.get("GLUTEN_CI_HELPER_SHA")
    if helper is None:
        bundle = hashlib.sha256()
        for path in (
            HERE / "run.py",
            HERE / "run-task.sh",
            HERE / "matrix.json",
            HERE / "ci-bolt-build.sh",
            HERE / "ci-bolt-spark-env.sh",
        ):
            bundle.update(bytes.fromhex(sha256(path)))
        helper = bundle.hexdigest()
    config = os.environ.get("GLUTEN_CI_CONFIG_SHA256")
    if config is None:
        config = hashlib.sha256(b"bolt-linux-amd64:clang-22:clang++-22").hexdigest()
    if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", helper):
        raise ValueError("Invalid GLUTEN_CI_HELPER_SHA")
    if not re.fullmatch(r"[0-9a-f]{64}", config):
        raise ValueError("Invalid GLUTEN_CI_CONFIG_SHA256")
    return {
        "source_sha": git_sha(args.source),
        "bolt_sha": git_sha(args.bolt),
        "driver_sha": driver,
        "helper_sha": helper,
        "config_sha256": config,
    }


def plain_file(path):
    return path.is_file() and not path.is_symlink()


def safe_native_path(name):
    path = PurePosixPath(name)
    if path.as_posix() != name:
        raise ValueError(f"Noncanonical native archive member: {name}")
    if path in NATIVE_LIBRARIES:
        return path
    if path.parent in (NATIVE_PACKAGE, NATIVE_RUNTIME) and LIBRARY_NAME.fullmatch(
        path.name
    ):
        return path
    raise ValueError(f"Unexpected native archive member: {name}")


def safe_parents(root, parts):
    path = root
    for part in parts:
        path /= part
        if path.is_symlink() or (path.exists() and not path.is_dir()):
            raise ValueError(f"Unsafe directory in checkout: {path}")
    return path


@contextmanager
def open_regular(root, path, maximum=None):
    """Open a bounded regular file below root without following symlinks."""
    relative = path.relative_to(root)
    if ".." in relative.parts:
        raise ValueError(f"Evidence path escapes its directory: {path}")
    directory_flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY
    directory = os.open(root, directory_flags)
    try:
        for part in relative.parts[:-1]:
            child = os.open(part, directory_flags, dir_fd=directory)
            os.close(directory)
            directory = child
        fd = os.open(
            relative.name,
            os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
            dir_fd=directory,
        )
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or (
                maximum is not None and info.st_size > maximum
            ):
                raise ValueError(f"Unexpected evidence file type or size: {path}")
            yield stream
    finally:
        os.close(directory)


def create_native_archive(
    artifacts, source, runtime, identity, *, compile_only=False, native_only=False
):
    mode = "compile" if compile_only else "native" if native_only else None
    safe_parents(source, NATIVE_PACKAGE.parts)
    safe_parents(source, UDF_EXAMPLES.parts)
    if runtime.is_symlink():
        raise ValueError("Unsafe native runtime directory")
    entries = {path: source / path for path in NATIVE_LIBRARIES}
    entries.update(
        {
            NATIVE_PACKAGE / file.name: file
            for file in (source / NATIVE_PACKAGE).glob("*.so*")
        }
    )
    entries.update({NATIVE_RUNTIME / file.name: file for file in runtime.iterdir()})
    for member, file in entries.items():
        safe_native_path(str(member))
        if not plain_file(file) or not file.stat().st_size:
            raise ValueError(f"Missing or unsafe native library: {file}")
    temporary = artifacts / f".{NATIVE_ARCHIVE}.tmp"
    try:
        with tarfile.open(temporary, "x:gz", compresslevel=1) as archive:
            for member, file in sorted(entries.items()):
                info = archive.gettarinfo(str(file), arcname=str(member))
                if not info.isfile():
                    raise ValueError(f"Nonregular native library: {file}")
                with file.open("rb") as stream:
                    archive.addfile(info, stream)
        digest = sha256(temporary)
        temporary.replace(artifacts / NATIVE_ARCHIVE)
        write_json(
            artifacts / NATIVE_MANIFEST,
            {
                "schema": 1,
                **identity,
                **({"mode": mode} if mode else {}),
                "archive_sha256": digest,
            },
        )
        return digest
    finally:
        temporary.unlink(missing_ok=True)


def verify_compile(source, reports, bolt_sha):
    with open_regular(source, source / BACKEND_LIBRARY) as library:
        size = os.fstat(library.fileno()).st_size
        if size == 0:
            raise ValueError("The compiled Bolt backend library is empty")
        backend = {
            "path": str(BACKEND_LIBRARY),
            "size": size,
            "sha256": hashlib.file_digest(library, "sha256").hexdigest(),
        }
    packages = []
    for name in ("bolt-export.json", "gluten-install.json", "bolt-package.json"):
        with open_regular(reports, reports / name, 64 * 1024 * 1024) as stream:
            value = json.load(stream)
        if name != "bolt-package.json":
            nodes = value["graph"]["nodes"].values()
            hosts = [
                n
                for n in nodes
                if n.get("name") == "bolt" and n.get("context") == "host"
            ]
            if len(hosts) != 1:
                raise ValueError("Expected one locally built Bolt package")
            value = hosts[0]
        packages.append({key: value[key] for key in ("ref", "package_id", "prev")})
    package = packages[0]
    if (
        any(package != other for other in packages[1:])
        or any(not isinstance(value, str) or not value for value in package.values())
        or package["ref"].partition("#")[0] != f"bolt/ci-{bolt_sha}"
    ):
        raise ValueError("Bolt package mismatch")
    return {
        "schema": 1,
        "bolt_sha": bolt_sha,
        "bolt_package": package,
        "backend": backend,
    }


def restore_native(artifacts, source, identity):
    manifest_path = artifacts / NATIVE_MANIFEST
    archive_path = artifacts / NATIVE_ARCHIVE
    if not plain_file(manifest_path) or not plain_file(archive_path):
        raise ValueError("Missing or unsafe native artifact")
    with open_regular(artifacts, manifest_path, 16384) as stream:
        manifest = json.loads(stream.read(16385))
    if (
        not isinstance(manifest, dict)
        or type(manifest.get("schema")) is not int
        or manifest["schema"] != 1
        or any(manifest.get(key) != value for key, value in identity.items())
    ):
        raise ValueError("Native artifact identity mismatch")
    if "mode" in manifest:
        raise ValueError("A compile-only/native-only artifact cannot supply full tests")

    with archive_path.open("rb") as compressed:
        digest = hashlib.file_digest(compressed, "sha256").hexdigest()
        if manifest.get("archive_sha256") != digest:
            raise ValueError("Native artifact digest mismatch")
        compressed.seek(0)
        with tarfile.open(fileobj=compressed, mode="r:gz") as archive:
            members = {}
            for member in archive.getmembers():
                path = safe_native_path(member.name)
                if (
                    not member.isreg()
                    or member.sparse is not None
                    or member.size <= 0
                    or path in members
                ):
                    raise ValueError(f"Invalid native archive member: {member.name}")
                destination = safe_parents(source, path.parts[:-1]) / path.name
                if destination.exists() or destination.is_symlink():
                    raise ValueError(
                        f"Native library already exists in checkout: {destination}"
                    )
                members[path] = member
            if not NATIVE_LIBRARIES.issubset(members):
                raise ValueError("Native archive is missing required libraries")
            for path, member in members.items():
                destination = source / path
                destination.parent.mkdir(parents=True, exist_ok=True)
                with (
                    archive.extractfile(member) as stream,
                    destination.open("xb") as output,
                ):
                    shutil.copyfileobj(stream, output)


def bind_bolt_checkout(source, bolt):
    # The source revision's package metadata locates Bolt at this path.
    path = safe_parents(source, ("ep", "build-bolt", "build")) / "bolt_ep"
    if path.exists() or path.is_symlink():
        if path.resolve() != bolt:
            raise ValueError(
                "The expected packaging path refers to a different Bolt checkout"
            )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.symlink_to(bolt, target_is_directory=True)


def verify_package(source, reports, bolt_sha, expected_libraries):
    jars = list((source / "package/target").glob("gluten-bolt-bundle-*.jar"))
    if len(jars) != 1:
        raise ValueError(f"Expected one packaged Gluten JAR: {jars}")
    with open_regular(source, jars[0]) as stream:
        with ZipFile(stream) as jar:
            for library, expected in expected_libraries.items():
                with jar.open(f"linux/amd64/{library}") as embedded:
                    actual = hashlib.file_digest(embedded, "sha256").hexdigest()
                if actual != expected:
                    raise ValueError(f"Packaged native library mismatch: {library}")
            with jar.open("gluten-build-info.properties") as info:
                metadata = info.read(65537)
            if (
                len(metadata) > 65536
                or f"bolt_revision={bolt_sha}" not in metadata.decode().splitlines()
            ):
                raise ValueError(
                    "Packaged Bolt revision does not match the native build"
                )
        stream.seek(0)
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
        stream.seek(0)
        with (reports / jars[0].name).open("xb") as output:
            shutil.copyfileobj(stream, output)
    with (reports / "package-verification.txt").open(
        "x", encoding="utf-8"
    ) as verification:
        verification.write(f"{digest}  {jars[0].name}\nbolt_revision={bolt_sha}\n")


def xml_counts(reports, report):
    with open_regular(reports, report, MAX_XML_BYTES) as stream:
        contents = stream.read(MAX_XML_BYTES + 1)
    if len(contents) > MAX_XML_BYTES:
        raise ValueError(f"JUnit report exceeds size limit: {report}")
    xml = contents.decode("utf-8")
    if "<!DOCTYPE" in xml or "<!ENTITY" in xml:
        raise ValueError(
            f"JUnit report contains an XML type or entity declaration: {report}"
        )
    root = ElementTree.fromstring(xml)
    if root.tag not in ("testsuites", "testsuite"):
        raise ValueError(f"Unexpected JUnit report root: {report}")
    counts = ZERO_COUNTS.copy()
    for case in root.iter("testcase"):
        counts["total"] += 1
        if case.find("failure") is not None or case.find("error") is not None:
            counts["failed"] += 1
        elif case.find("skipped") is not None:
            counts["skipped"] += 1
        else:
            counts["passed"] += 1
    return counts


def tpc_counts(reports, log):
    with open_regular(reports, log, MAX_TPC_LOG_BYTES) as stream:
        summaries = []
        bytes_read = 0
        while line := stream.readline(MAX_TPC_LOG_BYTES + 1 - bytes_read):
            bytes_read += len(line)
            if bytes_read > MAX_TPC_LOG_BYTES:
                raise ValueError(f"TPC log exceeds size limit: {log}")
            match = TPC_SUMMARY.fullmatch(
                line.decode("utf-8", errors="replace").strip()
            )
            if match:
                summaries.append(match.groups())
                if len(summaries) > 1:
                    break
    if len(summaries) != 1:
        raise ValueError(f"Expected one query summary: {log}")
    passed, total = map(int, summaries[0])
    if passed > total:
        raise ValueError(f"Invalid query counts: {log}")
    return {"total": total, "passed": passed, "failed": total - passed, "skipped": 0}


def jvm_report_groups(reports):
    maximum = 1024 * 1024
    with open_regular(
        reports, reports / "jvm/discovered-suites.txt", maximum
    ) as stream:
        data = stream.read(maximum + 1)
    if len(data) > maximum:
        raise ValueError("Discovered JVM suite list exceeds size limit")
    suites = data.decode("utf-8").splitlines()
    if any(not SUITE_NAME.fullmatch(suite) for suite in suites) or len(
        set(suites)
    ) != len(suites):
        raise ValueError("Invalid discovered JVM suites")
    delta = [
        suite for suite in suites if suite.startswith("org.apache.spark.sql.delta.")
    ]
    ordinary = [
        suite for suite in suites if suite != LOADER_SUITE and suite not in delta
    ]
    if not ordinary or LOADER_SUITE not in suites or not delta:
        raise ValueError("Missing backend, loader, or Delta suites")
    groups = {
        "backend": ordinary,
        **{suite: [suite] for suite in (LOADER_SUITE, *delta)},
    }
    return [
        [
            report
            for suite in selected
            for report in (reports / "jvm" / group / "backends-bolt/target").glob(
                f"*reports/TEST-{suite}.xml"
            )
        ]
        for group, selected in groups.items()
    ]


def summarize_tests(task, reports):
    stage = task["stage"]
    counts = ZERO_COUNTS.copy()
    parse = xml_counts
    try:
        if task["kind"] == "native":
            groups = [[reports / "TEST-native.xml"]]
        elif stage == "tpc":
            parse = tpc_counts
            groups = [
                [reports / f"tpc-{kind}/run.log"]
                for kind in task["selector"].split(",")
            ]
        elif stage == "jvm":
            groups = jvm_report_groups(reports)
        elif stage == "udf":
            groups = [
                list(
                    (reports / stage / "backends-bolt/target").glob(
                        f"*reports/TEST-{task['selector']}.xml"
                    )
                )
            ]
        else:
            module = f"spark{task['spark'].replace('.', '')}"
            groups = [
                list(
                    (reports / stage / "gluten-ut" / module / "target").glob(
                        "*reports/TEST-*.xml"
                    )
                )
            ]
    except (OSError, ValueError) as error:
        print(error, file=sys.stderr)
        return counts, False
    valid = bool(groups)
    for group in groups:
        executed = 0
        for report in group:
            try:
                for key, count in parse(reports, report).items():
                    counts[key] += count
                    if key in ("passed", "failed"):
                        executed += count
            except (OSError, ValueError, ElementTree.ParseError) as error:
                print(error, file=sys.stderr)
                valid = False
        valid = valid and executed > 0
    return counts, valid and counts["failed"] == 0


def evidence_paths(reports):
    return sorted(
        str(path.relative_to(reports))
        for path in reports.rglob("*")
        if path.suffix in (".log", ".txt", ".xml", ".json", ".patch", ".sha256", ".jar")
        and path != reports / "result.json"
        and plain_file(path)
    )


def execute(args, task):
    args.reports.mkdir(parents=True, exist_ok=True)
    result = {
        "schema": 1,
        "task": task["id"],
        "status": "failure",
        "exit_code": 1,
        "tests": ZERO_COUNTS.copy(),
        "evidence": [],
    }
    mode = "compile" if args.compile_only else "native" if args.native_only else "full"
    if mode != "full":
        result["mode"] = mode
    try:
        if any(args.reports.iterdir()):
            raise ValueError("The per-task reports directory must be empty")
        identity = identity_for(args)
        write_json(args.reports / "sources.json", identity)
        if task["kind"] == "native":
            args.artifacts.mkdir(parents=True, exist_ok=True)
            if any(
                (args.artifacts / name).exists()
                for name in (NATIVE_MANIFEST, NATIVE_ARCHIVE)
            ):
                raise ValueError(
                    "Native artifact directory already contains an artifact"
                )
        else:
            restore_native(args.artifacts, args.source, identity)
            bind_bolt_checkout(args.source, args.bolt)
        if task["kind"] == "package":
            expected_libraries = {
                name: sha256(args.source / NATIVE_PACKAGE / name)
                for name in PACKAGE_LIBRARIES
            }
        with tempfile.TemporaryDirectory(prefix="bolt-ci-") as directory:
            command = [
                "bash",
                str(HERE / "run-task.sh"),
                task["kind"],
                task["stage"],
                task["spark"] or "",
                str(task["jdk"]),
                ",".join(task["profiles"]),
                task.get("selector", ""),
                str(args.source),
                str(args.bolt),
                str(args.driver),
                str(args.reports),
                directory,
                mode,
            ]
            status = subprocess.run(command, cwd=args.source).returncode
            result["exit_code"] = 128 - status if status < 0 else status
            if task["kind"] != "package" and not args.compile_only:
                result["tests"], valid = summarize_tests(task, args.reports)
                if not valid and not result["exit_code"]:
                    result["exit_code"] = 1
            if not result["exit_code"] and task["kind"] == "native":
                if args.compile_only or args.native_only:
                    compile_result = verify_compile(
                        args.source, args.reports, identity["bolt_sha"]
                    )
                archive_sha = create_native_archive(
                    args.artifacts,
                    args.source,
                    Path(directory) / "runtime",
                    identity,
                    compile_only=args.compile_only,
                    native_only=args.native_only,
                )
                if args.compile_only or args.native_only:
                    write_json(
                        args.reports / COMPILE_VERIFICATION,
                        {**compile_result, "archive_sha256": archive_sha},
                    )
            elif not result["exit_code"] and task["kind"] == "package":
                verify_package(
                    args.source, args.reports, identity["bolt_sha"], expected_libraries
                )
        if not result["exit_code"]:
            result["status"] = "success"
    except (
        OSError,
        ValueError,
        AttributeError,
        TypeError,
        KeyError,
        BadZipFile,
        tarfile.TarError,
        subprocess.SubprocessError,
    ) as error:
        print(error, file=sys.stderr)
        result["exit_code"] = result["exit_code"] or 1
    result["evidence"] = evidence_paths(args.reports)
    write_json(args.reports / "result.json", result)
    return result["exit_code"]


def main():
    tasks = load_matrix()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", choices=tasks, required=True)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument(
        "--compile-only",
        action="store_true",
        help="Build native libraries without running tests",
    )
    modes.add_argument(
        "--native-only",
        action="store_true",
        help="Build native libraries and run native UT without Spark/JVM tasks",
    )
    for name in ("source", "bolt", "driver", "reports", "artifacts"):
        parser.add_argument(
            f"--{name}", type=lambda value: Path(value).resolve(), required=True
        )
    args = parser.parse_args()
    if args.compile_only and args.task != "native":
        parser.error("--compile-only requires --task native")
    if args.native_only and args.task != "native":
        parser.error("--native-only requires --task native")
    return execute(args, tasks[args.task])


if __name__ == "__main__":
    sys.exit(main())
