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

"""Strict, local and Actions entry points for the Gluten Bolt backend."""

import argparse
from collections import Counter
from contextlib import contextmanager, ExitStack
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
import zipfile


ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / ".github/gluten-ci-versions.json"
BLACKLIST = ROOT / ".github/gluten-ci-blacklist.txt"
PATCHES = ROOT / ".github/gluten-ci-patches"
SHA = re.compile(r"[0-9a-f]{40}")
IMAGE = re.compile(r"[a-zA-Z0-9._:/-]+@sha256:[0-9a-f]{64}")
DEFAULT_IMAGE = "bolt-registry:5000/bolt-ci:20260114"
DEFAULT_CONAN_IMAGE = "bolt-registry:5000/conan-server:latest"
STAGES = (
    "native",
    "jvm",
    "spark-1",
    "spark-2",
    "spark-3",
    "extended",
    "slow-hive",
    "tpc-h",
    "tpc-ds",
    "spark40",
    "spark41",
)
SPARK4_PROFILES = {"spark40": "4.0", "spark41": "4.1"}
SPARK4_SUITES = ",".join(
    ["org.apache.spark.sql.GlutenBoltBackendSmokeSuite"]
    + [
        f"org.apache.spark.sql.catalyst.expressions.Gluten{name}Suite"
        for name in (
            "ArithmeticExpression",
            "BitwiseExpressions",
            "ConditionalExpression",
            "LiteralExpression",
            "NullExpressions",
            "Predicate",
        )
    ]
)
SPARK_GROUPS = (
    "org.apache.spark.sql.streaming,org.apache.spark.GlutenSortShuffleSuite,org.apache.gluten",
    "org.apache.spark.sql.execution,org.apache.spark.sql.catalyst,org.apache.spark.sql.errors,org.apache.spark.sql.extension",
    "org.apache.spark.sql.GlutenSQL,org.apache.spark.sql.Gluten,org.apache.spark.sql.connector,org.apache.spark.sql.sources,org.apache.spark.sql.hive,org.apache.spark.sql.gluten,org.apache.spark.sql.shim",
)
EXCLUDED_TAGS = ",".join(
    f"org.apache.gluten.tags.{tag}"
    for tag in ("UDFTest", "EnhancedFeaturesTest", "CudfTest", "SkipTest")
)
BOLT_OPTIONS = {
    "spark_compatible": "True",
    "enable_testutil": "True",
    "shared": "False",
    "enable_hdfs": "False",
    "enable_s3": "False",
    "enable_asan": "False",
}


def write_atomic(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(content)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def write_json(path, value):
    write_atomic(path, (json.dumps(value, indent=2, sort_keys=True) + "\n").encode())


def git(path, *args):
    return subprocess.check_output(["git", "-C", str(path), *args], text=True).strip()


def run(args, cwd, log, env=None, json_output=None):
    """Keep failure evidence while preserving the subprocess exit status."""
    log.parent.mkdir(parents=True, exist_ok=True)
    command = shlex.join(str(arg) for arg in args)
    print(f"[{cwd}] {command}", flush=True)
    with log.open("a") as output:
        output.write(command + "\n")
        output.flush()
        if json_output:
            with json_output.open("w") as data:
                subprocess.run(
                    args, cwd=cwd, env=env, stdout=data, stderr=output, check=True
                )
        else:
            process = subprocess.Popen(
                args,
                cwd=cwd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            for line in process.stdout:
                output.write(line)
                print(line, end="", flush=True)
            process.stdout.close()
            if process.wait():
                raise subprocess.CalledProcessError(process.returncode, args)


def verify_reports(paths, blacklist=None):
    if not paths:
        raise ValueError("Required test XML reports are missing")
    tests = skipped = 0
    expected_failures = []
    for path in paths:
        suites = ET.parse(path).getroot().iter("testsuite")
        for suite in suites:
            if suite.findall("testsuite"):
                continue
            counts = {
                key: int(suite.get(key, "0"))
                for key in ("tests", "skipped", "failures", "errors")
            }
            if any(value < 0 for value in counts.values()):
                raise ValueError(f"Invalid test counts in {path}")
            cases = suite.findall("testcase")
            if len(cases) != counts["tests"]:
                raise ValueError(
                    f"XML test count does not match testcase discovery in {path}"
                )
            # ScalaTest emits <skipped/> for ignored/canceled tests without
            # adding a suite-level skipped attribute. Count the actual cases.
            counts["skipped"] = max(
                counts["skipped"],
                sum(case.find("skipped") is not None for case in cases),
            )
            if (
                any(value < 0 for value in counts.values())
                or counts["skipped"] > counts["tests"]
            ):
                raise ValueError(f"Invalid test counts in {path}")
            failed_cases = [case for case in cases if case.find("failure") is not None]
            if counts["errors"] or any(
                case.find("error") is not None for case in cases
            ):
                raise ValueError(f"Failed tests in {path}")
            if counts["failures"] > len(failed_cases):
                raise ValueError(f"Unidentified test failures in {path}")
            for case in failed_cases:
                key = (case.get("classname") or suite.get("name"), case.get("name"))
                if blacklist is None or key not in blacklist:
                    raise ValueError(f"Unlisted failed test in {path}: {key}")
                expected_failures.append("#".join(key))
            if counts["skipped"] + len(failed_cases) > counts["tests"]:
                raise ValueError(f"Invalid test counts in {path}")
            tests += counts["tests"]
            skipped += counts["skipped"]
    if tests <= skipped + len(expected_failures):
        raise ValueError(
            "No passing tests (empty discovery, all skipped or all expected failures)"
        )
    result = {"tests": tests, "skipped": skipped}
    if blacklist is not None:
        result.update(
            passed=tests - skipped - len(expected_failures),
            expected_failures=sorted(expected_failures),
        )
    return result


def verify_scalatest_run(log, paths):
    # With testFailureIgnore, ScalaTest's Maven plugin also swallows an
    # abnormal fork exit. Passing partial XML is insufficient without the
    # final runner summary and its executed-test counts.
    log = re.sub(r"\x1b\[[0-9;]*m", "", log)
    summary = re.findall(
        r"^Run completed in [^\n]+\nTotal number of tests run: (\d+)\n"
        r"Suites: completed (\d+), aborted (\d+)\n"
        r"Tests: succeeded (\d+), failed (\d+), canceled (\d+), ignored (\d+), pending (\d+)\s*$",
        log,
        re.MULTILINE,
    )
    if len(summary) != 1 or "*** RUN ABORTED ***" in log:
        raise ValueError("ScalaTest fork did not complete exactly one test run")
    total, suites, aborted, passed, failed, canceled, ignored, pending = map(
        int, summary[0]
    )
    if aborted or pending or not suites or not passed or total != passed + failed:
        raise ValueError("ScalaTest run aborted or has inconsistent completion counts")
    cases = []
    for path in paths:
        root = ET.parse(path).getroot()
        # Surefire writes its own versioned XML; it is already validated by
        # verify_reports and must not inflate ScalaTest's completion counts.
        if "surefire-test-report" in root.get(
            "{http://www.w3.org/2001/XMLSchema-instance}noNamespaceSchemaLocation", ""
        ):
            continue
        for suite in root.iter("testsuite"):
            cases.extend(suite.findall("testcase"))
    xml_skipped = sum(case.find("skipped") is not None for case in cases)
    xml_failed = sum(case.find("failure") is not None for case in cases)
    xml_passed = len(cases) - xml_skipped - xml_failed
    # ScalaTest can omit some ignored base-suite cases from XML; executed
    # passes/failures must still match exactly.
    if xml_passed != passed or xml_failed != failed or xml_skipped > canceled + ignored:
        raise ValueError("ScalaTest completion counts do not match its XML reports")


def bolt_node(graph):
    nodes = [
        node
        for node in graph["graph"]["nodes"].values()
        if node.get("name") == "bolt" and node.get("context") == "host"
    ]
    if len(nodes) != 1:
        raise ValueError("Expected exactly one host Bolt package in the Conan graph")
    node = nodes[0]
    for key in ("ref", "rrev", "package_id", "prev"):
        if not node.get(key):
            raise ValueError(f"Missing Bolt package identity: {key}")
    for key in ("spark_compatible", "enable_testutil"):
        if node["options"].get(key) != "True":
            raise ValueError(f"Bolt package must enable {key}")
    if (
        node["settings"].get("os") != "Linux"
        or node["settings"].get("arch") != "x86_64"
    ):
        raise ValueError("First-stage CI requires Linux x86_64")
    return node


def verify_package(exported, consumed):
    expected, actual = bolt_node(exported), bolt_node(consumed)
    if actual.get("binary") != "Cache":
        raise ValueError(
            "Gluten must consume the locally exported Bolt binary without building or downloading it"
        )
    for key in ("ref", "rrev", "package_id", "prev", "settings", "options"):
        if expected[key] != actual[key]:
            raise ValueError(f"Gluten consumed a different Bolt package: {key}")
    return {
        key: actual[key]
        for key in ("ref", "rrev", "package_id", "prev", "settings", "options")
    }


def verify_dependency_graph(expected, actual):
    """Check recipe/ABI identity in every context after dependency builds."""
    fields = ("ref", "context", "settings", "options", "package_id")

    def identity(node):
        result = {key: node.get(key) for key in fields}
        # package_info() can remove deprecated options after package lookup.
        # Recover only missing keys from the frozen binary configuration;
        # live option values still win, so actual configuration drift fails.
        frozen = (node.get("info") or {}).get("options") or {}
        result["options"] = {**frozen, **(node.get("options") or {})}
        return json.dumps(result, sort_keys=True)

    def identities(graph):
        if graph["graph"].get("error"):
            raise ValueError(
                f"Conan dependency graph has an error: {graph['graph']['error']}"
            )
        return Counter(
            identity(node)
            for node_id, node in graph["graph"]["nodes"].items()
            if node_id != "0"
        )

    before, after = identities(expected), identities(actual)
    if before != after:
        raise ValueError(
            "Conan dependency graph changed between preflight and installation"
        )
    return {
        "dependency_nodes": sum(after.values()),
        "dependency_graph_sha256": hashlib.sha256(
            json.dumps(sorted(after.elements())).encode()
        ).hexdigest(),
    }


def verify_tpc(log, expected):
    summaries = re.findall(r"Summary: (\d+) out of (\d+) queries passed\.", log)
    if summaries != [(str(len(expected)), str(len(expected)))]:
        raise ValueError("TPC comparison is missing, empty, incomplete or failed")
    blocks = re.split(r"Running test query (\w+) \(iteration 0\)\.\.\.", log)
    plans = {}
    for query, block in zip(blocks[1::2], blocks[2::2]):
        if query in plans or f"Successfully ran query {query}." not in block:
            raise ValueError(f"Duplicate or unsuccessful TPC query: {query}")
        physical = block.split("== Physical Plan ==", 1)
        if len(physical) != 2:
            raise ValueError(f"No executed physical plan for {query}")
        executed = physical[1].split("Successfully ran query", 1)[0]
        if "AdaptiveSparkPlan" in executed:
            if "isFinalPlan=true" not in executed or "== Final Plan ==" not in executed:
                raise ValueError(f"No final AQE execution plan for {query}")
            executed = executed.split("== Final Plan ==", 1)[1].split(
                "== Initial Plan ==", 1
            )[0]
        plans[query] = bool(re.search(r"\b\w+Transformer\b", executed))
    if set(plans) != expected:
        raise ValueError("TPC query discovery does not match the complete benchmark")
    native = sorted(query for query, used_native in plans.items() if used_native)
    if not native:
        raise ValueError("All TPC queries fell back to vanilla Spark")
    return {
        "native_queries": native,
        "fully_fallback_queries": sorted(expected - set(native)),
    }


def prepare(reports, local=False, gluten=None):
    if local and os.environ.get("GITHUB_ACTIONS") == "true":
        raise ValueError("Local mode cannot run in GitHub Actions")
    config = json.loads(CONFIG.read_text())
    matrix = {
        "spark": "3.5.5",
        "scala": "2.12",
        "jdk": "17",
        "os": "Linux",
        "architecture": "x86_64",
        "build_type": "Release",
    }
    if any(config.get(key) != value for key, value in matrix.items()):
        raise ValueError(
            "Unsupported Gluten CI matrix; update test commands and validation together"
        )
    repository = config.get("gluten_repository", "")
    branch = config.get("gluten_ref", "")
    if (repository, branch) != ("apache/gluten", "main"):
        raise ValueError("Gluten CI must use apache/gluten:main")
    image = None if local else os.environ.get("BOLT_GLUTEN_CI_IMAGE") or DEFAULT_IMAGE
    if not local and image != DEFAULT_IMAGE and not IMAGE.fullmatch(image):
        raise ValueError(
            "BOLT_GLUTEN_CI_IMAGE must use the Bolt CI image or an image@sha256:digest"
        )
    conan_image = (
        None
        if local
        else os.environ.get("BOLT_GLUTEN_CI_CONAN_IMAGE") or DEFAULT_CONAN_IMAGE
    )
    if (
        not local
        and conan_image != DEFAULT_CONAN_IMAGE
        and not IMAGE.fullmatch(conan_image)
    ):
        raise ValueError(
            "BOLT_GLUTEN_CI_CONAN_IMAGE overrides require an image@sha256:digest"
        )
    runner = (
        ["local"]
        if local
        else json.loads(
            os.environ.get("BOLT_GLUTEN_CI_RUNNER") or '["self-hosted", "medium"]'
        )
    )
    if (
        not isinstance(runner, list)
        or not runner
        or any(
            not isinstance(label, str) or not re.fullmatch(r"[\w.-]+", label)
            for label in runner
        )
    ):
        raise ValueError("BOLT_GLUTEN_CI_RUNNER must be a JSON array of runner labels")
    if local and gluten is not None:
        config["gluten_sha"] = git(gluten, "rev-parse", "HEAD")
    else:
        ref = "refs/heads/" + branch
        remote = subprocess.check_output(
            [
                "git",
                "ls-remote",
                "--exit-code",
                f"https://github.com/{repository}.git",
                ref,
            ],
            text=True,
        ).splitlines()
        if (
            len(remote) != 1
            or len(remote[0].split()) != 2
            or remote[0].split()[1] != ref
            or not SHA.fullmatch(remote[0].split()[0])
        ):
            raise ValueError(f"Cannot resolve Gluten branch {repository}:{branch}")
        config["gluten_sha"] = remote[0].split()[0]
    tested = os.environ.get("GITHUB_SHA") or git(ROOT, "rev-parse", "HEAD")
    if tested != git(ROOT, "rev-parse", "HEAD"):
        raise ValueError("Bolt checkout differs from the candidate merge SHA")
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    event = json.loads(Path(event_path).read_text()) if event_path else {}
    provenance = {
        **config,
        "bolt_repository": os.environ.get("GITHUB_REPOSITORY", "bytedance/bolt"),
        "bolt_ref": os.environ.get("GITHUB_REF", "local"),
        "bolt_tested_sha": tested,
        "bolt_pr_head_sha": event.get("pull_request", {}).get("head", {}).get("sha"),
        "merge_group_head_sha": event.get("merge_group", {}).get("head_sha"),
        "event": os.environ.get("GITHUB_EVENT_NAME", "local"),
        "image": image,
        "conan_image": conan_image,
        "execution_environment": "local-host" if local else "container",
        "runner": runner,
        "required_stages": STAGES,
        "configuration_sha256": hashlib.sha256(CONFIG.read_bytes()).hexdigest(),
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "discovery_script_sha256": hashlib.sha256(
            (ROOT / ".github/scripts/DiscoverGlutenSuites.java").read_bytes()
        ).hexdigest(),
        "blacklist_sha256": hashlib.sha256(BLACKLIST.read_bytes()).hexdigest(),
        "gluten_patch_sha256": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(PATCHES.glob("*.patch"))
        },
    }
    write_json(reports / "inputs.json", provenance)
    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a") as stream:
            for key, value in {
                "gluten_repository": repository,
                "gluten_sha": config["gluten_sha"],
                "image": image,
                "conan_image": conan_image,
                "runner": json.dumps(runner),
            }.items():
                stream.write(f"{key}={value}\n")


def gluten_patch_tree(gluten, inputs):
    """Derive the only accepted source tree from main and the declared patches."""
    if git(gluten, "rev-parse", "HEAD") != inputs["gluten_sha"]:
        raise ValueError("Gluten checkout differs from the resolved Apache main")
    patches = sorted(PATCHES.glob("*.patch"))
    digests = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in patches
    }
    if digests != inputs["gluten_patch_sha256"]:
        raise ValueError("Patch bundle differs from the prepared CI inputs")
    records = {}
    with tempfile.TemporaryDirectory(prefix="gluten-patch-index-") as directory:
        env = {**os.environ, "GIT_INDEX_FILE": str(Path(directory) / "index")}
        command = ["git", "-C", str(gluten)]
        subprocess.run([*command, "read-tree", "HEAD"], env=env, check=True)
        for path in patches:
            apply = [*command, "apply", "--cached"]
            check = subprocess.run(
                [*apply, "--check", str(path)], env=env, capture_output=True
            )
            if check.returncode == 0:
                subprocess.run([*apply, str(path)], env=env, check=True)
                status = "applied"
            elif (
                subprocess.run(
                    [*apply, "--reverse", "--check", str(path)],
                    env=env,
                    capture_output=True,
                ).returncode
                == 0
            ):
                status = "already-upstream"
            else:
                raise ValueError(
                    f"Gluten patch neither applies nor is already upstream: {path.name}"
                )
            records[path.name] = {"status": status, "sha256": digests[path.name]}
        tree = subprocess.check_output(
            [*command, "write-tree"], env=env, text=True
        ).strip()
    return tree, records


def verify_gluten_source(gluten, inputs):
    tree, records = gluten_patch_tree(gluten, inputs)
    if git(gluten, "diff", tree, "--"):
        raise ValueError("Gluten source differs from Apache main plus declared patches")
    return tree, records


def apply_gluten_patches(gluten, reports):
    inputs = json.loads((reports / "inputs.json").read_text())
    tree, records = gluten_patch_tree(gluten, inputs)
    if git(gluten, "diff", tree, "--"):
        if git(gluten, "diff", "HEAD", "--"):
            raise ValueError(
                "Unrecognized Gluten source changes before patch application"
            )
        for name, record in records.items():
            if record["status"] == "applied":
                subprocess.run(
                    ["git", "-C", str(gluten), "apply", "--index", str(PATCHES / name)],
                    check=True,
                )
    verify_gluten_source(gluten, inputs)
    write_json(
        reports / "gluten-patches.json",
        {"base_sha": inputs["gluten_sha"], "source_tree": tree, "patches": records},
    )
    for name, record in records.items():
        print(f"Gluten compatibility patch {name}: {record['status']}", flush=True)


def build(gluten, reports):
    inputs = json.loads((reports / "inputs.json").read_text())
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise ValueError("Native CI requires Linux x86_64")
    if git(ROOT, "rev-parse", "HEAD") != inputs["bolt_tested_sha"] or git(
        ROOT, "diff", "HEAD", "--"
    ):
        raise ValueError(f"Checkout is not the exact unmodified candidate: {ROOT}")
    verify_gluten_source(gluten, inputs)
    index = (
        Path(os.environ.get("CONAN_HOME", str(Path.home() / ".conan2")))
        / "conan-center-index"
    )
    if index.is_dir():
        inputs["conan_recipe_index_sha"] = git(index, "rev-parse", "HEAD")
        write_json(reports / "inputs.json", inputs)
    log = reports / "build.log"
    for command in (
        ["java", "-version"],
        ["c++", "--version"],
        ["conan", "--version"],
        ["cmake", "--version"],
        ["conan", "profile", "show"],
    ):
        run(command, ROOT, log)
    java = subprocess.run(
        ["java", "-version"], capture_output=True, text=True, check=True
    )
    if not re.search(r'version "17[.\"]', java.stderr + java.stdout):
        raise ValueError("This matrix requires JDK 17")
    version = "ci-" + inputs["bolt_tested_sha"]
    options = [
        arg
        for key, value in BOLT_OPTIONS.items()
        for arg in ("-o", f"bolt/*:{key}={value}")
    ]
    overrides = " ".join(options)
    # Some Bolt revisions have no repository overlay. Configure and package
    # with the same host profiles so Conan resolves the same dependency graph.
    profile = ["-pr:h", "default"]
    overlay = ROOT / "scripts/conan/bolt.profile"
    if overlay.is_file():
        profile.extend(["-pr:h", str(overlay)])
    run(
        [
            "make",
            "release_spark_with_test",
            "BOLT_CONAN_CONFIGURE_ONLY=1",
            f"BUILD_VERSION={version}",
            f"CONAN_OVERRIDE={overrides}",
            f"CONAN_HOST_PROFILE_ARGS={shlex.join(profile)}",
        ],
        ROOT,
        log,
    )
    run(
        [
            "cmake",
            "--build",
            "--preset",
            "conan-release",
            "--target",
            "bolt_engine",
            "bolt_testutils_static",
            "--parallel",
            os.environ.get("CI_NUM_THREADS", "16"),
        ],
        ROOT,
        log,
    )
    profile.extend(["-s", "build_type=Release"])
    run(
        [
            "conan",
            "export-pkg",
            ".",
            "--name=bolt",
            f"--version={version}",
            *profile,
            *options,
            "-c",
            "bolt/*:tools.build:skip_test=False",
            "--format=json",
        ],
        ROOT,
        log,
        json_output=reports / "bolt-export.json",
    )
    env = {**os.environ, "BOLT_BUILD_VERSION": version}
    gluten_options = [
        arg
        for key, value in {
            "shared": "True",
            "build_tests": "True",
            "enable_hdfs": "False",
            "enable_s3": "False",
            "enable_asan": "False",
        }.items()
        for arg in ("-o", f"gluten/*:{key}={value}")
    ]
    common = [
        ".",
        "--name=gluten",
        "--version=ci-" + inputs["gluten_sha"],
        *profile,
        *options,
        *gluten_options,
        "-c",
        "arrow/*:tools.build:download_source=True",
    ]
    # Graph resolution executes no package builds. Check the local Bolt binary
    # before installation, then verify the entire installed recipe/ABI graph.
    # A flat Conan lock can replay ranges differently across build subgraphs
    # (e.g. zlib 1.2.13 for Thrift and 1.3.1 for other tools). Keep the graph
    # snapshot and use the same isolated recipe cache with updates disabled.
    # Conan's missing-build policy can override --build=~bolt/* exclusions.
    run(
        [
            "conan",
            "graph",
            "info",
            *common,
            "--build=missing",
            "--lockfile=",
            "--format=json",
        ],
        gluten / "cpp",
        log,
        env,
        reports / "gluten-preflight.json",
    )
    exported = json.loads((reports / "bolt-export.json").read_text())
    preflight = json.loads((reports / "gluten-preflight.json").read_text())
    verify_package(exported, preflight)
    run(
        [
            "conan",
            "install",
            *common,
            "--lockfile=",
            "--build=missing",
            "--build=~bolt/*",
            "--format=json",
        ],
        gluten / "cpp",
        log,
        env,
        reports / "gluten-install.json",
    )
    installed = json.loads((reports / "gluten-install.json").read_text())
    identity = verify_package(
        json.loads((reports / "bolt-export.json").read_text()),
        installed,
    )
    write_json(reports / "bolt-package.json", identity)
    write_json(
        reports / "conan-dependencies.json",
        verify_dependency_graph(preflight, installed),
    )
    arrows = [
        node
        for node in installed["graph"]["nodes"].values()
        if node.get("name") == "arrow" and node.get("context") == "host"
    ]
    if len(arrows) != 1 or not arrows[0]["ref"].startswith("arrow/15.0.1-oss#"):
        raise ValueError(
            "The Arrow Java helper requires the installed arrow/15.0.1-oss recipe"
        )
    arrow_ref = arrows[0]["ref"]
    source = (
        Path(
            subprocess.check_output(
                ["conan", "cache", "path", arrow_ref, "--folder=source"], text=True
            ).strip()
        )
        / "src"
    )
    recipe = subprocess.check_output(
        ["conan", "cache", "path", arrow_ref], text=True
    ).strip()
    helper_recipe = subprocess.check_output(
        ["conan", "cache", "path", "arrow/15.0.1-oss"], text=True
    ).strip()
    if not source.is_dir() or recipe != helper_recipe:
        raise ValueError(
            "Arrow helper source cache does not match the installed recipe revision"
        )
    write_json(reports / "arrow-source.json", {"ref": arrow_ref, "source": str(source)})
    run(["cmake", "--preset", "conan-release"], gluten / "cpp", log, env)
    run(
        [
            "cmake",
            "--build",
            "--preset",
            "conan-release",
            "--parallel",
            os.environ.get("CI_NUM_THREADS", "16"),
        ],
        gluten / "cpp",
        log,
        env,
    )
    run(["bash", "dev/build_bolt_arrow.sh"], gluten, log, env)
    # Install this checkout's JVM/test artifacts before running each required
    # module independently. A failing backend test must not prevent the Spark
    # reactor from reaching its own test module or force it to rerun backend UT.
    run(
        runtime_command(
            gluten,
            [
                *maven(gluten),
                "install",
                "-Pspark-ut",
                "-pl",
                "backends-bolt,gluten-ut/spark35",
                "-am",
                "-DskipTests=true",
            ],
        ),
        gluten,
        log,
        env,
    )
    native = gluten / "cpp/build/releases"
    packaged = gluten / "cpp/build/package/bolt/linux/amd64"
    for name in ("libgluten.so", "libbolt_backend.so", "libglutenlibloader.so"):
        source = native / name
        if not source.is_file() or not (packaged / name).is_file():
            raise ValueError(f"Required native library is missing: {source}")
        if source.read_bytes() != (packaged / name).read_bytes():
            raise ValueError(
                f"Gluten native staging contains a different library: {name}"
            )
    write_json(
        reports / "native-libraries.json",
        {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in packaged.glob("*.so")
        },
    )


def runtime_command(gluten, command):
    activation = gluten / "cpp/build/Release/generators/conanrun.sh"
    if not activation.is_file():
        raise ValueError(
            "Missing Conan runtime activation; native dependency closure is unavailable"
        )
    return [
        "bash",
        "-c",
        'source "$1" && shift && exec "$@"',
        "gluten-ci",
        str(activation),
        *command,
    ]


def maven(gluten, spark="3.5"):
    return [
        str(gluten / "build/mvn"),
        "-ntp",
        "-Pbackends-bolt",
        "-Pdelta",
        f"-Pspark-{spark}",
        "-Pjava-17",
        "-DskipTests=false",
        "-Dmaven.test.skip=false",
        "-Dmaven.test.failure.ignore=false",
        "-DfailIfNoTests=false",
    ]


def spark_test_home(home):
    # local-cluster executors also use this property to locate Spark JARs.
    # Keep the runtime distribution, even when its SQL resources are linked
    # from a separate source tree.
    return home.resolve()


def java_executable(env):
    java_home = env.get("JAVA_HOME")
    java = (
        Path(java_home) / "bin/java"
        if java_home
        else shutil.which("java", path=env.get("PATH", os.defpath))
    )
    if not java or not Path(java).is_file():
        raise ValueError("Gluten tests require a Java executable")
    return str(Path(java).resolve())


def scalatest_jvm(forks):
    """Capture the real Runner exit that ScalaTest's failure-ignore flag hides."""
    java = java_executable(os.environ)
    if forks.exists():
        shutil.rmtree(forks)
    forks.mkdir(parents=True)
    # Surefire also consumes -Djvm and requires the executable's bin/java suffix.
    wrapper = forks / "bin/java"
    wrapper.parent.mkdir()
    wrapper.write_text(
        f"#!{sys.executable}\n"
        "import json, os, signal, subprocess, sys\n"
        "from pathlib import Path\n"
        f"child = subprocess.Popen([{java!r}, *sys.argv[1:]])\n"
        "def forward(signum, frame):\n"
        "    if child.poll() is None:\n"
        "        child.send_signal(signum)\n"
        "for signum in (signal.SIGINT, signal.SIGTERM):\n"
        "    signal.signal(signum, forward)\n"
        "code = child.wait()\n"
        "if 'org.scalatest.tools.Runner' in sys.argv[1:]:\n"
        f"    record = Path({str(forks.resolve())!r}) / f'scalatest-fork-{{os.getpid()}}.json'\n"
        "    record.write_text(json.dumps({'exit_code': code}) + '\\n')\n"
        "sys.exit(code if code >= 0 else 128 - code)\n"
    )
    wrapper.chmod(0o755)
    return wrapper.resolve()


def verify_scalatest_exit(forks, expected_failures):
    records = list(forks.glob("scalatest-fork-*.json"))
    if len(records) != 1:
        raise ValueError("ScalaTest requires exactly one fresh fork exit record")
    code = json.loads(records[0].read_text()).get("exit_code")
    expected = 1 if expected_failures else 0
    if type(code) is not int or code != expected:
        raise ValueError(f"ScalaTest fork exit {code!r} does not match test results")


def spark_environment(home, env):
    release = home / "RELEASE"
    if release.is_file():
        version_matches = bool(re.search(r"\bSpark 3\.5\.5\b", release.read_text()))
    else:
        try:
            with zipfile.ZipFile(home / "python/lib/pyspark.zip") as archive:
                version_matches = bool(
                    re.search(
                        r"__version__(?:\s*:\s*str)?\s*=\s*['\"]3\.5\.5['\"]",
                        archive.read("pyspark/version.py").decode(),
                    )
                )
        except (OSError, KeyError, zipfile.BadZipFile):
            version_matches = False
    sql = home / "sql/core/src/test/resources/sql-tests/inputs"
    zips = sorted((home / "python/lib").glob("*.zip"))
    if (
        not version_matches
        or not (home / "bin/spark-submit").is_file()
        or not (home / "jars").is_dir()
        or not sql.is_dir()
        or not zips
    ):
        raise ValueError(
            "Spark UT requires Spark 3.5.5 binaries, Python ZIPs and source SQL test resources"
        )
    pythonpath = os.pathsep.join(str(path) for path in zips)
    if env.get("PYTHONPATH"):
        pythonpath += os.pathsep + env["PYTHONPATH"]
    return {**env, "SPARK_HOME": str(home), "PYTHONPATH": pythonpath}


def gluten_package_jars(directory):
    jars = []
    for path in directory.glob("gluten-package-*.jar"):
        if path.name.endswith(
            ("-sources.jar", "-test-sources.jar", "-javadoc.jar", "-tests.jar")
        ):
            continue
        # Maven also attaches a small Spark classifier JAR with build metadata
        # and Apache license files. The unclassified artifact is the native bundle.
        if path.name.endswith("-3.5.jar"):
            with zipfile.ZipFile(path) as jar:
                for name in jar.namelist():
                    if not (
                        name.endswith("/")
                        or name
                        in (
                            "META-INF/MANIFEST.MF",
                            "META-INF/LICENSE",
                            "META-INF/NOTICE",
                            "META-INF/DEPENDENCIES",
                            "gluten-build-info.properties",
                        )
                        or (
                            name.startswith("META-INF/maven/")
                            and name.endswith(("/pom.xml", "/pom.properties"))
                        )
                    ):
                        raise ValueError(
                            "Spark classifier must contain only build/Maven/license "
                            f"metadata: {name}"
                        )
            continue
        jars.append(path)
    return jars


def verify_jar(gluten, reports):
    jars = gluten_package_jars(gluten / "package/target")
    if len(jars) != 1:
        raise ValueError("Expected exactly one Gluten package JAR")
    expected = json.loads((reports / "native-libraries.json").read_text())
    with zipfile.ZipFile(jars[0]) as jar:
        providers = [
            name.removeprefix("META-INF/gluten-components/")
            for name in jar.namelist()
            if name.startswith("META-INF/gluten-components/")
            and "backendsapi." in name
            and name.endswith("Backend")
        ]
        if providers != ["org.apache.gluten.backendsapi.bolt.BoltBackend"]:
            raise ValueError("Packaged JAR does not select only the Bolt backend")
        for name, digest in expected.items():
            entries = [entry for entry in jar.namelist() if entry.endswith("/" + name)]
            if (
                len(entries) != 1
                or hashlib.sha256(jar.read(entries[0])).hexdigest() != digest
            ):
                raise ValueError(
                    f"Packaged JAR contains a different native library: {name}"
                )
    return jars[0]


def snapshot_blacklist(reports):
    content = BLACKLIST.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    inputs = reports / "inputs.json"
    if (
        inputs.is_file()
        and json.loads(inputs.read_text()).get("blacklist_sha256") != digest
    ):
        raise ValueError("Blacklist changed after preparation")
    snapshot = reports / "blacklist.txt"
    if snapshot.is_file() and snapshot.read_bytes() != content:
        raise ValueError("Blacklist changed between test stages")
    entries = set()
    for number, line in enumerate(content.decode().splitlines(), 1):
        if not line.strip() or line.startswith("#"):
            continue
        suite, separator, name = line.partition("#")
        if (
            not separator
            or not re.fullmatch(r"[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)+", suite)
            or not name.strip()
            or name in ("IGNORE_ALL", "(aborted)")
            or (suite, name) in entries
        ):
            raise ValueError(
                f"Invalid or duplicate exact blacklist entry at line {number}"
            )
        entries.add((suite, name))
    write_json(
        reports / "blacklist.json",
        {"entries": len(entries), "sha256": digest},
    )
    write_atomic(snapshot, content)
    return entries


@contextmanager
def stage_lock(gluten, stage):
    """Reject concurrent stages that would overwrite the same test reports."""
    module = (
        "backends-bolt"
        if stage == "jvm"
        else "native"
        if stage == "native"
        else "gluten-it"
        if stage.startswith("tpc-")
        else "spark-ut"
    )
    lock = gluten / ".ci" / f"gluten-bolt-{module}.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    with ExitStack() as stack:
        if stage != "native":
            profile = stack.enter_context(
                (gluten / ".ci/gluten-bolt-profile.lock").open("a")
            )
            mode = fcntl.LOCK_EX if stage in SPARK4_PROFILES else fcntl.LOCK_SH
            try:
                fcntl.flock(profile, mode | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise ValueError(
                    "Another Java stage or profile change is already running"
                ) from error
        handle = stack.enter_context(lock.open("a"))
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError(f"Another {module} stage is already running") from error
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def test_stage(stage, gluten, reports):
    with stage_lock(gluten, stage):
        return run_test_stage(stage, gluten, reports)


def run_test_stage(stage, gluten, reports):
    log = reports / f"{stage}.log"
    if log.exists():
        log.unlink()
    output = reports / stage
    output.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "SPARK_TESTING": "true", "SPARK_SCALA_VERSION": "2.12"}
    if stage in SPARK4_PROFILES:
        return run_spark4_stage(stage, gluten, reports, output, env)
    if stage == "native":
        for library in (gluten / "cpp/build/releases").glob("*.so"):
            run(runtime_command(gluten, ["ldd", str(library)]), gluten, log, env)
        if "not found" in log.read_text():
            raise ValueError("Native library runtime dependencies are missing")
        xml = output / "TEST-native.xml"
        run(
            runtime_command(
                gluten,
                [
                    "ctest",
                    "--test-dir",
                    str(gluten / "cpp/build/Release"),
                    "--no-tests=error",
                    "--output-on-failure",
                    "--output-junit",
                    str(xml),
                    "--timeout",
                    "7200",
                    "-j",
                    os.environ.get("CI_NUM_THREADS", "16"),
                ],
            ),
            gluten,
            log,
            env,
        )
        return verify_reports([xml])
    if stage.startswith("tpc-"):
        if stage == "tpc-h":
            run(
                runtime_command(
                    gluten, [*maven(gluten), "install", "-DskipTests=true"]
                ),
                gluten,
                log,
                env,
            )
            verify_jar(gluten, reports)
            run(
                runtime_command(
                    gluten,
                    [
                        str(gluten / "build/mvn"),
                        "-ntp",
                        "install",
                        "-Pspark-3.5",
                        "-Pjava-17",
                    ],
                ),
                gluten / "tools/gluten-it",
                log,
                env,
            )
        candidate_jar = verify_jar(gluten, reports)
        runtime_jars = gluten_package_jars(
            gluten / "tools/gluten-it/package/target/lib"
        )
        if (
            len(runtime_jars) != 1
            or runtime_jars[0].read_bytes() != candidate_jar.read_bytes()
        ):
            raise ValueError(
                "TPC runtime classpath does not contain the current candidate Gluten package"
            )
        kind = "h" if stage == "tpc-h" else "ds"
        resources = (
            gluten / f"tools/gluten-it/common/src/main/resources/tpc{kind}-queries"
        )
        queries = {path.stem for path in resources.glob("*.sql")}
        if not queries:
            raise ValueError("TPC query resources are missing")
        run(
            runtime_command(
                gluten,
                [
                    "bash",
                    "sbin/gluten-it.sh",
                    "queries-compare",
                    "--local",
                    "--preset=velox",
                    "--baseline-preset=vanilla",
                    f"--benchmark-type={kind}",
                    "--error-on-memleak",
                    "--explain",
                    "--off-heap-size=10g",
                    "-s=1.0",
                    "--threads=16",
                    "--iterations=1",
                ],
            ),
            gluten / "tools/gluten-it",
            log,
            {**env, "GLUTEN_IT_JVM_ARGS": "-Xmx6G"},
        )
        return verify_tpc(log.read_text(), queries)
    # Separate report directories prevent earlier runs from satisfying discovery.
    spark_home = Path(
        os.environ.get("BOLT_GLUTEN_SPARK_HOME", "/opt/shims/spark35/spark_home")
    )
    env = spark_environment(spark_home, env)
    command = [
        *maven(gluten),
        "test",
        f"-DargLine=-Dspark.test.home={spark_test_home(spark_home)}",
    ]
    if stage == "jvm":
        command += ["-pl", "backends-bolt", f"-DtagsToExclude={EXCLUDED_TAGS}"]
        env.pop("GLUTEN_TEST_BLACKLIST", None)
        env["GLUTEN_TEST_ENABLE_SLOW_HIVE"] = "false"
        return run_backend_groups(gluten, reports, command, env)
    else:
        command += ["-Pspark-ut", "-pl", "gluten-ut/spark35"]
        if stage.startswith("spark-"):
            command += [
                f"-DwildcardSuites={SPARK_GROUPS[int(stage[-1]) - 1]}",
                "-DtagsToExclude=org.apache.spark.tags.ExtendedSQLTest,"
                "org.apache.spark.tags.SlowHiveTest," + EXCLUDED_TAGS,
            ]
        else:
            tag = "ExtendedSQLTest" if stage == "extended" else "SlowHiveTest"
            command += [
                f"-DtagsToInclude=org.apache.spark.tags.{tag}",
                f"-DtagsToExclude={EXCLUDED_TAGS}",
            ]
        report_roots = [gluten / "gluten-ut/spark35"]
    entries = snapshot_blacklist(reports)
    forks = output / "forks"
    command.append(f"-Djvm={scalatest_jvm(forks)}")
    # Execute the tests, then classify exact expected failures in Bolt. Never
    # activate the former Gluten registration-time ignore hook through the host.
    env.pop("GLUTEN_TEST_BLACKLIST", None)
    command = [
        "-Dmaven.test.failure.ignore=true"
        if arg == "-Dmaven.test.failure.ignore=false"
        else arg
        for arg in command
    ]
    env["GLUTEN_TEST_ENABLE_SLOW_HIVE"] = "true" if stage == "slow-hive" else "false"
    # ScalaTest can override reportsDirectory; remove stale XML in target modules,
    # then preserve each stage's XML immediately before the next invocation.
    for root in report_roots:
        for path in root.glob("target/*reports/TEST-*.xml"):
            path.unlink()
    paths = []
    try:
        run(runtime_command(gluten, command), gluten, log, env)
    finally:
        for root in report_roots:
            for path in root.glob("target/*reports/TEST-*.xml"):
                target = output / (root.name + "-" + path.name)
                shutil.copy2(path, target)
                paths.append(target)
    evidence = verify_reports(paths, blacklist=entries)
    verify_scalatest_run(log.read_text(), paths)
    verify_scalatest_exit(forks, evidence["expected_failures"])
    write_json(reports / f"blacklist-{stage}.json", evidence)
    return evidence


def discover_backend_suites(gluten, output, env):
    module = gluten / "backends-bolt"
    classes = module / "target/scala-2.12/test-classes"
    main_classes = module / "target/scala-2.12/classes"
    dependencies = output / "test-classpath.txt"
    names = output / "discovered-suites.txt"
    log = output / "discovery.log"
    for path in (dependencies, names, log):
        if path.exists():
            path.unlink()
    run(
        runtime_command(
            gluten,
            [*maven(gluten), "test-compile", "-pl", "backends-bolt"],
        ),
        gluten,
        log,
        env,
    )
    if not classes.is_dir() or not main_classes.is_dir():
        raise ValueError("Backend test compilation produced no class directories")
    run(
        runtime_command(
            gluten,
            [
                *maven(gluten),
                "dependency:build-classpath",
                "-pl",
                "backends-bolt",
                "-DincludeScope=test",
                f"-Dmdep.outputFile={dependencies.resolve()}",
            ],
        ),
        gluten,
        log,
        env,
    )
    classpath = os.pathsep.join(
        [str(classes), str(main_classes), dependencies.read_text().strip()]
    )
    run(
        [
            java_executable(env),
            "--class-path",
            classpath,
            str(ROOT / ".github/scripts/DiscoverGlutenSuites.java"),
            str(names),
            str(classes),
            str(main_classes),
        ],
        gluten,
        log,
        env,
    )
    suites = names.read_text().splitlines()
    if (
        not suites
        or len(suites) != len(set(suites))
        or any(
            not re.fullmatch(r"[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)+", name)
            for name in suites
        )
    ):
        raise ValueError("Invalid or empty backend suite discovery")
    return suites


def run_backend_groups(gluten, reports, command, env):
    output = reports / "jvm"
    entries = snapshot_blacklist(reports)
    suites = discover_backend_suites(gluten, output, env)
    delta = [name for name in suites if name.startswith("org.apache.spark.sql.delta.")]
    ordinary = [name for name in suites if name not in delta]
    if not ordinary:
        raise ValueError("No ordinary backend ScalaTest suites discovered")
    groups = [("backend", ordinary)] + [(name, [name]) for name in delta]
    write_json(output / "suite-groups.json", dict(groups))
    module = gluten / "backends-bolt"
    results = {"tests": 0, "skipped": 0, "expected_failures": [], "groups": {}}
    errors = []
    for number, (name, selected) in enumerate(groups):
        group = output / str(number)
        group.mkdir(exist_ok=True)
        log = group / "run.log"
        if log.exists():
            log.unlink()
        forks = group / "forks"
        args = [
            "scalatest:test" if number and arg == "test" else arg for arg in command
        ]
        args = [
            "-Dmaven.test.failure.ignore=true"
            if arg == "-Dmaven.test.failure.ignore=false"
            else arg
            for arg in args
        ]
        args += [f"-Dsuites={','.join(selected)}", f"-Djvm={scalatest_jvm(forks)}"]
        for path in module.glob("target/*reports/TEST-*.xml"):
            path.unlink()
        paths = []
        try:
            try:
                run(runtime_command(gluten, args), gluten, log, env)
            finally:
                for path in module.glob("target/*reports/TEST-*.xml"):
                    target = group / path.name
                    shutil.copy2(path, target)
                    paths.append(target)
            reported = {
                suite.get("name")
                for path in paths
                for suite in ET.parse(path).getroot().iter("testsuite")
            }
            if not set(selected).issubset(reported):
                raise ValueError("Missing discovered backend suite reports")
            evidence = verify_reports(paths, blacklist=entries)
            verify_scalatest_run(log.read_text(), paths)
            verify_scalatest_exit(forks, evidence["expected_failures"])
            results["groups"][name] = {"result": "success", **evidence}
            for key in ("tests", "skipped"):
                results[key] += evidence[key]
            results["expected_failures"].extend(evidence["expected_failures"])
        except (
            ValueError,
            OSError,
            subprocess.CalledProcessError,
            ET.ParseError,
        ) as error:
            errors.append(f"{name}: {error}")
            results["groups"][name] = {"result": "failure", "error": str(error)}
        write_json(reports / "blacklist-jvm.json", results)
    if errors:
        raise ValueError("Backend test groups failed: " + "; ".join(errors))
    return results


def verify_spark4_native(gluten, reports):
    expected = json.loads((reports / "native-libraries.json").read_text())
    classes = gluten / "backends-bolt/target/scala-2.13/classes"
    for name, digest in expected.items():
        paths = list(classes.rglob(name))
        if (
            len(paths) != 1
            or hashlib.sha256(paths[0].read_bytes()).hexdigest() != digest
        ):
            raise ValueError(
                f"Spark 4 backend contains a different native library: {name}"
            )


def run_spark4_stage(stage, gluten, reports, output, env):
    profile = SPARK4_PROFILES[stage]
    env = {**env, "SPARK_SCALA_VERSION": "2.13"}
    env.pop("GLUTEN_TEST_BLACKLIST", None)
    command = [
        *maven(gluten, profile),
        "-Pscala-2.13",
        "-Pspark-ut",
        "-Dmaven.compiler.release=17",
    ]
    run(
        runtime_command(
            gluten,
            [
                *command,
                "clean",
                "install",
                "-pl",
                f"backends-bolt,gluten-ut/{stage}",
                "-am",
                "-DskipTests=true",
            ],
        ),
        gluten,
        reports / f"{stage}-compile.log",
        env,
    )
    verify_spark4_native(gluten, reports)
    results = {}
    for name, module, suites, argline in (
        (
            "backend",
            "backends-bolt",
            "org.apache.gluten.execution.BoltParquetDataTypeValidationSuite,org.apache.gluten.execution.BoltOrcDataTypeValidationSuite",
            "-Dspark.sql.ansi.enabled=false",
        ),
        ("ut", f"gluten-ut/{stage}", SPARK4_SUITES, ""),
    ):
        directory = output / name
        directory.mkdir(parents=True, exist_ok=True)
        module_root = gluten / module
        for path in module_root.glob("target/*reports/TEST-*.xml"):
            path.unlink()
        log = reports / f"{stage}-{name}.log"
        if log.exists():
            log.unlink()
        forks = directory / "forks"
        invocation = [
            *command,
            "test",
            "-pl",
            module,
            f"-Dsuites={suites}",
            f"-Djvm={scalatest_jvm(forks)}",
        ]
        if argline:
            invocation.append(f"-DargLine={argline}")
        paths = []
        try:
            run(runtime_command(gluten, invocation), gluten, log, env)
        finally:
            for path in module_root.glob("target/*reports/TEST-*.xml"):
                target = directory / path.name
                shutil.copy2(path, target)
                paths.append(target)
        # Spark 4 coverage is intentionally strict; Spark 3.5's temporary
        # blacklist must not turn a new-version incompatibility into success.
        results[name] = verify_reports(paths)
        verify_scalatest_run(log.read_text(), paths)
        verify_scalatest_exit(forks, [])
    write_json(reports / f"{stage}-results.json", results)
    return results


def test_all(gluten, reports):
    results = {}
    for stage in STAGES:
        try:
            evidence = test_stage(stage, gluten, reports)
            results[stage] = {"result": "success", **evidence}
        except (
            ValueError,
            OSError,
            KeyError,
            subprocess.CalledProcessError,
            ET.ParseError,
        ) as error:
            results[stage] = {"result": "failure", "error": str(error)}
            print(f"::error::{stage}: {error}", flush=True)
        write_json(reports / "results.json", results)
    if any(result["result"] != "success" for result in results.values()):
        raise ValueError(
            "Required Gluten backend tests failed; see results.json and stage logs"
        )


def gate():
    needs = json.loads(os.environ["GLUTEN_CI_NEEDS"])
    for job in ("prepare", "compatibility", "integration"):
        if needs.get(job, {}).get("result") != "success":
            raise ValueError(f"Required job {job} did not succeed: {needs.get(job)}")
    print("All required Gluten Bolt integration jobs succeeded")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("prepare", "patch", "build", "test", "gate")
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Prepare on the local host without an image (unavailable in Actions)",
    )
    parser.add_argument("--gluten-dir", type=Path, default=ROOT / "gluten")
    parser.add_argument("--reports-dir", type=Path, default=ROOT / ".ci/gluten-bolt")
    args = parser.parse_args()
    if args.local and args.command != "prepare":
        parser.error("--local applies only to prepare")
    reports, gluten = args.reports_dir.resolve(), args.gluten_dir.resolve()
    reports.mkdir(parents=True, exist_ok=True)
    try:
        if args.command == "prepare":
            prepare(reports, local=args.local, gluten=gluten if args.local else None)
        elif args.command == "patch":
            apply_gluten_patches(gluten, reports)
        elif args.command == "build":
            build(gluten, reports)
        elif args.command == "test":
            test_all(gluten, reports)
        else:
            gate()
    except (
        ValueError,
        OSError,
        KeyError,
        subprocess.CalledProcessError,
        ET.ParseError,
    ) as error:
        write_json(reports / f"{args.command}-error.json", {"error": str(error)})
        print(f"::error::{error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
