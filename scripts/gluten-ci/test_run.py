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

import hashlib
import io
from contextlib import redirect_stderr
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

import run

HERE = Path(__file__).resolve().parent
NATIVE_LIBRARIES = (
    "cpp/build/package/bolt/linux/amd64/libgluten.so",
    "cpp/build/package/bolt/linux/amd64/libbolt_backend.so",
    "cpp/build/package/bolt/linux/amd64/libglutenlibloader.so",
    "cpp/build/Release/bolt/udf/examples/libmyudf.so",
    "cpp/build/Release/bolt/udf/examples/libmyudaf.so",
)
MATRIX = {
    "spark35": ("3.5", "2.12", 17, "iceberg,delta,hudi,paimon,lance,celeborn,uniffle"),
    "spark35-scala213": ("3.5", "2.13", 17, "iceberg,delta"),
    "spark35-java8": ("3.5", "2.12", 8, ""),
    "spark35-java11": ("3.5", "2.12", 11, ""),
    "spark40": ("4.0", "2.13", 17, "delta,paimon,lance"),
    "spark40-fast-build": ("4.0", "2.13", 17, "iceberg,delta,paimon,fast-build"),
    "spark41": ("4.1", "2.13", 17, "delta"),
    "spark40-java21": ("4.0", "2.13", 21, ""),
    "spark40-java25": ("4.0", "2.13", 25, ""),
    "spark41-java21": ("4.1", "2.13", 21, ""),
    "spark41-java25": ("4.1", "2.13", 25, ""),
    "spark42": ("4.2", "2.13", 17, "delta"),
}


class MatrixTest(unittest.TestCase):
    def test_matrix_matches_workflow(self):
        tasks = run.load_matrix()
        self.assertEqual(len(tasks), 39)
        self.assertEqual(tasks["native"]["kind"], "native")
        self.assertEqual(tasks["native"]["jdk"], 17)
        for name, (spark, scala, jdk, extras) in MATRIX.items():
            with self.subTest(name=name):
                task = tasks[f"package:{name}"]
                self.assertEqual(
                    (task["kind"], task["spark"], task["jdk"]), ("package", spark, jdk)
                )
                profiles = [
                    "backends-bolt",
                    f"spark-{spark}",
                    f"scala-{scala}",
                    f"java-{jdk}",
                ]
                if extras:
                    profiles.extend(extras.split(","))
                self.assertEqual(task["profiles"], profiles)
        stages = {
            "spark-1": "org.apache.spark.sql.streaming,org.apache.spark.GlutenSortShuffleSuite,org.apache.gluten",
            "spark-2": "org.apache.spark.sql.execution,org.apache.spark.sql.catalyst,org.apache.spark.sql.errors,org.apache.spark.sql.extension",
            "spark-3": "org.apache.spark.sql.GlutenSQL,org.apache.spark.sql.Gluten,org.apache.spark.sql.connector,org.apache.spark.sql.sources,org.apache.spark.sql.hive,org.apache.spark.sql.gluten,org.apache.spark.sql.shim",
            "extended": "org.apache.spark.tags.ExtendedSQLTest",
            "slow-hive": "org.apache.spark.tags.SlowHiveTest",
        }
        versions = {
            "3.5": ("2.12", ["iceberg", "delta", "hudi", "paimon"]),
            "4.0": ("2.13", ["delta", "paimon"]),
            "4.1": ("2.13", ["delta"]),
            "4.2": ("2.13", ["delta"]),
        }
        for spark, (scala, extras) in versions.items():
            expected = dict(stages)
            if spark == "3.5":
                expected.update(jvm="jvm,delta", tpc="h,ds")
            for stage, selector in expected.items():
                with self.subTest(spark=spark, stage=stage):
                    task = tasks[f"spark:{spark}:{stage}"]
                    self.assertEqual(
                        (task["kind"], task["spark"], task["jdk"], task["stage"]),
                        ("spark", spark, 17, stage),
                    )
                    self.assertEqual(task["selector"], selector)
                    self.assertEqual(
                        task["profiles"],
                        [
                            "fast-build",
                            "backends-bolt",
                            f"spark-{spark}",
                            f"scala-{scala}",
                            "java-17",
                            *extras,
                        ],
                    )

    def test_scala213_and_dedicated_backend_test_cells(self):
        tasks = run.load_matrix()
        for stage in ("spark-1", "spark-2", "spark-3"):
            with self.subTest(stage=stage):
                task = tasks.get(f"spark:3.5-scala213:{stage}")
                self.assertIsNotNone(task)
                self.assertEqual(task["spark"], "3.5")
                self.assertEqual(task["stage"], stage)
                self.assertEqual(
                    task["selector"], tasks[f"spark:3.5:{stage}"]["selector"]
                )
                self.assertEqual(
                    task["profiles"],
                    [
                        "fast-build",
                        "backends-bolt",
                        "spark-3.5",
                        "scala-2.13",
                        "java-17",
                        "iceberg",
                        "delta",
                    ],
                )
        self.assertFalse(any(task["spark"] == "3.4" for task in tasks.values()))
        self.assertEqual(
            tasks["spark:3.5:udf"]["selector"],
            "org.apache.gluten.expression.BoltUdfSuiteLocal",
        )

    def test_unknown_task_is_rejected_before_using_directories(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = [
                str(Path(directory) / name)
                for name in ("source", "bolt", "driver", "reports", "artifacts")
            ]
            command = [
                sys.executable,
                str(HERE / "run.py"),
                "--task",
                "spark:3.5:unknown",
            ]
            for name, path in zip(
                ("source", "bolt", "driver", "reports", "artifacts"), paths
            ):
                command.extend([f"--{name}", path])
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertIn("invalid choice", result.stderr)
            self.assertFalse(Path(paths[-2]).exists())


class ReportTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.reports = Path(temp.name)
        self.tasks = run.load_matrix()
        self.enterContext(redirect_stderr(io.StringIO()))

    def write(self, path, contents):
        path = self.reports / path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)

    def test_missing_or_only_skipped_native_results_fail(self):
        native = self.tasks["native"]
        self.assertEqual(
            run.summarize_tests(native, self.reports),
            (
                {"total": 0, "passed": 0, "failed": 0, "skipped": 0},
                False,
            ),
        )
        self.write(
            "TEST-native.xml", "<testsuite><testcase><skipped/></testcase></testsuite>"
        )
        self.assertEqual(
            run.summarize_tests(native, self.reports),
            (
                {"total": 1, "passed": 0, "failed": 0, "skipped": 1},
                False,
            ),
        )

    def test_native_counts_failures_and_errors(self):
        self.write(
            "TEST-native.xml",
            "<testsuite><testcase/><testcase><failure/></testcase>"
            "<testcase><error/></testcase><testcase><skipped/></testcase></testsuite>",
        )
        counts, valid = run.summarize_tests(self.tasks["native"], self.reports)
        self.assertEqual(counts, {"total": 4, "passed": 1, "failed": 2, "skipped": 1})
        self.assertFalse(valid)

    def test_spark_needs_reports_from_selected_sql_module(self):
        report = "<testsuite><testcase/></testsuite>"
        task = self.tasks["spark:4.2:extended"]
        self.write("extended/gluten-core/target/surefire-reports/TEST-core.xml", report)
        self.assertFalse(run.summarize_tests(task, self.reports)[1])
        self.write(
            "extended/gluten-ut/spark42/target/surefire-reports/TEST-sql.xml", report
        )
        self.assertEqual(
            run.summarize_tests(task, self.reports),
            (
                {"total": 1, "passed": 1, "failed": 0, "skipped": 0},
                True,
            ),
        )
        self.write(
            "extended/gluten-ut/spark42/target/surefire-reports/TEST-invalid.xml",
            "<testsuite",
        )
        self.assertFalse(run.summarize_tests(task, self.reports)[1])

    def test_native_rejects_junit_entity_declarations(self):
        self.write(
            "TEST-native.xml",
            '<!DOCTYPE testsuite [<!ENTITY x "injected">]>'
            '<testsuite><testcase name="&x;"/></testsuite>',
        )
        self.assertFalse(run.summarize_tests(self.tasks["native"], self.reports)[1])

    def test_oversized_junit_report_cannot_count_as_evidence(self):
        self.write("TEST-native.xml", "<testsuite><testcase/></testsuite>")
        with (self.reports / "TEST-native.xml").open("ab") as report:
            report.truncate(64 * 1024 * 1024 + 1)
        self.assertFalse(run.summarize_tests(self.tasks["native"], self.reports)[1])

    def test_spark_does_not_read_reports_through_linked_directories(self):
        self.write(
            "elsewhere/target/surefire-reports/TEST-sql.xml",
            "<testsuite><testcase/></testsuite>",
        )
        (self.reports / "spark-1/gluten-ut").mkdir(parents=True)
        (self.reports / "spark-1/gluten-ut/spark35").symlink_to(
            self.reports / "elsewhere",
            target_is_directory=True,
        )
        self.assertFalse(
            run.summarize_tests(self.tasks["spark:3.5:spark-1"], self.reports)[1]
        )

    def test_udf_needs_the_named_backend_suite(self):
        task = {
            "kind": "spark",
            "spark": "3.5",
            "stage": "udf",
            "selector": "org.apache.gluten.expression.BoltUdfSuiteLocal",
        }
        report = "<testsuite><testcase/></testsuite>"
        self.write(
            "udf/backends-bolt/target/scalatest-reports/TEST-unrelated.xml", report
        )
        self.assertFalse(run.summarize_tests(task, self.reports)[1])
        self.write(
            "udf/backends-bolt/target/scalatest-reports/TEST-org.apache.gluten.expression.BoltUdfSuiteLocal.xml",
            report,
        )
        self.assertEqual(run.summarize_tests(task, self.reports)[0]["total"], 1)
        self.assertTrue(run.summarize_tests(task, self.reports)[1])

    def test_tpc_needs_nonzero_comparisons_for_both_benchmarks(self):
        task = self.tasks["spark:3.5:tpc"]
        self.write("tpc-h/run.log", "    Summary: 22 out of 22 queries passed.\n")
        self.assertFalse(run.summarize_tests(task, self.reports)[1])
        self.write("tpc-ds/run.log", "Summary: 0 out of 0 queries passed.\n")
        self.assertFalse(run.summarize_tests(task, self.reports)[1])
        self.write("tpc-ds/run.log", "    Summary: 99 out of 99 queries passed.\n")
        self.assertEqual(
            run.summarize_tests(task, self.reports),
            (
                {"total": 121, "passed": 121, "failed": 0, "skipped": 0},
                True,
            ),
        )

    def test_jvm_needs_ordinary_loader_and_delta_reports(self):
        task = self.tasks["spark:3.5:jvm"]
        loader = "org.apache.gluten.jni.BoltNativeLoaderSuite"
        delta = "org.apache.spark.sql.delta.ExampleSuite"
        ordinary = "org.apache.gluten.ExampleSuite"
        self.write("jvm/discovered-suites.txt", f"{ordinary}\n{loader}\n{delta}\n")
        report = "<testsuite><testcase/></testsuite>"
        for group, suite in (("backend", ordinary), (loader, loader)):
            self.write(
                f"jvm/{group}/backends-bolt/target/scalatest-reports/TEST-{suite}.xml",
                report,
            )
        self.assertFalse(run.summarize_tests(task, self.reports)[1])
        self.write(
            f"jvm/{delta}/backends-bolt/target/scalatest-reports/TEST-{delta}.xml",
            report,
        )
        self.assertEqual(
            run.summarize_tests(task, self.reports),
            (
                {"total": 3, "passed": 3, "failed": 0, "skipped": 0},
                True,
            ),
        )


class ArtifactTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.artifacts = self.root / "artifacts"
        self.artifacts.mkdir()
        self.archive = self.artifacts / "native.tar.gz"
        self.identity = {
            "source_sha": "a" * 40,
            "bolt_sha": "b" * 40,
            "driver_sha": "c" * 40,
            "helper_sha": "d" * 40,
            "config_sha256": "e" * 64,
        }

    def make_archive(self, paths=NATIVE_LIBRARIES, malicious=None):
        with tarfile.open(self.archive, "w:gz") as archive:
            for path in paths:
                member = tarfile.TarInfo(path)
                member.size = 7
                archive.addfile(member, io.BytesIO(b"library"))
            if malicious is not None:
                archive.addfile(malicious)
        manifest = {
            "schema": 1,
            **self.identity,
            "archive_sha256": hashlib.sha256(self.archive.read_bytes()).hexdigest(),
        }
        (self.artifacts / "native-manifest.json").write_text(json.dumps(manifest))

    def test_native_archive_requires_matching_identity_and_digest(self):
        self.make_archive()
        with self.assertRaises(ValueError):
            run.restore_native(
                self.artifacts, self.source, {**self.identity, "bolt_sha": "f" * 40}
            )
        with self.archive.open("ab") as archive:
            archive.write(b"tampering")
        with self.assertRaises(ValueError):
            run.restore_native(self.artifacts, self.source, self.identity)
        self.assertEqual(list(self.source.iterdir()), [])

    def test_native_archive_rejects_paths_and_links_before_extracting(self):
        invalid_members = [
            tarfile.TarInfo(path)
            for path in (
                "/outside",
                "../outside",
                "cpp/build/package/bolt/linux/amd64/../../outside",
                "pom.xml",
                NATIVE_LIBRARIES[0],
            )
        ]
        for kind in (tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.FIFOTYPE):
            member = tarfile.TarInfo("bolt-runtime/libunwanted.so")
            member.type = kind
            member.linkname = "../outside"
            invalid_members.append(member)
        for member in invalid_members:
            with self.subTest(member=member.name, kind=member.type):
                self.make_archive(malicious=member)
                with self.assertRaises(ValueError):
                    run.restore_native(self.artifacts, self.source, self.identity)
                self.assertEqual(list(self.source.iterdir()), [])

    def test_native_archive_requires_all_packaged_and_example_libraries(self):
        self.make_archive(NATIVE_LIBRARIES[:-1])
        with self.assertRaises(ValueError):
            run.restore_native(self.artifacts, self.source, self.identity)
        self.assertEqual(list(self.source.iterdir()), [])

    def test_existing_symlink_cannot_redirect_extraction(self):
        self.make_archive()
        outside = self.root / "outside"
        outside.mkdir()
        (self.source / "cpp").symlink_to(outside, target_is_directory=True)
        with self.assertRaises(ValueError):
            run.restore_native(self.artifacts, self.source, self.identity)
        self.assertEqual(list(outside.iterdir()), [])

    def test_published_native_archive_round_trips(self):
        for library in NATIVE_LIBRARIES:
            path = self.source / library
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(library.encode())
        runtime = self.root / "runtime"
        runtime.mkdir()
        (runtime / "libdep.so.3").write_bytes(b"dependency")
        run.create_native_archive(self.artifacts, self.source, runtime, self.identity)
        downstream = self.root / "downstream"
        downstream.mkdir()
        run.restore_native(self.artifacts, downstream, self.identity)
        for library in NATIVE_LIBRARIES:
            self.assertEqual((downstream / library).read_bytes(), library.encode())
        self.assertEqual(
            (downstream / "bolt-runtime/libdep.so.3").read_bytes(), b"dependency"
        )

    def test_archive_restores_allowed_files(self):
        runtime = "bolt-runtime/libz.so.1"
        self.make_archive((*NATIVE_LIBRARIES, runtime))
        run.restore_native(self.artifacts, self.source, self.identity)
        for library in (*NATIVE_LIBRARIES, runtime):
            self.assertEqual((self.source / library).read_bytes(), b"library")


class ExplicitPathTest(unittest.TestCase):
    def test_spark_environment_sources_external_driver_and_target_pom(self):
        if shutil.which("xmllint") is None:
            self.skipTest("xmllint is needed for the Spark environment helper")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "target checkout"
            source.mkdir()
            (source / "pom.xml").write_text(
                "<project><profiles><profile><id>spark-3.5</id><properties>"
                "<spark.version>3.5.5</spark.version></properties></profile></profiles></project>"
            )
            driver = root / "external driver"
            driver_script = driver / ".github/scripts/gluten_ci.sh"
            driver_script.parent.mkdir(parents=True)
            driver_script.write_text("DRIVER_LOADED=trusted\n")
            env = {
                **os.environ,
                "GLUTEN_DIR": str(source),
                "GLUTEN_CI_DRIVER_DIR": str(driver),
                "GLUTEN_REPORTS_DIR": str(root / "reports"),
                "SPARK_PROFILE": "3.5",
            }
            result = subprocess.run(
                [
                    "bash",
                    "-euc",
                    'source "$1"; printf "%s\\n" "$DRIVER_LOADED" "$SPARK_VERSION" "${MVN[0]}"',
                    "test",
                    str(HERE / "ci-bolt-spark-env.sh"),
                ],
                cwd=root,
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                result.stdout.splitlines(),
                ["trusted", "3.5.5", str(source / "build/mvn")],
            )

            result = subprocess.run(
                [
                    "bash",
                    "-euc",
                    'source "$1"; printf "%s\\n" "$SPARK_SCALA_VERSION" "$SPARK_RESOURCE_PROFILE" "$SPARK_RESOURCE_MODULE"',
                    "test",
                    str(HERE / "ci-bolt-spark-env.sh"),
                ],
                cwd=root,
                env={**env, "GLUTEN_CI_SCALA_VERSION": "2.13"},
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                result.stdout.splitlines(),
                ["2.13", "3.5-scala2.13", "spark35-scala-2.13"],
            )


class CliTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.paths = {
            "source": self.root / "target checkout",
            "bolt": self.root / "bolt engine",
            "driver": self.root / "pinned driver",
            "reports": self.root / "reports",
            "artifacts": self.root / "artifacts",
        }
        identity = {"helper_sha": "a" * 40, "config_sha256": "b" * 64}
        for directory in ("source", "bolt", "driver"):
            path = self.paths[directory]
            subprocess.run(["git", "init", "-q", path], check=True)
            subprocess.run(
                [
                    "git",
                    "-C",
                    path,
                    "-c",
                    "user.email=test@example.org",
                    "-c",
                    "user.name=Fixture",
                    "commit",
                    "-qm",
                    directory,
                    "--allow-empty",
                ],
                check=True,
            )
            identity[f"{directory}_sha"] = subprocess.check_output(
                ["git", "-C", path, "rev-parse", "HEAD"],
                text=True,
            ).strip()
        self.paths["artifacts"].mkdir()
        archive_path = self.paths["artifacts"] / "native.tar.gz"
        with tarfile.open(archive_path, "w:gz") as archive:
            for library in NATIVE_LIBRARIES:
                member = tarfile.TarInfo(library)
                member.size = 7
                archive.addfile(member, io.BytesIO(b"library"))
        manifest = {
            "schema": 1,
            **identity,
            "archive_sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
        }
        (self.paths["artifacts"] / "native-manifest.json").write_text(
            json.dumps(manifest)
        )
        self.executable(
            self.root / "jdk/bin/javac", "#!/bin/bash\necho 'javac 17.0.1' >&2\n"
        )
        self.env = {
            **os.environ,
            "JAVA_HOME": str(self.root / "jdk"),
            "GLUTEN_MAVEN_REPO": str(self.root / "maven repo"),
            "GLUTEN_CI_HELPER_SHA": identity["helper_sha"],
            "GLUTEN_CI_CONFIG_SHA256": identity["config_sha256"],
            "GLUTEN_CI_DRIVER_SHA": identity["driver_sha"],
        }

    def executable(self, path, content):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        path.chmod(0o755)

    def invoke(self, task="package:spark35", options=(), script=HERE / "run.py"):
        command = [sys.executable, str(script), "--task", task, *options]
        for name, path in self.paths.items():
            command.extend([f"--{name}", str(path)])
        process = subprocess.run(
            command, cwd=self.root, env=self.env, capture_output=True, text=True
        )
        result_path = self.paths["reports"] / "result.json"
        return process, (
            json.loads(result_path.read_text()) if result_path.exists() else {}
        )

    def fake_maven_packages_jar(self, mutation=""):
        self.executable(
            self.paths["source"] / "build/mvn",
            f"""#!{sys.executable}
import os
from pathlib import Path
import subprocess
import sys
from zipfile import ZipFile

assert sys.argv[1:5] == ["-ntp", "clean", "install",
    "-Pbackends-bolt,spark-3.5,scala-2.12,java-17,iceberg,delta,hudi,paimon,lance,celeborn,uniffle,"
    "bolt-ut-delta,bolt-ut-hudi,bolt-ut-iceberg,bolt-ut-paimon"]
assert f"-Dmaven.repo.local={{os.environ['GLUTEN_MAVEN_REPO']}}" in sys.argv
assert "-DskipTests" in sys.argv
revision = subprocess.check_output(
    ["git", "-C", "ep/build-bolt/build/bolt_ep", "rev-parse", "HEAD"], text=True,
).strip()
libs = Path("cpp/build/package/bolt/linux/amd64")
{mutation}
target = Path("package/target")
target.mkdir(parents=True)
with ZipFile(target / "gluten-bolt-bundle-fixture.jar", "w") as jar:
    for lib in libs.glob("*.so"):
        jar.write(lib, "linux/amd64/" + lib.name)
    jar.writestr("gluten-build-info.properties", f"bolt_revision={{revision}}" + chr(10))
""",
        )

    def fake_native_build(
        self, *, empty_backend=False, package_mismatch=False, ctest_report=None
    ):
        bundle = self.root / "helpers/scripts/gluten-ci"
        shutil.copytree(HERE, bundle, ignore=shutil.ignore_patterns("__pycache__"))
        shutil.rmtree(self.paths["artifacts"])
        self.executable(
            self.paths["bolt"] / "scripts/install-bolt-deps.sh", "#!/bin/bash\nexit 0\n"
        )
        bin_dir = self.root / "bin"
        self.executable(bin_dir / "ldd", "#!/bin/bash\nexit 0\n")
        self.executable(
            bin_dir / "ctest",
            f"""#!{sys.executable}
import os
from pathlib import Path
reports = Path(os.environ["GLUTEN_REPORTS_DIR"])
(reports / "ctest-invoked.txt").touch()
report = {ctest_report!r}
if report is not None:
    (reports / "TEST-native.xml").write_text(report)
""",
        )
        self.env.update(
            PATH=f"{bin_dir}:{self.env['PATH']}", CONAN_HOME=str(self.root / "conan")
        )
        self.executable(
            bundle / "ci-bolt-build.sh",
            f"""#!/bin/bash
exec {sys.executable} - "$@" <<'PYTHON'
import json
from pathlib import Path
import subprocess
import sys
bolt, source, reports = map(Path, sys.argv[1:4])
revision = subprocess.check_output(["git", "-C", bolt, "rev-parse", "HEAD"], text=True).strip()
for name in {tuple(sorted(NATIVE_LIBRARIES))!r}:
    path = source / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"" if {empty_backend!r} and path.name == "libbolt_backend.so" else b"library")
conanrun = source / "cpp/build/Release/generators/conanrun.sh"
conanrun.parent.mkdir(parents=True)
conanrun.touch()
node = dict(name="bolt", context="host", ref=f"bolt/ci-{{revision}}#" + "a" * 32,
            package_id="b" * 40, prev="c" * 32)
package = {{key: node[key] for key in ("ref", "package_id", "prev")}}
(reports / "bolt-package.json").write_text(json.dumps(package))
(reports / "bolt-export.json").write_text(json.dumps({{"graph": {{"nodes": {{"0": node}}}}}}))
if {package_mismatch!r}:
    node["prev"] = "d" * 32
(reports / "gluten-install.json").write_text(json.dumps({{"graph": {{"nodes": {{"1": node}}}}}}))
PYTHON
""",
        )
        return bundle / "run.py"

    def test_native_only_requires_ut_and_verifies_the_compiled_package(self):
        script = self.fake_native_build(
            ctest_report='<testsuite><testcase name="native"/></testsuite>'
        )
        process, result = self.invoke("native", ["--native-only"], script)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertTrue((self.paths["reports"] / "ctest-invoked.txt").exists())
        self.assertEqual(result["mode"], "native")
        self.assertEqual(result["tests"], dict(total=1, passed=1, failed=0, skipped=0))
        self.assertIn("compile-verification.json", result["evidence"])
        manifest = json.loads(
            (self.paths["artifacts"] / "native-manifest.json").read_text()
        )
        self.assertEqual(manifest["mode"], "native")
        identity = json.loads((self.paths["reports"] / "sources.json").read_text())
        with self.assertRaisesRegex(ValueError, "native-only"):
            run.restore_native(self.paths["artifacts"], self.paths["source"], identity)

    def test_native_only_rejects_an_empty_test_suite(self):
        script = self.fake_native_build(ctest_report="<testsuite/>")
        process, result = self.invoke("native", ["--native-only"], script)
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertEqual(result["tests"]["total"], 0)
        self.assertFalse((self.paths["artifacts"] / "native-manifest.json").exists())

    def test_native_only_rejects_all_skipped_tests(self):
        script = self.fake_native_build(
            ctest_report='<testsuite><testcase name="native"><skipped/></testcase></testsuite>'
        )
        process, result = self.invoke("native", ["--native-only"], script)
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertEqual(result["tests"]["skipped"], 1)
        self.assertFalse((self.paths["artifacts"] / "native-manifest.json").exists())

    def test_native_only_fails_without_test_evidence(self):
        script = self.fake_native_build()
        process, result = self.invoke("native", ["--native-only"], script)
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertEqual(result["status"], "failure")
        self.assertFalse((self.paths["artifacts"] / "native-manifest.json").exists())

    def test_native_only_fails_on_a_reported_failure_even_if_ctest_exits_zero(self):
        script = self.fake_native_build(
            ctest_report='<testsuite><testcase name="native"><failure/></testcase></testsuite>'
        )
        process, result = self.invoke("native", ["--native-only"], script)
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertEqual(result["tests"]["failed"], 1)
        self.assertFalse((self.paths["artifacts"] / "native-manifest.json").exists())

    def test_native_only_fails_on_package_mismatch_despite_passing_ut(self):
        script = self.fake_native_build(
            package_mismatch=True,
            ctest_report='<testsuite><testcase name="native"/></testsuite>',
        )
        process, result = self.invoke("native", ["--native-only"], script)
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertIn("Bolt package mismatch", process.stderr)
        self.assertFalse((self.paths["artifacts"] / "native-manifest.json").exists())

    def test_compile_only_publishes_verified_artifact_without_invoking_ctest(self):
        script = self.fake_native_build()
        process, result = self.invoke("native", ["--compile-only"], script)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertFalse((self.paths["reports"] / "ctest-invoked.txt").exists())
        self.assertEqual(result["mode"], "compile")
        self.assertEqual(result["status"], "success")
        self.assertEqual(
            result["tests"], {"total": 0, "passed": 0, "failed": 0, "skipped": 0}
        )
        self.assertIn("compile-verification.json", result["evidence"])
        evidence = json.loads(
            (self.paths["reports"] / "compile-verification.json").read_text()
        )
        manifest = json.loads(
            (self.paths["artifacts"] / "native-manifest.json").read_text()
        )
        self.assertEqual(evidence["archive_sha256"], manifest["archive_sha256"])
        self.assertEqual(evidence["bolt_sha"], manifest["bolt_sha"])
        self.assertEqual(manifest["mode"], "compile")
        self.assertEqual(
            evidence["backend"],
            {
                "path": "cpp/build/package/bolt/linux/amd64/libbolt_backend.so",
                "size": 7,
                "sha256": hashlib.sha256(b"library").hexdigest(),
            },
        )
        downstream = self.root / "downstream"
        downstream.mkdir()
        identity = {
            key: manifest[key]
            for key in (
                "source_sha",
                "bolt_sha",
                "driver_sha",
                "helper_sha",
                "config_sha256",
            )
        }
        with self.assertRaisesRegex(ValueError, "compile-only"):
            run.restore_native(self.paths["artifacts"], downstream, identity)
        self.assertEqual(list(downstream.iterdir()), [])

    def test_compile_only_fails_with_empty_backend(self):
        script = self.fake_native_build(empty_backend=True)
        process, result = self.invoke("native", ["--compile-only"], script)
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertEqual(result["status"], "failure")
        self.assertNotIn("compile-verification.json", result["evidence"])
        self.assertFalse((self.paths["artifacts"] / "native-manifest.json").exists())

    def test_compile_only_fails_when_exported_and_consumed_packages_differ(self):
        script = self.fake_native_build(package_mismatch=True)
        process, result = self.invoke("native", ["--compile-only"], script)
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertIn("Bolt package mismatch", process.stderr)
        self.assertNotIn("compile-verification.json", result["evidence"])
        self.assertFalse((self.paths["artifacts"] / "native-manifest.json").exists())

    def test_default_native_still_requires_fresh_ctest_results(self):
        script = self.fake_native_build()
        process, result = self.invoke("native", script=script)
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertTrue((self.paths["reports"] / "ctest-invoked.txt").exists())
        self.assertEqual(result["status"], "failure")
        self.assertFalse((self.paths["artifacts"] / "native-manifest.json").exists())

    def test_compile_only_rejects_downstream_tasks_before_creating_reports(self):
        command = [
            sys.executable,
            str(HERE / "run.py"),
            "--task",
            "package:spark35",
            "--compile-only",
        ]
        for name, path in self.paths.items():
            command.extend([f"--{name}", str(path)])
        process = subprocess.run(command, env=self.env, capture_output=True, text=True)
        self.assertEqual(process.returncode, 2)
        self.assertIn("--compile-only requires --task native", process.stderr)
        self.assertFalse(self.paths["reports"].exists())

    def test_package_cli_verifies_and_preserves_the_packaged_jar(self):
        self.fake_maven_packages_jar()
        process, result = self.invoke()
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(
            result,
            {
                "schema": 1,
                "task": "package:spark35",
                "status": "success",
                "exit_code": 0,
                "tests": {"total": 0, "passed": 0, "failed": 0, "skipped": 0},
                "evidence": [
                    "gluten-bolt-bundle-fixture.jar",
                    "package-build.log",
                    "package-verification.txt",
                    "sources.json",
                ],
            },
        )
        self.assertTrue(
            (self.paths["reports"] / "gluten-bolt-bundle-fixture.jar").is_file()
        )

    def test_package_cli_rejects_libraries_rewritten_by_the_build(self):
        self.fake_maven_packages_jar('(libs / "libgluten.so").write_bytes(b"changed")')
        process, result = self.invoke()
        self.assertEqual(process.returncode, 1, process.stderr)
        self.assertEqual(result["status"], "failure")
        self.assertFalse(
            (self.paths["reports"] / "gluten-bolt-bundle-fixture.jar").exists()
        )

    def test_package_cli_preserves_build_failure_exit_code(self):
        self.executable(self.paths["source"] / "build/mvn", "#!/bin/bash\nexit 37\n")
        process, result = self.invoke()
        self.assertEqual(process.returncode, 37)
        self.assertEqual((result["status"], result["exit_code"]), ("failure", 37))
        self.assertIn("package-build.log", result["evidence"])

    def test_reports_from_a_previous_run_cannot_be_used(self):
        self.paths["reports"].mkdir()
        (self.paths["reports"] / "TEST-stale.xml").write_text(
            "<testsuite><testcase/></testsuite>"
        )
        self.fake_maven_packages_jar()
        process, result = self.invoke()
        self.assertEqual(process.returncode, 1, process.stderr)
        self.assertEqual(result["status"], "failure")
        self.assertFalse((self.paths["source"] / NATIVE_LIBRARIES[0]).exists())

    def test_default_driver_pin_rejects_mismatched_driver_before_sourcing_it(self):
        del self.env["GLUTEN_CI_DRIVER_SHA"]
        self.executable(
            self.paths["driver"] / ".github/scripts/gluten_ci.sh",
            'touch "$GLUTEN_REPORTS_DIR/driver-ran.txt"\nexit 37\n',
        )
        process, result = self.invoke("spark:3.5:spark-1")
        self.assertFalse((self.paths["reports"] / "driver-ran.txt").exists())
        self.assertFalse((self.paths["source"] / NATIVE_LIBRARIES[0]).exists())
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertEqual((result["status"], result["exit_code"]), ("failure", 1))
        self.assertIn("Bolt CI driver revision does not match", process.stderr)

    def test_driver_pin_must_be_a_full_hexadecimal_sha(self):
        self.env["GLUTEN_CI_DRIVER_SHA"] = "g" * 40
        process, result = self.invoke()
        self.assertFalse((self.paths["source"] / NATIVE_LIBRARIES[0]).exists())
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertEqual((result["status"], result["exit_code"]), ("failure", 1))
        self.assertIn("Invalid GLUTEN_CI_DRIVER_SHA", process.stderr)


if __name__ == "__main__":
    unittest.main()
