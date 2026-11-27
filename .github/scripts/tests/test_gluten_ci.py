# Copyright (c) ByteDance Ltd. and/or its affiliates.
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

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import zipfile

import yaml

SCRIPT = Path(__file__).resolve().parents[3] / "scripts/gluten-ci.sh"


class GlutenAdapterTest(unittest.TestCase):
    def invoke(self, function, *args):
        return subprocess.run(
            [
                "bash",
                "-euo",
                "pipefail",
                "-c",
                'source "$1"; shift; "$@"',
                "test",
                str(SCRIPT),
                function,
                *map(str, args),
            ],
            capture_output=True,
            text=True,
        )

    def test_reports_require_executed_passing_tests(self):
        for xml, expected in [
            ("<testsuite><testcase/></testsuite>", 0),
            ("<testsuite/>", 1),
            ("<testsuite><testcase><skipped/></testcase></testsuite>", 1),
            ("<testsuite><testcase><failure/></testcase></testsuite>", 1),
            ("<testsuite><testcase/><testcase><error/></testcase></testsuite>", 1),
            ("<testsuite>", 1),
            (None, 1),
        ]:
            with self.subTest(xml=xml), tempfile.TemporaryDirectory() as d:
                if xml is not None:
                    Path(d, "TEST-test.xml").write_text(xml)
                result = self.invoke("check_reports", d)
                self.assertEqual(result.returncode == 0, expected == 0, result.stderr)

    def test_consumed_bolt_package_must_match_exported_candidate(self):
        for field in (None, "ref", "package_id", "prev"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as d:
                package = dict(
                    name="bolt",
                    context="host",
                    ref="bolt/ci-" + "a" * 40 + "#recipe",
                    package_id="package",
                    prev="revision",
                )
                Path(d, "bolt-export.json").write_text(
                    json.dumps({"graph": {"nodes": {"0": package}}})
                )
                if field:
                    package[field] = "mismatch"
                Path(d, "gluten-install.json").write_text(
                    json.dumps({"graph": {"nodes": {"0": package}}})
                )
                result = self.invoke("check_bolt_package", d, "a" * 40)
                self.assertEqual(result.returncode == 0, field is None, result.stderr)


class WorkflowCommandsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for name in (
            "bolt/scripts",
            "gluten/backends-bolt/target/surefire-reports",
            "reports",
        ):
            (self.root / name).mkdir(parents=True)
        self.steps = yaml.load(
            (SCRIPT.parents[1] / ".github/workflows/gluten-ci.yaml").read_text(),
            Loader=yaml.BaseLoader,
        )["jobs"]["spark"]["steps"]
        # Replace expensive Maven/Java execution, keeping the workflow's selection,
        # native bundle verification and report validation commands intact.
        (self.root / "bolt/scripts/gluten-ci.sh").write_text(
            SCRIPT.read_text()
            + "\nspark_environment() { MVN=(fake_mvn); SPARK_HOME=/spark; }\n"
        )

    def run_step(self, name, stage, setup=""):
        script = next(s["run"] for s in self.steps if s.get("name") == name)
        return subprocess.run(
            ["bash", "-euo", "pipefail", "-c", setup + "\n" + script],
            cwd=self.root,
            env=os.environ
            | dict(
                GITHUB_WORKSPACE=str(self.root),
                GLUTEN_ROOT=str(self.root / "gluten"),
                REPORTS=str(self.root / "reports"),
                BOLT_SHA="a" * 40,
                STAGE=stage,
                SPARK_PROFILE="3.5",
                SCALA_VERSION="2.12",
                SELECTOR="UdfSuite",
            ),
            capture_output=True,
            text=True,
        )

    def test_package_keeps_verification_evidence_and_rejects_other_native_libraries(
        self,
    ):
        package = self.root / "gluten/cpp/build/package/bolt/linux/amd64"
        package.mkdir(parents=True)
        bundle = self.root / "gluten/package/target/bundle.jar"
        bundle.parent.mkdir(parents=True)
        bundle = bundle.with_name("gluten-bolt-bundle-test.jar")
        for wrong in (False, True):
            with self.subTest(wrong=wrong), zipfile.ZipFile(bundle, "w") as archive:
                archive.writestr(
                    "gluten-build-info.properties", "bolt_revision=" + "a" * 40 + "\n"
                )
                for lib in (
                    "libgluten.so",
                    "libbolt_backend.so",
                    "libglutenlibloader.so",
                ):
                    (package / lib).write_text(lib)
                    archive.writestr("linux/amd64/" + lib, "wrong" if wrong else lib)
            result = self.run_step(
                "Build with Apache Gluten Maven profiles",
                "package",
                'fake_mvn() { :; }; sha256sum() { shasum -a 256 "$@"; }',
            )
            self.assertEqual(result.returncode == 0, not wrong, result.stderr)
            if not wrong:
                self.assertTrue(
                    list((self.root / "reports").glob("*")),
                    "Package evidence is missing",
                )

    def test_jvm_loader_gets_library_path_and_udf_cannot_pass_on_unrelated_java_tests(
        self,
    ):
        setup = r"""
        java() {
          printf '%s\n' org.apache.gluten.jni.BoltNativeLoaderSuite org.apache.spark.sql.delta.DeltaSuite OrdinarySuite > "$4"
        }
        fake_mvn() {
          case "$*" in
            *dependency:build-classpath*) touch "$REPORTS/classpath.txt"; return;;
            *-Dsuites=org.apache.gluten.jni.BoltNativeLoaderSuite*)
              [[ "$*" == *"-Dbolt.native.loader.path=$GLUTEN_ROOT/cpp/build/package/bolt/linux/amd64/libglutenlibloader.so"* ]] || return 9;;
          esac
          local xml='<testsuite><testcase/></testsuite>'
          if [[ "$STAGE" == udf ]]; then
            xml='<testsuite><testcase><skipped/></testcase></testsuite>'
            if [[ " $* " == *' test '* ]]; then
              printf '%s\n' '<testsuite><testcase/></testsuite>' > backends-bolt/target/surefire-reports/TEST-java.xml
            fi
          fi
          printf '%s\n' "$xml" > backends-bolt/target/surefire-reports/TEST-selected.xml
        }
        """
        for stage in ("jvm", "udf"):
            with self.subTest(stage=stage):
                for report in (self.root / "gluten").rglob("TEST-*.xml"):
                    report.unlink()
                result = self.run_step(
                    "Run upstream Spark and backend test commands", stage, setup
                )
                self.assertEqual(result.returncode == 0, stage == "jvm", result.stderr)

    def test_both_git_checkouts_are_owned_by_the_maven_worker(self):
        prepare = next(
            s["run"]
            for s in self.steps
            if s.get("name", "").startswith("Prepare runtime")
        )
        ownership = "\n".join(
            line for line in prepare.splitlines() if line.strip().startswith("chown ")
        )
        result = subprocess.run(
            ["bash", "-eu", "-c", 'chown() { printf "%s\\n" "$@"; }; ' + ownership],
            env=os.environ
            | dict(
                root=str(self.root),
                GLUTEN_ROOT=str(self.root / "gluten"),
                REPORTS=str(self.root / "reports"),
            ),
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertIn(str(self.root / "bolt"), result.stdout.splitlines())
        self.assertIn(str(self.root / "gluten"), result.stdout.splitlines())


if __name__ == "__main__":
    unittest.main()
