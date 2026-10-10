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

import importlib.util
import hashlib
import json
from pathlib import Path
import shutil
import re
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile


SCRIPT = Path(__file__).resolve().parents[1] / "gluten_ci.py"


class GlutenCiTest(unittest.TestCase):
    def setUp(self):
        self.assertTrue(SCRIPT.is_file(), "The Gluten CI entry point is missing")
        spec = importlib.util.spec_from_file_location("gluten_ci", SCRIPT)
        self.ci = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.ci)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.ci.BLACKLIST = self.root / "empty-blacklist.txt"
        self.ci.BLACKLIST.write_text("# No exclusions in unrelated tests\n")

    def report(self, text):
        path = self.root / "TEST-suite.xml"
        path.write_text(text)
        return path

    def patch_fixture(self, content="OffloadDeltaScan()\n"):
        gluten = self.root / "gluten"
        gluten.mkdir()
        subprocess.run(["git", "init", "-q", str(gluten)], check=True)
        source = gluten / "component.scala"
        source.write_text(content)
        subprocess.run(["git", "-C", str(gluten), "add", "component.scala"], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(gluten),
                "-c",
                "user.name=CI test",
                "-c",
                "user.email=ci-test@example.invalid",
                "commit",
                "-qm",
                "fixture",
            ],
            check=True,
        )
        self.ci.PATCHES = self.root / "patches"
        self.ci.PATCHES.mkdir()
        patch_file = self.ci.PATCHES / "delta-api.patch"
        patch_file.write_text(
            "diff --git a/component.scala b/component.scala\n"
            "--- a/component.scala\n+++ b/component.scala\n@@ -1 +1 @@\n"
            "-OffloadDeltaScan()\n+OffloadDeltaScan(enableNativeDmlRowIndexScan = false)\n"
        )
        inputs = {
            "gluten_sha": self.ci.git(gluten, "rev-parse", "HEAD"),
            "gluten_patch_sha256": {
                patch_file.name: hashlib.sha256(patch_file.read_bytes()).hexdigest()
            },
        }
        (self.root / "inputs.json").write_text(json.dumps(inputs))
        return gluten, source, patch_file, inputs

    def test_declared_gluten_patch_is_applied_and_verified_idempotently(self):
        gluten, source, patch_file, inputs = self.patch_fixture()
        self.ci.apply_gluten_patches(gluten, self.root)
        self.assertEqual(
            source.read_text(),
            "OffloadDeltaScan(enableNativeDmlRowIndexScan = false)\n",
        )
        self.ci.verify_gluten_source(gluten, inputs)
        self.ci.apply_gluten_patches(gluten, self.root)
        evidence = json.loads((self.root / "gluten-patches.json").read_text())
        self.assertEqual(evidence["patches"][patch_file.name]["status"], "applied")

    def test_gluten_patch_already_in_main_is_recorded_without_source_changes(self):
        gluten, source, patch_file, inputs = self.patch_fixture(
            "OffloadDeltaScan(enableNativeDmlRowIndexScan = false)\n"
        )
        self.ci.apply_gluten_patches(gluten, self.root)
        self.assertEqual(self.ci.git(gluten, "diff", "HEAD"), "")
        self.ci.verify_gluten_source(gluten, inputs)
        evidence = json.loads((self.root / "gluten-patches.json").read_text())
        self.assertEqual(
            evidence["patches"][patch_file.name]["status"], "already-upstream"
        )

    def test_declared_gluten_patch_can_add_required_test_sources(self):
        gluten, source, patch_file, inputs = self.patch_fixture()
        patch_file.write_text(
            patch_file.read_text()
            + "diff --git a/SmokeSuite.scala b/SmokeSuite.scala\n"
            "new file mode 100644\n--- /dev/null\n+++ b/SmokeSuite.scala\n"
            "@@ -0,0 +1 @@\n+class SmokeSuite\n"
        )
        inputs["gluten_patch_sha256"][patch_file.name] = hashlib.sha256(
            patch_file.read_bytes()
        ).hexdigest()
        (self.root / "inputs.json").write_text(json.dumps(inputs))
        self.ci.apply_gluten_patches(gluten, self.root)
        self.assertEqual(
            (gluten / "SmokeSuite.scala").read_text(), "class SmokeSuite\n"
        )
        self.ci.verify_gluten_source(gluten, inputs)

    def test_gluten_patching_rejects_unrelated_or_changed_source(self):
        gluten, source, patch_file, inputs = self.patch_fixture()
        source.write_text(source.read_text() + "unrelated change\n")
        with self.assertRaisesRegex(ValueError, "Unrecognized Gluten source changes"):
            self.ci.apply_gluten_patches(gluten, self.root)
        source.write_text("OffloadDeltaScan()\n")
        self.ci.apply_gluten_patches(gluten, self.root)
        source.write_text(source.read_text() + "unrelated change\n")
        with self.assertRaisesRegex(ValueError, "declared patches"):
            self.ci.verify_gluten_source(gluten, inputs)

    def test_changed_patch_bundle_and_unapplicable_patch_are_fatal(self):
        gluten, source, patch_file, inputs = self.patch_fixture()
        patch_file.write_text(patch_file.read_text() + "\n")
        with self.assertRaisesRegex(ValueError, "Patch bundle differs"):
            self.ci.apply_gluten_patches(gluten, self.root)
        patch_file.write_text(patch_file.read_text().rstrip() + "\n")
        source.write_text("different API\n")
        subprocess.run(["git", "-C", str(gluten), "add", "component.scala"], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(gluten),
                "-c",
                "user.name=CI test",
                "-c",
                "user.email=ci-test@example.invalid",
                "commit",
                "-qm",
                "new API",
            ],
            check=True,
        )
        inputs["gluten_sha"] = self.ci.git(gluten, "rev-parse", "HEAD")
        (self.root / "inputs.json").write_text(json.dumps(inputs))
        with self.assertRaisesRegex(
            ValueError, "neither applies nor is already upstream"
        ):
            self.ci.apply_gluten_patches(gluten, self.root)

    def test_spark_test_home_preserves_the_runtime_distribution(self):
        source = self.root / "spark-source"
        binary = self.root / "spark-binary"
        (source / "sql").mkdir(parents=True)
        (binary / "jars").mkdir(parents=True)
        (binary / "sql").symlink_to(source / "sql", target_is_directory=True)
        alias = self.root / "spark-home"
        alias.symlink_to(binary, target_is_directory=True)
        for home in (binary, alias):
            test_home = self.ci.spark_test_home(home)
            self.assertEqual(test_home, binary)
            self.assertTrue((test_home / "jars").is_dir())
            self.assertEqual((test_home / "sql").resolve(), source / "sql")

    def test_gate_requires_the_supported_version_compilation_matrix(self):
        for result in (None, "failure", "skipped", "cancelled"):
            needs = {job: {"result": "success"} for job in ("prepare", "integration")}
            if result is not None:
                needs["compatibility"] = {"result": result}
            with (
                self.subTest(result=result),
                patch.dict("os.environ", {"GLUTEN_CI_NEEDS": json.dumps(needs)}),
            ):
                with self.assertRaisesRegex(ValueError, "compatibility"):
                    self.ci.gate()

    def test_stages_cannot_share_a_mutable_module_report_directory(self):
        with self.ci.stage_lock(self.root, "spark-1"):
            with self.assertRaisesRegex(ValueError, "already running"):
                with self.ci.stage_lock(self.root, "slow-hive"):
                    self.fail("Concurrent Spark UT stages acquired the same module")
            with self.ci.stage_lock(self.root, "jvm"):
                pass
        with self.ci.stage_lock(self.root, "slow-hive"):
            pass

    def test_stage_lock_is_released_after_a_failure(self):
        with self.assertRaisesRegex(RuntimeError, "interrupted"):
            with self.ci.stage_lock(self.root, "spark-2"):
                raise RuntimeError("interrupted")
        with self.ci.stage_lock(self.root, "extended"):
            pass

    def test_spark4_profile_changes_exclude_other_java_stages(self):
        with self.ci.stage_lock(self.root, "jvm"):
            with self.assertRaisesRegex(ValueError, "already running"):
                with self.ci.stage_lock(self.root, "spark40"):
                    self.fail("Profile changed during another Java stage")
        with self.ci.stage_lock(self.root, "spark41"):
            for stage in ("jvm", "spark-2", "tpc-h", "spark40"):
                with (
                    self.subTest(stage=stage),
                    self.assertRaisesRegex(ValueError, "already running"),
                ):
                    with self.ci.stage_lock(self.root, stage):
                        self.fail("Java stage overlapped a profile change")

    def test_spark4_stages_use_the_requested_profile(self):
        for module, profile in (("spark40", "4.0"), ("spark41", "4.1")):
            self.assertIn(module, self.ci.STAGES)
            command = self.ci.maven(self.root, profile)
            self.assertIn(f"-Pspark-{profile}", command)
            self.assertNotIn("-Pspark-3.5", command)

    def test_spark4_packaging_rejects_stale_native_resources(self):
        reports = self.root / "reports"
        reports.mkdir()
        classes = self.root / "backends-bolt/target/scala-2.13/classes/linux/amd64"
        classes.mkdir(parents=True)
        (classes / "libbolt_backend.so").write_bytes(b"old candidate")
        (reports / "native-libraries.json").write_text(
            json.dumps({"libbolt_backend.so": "0" * 64})
        )
        with self.assertRaisesRegex(ValueError, "different native"):
            self.ci.verify_spark4_native(self.root, reports)

    def test_scalatest_fork_exit_must_agree_with_classified_failures(self):
        forks = self.root / "forks"
        forks.mkdir()
        record = forks / "scalatest-fork-1.json"
        for code, failures, accepted in (
            (0, [], True),
            (1, ["suite#expected"], True),
            (137, [], False),
            (-6, [], False),
            (1, [], False),
            (0, ["suite#expected"], False),
        ):
            record.write_text(json.dumps({"exit_code": code}))
            with self.subTest(code=code, failures=failures):
                if accepted:
                    self.ci.verify_scalatest_exit(forks, failures)
                else:
                    with self.assertRaisesRegex(ValueError, "ScalaTest.*exit"):
                        self.ci.verify_scalatest_exit(forks, failures)

    def test_scalatest_exit_requires_exactly_one_fresh_fork_record(self):
        forks = self.root / "forks"
        forks.mkdir()
        with self.assertRaisesRegex(ValueError, "exactly one"):
            self.ci.verify_scalatest_exit(forks, [])
        for pid in (1, 2):
            (forks / f"scalatest-fork-{pid}.json").write_text('{"exit_code": 0}')
        with self.assertRaisesRegex(ValueError, "exactly one"):
            self.ci.verify_scalatest_exit(forks, [])

    def test_jvm_wrapper_records_the_actual_runner_exit(self):
        java = self.root / "bin/java"
        java.parent.mkdir()
        java.write_text(
            "#!/usr/bin/env python3\nimport sys\nsys.exit(int(sys.argv[-1]))\n"
        )
        java.chmod(0o755)
        forks = self.root / "forks"
        with patch.dict("os.environ", {"JAVA_HOME": str(self.root)}):
            wrapper = self.ci.scalatest_jvm(forks)
        result = subprocess.run([str(wrapper), "org.scalatest.tools.Runner", "137"])
        self.assertEqual(result.returncode, 137)
        records = list(forks.glob("scalatest-fork-*.json"))
        self.assertEqual(len(records), 1)
        self.assertEqual(json.loads(records[0].read_text()), {"exit_code": 137})
        with self.assertRaisesRegex(ValueError, "ScalaTest.*exit"):
            self.ci.verify_scalatest_exit(forks, [])

    def test_prepare_rejects_a_matrix_the_commands_do_not_implement(self):
        config = json.loads(self.ci.CONFIG.read_text())
        config["jdk"] = "21"
        path = self.root / "versions.json"
        path.write_text(json.dumps(config))
        self.ci.CONFIG = path
        with self.assertRaisesRegex(ValueError, "Unsupported.*matrix"):
            self.ci.prepare(self.root)

    def test_local_prepare_records_host_without_an_image(self):
        with patch.dict("os.environ", {}, clear=True):
            self.ci.prepare(self.root, local=True, gluten=self.ci.ROOT)
        inputs = json.loads((self.root / "inputs.json").read_text())
        self.assertIsNone(inputs["image"])
        self.assertEqual(inputs["execution_environment"], "local-host")
        self.assertEqual(inputs["runner"], ["local"])
        self.assertEqual(
            inputs["bolt_tested_sha"], self.ci.git(self.ci.ROOT, "rev-parse", "HEAD")
        )

    def test_actions_cannot_bypass_image_validation_with_local_mode(self):
        with patch.dict(
            "os.environ",
            {"GITHUB_ACTIONS": "true", "BOLT_GLUTEN_CI_IMAGE": "invalid-image"},
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "Local mode.*Actions"):
                self.ci.prepare(self.root, local=True)
            with self.assertRaisesRegex(ValueError, "BOLT_GLUTEN_CI_IMAGE"):
                self.ci.prepare(self.root)

    def test_prepare_reuses_the_bolt_ci_image_without_repository_variables(self):
        actual_output = subprocess.check_output

        def checkout_output(args, **kwargs):
            if args[:2] == ["git", "ls-remote"]:
                return "a" * 40 + "\trefs/heads/main\n"
            return actual_output(args, **kwargs)

        output = self.root / "actions-output"
        for image_variables in (
            {},
            {"BOLT_GLUTEN_CI_IMAGE": "", "BOLT_GLUTEN_CI_CONAN_IMAGE": ""},
            {"BOLT_GLUTEN_CI_CONAN_IMAGE": "registry.example/conan@sha256:" + "b" * 64},
        ):
            with (
                self.subTest(variables=image_variables),
                patch.dict(
                    "os.environ",
                    {
                        "GITHUB_ACTIONS": "true",
                        "GITHUB_OUTPUT": str(output),
                        "BOLT_GLUTEN_CI_RUNNER": "",
                        **image_variables,
                    },
                    clear=True,
                ),
                patch.object(subprocess, "check_output", side_effect=checkout_output),
            ):
                self.ci.prepare(self.root)
                inputs = json.loads((self.root / "inputs.json").read_text())
                self.assertEqual(inputs["image"], "bolt-registry:5000/bolt-ci:20260114")
                self.assertEqual(inputs["runner"], ["self-hosted", "medium"])
                self.assertEqual(inputs["execution_environment"], "container")
                self.assertEqual(
                    inputs["conan_image"],
                    image_variables.get("BOLT_GLUTEN_CI_CONAN_IMAGE")
                    or "bolt-registry:5000/conan-server:latest",
                )
                self.assertIn("image=" + inputs["image"] + "\n", output.read_text())
                self.assertIn(
                    "conan_image=" + inputs["conan_image"] + "\n", output.read_text()
                )

    def test_conan_image_overrides_require_a_digest(self):
        for image in (
            "registry.example/conan:latest",
            "registry.example/conan@sha256:bad",
        ):
            with (
                self.subTest(image=image),
                patch.dict(
                    "os.environ", {"BOLT_GLUTEN_CI_CONAN_IMAGE": image}, clear=True
                ),
                patch.object(
                    subprocess,
                    "check_output",
                    return_value="a" * 40 + "\trefs/heads/main\n",
                ),
            ):
                with self.assertRaisesRegex(ValueError, "BOLT_GLUTEN_CI_CONAN_IMAGE"):
                    self.ci.prepare(self.root)

    def test_local_prepare_records_the_checked_out_gluten_source(self):
        with (
            patch.dict("os.environ", {}, clear=True),
            patch.object(self.ci, "git", return_value="a" * 40),
        ):
            self.ci.prepare(self.root, local=True, gluten=self.root)
        inputs = json.loads((self.root / "inputs.json").read_text())
        self.assertEqual(inputs["gluten_sha"], "a" * 40)

    def test_prepare_rejects_sources_other_than_apache_gluten_main(self):
        config = json.loads(self.ci.CONFIG.read_text())
        path = self.root / "versions.json"
        self.ci.CONFIG = path
        for repository, branch in (
            ("example/gluten", "main"),
            ("apache/gluten", "ci/bolt"),
            ("apache/gluten", "a" * 40),
        ):
            config.update(gluten_repository=repository, gluten_ref=branch)
            path.write_text(json.dumps(config))
            with (
                self.subTest(repository=repository, branch=branch),
                patch.dict("os.environ", {}, clear=True),
                self.assertRaisesRegex(ValueError, "apache/gluten:main"),
            ):
                self.ci.prepare(self.root, local=True, gluten=self.ci.ROOT)

    def test_prepare_resolves_apache_gluten_main_once(self):
        config = json.loads(self.ci.CONFIG.read_text())
        config.pop("gluten_sha", None)
        config.update(gluten_repository="apache/gluten", gluten_ref="main")
        path = self.root / "versions.json"
        path.write_text(json.dumps(config))
        self.ci.CONFIG = path
        image = "registry.example/ci@sha256:" + "b" * 64
        actual_output = subprocess.check_output
        remote_queries = []

        def checkout_output(args, **kwargs):
            if args[:2] == ["git", "ls-remote"]:
                remote_queries.append(args)
                return "a" * 40 + "\trefs/heads/main\n"
            return actual_output(args, **kwargs)

        with (
            patch.dict("os.environ", {"BOLT_GLUTEN_CI_IMAGE": image}, clear=True),
            patch.object(subprocess, "check_output", side_effect=checkout_output),
        ):
            self.ci.prepare(self.root)
        inputs = json.loads((self.root / "inputs.json").read_text())
        self.assertEqual(inputs["gluten_repository"], "apache/gluten")
        self.assertEqual(inputs["gluten_ref"], "main")
        self.assertEqual(inputs["gluten_sha"], "a" * 40)
        self.assertEqual(inputs["image"], image)
        self.assertEqual(
            remote_queries,
            [
                [
                    "git",
                    "ls-remote",
                    "--exit-code",
                    "https://github.com/apache/gluten.git",
                    "refs/heads/main",
                ]
            ],
        )

    def test_prepare_rejects_an_unresolved_or_wrong_remote_branch(self):
        config = json.loads(self.ci.CONFIG.read_text())
        config.pop("gluten_sha", None)
        config.update(gluten_repository="apache/gluten", gluten_ref="main")
        path = self.root / "versions.json"
        path.write_text(json.dumps(config))
        self.ci.CONFIG = path
        actual_output = subprocess.check_output
        for remote in ("", "a" * 40 + "\trefs/heads/other\n"):

            def checkout_output(args, **kwargs):
                return (
                    remote
                    if args[:2] == ["git", "ls-remote"]
                    else actual_output(args, **kwargs)
                )

            with (
                self.subTest(remote=remote),
                patch.dict(
                    "os.environ",
                    {"BOLT_GLUTEN_CI_IMAGE": "ci@sha256:" + "b" * 64},
                    clear=True,
                ),
                patch.object(subprocess, "check_output", side_effect=checkout_output),
                self.assertRaisesRegex(ValueError, "resolve.*branch"),
            ):
                self.ci.prepare(self.root)

    def test_reports_reject_empty_failed_and_all_skipped_runs(self):
        for text in (
            '<testsuite tests="0"/>',
            '<testsuite tests="2" skipped="2"/>',
            '<testsuite tests="2" failures="1"/>',
            '<testsuite tests="2" errors="1"/>',
            '<testsuite tests="2" skipped="-1"/>',
            '<testsuite tests="1"><testcase name="ignored"><skipped/></testcase></testsuite>',
            '<testsuite tests="1"><testcase name="bad"><failure/></testcase></testsuite>',
            '<testsuite tests="1"><testcase name="bad"><error/></testcase></testsuite>',
        ):
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.ci.verify_reports([self.report(text)])
        with self.assertRaises(ValueError):
            self.ci.verify_reports([])

    def test_reports_count_leaf_suites_once(self):
        report = self.report(
            '<testsuites tests="5"><testsuite tests="3" skipped="1">'
            "<testcase><skipped/></testcase><testcase/><testcase/></testsuite>"
            '<testsuite tests="2"><testcase/><testcase/></testsuite></testsuites>'
        )
        self.assertEqual(self.ci.verify_reports([report]), {"tests": 5, "skipped": 1})
        report = self.report(
            '<testsuite tests="2"><testcase name="ignored"><skipped/></testcase>'
            '<testcase name="executed"/></testsuite>'
        )
        self.assertEqual(self.ci.verify_reports([report]), {"tests": 2, "skipped": 1})

    def test_blacklist_snapshots_exact_entries_without_gluten_support(self):
        path = self.root / "blacklist.txt"
        path.write_text("# Known failures\norg.example.Suite#test # suffix\n")
        self.ci.BLACKLIST = path
        entries = self.ci.snapshot_blacklist(self.root / "reports")
        self.assertEqual(entries, {("org.example.Suite", "test # suffix")})
        evidence = json.loads((self.root / "reports/blacklist.json").read_text())
        self.assertEqual(evidence["entries"], 1)
        self.assertEqual(
            (self.root / "reports/blacklist.txt").read_text(), path.read_text()
        )
        for value in (
            "org.example.Suite",
            "org.example.Suite#IGNORE_ALL",
            "org.example.*#test",
            "org.example.Suite#(aborted)",
            "org.example.Suite#test\norg.example.Suite#test",
        ):
            path.write_text(value)
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.ci.snapshot_blacklist(self.root / "invalid")
        path.write_text("org.example.Suite#another test\n")
        with self.assertRaisesRegex(ValueError, "changed between test stages"):
            self.ci.snapshot_blacklist(self.root / "reports")
        self.ci.write_json(
            self.root / "prepared/inputs.json", {"blacklist_sha256": "old"}
        )
        with self.assertRaisesRegex(ValueError, "changed after preparation"):
            self.ci.snapshot_blacklist(self.root / "prepared")

    def test_reports_classify_exact_expected_failures_without_rewriting_xml(self):
        xml = (
            '<testsuite name="org.example.Suite" tests="3" failures="1">'
            '<testcase name="known"><failure>assertion</failure></testcase>'
            '<testcase name="passing"/>'
            '<testcase name="ignored"><skipped/></testcase></testsuite>'
        )
        report = self.report(xml)
        result = self.ci.verify_reports(
            [report], blacklist={("org.example.Suite", "known")}
        )
        self.assertEqual(result["passed"], 1)
        self.assertEqual(result["expected_failures"], ["org.example.Suite#known"])
        self.assertEqual(report.read_text(), xml)
        for entries in (set(), {("org.example.OtherSuite", "known")}):
            with self.subTest(entries=entries), self.assertRaises(ValueError):
                self.ci.verify_reports([report], blacklist=entries)

    def test_blacklist_cannot_hide_errors_unidentified_failures_or_no_passes(self):
        for xml in (
            '<testsuite name="org.example.Suite" tests="2" errors="1">'
            '<testcase name="known"><error/></testcase><testcase name="good"/></testsuite>',
            '<testsuite name="org.example.Suite" tests="2" failures="1"/>',
            '<testsuite name="org.example.Suite" tests="1" failures="1">'
            '<testcase name="known"><failure/></testcase></testsuite>',
        ):
            with self.subTest(xml=xml), self.assertRaises(ValueError):
                self.ci.verify_reports(
                    [self.report(xml)], blacklist={("org.example.Suite", "known")}
                )

    def test_scalatest_requires_completion_even_when_partial_xml_passes(self):
        report = self.report(
            '<testsuite tests="1"><testcase name="passing"/></testsuite>'
        )
        summary = (
            "Run completed in 1 second.\nTotal number of tests run: 1\n"
            "Suites: completed 1, aborted 0\n"
            "Tests: succeeded 1, failed 0, canceled 0, ignored 0, pending 0\n"
        )
        for log in (
            "Starting suite...\nJVM exited with code 137\n",
            summary.replace("Run completed", "Run aborted"),
            summary.replace("aborted 0", "aborted 1"),
            summary + summary,
            summary.replace("succeeded 1", "succeeded 2"),
        ):
            with self.subTest(log=log), self.assertRaises(ValueError):
                self.ci.verify_scalatest_run(log, [report])
        self.ci.verify_scalatest_run(summary, [report])

    def test_xml_counts_cannot_invent_a_passing_test(self):
        for xml in (
            '<testsuite tests="1"/>',
            '<testsuite tests="2"><testcase><skipped/></testcase></testsuite>',
            '<testsuite tests="3" failures="1"><testcase name="known"><failure/></testcase>'
            '<testcase name="good"/></testsuite>',
        ):
            with self.subTest(xml=xml), self.assertRaises(ValueError):
                self.ci.verify_reports(
                    [self.report(xml)], blacklist={("org.example.Suite", "known")}
                )

    def test_junit_passes_cannot_hide_all_skipped_scalatest(self):
        junit = self.root / "TEST-junit.xml"
        junit.write_text(
            '<testsuite xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            'xsi:noNamespaceSchemaLocation="surefire-test-report-3.0.xsd" tests="1">'
            "<testcase/></testsuite>"
        )
        scala = self.report(
            '<testsuite tests="1"><testcase><skipped/></testcase></testsuite>'
        )
        log = (
            "Run completed in 1 second.\nTotal number of tests run: 0\n"
            "Suites: completed 1, aborted 0\n"
            "Tests: succeeded 0, failed 0, canceled 0, ignored 1, pending 0\n"
        )
        self.ci.verify_reports([junit, scala], blacklist=set())
        with self.assertRaises(ValueError):
            self.ci.verify_scalatest_run(log, [junit, scala])
        scala.write_text('<testsuite tests="1"><testcase/></testsuite>')
        self.ci.verify_scalatest_run(
            log.replace("tests run: 0", "tests run: 1")
            .replace("succeeded 0", "succeeded 1")
            .replace("ignored 1", "ignored 0"),
            [junit, scala],
        )

    def test_backend_jvm_explicitly_excludes_skipped_suites(self):
        activation = self.root / "cpp/build/Release/generators/conanrun.sh"
        activation.parent.mkdir(parents=True)
        activation.touch()

        def record_report(*args, **kwargs):
            report = (
                self.root / "backends-bolt/target/surefire-reports/TEST-backend.xml"
            )
            report.parent.mkdir(parents=True, exist_ok=True)
            suite = next(
                arg.removeprefix("-Dsuites=")
                for arg in args[0]
                if arg.startswith("-Dsuites=")
            )
            report.write_text(
                f'<testsuite name="{suite}" tests="1"><testcase/></testsuite>'
            )
            args[2].write_text(
                "Run completed in 1 second.\nTotal number of tests run: 1\n"
                "Suites: completed 1, aborted 0\n"
                "Tests: succeeded 1, failed 0, canceled 0, ignored 0, pending 0\n"
            )
            fork = next(
                arg.removeprefix("-Djvm=")
                for arg in args[0]
                if arg.startswith("-Djvm=")
            )
            (Path(fork).parent.parent / "scalatest-fork-1.json").write_text(
                '{"exit_code": 0}'
            )

        with (
            patch.object(
                self.ci,
                "discover_backend_suites",
                return_value=["org.example.FutureSuite"]
                + [f"org.apache.spark.sql.delta.Delete{i}Suite" for i in range(5)],
            ),
            patch.object(
                self.ci, "spark_environment", side_effect=lambda home, env: env
            ),
            patch.object(self.ci, "run", side_effect=record_report) as command,
        ):
            self.ci.test_stage("jvm", self.root, self.root / "reports")
        arguments = command.call_args.args[0]
        excluded = next(
            (
                arg.split("=", 1)[1]
                for arg in arguments
                if arg.startswith("-DtagsToExclude=")
            ),
            "",
        ).split(",")
        self.assertIn("org.apache.gluten.tags.SkipTest", excluded)
        self.assertIn("org.apache.gluten.tags.UDFTest", excluded)
        self.assertIn("org.apache.gluten.tags.EnhancedFeaturesTest", excluded)
        self.assertIn("org.apache.gluten.tags.CudfTest", excluded)
        self.assertNotIn("-am", arguments)
        self.assertEqual(command.call_count, 6)
        ordinary = command.call_args_list[0].args[0]
        self.assertIn("-Dsuites=org.example.FutureSuite", ordinary)
        for call in command.call_args_list[1:]:
            self.assertIn("scalatest:test", call.args[0])
            self.assertTrue(any(arg.startswith("-Dsuites=") for arg in call.args[0]))
        evidence = json.loads((self.root / "reports/blacklist-jvm.json").read_text())
        self.assertEqual(evidence["tests"], 6)
        self.assertEqual(len(evidence["groups"]), 6)

    @unittest.skipUnless(shutil.which("javac") and shutil.which("java"), "Requires JDK")
    def test_discovery_preserves_unusual_packages_without_initializing_suites(self):
        version = subprocess.check_output(
            ["java", "-version"], stderr=subprocess.STDOUT
        ).decode()
        if int(re.search(r'version "(?:1\.)?(\d+)', version)[1]) < 17:
            self.skipTest("Java source-mode discovery requires JDK 17")
        sources = {
            "org/scalatest/Suite.java": "package org.scalatest; public abstract class Suite {}",
            "org/scalatest/DoNotDiscover.java": (
                "package org.scalatest; import java.lang.annotation.*; "
                "@Retention(RetentionPolicy.RUNTIME) public @interface DoNotDiscover {}"
            ),
            "org/scalatest/WrapWith.java": (
                "package org.scalatest; import java.lang.annotation.*; "
                "@Inherited @Retention(RetentionPolicy.RUNTIME) public @interface WrapWith "
                "{ Class<? extends Suite> value(); }"
            ),
            "future/FutureSuite.java": (
                "package future; public class FutureSuite extends org.scalatest.Suite {"
                'static { if(System.nanoTime()>0) throw new RuntimeException("initialized"); } }'
            ),
            "future/HiddenSuite.java": (
                "package future; @org.scalatest.DoNotDiscover "
                "public class HiddenSuite extends org.scalatest.Suite {}"
            ),
            "future/AbstractSuite.java": (
                "package future; public abstract class AbstractSuite extends org.scalatest.Suite {}"
            ),
            "future/PrivateSuite.java": (
                "package future; public class PrivateSuite extends org.scalatest.Suite "
                "{ private PrivateSuite() {} }"
            ),
            "future/Wrapper.java": (
                "package future; public class Wrapper extends org.scalatest.Suite "
                "{ public Wrapper(Class<?> type) {} }"
            ),
            "future/WrappedSuite.java": (
                "package future; @org.scalatest.WrapWith(future.Wrapper.class) "
                "public class WrappedSuite {}"
            ),
            "future/InheritedWrappedSuite.java": (
                "package future; public class InheritedWrappedSuite extends WrappedSuite {}"
            ),
            "future/VisibleSuite.java": (
                "package future; public class VisibleSuite extends HiddenSuite {}"
            ),
            "future/AbstractWrappedSuite.java": (
                "package future; @org.scalatest.WrapWith(future.Wrapper.class) "
                "abstract class AbstractWrappedSuite {}"
            ),
        }
        paths = []
        for name, code in sources.items():
            path = self.root / "sources" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(code)
            paths.append(str(path))
        classes = self.root / "classes"
        classes.mkdir()
        subprocess.run(
            ["javac", "-d", str(classes), *paths], check=True, capture_output=True
        )
        names = self.root / "suites.txt"
        subprocess.run(
            [
                "java",
                "--class-path",
                str(classes),
                str(SCRIPT.parent / "DiscoverGlutenSuites.java"),
                str(names),
                str(classes),
                str(classes),
            ],
            check=True,
            capture_output=True,
        )
        self.assertEqual(
            names.read_text().splitlines(),
            [
                "future.AbstractWrappedSuite",
                "future.FutureSuite",
                "future.InheritedWrappedSuite",
                "future.VisibleSuite",
                "future.WrappedSuite",
            ],
        )

    def test_backend_group_abort_is_fatal_but_other_groups_still_run(self):
        self.check_backend_group_failure(
            '<testsuite name="{suite}" tests="0" errors="1"/>'
        )

    def test_truncated_backend_report_is_fatal_but_delta_groups_still_run(self):
        self.check_backend_group_failure('<testsuite name="{suite}"><testcase')

    def check_backend_group_failure(self, first_report):
        activation = self.root / "cpp/build/Release/generators/conanrun.sh"
        activation.parent.mkdir(parents=True)
        activation.touch()
        calls = []

        def reports(args, cwd, log, env):
            suite = next(
                a.removeprefix("-Dsuites=") for a in args if a.startswith("-Dsuites=")
            )
            calls.append(suite)
            report = self.root / "backends-bolt/target/surefire-reports/TEST-suite.xml"
            report.parent.mkdir(parents=True, exist_ok=True)
            if len(calls) == 1:
                report.write_text(first_report.format(suite=suite))
                log.write_text("Run aborted.\n")
            else:
                report.write_text(
                    f'<testsuite name="{suite}" tests="1"><testcase/></testsuite>'
                )
                log.write_text(
                    "Run completed in 1 second.\nTotal number of tests run: 1\n"
                    "Suites: completed 1, aborted 0\n"
                    "Tests: succeeded 1, failed 0, canceled 0, ignored 0, pending 0\n"
                )
            fork = next(
                a.removeprefix("-Djvm=") for a in args if a.startswith("-Djvm=")
            )
            (Path(fork).parent.parent / "scalatest-fork-1.json").write_text(
                '{"exit_code":0}'
            )

        with (
            patch.object(
                self.ci,
                "discover_backend_suites",
                return_value=[
                    "org.example.FutureSuite",
                    "org.apache.spark.sql.delta.DeleteSuite",
                ],
            ),
            patch.object(
                self.ci, "spark_environment", side_effect=lambda home, env: env
            ),
            patch.object(self.ci, "run", side_effect=reports),
            self.assertRaisesRegex(ValueError, "Backend test groups failed"),
        ):
            self.ci.test_stage("jvm", self.root, self.root / "reports")
        self.assertEqual(
            calls, ["org.example.FutureSuite", "org.apache.spark.sql.delta.DeleteSuite"]
        )
        evidence = json.loads((self.root / "reports/blacklist-jvm.json").read_text())
        self.assertEqual(evidence["groups"]["backend"]["result"], "failure")
        self.assertEqual(evidence["groups"][calls[1]]["result"], "success")
        self.assertEqual(evidence["tests"], 1)

    def test_missing_stage_metadata_is_fatal_but_later_stages_still_run(self):
        calls = []

        def stage(name, gluten, reports):
            calls.append(name)
            if len(calls) == 1:
                raise KeyError("JAVA_HOME")
            return {"tests": 1}

        with patch.object(self.ci, "test_stage", side_effect=stage):
            with self.assertRaisesRegex(
                ValueError, "Required Gluten backend tests failed"
            ):
                self.ci.test_all(self.root, self.root)
        self.assertEqual(calls, list(self.ci.STAGES))
        results = json.loads((self.root / "results.json").read_text())
        self.assertEqual(results[calls[0]]["result"], "failure")
        self.assertTrue(all(results[name]["result"] == "success" for name in calls[1:]))

    def test_backend_discovery_uses_java_from_the_supplied_path(self):
        activation = self.root / "cpp/build/Release/generators/conanrun.sh"
        activation.parent.mkdir(parents=True)
        activation.touch()
        java = self.root / "jdk/bin/java"
        java.parent.mkdir(parents=True)
        java.touch()
        java.chmod(0o755)
        output = self.root / "reports"
        output.mkdir()
        classes = self.root / "backends-bolt/target/scala-2.12/test-classes"
        classes.mkdir(parents=True)
        (classes.parent / "classes").mkdir()
        commands = []

        def run(args, cwd, log, env):
            commands.append(args)
            if "dependency:build-classpath" in args:
                (output / "test-classpath.txt").write_text("fixture.jar")
            if "--class-path" in args:
                Path(args[-3]).write_text("org.example.FutureSuite\n")

        with patch.object(self.ci, "run", side_effect=run):
            suites = self.ci.discover_backend_suites(
                self.root, output, {"PATH": str(java.parent)}
            )
        self.assertEqual(suites, ["org.example.FutureSuite"])
        self.assertEqual(commands[-1][0], str(java.resolve()))

    def test_spark_stages_run_only_the_required_test_module(self):
        activation = self.root / "cpp/build/Release/generators/conanrun.sh"
        activation.parent.mkdir(parents=True)
        activation.touch()

        def record_report(*args, **kwargs):
            report = (
                self.root / "gluten-ut/spark35/target/surefire-reports/TEST-spark.xml"
            )
            report.parent.mkdir(parents=True, exist_ok=True)
            report.write_text('<testsuite tests="1"><testcase/></testsuite>')
            args[2].write_text(
                "Run completed in 1 second.\nTotal number of tests run: 1\n"
                "Suites: completed 1, aborted 0\n"
                "Tests: succeeded 1, failed 0, canceled 0, ignored 0, pending 0\n"
            )
            fork = next(
                arg.removeprefix("-Djvm=")
                for arg in args[0]
                if arg.startswith("-Djvm=")
            )
            (Path(fork).parent.parent / "scalatest-fork-1.json").write_text(
                '{"exit_code": 0}'
            )

        for stage in ("spark-1", "spark-2", "spark-3", "extended", "slow-hive"):
            with (
                self.subTest(stage=stage),
                patch.object(
                    self.ci, "spark_environment", side_effect=lambda home, env: env
                ),
                patch.object(self.ci, "run", side_effect=record_report) as command,
            ):
                self.ci.test_stage(stage, self.root, self.root / "reports")
                self.assertEqual(
                    command.call_args.args[3]["GLUTEN_TEST_ENABLE_SLOW_HIVE"],
                    "true" if stage == "slow-hive" else "false",
                )
                arguments = command.call_args.args[0]
                self.assertIn("-pl", arguments)
                self.assertEqual(
                    arguments[arguments.index("-pl") + 1], "gluten-ut/spark35"
                )
                self.assertNotIn("-am", arguments)

    def test_gate_fails_for_missing_skipped_cancelled_and_failed_jobs(self):
        for needs in (
            {},
            {"prepare": {"result": "success"}},
            {"prepare": {"result": "success"}, "integration": {"result": "skipped"}},
            {"prepare": {"result": "success"}, "integration": {"result": "cancelled"}},
            {"prepare": {"result": "success"}, "integration": {"result": "failure"}},
        ):
            with self.subTest(needs=needs):
                result = subprocess.run(
                    [
                        sys.executable,
                        str(SCRIPT),
                        "gate",
                        "--reports-dir",
                        str(self.root),
                    ],
                    env={"GLUTEN_CI_NEEDS": json.dumps(needs)},
                    capture_output=True,
                )
                self.assertNotEqual(result.returncode, 0)
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "gate", "--reports-dir", str(self.root)],
            env={
                "GLUTEN_CI_NEEDS": json.dumps(
                    {
                        "prepare": {"result": "success"},
                        "compatibility": {"result": "success"},
                        "integration": {"result": "success"},
                    }
                )
            },
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode())

    def test_package_rejects_revision_or_abi_drift(self):
        node = {
            "name": "bolt",
            "ref": "bolt/ci-source#recipe",
            "rrev": "recipe",
            "package_id": "id",
            "prev": "package",
            "context": "host",
            "settings": {
                "os": "Linux",
                "arch": "x86_64",
                "compiler": "gcc",
                "compiler.version": "12",
                "compiler.libcxx": "libstdc++11",
                "build_type": "Release",
            },
            "options": {
                "spark_compatible": "True",
                "enable_testutil": "True",
                "enable_hdfs": "False",
                "enable_s3": "False",
                "enable_asan": "False",
            },
        }
        node["binary"] = "Cache"
        exported = {"graph": {"nodes": {"0": node}}}
        self.ci.verify_package(exported, exported)
        for key, value in (
            ("binary", "Build"),
            ("prev", "wrong"),
            ("package_id", "wrong"),
            ("rrev", None),
            ("context", "build"),
        ):
            changed = dict(node, **{key: value})
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.ci.verify_package(exported, {"graph": {"nodes": {"1": changed}}})
        for options in (
            dict(node["options"], spark_compatible="False"),
            dict(node["options"], enable_testutil="False"),
        ):
            with self.assertRaises(ValueError):
                bad = {"graph": {"nodes": {"0": dict(node, options=options)}}}
                self.ci.verify_package(bad, bad)

    def test_dependency_graph_allows_build_completion_but_rejects_drift(self):
        node = {
            "ref": "zlib/1.3.1#recipe",
            "context": "host",
            "settings": {"compiler.version": "12"},
            "options": {"shared": "False"},
            "package_id": "id",
            "binary": "Build",
        }
        graph = {"graph": {"nodes": {"0": {}, "1": node}, "error": None}}
        installed = dict(node, binary="Cache", prev="new-package")
        actual = {"graph": {"nodes": {"0": {}, "99": installed}, "error": None}}
        self.ci.verify_dependency_graph(graph, actual)
        for key, value in (
            ("ref", "zlib/1.3.1#different-recipe"),
            ("context", "build"),
            ("package_id", "different-id"),
            ("settings", {"compiler.version": "13"}),
            ("options", {"shared": "True"}),
        ):
            with (
                self.subTest(key=key),
                self.assertRaisesRegex(ValueError, "graph.*changed"),
            ):
                self.ci.verify_dependency_graph(
                    graph,
                    {"graph": {"nodes": {"1": dict(installed, **{key: value})}}},
                )
        with self.assertRaisesRegex(ValueError, "graph.*error"):
            self.ci.verify_dependency_graph(
                graph,
                {"graph": {"nodes": {"1": installed}, "error": {"type": "conflict"}}},
            )

    def test_dependency_graph_recovers_options_removed_by_package_info(self):
        options = {"shared": "False", "plasma": "deprecated"}
        node = {
            "ref": "arrow/15.0.1-oss#recipe",
            "context": "host",
            "settings": {"compiler.version": "12"},
            "options": options,
            "info": {"options": dict(options)},
            "package_id": "same-binary",
        }
        before = {"graph": {"nodes": {"0": {}, "1": node}}}
        installed = dict(node, options={"shared": "False"})
        after = {"graph": {"nodes": {"0": {}, "1": installed}}}
        self.ci.verify_dependency_graph(before, after)
        for update in (
            {"options": {"shared": "True"}},
            {"info": {"options": {"shared": "False", "plasma": "True"}}},
        ):
            with (
                self.subTest(update=update),
                self.assertRaisesRegex(ValueError, "graph.*changed"),
            ):
                self.ci.verify_dependency_graph(
                    before,
                    {"graph": {"nodes": {"0": {}, "1": dict(installed, **update)}}},
                )

    def test_tpc_rejects_baseline_only_native_and_missing_queries(self):
        log = (
            "Running baseline query q1 (iteration 0)...\n"
            "== Physical Plan ==\nHashAggregateTransformer\n"
            "Running test query q1 (iteration 0)...\n"
            "== Physical Plan ==\nHashAggregate\n"
            "Successfully ran query q1. Returned row count: 1\n"
            "Summary: 1 out of 1 queries passed.\n"
        )
        with self.assertRaises(ValueError):
            self.ci.verify_tpc(log, {"q1"})
        native = log.replace("\nHashAggregate\n", "\nHashAggregateTransformer\n")
        self.assertEqual(self.ci.verify_tpc(native, {"q1"})["native_queries"], ["q1"])
        with self.assertRaises(ValueError):
            self.ci.verify_tpc(native, {"q1", "q6"})
        with self.assertRaises(ValueError):
            self.ci.verify_tpc(native.replace("1 out of 1", "0 out of 1"), {"q1"})

    def test_tpc_requires_native_nodes_in_executed_final_aqe_plan(self):
        log = (
            "Running test query q1 (iteration 0)...\n== Physical Plan ==\n"
            "AdaptiveSparkPlan isFinalPlan=true\n== Final Plan ==\nHashAggregate\n"
            "== Initial Plan ==\nHashAggregateTransformer\n"
            "Successfully ran query q1. Returned row count: 1\n"
            "Summary: 1 out of 1 queries passed.\n"
        )
        with self.assertRaises(ValueError):
            self.ci.verify_tpc(log, {"q1"})
        final = log.replace(
            "== Final Plan ==\nHashAggregate\n",
            "== Final Plan ==\nHashAggregateTransformer\n",
        )
        self.assertEqual(self.ci.verify_tpc(final, {"q1"})["native_queries"], ["q1"])

    def test_command_failure_preserves_log_and_exit_failure(self):
        log = self.root / "failure.log"
        with self.assertRaises(subprocess.CalledProcessError):
            self.ci.run(
                [sys.executable, "-c", "print('failure evidence'); exit(7)"],
                self.root,
                log,
            )
        self.assertIn("failure evidence", log.read_text())

    def test_build_keeps_configuration_and_export_host_profiles_consistent(self):
        class ExportReached(Exception):
            pass

        for overlay_exists in (False, True):
            with self.subTest(repository_overlay=overlay_exists):
                checkout = self.root / str(overlay_exists)
                checkout.mkdir()
                overlay = checkout / "scripts/conan/bolt.profile"
                expected_profiles = ["-pr:h", "default"]
                if overlay_exists:
                    overlay.parent.mkdir(parents=True)
                    overlay.write_text(
                        "[replace_tool_requires]\ncmake/*: cmake/3.31.10\n"
                    )
                    expected_profiles.extend(["-pr:h", str(overlay)])
                reports = checkout / "reports"
                reports.mkdir()
                (reports / "inputs.json").write_text(
                    json.dumps({"bolt_tested_sha": "a" * 40})
                )
                commands = []

                def run(args, cwd, log, env=None, json_output=None):
                    commands.append(args)
                    if args[:2] == ["conan", "export-pkg"]:
                        raise ExportReached

                with (
                    patch.object(self.ci, "ROOT", checkout),
                    patch.object(self.ci, "git", side_effect=["a" * 40, ""]),
                    patch.object(self.ci, "verify_gluten_source"),
                    patch.object(self.ci, "run", side_effect=run),
                    patch.dict("os.environ", {"CONAN_HOME": str(checkout / "cache")}),
                    self.assertRaises(ExportReached),
                ):
                    self.ci.build(checkout / "gluten", reports)

                exported_profiles = []
                for index, arg in enumerate(commands[-1]):
                    if arg == "-pr:h":
                        exported_profiles.extend([arg, commands[-1][index + 1]])
                self.assertEqual(exported_profiles, expected_profiles)
                configured = next(args for args in commands if args[0] == "make")
                profile_override = next(
                    (
                        arg
                        for arg in configured
                        if arg.startswith("CONAN_HOST_PROFILE_ARGS=")
                    ),
                    None,
                )
                self.assertIsNotNone(profile_override)
                self.assertEqual(
                    shlex.split(profile_override.split("=", 1)[1]), expected_profiles
                )

    def test_spark_environment_requires_binary_and_source_test_resources(self):
        home = self.root / "spark_home"
        with self.assertRaises(ValueError):
            self.ci.spark_environment(home, {})
        for path in (
            home / "bin/spark-submit",
            home / "jars/spark.jar",
            home / "python/lib/pyspark.zip",
            home / "python/lib/py4j.zip",
            home / "sql/core/src/test/resources/sql-tests/inputs/query.sql",
        ):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
        (home / "RELEASE").write_text("Spark 3.5.5 built for Hadoop 3\n")
        env = self.ci.spark_environment(home, {"PYTHONPATH": "existing"})
        self.assertEqual(env["SPARK_HOME"], str(home))
        self.assertTrue(env["PYTHONPATH"].endswith(":existing"))
        (home / "RELEASE").write_text("Spark 4.0.0 built for Hadoop 3\n")
        with self.assertRaises(ValueError):
            self.ci.spark_environment(home, {})

    def test_spark_environment_accepts_versioned_pyspark_distribution(self):
        home = self.root / "pyspark_home"
        for name in (
            "bin/spark-submit",
            "jars/spark-core_2.12-3.5.5.jar",
            "python/lib/py4j-0.10.9.7-src.zip",
            "sql/core/src/test/resources/sql-tests/inputs/query.sql",
        ):
            path = home / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
        with zipfile.ZipFile(home / "python/lib/pyspark.zip", "w") as archive:
            archive.writestr("pyspark/version.py", "__version__: str = '3.5.5'\n")
        self.assertEqual(self.ci.spark_environment(home, {})["SPARK_HOME"], str(home))
        with zipfile.ZipFile(home / "python/lib/pyspark.zip", "w") as archive:
            archive.writestr("pyspark/version.py", "__version__ = '4.0.0'\n")
        with self.assertRaises(ValueError):
            self.ci.spark_environment(home, {})

    def test_binary_jar_is_verified_even_when_sources_jar_is_present(self):
        import hashlib

        target = self.root / "package/target"
        target.mkdir(parents=True)
        (target / "gluten-package-1.0-sources.jar").touch()
        (self.root / "native-libraries.json").write_text(
            json.dumps({"libbolt_backend.so": hashlib.sha256(b"candidate").hexdigest()})
        )
        jar_path = target / "gluten-package-1.0.jar"

        def package(provider, native):
            with zipfile.ZipFile(jar_path, "w") as jar:
                jar.writestr("META-INF/gluten-components/" + provider, "")
                jar.writestr("linux/amd64/libbolt_backend.so", native)

        package("org.apache.gluten.backendsapi.bolt.BoltBackend", b"candidate")
        self.ci.verify_jar(self.root, self.root)
        package("org.apache.gluten.backendsapi.bolt.BoltBackend", b"remote-main")
        with self.assertRaises(ValueError):
            self.ci.verify_jar(self.root, self.root)
        package("org.apache.gluten.backendsapi.velox.VeloxBackend", b"candidate")
        with self.assertRaises(ValueError):
            self.ci.verify_jar(self.root, self.root)

    def test_binary_jar_accepts_only_metadata_spark_classifier(self):
        import hashlib

        target = self.root / "package/target"
        target.mkdir(parents=True)
        (self.root / "native-libraries.json").write_text(
            json.dumps({"libbolt_backend.so": hashlib.sha256(b"candidate").hexdigest()})
        )
        bundle = target / "gluten-package-1.0.jar"
        with zipfile.ZipFile(bundle, "w") as jar:
            jar.writestr(
                "META-INF/gluten-components/org.apache.gluten.backendsapi.bolt.BoltBackend",
                "",
            )
            jar.writestr("linux/amd64/libbolt_backend.so", b"candidate")
        classifier = target / "gluten-package-1.0-3.5.jar"
        with zipfile.ZipFile(classifier, "w") as jar:
            jar.writestr("gluten-build-info.properties", "spark.version=3.5.5")
            jar.writestr(
                "META-INF/maven/org.apache.gluten/gluten-package/pom.properties", ""
            )
            for name in ("LICENSE", "NOTICE", "DEPENDENCIES"):
                jar.writestr("META-INF/" + name, "Apache packaging metadata")
        self.assertEqual(self.ci.verify_jar(self.root, self.root), bundle)
        with zipfile.ZipFile(classifier, "a") as jar:
            jar.writestr("org/apache/gluten/Stale.class", b"stale")
        with self.assertRaisesRegex(ValueError, "classifier.*metadata"):
            self.ci.verify_jar(self.root, self.root)


if __name__ == "__main__":
    unittest.main()
