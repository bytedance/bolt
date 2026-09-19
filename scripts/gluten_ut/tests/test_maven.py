#!/usr/bin/env python3
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

"""Check Maven measurements without running Spark or a Maven reactor."""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "summarize_maven.py"
SPEC = importlib.util.spec_from_file_location("summarize_maven", SCRIPT)
MAVEN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MAVEN)
LOG = """100 [INFO] --- surefire:3.5.6:test (default-test) @ example ---
200 [INFO] --- scalatest:2.2.0:test (test) @ example ---
Run completed in 1 second.
1200 [INFO] Building Next module
1300 [INFO] Reactor Summary for Gluten
1400 [INFO] BUILD SUCCESS
"""


class MavenTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.gluten = self.root / "gluten"
        self.logs = self.root / "logs"
        self.parallel = self.root / "parallel"
        self.reports = self.gluten / "module/target/surefire-reports"
        self.reports.mkdir(parents=True)
        self.logs.mkdir()
        self.parallel.mkdir()
        (self.logs / "_maven.log").write_text(LOG)
        (self.logs / "_maven.rc").write_text("0")
        (self.logs / "_elapsed_seconds.txt").write_text("2")
        (self.logs / "_start_epoch.txt").write_text("10")

    def report(self, *, content="", suite="example.Suite"):
        path = self.reports / f"TEST-{suite}.xml"
        path.write_text(
            f'<testsuite name="{suite}"><testcase classname="{suite}" name="case">'
            f"{content}</testcase></testsuite>"
        )
        return path

    def run_summary(self):
        env = dict(os.environ)
        env.pop("GITHUB_STEP_SUMMARY", None)
        process = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--gluten-home",
                str(self.gluten),
                "--log-dir",
                str(self.logs),
                "--parallel-log-dir",
                str(self.parallel),
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertIn(process.returncode, (0, 1), process.stderr)
        return process.returncode, json.loads((self.logs / "_summary.json").read_text())

    def test_serial_plugin_intervals(self):
        seconds, started, completed, aborts, unfinished = MAVEN.lifecycle_metrics(LOG)
        self.assertAlmostEqual(seconds, 1.1)
        self.assertEqual((started, completed, aborts, unfinished), (1, 1, [], False))

    def test_full_plugin_names_from_ci_maven(self):
        log = LOG.replace("--- surefire:", "--- maven-surefire-plugin:").replace(
            "--- scalatest:", "--- scalatest-maven-plugin:"
        )
        self.assertEqual(MAVEN.lifecycle_metrics(log), (1.1, 1, 1, [], False))

    def test_partial_parallel_jobs_are_explicit_in_comparison(self):
        self.report()
        (self.parallel / "_phases.tsv").write_text("1\t1\tSummary\n2\t1\tDone\n")
        jobs = self.parallel / "jobs"
        jobs.mkdir()
        (jobs / "complete.log").write_text("Total number of tests run: 3\n")
        (jobs / "aborted.log").write_text("*** RUN ABORTED ***\n")
        self.run_summary()
        markdown = (self.logs / "_summary.md").read_text()
        self.assertIn("| 2 | 1 | 3 |", markdown)
        self.assertIn("Parallel jobs without a final test count: 1", markdown)
        self.assertIn("`aborted`", markdown)

    def test_skipped_cases_and_discovery_aggregate_are_not_executed(self):
        self.report()
        self.report(suite="example.Skipped", content="<skipped/>")
        self.report(suite="org.scalatest.tools.DiscoverySuite")
        self.report(suite="example.PartitionDiscoverySuite")
        rc, summary = self.run_summary()
        self.assertEqual(rc, 0)
        self.assertEqual((summary["executed"], summary["skipped"]), (2, 1))

    def test_maven_success_does_not_hide_ignored_test_failures(self):
        self.report(content="<failure/>")
        rc, summary = self.run_summary()
        self.assertEqual(rc, 1)
        self.assertEqual(summary["failures"], ["example.Suite#case"])
        self.assertEqual(summary["maven_exit"], 0)

    def test_abort_without_xml_failure_is_failure(self):
        self.report()
        (self.logs / "_maven.log").write_text(LOG + "example.Other *** ABORTED ***\n")
        rc, summary = self.run_summary()
        self.assertEqual(rc, 1)
        self.assertEqual(len(summary["aborts"]), 1)

    def test_partial_run_cannot_pass_with_successful_reports(self):
        self.report()
        (self.logs / "_maven.log").write_text(
            LOG.replace("Run completed in 1 second.", "")
        )
        rc, summary = self.run_summary()
        self.assertEqual(rc, 1)
        self.assertIn("1 started, 0 completed", " ".join(summary["problems"]))

    def test_stale_and_invalid_reports(self):
        stale = self.report()
        os.utime(stale, (1, 1))
        broken = self.report(suite="example.Broken")
        broken.write_text("<invalid")
        rc, summary = self.run_summary()
        self.assertEqual(rc, 1)
        self.assertEqual(summary["executed"], 0)
        self.assertTrue(any("Invalid XML" in p for p in summary["problems"]))

    def test_coverage_uses_module_and_class(self):
        self.report()
        (self.parallel / "_classified.tsv").write_text(
            "scalatest\tmodule\texample.Suite\t1\nscalatest\tother\texample.Suite\t1\n"
        )
        rc, summary = self.run_summary()
        self.assertEqual(rc, 0)
        self.assertEqual(
            summary["unreported_discovered_suites"], ["other: example.Suite"]
        )


if __name__ == "__main__":
    unittest.main()
