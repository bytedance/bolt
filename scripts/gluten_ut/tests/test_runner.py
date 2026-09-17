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

"""Regression checks for discovery and failure reporting, without Spark or Maven."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parents[1]
SUITE = "example.SampleSuite"
COMPLETED = "Run completed in 1 second.\n"


class RunnerTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.jobs = self.root / "jobs"
        self.reports = self.root / "reports" / "job"
        self.jobs.mkdir()
        self.reports.mkdir(parents=True)
        self.plan = self.root / "plan.tsv"
        self.blacklist = self.root / "blacklist.txt"
        self.blacklist.write_text("")

    def run_script(self, name, *args):
        return subprocess.run(
            [sys.executable, str(SCRIPTS / name), *map(str, args)],
            capture_output=True,
            text=True,
            timeout=10,
        )

    def summarize(self, *, kind="scalatest", log=COMPLETED, rc=0):
        self.plan.write_text(f"job\tmodule\t{kind}\t1\t{SUITE}\t-\n")
        (self.jobs / "job.log").write_text(log)
        if rc is not None:
            (self.jobs / "job.rc").write_text(str(rc))
        return self.run_script(
            "summarize.py",
            "--plan",
            self.plan,
            "--jobs-dir",
            self.jobs,
            "--reports-dir",
            self.reports.parent,
            "--blacklist",
            self.blacklist,
            "--timings-dir",
            self.root,
        )

    def report(self, *, failure=False, empty=False):
        case = (
            ""
            if empty
            else (
                f'<testcase classname="{SUITE}" name="case" time="1">'
                + ('<failure message="failed"/>' if failure else "")
                + "</testcase>"
            )
        )
        (self.reports / f"TEST-{SUITE}.xml").write_text(
            f'<testsuite name="{SUITE}">{case}</testsuite>'
        )

    def assert_job_failed(self, result):
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn(f"{SUITE}#(jvm-failed)", result.stdout)

    def test_success_and_empty_suite(self):
        for empty in (False, True):
            with self.subTest(empty=empty):
                self.report(empty=empty)
                result = self.summarize()
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_known_test_failure_is_allowed(self):
        self.report(failure=True)
        self.blacklist.write_text(f"{SUITE}#case\n")
        result = self.summarize(rc=1)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("expected failures:   1", result.stdout)

    def test_unknown_test_failure_fails(self):
        self.report(failure=True)
        result = self.summarize(rc=1)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn(f"! {SUITE}#case", result.stdout)

    def test_known_abort_is_allowed(self):
        self.blacklist.write_text(f"{SUITE}#(aborted)\n")
        result = self.summarize(log=f"{SUITE} *** ABORTED ***\n{COMPLETED}", rc=1)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_partial_report_cannot_hide_crash(self):
        self.report()
        self.assert_job_failed(self.summarize(log="", rc=137))

    def test_abnormal_exit_after_completion_fails(self):
        self.report(failure=True)
        self.blacklist.write_text(f"{SUITE}#case\n{SUITE}#(jvm-failed)\n")
        self.assert_job_failed(self.summarize(rc=137))

    def test_nonzero_exit_without_reported_failure_fails(self):
        self.report()
        self.assert_job_failed(self.summarize(rc=1))

    def test_missing_exit_code_fails_even_with_known_failure(self):
        self.report(failure=True)
        self.blacklist.write_text(f"{SUITE}#case\n")
        self.assert_job_failed(self.summarize(rc=None))

    def test_missing_or_malformed_report_fails(self):
        self.assert_job_failed(self.summarize())
        (self.reports / f"TEST-{SUITE}.xml").write_text("<testsuite")
        self.assert_job_failed(self.summarize())

    def test_junit_requires_completion_even_with_zero_exit(self):
        self.assert_job_failed(self.summarize(kind="junit", log="", rc=0))

    def test_junit_success(self):
        result = self.summarize(kind="junit", log="OK (1 test)\n")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_junit_success_summary_cannot_hide_abnormal_exit(self):
        self.assert_job_failed(
            self.summarize(kind="junit", log="OK (1 test)\n", rc=137)
        )

    def test_junit_parameterized_failure_can_be_blacklisted(self):
        self.blacklist.write_text(f"{SUITE}#case[0]\n")
        result = self.summarize(
            kind="junit",
            log=f"1) case[0]({SUITE})\nTests run: 1,  Failures: 1\n",
            rc=1,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("expected failures:   1", result.stdout)

    def test_junit_unparsed_failure_cannot_pass(self):
        self.assert_job_failed(
            self.summarize(kind="junit", log="Tests run: 1,  Failures: 1\n", rc=1)
        )

    def test_invalid_shard_selection_fails(self):
        classified = self.root / "classified.tsv"
        classified.write_text(f"scalatest\tmodule\t{SUITE}\t1\n")
        for index, count in ((1, 1), (-1, 2), (0, 0)):
            with self.subTest(index=index, count=count):
                result = self.run_script(
                    "plan.py",
                    "--classified",
                    classified,
                    "--jobs-dir",
                    self.jobs,
                    "--plan",
                    self.plan,
                    "--shard-index",
                    index,
                    "--shard-count",
                    count,
                )
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)

    def test_same_suite_name_in_two_modules_preserves_both_test_lists(self):
        classified = self.root / "classified.tsv"
        classified.write_text(
            f"scalatest\tfirst\t{SUITE}\t1\n"
            f"test\tfirst\t{SUITE}\tfirst test\n"
            f"scalatest\tsecond\t{SUITE}\t2\n"
            f"test\tsecond\t{SUITE}\tsecond test\n"
            f"test\tsecond\t{SUITE}\tthird test\n"
        )
        timings = self.root / "suite_times.txt"
        timings.write_text(f"{SUITE}\t180\n")
        result = self.run_script(
            "plan.py",
            "--classified",
            classified,
            "--suite-times",
            timings,
            "--target",
            "90",
            "--jobs-dir",
            self.jobs,
            "--plan",
            self.plan,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        rows = [line.split("\t") for line in self.plan.read_text().splitlines()]
        self.assertEqual({row[1] for row in rows}, {"first", "second"})
        first = [row for row in rows if row[1] == "first"]
        self.assertEqual(len(first), 1)
        self.assertEqual(first[0][4:], [SUITE, "-"])
        second_tests = [
            name
            for row in rows
            if row[1] == "second"
            for name in Path(row[5]).read_text().splitlines()
        ]
        self.assertCountEqual(second_tests, ["second test", "third test"])

    def executable(self, name, source):
        path = self.root / "bin" / name
        path.parent.mkdir(exist_ok=True)
        path.write_text("#!/usr/bin/env bash\nset -eu\n" + source)
        path.chmod(0o755)
        return path

    def test_maven_console_colors_do_not_reach_the_jvm(self):
        gluten = self.root / "gluten"
        (gluten / "module/target/classes").mkdir(parents=True)
        (gluten / "module/target/test-classes").mkdir()
        spark = self.root / "spark"
        spark.mkdir()
        self.executable("sleep", "exec /bin/sleep 0.01\n")
        self.executable(
            "bwrap",
            'while [[ "$1" != "$JAVA_HOME/bin/java" ]]; do shift; done\nexec "$@"\n',
        )
        self.executable(
            "mvn",
            r"""
for arg; do
  case "$arg" in
    -Doutput=*) printf '%s\n' '-XX:+IgnoreUnrecognizedVMOptions -Dfile.encoding=UTF-8' > "${arg#*=}" ;;
    -Dmdep.outputFile=*) echo /unused.jar > "${arg#*=}" ;;
  esac
done
if [[ "$*" == *help:evaluate* ]]; then
  printf '%s\033[0m\n' '-XX:+IgnoreUnrecognizedVMOptions -Dfile.encoding=UTF-8'
fi
""",
        )
        self.executable(
            "java",
            f"""
if [[ "$*" == *$'\\033'* ]]; then
  echo 'IllegalCharsetNameException: ANSI in JVM arguments' >&2
  exit 1
fi
if [[ "$*" == *-version* ]]; then
  exit "${{FAIL_PREFLIGHT:-0}}"
elif [[ "$*" == *SuiteClassifier.java* ]]; then
  printf 'junit\\tmodule\\t{SUITE}\\n'
else
  [[ "$*" == *-Dfile.encoding=UTF-8* ]]
  printf 'OK (1 test)\\n'
fi
""",
        )
        env = {
            **os.environ,
            "PATH": f"{self.root / 'bin'}:{os.environ['PATH']}",
            "GLUTEN_HOME": str(gluten),
            "SPARK_HOME": str(spark),
            "JAVA_HOME": str(self.root),
            "MVN_BIN": str(self.root / "bin/mvn"),
            "LOG_DIR": str(self.root / "logs"),
            "TIMINGS_DIR": str(self.root / "timings"),
            "BLACKLIST_FILE": str(self.blacklist),
            "JOBS": "1",
            "SKIP_INSTALL": "1",
            "REFRESH_TIMINGS": "0",
        }
        for fail_preflight in ("0", "1"):
            with self.subTest(fail_preflight=fail_preflight):
                result = subprocess.run(
                    ["bash", str(SCRIPTS / "run.sh")],
                    env={**env, "FAIL_PREFLIGHT": fail_preflight},
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertEqual(
                    result.returncode,
                    int(fail_preflight),
                    result.stdout + result.stderr,
                )
                if fail_preflight == "1":
                    self.assertNotIn("Step 3/5", result.stdout)
                else:
                    self.assertIn("unexpected failures: 0", result.stdout)

    def test_discovery_failure_stops_before_planning(self):
        gluten = self.root / "gluten"
        for module in ("good", "broken"):
            (gluten / module / "target" / "classes").mkdir(parents=True)
            (gluten / module / "target" / "test-classes").mkdir()
        spark = self.root / "spark"
        spark.mkdir()
        self.executable("bwrap", "exit 0\n")
        self.executable(
            "mvn",
            """
if [[ "$*" == *help:evaluate* ]]; then
  for arg; do
    if [[ "$arg" == -Doutput=* ]]; then
      echo '-XX:+IgnoreUnrecognizedVMOptions' > "${arg#*=}"
    fi
  done
  echo '-XX:+IgnoreUnrecognizedVMOptions'
  exit 0
fi
if [[ "$FAIL_STAGE" == classpath && "$*" == *' -pl broken '* ]]; then
  exit 1
fi
for arg; do
  if [[ "$arg" == -Dmdep.outputFile=* ]]; then
    echo /unused.jar > "${arg#*=}"
  fi
done
""",
        )
        self.executable(
            "java",
            f"""
[[ "$*" == *-version* ]] && exit 0
module="${{@: -2:1}}"
printf 'scalatest\\t%s\\t{SUITE}\\t1\\n' "$module"
if [[ "$FAIL_STAGE" == classifier && "$module" == broken ]]; then
  exit 1
fi
""",
        )
        for stage in ("classpath", "classifier"):
            with self.subTest(stage=stage):
                env = {
                    **os.environ,
                    "PATH": f"{self.root / 'bin'}:{os.environ['PATH']}",
                    "GLUTEN_HOME": str(gluten),
                    "SPARK_HOME": str(spark),
                    "JAVA_HOME": str(self.root),
                    "MVN_BIN": str(self.root / "bin" / "mvn"),
                    "LOG_DIR": str(self.root / "logs"),
                    "TIMINGS_DIR": str(self.root / "timings"),
                    "JOBS": "2",
                    "SKIP_INSTALL": "1",
                    "REFRESH_TIMINGS": "0",
                    "FAIL_STAGE": stage,
                }
                result = subprocess.run(
                    ["bash", str(SCRIPTS / "run.sh")],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=20,
                )
                self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertNotIn("Step 3/5", result.stdout)


if __name__ == "__main__":
    unittest.main()
