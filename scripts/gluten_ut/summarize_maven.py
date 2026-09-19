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

"""Archive Maven reports and measure the serial Surefire/ScalaTest lifecycle.

Maven ignores test failures to finish the reactor. This checker makes failures,
aborts and incomplete runs visible again. This experimental baseline deliberately
reports raw failures; the parallel runner remains the blacklist-aware CI gate.
"""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import xml.etree.ElementTree as ET

ANSI = re.compile(r"\x1b\[[0-9;]*m")
GOAL = re.compile(r"^(\d+) \[INFO\] --- (\S+) .* @ (\S+) ---$")
BOUNDARY = re.compile(r"^(\d+) \[INFO\] (?:Building |Reactor Summary)")


def lifecycle_metrics(log):
    """Sum serial test-plugin intervals, including discovery and fork startup."""
    active = None
    seconds = 0.0
    started = completed = 0
    aborts = []
    for line in ANSI.sub("", log).splitlines():
        goal = GOAL.match(line)
        boundary = goal or BOUNDARY.match(line)
        if boundary and active is not None:
            seconds += (int(boundary[1]) - active) / 1000
            active = None
        if goal:
            plugin, _, target = goal[2].split(":")
            # Older Maven versions print artifact IDs instead of short names.
            plugin = {
                "maven-surefire-plugin": "surefire",
                "scalatest-maven-plugin": "scalatest",
            }.get(plugin, plugin)
            if plugin in ("surefire", "scalatest") and target == "test":
                active = int(goal[1])
                started += plugin == "scalatest"
        completed += line.startswith("Run completed in")
        if "*** ABORTED ***" in line or "*** RUN ABORTED ***" in line:
            aborts.append(line)
    return seconds, started, completed, aborts, active is not None


def collect_reports(gluten_home, log_dir, started_epoch):
    reports = log_dir / "reports"
    if reports.exists():
        shutil.rmtree(reports)
    cases = []
    suites = set()
    problems = []
    count = 0
    # The reactor's own reports only, never Arrow / other vendored projects.
    for source in sorted(gluten_home.glob("**/target/surefire-reports/*")):
        relative = source.relative_to(gluten_home)
        if "ep" in relative.parts or not source.is_file():
            continue
        # A failed reactor can leave old reports in modules it never reached.
        if source.stat().st_mtime < started_epoch:
            continue
        if not (source.name.startswith("TEST-") or source.suffix == ".txt"):
            continue
        dest = reports / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, dest)
        if not source.name.startswith("TEST-") or source.suffix != ".xml":
            continue
        if source.name == "TEST-org.scalatest.tools.DiscoverySuite.xml":
            continue
        module = str(relative.parent.parent.parent)
        try:
            root = ET.parse(source).getroot()
        except ET.ParseError:
            problems.append(f"Invalid XML: {relative}")
            continue
        count += 1
        for suite in root.iter("testsuite"):
            suites.add((module, suite.get("name")))
        for case in root.iter("testcase"):
            suites.add((module, case.get("classname")))
            cases.append(
                {
                    "module": module,
                    "key": f"{case.get('classname')}#{case.get('name')}",
                    "skipped": case.find("skipped") is not None,
                    "failed": case.find("failure") is not None
                    or case.find("error") is not None,
                }
            )
    if not count:
        problems.append("No fresh test reports")
    return cases, suites, problems


def summarize(gluten_home, log_dir, parallel_log_dir):
    log = (log_dir / "_maven.log").read_text(errors="replace")
    rc = int((log_dir / "_maven.rc").read_text())
    elapsed = int((log_dir / "_elapsed_seconds.txt").read_text())
    started_epoch = int((log_dir / "_start_epoch.txt").read_text())
    seconds, started, completed, aborts, unfinished = lifecycle_metrics(log)
    cases, suites, problems = collect_reports(gluten_home, log_dir, started_epoch)
    if rc or "[INFO] BUILD SUCCESS" not in log:
        problems.append(f"Maven did not complete successfully (exit {rc})")
    if unfinished or not started or started != completed:
        problems.append(
            f"ScalaTest invocations: {started} started, {completed} completed"
        )
    executed = sum(not case["skipped"] for case in cases)
    if not executed:
        problems.append("No executed tests")
    missing = None
    classified = parallel_log_dir / "_classified.tsv"
    if classified.exists():
        discovered = {
            (fields[1], fields[2])
            for line in classified.read_text().splitlines()
            if len(fields := line.split("\t")) >= 3
        }
        missing = [
            f"{module}: {suite}" for module, suite in sorted(discovered - suites)
        ]
    failures = [case["key"] for case in cases if case["failed"]]
    result = {
        "elapsed_seconds": elapsed,
        "test_plugin_seconds": round(seconds, 3),
        "maven_exit": rc,
        "executed": executed,
        "skipped": len(cases) - executed,
        "failed": len(failures),
        "failures": failures,
        "aborts": aborts,
        "problems": problems,
        "unreported_discovered_suites": missing,
    }
    (log_dir / "_summary.json").write_text(json.dumps(result, indent=2) + "\n")
    lines = [
        "### Gluten Maven baseline",
        "",
        "| Measurement | Result |",
        "| --- | ---: |",
        f"| `clean test` wall time | {elapsed}s |",
        f"| Serial test plugins, including discovery / JVM startup | {seconds:.1f}s |",
        f"| Executed / skipped tests | {executed} / {len(cases) - executed} |",
        f"| Raw failed tests / suite or run aborts | {len(failures)} / {len(aborts)} |",
        f"| Infrastructure / incomplete-run errors | {len(problems)} |",
        "",
        "Same Bolt profiles, tags and JVM limits as the parallel runner. Maven uses "
        "the POM's discovery rules, one serial reactor and no suite sharding. "
        "Upstream Velox CI distributes its normal and slow tests over five runners.",
        "",
        "Compare totals with care: the parallel runner builds with `clean install "
        "-DskipTests`, while this baseline uses `clean test`. The second run can "
        "benefit from dependencies fetched by the first.",
    ]
    phases = parallel_log_dir / "_phases.tsv"
    if phases.exists():
        timings = [line.split("\t", 2) for line in phases.read_text().splitlines()]
        total = next((row[0] for row in timings if row[2] == "Done"), "incomplete")
        dispatch = next(
            (row[1] for row in timings if row[2] == "Summary"), "incomplete"
        )
        parallel_tests = 0
        uncounted_jobs = []
        for path in sorted((parallel_log_dir / "jobs").glob("*.log")):
            counts = re.findall(
                r"^(?:Total number of tests run: |OK \(|Tests run: )(\d+)",
                ANSI.sub("", path.read_text(errors="replace")),
                re.MULTILINE,
            )
            if counts:
                parallel_tests += int(counts[-1])
            else:
                uncounted_jobs.append(path.stem)
        lines += [
            "",
            "| Runner | Total wall time (s) | Test execution wall time (s) | Reported executed tests |",
            "| --- | ---: | ---: | ---: |",
            f"| Parallel: build + classify + dispatch | {total} | {dispatch} | {parallel_tests} |",
            f"| Maven: clean test | {elapsed} | {seconds:.1f} | {executed} |",
        ]
        if uncounted_jobs:
            lines += [
                "",
                f"Parallel jobs without a final test count: {len(uncounted_jobs)}. "
                "Their partial execution is omitted from the count above; "
                "these timings are not a complete equal-work comparison.",
                "",
                *[f"- `{job}`" for job in uncounted_jobs],
            ]
    if missing is not None:
        lines += [
            "",
            f"Discovered suites without a Maven XML report: {len(missing)}. "
            "This includes excluded suites and suites Maven does not discover; "
            "see `_summary.json` for the list.",
        ]
    if problems:
        lines += ["", *[f"- {problem}" for problem in problems]]
    markdown = "\n".join(lines) + "\n"
    print(markdown)
    (log_dir / "_summary.md").write_text(markdown)
    if path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(path, "a") as output:
            output.write(markdown)
    return 1 if failures or aborts or problems else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gluten-home", required=True, type=Path)
    parser.add_argument("--log-dir", required=True, type=Path)
    parser.add_argument("--parallel-log-dir", required=True, type=Path)
    args = parser.parse_args()
    return summarize(args.gluten_home, args.log_dir, args.parallel_log_dir)


if __name__ == "__main__":
    raise SystemExit(main())
