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

"""Classify the outcome of every job against blacklist.txt.

Failure keys (one per line in blacklist.txt):
    <FQCN>#<test name>     a failed scalatest test (from the JUnit XML report)
                           or a failed JUnit method
    <FQCN>#(aborted)       scalatest suite aborted (from the job log)
    <FQCN>#(jvm-failed)    the JVM exited abnormally before reporting; never
                           matched against the blacklist on purpose

Also writes _suite_times.txt / _test_times.txt (measured from the XML) next
to the plan, for refreshing the cached timing hints.
"""

import argparse
import glob
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict

ABORTED_RE = re.compile(r"^(\S+) \*\*\* ABORTED \*\*\*")
JUNIT_FAIL_RE = re.compile(r"^\d+\) (.+)\(([^()]+)\)$")
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def read_lines(path):
    try:
        with open(path, errors="replace") as fh:
            return [ANSI_RE.sub("", line.rstrip("\n")) for line in fh]
    except OSError:
        return []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True)
    ap.add_argument("--jobs-dir", required=True, help="<job>.log / <job>.rc live here")
    ap.add_argument("--reports-dir", required=True, help="<job>/TEST-*.xml live here")
    ap.add_argument("--blacklist", required=True)
    ap.add_argument("--timings-dir", required=True)
    ap.add_argument(
        "--test-times-min",
        type=float,
        default=120.0,
        help="only keep per-test times of suites at least this heavy",
    )
    args = ap.parse_args()

    with open(args.blacklist) as fh:
        blacklist = [line.rstrip("\n") for line in fh if line.strip()]
    blacklisted = set(blacklist)

    suite_secs = defaultdict(float)
    test_secs = {}
    keys = []  # (key, job)

    with open(args.plan) as fh:
        plan = [line.rstrip("\n").split("\t") for line in fh if line.strip()]

    for job_id, module, kind, _weight, members, _tests_file in plan:
        suites = members.split(",")
        job_keys = []
        log = read_lines(os.path.join(args.jobs_dir, job_id + ".log"))
        try:
            with open(os.path.join(args.jobs_dir, job_id + ".rc")) as fh:
                rc = int(fh.read().strip())
        except (OSError, ValueError):
            rc = None

        if kind == "junit":
            found = False
            for line in log:
                m = JUNIT_FAIL_RE.match(line)
                if m:
                    job_keys.append((f"{m.group(2)}#{m.group(1)}", job_id))
                if line.startswith("OK (") or line.startswith("Tests run:"):
                    found = True
            keys.extend(job_keys)
            if not found or rc not in (0, 1) or (rc == 1 and not job_keys):
                keys.extend((f"{s}#(jvm-failed)", job_id) for s in suites)
            continue

        # scalatest: failures and timings from the XML, aborts from the log
        reported = set()
        invalid_report = False
        for xml in glob.glob(os.path.join(args.reports_dir, job_id, "TEST-*.xml")):
            if "DiscoverySuite" in os.path.basename(xml):
                continue
            try:
                root = ET.parse(xml).getroot()
            except ET.ParseError:
                invalid_report = True
                continue
            reported.add(root.get("name"))
            for tc in root.iter("testcase"):
                cls, name = tc.get("classname"), tc.get("name")
                secs = float(tc.get("time", 0) or 0)
                reported.add(cls)
                suite_secs[cls] += secs
                test_secs[(cls, name)] = secs
                if tc.find("failure") is not None or tc.find("error") is not None:
                    job_keys.append((f"{cls}#{name}", job_id))
        for line in log:
            m = ABORTED_RE.match(line)
            if m:
                job_keys.append((f"{m.group(1)}#(aborted)", job_id))
                reported.add(m.group(1))
        keys.extend(job_keys)
        # Exit 1 is expected for reported test failures and suite aborts. A
        # crash, missing exit status, or incomplete run must never be excused
        # by a partial XML report or a blacklisted failure earlier in the job.
        incomplete = (
            invalid_report
            or rc not in (0, 1)
            or (rc == 1 and not job_keys)
            or not any(line.startswith("Run completed in") for line in log)
        )
        keys.extend(
            (f"{s}#(jvm-failed)", job_id)
            for s in suites
            if incomplete or s not in reported
        )

    fired = set()
    expected = unexpected = 0
    for key, job_id in sorted(set(keys)):
        if key.endswith("#(jvm-failed)") or key not in blacklisted:
            unexpected += 1
            print(f"  ! {key}    [{job_id}]")
        else:
            fired.add(key)
            expected += 1
    # Only entries this run could have fired can be stale: the suite ran here
    # as a whole, or (for a suite split into test shards) the named test did.
    ran_whole, ran_tests = set(), set()
    for _, _, _, _, members, tests_file in plan:
        if tests_file == "-":
            ran_whole.update(members.split(","))
        else:
            try:
                with open(tests_file) as fh:
                    ran_tests.update((members, t.rstrip("\n")) for t in fh if t.strip())
            except OSError:
                ran_whole.update(members.split(","))

    def could_fire(entry):
        suite, _, name = entry.partition("#")
        if suite in ran_whole or (suite, name) in ran_tests:
            return True
        # "(aborted)" and friends can fire from any test shard of the suite.
        return name.startswith("(") and any(s == suite for s, _ in ran_tests)

    stale = [e for e in blacklist if e not in fired and could_fire(e)]
    if stale:
        print(
            "stale blacklist entries (didn't fail this run; remove if consistently passing):"
        )
        for e in stale:
            print(f"  ? {e}")

    os.makedirs(args.timings_dir, exist_ok=True)
    with open(os.path.join(args.timings_dir, "_suite_times.txt"), "w") as fh:
        for cls, secs in sorted(suite_secs.items(), key=lambda kv: (-kv[1], kv[0])):
            fh.write(f"{cls}\t{secs:.1f}\n")
    with open(os.path.join(args.timings_dir, "_test_times.txt"), "w") as fh:
        rows = [
            (c, n, s)
            for (c, n), s in test_secs.items()
            if suite_secs[c] >= args.test_times_min
        ]
        for c, n, s in sorted(
            rows, key=lambda r: (-suite_secs[r[0]], r[0], -r[2], r[1])
        ):
            fh.write(f"{c}\t{n}\t{s:.1f}\n")

    print(
        f"suites reported: {len(suite_secs)}, tests: {len(test_secs)}, test time: {sum(suite_secs.values()) / 60:.1f} min"
    )
    print(f"expected failures:   {expected} (on blacklist; not counted)")
    print(f"unexpected failures: {unexpected}")
    sys.exit(1 if unexpected else 0)


if __name__ == "__main__":
    main()
