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

"""Turn the discovered test classes into JVM jobs of roughly equal weight.

Input is the SuiteClassifier output (scalatest / test / junit records) plus the
checked-in timing hints. Output is one job per line, heaviest first:

    <job id> <module> <kind> <weight secs> <fqcn,fqcn,...> <tests file or ->

- Big scalatest suites (>= --shard-min) are split into shards of named tests
  (`-t`), balanced by per-test timings.
- The remaining suites of a module are packed (first-fit decreasing) into
  batches of about --target seconds; each batch is one JVM.
- JUnit classes of a module form one job.

Suites / tests without timing hints get a default weight, so a brand-new suite
still runs; it just packs less accurately until the hints are refreshed.
"""

import argparse
import math
import os
import re
from collections import defaultdict

DEFAULT_SUITE_SECS = 15.0


def read_tsv(path):
    if not path or not os.path.isfile(path):
        return []
    with open(path) as fh:
        return [line.rstrip("\n").split("\t") for line in fh if line.strip()]


def job_name(index, label):
    label = re.sub(r"[^A-Za-z0-9_.-]", "_", label)
    return f"{index:03d}-{label}"


def lpt(items, nbins):
    """Longest-processing-time-first assignment of (weight, item) into nbins bins."""
    bins = [[0.0, []] for _ in range(nbins)]
    for weight, item in sorted(items, key=lambda x: -x[0]):
        target = min(bins, key=lambda b: b[0])
        target[0] += weight
        target[1].append(item)
    return [(w, members) for w, members in bins if members]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--classified", required=True)
    ap.add_argument("--suite-times")
    ap.add_argument("--test-times")
    ap.add_argument(
        "--target", type=float, default=60.0, help="batch / shard weight (secs)"
    )
    ap.add_argument(
        "--shard-min", type=float, default=120.0, help="split suites heavier than this"
    )
    ap.add_argument(
        "--jobs-dir", required=True, help="where per-job test lists are written"
    )
    ap.add_argument("--plan", required=True)
    ap.add_argument(
        "--shard-index", type=int, default=0, help="run only this shard of the jobs"
    )
    ap.add_argument(
        "--shard-count",
        type=int,
        default=1,
        help="split the jobs across this many runners",
    )
    args = ap.parse_args()

    suites = {}  # fqcn -> (module, kind)
    tests = defaultdict(list)  # fqcn -> [test names]
    for rec in read_tsv(args.classified):
        if rec[0] in ("scalatest", "junit"):
            suites[rec[2]] = (rec[1], rec[0])
        elif rec[0] == "test":
            tests[rec[2]].append(rec[3])

    suite_secs = {r[0]: float(r[1]) for r in read_tsv(args.suite_times) if len(r) >= 2}
    test_secs = {
        (r[0], r[1]): float(r[2]) for r in read_tsv(args.test_times) if len(r) >= 3
    }

    def suite_weight(fqcn):
        return suite_secs.get(fqcn, DEFAULT_SUITE_SECS)

    os.makedirs(args.jobs_dir, exist_ok=True)
    jobs = []  # (weight, module, kind, [fqcn], tests or None, label)

    per_module = defaultdict(lambda: {"scalatest": [], "junit": []})
    for fqcn, (module, kind) in suites.items():
        per_module[module][kind].append(fqcn)

    for module in sorted(per_module):
        # 1. shard the heavy suites
        batchable = []
        for fqcn in per_module[module]["scalatest"]:
            weight = suite_weight(fqcn)
            names = tests.get(fqcn, [])
            nshards = math.ceil(weight / args.target)
            if weight >= args.shard_min and len(names) > 1 and nshards >= 2:
                known = [test_secs[(fqcn, n)] for n in names if (fqcn, n) in test_secs]
                unknown_default = (
                    (weight - sum(known)) / max(1, len(names) - len(known))
                    if len(known) < len(names)
                    else 0.0
                )
                weighted = [
                    (test_secs.get((fqcn, n), max(unknown_default, 0.5)), n)
                    for n in names
                ]
                for i, (w, members) in enumerate(
                    lpt(weighted, min(nshards, len(names)))
                ):
                    jobs.append(
                        (
                            w,
                            module,
                            "scalatest",
                            [fqcn],
                            members,
                            f"{fqcn.rsplit('.', 1)[-1]}-s{i + 1}",
                        )
                    )
            else:
                batchable.append((weight, fqcn))
        # 2. pack the rest into batches (first-fit decreasing)
        batches = []  # [weight, [fqcn]]
        for weight, fqcn in sorted(batchable, key=lambda x: (-x[0], x[1])):
            for b in batches:
                if b[0] + weight <= args.target:
                    b[0] += weight
                    b[1].append(fqcn)
                    break
            else:
                batches.append([weight, [fqcn]])
        for w, members in batches:
            label = members[0].rsplit(".", 1)[-1] + (
                f"+{len(members) - 1}" if len(members) > 1 else ""
            )
            jobs.append((w, module, "scalatest", members, None, label))
        # 3. junit classes: one job per module
        junit = sorted(per_module[module]["junit"])
        if junit:
            jobs.append(
                (max(5.0, 2.0 * len(junit)), module, "junit", junit, None, "junit")
            )

    jobs.sort(key=lambda j: (-j[0], j[1], j[5]))
    # Sharding across runners: deal the weight-sorted jobs round-robin, so
    # every shard gets a similar mix of heavy and light jobs.
    all_jobs = jobs
    jobs = []
    with open(args.plan, "w") as fh:
        for i, job in enumerate(all_jobs):
            if i % args.shard_count != args.shard_index:
                continue
            jobs.append(job)
            weight, module, kind, members, test_names, label = job
            job_id = job_name(i, label)
            tests_file = "-"
            if test_names is not None:
                tests_file = os.path.join(args.jobs_dir, job_id + ".tests")
                with open(tests_file, "w") as tf:
                    tf.write("\n".join(test_names) + "\n")
            fh.write(
                f"{job_id}\t{module}\t{kind}\t{weight:.1f}\t{','.join(members)}\t{tests_file}\n"
            )

    nsuites = len(suites)
    total = sum(j[0] for j in all_jobs)
    print(
        f"planned {len(all_jobs)} jobs for {nsuites} test classes; estimated work {total / 60:.1f} min, heaviest job {max(j[0] for j in all_jobs):.0f}s"
    )
    if args.shard_count > 1:
        print(
            f"shard {args.shard_index}/{args.shard_count}: {len(jobs)} jobs, estimated work {sum(j[0] for j in jobs) / 60:.1f} min"
        )


if __name__ == "__main__":
    main()
