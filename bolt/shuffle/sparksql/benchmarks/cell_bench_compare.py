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
"""The standard Cell shuffle benchmark: one fixed set of scenarios and
metrics, compared across builds.

Each variant is a directory holding bolt_cell_split_benchmark and the
libbolt_engine.so it was built with (snapshot a build with --snapshot).
Variants run alternately, round after round, pinned to one core, so drift
of the machine hits every variant alike.

Write scenarios (Cell writer, 1 GiB of input each), one per code path:

  fixed4_P1024          encoded fixed-width columns
  fixed4_P4096          the same at a high partition count
  fixed4nulls10_P1024   sparse nulls on every column
  wide100sparse_P200    a wide table, two columns with sparse nulls
  strings_P1024         raw strings
  strdict_P1024         dictionary strings
  prod_P30000           production shape: huge P, indexed input
  strings_P1024 CellCap the spill path under a 256MB cell cap

Write metrics: ns/row (split + stop, best trial), split ms, stop ms,
compress ms, bytes written, MB evicted, sealed windows. Read metrics, from
--phases on fixed4_P1024, strdict_P1024 and prod_P30000: reader ns/row and
decode ms, Cell with dictionaries.

Checks: bytes written must match the first variant unless the change is
meant to alter the output; a scenario whose compress ms (which no writer
change moves) spreads more than 5% across a variant's rounds is marked
noisy and should be rerun.

  cell_bench_compare.py --snapshot _build/Release /tmp/bench/base
  cell_bench_compare.py base=/tmp/bench/base new=/tmp/bench/new
"""

import argparse
import os
import re
import shutil
import subprocess
import sys

WRITE_SCENARIOS = [
    ("fixed4_P1024", "Cell"),
    ("fixed4_P4096", "Cell"),
    ("fixed4nulls10_P1024", "Cell"),
    ("wide100sparse_P200", "Cell"),
    ("strings_P1024", "Cell"),
    ("strdict_P1024", "Cell"),
    ("prod_P30000", "Cell"),
    ("strings_P1024", "CellCap"),
]
READ_SCENARIOS = ["fixed4_P1024", "strdict_P1024", "prod_P30000"]
BINARY = "bolt_cell_split_benchmark"
ENGINE = "libbolt_engine.so"
NOISE = 0.05

WRITE_TIME = re.compile(r"^write_(\S+?)_(Cell|CellCap)\s+([\d.]+)(ns|us|ms)\s")
SUMMARY = re.compile(
    r"^(\S+)/(\S+)\s+(\d+)\s+(\d+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)"
    r"\s+([\d.]+)\s+([\d.]+)\s+(\d+)\s*$"
)
READER = re.compile(
    r"^Cell-dict\s+([\d.]+)\s+\|\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)"
    r"\s+\|\s+(\d+)\s+([\d.]+)\s*$"
)
UNIT_NS = {"ns": 1.0, "us": 1e3, "ms": 1e6}


def snapshot(build_dir, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    shutil.copy2(os.path.join(build_dir, "bolt", ENGINE), out_dir)
    shutil.copy2(
        os.path.join(build_dir, "bolt/shuffle/sparksql/benchmarks", BINARY),
        out_dir,
    )
    print(f"snapshot of {build_dir} in {out_dir}")


def run(variant_dir, cpu, args):
    env = dict(os.environ, LD_LIBRARY_PATH=variant_dir)
    command = ["taskset", "-c", str(cpu), os.path.join(variant_dir, BINARY)]
    result = subprocess.run(
        command + args,
        cwd=variant_dir,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.splitlines()


def write_round(variant_dir, cpu, seconds, extra):
    names = "|".join(sorted({s for s, _ in WRITE_SCENARIOS}))
    lines = run(
        variant_dir,
        cpu,
        [f"--bm_max_secs={seconds}", f"--bm_regex=write_({names})_(Cell|CellCap)$"]
        + extra,
    )
    out = {}
    for line in lines:
        m = WRITE_TIME.match(line)
        if m:
            label = "CellCap" if m.group(2) == "CellCap" else "Cell"
            key = (m.group(1), label)
            out.setdefault(key, {})["ns_row"] = float(m.group(3)) * UNIT_NS[m.group(4)]
            continue
        m = SUMMARY.match(line)
        if m:
            label = "CellCap" if "-cap" in m.group(2) else m.group(2)
            if label not in ("Cell", "CellCap"):
                continue
            row = out.setdefault((m.group(1), label), {})
            row.update(
                bytes=int(m.group(4)),
                split_ms=float(m.group(5)),
                stop_ms=float(m.group(6)),
                compress_ms=float(m.group(7)),
                evict_mb=float(m.group(9)),
                windows=int(m.group(10)),
            )
    return out


def read_round(variant_dir, cpu, extra):
    out = {}
    for scenario in READ_SCENARIOS:
        lines = run(
            variant_dir, cpu, ["--phases", f"--phases_scenario={scenario}"] + extra
        )
        for line in lines:
            m = READER.match(line)
            if m:
                out[scenario] = {
                    "read_ns_row": float(m.group(7)),
                    "decode_ms": float(m.group(3)),
                }
    return out


def spread(values):
    return (max(values) - min(values)) / min(values) if min(values) > 0 else 0.0


def fmt(values, digits=1):
    return " / ".join(f"{v:.{digits}f}" for v in values)


def delta(best, base):
    return f"{100.0 * (best - base) / base:+.1f}%" if base else "n/a"


def report(variants, writes, reads):
    names = [n for n, _ in variants]
    base = names[0]
    print("\n## Write (ns/row, each round; delta of the best round vs " + base + ")\n")
    print(
        "| scenario | "
        + " | ".join(names)
        + " | "
        + " | ".join(f"{n} vs {base}" for n in names[1:])
        + " |"
    )
    print("|" + "---|" * (1 + 2 * len(names) - 1))
    for key in WRITE_SCENARIOS:
        cells, best = [], {}
        for n in names:
            rounds = [w[key] for w in writes[n] if key in w]
            ns = [r["ns_row"] for r in rounds if "ns_row" in r]
            best[n] = min(ns) if ns else 0.0
            note = ""
            comp = [r["compress_ms"] for r in rounds if "compress_ms" in r]
            if comp and spread(comp) > NOISE:
                note = " (noisy)"
            sizes = {r.get("bytes") for r in rounds}
            base_sizes = {
                r.get("bytes") for w in writes[base] for k, r in w.items() if k == key
            }
            if n != base and sizes != base_sizes:
                note += " (bytes differ)"
            cells.append(fmt(ns, 2) + note)
        deltas = [delta(best[n], best[base]) for n in names[1:]]
        print(
            f"| {key[0]} {key[1]} | "
            + " | ".join(cells)
            + " | "
            + " | ".join(deltas)
            + " |"
        )

    print("\n## Write phases (split / stop / compress ms, best round by ns/row)\n")
    print("| scenario | " + " | ".join(names) + " | bytes | evictMB | windows |")
    print("|" + "---|" * (len(names) + 4))
    for key in WRITE_SCENARIOS:
        cells = []
        last = {}
        for n in names:
            rounds = [w[key] for w in writes[n] if key in w and "split_ms" in w[key]]
            if not rounds:
                cells.append("-")
                continue
            r = min(rounds, key=lambda x: x.get("ns_row", 0.0))
            cells.append(
                f"{r['split_ms']:.0f} / {r['stop_ms']:.0f} / {r['compress_ms']:.0f}"
            )
            last = r
        print(
            f"| {key[0]} {key[1]} | "
            + " | ".join(cells)
            + f" | {last.get('bytes', '-')} | {last.get('evict_mb', 0):.0f} | {last.get('windows', '-')} |"
        )

    print("\n## Read (Cell with dictionaries: reader ns/row, each round)\n")
    print(
        "| scenario | "
        + " | ".join(names)
        + " | "
        + " | ".join(f"{n} vs {base}" for n in names[1:])
        + " |"
    )
    print("|" + "---|" * (1 + 2 * len(names) - 1))
    for scenario in READ_SCENARIOS:
        cells, best = [], {}
        for n in names:
            ns = [r[scenario]["read_ns_row"] for r in reads[n] if scenario in r]
            best[n] = min(ns) if ns else 0.0
            cells.append(fmt(ns, 2))
        deltas = [delta(best[n], best[base]) for n in names[1:]]
        print(
            f"| {scenario} | " + " | ".join(cells) + " | " + " | ".join(deltas) + " |"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("variants", nargs="*", help="name=dir, the first is the base")
    parser.add_argument("--snapshot", nargs=2, metavar=("BUILD_DIR", "OUT_DIR"))
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--cpu", type=int, default=3)
    parser.add_argument("--bm-max-secs", type=int, default=6)
    parser.add_argument("--no-read", action="store_true", help="skip the reader phases")
    parser.add_argument(
        "--input-mb",
        type=int,
        help="input per scenario; the standard run keeps the 1 GiB default",
    )
    args = parser.parse_args()
    if args.snapshot:
        snapshot(*args.snapshot)
        return 0
    if len(args.variants) < 1:
        parser.error("give at least one name=dir variant")
    variants = [tuple(v.split("=", 1)) for v in args.variants]
    writes = {n: [] for n, _ in variants}
    reads = {n: [] for n, _ in variants}
    extra = [f"--input_mb={args.input_mb}"] if args.input_mb else []
    for i in range(args.rounds):
        for name, directory in variants:
            print(f"round {i + 1}: {name} write", file=sys.stderr, flush=True)
            writes[name].append(
                write_round(directory, args.cpu, args.bm_max_secs, extra)
            )
            if not args.no_read:
                print(f"round {i + 1}: {name} read", file=sys.stderr, flush=True)
                reads[name].append(read_round(directory, args.cpu, extra))
    report(variants, writes, reads)
    return 0


if __name__ == "__main__":
    sys.exit(main())
