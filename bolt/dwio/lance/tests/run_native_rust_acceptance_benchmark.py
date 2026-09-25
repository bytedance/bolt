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

"""Run paired native/Rust Lance scans with alternating execution order."""

from __future__ import annotations

import argparse
import itertools
import json
import math
import os
from pathlib import Path
import re
import resource
import statistics
import subprocess
import time
from typing import Any


STATS_PREFIX = "BOLT_LANCE_BENCHMARK_STATS "
RUST_STATS_PREFIX = "BOLT_LANCE_RUST_MAIN_STATS "


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--native-benchmark", required=True, type=Path)
    parser.add_argument("--rust-benchmark", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--rounds", type=int, default=7)
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--threads", type=int, default=16)
    parser.add_argument("--columns", type=int, default=0)
    parser.add_argument("--column-names", default="")
    parser.add_argument("--scenario", default="full_scan")
    parser.add_argument("--native-scenario")
    parser.add_argument("--rust-scenario")
    parser.add_argument("--row-indices", default="")
    parser.add_argument("--rust-cache-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--filter-column", default="filter_key")
    parser.add_argument("--filter-min", type=int, default=0)
    parser.add_argument("--filter-max", type=int, default=0)
    parser.add_argument("--checksum-column", default="row_id")
    parser.add_argument("--expected-output-rows", type=int)
    parser.add_argument("--expected-checksum", type=int)
    return parser.parse_args()


def parse_json_metric(output: str) -> float:
    decoder = json.JSONDecoder()
    metric: float | None = None
    for offset, char in enumerate(output):
        if char != "{":
            continue
        try:
            value, _ = decoder.raw_decode(output[offset:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and isinstance(value.get("scan"), (int, float)):
            metric = float(value["scan"])
    if metric is None:
        raise ValueError(f"benchmark emitted no scan metric: {output[-2000:]}")
    return metric


def parse_stats(stderr: str, prefix: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for line in stderr.splitlines():
        if not line.startswith(prefix):
            continue
        for token in line[len(prefix) :].split():
            key, separator, value = token.partition("=")
            if not separator:
                continue
            if re.fullmatch(r"-?\d+", value):
                result[key] = int(value)
            else:
                result[key] = value
    return result


def process_memory_bytes(pid: int) -> tuple[int, int]:
    peak = 0
    current = 0
    try:
        status = Path(f"/proc/{pid}/status").read_text()
    except (FileNotFoundError, ProcessLookupError):
        return current, peak
    for line in status.splitlines():
        if line.startswith("VmRSS:"):
            current = int(line.split()[1]) * 1024
        elif line.startswith("VmHWM:"):
            peak = int(line.split()[1]) * 1024
    return current, peak


def run_process(
    command: list[str], env: dict[str, str], affinity: set[int]
) -> dict[str, Any]:
    def set_affinity() -> None:
        if hasattr(os, "sched_setaffinity"):
            os.sched_setaffinity(0, affinity)

    started = time.monotonic()
    usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    process = subprocess.Popen(
        command,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=set_affinity if hasattr(os, "sched_setaffinity") else None,
    )
    peak_rss_bytes = 0
    while True:
        _, sampled_peak = process_memory_bytes(process.pid)
        peak_rss_bytes = max(peak_rss_bytes, sampled_peak)
        if process.poll() is not None:
            break
        time.sleep(0.001)
    stdout, stderr = process.communicate()
    usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    wall_seconds = time.monotonic() - started
    if process.returncode != 0:
        raise RuntimeError(
            f"benchmark failed with exit code {process.returncode}: {command}\n"
            f"stdout={stdout[-4000:]}\nstderr={stderr[-4000:]}"
        )
    return {
        "command": command,
        "wall_seconds": wall_seconds,
        "peak_rss_bytes": peak_rss_bytes,
        "user_cpu_seconds": usage_after.ru_utime - usage_before.ru_utime,
        "system_cpu_seconds": usage_after.ru_stime - usage_before.ru_stime,
        "scan_picoseconds_per_row": parse_json_metric(stdout),
        "stdout": stdout,
        "stderr": stderr,
    }


def summarize(values: list[float]) -> dict[str, float]:
    return {
        "median": statistics.median(values),
        "minimum": min(values),
        "maximum": max(values),
        "mean": statistics.fmean(values),
    }


def exact_sign_flip(log_ratios: list[float]) -> dict[str, float | int]:
    observed = abs(statistics.fmean(log_ratios))
    extreme = 0
    total = 0
    for signs in itertools.product((-1.0, 1.0), repeat=len(log_ratios)):
        statistic = abs(
            statistics.fmean(sign * value for sign, value in zip(signs, log_ratios))
        )
        extreme += statistic >= observed - 1e-15
        total += 1
    return {
        "observed_abs_mean_log_ratio": observed,
        "p_value": extreme / total,
        "permutations": total,
    }


def main() -> int:
    args = parse_args()
    if args.rounds <= 0 or args.batch_size <= 0 or args.threads <= 0:
        raise ValueError("rounds, batch size, and threads must be positive")
    for path in (args.dataset, args.native_benchmark, args.rust_benchmark):
        if not path.is_file():
            raise ValueError(f"file does not exist: {path}")
    if args.output.exists():
        raise ValueError(f"refusing to overwrite output: {args.output}")

    available = (
        sorted(os.sched_getaffinity(0))
        if hasattr(os, "sched_getaffinity")
        else list(range(os.cpu_count() or 1))
    )
    if len(available) < args.threads:
        raise ValueError(
            f"requested {args.threads} CPUs but only {len(available)} are available"
        )
    affinity = set(available[: args.threads])

    common_env = os.environ.copy()
    common_env.update(
        {
            "BOLT_LANCE_BENCHMARK_FILE": str(args.dataset),
            "BOLT_LANCE_BENCHMARK_SCENARIO": args.scenario,
            "BOLT_LANCE_BENCHMARK_BATCH_SIZE": str(args.batch_size),
            "BOLT_LANCE_BENCHMARK_COLUMNS": str(args.columns),
            "BOLT_LANCE_BENCHMARK_DECODE_THREADS": str(args.threads),
            "BOLT_LANCE_BENCHMARK_RUNTIME_THREADS": str(args.threads),
            "LANCE_CPU_THREADS": str(args.threads),
            "LANCE_IO_THREADS": str(args.threads),
            "BOLT_LANCE_BENCHMARK_CACHE_BYTES": str(args.rust_cache_bytes),
            "BOLT_LANCE_BENCHMARK_PRINT_STATS": "1",
            "BOLT_LANCE_BENCHMARK_FILTER_COLUMN": args.filter_column,
            "BOLT_LANCE_BENCHMARK_FILTER_MIN": str(args.filter_min),
            "BOLT_LANCE_BENCHMARK_FILTER_MAX": str(args.filter_max),
            "BOLT_LANCE_BENCHMARK_CHECKSUM_COLUMN": args.checksum_column,
        }
    )
    if args.column_names:
        common_env["BOLT_LANCE_BENCHMARK_COLUMN_NAMES"] = args.column_names
    if args.row_indices:
        common_env["BOLT_LANCE_BENCHMARK_ROW_INDICES"] = args.row_indices
    if args.expected_output_rows is not None:
        common_env["BOLT_LANCE_BENCHMARK_EXPECTED_OUTPUT_ROWS"] = str(
            args.expected_output_rows
        )
    if args.expected_checksum is not None:
        common_env["BOLT_LANCE_BENCHMARK_EXPECTED_CHECKSUM"] = str(
            args.expected_checksum
        )
    commands = {
        "native": [
            str(args.native_benchmark),
            "--bm_profile",
            "--bm_profile_iters=1",
            "--bm_regex=scan",
            "--json",
        ],
        "rust": [str(args.rust_benchmark)],
    }

    measurements: list[dict[str, Any]] = []
    for round_number in range(1, args.rounds + 1):
        order = ("native", "rust") if round_number % 2 else ("rust", "native")
        for position, implementation in enumerate(order, start=1):
            env = common_env.copy()
            if implementation == "native":
                env["BOLT_LANCE_BENCHMARK_SCENARIO"] = (
                    args.native_scenario or args.scenario
                )
                env["BOLT_LANCE_READER_MODE"] = "native"
            else:
                env["BOLT_LANCE_BENCHMARK_SCENARIO"] = (
                    args.rust_scenario or args.scenario
                )
            print(
                f"round {round_number}/{args.rounds} position {position}: {implementation}",
                flush=True,
            )
            measurement = run_process(commands[implementation], env, affinity)
            reader_stats = parse_stats(
                measurement["stderr"],
                STATS_PREFIX if implementation == "native" else RUST_STATS_PREFIX,
            )
            input_rows = int(reader_stats["input_rows"])
            measurement.update(
                {
                    "round": round_number,
                    "position": position,
                    "implementation": implementation,
                    "reader_stats": reader_stats,
                    "scan_seconds": measurement["scan_picoseconds_per_row"]
                    * input_rows
                    / 1e12,
                }
            )
            measurements.append(measurement)
            print(
                f"  wall={measurement['wall_seconds']:.3f}s "
                f"rss={measurement['peak_rss_bytes'] / 2**30:.3f}GiB",
                flush=True,
            )

    by_impl = {
        implementation: [
            measurement
            for measurement in measurements
            if measurement["implementation"] == implementation
        ]
        for implementation in ("native", "rust")
    }
    native_wall = [value["wall_seconds"] for value in by_impl["native"]]
    rust_wall = [value["wall_seconds"] for value in by_impl["rust"]]
    ratios = [native / rust for native, rust in zip(native_wall, rust_wall)]
    native_scan = [value["scan_seconds"] for value in by_impl["native"]]
    rust_scan = [value["scan_seconds"] for value in by_impl["rust"]]
    scan_ratios = [native / rust for native, rust in zip(native_scan, rust_scan)]
    report = {
        "configuration": {
            "dataset": str(args.dataset),
            "rounds": args.rounds,
            "schedule": "odd native-rust, even rust-native",
            "batch_size": args.batch_size,
            "threads": args.threads,
            "columns": args.columns,
            "column_names": args.column_names,
            "scenario": args.scenario,
            "native_scenario": args.native_scenario or args.scenario,
            "rust_scenario": args.rust_scenario or args.scenario,
            "row_indices": args.row_indices,
            "rust_cache_bytes": args.rust_cache_bytes,
            "cpu_affinity": sorted(affinity),
        },
        "measurements": measurements,
        "summary": {
            "native_wall_seconds": summarize(native_wall),
            "rust_wall_seconds": summarize(rust_wall),
            "native_scan_seconds": summarize(native_scan),
            "rust_scan_seconds": summarize(rust_scan),
            "native_peak_rss_bytes": summarize(
                [value["peak_rss_bytes"] for value in by_impl["native"]]
            ),
            "rust_peak_rss_bytes": summarize(
                [value["peak_rss_bytes"] for value in by_impl["rust"]]
            ),
            "native_user_cpu_seconds": summarize(
                [value["user_cpu_seconds"] for value in by_impl["native"]]
            ),
            "rust_user_cpu_seconds": summarize(
                [value["user_cpu_seconds"] for value in by_impl["rust"]]
            ),
            "native_system_cpu_seconds": summarize(
                [value["system_cpu_seconds"] for value in by_impl["native"]]
            ),
            "rust_system_cpu_seconds": summarize(
                [value["system_cpu_seconds"] for value in by_impl["rust"]]
            ),
            "paired_native_over_rust": summarize(ratios),
            "exact_sign_flip": exact_sign_flip([math.log(value) for value in ratios]),
            "paired_native_over_rust_scan": summarize(scan_ratios),
            "exact_scan_sign_flip": exact_sign_flip(
                [math.log(value) for value in scan_ratios]
            ),
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
