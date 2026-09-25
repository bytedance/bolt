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

"""Run one comparable all-types scan for each scenario and CPU count."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any


CPU_COUNTS = (1, 16)
SCENARIOS = (
    ("full_scan_all_types", "full_scan_checksum", "rows"),
    ("filter_1pct_all_types", "filter_checksum", "filter_output_rows"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("native", "parquet", "rust"))
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--native-benchmark", type=Path)
    parser.add_argument("--rust-benchmark", type=Path)
    parser.add_argument(
        "--print-stats",
        action="store_true",
        help="Forward detailed per-scan statistics to stderr.",
    )
    # Makes the suite convenient to call directly or through benchmark tools
    # that append Folly's JSON flag.  Output is always JSON.
    parser.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args()


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text())
    required = (
        "rows",
        "column_count",
        "lance_file",
        "parquet_file",
        "full_scan_checksum",
        "checksum_column",
        "filter_column",
        "filter_min",
        "filter_max",
        "filter_output_rows",
        "filter_checksum",
    )
    missing = [key for key in required if key not in manifest]
    if missing:
        raise ValueError(f"benchmark manifest is missing: {', '.join(missing)}")
    if int(manifest["rows"]) <= 0 or int(manifest["column_count"]) <= 0:
        raise ValueError("manifest rows and column_count must be greater than zero")
    for key in ("lance_file", "parquet_file"):
        if not Path(manifest[key]).is_file():
            raise ValueError(f"manifest {key} does not exist: {manifest[key]}")
    return manifest


def available_cpus() -> list[int]:
    if hasattr(os, "sched_getaffinity"):
        return sorted(os.sched_getaffinity(0))
    count = os.cpu_count() or 1
    return list(range(count))


def parse_metric(stdout: str) -> float:
    decoder = json.JSONDecoder()
    result: dict[str, Any] | None = None
    for offset, char in enumerate(stdout):
        if char != "{":
            continue
        try:
            value, _ = decoder.raw_decode(stdout[offset:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and "scan" in value:
            result = value
    if result is None or not isinstance(result["scan"], (int, float)):
        raise ValueError(
            f"child benchmark did not emit a numeric scan metric: {stdout[-2000:]}"
        )
    return float(result["scan"])


def common_environment(
    manifest: dict[str, Any], scenario: tuple[str, str, str]
) -> dict[str, str]:
    scenario_name, checksum_key, output_rows_key = scenario
    return {
        "BOLT_LANCE_BENCHMARK_FILE": str(manifest["lance_file"]),
        "BOLT_LANCE_BENCHMARK_PARQUET_FILE": str(manifest["parquet_file"]),
        "BOLT_LANCE_BENCHMARK_SCENARIO": scenario_name,
        "BOLT_LANCE_BENCHMARK_COLUMNS": str(manifest["column_count"]),
        "BOLT_LANCE_BENCHMARK_BATCH_SIZE": "1024",
        "BOLT_LANCE_BENCHMARK_EXPECTED_ROWS": str(manifest["rows"]),
        "BOLT_LANCE_BENCHMARK_EXPECTED_OUTPUT_ROWS": str(manifest[output_rows_key]),
        "BOLT_LANCE_BENCHMARK_EXPECTED_CHECKSUM": str(manifest[checksum_key]),
        "BOLT_LANCE_BENCHMARK_CHECKSUM_COLUMN": str(manifest["checksum_column"]),
        "BOLT_LANCE_BENCHMARK_FILTER_COLUMN": str(manifest["filter_column"]),
        "BOLT_LANCE_BENCHMARK_FILTER_MIN": str(manifest["filter_min"]),
        "BOLT_LANCE_BENCHMARK_FILTER_MAX": str(manifest["filter_max"]),
    }


def command_for_mode(args: argparse.Namespace) -> list[str]:
    if args.mode == "rust":
        if args.rust_benchmark is None:
            raise ValueError("--rust-benchmark is required in rust mode")
        executable = args.rust_benchmark
        command: list[str] = [str(executable)]
    else:
        if args.native_benchmark is None:
            raise ValueError("--native-benchmark is required in native or parquet mode")
        executable = args.native_benchmark
        command = [
            str(executable),
            "--bm_profile",
            "--bm_profile_iters=1",
            "--bm_regex=scan",
            "--json",
        ]
    if not executable.is_file():
        raise ValueError(f"benchmark executable does not exist: {executable}")
    return command


def main() -> int:
    args = parse_args()
    manifest = load_manifest(args.manifest)
    command = command_for_mode(args)
    cpus = available_cpus()
    if len(cpus) < max(CPU_COUNTS):
        raise SystemExit(
            f"benchmark suite needs {max(CPU_COUNTS)} CPUs but only {len(cpus)} are available"
        )

    metrics: dict[str, float] = {}
    for cpu_count in CPU_COUNTS:
        affinity = cpus[:cpu_count]
        for scenario in SCENARIOS:
            env = os.environ.copy()
            env.update(common_environment(manifest, scenario))
            if args.print_stats:
                env["BOLT_LANCE_BENCHMARK_PRINT_STATS"] = "1"
            if args.mode == "rust":
                env["BOLT_LANCE_BENCHMARK_RUNTIME_THREADS"] = str(cpu_count)
                env["LANCE_CPU_THREADS"] = str(cpu_count)
                env["LANCE_IO_THREADS"] = str(cpu_count)
            else:
                env["BOLT_LANCE_READER_MODE"] = args.mode
                env["BOLT_LANCE_BENCHMARK_DECODE_THREADS"] = str(cpu_count)

            def set_affinity() -> None:
                os.sched_setaffinity(0, affinity)

            completed = subprocess.run(
                command,
                check=False,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=set_affinity if hasattr(os, "sched_setaffinity") else None,
            )
            if completed.stderr:
                print(completed.stderr, file=sys.stderr, end="")
            if completed.returncode != 0:
                print(completed.stdout, file=sys.stderr, end="")
                raise SystemExit(
                    f"{args.mode} {scenario[0]} cpu{cpu_count} failed with "
                    f"exit code {completed.returncode}"
                )
            case = f"{scenario[0]}_cpu{cpu_count}"
            metrics[case] = parse_metric(completed.stdout)

    print(json.dumps(metrics, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
