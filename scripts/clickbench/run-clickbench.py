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

"""Run the Bolt ClickBench workload with ClickBench-compatible output."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import subprocess
import sys
import time
from pathlib import Path

QUERY_COUNT = 43
TIMING = re.compile(r"^CLICKBENCH_SECONDS ([0-9]+(?:\.[0-9]+)?)$", re.MULTILINE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--data-path", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("clickbench-results.json"))
    parser.add_argument("--tries", type=int, default=3)
    parser.add_argument("--num-drivers", type=int, default=os.cpu_count() or 1)
    parser.add_argument("--num-io-threads", type=int, default=8)
    parser.add_argument("--num-splits-per-file", type=int, default=10)
    parser.add_argument("--query", type=int, action="append", dest="queries")
    parser.add_argument(
        "--drop-os-cache",
        action="store_true",
        help="Run sync and sudo tee /proc/sys/vm/drop_caches before each query.",
    )
    args = parser.parse_args()
    if args.tries < 1:
        parser.error("--tries must be positive")
    args.queries = args.queries or list(range(1, QUERY_COUNT + 1))
    if any(query < 1 or query > QUERY_COUNT for query in args.queries):
        parser.error("--query must be between 1 and 43")
    return args


def drop_os_cache() -> None:
    subprocess.run(["sync"], check=True)
    subprocess.run(
        ["sudo", "tee", "/proc/sys/vm/drop_caches"],
        input="3\n",
        text=True,
        stdout=subprocess.DEVNULL,
        check=True,
    )


def run_once(
    args: argparse.Namespace, query: int
) -> tuple[float | None, dict[str, object]]:
    command = [
        str(args.binary),
        f"--bolt_benchmark_clickbench_data_path={args.data_path}",
        f"--bolt_benchmark_clickbench_query={query}",
        f"--bolt_benchmark_num_drivers={args.num_drivers}",
        f"--bolt_benchmark_num_io_threads={args.num_io_threads}",
        f"--bolt_benchmark_num_splits_per_file={args.num_splits_per_file}",
        "--bolt_benchmark_include_results=true",
    ]
    started = time.time()
    completed = subprocess.run(command, text=True, capture_output=True)
    match = TIMING.search(completed.stdout)
    result = {
        "command": command,
        "exit_code": completed.returncode,
        "wall_seconds": time.time() - started,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }
    if completed.returncode != 0 or match is None:
        return None, result
    return float(match.group(1)), result


def git_revision(path: Path) -> str | None:
    completed = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "HEAD"],
        text=True,
        capture_output=True,
    )
    return completed.stdout.strip() if completed.returncode == 0 else None


def main() -> int:
    args = parse_args()
    args.binary = args.binary.resolve()
    args.data_path = args.data_path.resolve()
    if not args.binary.is_file():
        raise SystemExit(f"benchmark binary does not exist: {args.binary}")
    if not args.data_path.exists():
        raise SystemExit(f"data path does not exist: {args.data_path}")

    details: dict[str, object] = {
        "format_version": 1,
        "bolt_revision": git_revision(Path(__file__).resolve().parents[2]),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "data_path": str(args.data_path),
        "parameters": {
            "tries": args.tries,
            "num_drivers": args.num_drivers,
            "num_io_threads": args.num_io_threads,
            "num_splits_per_file": args.num_splits_per_file,
            "drop_os_cache": args.drop_os_cache,
        },
        "queries": {},
    }
    failed = False
    for query in args.queries:
        samples: list[float | None] = []
        attempts: list[dict[str, object]] = []
        if args.drop_os_cache:
            drop_os_cache()
        for _ in range(args.tries):
            seconds, attempt = run_once(args, query)
            samples.append(seconds)
            attempts.append(attempt)
            failed = failed or seconds is None
        details["queries"][f"q{query}"] = {  # type: ignore[index]
            "seconds": samples,
            "attempts": attempts,
        }
        print(json.dumps(samples, separators=(",", ":")) + ",", flush=True)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(details, indent=2) + "\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
