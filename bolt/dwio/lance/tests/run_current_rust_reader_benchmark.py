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

"""Build or run a benchmark against a local checkout of current Lance."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cargo",
        type=Path,
        help="Cargo executable. Defaults to the cargo found on PATH.",
    )
    parser.add_argument(
        "--lance-repo",
        required=True,
        type=Path,
        help="Path to the Lance source checkout to benchmark.",
    )
    parser.add_argument(
        "--build-root",
        type=Path,
        default=Path(tempfile.gettempdir()) / "bolt-current-rust-lance-benchmark",
        help="Persistent build root outside the Bolt checkout.",
    )
    parser.add_argument(
        "--prepare-only",
        action="store_true",
        help="Build the harness and print its executable path without running it.",
    )
    args, passthrough = parser.parse_known_args()
    args.passthrough = passthrough
    return args


def run_checked(command: list[str], cwd: Path) -> str:
    return subprocess.run(
        command,
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout.strip()


def write_if_changed(path: Path, contents: str) -> None:
    if path.exists() and path.read_text() == contents:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents)


def main() -> int:
    args = parse_args()
    lance_repo = args.lance_repo.resolve()
    source = Path(__file__).with_name("CurrentRustLanceReaderBenchmark.rs")
    commit = run_checked(["git", "rev-parse", "HEAD"], lance_repo)
    cache_key = hashlib.sha256(
        (commit + source.read_text()).encode("utf-8")
    ).hexdigest()[:16]
    project = args.build_root / cache_key
    target = args.build_root / "target"
    executable = target / "release" / "bolt-current-rust-lance-benchmark"
    cargo = str(args.cargo) if args.cargo else shutil.which("cargo")
    if cargo is None:
        raise SystemExit(
            "cargo was not found; install the Rust toolchain or pass --cargo /path/to/cargo"
        )
    manifest = f"""[package]
name = "bolt-current-rust-lance-benchmark"
version = "0.1.0"
edition = "2024"

[dependencies]
anyhow = "1"
arrow-array = "58"
arrow-select = "58"
futures = "0.3"
lance-core = {{ path = "{lance_repo / "rust/lance-core"}" }}
lance-encoding = {{ path = "{lance_repo / "rust/lance-encoding"}" }}
lance-file = {{ path = "{lance_repo / "rust/lance-file"}" }}
lance-io = {{ path = "{lance_repo / "rust/lance-io"}", default-features = false }}
tokio = {{ version = "1", features = ["rt-multi-thread"] }}
"""
    write_if_changed(project / "Cargo.toml", manifest)
    destination = project / "src/main.rs"
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not destination.exists() or destination.read_bytes() != source.read_bytes():
        shutil.copyfile(source, destination)

    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(target)
    cargo_dir = str(Path(cargo).resolve().parent)
    env["PATH"] = cargo_dir + os.pathsep + env.get("PATH", "")
    sibling_rustc = Path(cargo).resolve().with_name("rustc")
    if sibling_rustc.exists():
        env["RUSTC"] = str(sibling_rustc)
    subprocess.run(
        [
            cargo,
            "build",
            "--release",
            "--manifest-path",
            str(project / "Cargo.toml"),
        ],
        cwd=lance_repo,
        env=env,
        check=True,
    )
    if args.prepare_only:
        print(executable)
        return 0

    os.execv(executable, [str(executable), *args.passthrough])
    return 0


if __name__ == "__main__":
    sys.exit(main())
