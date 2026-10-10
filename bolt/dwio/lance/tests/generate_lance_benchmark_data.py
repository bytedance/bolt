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

"""Build the current-Lance generator and create comparable benchmark files."""

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
    parser.add_argument("output", type=Path)
    parser.add_argument("--rows", type=int, default=1_048_576)
    parser.add_argument("--batch-rows", type=int, default=65_536)
    parser.add_argument("--cargo", type=Path)
    parser.add_argument("--lance-repo", required=True, type=Path)
    parser.add_argument(
        "--build-root",
        type=Path,
        default=Path(tempfile.gettempdir()) / "bolt-lance-benchmark-generator",
    )
    return parser.parse_args()


def checked_output(command: list[str], cwd: Path) -> str:
    return subprocess.run(
        command, cwd=cwd, check=True, text=True, stdout=subprocess.PIPE
    ).stdout.strip()


def write_if_changed(path: Path, contents: str) -> None:
    if path.exists() and path.read_text() == contents:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents)


def main() -> int:
    args = parse_args()
    if args.rows <= 0 or args.batch_rows <= 0:
        raise SystemExit("--rows and --batch-rows must be greater than zero")
    lance_repo = args.lance_repo.resolve()
    source = Path(__file__).with_name("GenerateLanceBenchmarkData.rs")
    commit = checked_output(["git", "rev-parse", "HEAD"], lance_repo)
    cache_key = hashlib.sha256((commit + source.read_text()).encode()).hexdigest()[:16]
    project = args.build_root / cache_key
    target = args.build_root / "target"
    executable = target / "release" / "bolt-lance-benchmark-data"
    cargo = str(args.cargo) if args.cargo else shutil.which("cargo")
    if cargo is None:
        raise SystemExit("cargo was not found; pass --cargo /path/to/cargo")
    manifest = f"""[package]
name = "bolt-lance-benchmark-data"
version = "0.1.0"
edition = "2024"

[dependencies]
anyhow = "1"
arrow-array = "58"
arrow-buffer = "58"
arrow-schema = "58"
half = "2.7"
lance-core = {{ path = "{lance_repo / "rust/lance-core"}" }}
lance-encoding = {{ path = "{lance_repo / "rust/lance-encoding"}" }}
lance-file = {{ path = "{lance_repo / "rust/lance-file"}" }}
lance-io = {{ path = "{lance_repo / "rust/lance-io"}", default-features = false }}
object_store = "0.14"
parquet = {{ version = "58", features = ["arrow", "zstd"] }}
tokio = {{ version = "1", features = ["macros", "rt-multi-thread"] }}
"""
    write_if_changed(project / "Cargo.toml", manifest)
    destination = project / "src/main.rs"
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not destination.exists() or destination.read_bytes() != source.read_bytes():
        shutil.copyfile(source, destination)
    build_env = os.environ.copy()
    build_env["CARGO_TARGET_DIR"] = str(target)
    cargo_dir = str(Path(cargo).resolve().parent)
    build_env["PATH"] = cargo_dir + os.pathsep + build_env.get("PATH", "")
    sibling_rustc = Path(cargo).resolve().with_name("rustc")
    if sibling_rustc.exists():
        build_env["RUSTC"] = str(sibling_rustc)
    subprocess.run(
        [cargo, "build", "--release", "--manifest-path", str(project / "Cargo.toml")],
        cwd=lance_repo,
        env=build_env,
        check=True,
    )
    args.output.mkdir(parents=True, exist_ok=True)
    return subprocess.run(
        [
            str(executable),
            str(args.output.resolve()),
            str(args.rows),
            str(args.batch_rows),
        ],
        check=False,
    ).returncode


if __name__ == "__main__":
    sys.exit(main())
