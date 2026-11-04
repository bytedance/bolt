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

"""Manage the two source archives used by the arrow/15.0.1-oss recipe."""

import argparse
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile

HERE = Path(__file__).resolve().parent
# bytedance/conan-center-index@50b9807, recipes/arrow/all/conandata.yml
SOURCES = {
    "55db63ed9fd6917b7abfe5d4186c9f532cbe48aa53f4040d57e7c29ad70bcefa": (
        "https://archive.apache.org/dist/arrow/arrow-15.0.1/apache-arrow-15.0.1.tar.gz"
    ),
    "4ed375f69d972a57fdc5ec406c17003a111831d8640d3f1733eccd4b3ff45628": (
        "https://codeload.github.com/substrait-io/substrait/tar.gz/refs/tags/v0.27.0"
    ),
}


def verify(path, expected):
    if path.is_symlink() or not path.is_file():
        raise ValueError("Expected a regular source archive")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected:
        raise ValueError("Source archive checksum mismatch")


def warm(blobs):
    blobs.mkdir(parents=True, exist_ok=True)
    blobs.chmod(blobs.stat().st_mode | 0o055)
    with tempfile.TemporaryDirectory(dir=blobs) as temp:
        for sha, url in SOURCES.items():
            target = blobs / sha
            if target.is_file():
                verify(target, sha)
                target.chmod(target.stat().st_mode | 0o444)
            else:
                stage = Path(temp) / sha
                subprocess.run(
                    [
                        "curl",
                        "-fsSL",
                        "--retry",
                        "3",
                        "--max-time",
                        "300",
                        "-o",
                        stage,
                        url,
                    ],
                    check=True,
                )
                verify(stage, sha)
                stage.chmod(0o644)
                os.replace(stage, target)


def export_cache(blobs, archive):
    with tempfile.TemporaryDirectory(dir=archive.parent) as temp:
        stage = Path(temp) / "snapshot.tar"
        with tarfile.open(stage, "w") as snapshot:
            for sha in SOURCES:
                verify(blobs / sha, sha)
                snapshot.add(blobs / sha, arcname="s/" + sha, recursive=False)
        # Publish a complete snapshot and refuse to overwrite an existing export.
        os.link(stage, archive)


def import_cache(blobs, archive):
    blobs.mkdir(parents=True, exist_ok=True)
    with (
        tempfile.TemporaryDirectory(dir=blobs) as temp,
        tarfile.open(archive) as snapshot,
    ):
        for sha in SOURCES:
            member = snapshot.getmember("s/" + sha)
            if not member.isfile():
                raise ValueError("Expected a regular source archive")
            stage = Path(temp) / sha
            with snapshot.extractfile(member) as src, stage.open("wb") as dst:
                shutil.copyfileobj(src, dst)
            verify(stage, sha)
            stage.chmod(0o644)
        for sha in SOURCES:
            os.replace(Path(temp) / sha, blobs / sha)


def configure_local(seed):
    # CI jobs get two local files and an independent, writable Conan home.
    # Keep source service addresses and source_credentials.json out of public jobs.
    home = Path(
        subprocess.check_output(
            ["conan", "config", "home"], text=True, stderr=subprocess.DEVNULL
        ).strip()
    )
    local = home / "source-backup"
    blobs = local / "s"
    blobs.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=local) as temp:
        for sha in SOURCES:
            stage = Path(temp) / sha
            shutil.copyfile(seed / sha, stage)
            verify(stage, sha)
        for sha in SOURCES:
            os.replace(Path(temp) / sha, blobs / sha)
        conf = home / "global.conf"
        lines = conf.read_text().splitlines() if conf.exists() else []
        keys = {
            "core.sources:download_cache",
            "core.sources:download_urls",
            "core.sources:upload_url",
        }
        lines = [line for line in lines if line.partition("=")[0].strip() not in keys]
        lines.extend(
            [
                f"core.sources:download_cache={local}",
                'core.sources:download_urls=["origin"]',
            ]
        )
        stage = Path(temp) / "global.conf"
        stage.write_text("\n".join(lines) + "\n")
        stage.chmod(0o600)
        os.replace(stage, conf)
    print("Arrow and Substrait sources are ready in the local Conan cache.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "warm", help="verify local sources and fetch any missing archives"
    )
    commands.add_parser(
        "serve", help="warm and start the standalone read-only source service"
    )
    commands.add_parser(
        "configure-local", help="seed a public job from its read-only file mount"
    )
    for name in ("export", "import"):
        commands.add_parser(name).add_argument("archive", type=Path)
    args = parser.parse_args()
    if args.command == "configure-local":
        try:
            configure_local(
                Path(os.environ.get("BOLT_ARROW_SEED_DIR", "/opt/bolt-arrow-sources"))
            )
        except (OSError, ValueError, subprocess.CalledProcessError):
            raise SystemExit(
                "Check the read-only source mount and the job's Conan home."
            ) from None
        return
    cache = Path(
        os.environ.get("BOLT_ARROW_CACHE_DIR", "/data/conan-source-cache")
    ).resolve()
    blobs = cache / "s"
    if args.command == "export":
        export_cache(blobs, args.archive)
    elif args.command == "import":
        import_cache(blobs, args.archive)
    else:
        warm(blobs)
        if args.command == "serve":
            subprocess.run(
                [
                    "docker",
                    "compose",
                    "-p",
                    "bolt-arrow-cache",
                    "-f",
                    str(HERE / "compose.arrow-cache.yml"),
                    "up",
                    "-d",
                    "--wait",
                    "bolt-source-cache",
                ],
                env={**os.environ, "BOLT_ARROW_CACHE_DIR": str(cache)},
                check=True,
            )


if __name__ == "__main__":
    main()
