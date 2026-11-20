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

"""Prewarm local source archives and mount them into runner tool caches."""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path


def digest(path):
    if path.is_symlink():
        raise ValueError("Cache entries must be regular files, not symlinks")
    with path.open("rb") as source:
        result = hashlib.sha256()
        while block := source.read(1024 * 1024):
            result.update(block)
    return result.hexdigest()


def atomic_copy(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as temp:
        temporary = Path(temp.name)
        try:
            with source.open("rb") as input_file:
                shutil.copyfileobj(input_file, temp)
            temp.flush()
            os.fchmod(temp.fileno(), 0o644)
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)


def prepare(root, image, version, cci_ref, seed_dir):
    cache = root / "conan-source-cache"
    cache.mkdir(parents=True, exist_ok=True)
    # Reuse verified archives from older ECS deployments, without their metadata.
    if seed_dir and seed_dir.is_dir():
        for source in seed_dir.iterdir():
            if re.fullmatch(r"[0-9a-f]{64}", source.name) and source.is_file():
                if digest(source) == source.name:
                    atomic_copy(source, cache / "s" / source.name)
    installer = Path(__file__).resolve().parents[2] / "scripts/install-bolt-deps.sh"
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            "root",
            "-e",
            "BASH_ENV=",
            "-e",
            "CONAN_HOME=/tmp/conan",
            "-v",
            f"{cache}:/source-cache",
            "-v",
            f"{installer}:/opt/install-bolt-deps.sh:ro",
            "--entrypoint",
            "/bin/bash",
            image,
            "-euc",
            "mkdir -p /tmp/bolt/scripts; git init -q /tmp/bolt; "
            "cp /opt/install-bolt-deps.sh /tmp/bolt/scripts/; "
            'bash /tmp/bolt/scripts/install-bolt-deps.sh --branch "$1"; '
            'conan source "$CONAN_HOME/conan-center-index/recipes/arrow/all" '
            '--version="$2" -cc core.sources:download_cache=/source-cache',
            "prewarm",
            cci_ref,
            version,
        ],
        check=True,
    )
    # Publish only verified blobs, never Conan metadata, URLs or lock files.
    published = cache / "archives"
    published.mkdir(exist_ok=True)
    published.chmod(0o755)
    for directory in (cache, cache / "s"):
        directory.chmod(0o755)
    for archive in (cache / "s").iterdir():
        if re.fullmatch(r"[0-9a-f]{64}", archive.name) and archive.is_file():
            if digest(archive) != archive.name:
                raise ValueError("Source backup checksum mismatch")
            archive.chmod(0o644)
            atomic_copy(archive, published / archive.name)
    atomic_copy(
        Path(__file__).with_name("compose.local-sources.yml"),
        cache / "compose.local-sources.yml",
    )


def copy_home_cache(source, destination):
    """Only trusted dependency caches, never credentials or job outputs."""
    for relative in (".m2/repository", ".cache/pip", ".cache/spark"):
        base = source / relative
        parts = Path(relative).parts
        if not base.is_dir() or any(
            source.joinpath(*parts[:i]).is_symlink() for i in range(1, len(parts) + 1)
        ):
            continue
        for directory, dirs, files in os.walk(base, followlinks=False):
            parent = Path(directory)
            links = [name for name in dirs if (parent / name).is_symlink()]
            dirs[:] = [name for name in dirs if name not in links]
            for name in files + links:
                item = parent / name
                target = destination / item.relative_to(source)
                if target.exists() or target.is_symlink():
                    continue
                if item.is_symlink():
                    resolved = item.resolve()
                    if resolved.is_relative_to(base.resolve()):
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.symlink_to(
                            os.path.relpath(resolved, item.parent.resolve())
                        )
                elif item.is_file():
                    atomic_copy(item, target)
                    target.chmod(0o755 if item.stat().st_mode & 0o111 else 0o644)


def auto_configure(root):
    """Optional image bootstrap; unavailable caches must not break a build."""
    home = Path(os.environ["HOME"])
    try:
        marker = home / ".cache/.bolt-home-cache-ready"
        if (root / "home").is_dir() and not marker.exists():
            copy_home_cache(root / "home", home)
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.touch()
    except OSError:
        pass


def build_image(image, output, home_seed):
    if not output or output == image:
        raise ValueError(
            "Use --output-image with a distinct tag to preserve the base image"
        )
    config = json.loads(subprocess.check_output(["docker", "image", "inspect", image]))[
        0
    ]["Config"]
    if any(
        value.startswith("BASH_ENV=") and value != "BASH_ENV="
        for value in config.get("Env", [])
    ):
        raise ValueError(
            "Base image already defines BASH_ENV; keep its bootstrap intact"
        )
    with tempfile.TemporaryDirectory() as directory:
        context = Path(directory)
        seed = context / "cache"
        seed.mkdir()
        if home_seed:
            copy_home_cache(home_seed, seed / "home")
        atomic_copy(Path(__file__), seed / "arrow-cache.py")
        (seed / "init.sh").write_text(
            "python3 /opt/bolt-ci-cache/arrow-cache.py auto-configure "
            "--source-dir /opt/bolt-ci-cache >/dev/null 2>&1 || :\n"
        )
        (context / "Dockerfile").write_text(
            "ARG BASE_IMAGE=scratch\nFROM ${BASE_IMAGE}\n"
            "COPY --chmod=0755 cache/ /opt/bolt-ci-cache/\n"
            "ENV BASH_ENV=/opt/bolt-ci-cache/init.sh\n"
        )
        subprocess.run(
            [
                "docker",
                "build",
                "--build-arg",
                f"BASE_IMAGE={image}",
                "--tag",
                output,
                str(context),
            ],
            check=True,
        )


def deploy(root, project, compose_files):
    compose = ["docker", "compose", "--project-name", project]
    for name in compose_files:
        compose.extend(["--file", str(name.resolve(strict=True))])
    compose.extend(
        ["--file", str(root / "conan-source-cache/compose.local-sources.yml")]
    )
    env = {**os.environ, "HOST_DATA_DIR": str(root), "COMPOSE_REMOVE_ORPHANS": "false"}
    subprocess.run(
        [
            *compose,
            "up",
            "-d",
            "--no-deps",
            "--no-build",
            "--pull",
            "never",
            "runner-medium",
            "runner-small",
        ],
        env=env,
        check=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "up", "image", "auto-configure"))
    parser.add_argument(
        "--image",
        default=os.environ.get("BOLT_CI_IMAGE", "localhost:5000/bolt-ci:20260921"),
    )
    parser.add_argument("--version", default="15.0.1-oss", help="Arrow recipe version")
    parser.add_argument(
        "--cci-ref", default="main", help="Recipe index branch or tag used by CI"
    )
    parser.add_argument("--project", default="runners", help="Existing Compose project")
    parser.add_argument(
        "--compose-file",
        action="append",
        type=Path,
        help="Original Compose file; repeat for every existing override, in order",
    )
    parser.add_argument(
        "--seed-dir", type=Path, help="Existing source-backup/s to migrate"
    )
    parser.add_argument(
        "--output-image", help="Optional HOME dependency cache image tag"
    )
    parser.add_argument(
        "--home-seed", type=Path, help="Trusted prewarmed HOME (no Arrow sources)"
    )
    parser.add_argument("--source-dir", type=Path, default=Path("/opt/bolt-ci-cache"))
    args = parser.parse_args()
    if args.command == "auto-configure":
        auto_configure(args.source_dir)
    elif args.command == "image":
        if not args.home_seed or not args.home_seed.is_dir():
            parser.error(
                "image requires --home-seed; Arrow sources use the local runner mount"
            )
        build_image(args.image, args.output_image, args.home_seed)
    else:
        if args.command == "up" and not args.compose_file:
            parser.error("up requires --compose-file with the original stack files")
        root = Path(os.environ.get("HOST_DATA_DIR", "/data")).resolve()
        prepare(root, args.image, args.version, args.cci_ref, args.seed_dir)
        if args.command == "up":
            deploy(root, args.project, args.compose_file)


if __name__ == "__main__":
    main()
