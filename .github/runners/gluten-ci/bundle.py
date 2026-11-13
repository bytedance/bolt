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

"""Create and verify an offline deployment from explicit Git revisions."""

import argparse
import json
import os
import subprocess
import tarfile
import tempfile
from pathlib import Path, PurePosixPath

from bolt_ci.config import HELPERS, digest_file, load_matrix
from bolt_ci.policy import valid_sha

MANIFEST = "MANIFEST.json"
DRIVER_SHA = "72f74589df524b2f4a7695b4f10c5b13a95812e8"
DRIVER_SCRIPTS = ".github/scripts"
DEPLOY = ".github/runners/gluten-ci"
FILES = (
    ".dockerignore",
    "Dockerfile",
    "README.md",
    "bolt_ci",
    "bundle.py",
    "ci.sh",
    "compose.yml",
    "env.example",
    "network_guard.py",
    "prepare.py",
)


def inventory(root):
    files = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or not (path.is_file() or path.is_dir()):
            raise ValueError("Bundle contains an unsupported file")
        name = path.relative_to(root).as_posix()
        if path.is_file() and name != MANIFEST:
            files[name] = {
                "sha256": digest_file(path),
                "executable": bool(path.stat().st_mode & 0o111),
            }
    return files


def write_manifest(root, bolt, helper, driver):
    manifest = {
        "schema": 2,
        "bolt_commit": bolt,
        "helper_commit": helper,
        "driver_commit": driver,
        "files": inventory(root),
    }
    (root / MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")


def verify_manifest(root):
    path = root / MANIFEST
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 4_000_000:
        raise ValueError("Deployment manifest missing or invalid")
    manifest = json.loads(path.read_bytes())
    if (
        manifest["schema"] != 2
        or manifest["helper_commit"] != manifest["bolt_commit"]
        or any(
            not valid_sha(manifest[key])
            for key in ("bolt_commit", "helper_commit", "driver_commit")
        )
        or manifest["files"] != inventory(root)
    ):
        raise ValueError("Deployment bundle checksum or file set does not match")
    return manifest


def git(repo, *args, data=None):
    return subprocess.run(
        ["git", "-C", str(repo), "-c", "core.hooksPath=/dev/null", *args],
        input=data,
        stdout=subprocess.PIPE,
        check=True,
        env={
            "PATH": os.environ["PATH"],
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_NO_LAZY_FETCH": "1",
        },
    ).stdout


def export(repo, revision, target, paths):
    # The repositories can mark CI files export-ignore in .gitattributes.
    # Copy only committed allowlisted blobs, preserving their executable bits.
    if (
        git(repo, "rev-parse", "--verify", f"{revision}^{{commit}}").strip().decode()
        != revision
    ):
        raise ValueError("Bundle revision is not a commit")
    entries = git(repo, "ls-tree", "-r", "-z", revision, "--", *paths).split(b"\0")
    found = set()
    total_bytes = 0
    for entry in filter(None, entries):
        description, raw_name = entry.split(b"\t", 1)
        mode, kind, digest = description.split()
        name = raw_name.decode()
        selection = next(
            (path for path in paths if name == path or name.startswith(path + "/")),
            None,
        )
        if selection is None or kind != b"blob" or mode not in {b"100644", b"100755"}:
            raise ValueError("Bundle path must refer to a regular committed file")
        relative = PurePosixPath(name)
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or relative.as_posix() != name
        ):
            raise ValueError("Invalid path in deployment Git tree")
        if "/bolt_ci/" in name and relative.suffix != ".py":
            raise ValueError(
                "The deployment package may only export Python controller files"
            )
        size = int(git(repo, "cat-file", "-s", digest.decode()))
        total_bytes += size
        if size > 4 * 1024**2 or total_bytes > 32 * 1024**2:
            raise ValueError("Deployment Git files exceed the bundle size limits")
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("xb") as stream:
            stream.write(git(repo, "cat-file", "blob", digest.decode()))
        destination.chmod(0o755 if mode == b"100755" else 0o644)
        found.add(selection)
    if found != set(paths):
        raise ValueError(
            "A required deployment path is missing from the pinned revision"
        )


def export_driver(repo, target):
    # Export the pin and its trees and scripts. The worker needs no origin,
    # hooks, reflog or credentials for this read-only sparse checkout.
    target.mkdir()
    git(target, "init", "--template=", "--quiet")
    paths = git(repo, "ls-tree", "-r", "-t", "-z", DRIVER_SHA).split(b"\0")
    objects = {
        DRIVER_SHA.encode(),
        git(repo, "rev-parse", f"{DRIVER_SHA}^{{tree}}").strip(),
    }
    for entry in filter(None, paths):
        description, name = entry.split(b"\t", 1)
        _, kind, digest = description.split()
        if kind == b"tree" or name.startswith(DRIVER_SCRIPTS.encode() + b"/"):
            objects.add(digest)
    packed = git(
        repo, "pack-objects", "--stdout", data=b"\n".join(sorted(objects)) + b"\n"
    )
    git(target, "index-pack", "--stdin", data=packed)
    (target / ".git/shallow").write_text(DRIVER_SHA + "\n")
    git(target, "update-ref", "--no-deref", "HEAD", DRIVER_SHA)
    git(target, "sparse-checkout", "set", "--no-cone", f"/{DRIVER_SCRIPTS}/")
    git(target, "read-tree", "-mu", "HEAD")
    required = target / DRIVER_SCRIPTS / "DiscoverGlutenSuites.java"
    if (
        not required.is_file()
        or git(target, "rev-parse", "HEAD").strip().decode() != DRIVER_SHA
    ):
        raise ValueError("Pinned driver scripts are missing")


def create(bolt_repo, bolt_revision, driver_repo, output):
    if not valid_sha(bolt_revision):
        raise ValueError("Bundle inputs must be full commit IDs")
    with tempfile.TemporaryDirectory(dir=output.parent) as temp:
        workspace = Path(temp)
        root = workspace / "bolt-gluten-ci"
        root.mkdir()
        export(
            bolt_repo, bolt_revision, workspace, [f"{DEPLOY}/{path}" for path in FILES]
        )
        (workspace / DEPLOY).rename(root / "deploy")
        export(bolt_repo, bolt_revision, root / "helpers", HELPERS)
        load_matrix(root / "helpers")
        export_driver(driver_repo, root / "driver")
        write_manifest(root, bolt_revision, bolt_revision, DRIVER_SHA)
        verify_manifest(root)
        candidate = workspace / "bundle.tar.gz"
        with tarfile.open(candidate, "w:gz") as archive:
            archive.add(root, arcname=root.name)
        os.link(candidate, output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("root", type=Path)
    pack = commands.add_parser("create")
    for name in ("bolt-repo", "driver-repo", "output"):
        pack.add_argument(f"--{name}", type=Path, required=True)
    for name in ("bolt-revision",):
        pack.add_argument(f"--{name}", required=True)
    args = parser.parse_args()
    if args.command == "verify":
        verify_manifest(args.root)
        print("Deployment bundle verified")
        return
    create(
        args.bolt_repo,
        args.bolt_revision,
        args.driver_repo,
        args.output,
    )
    print("Deployment bundle created")


if __name__ == "__main__":
    main()
