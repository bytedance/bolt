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

"""Deploy an Arrow source mirror and seed job-private Conan download caches."""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen


def load_sources(manifest):
    data = json.loads(manifest.read_text())
    sources = data.get("sources") if isinstance(data, dict) else None
    if not isinstance(sources, list) or not sources:
        raise ValueError("Source manifest must contain a nonempty sources list")
    seen = set()
    for source in sources:
        if not isinstance(source, dict):
            raise ValueError("Source entry must be an object")
        name, sha, urls = (source.get(key) for key in ("name", "sha256", "urls"))
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(sha, str)
            or not re.fullmatch(r"[0-9a-f]{64}", sha)
            or sha in seen
            or not isinstance(urls, list)
            or not urls
            or not all(isinstance(url, str) and url for url in urls)
        ):
            raise ValueError(
                "Source entry needs a name, unique SHA-256 and download URLs"
            )
        seen.add(sha)
    return sources


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


def check_seed(seed, sources):
    for source in sources:
        path = seed / source["sha256"]
        if not path.is_file() or digest(path) != source["sha256"]:
            raise ValueError(f"Missing or corrupt cache entry: {source['name']}")


def warm(seed, sources):
    seed.mkdir(parents=True, exist_ok=True)
    seed.chmod(0o755)
    for source in sources:
        target = seed / source["sha256"]
        if target.is_symlink():
            raise ValueError("Refusing a symlink cache entry")
        if target.is_file() and digest(target) == source["sha256"]:
            target.chmod(0o644)
            print(f"Cached: {source['name']}", flush=True)
            continue
        for url in source["urls"]:
            print(f"Downloading: {source['name']}", flush=True)
            with tempfile.NamedTemporaryFile(dir=seed, delete=False) as temp:
                temporary = Path(temp.name)
                try:
                    with urlopen(url, timeout=60) as response:
                        shutil.copyfileobj(response, temp)
                    temp.flush()
                    if digest(temporary) != source["sha256"]:
                        raise ValueError("Source checksum mismatch")
                    os.fchmod(temp.fileno(), 0o644)
                    os.replace(temporary, target)
                    break
                except (OSError, URLError, ValueError):
                    # Do not put operator URLs or machine details in CI logs.
                    print(f"Download failed: {source['name']}", flush=True)
                finally:
                    temporary.unlink(missing_ok=True)
        else:
            raise ValueError(f"Unable to seed {source['name']}; no valid download")
    check_seed(seed, sources)


def configure_local(seed, home, sources, quiet=False):
    """Copy blobs, never share Conan's writable metadata/locks with the seed."""
    check_seed(seed, sources)
    cache = home / "source-backup"
    for source in sources:
        target = cache / "s" / source["sha256"]
        atomic_copy(seed / source["sha256"], target)
        # Stale metadata/dirty markers could force an unnecessary re-download.
        target.with_suffix(".json").unlink(missing_ok=True)
        target.with_suffix(".dirty").unlink(missing_ok=True)
    conf = home / "global.conf"
    settings = {
        "core.sources:download_cache": str(cache),
        "core.sources:download_urls": '["origin"]',
    }
    lines = conf.read_text().splitlines() if conf.exists() else []
    lines = [line for line in lines if line.partition("=")[0].strip() not in settings]
    lines.extend(f"{key}={value}" for key, value in settings.items())
    conf.write_text("\n".join(lines) + "\n")
    if not quiet:
        print("Source archives configured in the private Conan source cache")


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
    conan = Path(os.environ.get("CONAN_HOME", str(home / ".conan2")))
    try:
        marker = conan / ".bolt-source-cache-ready"
        if not marker.exists():
            sources = load_sources(root / "s/manifest.json")
            configure_local(root / "s", conan, sources, quiet=True)
            marker.touch()
    except (OSError, ValueError):
        pass
    try:
        marker = home / ".cache/.bolt-home-cache-ready"
        if (root / "home").is_dir() and not marker.exists():
            copy_home_cache(root / "home", home)
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.touch()
    except OSError:
        pass


def build_image(root, image, output, home_seed, sources):
    if not output or output == image:
        raise ValueError("Use --output-image with a distinct tag for verification")
    check_seed(root / "s", sources)
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
        for source in sources:
            atomic_copy(root / "s" / source["sha256"], seed / "s" / source["sha256"])
        # The image contains no operator download URLs.
        (seed / "s/manifest.json").write_text(
            json.dumps(
                {
                    "sources": [
                        {
                            "name": source["sha256"],
                            "sha256": source["sha256"],
                            "urls": ["https://invalid.invalid/"],
                        }
                        for source in sources
                    ]
                }
            )
        )
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


def make_server(seed, bind, port, sources):
    check_seed(seed, sources)
    allowed = {"/s/" + source["sha256"]: seed / source["sha256"] for source in sources}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_HEAD(self):
            self.send_content(False)

        def do_GET(self):
            self.send_content(True)

        def send_content(self, body):
            if self.path == "/healthz":
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            path = allowed.get(self.path)
            if path is None or path.is_symlink() or not path.is_file():
                self.send_error(404)
                return
            with path.open("rb") as source:
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header(
                    "Content-Length", str(os.fstat(source.fileno()).st_size)
                )
                self.end_headers()
                if body:
                    shutil.copyfileobj(source, self.wfile)

    return ThreadingHTTPServer((bind, port), Handler)


def prepare(root, manifest, sources):
    root.mkdir(parents=True, exist_ok=True)
    root.chmod(0o755)
    warm(root / "s", sources)
    atomic_copy(manifest, root / "s" / "manifest.json")
    atomic_copy(manifest, root / "arrow-cache-sources.json")
    directory = Path(__file__).resolve().parent
    for name in (
        "arrow-cache.py",
        "compose.arrow-cache.yml",
    ):
        source = directory / name
        if source.resolve() != (root / name).resolve():
            atomic_copy(source, root / name)


def compose(root, *arguments):
    # Explicit project/file ignore stale COMPOSE_FILE settings in a web shell.
    subprocess.run(
        [
            "docker",
            "compose",
            "--project-name",
            os.environ.get("ARROW_CACHE_PROJECT", "bolt-arrow-cache"),
            "--file",
            str(root / "compose.arrow-cache.yml"),
            *arguments,
        ],
        env={
            **os.environ,
            "BOLT_ARROW_CACHE_DIR": str(root),
            "COMPOSE_REMOVE_ORPHANS": "false",
            "COMPOSE_IGNORE_ORPHANS": "true",
        },
        check=True,
    )


def check_conan(seed, sources):
    home = Path(os.environ["CONAN_HOME"]).resolve()
    configure_local(seed, home, sources)
    with tempfile.TemporaryDirectory() as directory:
        recipe = Path(directory) / "conanfile.py"
        body = (
            "from conan import ConanFile\nfrom conan.tools.files import download\n"
            "class Probe(ConanFile):\n    name = 'arrow-cache-probe'\n    version = '1'\n"
            "    def source(self):\n"
        )
        for source in sources:
            sha = source["sha256"]
            body += f"        download(self, 'https://invalid.invalid/{sha}', '{sha}', sha256='{sha}')\n"
        recipe.write_text(body)
        subprocess.run(["conan", "source", directory], check=True)
        check_seed(Path(directory), sources)
    print("PASS: Conan consumed configured source archives without network access")


def verify(root, image, port, manifest, sources):
    check_seed(root / "s", sources)
    host = os.environ.get("ARROW_CACHE_BIND", "127.0.0.1")
    host = {"0.0.0.0": "127.0.0.1", "::": "::1"}.get(host, host)
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    for source in sources:
        with urlopen(
            f"http://{host}:{port}/s/{source['sha256']}", timeout=15
        ) as response:
            if hashlib.sha256(response.read()).hexdigest() != source["sha256"]:
                raise ValueError("HTTP source checksum mismatch")
    print("PASS: HTTP service returned all configured source archives", flush=True)
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network=none",
            "--user",
            "65534:65534",
            "--env",
            "HOME=/tmp",
            "--env",
            "CONAN_HOME=/tmp/conan",
            "--mount",
            f"type=bind,src={root / 's'},dst=/opt/bolt-arrow-sources,readonly",
            "--mount",
            f"type=bind,src={root / 'arrow-cache.py'},dst=/opt/bolt-arrow-cache.py,readonly",
            "--mount",
            f"type=bind,src={manifest.resolve()},dst=/opt/source-manifest.json,readonly",
            "--entrypoint",
            "python3",
            image,
            "/opt/bolt-arrow-cache.py",
            "check-conan",
            "--manifest",
            "/opt/source-manifest.json",
        ],
        check=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "prepare",
            "up",
            "verify",
            "stop",
            "serve",
            "configure-local",
            "check-conan",
            "auto-configure",
            "image",
        ),
    )
    parser.add_argument(
        "--image",
        default=os.environ.get("BOLT_CI_IMAGE", "localhost:5000/bolt-ci:20260921"),
    )
    parser.add_argument(
        "--source-dir", type=Path, default=Path("/opt/bolt-arrow-sources")
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        help="Source archive manifest (defaults to the shipped or deployed manifest)",
    )
    parser.add_argument("--bind", default="0.0.0.0")
    parser.add_argument(
        "--output-image", help="New cache image tag; never overwrites the base"
    )
    parser.add_argument(
        "--home-seed",
        type=Path,
        help="Trusted prewarmed HOME (dependency directories only)",
    )
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    if args.command == "auto-configure":
        auto_configure(args.source_dir)
        return
    root = Path(
        os.environ.get(
            "BOLT_ARROW_CACHE_DIR",
            str(Path(os.environ.get("HOST_DATA_DIR", "/data")) / "conan-source-cache"),
        )
    ).resolve()
    if args.command == "stop":
        compose(root, "stop", "bolt-source-cache")
        return
    manifest = args.manifest
    if manifest is None:
        if args.command in ("prepare", "up"):
            manifest = Path(__file__).with_name("arrow-cache-sources.json")
        elif args.command in ("verify", "image"):
            manifest = root / "s" / "manifest.json"
        else:
            manifest = args.source_dir / "manifest.json"
    sources = load_sources(manifest)
    if args.command in ("prepare", "up"):
        prepare(root, manifest, sources)
        if args.command == "up":
            compose(
                root,
                "up",
                "-d",
                "--wait",
                "--wait-timeout",
                "90",
                "--force-recreate",
                "bolt-source-cache",
            )
        print("Cache prepared; use image to enable automatic CI cache consumption")
    elif args.command == "image":
        build_image(root, args.image, args.output_image, args.home_seed, sources)
    elif args.command == "verify":
        verify(
            root,
            args.image,
            int(os.environ.get("ARROW_CACHE_PORT", "9301")),
            manifest,
            sources,
        )
    elif args.command == "configure-local":
        configure_local(
            args.source_dir, Path(os.environ["CONAN_HOME"]).resolve(), sources
        )
    elif args.command == "check-conan":
        check_conan(args.source_dir, sources)
    else:
        with make_server(args.source_dir, args.bind, args.port, sources) as server:
            server.serve_forever()


if __name__ == "__main__":
    main()
