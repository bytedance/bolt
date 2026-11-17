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


def configure_local(seed, home, sources):
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
    print("Source archives configured in the private Conan source cache")


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
        "compose.arrow-cache-runners.yml",
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
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
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
        elif args.command == "verify":
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
        print(
            "Cache prepared; apply compose.arrow-cache-runners.yml to idle runners to enable CI mounts"
        )
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
