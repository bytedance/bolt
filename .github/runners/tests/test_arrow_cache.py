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

"""Arrow seed integrity, HTTP boundaries and real Conan cache consumption."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import urlopen

SCRIPT = Path(__file__).resolve().parents[1] / "arrow-cache.py"


class ArrowCacheTest(unittest.TestCase):
    def setUp(self):
        self.assertTrue(
            SCRIPT.is_file(), "The deployable Arrow cache entry point is missing"
        )
        spec = importlib.util.spec_from_file_location("arrow_cache", SCRIPT)
        self.cache = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.cache)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.body = b"verified source archive\n"
        self.sha = hashlib.sha256(self.body).hexdigest()
        self.origin = self.root / "origin"
        self.origin.write_bytes(self.body)
        self.sources = (
            {"name": "fixture", "sha256": self.sha, "urls": [self.origin.as_uri()]},
        )
        self.seed = self.root / "seed"

    def test_custom_manifest_survives_installation_without_source_checkout(self):
        manifest = self.root / "resources.json"
        manifest.write_text(json.dumps({"sources": self.sources}))
        deployed = self.root / "installed"
        env = os.environ | {"BOLT_ARROW_CACHE_DIR": str(deployed)}
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "prepare", "--manifest", str(manifest)],
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        manifest.unlink()
        self.origin.unlink()
        home = self.root / "job-conan"
        result = subprocess.run(
            [
                sys.executable,
                str(deployed / "arrow-cache.py"),
                "configure-local",
                "--source-dir",
                str(deployed / "s"),
            ],
            env=env | {"CONAN_HOME": str(home)},
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((home / "source-backup/s" / self.sha).read_bytes(), self.body)
        self.assertEqual(
            self.cache.load_sources(deployed / "s/manifest.json"), list(self.sources)
        )

    def test_invalid_manifest_cannot_create_cache_paths(self):
        manifest = self.root / "resources.json"
        for sources in (
            [],
            [dict(self.sources[0], sha256="../outside")],
            [dict(self.sources[0], urls=[])],
            [self.sources[0]] * 2,
        ):
            with self.subTest(sources=sources):
                manifest.write_text(json.dumps({"sources": sources}))
                with self.assertRaises(ValueError):
                    self.cache.load_sources(manifest)

    def test_warm_reuses_valid_content_and_repairs_corruption(self):
        self.cache.warm(self.seed, self.sources)
        self.origin.unlink()
        self.cache.warm(self.seed, self.sources)
        self.assertEqual((self.seed / self.sha).read_bytes(), self.body)
        (self.seed / self.sha).write_bytes(b"corrupt")
        self.origin.write_bytes(self.body)
        self.cache.warm(self.seed, self.sources)
        self.assertEqual((self.seed / self.sha).read_bytes(), self.body)

    def test_bad_download_never_replaces_existing_content(self):
        self.seed.mkdir()
        target = self.seed / self.sha
        target.write_bytes(b"previous")
        self.origin.write_bytes(b"incorrect download")
        with self.assertRaises(ValueError):
            self.cache.warm(self.seed, self.sources)
        self.assertEqual(target.read_bytes(), b"previous")
        self.assertEqual(list(self.seed.iterdir()), [target])

    def test_seed_symlink_is_rejected_without_touching_target(self):
        self.seed.mkdir()
        (self.seed / self.sha).symlink_to(self.origin)
        with self.assertRaises(ValueError):
            self.cache.warm(self.seed, self.sources)
        self.assertEqual(self.origin.read_bytes(), self.body)

    def test_configure_preserves_remote_and_copies_to_private_cache(self):
        self.cache.warm(self.seed, self.sources)
        home = self.root / "conan"
        home.mkdir()
        (home / "remotes.json").write_text('{"remotes": []}')
        (home / "global.conf").write_text(
            'core.download:parallel=4\ncore.sources:download_urls=["http://private/"]\n'
        )
        self.cache.configure_local(self.seed, home, self.sources)
        self.cache.configure_local(self.seed, home, self.sources)
        conf = (home / "global.conf").read_text()
        self.assertIn("core.download:parallel=4", conf)
        self.assertNotIn("private", conf)
        self.assertEqual(conf.count("core.sources:download_cache="), 1)
        self.assertEqual((home / "remotes.json").read_text(), '{"remotes": []}')
        copied = home / "source-backup" / "s" / self.sha
        self.assertEqual(copied.read_bytes(), self.body)
        copied.write_bytes(b"job mutation")
        self.assertEqual((self.seed / self.sha).read_bytes(), self.body)

    def test_http_serves_verified_archives_but_no_directory_or_symlinks(self):
        self.cache.warm(self.seed, self.sources)
        with self.cache.make_server(self.seed, "127.0.0.1", 0, self.sources) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base = f"http://127.0.0.1:{server.server_port}"
                with urlopen(base + "/s/" + self.sha) as response:
                    self.assertEqual(response.read(), self.body)
                with urlopen(base + "/healthz") as response:
                    self.assertEqual(response.status, 200)
                for path in ("/", "/s/", "/../origin", "/s/" + "0" * 64):
                    with self.assertRaises(HTTPError) as error:
                        urlopen(base + path)
                    self.assertEqual(error.exception.code, 404)
                (self.seed / self.sha).unlink()
                (self.seed / self.sha).symlink_to(self.origin)
                with self.assertRaises(HTTPError):
                    urlopen(base + "/s/" + self.sha)
            finally:
                server.shutdown()
                thread.join()

    def test_compose_disables_inherited_orphan_removal(self):
        # The Docker boundary fails if a cache-only up could remove existing runners.
        binary = self.root / "docker"
        binary.write_text(
            '#!/bin/sh\n[ "$COMPOSE_REMOVE_ORPHANS" = false ] || exit 99\n'
        )
        binary.chmod(0o755)
        with patch.dict(
            os.environ,
            {
                "PATH": str(self.root) + os.pathsep + os.environ["PATH"],
                "COMPOSE_REMOVE_ORPHANS": "true",
            },
        ):
            self.cache.compose(self.root, "up", "-d", "bolt-source-cache")

    def test_real_conan_consumes_seed_with_unreachable_origin(self):
        self.cache.warm(self.seed, self.sources)
        home = self.root / "conan"
        self.cache.configure_local(self.seed, home, self.sources)
        recipe = self.root / "recipe"
        recipe.mkdir()
        (recipe / "conanfile.py").write_text(
            "from conan import ConanFile\nfrom conan.tools.files import download\n"
            "class Recipe(ConanFile):\n"
            '    name = "cache-probe"\n    version = "1"\n'
            "    def source(self):\n"
            f'        download(self, "http://127.0.0.1:1/unreachable", "result", sha256="{self.sha}")\n'
        )
        result = subprocess.run(
            ["conan", "source", str(recipe)],
            env={**os.environ, "CONAN_HOME": str(home)},
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(
            "retrieved from local download cache", result.stdout + result.stderr
        )
        self.assertEqual((recipe / "result").read_bytes(), self.body)


if __name__ == "__main__":
    unittest.main()
