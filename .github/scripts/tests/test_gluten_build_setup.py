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

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[3]


class BuildSetupTest(unittest.TestCase):
    def test_profile_setup_honors_isolated_conan_home(self):
        action = yaml.safe_load(
            (ROOT / ".github/actions/bolt-build-base/action.yml").read_text()
        )
        step = next(
            s for s in action["runs"]["steps"] if s.get("name") == "Setup conan profile"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            home = root / "home"
            home.mkdir()
            conan = root / "isolated-conan"
            result = subprocess.run(
                ["bash", "-e", "-o", "pipefail", "-c", step["run"]],
                cwd=ROOT,
                env={**os.environ, "HOME": str(home), "CONAN_HOME": str(conan)},
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn(
                "core.download:parallel=1", (conan / "global.conf").read_text()
            )
            self.assertFalse((home / ".conan2").exists())
