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

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "gluten_native_tests.py"


class NativeTestTransferTest(unittest.TestCase):
    def test_transferred_tests_run_without_the_original_build(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "old-checkout"
            work = source / "cpp/build/Release/tests"
            work.mkdir(parents=True)
            (work / "data").mkdir()
            (work / "data/answer").write_text("42\n")
            executable = work / "example"
            executable.write_text('#!/bin/sh\ntest "$(cat data/answer)" = 42\n')
            executable.chmod(0o755)
            manifest = root / "tests.json"
            manifest.write_text(
                json.dumps(
                    {
                        "tests": [
                            {
                                "name": "test with ]] and a space",
                                "command": [str(executable)],
                                "properties": [
                                    {"name": "WORKING_DIRECTORY", "value": str(work)}
                                ],
                            }
                        ]
                    }
                )
            )
            archive = root / "tests.tgz"
            subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "pack",
                    str(source),
                    str(manifest),
                    str(archive),
                ],
                check=True,
            )
            destination = root / "new-checkout"
            destination.mkdir()
            subprocess.run(
                ["tar", "-C", str(destination), "-xzf", str(archive)], check=True
            )
            executable.unlink()
            test_directory = root / "ctest"
            subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "prepare",
                    str(destination),
                    str(manifest),
                    str(test_directory),
                ],
                check=True,
            )
            result = subprocess.run(
                [
                    "ctest",
                    "--test-dir",
                    str(test_directory),
                    "--no-tests=error",
                    "--output-on-failure",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("100% tests passed", result.stdout)

    def test_empty_test_manifest_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "tests.json"
            manifest.write_text('{"tests": []}')
            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "pack",
                    str(root),
                    str(manifest),
                    str(root / "tests.tgz"),
                ],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("No native tests", result.stderr)
