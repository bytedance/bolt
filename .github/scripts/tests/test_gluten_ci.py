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

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
import zipfile


HELPER = Path(__file__).resolve().parents[1] / "gluten_ci.sh"


class GlutenCiTest(unittest.TestCase):
    def shell(self, command, **env):
        return subprocess.run(
            ["bash", "-c", 'source "$HELPER"; ' + command],
            env={
                **os.environ,
                "HELPER": str(HELPER),
                "GITHUB_ACTIONS": "false",
                "GITHUB_ENV": "",
                "GLUTEN_COMPILE_DEADLINE": "",
                **env,
            },
            text=True,
            capture_output=True,
        )

    def test_expired_compile_budget_does_not_start_command(self):
        result = self.shell("ci_compile printf started", GLUTEN_COMPILE_DEADLINE="1")
        self.assertEqual(result.returncode, 124, result.stderr)
        self.assertNotIn("started", result.stdout)

    def test_compile_preserves_failure_status(self):
        result = self.shell(
            "ci_compile bash -c 'exit 42'",
            GLUTEN_COMPILE_DEADLINE=str(int(time.time()) + 60),
        )
        self.assertEqual(result.returncode, 42, result.stderr)

    def test_deadline_terminates_slow_compile(self):
        start = time.monotonic()
        result = self.shell(
            "ci_compile sleep 30",
            GLUTEN_COMPILE_DEADLINE=str(int(time.time()) + 2),
        )
        self.assertEqual(result.returncode, 124, result.stderr)
        self.assertLess(time.monotonic() - start, 10)

    def test_deadline_also_terminates_compile_children(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = str(Path(directory) / "survived")
            result = self.shell(
                "ci_compile bash -c '(sleep 3; touch \"$MARKER\") & wait'",
                GLUTEN_COMPILE_DEADLINE=str(int(time.time()) + 1),
                MARKER=marker,
            )
            self.assertEqual(result.returncode, 124, result.stderr)
            time.sleep(3)
            self.assertFalse(Path(marker).exists(), "Compiler child survived deadline")

    def test_spark_shards_do_not_execute_backend_again(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gluten = root / "gluten"
            gluten.mkdir()
            result = self.shell(
                r"""ci_runtime() { "$@"; }
                fake_maven() {
                  local projects=all
                  while (($#)); do
                    if [[ "$1" == -pl ]]; then projects="$2"; break; fi
                    shift
                  done
                  if [[ "$projects" != *'!backends-bolt'* &&
                    ( "$projects" == all || "$projects" == *backends-bolt* ) ]]; then
                    touch "$GLUTEN_DIR/backend-executed"
                  fi
                  if [[ "$projects" == "gluten-ut/spark35" || "$projects" == "backends-bolt,gluten-ut/spark35" ]]; then
                    mkdir -p "$GLUTEN_DIR/gluten-ut/spark35/target/surefire-reports"
                    printf '<testsuite><testcase name="sql"/></testsuite>' > "$GLUTEN_DIR/gluten-ut/spark35/target/surefire-reports/TEST-Sql.xml"
                  fi
                }
                ci_maven_test spark-1 reactor fake_maven surefire:test scalatest:test
                """,
                GLUTEN_DIR=str(gluten),
                GLUTEN_REPORTS_DIR=str(root / "reports"),
                SPARK_MODULE="spark35",
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse((gluten / "backend-executed").exists())

    def test_runtime_finds_transferred_native_libraries(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gluten = root / "gluten"
            library = gluten / "cpp/build/package/bolt/linux/amd64"
            library.mkdir(parents=True)
            (root / "library.c").write_text("int answer(void) { return 42; }\n")
            (root / "main.c").write_text(
                "int answer(void); int main(void) { return answer() != 42; }\n"
            )
            subprocess.run(
                [
                    "cc",
                    "-shared",
                    "-fPIC",
                    str(root / "library.c"),
                    "-o",
                    str(library / "libgluten_ci_fixture.so"),
                ],
                check=True,
            )
            executable = root / "example"
            subprocess.run(
                [
                    "cc",
                    str(root / "main.c"),
                    "-L" + str(library),
                    "-lgluten_ci_fixture",
                    "-o",
                    str(executable),
                ],
                check=True,
            )
            result = self.shell(
                'ci_runtime "$EXAMPLE"', GLUTEN_DIR=str(gluten), EXAMPLE=str(executable)
            )
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_conan_path_and_packages_persist_between_runner_jobs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gluten = root / "gluten"
            (gluten / "build").mkdir(parents=True)
            subprocess.run(["git", "init", "-q", str(gluten)], check=True)
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(gluten),
                    "-c",
                    "user.name=Test",
                    "-c",
                    "user.email=test@example.com",
                    "commit",
                    "-q",
                    "--allow-empty",
                    "-m",
                    "fixture",
                ],
                check=True,
            )
            env = {
                "GLUTEN_DIR": str(gluten),
                "GLUTEN_REPORTS_DIR": str(root / "reports"),
                "GLUTEN_CACHE_ROOT": str(root / "cache"),
                "RUNNER_TEMP": str(root / "runner"),
                "RUNNER_NAME": "fixture",
                "GITHUB_ACTIONS": "false",
                "GITHUB_ENV": "",
                "CI_BUILD": "spark35",
            }
            first = self.shell(
                'ci_cache_environment; printf "%s" "$CONAN_HOME"',
                GITHUB_RUN_ID="1",
                **env,
            )
            self.assertEqual(first.returncode, 0, first.stderr)
            home = Path(first.stdout)
            (home / "cached-package").touch()
            second = self.shell(
                'ci_cache_environment; printf "%s" "$CONAN_HOME"',
                GITHUB_RUN_ID="2",
                **env,
            )
            self.assertEqual(second.returncode, 0, second.stderr)
            self.assertEqual(
                first.stdout, second.stdout, "Changing paths prevents ccache reuse"
            )
            self.assertTrue(
                (home / "cached-package").exists(),
                "Sequential jobs must reuse dependencies",
            )
            third = self.shell(
                'ci_cache_environment; printf "%s" "$CONAN_HOME"',
                **{
                    **env,
                    "RUNNER_NAME": "another-runner",
                    "RUNNER_TEMP": str(root / "other"),
                },
            )
            self.assertEqual(third.returncode, 0, third.stderr)
            self.assertFalse((Path(third.stdout) / "cached-package").exists())

    def test_container_cache_prefix_matches_existing_bolt_ci(self):
        result = self.shell(
            "ci_conan_home native",
            GITHUB_ACTIONS="true",
            HOME="/github/home",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "/github/home/.conan2")

    def test_artifact_cache_reuses_verified_archives_and_repairs_corruption(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "source.zip"
            with zipfile.ZipFile(archive, "w") as zipped:
                zipped.writestr("payload.txt", "candidate native library")
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            metadata = {
                "artifacts": [
                    {
                        "id": 123,
                        "digest": "sha256:" + digest,
                        "archive_download_url": "https://fixture/archive",
                    }
                ]
            }
            (root / "metadata.json").write_text(json.dumps(metadata))
            binary = root / "bin"
            binary.mkdir()
            curl = binary / "curl"
            curl.write_text("""#!/usr/bin/env python3
import os, pathlib, shutil, sys, time
root = pathlib.Path(os.environ["FIXTURE"])
args = sys.argv[1:]
if "--get" in args:
    print((root / "metadata.json").read_text())
else:
    with (root / "downloads").open("a") as log:
        log.write("download\\n")
    time.sleep(0.2)
    shutil.copyfile(root / "source.zip", args[args.index("--output") + 1])
""")
            curl.chmod(0o755)
            reports = root / "reports"
            env = {
                "FIXTURE": str(root),
                "PATH": str(binary) + ":" + os.environ["PATH"],
                "GLUTEN_CACHE_ROOT": str(root / "cache"),
                "GLUTEN_REPORTS_DIR": str(reports),
                "RUNNER_TEMP": str(root),
                "GITHUB_TOKEN": "fixture",
                "GITHUB_REPOSITORY": "fixture/bolt",
                "GITHUB_RUN_ID": "1",
            }
            old_cache = root / "cache/artifacts"
            old_cache.mkdir(parents=True)
            for number in range(1000):
                stale = old_cache / f"stale-{number}"
                stale.touch()
                old = time.time() - 5 * 86400
                os.utime(stale, (old, old))
            for iteration in range(3):
                if iteration:
                    (reports / "payload.txt").unlink()
                if iteration == 2:
                    cached = next((root / "cache/artifacts").glob("*.zip"))
                    cached.write_bytes(b"interrupted transfer")
                if iteration == 0:
                    with ThreadPoolExecutor(max_workers=8) as executor:
                        futures = [
                            executor.submit(
                                self.shell, "ci_download_artifact fixture", **values
                            )
                            for values in [env]
                            + [
                                {
                                    **env,
                                    "GLUTEN_REPORTS_DIR": str(
                                        root / f"other-reports-{i}"
                                    ),
                                }
                                for i in range(7)
                            ]
                        ]
                        results = [future.result() for future in futures]
                    for other in results:
                        self.assertEqual(
                            other.returncode, 0, other.stdout + other.stderr
                        )
                    result = results[0]
                else:
                    result = self.shell("ci_download_artifact fixture", **env)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(
                    (reports / "payload.txt").read_text(), "candidate native library"
                )
                self.assertEqual(
                    (root / "downloads").read_text().count("download"),
                    1 if iteration < 2 else 2,
                )

    def test_rebuilt_or_wrong_candidate_package_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected = {
                "name": "bolt",
                "context": "host",
                "ref": "bolt/ci-candidate#recipe",
                "package_id": "package",
                "prev": "revision",
                "binary": "Cache",
            }
            (root / "bolt-export.json").write_text(
                json.dumps({"graph": {"nodes": {"0": expected}}})
            )
            for change in ({}, {"prev": "other"}, {"binary": "Build"}):
                with self.subTest(change=change):
                    actual = {**expected, **change}
                    (root / "gluten-install.json").write_text(
                        json.dumps({"graph": {"nodes": {"1": actual}}})
                    )
                    result = self.shell(
                        "ci_verify_bolt_package", GLUTEN_REPORTS_DIR=directory
                    )
                    self.assertEqual(
                        result.returncode, 0 if not change else 1, result.stderr
                    )


if __name__ == "__main__":
    unittest.main()
