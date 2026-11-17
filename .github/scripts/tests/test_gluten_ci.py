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
                **env,
            },
            text=True,
            capture_output=True,
        )

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


if __name__ == "__main__":
    unittest.main()
