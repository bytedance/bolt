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

import importlib.util
import os
from pathlib import Path
import subprocess
import shutil
import shlex
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "profile_setup", ROOT / "scripts/configure-conan-profile.py"
)
profile_setup = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = profile_setup
spec.loader.exec_module(profile_setup)


class SetupReviewTest(unittest.TestCase):
    def run_shell(self, body, script="setup-dev-env.sh"):
        source = f"source {shlex.quote(str(ROOT / 'scripts' / script))}"
        return subprocess.run(
            ["bash", "-c", source + "\n" + body], text=True, capture_output=True
        )

    def test_versioned_compiler_and_separate_linker(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            cc = root / "compiler"
            ld = root / "linker"
            cc.mkdir()
            ld.mkdir()
            for name, version in [
                ("clang", "18"),
                ("clang-99", "99"),
                ("clang++-99", "99"),
            ]:
                path = cc / name
                path.write_text(
                    f'#!/bin/bash\ncase "$1" in\n-dumpversion) echo {version};;\n--version) echo "InstalledDir: {cc}";;\nesac\n'
                )
                path.chmod(0o755)
            for name in ["ld.lld-99", "lld-99"]:
                path = ld / name
                path.write_text("#!/bin/bash\nexit 0\n")
                path.chmod(0o755)
            result = self.run_shell(f'''
export PATH="{cc}:{ld}:$PATH"
OS_TYPE=Linux
find_installed_clang
find_clang_linker
[[ "$CLANG_COMPILER" == "{cc}/clang-99" ]]
[[ "$CLANG_LINKER" == "{ld}/ld.lld-99" ]]
sudo() {{ "$@"; }}
update-alternatives() {{ printf '%s\\n' "$*"; }}
set_default_llvm
python() {{ printf '%s\\n' "$*"; }}
configure_conan_profile
''')
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(f"--set ld.lld {ld}/ld.lld-99", result.stdout)
            self.assertIn(f"--set lld {ld}/lld-99", result.stdout)
            self.assertIn(f"--set clang {cc}/clang-99", result.stdout)
            self.assertIn(f"--compiler={cc}/clang-99", result.stdout)

    def test_missing_libcxx(self):
        result = self.run_shell(
            "CLANG_COMPILER=/bin/false\nCLANG_LINKER=/bin/true\ncheck_libcxx"
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("libc++ and libc++abi", result.stdout)

    def test_setup_stops_before_recipes_when_profile_fails(self):
        result = self.run_shell("""
check_basic_tools() { :; }
install_clang() { :; }
set_default_llvm() { :; }
install_miniconda() { :; }
install_python_dependencies() { :; }
configure_conan_profile() { return 42; }
install_bolt_dependencies() { echo unexpected-recipes; }
install_git_hooks() { echo unexpected-hooks; }
main
""")
        self.assertEqual(result.returncode, 42, result.stderr)
        self.assertNotIn("unexpected-", result.stdout)

    def test_miniconda_platforms(self):
        for os_type, arch, platform in [
            ("Linux", "aarch64", "Linux-aarch64"),
            ("Linux", "x86_64", "Linux-x86_64"),
            ("Darwin", "arm64", "MacOSX-arm64"),
            ("Darwin", "x86_64", "MacOSX-x86_64"),
        ]:
            with self.subTest(platform=platform):
                result = self.run_shell(
                    f"OS_TYPE={os_type}\nARCH_TYPE={arch}\nminiconda_url"
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertTrue(result.stdout.strip().endswith(f"-{platform}.sh"))

    def test_recipe_arguments_and_remote(self):
        result = self.run_shell(
            """
conan() { printf '%s\\n' "$*" >&2; }
pip() { echo unexpected-pip >&2; return 1; }
download_conan_recipes() { printf 'clone <%s> <%s>\\n' "$1" "$2"; }
main --branch release-test --repository '/tmp/recipe index'
""",
            script="install-bolt-deps.sh",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("clone </tmp/recipe index> <release-test>", result.stdout)
        self.assertIn(
            "remote add --index 0 --type local-recipes-index bolt-cci-local",
            result.stderr,
        )
        self.assertNotIn("unexpected-pip", result.stderr)

    def test_recipe_ref_defaults(self):
        result = self.run_shell(
            """
git() { echo v1.2.3; }
resolve_cci_ref ''
git() { return 1; }
resolve_cci_ref ''
resolve_cci_ref explicit-branch
""",
            script="install-bolt-deps.sh",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(), ["v1.2.3", "main", "explicit-branch"]
        )

    @unittest.skipUnless(shutil.which("conan"), "Conan is required")
    def test_active_conan_settings(self):
        with tempfile.TemporaryDirectory() as d:
            old_home = os.environ.get("CONAN_HOME")
            os.environ["CONAN_HOME"] = d
            try:
                subprocess.run(
                    ["conan", "config", "home"], check=True, capture_output=True
                )
                settings = Path(d) / "settings.yml"
                # Model an existing Conan settings file that only recognizes Clang 22.
                import yaml

                data = yaml.safe_load(settings.read_text())
                data["compiler"]["clang"]["version"] = ["22"]
                settings.write_text(yaml.safe_dump(data))
                profile_setup.validate_conan_compiler(
                    profile_setup.CompilerInfo("clang", "22", {})
                )
                with self.assertRaisesRegex(RuntimeError, "Upgrade Conan"):
                    profile_setup.validate_conan_compiler(
                        profile_setup.CompilerInfo("clang", "23", {})
                    )
                existing_profile = Path(d) / "profiles" / "default"
                existing_profile.parent.mkdir(exist_ok=True)
                existing_profile.write_text("existing profile\n")
                with (
                    patch.object(sys, "argv", ["configure-conan-profile.py"]),
                    patch.object(
                        profile_setup,
                        "select_compiler_and_libcxx",
                        return_value=(
                            profile_setup.CompilerInfo("clang", "23", {}),
                            "libc++",
                        ),
                    ),
                ):
                    self.assertEqual(profile_setup.main(), 1)
                self.assertEqual(existing_profile.read_text(), "existing profile\n")
                (Path(d) / "settings_user.yml").write_text(
                    'compiler:\n  clang:\n    version: ["22", "23"]\n'
                )
                profile_setup.validate_conan_compiler(
                    profile_setup.CompilerInfo("clang", "23", {})
                )
            finally:
                if old_home is None:
                    os.environ.pop("CONAN_HOME", None)
                else:
                    os.environ["CONAN_HOME"] = old_home


if __name__ == "__main__":
    unittest.main()
