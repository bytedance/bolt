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

"""Regression tests for publishing complete Conan profiles without data loss."""

import ast
import contextlib
import hashlib
import importlib.util
import io
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "configure-conan-profile.py"
SPEC = importlib.util.spec_from_file_location("configure_conan_profile", SCRIPT)
configure = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = configure
SPEC.loader.exec_module(configure)


class ConfigureConanProfileTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR"))
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.profile = self.root / "profiles" / "toolchains" / "selected"
        self.profile.parent.mkdir(parents=True)
        self.other = self.profile.parent / "other"
        self.other.write_bytes(b"other profile\n")
        self.output = io.StringIO()
        redirect = contextlib.redirect_stdout(self.output)
        redirect.__enter__()
        self.addCleanup(redirect.__exit__, None, None, None)
        self.compiler = configure.CompilerInfo(
            "clang", "21", {"c": "/toolchain/clang", "cpp": "/toolchain/clang++"}
        )
        self.gcc = configure.GccToolchainInfo(
            "14", Path("/gcc"), {"c": "/gcc/bin/gcc", "cpp": "/gcc/bin/g++"}
        )
        self.patch("discover_latest_gcc_toolchain", return_value=self.gcc)
        self.patch(
            "resolve_llvm_tool", side_effect=lambda compiler, tool: f"/llvm/{tool}"
        )

    def patch(self, name, **kwargs):
        patcher = mock.patch.object(configure, name, **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def test_compiler_paths_preserve_links_and_find_installation_cpp_driver(self):
        installation = self.root / "llvm-22" / "bin"
        installation.mkdir(parents=True)
        clang = installation / "clang"
        clang.write_text('#!/bin/sh\necho "clang version 22.1.8"\n')
        clang.chmod(0o755)
        clang_cpp = installation / "clang++"
        clang_cpp.symlink_to("clang")
        commands = self.root / "bin"
        commands.mkdir()
        (commands / "clang").symlink_to(clang)
        (commands / "clang++").symlink_to(clang_cpp)
        (commands / "clang-22").symlink_to(clang)
        (commands / "clang++-22").symlink_to(clang_cpp)

        with mock.patch.dict(os.environ, {"PATH": str(commands)}):
            for compiler in ("clang", "clang-22", str(commands / "clang")):
                with self.subTest(compiler=compiler):
                    self.assertEqual(
                        configure.resolve_executable(compiler),
                        str(commands / Path(compiler).name),
                    )
                    self.assertEqual(
                        configure.resolve_compiler_pair(compiler, "clang"),
                        str(clang_cpp),
                    )
            self.assertEqual(
                configure.resolve_executable(str(commands / "clang++-22")),
                str(commands / "clang++-22"),
            )

    def test_clang_cpp_link_to_versioned_driver_without_versioned_cpp(self):
        installation = self.root / "llvm-22" / "bin"
        installation.mkdir(parents=True)
        driver = installation / "clang-22"
        driver.write_text(
            '#!/bin/sh\ncase "$1" in\n'
            '--version) echo "clang version 22.1.8" ;;\n'
            '--print-driver-mode) echo "${0##*/}" ;;\nesac\n'
        )
        driver.chmod(0o755)
        clang = installation / "clang"
        clang.symlink_to(driver.name)
        clang_cpp = installation / "clang++"
        clang_cpp.symlink_to(driver.name)
        commands = self.root / "bin"
        commands.mkdir()
        (commands / "clang").symlink_to(clang)
        (commands / "clang++").symlink_to(clang_cpp)

        with mock.patch.dict(os.environ, {"PATH": str(commands)}):
            for compiler in ("clang", str(commands / "clang"), str(driver)):
                with self.subTest(compiler=compiler):
                    detected = configure.detect_compiler(compiler)
                    self.assertEqual(detected.version, "22")
                    self.assertEqual(detected.executables["cpp"], str(clang_cpp))
                    self.assertEqual(
                        configure.run_command(
                            [detected.executables["cpp"], "--print-driver-mode"]
                        ),
                        "clang++",
                    )
            self.assertEqual(
                configure.resolve_executable("clang++"), str(commands / "clang++")
            )

    def test_compiler_named_ccache_links_preserve_wrapper_invocation(self):
        wrapper = self.root / "libexec" / "ccache"
        wrapper.parent.mkdir()
        wrapper.write_text(
            '#!/bin/sh\ncase "${0##*/}" in\n'
            'clang*) echo "clang version 22.1.8" ;;\n'
            '*) echo "ccache version 4.12" ;;\nesac\n'
        )
        wrapper.chmod(0o755)
        commands = self.root / "bin"
        commands.mkdir()
        for name in ("clang", "clang++", "clang-22", "clang++-22"):
            (commands / name).symlink_to(wrapper)

        with mock.patch.dict(os.environ, {"PATH": str(commands)}):
            for compiler in ("clang", "clang-22", str(commands / "clang")):
                with self.subTest(compiler=compiler):
                    detected = configure.detect_compiler(compiler)
                    self.assertEqual(detected.version, "22")
                    self.assertEqual(
                        detected.executables,
                        {
                            "c": str(commands / Path(compiler).name),
                            "cpp": str(
                                commands
                                / Path(compiler).name.replace("clang", "clang++")
                            ),
                        },
                    )

    def test_clang_finds_cpp_in_installation_without_cpp_on_path(self):
        installation = self.root / "llvm-22" / "bin"
        installation.mkdir(parents=True)
        clang = installation / "clang"
        clang.write_text('#!/bin/sh\necho "clang version 22.1.8"\n')
        clang.chmod(0o755)
        clang_cpp = installation / "clang++"
        clang_cpp.symlink_to("clang")
        commands = self.root / "bin"
        commands.mkdir()
        (commands / "clang").symlink_to(clang)
        # An unrelated clang++ on PATH must not override the selected toolchain.
        unrelated_cpp = commands / "clang++"
        unrelated_cpp.write_text('#!/bin/sh\necho "clang version 18.0.0"\n')
        unrelated_cpp.chmod(0o755)

        with mock.patch.dict(os.environ, {"PATH": str(commands)}):
            for cpp_on_path in (True, False):
                with self.subTest(cpp_on_path=cpp_on_path):
                    if not cpp_on_path:
                        unrelated_cpp.unlink()
                    compiler = configure.detect_compiler("clang")
                    self.assertEqual(compiler.version, "22")
                    self.assertEqual(
                        compiler.executables,
                        {"c": str(commands / "clang"), "cpp": str(clang_cpp)},
                    )

    def test_compiler_pair_skips_mismatched_installation_sibling(self):
        installation = self.root / "tools"
        installation.mkdir()
        commands = self.root / "bin"
        commands.mkdir()
        for path, version in (
            (installation / "clang", 22),
            (installation / "clang++", 18),
            (commands / "clang++-22", 22),
        ):
            path.write_text(f'#!/bin/sh\necho "clang version {version}.0.0"\n')
            path.chmod(0o755)
        (commands / "clang-22").symlink_to(installation / "clang")
        with mock.patch.dict(os.environ, {"PATH": str(commands)}):
            selected = configure.detect_compiler("clang-22")
            self.assertEqual(selected.version, "22")
            self.assertEqual(selected.executables["cpp"], str(commands / "clang++-22"))

    def update(self, **overrides):
        arguments = dict(
            profile_file=self.profile,
            profile_name="toolchains/selected",
            compiler_info=self.compiler,
            libcxx="libc++",
            lto="none",
            linker="ld",
            extra_cflags=[],
            extra_link_flags=[],
        )
        arguments.update(overrides)
        configure.update_selected_profile(**arguments)

    def test_generic_clang_selects_latest_matching_pair_on_path(self):
        early = self.root / "early"
        later = self.root / "later"
        early.mkdir()
        later.mkdir()
        for directory, name, version in (
            (early, "clang", 18),
            (early, "clang-9", 9),
            (later, "clang-22", 22),
            (later, "clang-23", 23),
        ):
            executable = directory / name
            executable.write_text(f'#!/bin/sh\necho "clang version {version}.0.0"\n')
            executable.chmod(0o755)
            if version != 23:
                (directory / name.replace("clang", "clang++")).symlink_to(name)

        with mock.patch.dict(os.environ, {"PATH": f"{early}{os.pathsep}{later}"}):
            selected, libcxx = configure.select_compiler_and_libcxx("clang", "libc++")
            self.assertEqual(selected.version, "22")
            self.assertEqual(libcxx, "libc++")
            self.assertEqual(
                selected.executables,
                {"c": str(later / "clang-22"), "cpp": str(later / "clang++-22")},
            )
            for compiler in ("clang-9", str(early / "clang")):
                with self.subTest(compiler=compiler):
                    selected, _ = configure.select_compiler_and_libcxx(
                        compiler, "libc++"
                    )
                    self.assertEqual(
                        selected.version, "9" if compiler == "clang-9" else "18"
                    )

    def test_generic_clang_finds_versioned_cpp_in_another_path_directory(self):
        early = self.root / "early"
        later = self.root / "later"
        early.mkdir()
        later.mkdir()
        for path, version in (
            (early / "clang", 18),
            (early / "clang++", 18),
            (early / "clang-22", 22),
            (later / "clang++-22", 22),
        ):
            path.write_text(f'#!/bin/sh\necho "clang version {version}.0.0"\n')
            path.chmod(0o755)

        with mock.patch.dict(os.environ, {"PATH": f"{early}{os.pathsep}{later}"}):
            for compiler in ("clang", "clang-22", str(early / "clang-22")):
                with self.subTest(compiler=compiler):
                    selected, _ = configure.select_compiler_and_libcxx(
                        compiler, "libc++"
                    )
                    self.assertEqual(selected.version, "22")
                    self.assertEqual(
                        selected.executables,
                        {
                            "c": str(early / "clang-22"),
                            "cpp": str(later / "clang++-22"),
                        },
                    )

    def test_generic_clang_does_not_fall_back_to_gcc(self):
        with (
            mock.patch.dict(os.environ, {"PATH": str(self.root)}),
            mock.patch.object(sys, "platform", "linux"),
        ):
            with self.assertRaisesRegex(RuntimeError, "Cannot find any usable Clang"):
                configure.select_compiler_and_libcxx("clang", None)

    def test_generic_clang_preserves_apple_clang_fallback(self):
        apple_clang = configure.CompilerInfo("apple-clang", "17", {})
        self.patch("discover_latest_clang", side_effect=RuntimeError("not found"))
        self.patch("discover_apple_clang", return_value=apple_clang)
        with mock.patch.object(sys, "platform", "darwin"):
            self.assertEqual(
                configure.select_compiler_and_libcxx("clang", None),
                (apple_clang, "libc++"),
            )

    def assert_complete(self, profile):
        text = profile.read_text()
        for section in ("[settings]", "[conf]", "[replace_tool_requires]"):
            self.assertIn(section, text)
        self.assertIn("os={{ detect_api.detect_os() }}", text)
        self.assertIn("arch={{ detect_api.detect_arch() }}", text)
        self.assertIn("build_type=Release", text)
        self.assertIn("compiler.cppstd=gnu20", text)
        self.assertIn(f"cmake/*: cmake/{configure.CMAKE_VERSION}", text)
        return text

    def test_failures_preserve_destination_and_clean_staging(self):
        original = b"[settings]\nos=Macos\narch=armv8\n# keep exactly\xff\r\n"
        real_update = configure.update_profile_entries

        for state in ("existing", "absent", "symlink", "dangling"):
            for failure in ("gcc", "llvm-ar", "llvm-ranlib", "write", "replace"):
                with self.subTest(state=state, failure=failure):
                    self.profile.unlink(missing_ok=True)
                    target = self.root / "symlink-target"
                    target.unlink(missing_ok=True)
                    if state == "existing":
                        self.profile.write_bytes(original)
                    elif state in ("symlink", "dangling"):
                        if state == "symlink":
                            target.write_bytes(original)
                        self.profile.symlink_to(target)
                    before = set(self.profile.parent.iterdir())
                    self.output.seek(0)
                    self.output.truncate()
                    error = OSError(f"injected {failure} failure")

                    def llvm_failure(compiler, tool):
                        if tool == failure:
                            raise error
                        return f"/llvm/{tool}"

                    def write_failure(path, section, *args, **kwargs):
                        if section == "conf":
                            # The real template and settings writes have completed.
                            self.assertIn("compiler=clang", path.read_text())
                            raise error
                        return real_update(path, section, *args, **kwargs)

                    def replace_failure(source, destination):
                        self.assert_complete(source)
                        self.assertEqual(destination, self.profile)
                        raise error

                    with contextlib.ExitStack() as stack:
                        arguments = {}
                        if failure == "gcc":
                            error = RuntimeError("Cannot find any usable GCC toolchain")
                            arguments["libcxx"] = "libstdc++11"
                            stack.enter_context(
                                mock.patch.object(
                                    configure,
                                    "discover_latest_gcc_toolchain",
                                    side_effect=error,
                                )
                            )
                        elif failure.startswith("llvm-"):
                            stack.enter_context(
                                mock.patch.object(
                                    configure,
                                    "resolve_llvm_tool",
                                    side_effect=llvm_failure,
                                )
                            )
                        elif failure == "write":
                            stack.enter_context(
                                mock.patch.object(
                                    configure,
                                    "update_profile_entries",
                                    side_effect=write_failure,
                                )
                            )
                        else:
                            stack.enter_context(
                                mock.patch.object(
                                    Path,
                                    "replace",
                                    autospec=True,
                                    side_effect=replace_failure,
                                )
                            )
                        with self.assertRaises(type(error)) as caught:
                            self.update(**arguments)
                        self.assertIs(caught.exception, error)

                    self.assertEqual(set(self.profile.parent.iterdir()), before)
                    self.assertEqual(self.other.read_bytes(), b"other profile\n")
                    self.assertNotIn("configured for", self.output.getvalue())
                    if state == "existing":
                        self.assertEqual(self.profile.read_bytes(), original)
                    elif state == "absent":
                        self.assertFalse(os.path.lexists(self.profile))
                    else:
                        self.assertTrue(self.profile.is_symlink())
                        self.assertEqual(self.profile.readlink(), target)
                        if state == "symlink":
                            self.assertEqual(target.read_bytes(), original)
                        else:
                            self.assertFalse(os.path.lexists(target))

    def test_success_publishes_complete_reset_profile_once(self):
        real_replace = Path.replace
        data = self.root / "sample profile.prof"
        data.write_bytes(b"self-contained profile data")
        pgo = configure.read_pgo_profile(data, "sample")
        self.assertEqual(pgo.sha256, hashlib.sha256(data.read_bytes()).hexdigest())
        cases = (
            ("gcc", "libstdc++11", "none", None),
            ("apple-clang", "libc++", "full", None),
            ("clang", "libstdc++11", "thin", pgo),
        )
        for family, libcxx, lto, profile_data in cases:
            for original in (
                b"[settings]\nos=Custom\n[options]\n*:shared=True\n",
                b"\xff\xfeinvalid UTF-8",
                b"[conf]\ntools.build:cflags=not a list\n",
            ):
                with self.subTest(family=family, original=original):
                    self.output.seek(0)
                    self.output.truncate()
                    self.profile.write_bytes(original)
                    before = set(self.profile.parent.iterdir())
                    compiler = configure.CompilerInfo(
                        family, "21", self.compiler.executables
                    )

                    def publish(source, destination):
                        self.assertEqual(destination.read_bytes(), original)
                        self.assertNotIn("configured for", self.output.getvalue())
                        self.assertEqual(source.parent.parent, destination.parent)
                        self.assert_complete(source)
                        return real_replace(source, destination)

                    with mock.patch.object(
                        Path, "replace", autospec=True, side_effect=publish
                    ) as replace:
                        self.update(
                            compiler_info=compiler,
                            libcxx=libcxx,
                            lto=lto,
                            linker=None,
                            extra_cflags=["-g"],
                            extra_link_flags=["-Wl,--as-needed"],
                            libc="gnu",
                            libc_version="2.17",
                            pgo=profile_data,
                        )
                        self.assertEqual(replace.call_count, 1)
                    self.assertEqual(set(self.profile.parent.iterdir()), before)
                    self.assertEqual(self.other.read_bytes(), b"other profile\n")
                    text = self.assert_complete(self.profile)
                    self.assertIn(f"compiler={family}\n", text)
                    self.assertIn(f"compiler.libcxx={libcxx}\n", text)
                    self.assertIn("os.libc=gnu\nos.libc_version=2.17", text)
                    self.assertNotIn(configure.PROFILE_TEMPLATE_PREAMBLE, text)
                    self.assertNotIn("Custom", text)
                    self.assertNotIn("[options]", text)
                    conf = dict(
                        line.split("=", 1) for line in text.splitlines() if "=" in line
                    )
                    cflags = ["-g"] + ([pgo.flag] if profile_data else [])
                    linkflags = ["-Wl,--as-needed"] + (
                        [pgo.flag] if profile_data else []
                    )
                    cxxflags = list(cflags)
                    if family == "clang":
                        cxxflags.append("--gcc-toolchain=/gcc")
                        self.assertIn("AR=/llvm/llvm-ar", text)
                        self.assertIn("RANLIB=/llvm/llvm-ranlib", text)
                    else:
                        self.assertNotIn("[buildenv]", text)
                    if family != "gcc":
                        linkflags.append("-fuse-ld=lld")
                        for flags in (cflags, cxxflags, linkflags):
                            flags.append(f"-flto={lto}")
                        self.assertEqual(conf[configure.LTO_CONF_KEY], lto)
                    else:
                        self.assertNotIn(configure.LTO_CONF_KEY, conf)
                    self.assertEqual(
                        ast.literal_eval(conf["tools.build:cflags"]), cflags
                    )
                    self.assertEqual(
                        ast.literal_eval(conf["tools.build:cxxflags"]), cxxflags
                    )
                    for key in (
                        "tools.build:exelinkflags",
                        "tools.build:sharedlinkflags",
                    ):
                        self.assertEqual(ast.literal_eval(conf[key]), linkflags)
                    if profile_data:
                        self.assertEqual(conf[configure.PGO_MODE_CONF_KEY], "sample")
                        self.assertEqual(conf[configure.PGO_HASH_CONF_KEY], pgo.sha256)
                        self.assertEqual(
                            ast.literal_eval(conf[configure.PACKAGE_ID_CONFS_KEY]),
                            [
                                configure.LTO_CONF_KEY,
                                configure.PGO_MODE_CONF_KEY,
                                configure.PGO_HASH_CONF_KEY,
                            ],
                        )
                    output = self.output.getvalue()
                    self.assertIn(f"Updating Conan profile: {self.profile}\n", output)
                    self.assertIn(
                        "Profile 'toolchains/selected' configured for", output
                    )
                    self.assertNotIn(".bolt-profile-", output)

    def test_success_replaces_symlink_entry_without_touching_target(self):
        for dangling in (False, True):
            with self.subTest(dangling=dangling):
                self.profile.unlink(missing_ok=True)
                target = self.root / "symlink-target"
                if not dangling:
                    target.write_bytes(b"keep target unchanged")
                else:
                    target.unlink(missing_ok=True)
                self.profile.symlink_to(target)
                self.update()
                self.assertFalse(self.profile.is_symlink())
                self.assert_complete(self.profile)
                if dangling:
                    self.assertFalse(os.path.lexists(target))
                else:
                    self.assertEqual(target.read_bytes(), b"keep target unchanged")
                self.assertEqual(
                    set(self.profile.parent.iterdir()), {self.profile, self.other}
                )

    def test_new_profile_uses_normal_file_mode_and_libc_template(self):
        self.profile = self.root / "profiles" / "new" / "default"
        previous_umask = os.umask(0o022)
        try:
            self.update()
        finally:
            os.umask(previous_umask)
        self.assertEqual(stat.S_IMODE(self.profile.stat().st_mode), 0o644)
        text = self.assert_complete(self.profile)
        self.assertTrue(text.startswith(configure.PROFILE_TEMPLATE_PREAMBLE + "\n"))
        self.assertIn("{% if libc %}\nos.libc={{ libc }}\n{% endif %}", text)
        self.assertNotIn("-fuse-ld=", text)
        self.assertEqual(list(self.profile.parent.iterdir()), [self.profile])


if __name__ == "__main__":
    unittest.main()
