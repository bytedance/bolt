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

# This tool helps users configure Conan profiles:
# 1. It adds libc and libc_version to Conan settings so Conan servers can
#    distinguish packages built against different libc versions.
# 2. It configures global LTO flags.
# 3. It makes switching compilers and C++ runtime libraries easy.
#
# Usage examples:
#   # Show all supported options.
#   python3 scripts/configure-conan-profile.py --help
#
#   # Configure the default profile for clang, libstdc++11, and no LTO.
#   python3 scripts/configure-conan-profile.py
#
#   # Configure a named profile for Clang 18, libc++, and ThinLTO.
#   python3 scripts/configure-conan-profile.py \
#       --profile clang18-thinlto --compiler clang-18 --libcxx libc++ --lto thin
#
#   # Configure a named profile for GCC 14 and libstdc++11.
#   python3 scripts/configure-conan-profile.py \
#       --profile gcc14 --libcxx libstdc++11
#
#   # Select mold explicitly, or use --linker ld to keep the compiler default.
#   python3 scripts/configure-conan-profile.py --linker mold
#
#   # Add custom compiler and linker flags to the profile.
#   python3 scripts/configure-conan-profile.py \
#       --compile=clang                    \
#       --extra-cflags="-g -fdebug-info-for-profiling -fpseudo-probe-for-profiling" \
#       --extra-link-flags="-Wl,--as-needed"
#
#   # Only add os.libc/os.libc_version to settings_user.yml.
#   python3 scripts/configure-conan-profile.py --skip-profile
#
# Set CONAN_HOME to update a Conan cache other than ~/.conan2.

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml


# With no options, update the default profile using clang and libstdc++11 (the C++11 ABI).
DEFAULT_PROFILE_NAME = "default"
DEFAULT_COMPILER = "clang"
DEFAULT_LIBCXX = "libstdc++11"
DEFAULT_LTO = "none"
CMAKE_VERSION = "3.31.10"

PROFILE_TEMPLATE_PREAMBLE = "{% set libc, libc_version = detect_api.detect_libc() %}"
PROFILE_TEMPLATE_SETTINGS = [
    "{% if libc %}",
    "os.libc={{ libc }}",
    "{% endif %}",
    "{% if libc_version %}",
    "os.libc_version={{ libc_version }}",
    "{% endif %}",
]
PROFILE_DEFAULT_SETTINGS = {
    "os": "{{ detect_api.detect_os() }}",
    "arch": "{{ detect_api.detect_arch() }}",
    "build_type": "Release",
}
GCC_TOOLCHAIN_FLAG_KEYS = [
    f"tools.build:{kind}"
    for kind in ("cflags", "cxxflags", "exelinkflags", "sharedlinkflags")
]
LTO_CONF_KEY = "user.flags:lto"
PACKAGE_ID_CONFS_KEY = "tools.info.package_id:confs"
CMAKE_TOOL_REQUIRE = {"cmake/*": f"cmake/{CMAKE_VERSION}"}


@dataclass(frozen=True)
class CompilerInfo:
    family: str
    version: str
    executables: dict[str, str]


@dataclass(frozen=True)
class GccToolchainInfo:
    version: str
    root: Path
    executables: dict[str, str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Configure Conan profile compiler settings and extend Conan Linux settings "
            "to support os.libc/os.libc_version."
        )
    )
    # Example: python3 scripts/configure-conan-profile.py --profile gcc16 --compiler gcc-16
    parser.add_argument(
        "--profile",
        dest="profile_name",
        default=DEFAULT_PROFILE_NAME,
        help=f"Conan profile name to update (default: {DEFAULT_PROFILE_NAME})",
    )
    # Use --skip-profile to extend Linux libc settings without requiring installed compilers.
    parser.add_argument(
        "--skip-profile",
        action="store_true",
        help="Only update settings_user.yml, do not touch any Conan profile",
    )
    parser.add_argument(
        "--compiler",
        default=DEFAULT_COMPILER,
        help="Compiler name or path, for example clang, gcc-12 or /opt/compiler/bin/clang",
    )
    # Clang with libstdc++ selects the newest GCC/G++ pair on PATH; --libcxx libc++ skips this.
    parser.add_argument(
        "--libcxx",
        default=DEFAULT_LIBCXX,
        choices=("libc++", "libstdc++11"),
        help=f"C++ runtime library to configure (default: {DEFAULT_LIBCXX})",
    )
    parser.add_argument(
        "--lto",
        default=DEFAULT_LTO,
        choices=("none", "thin", "full"),
        help=(
            "Clang link-time optimization mode: none, thin or full "
            f"(default: {DEFAULT_LTO})"
        ),
    )
    parser.add_argument(
        "--linker",
        default=None,
        help=(
            "Linker for -fuse-ld (default: lld for Clang, compiler default for GCC); "
            "use 'ld' to omit -fuse-ld"
        ),
    )
    parser.add_argument(
        "--extra-cflags",
        action="extend",
        type=shlex.split,
        default=[],
        metavar="FLAGS",
        help=(
            "Extra shell-style flags to add to tools.build:cflags and "
            "tools.build:cxxflags; may be repeated"
        ),
    )
    parser.add_argument(
        "--extra-link-flags",
        action="extend",
        type=shlex.split,
        default=[],
        metavar="FLAGS",
        help=(
            "Extra shell-style flags to add to tools.build:exelinkflags and "
            "tools.build:sharedlinkflags; may be repeated"
        ),
    )
    args = parser.parse_args()
    compiler_name = Path(args.compiler).name
    if not compiler_name.startswith(("clang", "gcc")):
        parser.error("--compiler must name a clang or gcc compiler")
    return args


def run_command(command: list[str]) -> str:
    return subprocess.run(
        command, check=True, capture_output=True, text=True
    ).stdout.strip()


def resolve_executable(command: str) -> str:
    if "/" in command:
        executable = Path(command).expanduser().resolve()
        if not executable.is_file() or not os.access(executable, os.X_OK):
            raise RuntimeError(
                f"Required compiler not found or not executable: {command}"
            )
        return str(executable)

    executable = shutil.which(command)
    if executable is None:
        raise RuntimeError(f"Required compiler not found: {command}")
    return os.path.abspath(executable)


def compiler_pair_name(compiler: str, family: str) -> str:
    stem = Path(compiler).name
    replacement = "clang++" if family == "clang" else "g++"
    pair_stem = stem.replace(family, replacement, 1)
    if "/" not in compiler:
        return pair_stem
    return str(Path(compiler).expanduser().resolve().parent / pair_stem)


def resolve_compiler_pair(compiler: str, family: str) -> str:
    candidates = [compiler_pair_name(compiler, family)]
    generic_pair = "clang++" if family == "clang" else "g++"

    if "/" in compiler:
        sibling_generic = str(
            Path(compiler).expanduser().resolve().parent / generic_pair
        )
        if sibling_generic not in candidates:
            candidates.append(sibling_generic)
    elif generic_pair not in candidates:
        candidates.append(generic_pair)

    for candidate in candidates:
        try:
            return resolve_executable(candidate)
        except RuntimeError:
            continue

    raise RuntimeError(
        f"Required C++ compiler not found for {compiler}. Tried: {', '.join(candidates)}"
    )


def resolve_llvm_tool(clang_executable: str, tool: str) -> str:
    clang_path = Path(clang_executable)
    version_suffix = clang_path.name.removeprefix("clang")
    tool_names = [f"{tool}{version_suffix}"] if version_suffix else []
    tool_names.append(tool)

    sibling_candidates = list(
        dict.fromkeys(clang_path.parent / name for name in tool_names)
    )
    for candidate in sibling_candidates:
        if candidate.is_file():
            return os.path.abspath(candidate)

    for tool_name in tool_names:
        executable = shutil.which(tool_name)
        if executable is not None:
            return os.path.abspath(executable)

    candidates = [*(str(candidate) for candidate in sibling_candidates), *tool_names]
    raise RuntimeError(
        f"Required LLVM tool not found for {clang_executable}. Tried: "
        f"{', '.join(candidates)}"
    )


def detect_clang_version(executable: str) -> str:
    output = subprocess.run(
        [executable, "--version"],
        check=True,
        capture_output=True,
        text=True,
    )
    version_match = re.search(r"(?:clang|Apple clang) version (\d+)", output.stdout)
    if version_match is None:
        raise RuntimeError(f"Cannot detect clang version from {executable}")
    return version_match.group(1)


def detect_gcc_version(executable: str) -> str:
    for flag in ("-dumpfullversion", "-dumpversion"):
        version = run_command([executable, flag])
        version_match = re.match(r"(\d+)", version)
        if version_match is not None:
            return version_match.group(1)
    raise RuntimeError(f"Cannot detect GCC version from {executable}")


def detect_compiler(compiler: str) -> CompilerInfo:
    compiler_name = Path(compiler).name
    family = "clang" if compiler_name.startswith("clang") else "gcc"
    c_path = resolve_executable(compiler)
    cpp_path = resolve_compiler_pair(compiler, family)
    version_detector = detect_clang_version if family == "clang" else detect_gcc_version
    c_version = version_detector(c_path)
    cpp_version = version_detector(cpp_path)
    if c_version != cpp_version:
        raise RuntimeError(
            f"C compiler {c_path} version {c_version} does not match "
            f"C++ compiler {cpp_path} version {cpp_version}"
        )
    return CompilerInfo(
        family=family,
        version=c_version,
        executables={"c": c_path, "cpp": cpp_path},
    )


def normalize_libcxx(libcxx: str) -> str:
    if libcxx in ("libc++", "libstdc++11"):
        return libcxx
    raise ValueError(
        f"Unsupported C++ runtime library: {libcxx}. "
        "Expected one of: libc++, libstdc++11"
    )


def read_profile_lines(profile_file: Path) -> list[str]:
    if not profile_file.exists():
        return []
    return profile_file.read_text().splitlines()


def update_settings_user_file(settings_user_file: Path) -> None:
    if settings_user_file.exists():
        data = yaml.safe_load(settings_user_file.read_text()) or {}
    else:
        data = {}

    os_settings = data.setdefault("os", {})
    linux_settings = os_settings.setdefault("Linux", {})
    if linux_settings is None:
        linux_settings = {}
        os_settings["Linux"] = linux_settings

    linux_settings["libc"] = [None, "gnu", "musl"]
    linux_settings["libc_version"] = [None, "ANY"]
    settings_user_file.write_text(yaml.safe_dump(data, sort_keys=False))


def update_profile_with_libc_template(profile_file: Path) -> None:
    lines = read_profile_lines(profile_file)
    output: list[str] = []
    in_settings = False
    settings_seen = False
    template_inserted = False

    for line in lines:
        stripped = line.strip()
        is_section = stripped.startswith("[") and stripped.endswith("]")

        if stripped == PROFILE_TEMPLATE_PREAMBLE:
            continue

        if stripped == "[settings]":
            settings_seen = True
            in_settings = True
            output.append(line)
            continue

        if is_section:
            if in_settings and not template_inserted:
                output.extend(PROFILE_TEMPLATE_SETTINGS)
                template_inserted = True
            in_settings = False
            output.append(line)
            continue

        if in_settings and (
            stripped in PROFILE_TEMPLATE_SETTINGS
            or stripped.startswith("os.libc=")
            or stripped.startswith("os.libc_version=")
        ):
            continue

        output.append(line)

    if not settings_seen:
        output = ["[settings]", *PROFILE_TEMPLATE_SETTINGS, "", *output]
    elif in_settings and not template_inserted:
        output.extend(PROFILE_TEMPLATE_SETTINGS)

    while output and output[0] == "":
        output.pop(0)

    profile_file.parent.mkdir(parents=True, exist_ok=True)
    profile_file.write_text(
        "\n".join([PROFILE_TEMPLATE_PREAMBLE, "", *output]).rstrip() + "\n"
    )


def update_profile_entries(
    profile_file: Path,
    section: str,
    entries: dict[str, str],
    remove_keys: set[str] | None = None,
    separator: str = "=",
) -> None:
    output: list[str] = []
    in_section = False
    section_seen = False
    removed = remove_keys or set()

    for line in read_profile_lines(profile_file):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_section = stripped == f"[{section}]"
            output.append(line)
            if in_section:
                section_seen = True
                output.extend(
                    f"{key}{separator}{value}" for key, value in entries.items()
                )
            continue

        if in_section and separator in stripped:
            key, _ = stripped.split(separator, 1)
            if (
                section == "conf"
                and key.strip() == "tools.cmake.cmaketoolchain:toolset_arch"
            ):
                continue
            if key.strip() in entries or key.strip() in removed:
                continue
        output.append(line)

    if not section_seen and entries:
        if output and output[-1] != "":
            output.append("")
        output.append(f"[{section}]")
        output.extend(f"{key}{separator}{value}" for key, value in entries.items())

    profile_file.write_text("\n".join(output).rstrip() + "\n")


def parse_json_list(value: str, key: str) -> list[str]:
    parsed = json.loads(value)
    if not isinstance(parsed, list) or not all(
        isinstance(flag, str) for flag in parsed
    ):
        raise ValueError(f"{key} must be a JSON list of strings")
    return parsed


def read_existing_conf_lists(profile_file: Path) -> dict[str, list[str]]:
    flags = {key: [] for key in GCC_TOOLCHAIN_FLAG_KEYS}
    in_conf = False

    for line in read_profile_lines(profile_file):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_conf = stripped == "[conf]"
            continue
        if not in_conf or "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        normalized_key = key.strip()
        if normalized_key in flags:
            flags[normalized_key] = parse_json_list(value, normalized_key)

    return flags


def lto_package_id_entries(profile_file: Path, lto: str) -> dict[str, str]:
    package_id_confs: list[str] = []
    in_conf = False

    for line in read_profile_lines(profile_file):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_conf = stripped == "[conf]"
            continue
        if not in_conf or "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        if key.strip() == PACKAGE_ID_CONFS_KEY:
            package_id_confs = parse_json_list(value, PACKAGE_ID_CONFS_KEY)

    package_id_confs = [key for key in package_id_confs if key != LTO_CONF_KEY]
    if lto == "none":
        return (
            {PACKAGE_ID_CONFS_KEY: json.dumps(package_id_confs)}
            if package_id_confs
            else {}
        )

    package_id_confs.append(LTO_CONF_KEY)
    return {
        LTO_CONF_KEY: lto,
        PACKAGE_ID_CONFS_KEY: json.dumps(package_id_confs),
    }


def toolchain_flag_entries(
    profile_file: Path,
    toolchain_root: Path | None,
    linker: str | None,
    lto: str,
    extra_cflags: list[str],
    extra_link_flags: list[str],
) -> dict[str, str]:
    flags = read_existing_conf_lists(profile_file)
    entries: dict[str, str] = {}

    for key, values in flags.items():
        normalized_values = [
            flag
            for flag in values
            if not flag.startswith("--gcc-toolchain=")
            and not flag.startswith("-fuse-ld=")
            and not flag.startswith("-flto")
        ]
        extra_flags = (
            extra_cflags
            if key in ("tools.build:cflags", "tools.build:cxxflags")
            else extra_link_flags
        )
        if extra_flags and normalized_values[-len(extra_flags) :] != extra_flags:
            normalized_values.extend(extra_flags)
        if toolchain_root is not None:
            normalized_values.append(f"--gcc-toolchain={toolchain_root}")
        if linker and key in (
            "tools.build:exelinkflags",
            "tools.build:sharedlinkflags",
        ):
            normalized_values.append(f"-fuse-ld={linker}")
        if lto != "none":
            normalized_values.append(f"-flto={lto}")
        entries[key] = json.dumps(normalized_values)

    return entries


def gcc_toolchain_root(gcc_executable: str) -> Path:
    gcc_path = Path(gcc_executable)
    if gcc_path.parent.name == "bin":
        return gcc_path.parent.parent
    return gcc_path.parent


def discover_latest_gcc_toolchain() -> GccToolchainInfo:
    gcc_pattern = re.compile(r"^gcc(?:-\d+(?:\.\d+)*)?$")
    candidates: dict[str, GccToolchainInfo] = {}

    for path_dir in os.environ.get("PATH", "").split(os.pathsep):
        if not path_dir:
            continue
        directory = Path(path_dir)
        if not directory.is_dir():
            continue
        for entry in directory.iterdir():
            if not gcc_pattern.fullmatch(entry.name):
                continue
            try:
                compiler = detect_compiler(str(entry))
            except RuntimeError:
                continue
            if compiler.family != "gcc":
                continue
            candidates[compiler.executables["c"]] = GccToolchainInfo(
                version=compiler.version,
                root=gcc_toolchain_root(compiler.executables["c"]),
                executables=compiler.executables,
            )

    if not candidates:
        raise RuntimeError(
            "Cannot find any usable GCC toolchain on PATH. "
            "Install gcc-xx/g++-xx or use --libcxx libc++ with clang."
        )

    return max(
        candidates.values(),
        key=lambda toolchain: (int(toolchain.version), toolchain.executables["c"]),
    )


def existing_profile_settings(profile_file: Path) -> dict[str, str]:
    settings: dict[str, str] = {}
    in_settings = False

    for line in read_profile_lines(profile_file):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_settings = stripped == "[settings]"
            continue
        if not in_settings or "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        settings[key.strip()] = value.strip()

    return settings


def update_selected_profile(
    profile_file: Path,
    profile_name: str,
    compiler_info: CompilerInfo,
    libcxx: str,
    lto: str,
    linker: str | None,
    extra_cflags: list[str],
    extra_link_flags: list[str],
) -> None:
    profile_file.parent.mkdir(parents=True, exist_ok=True)
    update_profile_with_libc_template(profile_file)

    profile_settings = existing_profile_settings(profile_file)
    defaults = {
        key: value
        for key, value in PROFILE_DEFAULT_SETTINGS.items()
        if key not in profile_settings
    }
    compiler_libcxx = normalize_libcxx(libcxx)

    conf = {
        "tools.build:compiler_executables": json.dumps(
            compiler_info.executables, sort_keys=True
        ),
    }

    selected_gcc_toolchain: GccToolchainInfo | None = None
    if compiler_info.family == "clang" and compiler_libcxx == "libstdc++11":
        selected_gcc_toolchain = discover_latest_gcc_toolchain()
    selected_linker = linker
    if selected_linker is None and compiler_info.family == "clang":
        selected_linker = "lld"
    if selected_linker == "ld":
        selected_linker = None
    conf.update(
        toolchain_flag_entries(
            profile_file,
            selected_gcc_toolchain.root if selected_gcc_toolchain is not None else None,
            linker=selected_linker,
            lto=lto if compiler_info.family == "clang" else "none",
            extra_cflags=extra_cflags,
            extra_link_flags=extra_link_flags,
        )
    )
    effective_lto = lto if compiler_info.family == "clang" else "none"
    conf.update(lto_package_id_entries(profile_file, effective_lto))
    llvm_tools: dict[str, str] = {}
    if compiler_info.family == "clang":
        llvm_tools = {
            "AR": resolve_llvm_tool(compiler_info.executables["c"], "llvm-ar"),
            "RANLIB": resolve_llvm_tool(compiler_info.executables["c"], "llvm-ranlib"),
        }

    print(f"🔧 Updating Conan profile: {profile_file}")
    update_profile_entries(
        profile_file,
        "settings",
        {
            **defaults,
            "compiler": compiler_info.family,
            "compiler.version": compiler_info.version,
            "compiler.cppstd": "gnu20",
            "compiler.libcxx": compiler_libcxx,
        },
    )
    update_profile_entries(
        profile_file,
        "conf",
        conf,
        remove_keys={LTO_CONF_KEY, PACKAGE_ID_CONFS_KEY} - conf.keys(),
    )
    update_profile_entries(
        profile_file,
        "buildenv",
        llvm_tools,
        remove_keys={"AR", "RANLIB"} - llvm_tools.keys(),
    )
    update_profile_entries(
        profile_file,
        "replace_tool_requires",
        CMAKE_TOOL_REQUIRE,
        separator=": ",
    )

    summary = (
        f"✅ Profile '{profile_name}' configured for "
        f"{compiler_info.family} {compiler_info.version}, compiler.libcxx={compiler_libcxx}, "
        "compiler.cppstd=gnu20, absolute compiler paths, "
        f"CMake {CMAKE_VERSION}, and os.libc/os.libc_version detection"
    )
    if selected_gcc_toolchain is not None:
        summary += (
            f" using GCC toolchain {selected_gcc_toolchain.version} at "
            f"{selected_gcc_toolchain.root}"
        )
    if selected_linker:
        summary += f" with linker={selected_linker}"
    if compiler_info.family == "clang" and lto != "none":
        summary += f" with {lto} LTO"
    print(summary)


def main() -> int:
    args = parse_args()
    # Initialize Conan 2 first (e.g. conan profile detect); set CONAN_HOME to target another cache.
    conan_home = Path(os.environ.get("CONAN_HOME", "~/.conan2")).expanduser()
    settings_file = conan_home / "settings.yml"
    settings_user_file = conan_home / "settings_user.yml"

    if not settings_file.is_file():
        print(f"❌ Conan settings file not found: {settings_file}", file=sys.stderr)
        return 1

    compiler_info: CompilerInfo | None = None
    if not args.skip_profile:
        try:
            compiler_info = detect_compiler(args.compiler)
        except (OSError, RuntimeError, subprocess.SubprocessError) as error:
            print(
                f"❌ Cannot use compiler '{args.compiler}': {error}",
                file=sys.stderr,
            )
            return 1

    print(f"🔧 Updating Conan user settings file: {settings_user_file}")
    update_settings_user_file(settings_user_file)
    print(
        "✅ Added Linux settings override to settings_user.yml: os.libc / os.libc_version"
    )

    if args.skip_profile:
        print("ℹ️  Skipped profile update by request")
        return 0

    assert compiler_info is not None
    profile_file = conan_home / "profiles" / args.profile_name
    update_selected_profile(
        profile_file,
        args.profile_name,
        compiler_info,
        args.libcxx,
        args.lto,
        args.linker,
        args.extra_cflags,
        args.extra_link_flags,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
