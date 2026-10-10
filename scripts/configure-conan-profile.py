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
# 2. It configures global LTO flags and Clang PGO and includes the profile content hash in package IDs.
#    When lto=thin or --pgo-profile is specified, the LTO setting and the hash of the PGO
#    profile data are included as conf keys in package_id, ensuring that different LTO
#    settings and PGO profile data produce different package IDs.
#
# Usage examples:
#   # Show all supported options.
#   python3 scripts/configure-conan-profile.py --help
#
#   # Configure the default profile using an automatically selected toolchain.
#   python3 scripts/configure-conan-profile.py
#
# Default toolchain selection:
#
# | Platform and available toolchains | Compiler | C++ standard library | Linker |
# | --- | --- | --- | --- |
# | macOS with Apple Clang | Apple Clang | libc++ | lld |
# | Other platforms with Clang and GCC | Highest-version Clang | libstdc++11 using the highest-version GCC toolchain | lld |
# | Other platforms with only Clang | Highest-version Clang | libc++ | lld |
# | Other platforms with GCC but no Clang | Highest-version GCC | libstdc++11 | ld |
#
# Explicit --compiler, --libcxx, and --linker options override these defaults.
# Use --linker ld to select the compiler-default linker without adding a
# -fuse-ld flag.
# The selected profile is deleted and regenerated from scratch; profiles with
# other names are left unchanged.
#
#   # Configure a named profile for Clang 18, libc++, and ThinLTO.
#   python3 scripts/configure-conan-profile.py \
#       --profile clang22-thinlto --compiler clang-22 --libcxx libc++ --lto thin
#
#   # Configure a named profile for GCC 16 and libstdc++11.
#   python3 scripts/configure-conan-profile.py \
#       --compiler gcc-16 --libcxx libstdc++11
#
#   # Select mold explicitly, or use --linker ld to keep the compiler default.
#   python3 scripts/configure-conan-profile.py --linker mold
#
#   # Override libc detection for the target environment.
#   python3 scripts/configure-conan-profile.py --libc musl --libc-version 1.2.5
#
#   # Add custom compiler and linker flags to the profile.
#   python3 scripts/configure-conan-profile.py \
#       --compiler=clang                    \
#       --extra-cflags="-g -fdebug-info-for-profiling -fpseudo-probe-for-profiling" \
#       --extra-link-flags="-Wl,--as-needed" \
#       --libc gnu --libc-version 2.17
#
#   # Only add os.libc/os.libc_version to settings_user.yml.
#   python3 scripts/configure-conan-profile.py --skip-profile
#
#   # Use a converted perf profile (use instrumentation for merged .profraw data).
#   python3 dev/configure-conan-profile.py --lto thin \
#       --pgo-profile /absolute/path/pgo.prof --pgo-mode sample

import argparse
import ast
import hashlib
import os
import re
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml


DEFAULT_PROFILE_NAME = "default"
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
PGO_MODE_CONF_KEY = "user.pgo:mode"
PGO_HASH_CONF_KEY = "user.pgo:sha256"
PACKAGE_ID_CONFS_KEY = "tools.info.package_id:confs"
PGO_USE_FLAGS = ("-fprofile-use", "-fprofile-sample-use", "-fprofile-instr-use")
PGO_GENERATE_FLAGS = (
    "-fprofile-generate",
    "-fprofile-instr-generate",
    "-fcs-profile-generate",
)
PGO_MAPPING_FLAGS = ("-fpseudo-probe-for-profiling", "-fdebug-info-for-profiling")
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


@dataclass(frozen=True)
class PgoProfile:
    mode: str
    sha256: str
    flag: str


def read_pgo_profile(path: Path, mode: str) -> PgoProfile:
    path = path.expanduser().resolve(strict=True)
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    if size == 0:
        raise ValueError(f"PGO profile is empty: {path}")
    option = {
        "sample": "-fprofile-sample-use",
        "instrumentation": "-fprofile-use",
    }[mode]
    # Conan joins flags into a shell command; preserve paths containing spaces.
    return PgoProfile(mode, digest.hexdigest(), shlex.quote(f"{option}={path}"))


def is_pgo_flag(flag: str, options: tuple[str, ...]) -> bool:
    return flag.split("=", 1)[0] in options


def remove_pgo_flags(flags: list[str]) -> list[str]:
    result: list[str] = []
    iterator = iter(flags)
    for flag in iterator:
        # Managed flags can be shell-quoted to support paths containing spaces.
        normalized = flag.lstrip("\"'")
        if normalized in PGO_USE_FLAGS:
            next(iterator, None)  # Also handle a separate path argument.
        elif not is_pgo_flag(
            normalized, PGO_USE_FLAGS + PGO_GENERATE_FLAGS + PGO_MAPPING_FLAGS
        ):
            result.append(flag)
    return result


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
        help=f"Conan profile name to replace (default: {DEFAULT_PROFILE_NAME})",
    )
    # Use --skip-profile to extend Linux libc settings without requiring installed compilers.
    parser.add_argument(
        "--skip-profile",
        action="store_true",
        help="Only update settings_user.yml, do not touch any Conan profile",
    )
    parser.add_argument(
        "--compiler",
        help=(
            "Compiler name or path, for example clang, gcc-12 or "
            "/opt/compiler/bin/clang (default: auto-detect)"
        ),
    )
    # Clang with libstdc++ selects the newest GCC/G++ pair on PATH; --libcxx libc++ skips this.
    parser.add_argument(
        "--libcxx",
        choices=("libc++", "libstdc++11"),
        help="C++ runtime library to configure (default: auto-detect)",
    )
    parser.add_argument(
        "--libc",
        choices=("gnu", "musl"),
        help="Override os.libc (default: auto-detect)",
    )
    parser.add_argument(
        "--libc-version",
        help="Override os.libc_version (default: auto-detect)",
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
            "Linker for -fuse-ld (default: lld for Clang/Apple Clang, ld otherwise); "
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
    parser.add_argument(
        "--pgo-profile",
        type=Path,
        help="LLVM profile file to hash and use; omit to disable previous PGO settings",
    )
    parser.add_argument(
        "--pgo-mode",
        choices=("sample", "instrumentation"),
        default="sample",
        help="PGO profile format (default: sample)",
    )
    args = parser.parse_args()
    compiler_name = Path(args.compiler).name if args.compiler else None
    if compiler_name is not None and not compiler_name.startswith(("clang", "gcc")):
        parser.error("--compiler must name a clang or gcc compiler")
    if args.pgo_profile and (
        args.skip_profile
        or (compiler_name is not None and not compiler_name.startswith("clang"))
    ):
        parser.error(
            "--pgo-profile requires Clang and cannot be used with --skip-profile"
        )
    for flag in args.extra_cflags + args.extra_link_flags:
        if is_pgo_flag(flag, PGO_USE_FLAGS):
            parser.error("Use --pgo-profile and --pgo-mode instead of PGO use flags")
        if args.pgo_profile and is_pgo_flag(flag, PGO_GENERATE_FLAGS):
            parser.error("PGO generation flags cannot be combined with --pgo-profile")
    try:
        args.pgo = (
            read_pgo_profile(args.pgo_profile, args.pgo_mode)
            if args.pgo_profile
            else None
        )
    except (OSError, ValueError) as error:
        parser.error(f"Cannot read PGO profile: {error}")
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


def detect_clang(executable: str) -> tuple[str, str]:
    output = subprocess.run(
        [executable, "--version"],
        check=True,
        capture_output=True,
        text=True,
    )
    version_match = re.search(r"(Apple clang|clang) version (\d+)", output.stdout)
    if version_match is None:
        raise RuntimeError(f"Cannot detect clang version from {executable}")
    family = "apple-clang" if version_match.group(1) == "Apple clang" else "clang"
    return family, version_match.group(2)


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
    if family == "clang":
        c_family, c_version = detect_clang(c_path)
        cpp_family, cpp_version = detect_clang(cpp_path)
    else:
        c_family = cpp_family = family
        c_version = detect_gcc_version(c_path)
        cpp_version = detect_gcc_version(cpp_path)
    if (c_family, c_version) != (cpp_family, cpp_version):
        raise RuntimeError(
            f"C compiler {c_path} ({c_family} {c_version}) does not match "
            f"C++ compiler {cpp_path} ({cpp_family} {cpp_version})"
        )
    return CompilerInfo(
        family=c_family,
        version=c_version,
        executables={"c": c_path, "cpp": cpp_path},
    )


def is_clang_family(family: str) -> bool:
    return family in ("clang", "apple-clang")


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


def update_profile_with_libc_template(
    profile_file: Path,
    libc: str | None = None,
    libc_version: str | None = None,
) -> None:
    libc_settings: list[str] = []
    for key, value in (("libc", libc), ("libc_version", libc_version)):
        if value is None:
            libc_settings.extend(
                [f"{{% if {key} %}}", f"os.{key}={{{{ {key} }}}}", "{% endif %}"]
            )
        else:
            libc_settings.append(f"os.{key}={value}")

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
                output.extend(libc_settings)
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
        output = ["[settings]", *libc_settings, "", *output]
    elif in_settings and not template_inserted:
        output.extend(libc_settings)

    while output and output[0] == "":
        output.pop(0)

    profile_file.parent.mkdir(parents=True, exist_ok=True)
    if libc is None or libc_version is None:
        output = [PROFILE_TEMPLATE_PREAMBLE, "", *output]
    profile_file.write_text("\n".join(output).rstrip() + "\n")


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
    """Parse Conan string lists with single or double quotes."""
    try:
        parsed = ast.literal_eval(value.strip())
    except (SyntaxError, ValueError) as error:
        raise ValueError(f"{key} must be a list of strings") from error
    if not isinstance(parsed, list) or not all(
        isinstance(flag, str) for flag in parsed
    ):
        raise ValueError(f"{key} must be a list of strings")
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


def lto_package_id_entries(
    profile_file: Path, lto: str, pgo: PgoProfile | None = None
) -> dict[str, str]:
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

    managed_keys = {LTO_CONF_KEY, PGO_MODE_CONF_KEY, PGO_HASH_CONF_KEY}
    package_id_confs = [key for key in package_id_confs if key not in managed_keys]
    entries: dict[str, str] = {}
    if lto != "none":
        entries[LTO_CONF_KEY] = lto
    if pgo is not None:
        entries[PGO_MODE_CONF_KEY] = pgo.mode
        entries[PGO_HASH_CONF_KEY] = pgo.sha256
    package_id_confs.extend(entries)
    if package_id_confs:
        entries[PACKAGE_ID_CONFS_KEY] = repr(package_id_confs)
    return entries


def toolchain_flag_entries(
    profile_file: Path,
    toolchain_root: Path | None,
    linker: str | None,
    lto: str,
    extra_cflags: list[str],
    extra_link_flags: list[str],
    pgo: PgoProfile | None = None,
) -> dict[str, str]:
    flags = read_existing_conf_lists(profile_file)
    entries: dict[str, str] = {}

    for key, values in flags.items():
        normalized_values = [
            flag
            for flag in remove_pgo_flags(values)
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
        if pgo is not None:
            normalized_values.append(pgo.flag)
        if toolchain_root is not None:
            normalized_values.append(f"--gcc-toolchain={toolchain_root}")
        if linker and key in (
            "tools.build:exelinkflags",
            "tools.build:sharedlinkflags",
        ):
            normalized_values.append(f"-fuse-ld={linker}")
        if lto != "none":
            normalized_values.append(f"-flto={lto}")
        entries[key] = repr(normalized_values)

    return entries


def gcc_toolchain_root(gcc_executable: str) -> Path:
    gcc_path = Path(gcc_executable)
    if gcc_path.parent.name == "bin":
        return gcc_path.parent.parent
    return gcc_path.parent


def discover_latest_clang() -> CompilerInfo:
    clang_pattern = re.compile(r"^clang(?:-\d+(?:\.\d+)*)?$")
    candidates: dict[str, CompilerInfo] = {}

    for path_dir in os.environ.get("PATH", "").split(os.pathsep):
        if not path_dir:
            continue
        directory = Path(path_dir)
        if not directory.is_dir():
            continue
        for entry in directory.iterdir():
            if not clang_pattern.fullmatch(entry.name):
                continue
            try:
                compiler = detect_compiler(str(entry))
            except (RuntimeError, subprocess.CalledProcessError, OSError):
                continue
            if compiler.family != "clang":
                continue
            candidates[compiler.executables["c"]] = compiler

    if not candidates:
        raise RuntimeError("Cannot find any usable Clang toolchain on PATH")

    return max(
        candidates.values(),
        key=lambda candidate: (int(candidate.version), candidate.executables["c"]),
    )


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
            except (RuntimeError, subprocess.CalledProcessError, OSError):
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


def discover_apple_clang() -> CompilerInfo:
    candidates: list[str] = []
    try:
        candidates.append(run_command(["xcrun", "--find", "clang"]))
    except (OSError, subprocess.SubprocessError):
        pass
    candidates.append("clang")

    for candidate in dict.fromkeys(candidates):
        try:
            compiler = detect_compiler(candidate)
        except (OSError, RuntimeError, subprocess.SubprocessError):
            continue
        if compiler.family == "apple-clang":
            return compiler

    raise RuntimeError("Cannot find a usable Apple Clang toolchain")


def select_compiler_and_libcxx(
    compiler: str | None, libcxx: str | None
) -> tuple[CompilerInfo, str]:
    if compiler is not None:
        compiler_info = detect_compiler(compiler)
    elif sys.platform == "darwin":
        compiler_info = discover_apple_clang()
    else:
        try:
            compiler_info = discover_latest_clang()
        except RuntimeError:
            gcc = discover_latest_gcc_toolchain()
            compiler_info = CompilerInfo("gcc", gcc.version, gcc.executables)

    if libcxx is not None:
        return compiler_info, libcxx
    if compiler_info.family == "apple-clang":
        return compiler_info, "libc++"
    if compiler_info.family == "gcc":
        return compiler_info, "libstdc++11"

    try:
        discover_latest_gcc_toolchain()
        return compiler_info, "libstdc++11"
    except RuntimeError:
        return compiler_info, "libc++"


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
    libc: str | None = None,
    libc_version: str | None = None,
    pgo: PgoProfile | None = None,
) -> None:
    profile_file.unlink(missing_ok=True)
    profile_file.parent.mkdir(parents=True, exist_ok=True)
    update_profile_with_libc_template(profile_file, libc, libc_version)

    profile_settings = existing_profile_settings(profile_file)
    defaults = {
        key: value
        for key, value in PROFILE_DEFAULT_SETTINGS.items()
        if key not in profile_settings
    }
    compiler_libcxx = normalize_libcxx(libcxx)

    conf = {
        "tools.build:compiler_executables": repr(
            dict(sorted(compiler_info.executables.items()))
        ),
    }

    selected_gcc_toolchain: GccToolchainInfo | None = None
    if compiler_info.family == "clang" and compiler_libcxx == "libstdc++11":
        selected_gcc_toolchain = discover_latest_gcc_toolchain()
    selected_linker = linker
    if selected_linker is None and is_clang_family(compiler_info.family):
        selected_linker = "lld"
    if selected_linker == "ld":
        selected_linker = None
    conf.update(
        toolchain_flag_entries(
            profile_file,
            selected_gcc_toolchain.root if selected_gcc_toolchain is not None else None,
            linker=selected_linker,
            lto=lto if is_clang_family(compiler_info.family) else "none",
            extra_cflags=extra_cflags,
            extra_link_flags=extra_link_flags,
            pgo=pgo,
        )
    )
    effective_lto = lto if is_clang_family(compiler_info.family) else "none"
    conf.update(lto_package_id_entries(profile_file, effective_lto, pgo))
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
        remove_keys={
            LTO_CONF_KEY,
            PGO_MODE_CONF_KEY,
            PGO_HASH_CONF_KEY,
            PACKAGE_ID_CONFS_KEY,
        }
        - conf.keys(),
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
        f"CMake {CMAKE_VERSION}, "
        f"os.libc={libc if libc is not None else 'auto-detect'}, "
        f"os.libc_version={libc_version if libc_version is not None else 'auto-detect'}"
    )
    if selected_gcc_toolchain is not None:
        summary += (
            f" using GCC toolchain {selected_gcc_toolchain.version} at "
            f"{selected_gcc_toolchain.root}"
        )
    if selected_linker:
        summary += f" with linker={selected_linker}"
    if is_clang_family(compiler_info.family) and lto != "none":
        summary += f" with {lto} LTO"
    if pgo is not None:
        summary += f" with {pgo.mode} PGO (SHA-256: {pgo.sha256})"
    print(summary)


def main() -> int:
    args = parse_args()
    # Set CONAN_HOME to target another cache.
    conan_home = Path(os.environ.get("CONAN_HOME") or "~/.conan2").expanduser()
    settings_file = conan_home / "settings.yml"

    if not settings_file.is_file():
        try:
            conan_home = Path(run_command(["conan", "config", "home"]))
        except (OSError, subprocess.SubprocessError) as error:
            print(f"❌ Cannot initialize Conan home: {error}", file=sys.stderr)
            return 1
        settings_file = conan_home / "settings.yml"

    if not settings_file.is_file():
        print(f"❌ Conan settings file not found: {settings_file}", file=sys.stderr)
        return 1

    settings_user_file = conan_home / "settings_user.yml"

    compiler_info: CompilerInfo | None = None
    libcxx: str | None = None
    if not args.skip_profile:
        try:
            compiler_info, libcxx = select_compiler_and_libcxx(
                args.compiler, args.libcxx
            )
        except (OSError, RuntimeError, subprocess.SubprocessError) as error:
            requested_compiler = args.compiler or "an automatically selected compiler"
            print(
                f"❌ Cannot use {requested_compiler}: {error}",
                file=sys.stderr,
            )
            return 1
        if args.pgo is not None and not is_clang_family(compiler_info.family):
            print("❌ --pgo-profile requires Clang", file=sys.stderr)
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
    assert libcxx is not None
    profile_file = conan_home / "profiles" / args.profile_name
    update_selected_profile(
        profile_file,
        args.profile_name,
        compiler_info,
        libcxx,
        args.lto,
        args.linker,
        args.extra_cflags,
        args.extra_link_flags,
        libc=args.libc,
        libc_version=args.libc_version,
        pgo=args.pgo,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
