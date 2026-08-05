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

"""Select linkers from the effective Conan host and build profiles."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


COMPILER_EXECUTABLES_CONF = "tools.build:compiler_executables"
EXE_LINK_FLAGS_CONF = "tools.build:exelinkflags"
SHARED_LINK_FLAGS_CONF = "tools.build:sharedlinkflags"
BOLT_LINKER_CONF = "user.bolt:linker"
FUSE_LD_PREFIX = "-fuse-ld="
UNSET = object()


class LinkerSelectionError(Exception):
    pass


@dataclass(frozen=True)
class LinkerSelection:
    linker: str | None
    compiler: tuple[str, ...] | None
    source: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect effective Conan profiles and emit linker-related Conan options"
        )
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="File that receives one line of Conan command-line options",
    )
    parser.add_argument(
        "profile_command",
        nargs=argparse.REMAINDER,
        help="The `conan profile show --format=json` command to execute",
    )
    return parser.parse_args()


def load_profiles(command: list[str], runner=subprocess.run) -> dict:
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise LinkerSelectionError("missing Conan profile command")

    result = runner(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise LinkerSelectionError(f"Conan profile inspection failed: {detail}")

    try:
        profiles = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise LinkerSelectionError(
            f"Conan profile inspection returned invalid JSON: {error}"
        ) from error

    for context in ("host", "build"):
        if not isinstance(profiles.get(context), dict):
            raise LinkerSelectionError(
                f"Conan profile output is missing the {context} context"
            )
    return profiles


def _command_from_value(value: object) -> tuple[str, ...] | None:
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return tuple(value) or None
    if not isinstance(value, str) or not value.strip():
        return None
    if Path(value).exists():
        return (value,)
    command = tuple(shlex.split(value))
    return command or None


def _expected_version(version: object) -> tuple[int, ...]:
    if version is None:
        return ()
    return tuple(int(part) for part in re.findall(r"\d+", str(version)))


def _compiler_defines(
    command: tuple[str, ...], runner=subprocess.run
) -> dict[str, str] | None:
    try:
        result = runner(
            [*command, "-dM", "-E", "-x", "c++", os.devnull],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except OSError:
        return None
    if result.returncode != 0:
        return None

    defines: dict[str, str] = {}
    for line in result.stdout.splitlines():
        parts = line.split(maxsplit=2)
        if len(parts) == 3 and parts[0] == "#define":
            defines[parts[1]] = parts[2]
    return defines


def compiler_matches_profile(
    command: tuple[str, ...], profile: dict, runner=subprocess.run
) -> bool:
    settings = profile.get("settings", {})
    compiler = str(settings.get("compiler", ""))
    expected = _expected_version(settings.get("compiler.version"))
    defines = _compiler_defines(command, runner=runner)
    if defines is None:
        return False

    if compiler == "gcc":
        if "__GNUC__" not in defines or "__clang__" in defines:
            return False
        actual = tuple(
            int(defines.get(name, "0"))
            for name in ("__GNUC__", "__GNUC_MINOR__", "__GNUC_PATCHLEVEL__")
        )
    elif compiler in ("clang", "apple-clang"):
        if "__clang__" not in defines:
            return False
        actual = tuple(
            int(defines.get(name, "0"))
            for name in (
                "__clang_major__",
                "__clang_minor__",
                "__clang_patchlevel__",
            )
        )
    else:
        return False

    return not expected or actual[: len(expected)] == expected


def _compiler_candidates(profile: dict) -> list[str]:
    settings = profile.get("settings", {})
    compiler = str(settings.get("compiler", ""))
    expected = _expected_version(settings.get("compiler.version"))
    major = str(expected[0]) if expected else ""

    if compiler == "gcc":
        return [
            name for name in (f"g++-{major}" if major else "", "g++", "c++") if name
        ]
    if compiler == "clang":
        return [
            name for name in (f"clang++-{major}" if major else "", "clang++") if name
        ]
    if compiler == "apple-clang":
        return ["clang++"]
    return []


def resolve_compiler(
    context: str,
    profile: dict,
    native_build: bool,
    environ: Mapping[str, str] = os.environ,
    which=shutil.which,
    runner=subprocess.run,
) -> tuple[tuple[str, ...] | None, str]:
    conf = profile.get("conf", {})
    compiler_executables = conf.get(COMPILER_EXECUTABLES_CONF, {})
    configured = None
    if isinstance(compiler_executables, dict):
        configured = _command_from_value(compiler_executables.get("cpp"))
    if configured is not None:
        if compiler_matches_profile(configured, profile, runner=runner):
            return configured, "Conan compiler_executables"
        return None, "configured C++ compiler does not match the Conan profile"

    environment_names = ["CXX"] if context == "host" else ["CXX_FOR_BUILD"]
    if context == "build" and native_build:
        environment_names.append("CXX")
    for name in environment_names:
        command = _command_from_value(environ.get(name))
        if command is None:
            continue
        if compiler_matches_profile(command, profile, runner=runner):
            return command, name
        return None, f"{name} does not match the Conan profile"

    for candidate in _compiler_candidates(profile):
        executable = which(candidate)
        if executable is None:
            continue
        command = (executable,)
        if compiler_matches_profile(command, profile, runner=runner):
            return command, "profile-compatible compiler on PATH"

    return None, "no profile-compatible C++ compiler was found"


def configured_linker(profile: dict) -> str | None:
    conf = profile.get("conf", {})
    configured: set[str] = set()
    bolt_linker = conf.get(BOLT_LINKER_CONF)
    if isinstance(bolt_linker, str) and bolt_linker:
        configured.add(bolt_linker)

    for key in (EXE_LINK_FLAGS_CONF, SHARED_LINK_FLAGS_CONF):
        flags = conf.get(key, [])
        if isinstance(flags, str):
            flags = [flags]
        if not isinstance(flags, list):
            continue
        for flag in flags:
            if isinstance(flag, str) and flag.startswith(FUSE_LD_PREFIX):
                configured.add(flag[len(FUSE_LD_PREFIX) :])

    if len(configured) > 1:
        values = ", ".join(sorted(configured))
        raise LinkerSelectionError(f"profile configures conflicting linkers: {values}")
    return next(iter(configured), None)


def probe_linker(
    command: tuple[str, ...], linker: str, profile: dict, runner=subprocess.run
) -> bool:
    conf = profile.get("conf", {})
    extra_flags = conf.get("tools.build:cxxflags", [])
    if not isinstance(extra_flags, list):
        extra_flags = []
    sysroot = conf.get("tools.build:sysroot")
    sysroot_flag = [f"--sysroot={sysroot}"] if isinstance(sysroot, str) else []

    try:
        result = runner(
            [
                *command,
                *sysroot_flag,
                *extra_flags,
                f"{FUSE_LD_PREFIX}{linker}",
                "-x",
                "c++",
                "-",
                "-o",
                os.devnull,
            ],
            check=False,
            input="int main() { return 0; }\n",
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def select_linker(
    context: str,
    profile: dict,
    native_build: bool,
    explicit: object = UNSET,
    environ: Mapping[str, str] = os.environ,
    which=shutil.which,
    runner=subprocess.run,
) -> LinkerSelection:
    if explicit is not UNSET:
        value = str(explicit)
        if not value:
            return LinkerSelection(None, None, "disabled by BOLT_LINKER")
        if any(character.isspace() for character in value):
            raise LinkerSelectionError("BOLT_LINKER cannot contain whitespace")
        return LinkerSelection(value, None, "BOLT_LINKER")

    profile_linker = configured_linker(profile)
    if profile_linker is not None:
        return LinkerSelection(profile_linker, None, "Conan profile")

    compiler_command, source = resolve_compiler(
        context,
        profile,
        native_build,
        environ=environ,
        which=which,
        runner=runner,
    )
    if compiler_command is None:
        return LinkerSelection(None, None, source)

    settings = profile.get("settings", {})
    compiler = str(settings.get("compiler", ""))
    candidates: list[str] = []
    if settings.get("os") == "Linux":
        candidates.append("mold")
    if compiler == "gcc":
        candidates.append("gold")
    elif compiler in ("clang", "apple-clang"):
        candidates.append("lld")

    for linker in candidates:
        if probe_linker(compiler_command, linker, profile, runner=runner):
            return LinkerSelection(linker, compiler_command, source)
    return LinkerSelection(None, compiler_command, "no preferred linker passed probing")


def _link_flags(profile: dict, key: str) -> list[str]:
    value = profile.get("conf", {}).get(key, [])
    if isinstance(value, str):
        return [value]
    return value if isinstance(value, list) else []


def conan_options(profiles: dict, selections: dict[str, LinkerSelection]) -> list[str]:
    options: list[str] = []
    for context, scope in (("host", "h"), ("build", "b")):
        selection = selections[context]
        if selection.linker is None:
            continue
        flag = f"{FUSE_LD_PREFIX}{selection.linker}"
        for key in (EXE_LINK_FLAGS_CONF, SHARED_LINK_FLAGS_CONF):
            if flag not in _link_flags(profiles[context], key):
                value = json.dumps([flag], separators=(",", ":"))
                options.append(f"-c:{scope}={key}+={value}")
        if context == "host":
            options.append(f"-c:h={BOLT_LINKER_CONF}={selection.linker}")
    return options


def _native_build(profiles: dict) -> bool:
    host = profiles["host"].get("settings", {})
    build = profiles["build"].get("settings", {})
    return (host.get("os"), host.get("arch")) == (
        build.get("os"),
        build.get("arch"),
    )


def _format_selection(selection: LinkerSelection) -> str:
    if selection.linker is None:
        return f"compiler default ({selection.source})"
    compiler = (
        f", compiler={' '.join(shlex.quote(part) for part in selection.compiler)}"
        if selection.compiler
        else ""
    )
    return f"{selection.linker} ({selection.source}{compiler})"


def main() -> int:
    args = parse_args()
    try:
        profiles = load_profiles(args.profile_command)
        explicit = os.environ.get("BOLT_LINKER", UNSET)
        native_build = _native_build(profiles)
        selections = {
            context: select_linker(
                context,
                profiles[context],
                native_build,
                explicit=explicit,
            )
            for context in ("host", "build")
        }
        options = conan_options(profiles, selections)
        args.output.write_text(" ".join(options) + "\n")
    except (LinkerSelectionError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    print(
        "Conan linker selection: "
        f"host={_format_selection(selections['host'])}; "
        f"build={_format_selection(selections['build'])}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
