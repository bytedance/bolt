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

"""PEP 517/660 backend: prepare Conan, then let scikit-build-core build Bolt."""

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
from urllib.parse import urlsplit

from scikit_build_core import build as _backend
from scikit_build_core.settings.skbuild_model import normalize_build_types
from scikit_build_core.settings.skbuild_read_settings import SettingsReader


RECIPE_URL = "https://github.com/bytedance/conan-center-index.git"


def systemResources():
    """Read the same CPU and whole-GiB memory values used by the Makefile."""
    if sys.platform.startswith("linux"):
        memory = {
            fields[0].rstrip(":"): int(fields[1])
            for line in Path("/proc/meminfo").read_text().splitlines()
            if (fields := line.split())[0] in ("MemTotal:", "MemFree:")
        }
        return {
            "CPU_CORES": sum(
                line.startswith("processor")
                for line in Path("/proc/cpuinfo").read_text().splitlines()
            ),
            "MEMORY": memory["MemTotal"] // (1024 * 1024),
            "FREE_MEMORY": memory["MemFree"] // (1024 * 1024),
        }
    if sys.platform == "darwin":
        memory = int(subprocess.check_output(["sysctl", "-n", "hw.memsize"]))
        cores = int(subprocess.check_output(["sysctl", "-n", "hw.ncpu"]))
        pages = next(
            int(line.split(":", 1)[1].strip().rstrip("."))
            for line in subprocess.check_output(["vm_stat"], text=True).splitlines()
            if line.startswith("Pages free:")
        )
        return {
            "CPU_CORES": cores,
            "MEMORY": memory // (1024**3),
            # Match the Makefile's existing vm_stat calculation.
            "FREE_MEMORY": pages * 4096 // (1024**3),
        }
    return {"CPU_CORES": 4, "MEMORY": 8, "FREE_MEMORY": 4}


def configureParallelism(env):
    """Apply Make's defaults and overrides to both Conan and CMake builds."""
    compileJobs = env.get("CMAKE_BUILD_PARALLEL_LEVEL") or env.get(
        "NUM_THREADS", env.get("CI_NUM_THREADS") or None
    )
    linkJobs = env.get("NUM_LINK_JOB", env.get("CI_NUM_LINK_JOB") or None)
    if compileJobs is None or linkJobs is None:
        keys = ("CPU_CORES", "MEMORY", "FREE_MEMORY")
        resources = {} if all(key in env for key in keys) else systemResources()
        resources.update({key: int(env[key]) for key in keys if key in env})
        if compileJobs is None:
            compileJobs = max(1, min(resources["CPU_CORES"], resources["MEMORY"] // 4))
        if linkJobs is None:
            linkJobs = max(4, resources["FREE_MEMORY"] // 10)
    env["CMAKE_BUILD_PARALLEL_LEVEL"] = str(compileJobs)
    env["NUM_THREADS"] = str(compileJobs)
    env["NUM_LINK_JOB"] = str(linkJobs)


def run(command, env, *, cwd, capture=False):
    return subprocess.run(
        command,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE if capture else sys.stderr,
        text=True,
        check=True,
    )


def recipeRemotes(conan, env, sourceRoot, outputDir):
    remotes = json.loads(
        run(
            [*conan, "remote", "list", "--format=json"],
            env,
            cwd=sourceRoot,
            capture=True,
        ).stdout
    )

    def installRemotes(recipe):
        enabled = [remote for remote in remotes if remote.get("enabled", True)]
        names = [remote["name"] for remote in enabled]
        if recipe not in names:
            names.append(recipe)
        # Preserve configured cache priority, but never let ConanCenter's
        # unpatched recipes shadow Bolt's recipes. Apply this order only to the
        # install command; do not rewrite existing remote configuration.
        for remote in enabled:
            if remote["name"] == "conancenter" or urlsplit(remote["url"]).hostname in (
                "center.conan.io",
                "center2.conan.io",
            ):
                if names.index(recipe) > names.index(remote["name"]):
                    names.remove(recipe)
                    names.insert(names.index(remote["name"]), recipe)
                break
        return names

    for remote in remotes:
        if (
            remote["name"] == "bolt-cci-local"
            and remote.get("enabled", True)
            and (Path(remote["url"]) / "recipes").is_dir()
        ):
            return installRemotes(remote["name"])

    # Keep an existing user checkout untouched. The fallback lives in Conan's
    # persistent cache, even when building from a temporary extracted sdist.
    recipes = outputDir / "recipes"
    name = (
        "bolt-python-"
        + hashlib.sha256(str(recipes.resolve()).encode()).hexdigest()[:12]
    )
    existing = next((remote for remote in remotes if remote["name"] == name), None)
    if existing and (
        not existing.get("enabled", True)
        or Path(existing["url"]).resolve() != recipes.resolve()
    ):
        raise ValueError(f"Conan remote {name} conflicts with {recipes}")
    if not recipes.exists():
        recipes.parent.mkdir(parents=True, exist_ok=True)
        run(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "--branch",
                "main",
                RECIPE_URL,
                str(recipes),
            ],
            env,
            cwd=sourceRoot,
        )
    if not (recipes / "recipes").is_dir():
        raise ValueError(f"Incomplete Conan recipes checkout: {recipes}")
    if existing is None:
        run(
            [
                *conan,
                "remote",
                "add",
                "--type",
                "local-recipes-index",
                name,
                str(recipes),
            ],
            env,
            cwd=sourceRoot,
        )
    return installRemotes(name)


def prepareConan(sourceRoot, buildType, env):
    if env.get("BOLT_CONAN_GENERATORS"):
        return Path(env["BOLT_CONAN_GENERATORS"]).resolve()
    # Use the backend interpreter's Conan, including pip/uv's isolated overlays,
    # rather than an unrelated executable on the user's PATH.
    conan = [sys.executable, "-m", "conans.conan"]
    outputDir = sourceRoot / "_build/python-conan"
    run([*conan, "profile", "detect", "--exist-ok"], env, cwd=sourceRoot)
    conanHome = Path(
        run(
            [*conan, "config", "home"], env, cwd=sourceRoot, capture=True
        ).stdout.strip()
    )
    remotes = recipeRemotes(conan, env, sourceRoot, conanHome / "bolt-python")
    run(
        [
            *conan,
            "install",
            str(sourceRoot),
            "--version=main",
            "--output-folder",
            str(outputDir),
            "--profile:host",
            env.get("BOLT_CONAN_PROFILE", "default"),
            "--profile:build",
            "default",
            "--settings",
            f"build_type={env.get('BOLT_DEPENDENCY_BUILD_TYPE', 'Release')}",
            "--settings",
            f"&:build_type={buildType}",
            "--options",
            "&:python_bind=True",
            "--options",
            "&:enable_testutil=True",
            "--options",
            "&:enable_jit=False",
            "--options",
            "&:spark_compatible=False",
            "--options",
            "&:shared=False",
            "--conf",
            "tools.cmake.cmaketoolchain:generator=Ninja",
            "--conf",
            f"tools.build:jobs={env['CMAKE_BUILD_PARALLEL_LEVEL']}",
            *[argument for remote in remotes for argument in ("--remote", remote)],
            "--build=missing",
        ],
        env,
        cwd=sourceRoot,
    )
    return outputDir / "_build" / buildType / "generators"


def conanCmakeArgs(generators, buildType):
    """Apply Conan's dependency configuration to the frontend's build directory."""
    generators = Path(generators).resolve(strict=True)
    presets = json.loads((generators / "CMakePresets.json").read_text())
    toolchain = (generators / "conan_toolchain.cmake").resolve(strict=True)

    def value(entry):
        return entry["value"] if isinstance(entry, dict) else entry

    matches = [
        preset.get("cacheVariables", {})
        for preset in presets.get("configurePresets", [])
        if value(preset.get("cacheVariables", {}).get("CMAKE_BUILD_TYPE")) == buildType
    ]
    if len(matches) != 1:
        raise ValueError(
            f"Expected one Conan configure preset for {buildType}; found {len(matches)}"
        )
    variables = matches[0]
    for name in ("BOLT_BUILD_PYTHON_PACKAGE", "BOLT_BUILD_TEST_UTILS"):
        enabled = value(variables.get(name))
        normalized = str(enabled).upper()
        if (
            not enabled
            or normalized in ("0", "OFF", "NO", "FALSE", "N", "IGNORE", "NOTFOUND")
            or normalized.endswith("-NOTFOUND")
        ):
            raise ValueError(f"The Conan preset must enable {name}")
    if value(variables.get("BOLT_TEST_LINKAGE")) != "static":
        raise ValueError(
            "Python wheels require BOLT_TEST_LINKAGE=static in the Conan preset"
        )

    # Older builds cached the now-removed Conan.cmake hook.
    args = ["-UCMAKE_PROJECT_INCLUDE_BEFORE"]
    for name, entry in variables.items():
        if name in ("CMAKE_BUILD_TYPE", "CMAKE_INSTALL_PREFIX"):
            continue
        item = value(entry)
        if item is None:
            args.append(f"-U{name}")
            continue
        cacheType = "BOOL" if isinstance(item, bool) else "STRING"
        if isinstance(entry, dict):
            cacheType = entry.get("type", cacheType)
        item = ("ON" if item else "OFF") if isinstance(item, bool) else str(item)
        args.append(f"-D{name}:{cacheType}={item}")
    return [
        *args,
        f"--toolchain={toolchain}",
        f"-DPython3_EXECUTABLE:FILEPATH={sys.executable}",
    ]


@contextmanager
def buildEnvironment(sourceRoot, buildType):
    """Activate native build tools only for the duration of a PEP 517 hook."""
    env = os.environ.copy()
    if env.get("BOLT_TEST_LINKAGE", "static") != "static":
        raise ValueError("Python wheels require BOLT_TEST_LINKAGE=static")
    env["BOLT_TEST_LINKAGE"] = "static"
    configureParallelism(env)
    print(
        "Bolt build parallelism: "
        f"CMAKE_BUILD_PARALLEL_LEVEL={env['CMAKE_BUILD_PARALLEL_LEVEL']}, "
        f"NUM_LINK_JOB={env['NUM_LINK_JOB']}",
        file=sys.stderr,
    )
    generators = prepareConan(sourceRoot, buildType, env)
    activation = generators / "conanbuild.sh"
    if not activation.is_file():
        raise ValueError(f"Missing Conan build environment: {activation}")
    env["BOLT_CONAN_GENERATORS"] = str(generators)
    # Capture the activated environment in memory. Positional arguments keep
    # paths literal; redirect script chatter so it cannot corrupt the result.
    result = run(
        [
            "bash",
            "-c",
            'set -e; source "$1" >&2; shift; exec "$@"',
            "bolt-build",
            str(activation),
            sys.executable,
            "-c",
            "import json, os; print(json.dumps(dict(os.environ)))",
        ],
        env,
        cwd=generators,
        capture=True,
    )
    activated = json.loads(result.stdout)
    activated["CMAKE_ARGS"] = shlex.join(
        [
            *shlex.split(activated.get("CMAKE_ARGS", "")),
            *conanCmakeArgs(generators, buildType),
        ]
    )
    # scikit-build-core otherwise prefers a Python cmake wheel over Conan's
    # pinned CMake, even when conanbuild.sh puts the latter first on PATH.
    cmake = shutil.which("cmake", path=activated.get("PATH"))
    if cmake:
        activated.setdefault("CMAKE_EXECUTABLE", cmake)
    previous = os.environ.copy()
    try:
        os.environ.clear()
        os.environ.update(activated)
        yield
    finally:
        os.environ.clear()
        os.environ.update(previous)


def _build(wheel_directory, config_settings, metadata_directory, *, editable):
    # Use the same settings and sdist overrides as scikit-build-core, including
    # frontend config-settings and SKBUILD_* environment variables.
    reader = SettingsReader.from_file(
        "pyproject.toml", config_settings, state="editable" if editable else "wheel"
    )
    reader.validate_may_exit()
    settings = reader.settings
    buildTypeOverride = "CMAKE_BUILD_TYPE" in settings.cmake.define
    arguments = iter(settings.cmake.args)
    for argument in arguments:
        if argument == "-D":
            argument += next(arguments, "")
        if argument.startswith("-D"):
            name = argument[2:].split("=", 1)[0].split(":", 1)[0].strip()
            buildTypeOverride |= name == "CMAKE_BUILD_TYPE"
    if buildTypeOverride:
        raise ValueError(
            "Set cmake.build-type instead of defining CMAKE_BUILD_TYPE directly"
        )
    buildTypes = normalize_build_types(settings.cmake.build_type)
    if len(buildTypes) != 1:
        raise ValueError("Bolt's Conan build requires a single cmake.build-type")
    if editable and (settings.editable.rebuild or settings.editable.rebuild_dir):
        raise ValueError(
            "editable.rebuild and editable.rebuild-dir are unsupported with isolated Conan builds; "
            "reinstall the editable package to rebuild C++ changes"
        )
    with buildEnvironment(settings.cmake.source_dir.resolve(), buildTypes[0]):
        hook = _backend.build_editable if editable else _backend.build_wheel
        return hook(wheel_directory, config_settings, metadata_directory)


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    return _build(wheel_directory, config_settings, metadata_directory, editable=False)


def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    return _build(wheel_directory, config_settings, metadata_directory, editable=True)


# Dependency resolution, metadata and source archives do not need Conan/CMake.
# Preserve scikit-build-core's optional hook detection as well.
def __getattr__(name):
    return getattr(_backend, name)
