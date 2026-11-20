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

"""Build a uniquely named development wheel through Bolt's build backend.

Run through `make python-wheel` with Python and pip available. Pip provisions
the build tools in isolation using the project's pyproject.toml.
All build output goes to stderr; stdout contains only the resulting wheel path.
This does not install Bolt into the caller's project or publish the wheel.
"""

import argparse
import os
from pathlib import Path
import subprocess
import sys
import sysconfig
import time


PYTHON_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PYTHON_ROOT.parents[1]
DEFAULT_BUILD_DIR = f"build/dev/{{cache_tag}}-{{build_type}}-{sysconfig.get_platform()}"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=REPO_ROOT / "dist")
    parser.add_argument("--build-dir", default=DEFAULT_BUILD_DIR)
    parser.add_argument("--profile", default="default", help="Conan host profile")
    parser.add_argument(
        "--dependency-build-type",
        choices=("Release", "RelWithDebInfo", "Debug"),
        default="Release",
    )
    parser.add_argument(
        "--build-type",
        choices=("Release", "RelWithDebInfo", "Debug"),
        default="Release",
    )
    args = parser.parse_args(argv)
    outputDir = args.out_dir.resolve()
    buildTag = str(time.time_ns())
    env = os.environ.copy()
    env["BOLT_CONAN_PROFILE"] = args.profile
    env["BOLT_DEPENDENCY_BUILD_TYPE"] = args.dependency_build_type
    try:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "wheel",
                str(PYTHON_ROOT),
                "--no-deps",
                "--verbose",
                "--wheel-dir",
                str(outputDir),
                f"-Cwheel.build-tag={buildTag}",
                f"-Cbuild-dir={args.build_dir}",
                f"-Ccmake.build-type={args.build_type}",
            ],
            cwd=REPO_ROOT,
            env=env,
            stdout=sys.stderr,
            check=True,
        )
    except subprocess.CalledProcessError as error:
        return error.returncode
    wheels = list(outputDir.glob(f"bolt_engine-*-{buildTag}-*.whl"))
    if len(wheels) != 1:
        parser.exit(
            1, f"Expected one wheel with build tag {buildTag}, found {len(wheels)}.\n"
        )
    print(wheels[0])
    return 0


if __name__ == "__main__":
    sys.exit(main())
