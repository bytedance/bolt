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

import os
import subprocess
import sys
from typing import List, Mapping


def _truthy(value: str) -> bool:
    return value.lower() not in ("", "0", "false", "no")


def build_command(env: Mapping[str, str]) -> List[str]:
    command = ["gitleaks", "git", "--redact", "--verbose"]
    in_ci = _truthy(env.get("GITHUB_ACTIONS", "")) or _truthy(env.get("IN_CI", ""))
    if not in_ci:
        return command + ["--pre-commit", "--staged"]

    base_sha = env.get("CI_BASE_SHA", "").strip()
    if not base_sha:
        raise ValueError("CI_BASE_SHA is required when running gitleaks in CI")
    return command + [f"--log-opts={base_sha}..HEAD"]


def main() -> int:
    try:
        command = build_command(os.environ)
    except ValueError as error:
        print(f"Error: {error}", file=sys.stderr)
        return 2
    return subprocess.run(command).returncode


if __name__ == "__main__":
    raise SystemExit(main())
