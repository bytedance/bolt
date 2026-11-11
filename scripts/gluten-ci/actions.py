# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Prepare the GitHub Actions matrix and stage checked task evidence."""

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import sys

from run import HERE, NATIVE_ARCHIVE, NATIVE_MANIFEST, load_matrix, open_regular, sha256


def prepare():
    mode = os.environ["CI_MODE"]
    if mode not in ("compile", "full"):
        raise ValueError("Expected compile or full mode")
    for name in ("BOLT", "GLUTEN", "HELPER", "DRIVER"):
        if not re.fullmatch(
            r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", os.environ[f"{name}_REPOSITORY"]
        ):
            raise ValueError(f"Invalid {name} repository")
        if not re.fullmatch(r"[0-9a-f]{40}", os.environ[f"{name}_SHA"]):
            raise ValueError(f"Expected a full lowercase Git SHA for {name}")
    labels = json.loads(os.environ["CI_RUNNER_LABELS"])
    if (
        not isinstance(labels, list)
        or "self-hosted" not in labels
        or any(
            not isinstance(label, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", label)
            for label in labels
        )
    ):
        raise ValueError("Expected a JSON array of runner labels including self-hosted")
    image = os.environ["CI_RUNNER_IMAGE"]
    if not re.fullmatch(r"[A-Za-z0-9_.:/@-]+", image):
        raise ValueError("Invalid worker image")
    tasks = load_matrix()
    native = tasks["native"]
    downstream = [
        {"task": task["id"], "jdk": task["jdk"]}
        for task in tasks.values()
        if task["kind"] != "native"
    ]
    if native["kind"] != "native" or not downstream:
        raise ValueError("Incomplete Gluten CI task matrix")
    config = [
        mode,
        image,
        os.environ["HELPER_SHA"],
        os.environ["DRIVER_SHA"],
        sha256(HERE / "matrix.json"),
    ]
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        print(
            f"matrix={json.dumps({'include': downstream if mode == 'full' else []})}",
            file=output,
        )
        print(f"native_jdk={native['jdk']}", file=output)
        print(f"runner_labels={json.dumps(labels)}", file=output)
        print(
            f"config_sha256={hashlib.sha256(json.dumps(config).encode()).hexdigest()}",
            file=output,
        )


def copy_regular(root, target, relative, *, maximum=None):
    path = PurePosixPath(relative)
    if str(path) != relative or not all(
        part not in (".", "..") and re.fullmatch(r"[A-Za-z0-9_.$+-]+", part)
        for part in path.parts
    ):
        raise ValueError("Invalid evidence path")
    with open_regular(root, root / path, maximum) as stream:
        destination = target / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("xb") as output:
            shutil.copyfileobj(stream, output)


def stage_task(work, task, status):
    reports = work / "reports"
    output = work / "upload/reports"
    result = reports / "result.json"
    if status and not result.exists():
        return
    with open_regular(reports, result, 8 * 1024 * 1024) as stream:
        metadata = json.load(stream)
    if (
        not isinstance(metadata, dict)
        or type(metadata.get("schema")) is not int
        or metadata["schema"] != 1
        or metadata.get("task") != task
        or type(metadata.get("exit_code")) is not int
        or metadata["exit_code"] != status
        or metadata.get("status") != ("failure" if status else "success")
        or not isinstance(metadata.get("evidence"), list)
    ):
        raise ValueError("Invalid task result")
    copy_regular(reports, output, "result.json")
    for evidence in metadata["evidence"]:
        copy_regular(reports, output, evidence)
    if task == "native" and not status:
        native = work / "native-output"
        target = work / "upload/native"
        copy_regular(native, target, NATIVE_MANIFEST, maximum=16 * 1024)
        copy_regular(native, target, NATIVE_ARCHIVE)


if __name__ == "__main__":
    try:
        if sys.argv[1:] == ["prepare"]:
            prepare()
        else:
            command, workspace, task, status = sys.argv[1:]
            if command != "stage":
                raise ValueError("Expected prepare or stage")
            stage_task(Path(workspace), task, int(status))
    except (KeyError, OSError, TypeError, ValueError) as error:
        sys.exit(str(error))
