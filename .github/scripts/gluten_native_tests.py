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

"""Transfer discovered native tests without moving compiler/Conan build trees."""

import json
from pathlib import Path
import sys
import tarfile


def quote(value):
    delimiter = "="
    while f"]{delimiter}]" in value:
        delimiter += "="
    return f"[{delimiter}[{value}]{delimiter}]"


def pack(root, manifest_path, archive):
    manifest = json.loads(manifest_path.read_text())
    tests = manifest.get("tests", [])
    if not tests:
        raise ValueError("No native tests were discovered")
    root = root.resolve()
    paths = set()
    for test in tests:
        executable = Path(test["command"][0]).resolve()
        paths.add(executable.relative_to(root))
    # Native fixtures use the staged build-directory data as well as source data
    # already supplied by the exact Gluten checkout in the test job.
    for data in (root / "cpp/build/Release").rglob("data"):
        if data.is_dir():
            paths.add(data.relative_to(root))
    with tarfile.open(archive, "w:gz", compresslevel=1) as bundle:
        for path in sorted(paths):
            bundle.add(root / path, arcname=str(path))
    manifest["source_root"] = str(root)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")


def prepare(root, manifest_path, directory):
    manifest = json.loads(manifest_path.read_text())
    if not manifest.get("tests"):
        raise ValueError("No native tests were discovered")
    old = Path(manifest["source_root"])
    root = root.resolve()

    def relocate(value):
        if value == str(old) or value.startswith(str(old) + "/"):
            return str(root) + value[len(str(old)) :]
        return value

    lines = []
    for test in manifest["tests"]:
        name = quote(test["name"])
        command = " ".join(quote(relocate(arg)) for arg in test["command"])
        lines.append(f"add_test({name} {command})")
        for prop in test.get("properties", []):
            if prop["name"] == "DEF_SOURCE_LINE":
                continue
            value = prop["value"]
            if isinstance(value, list):
                value = ";".join(map(str, value))
            value = relocate(str(value))
            if prop["name"] == "WORKING_DIRECTORY":
                Path(value).mkdir(parents=True, exist_ok=True)
            lines.append(
                f"set_tests_properties({name} PROPERTIES {prop['name']} {quote(value)})"
            )
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "CTestTestfile.cmake").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    mode, root, manifest, destination = sys.argv[1:]
    operation = {"pack": pack, "prepare": prepare}[mode]
    operation(Path(root), Path(manifest), Path(destination))
