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

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import yaml

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
WORKFLOW = ROOT / ".github/workflows/bolt_backend_x86.yml"
COMPOSITE = ROOT / ".github/actions/bolt-ci-task/action.yml"


class WorkflowTest(unittest.TestCase):
    def test_workflow_only_runs_manually_or_when_called(self):
        self.assertTrue(WORKFLOW.is_file())
        workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(set(workflow["on"]), {"workflow_call", "workflow_dispatch"})
        self.assertEqual(
            set(workflow["on"]["workflow_call"]["inputs"]),
            set(workflow["on"]["workflow_dispatch"]["inputs"]),
        )
        self.assertEqual(workflow["permissions"], {"contents": "read"})
        self.assertEqual(
            set(workflow["jobs"]["downstream"]["needs"]), {"prepare", "native"}
        )
        self.assertIn("inputs.mode == 'full'", workflow["jobs"]["downstream"]["if"])
        self.assertEqual(
            workflow["jobs"]["downstream"]["strategy"]["fail-fast"], "false"
        )

    def test_gluten_ref_is_resolved_once_for_every_worker(self):
        workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
        jobs = workflow["jobs"]
        prepare = jobs["prepare"]
        checkout = next(s for s in prepare["steps"] if s.get("id") == "gluten-source")
        self.assertEqual(
            checkout["with"]["repository"], "${{ inputs.gluten_repository }}"
        )
        self.assertEqual(checkout["with"]["ref"], "${{ inputs.gluten_sha }}")
        self.assertEqual(checkout["with"]["persist-credentials"], "false")
        self.assertEqual(
            prepare["outputs"]["gluten_sha"], "${{ steps.matrix.outputs.gluten_sha }}"
        )
        script = prepare["steps"][-1]["run"]
        self.assertIn('GLUTEN_SHA="$(git -C gluten-source rev-parse HEAD)"', script)
        self.assertIn('echo "gluten_sha=$GLUTEN_SHA" >> "$GITHUB_OUTPUT"', script)
        for name in ("native", "downstream"):
            self.assertEqual(
                jobs[name]["env"]["GLUTEN_SHA"],
                "${{ needs.prepare.outputs.gluten_sha }}",
            )

    def test_conan_read_locks_have_service_only_write_access(self):
        workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
        native = workflow["jobs"]["native"]
        service = native["services"]["conanserver"]
        self.assertEqual(
            service["volumes"], ["/data/conan-server-data:/var/conan/data:rw"]
        )
        self.assertFalse(service.get("env"))
        self.assertTrue(all(v.endswith(":ro") for v in native["container"]["volumes"]))
        self.assertNotIn("CONAN_ADMIN", WORKFLOW.read_text())

    def test_action_checks_out_every_input_without_persisting_credentials(self):
        self.assertTrue(COMPOSITE.is_file())
        action = yaml.load(COMPOSITE.read_text(), Loader=yaml.BaseLoader)
        steps = action["runs"]["steps"]
        checkout = [
            step["with"]
            for step in steps
            if step.get("uses", "").startswith("actions/checkout@")
        ]
        self.assertEqual(
            {(step["repository"], step["ref"]) for step in checkout},
            {
                (f"${{{{ env.{name}_REPOSITORY }}}}", f"${{{{ env.{name}_SHA }}}}")
                for name in ("BOLT", "GLUTEN", "DRIVER")
            },
        )
        self.assertTrue(
            all(step["persist-credentials"] == "false" for step in checkout)
        )


class ArtifactUploadTest(unittest.TestCase):
    def test_stage_rejects_linked_evidence(self):
        self.assertTrue((HERE / "actions.py").is_file())
        from actions import stage_task

        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            reports = work / "reports"
            reports.mkdir()
            outside = work / "outside.txt"
            outside.write_text("outside the report directory")
            (reports / "linked.txt").symlink_to(outside)
            (reports / "result.json").write_text(
                json.dumps(
                    {
                        "task": "package:spark35",
                        "schema": 1,
                        "status": "success",
                        "exit_code": 0,
                        "evidence": ["linked.txt"],
                    }
                )
            )
            with self.assertRaises(OSError):
                stage_task(work, "package:spark35", 0)
            self.assertFalse((work / "upload/reports/linked.txt").exists())

    def test_stage_requires_both_native_files_for_success(self):
        self.assertTrue((HERE / "actions.py").is_file())
        from actions import stage_task

        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            reports = work / "reports"
            reports.mkdir()
            (reports / "result.json").write_text(
                json.dumps(
                    {
                        "task": "native",
                        "schema": 1,
                        "status": "success",
                        "exit_code": 0,
                        "evidence": [],
                    }
                )
            )
            native = work / "native-output"
            native.mkdir()
            (native / "native.tar.gz").write_bytes(b"archive")
            with self.assertRaises(OSError):
                stage_task(work, "native", 0)


class PrepareTest(unittest.TestCase):
    def prepare(self, mode, **overrides):
        script = HERE / "actions.py"
        self.assertTrue(script.is_file())
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "output"
            env = {
                **os.environ,
                "GITHUB_OUTPUT": str(output),
                "CI_MODE": mode,
                "CI_RUNNER_LABELS": '["self-hosted","medium"]',
                "CI_RUNNER_IMAGE": "bolt-registry:5000/bolt-ci:20260921",
                **{
                    f"{name}_REPOSITORY": f"example/{name.lower()}"
                    for name in ("BOLT", "GLUTEN", "HELPER", "DRIVER")
                },
                **{
                    f"{name}_SHA": ch * 40
                    for name, ch in zip(("BOLT", "GLUTEN", "HELPER", "DRIVER"), "abcd")
                },
                **overrides,
            }
            process = subprocess.run(
                [sys.executable, str(script), "prepare"],
                env=env,
                capture_output=True,
                text=True,
            )
            values = {}
            if output.exists():
                values = dict(
                    line.split("=", 1) for line in output.read_text().splitlines()
                )
            return process, values

    def test_compile_has_no_downstream_work(self):
        process, values = self.prepare("compile")
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(json.loads(values["matrix"]), {"include": []})

    def test_native_has_no_spark_tasks_and_a_distinct_fingerprint(self):
        process, values = self.prepare("native")
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(json.loads(values["matrix"]), {"include": []})
        for mode in ("compile", "full"):
            self.assertNotEqual(
                values["config_sha256"], self.prepare(mode)[1]["config_sha256"]
            )

    def test_full_schedules_every_non_native_task_and_fingerprints_mode(self):
        process, values = self.prepare("full")
        self.assertEqual(process.returncode, 0, process.stderr)
        actual = json.loads(values["matrix"])["include"]
        tasks = json.loads((HERE / "matrix.json").read_text())
        self.assertEqual(
            [(cell["task"], cell["jdk"]) for cell in actual],
            [(cell["id"], cell["jdk"]) for cell in tasks if cell["kind"] != "native"],
        )
        self.assertEqual(len(actual), 38)
        self.assertNotEqual(
            values["config_sha256"], self.prepare("compile")[1]["config_sha256"]
        )
        self.assertNotEqual(
            values["config_sha256"],
            self.prepare("full", CI_RUNNER_IMAGE="registry.example/bolt/worker:other")[
                1
            ]["config_sha256"],
        )

    def test_invalid_mode_or_ref_produces_no_outputs(self):
        for mode, options in (
            ("bad", {}),
            ("full", {"BOLT_SHA": "main"}),
            ("full", {"CI_RUNNER_LABELS": '["ubuntu-latest"]'}),
            ("full", {"HELPER_REPOSITORY": "other\nconfig_sha256=bad"}),
        ):
            with self.subTest(mode=mode, options=options):
                process, values = self.prepare(mode, **options)
                self.assertNotEqual(process.returncode, 0)
                self.assertEqual(values, {})
