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

"""Check the compile gate and nightly workflow against their GitHub contracts."""

import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from merge_queue_ci import make_fingerprint  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS = ROOT / ".github/workflows"
GLUTEN = "main"
DRIVER = "72f74589df524b2f4a7695b4f10c5b13a95812e8"
MIRROR = "bolt_backend_x86.yml"


def workflow(name):
    return yaml.load((WORKFLOWS / name).read_text(), Loader=yaml.BaseLoader)


def condition(expression, context):
    """Evaluate the boolean and equality subset used by these job conditions."""
    source = expression.strip().removeprefix("${{").removesuffix("}}").strip()
    source = re.sub(r"(?:needs|github)\.[\w.-]+", lambda m: repr(context[m[0]]), source)
    source = (
        source.replace("always()", "True").replace("&&", " and ").replace("||", " or ")
    )
    source = re.sub(r"!(?!=)", " not ", source)
    tokens = re.compile(r"(?:True|False|and|or|not|==|!=|[()\s]|'[A-Za-z0-9_./-]*')+")
    if not tokens.fullmatch(source):
        raise AssertionError(f"Unsupported test expression: {source}")
    return eval(" ".join(source.split()), {"__builtins__": {}}, {})


class GlutenCIWorkflowsTest(unittest.TestCase):
    def assert_pinned_caller(self, job, mode):
        self.assertEqual(job["uses"], f"./.github/workflows/{MIRROR}")
        self.assertNotIn("secrets", job)
        inputs = job["with"]
        self.assertEqual(inputs["mode"], mode)
        self.assertEqual(inputs["bolt_repository"], "${{ github.repository }}")
        self.assertEqual(inputs["bolt_sha"], "${{ github.sha }}")
        self.assertEqual(inputs["gluten_repository"], "apache/gluten")
        self.assertEqual(inputs["helper_repository"], "${{ github.repository }}")
        self.assertEqual(inputs["gluten_sha"], GLUTEN)
        self.assertEqual(inputs["helper_sha"], "${{ github.sha }}")
        self.assertEqual(inputs["driver_repository"], "bytedance/bolt")
        self.assertEqual(inputs["driver_sha"], DRIVER)

    def test_pr_compile_and_source_artifact_require_compile_or_verified_reuse(self):
        build = workflow("build-test.yml")
        self.assertEqual(set(build["on"]), {"pull_request", "merge_group"})
        jobs = build["jobs"]
        compile_job = jobs["gluten-backend-compile"]
        self.assert_pinned_caller(compile_job, "compile")
        self.assertEqual(compile_job["needs"], "reuse-check")
        source = jobs["source-result"]
        self.assertIn("gluten-backend-compile", source["needs"])

        context = {
            "needs.reuse-check.result": "success",
            "needs.changes.result": "success",
            "needs.build-test.result": "skipped",
            "needs.build-only.result": "skipped",
            "needs.changes.outputs.source_code": "false",
        }
        for reused, result, launch, publish in (
            ("false", "success", True, True),
            ("false", "skipped", True, False),
            ("false", "failure", True, False),
            ("false", "cancelled", True, False),
            ("true", "skipped", False, True),
            ("true", "failure", False, False),
        ):
            with self.subTest(reused=reused, result=result):
                status = context | {
                    "needs.reuse-check.outputs.hit": reused,
                    "needs.gluten-backend-compile.result": result,
                }
                self.assertEqual(condition(compile_job["if"], status), launch)
                self.assertEqual(condition(source["if"], status), publish)
                self.assertFalse(
                    condition(
                        source["if"], status | {"needs.reuse-check.result": "failure"}
                    )
                )

    def test_compile_prerequisite_scripts_exist(self):
        steps = workflow("build-test.yml")["jobs"]["reuse-check"]["steps"]
        for step in steps:
            for path in re.findall(
                r"python3 (\.github/scripts/[\w.-]+\.py)", step.get("run", "")
            ):
                self.assertTrue((ROOT / path).is_file(), path)

    def test_nightly_requires_main_and_only_upstream_can_schedule(self):
        nightly = workflow("gluten-backend-nightly.yml")
        self.assertEqual(
            nightly["on"],
            {"schedule": [{"cron": "0 18 * * *"}], "workflow_dispatch": ""},
        )
        self.assertNotEqual(
            nightly["concurrency"]["group"],
            workflow("build-test.yml")["concurrency"]["group"],
        )
        (job,) = nightly["jobs"].values()
        self.assert_pinned_caller(job, "full")
        for event, repo, ref, expected in (
            ("schedule", "bytedance/bolt", "refs/heads/main", True),
            ("schedule", "contributor/bolt", "refs/heads/main", False),
            ("workflow_dispatch", "bytedance/bolt", "refs/heads/main", True),
            ("workflow_dispatch", "bytedance/bolt", "refs/heads/feature", False),
        ):
            with self.subTest(event=event, repo=repo, ref=ref):
                self.assertEqual(
                    condition(
                        job["if"],
                        {
                            "github.event_name": event,
                            "github.repository": repo,
                            "github.ref": ref,
                        },
                    ),
                    expected,
                )

    def test_current_gate_salt_reuses_only_the_same_candidate_tree_and_base(self):
        salt, lint, gate = (
            workflow(name)["env"]["CI_REUSE_SALT"]
            for name in (
                "build-test.yml",
                "pre-commit-checks.yml",
                "merge-queue-gate.yml",
            )
        )
        self.assertEqual(salt, "bolt-ci:20260921|conan-server:latest|build-test-v2")
        self.assertEqual((lint, gate), (salt, salt))
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary)
            subprocess.run(["git", "init", "-q", work], check=True)

            def key(caller, mirror, base="a" * 40):
                (work / "build-test.yml").write_bytes(caller)
                (work / MIRROR).write_bytes(mirror)
                subprocess.run(["git", "-C", work, "add", "."], check=True)
                tree = subprocess.check_output(
                    ["git", "-C", work, "write-tree"], text=True
                ).strip()
                return make_fingerprint(tree, base, salt)["artifact_name"]

            caller = (WORKFLOWS / "build-test.yml").read_bytes()
            mirror = (WORKFLOWS / MIRROR).read_bytes()
            candidate = key(caller, mirror)
            self.assertEqual(candidate, key(caller, mirror))
            self.assertNotEqual(
                candidate, key(caller.replace(GLUTEN.encode(), b"b" * 40), mirror)
            )
            self.assertNotEqual(candidate, key(caller, mirror + b"\n"))
            self.assertNotEqual(candidate, key(caller, mirror, base="b" * 40))

    def test_workflow_uses_only_bolt_owned_helpers(self):
        definition = workflow(MIRROR)
        self.assertEqual(set(definition["on"]), {"workflow_call", "workflow_dispatch"})
        self.assertEqual(
            definition["jobs"]["downstream"]["if"], "inputs.mode == 'full'"
        )
        for name in ("prepare", "native", "downstream"):
            steps = definition["jobs"][name]["steps"]
            checkout = next(
                step["with"]
                for step in steps
                if step.get("uses", "").startswith("actions/checkout@")
            )
            self.assertEqual(checkout["repository"], "${{ inputs.helper_repository }}")
            self.assertEqual(checkout["ref"], "${{ inputs.helper_sha }}")
        prepare = definition["jobs"]["prepare"]["steps"][-1]["run"]
        self.assertIn("scripts/gluten-ci/actions.py prepare", prepare)
        self.assertTrue((ROOT / "scripts/gluten-ci/run.py").is_file())
        self.assertTrue((ROOT / ".github/actions/bolt-ci-task/action.yml").is_file())
