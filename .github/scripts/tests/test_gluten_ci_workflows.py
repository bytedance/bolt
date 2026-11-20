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
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS = ROOT / ".github/workflows"
MIRROR = "gluten-ci.yaml"


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
    def assert_candidate_caller(self, job, mode):
        self.assertEqual(job["uses"], f"./.github/workflows/{MIRROR}")
        self.assertNotIn("secrets", job)
        self.assertEqual(
            job["with"],
            {
                "mode": mode,
                "bolt_sha": "${{ github.sha }}",
                "gluten_sha": "main",
            },
        )

    def test_nightly_matrix_preserves_all_38_downstream_tasks(self):
        cells = workflow(MIRROR)["jobs"]["spark"]["strategy"]["matrix"]["include"]
        self.assertEqual(len(cells), 38)
        self.assertEqual(len({cell["id"] for cell in cells}), 38)
        self.assertEqual(
            {cell["spark"] for cell in cells}, {"3.5", "4.0", "4.1", "4.2"}
        )
        self.assertEqual(sum(c["stage"] == "package" for c in cells), 12)
        for version in ("3.5", "4.0", "4.1", "4.2"):
            stages = {c["stage"] for c in cells if c["spark"] == version}
            self.assertTrue(
                {"spark-1", "spark-2", "spark-3", "extended", "slow-hive"} <= stages
            )

    def test_gluten_runs_and_must_pass_even_when_bolt_results_are_reused(self):
        build = workflow("build-test.yml")
        self.assertEqual(set(build["on"]), {"pull_request", "merge_group"})
        jobs = build["jobs"]
        compile_job = jobs["gluten-backend-compile"]
        self.assert_candidate_caller(compile_job, "native")
        self.assertNotIn("needs", compile_job)
        self.assertNotIn("if", compile_job)
        source = jobs["source-result"]
        self.assertIn("gluten-backend-compile", source["needs"])
        self.assertIn("gluten-ci-check", source["needs"])

        context = {
            "needs.reuse-check.result": "success",
            "needs.changes.result": "success",
            "needs.gluten-ci-check.result": "success",
            "needs.build-test.result": "skipped",
            "needs.build-only.result": "skipped",
            "needs.changes.outputs.source_code": "false",
        }
        for reused, result, publish in (
            ("false", "success", True),
            ("false", "skipped", False),
            ("false", "failure", False),
            ("false", "cancelled", False),
            ("true", "success", True),
            ("true", "skipped", False),
            ("true", "failure", False),
            ("true", "cancelled", False),
        ):
            with self.subTest(reused=reused, result=result):
                status = context | {
                    "needs.reuse-check.outputs.hit": reused,
                    "needs.gluten-backend-compile.result": result,
                }
                self.assertEqual(condition(source["if"], status), publish)
                self.assertFalse(
                    condition(
                        source["if"], status | {"needs.reuse-check.result": "failure"}
                    )
                )
                for check_result in ("failure", "cancelled", "skipped"):
                    self.assertFalse(
                        condition(
                            source["if"],
                            status | {"needs.gluten-ci-check.result": check_result},
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
        self.assert_candidate_caller(job, "full")
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

    def test_partial_nightly_retry_downloads_successful_native_artifact(self):
        jobs = workflow(MIRROR)["jobs"]
        upload = next(
            s for s in jobs["native"]["steps"] if s.get("if") == "inputs.mode == 'full'"
        )
        download = next(
            s["with"]
            for s in jobs["spark"]["steps"]
            if s.get("uses", "").startswith("actions/download-artifact@")
        )

        def render(value, context):
            return re.sub(r"\$\{\{\s*(.*?)\s*\}\}", lambda m: context[m[1]], value)

        # Native completed in attempt 1; only the failed downstream job reruns.
        native_context = {
            "github.run_attempt": "1",
            f"steps.{upload.get('id')}.outputs.artifact-id": "101",
        }
        retry_context = {"github.run_attempt": "2"}
        retry_context.update(
            {
                f"needs.native.outputs.{key}": render(value, native_context)
                for key, value in jobs["native"].get("outputs", {}).items()
            }
        )
        requested = render(
            download.get("artifact-ids", download.get("name", "")), retry_context
        )
        self.assertEqual(requested, "101")
        self.assertEqual(download["merge-multiple"], "true")
        self.assertNotIn("run-id", download)
        self.assertNotIn("repository", download)

    def test_workflow_uses_candidate_checkout_without_cache_deployment_details(self):
        definition = workflow(MIRROR)
        self.assertEqual(set(definition["on"]), {"workflow_call", "workflow_dispatch"})
        self.assertEqual(definition["jobs"]["spark"]["if"], "inputs.mode == 'full'")
        for name in ("native", "spark"):
            checkouts = [
                s["with"]
                for s in definition["jobs"][name]["steps"]
                if s.get("uses", "").startswith("actions/checkout@")
            ]
            self.assertEqual(len(checkouts), 2)
            self.assertEqual(checkouts[0]["ref"], "${{ env.BOLT_SHA }}")
            self.assertEqual(checkouts[1]["repository"], "apache/gluten")
            self.assertEqual(checkouts[1]["ref"], "${{ env.GLUTEN_SHA }}")
        text = (WORKFLOWS / MIRROR).read_text()
        for forbidden in (
            "actions/cache/",
            "bolt-arrow",
            "configure-local",
            "PIP_CACHE_DIR",
            "GLUTEN_CACHE_ROOT",
            "GLUTEN_MAVEN_REPO",
        ):
            self.assertNotIn(forbidden, text)
        self.assertIn("CCACHE_SECONDARY_STORAGE", text)
