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

import datetime as dt
import io
import json
import re
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import urllib.request
import zipfile
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import merge_queue_ci as queue  # noqa: E402
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
    def test_consolidated_branch_has_one_entry_point_and_its_own_driver(self):
        self.assertFalse((WORKFLOWS / "gluten-bolt.yml").exists())
        for name in ("gluten_ci.sh", "DiscoverGlutenSuites.java"):
            self.assertTrue((ROOT / ".github/scripts" / name).is_file(), name)

    def assert_pinned_caller(self, job, mode):
        self.assertEqual(job["uses"], f"./.github/workflows/{MIRROR}")
        if mode == "native":
            helper = "6e021a135b8632b92669139df4ebf97da40741ee"
            repository = "bytedance/bolt"
            gluten = "${{ needs.reuse-check.outputs.gluten_sha }}"
        else:
            helper, repository, gluten = (
                "${{ github.sha }}",
                "${{ github.repository }}",
                GLUTEN,
            )
        self.assertNotIn("secrets", job)
        inputs = job["with"]
        self.assertEqual(inputs["mode"], mode)
        self.assertEqual(inputs["bolt_repository"], "${{ github.repository }}")
        self.assertEqual(inputs["bolt_sha"], "${{ github.sha }}")
        self.assertEqual(inputs["gluten_repository"], "apache/gluten")
        self.assertEqual(inputs["helper_repository"], repository)
        self.assertEqual(inputs["gluten_sha"], gluten)
        self.assertEqual(inputs["helper_sha"], helper)
        self.assertEqual(inputs["driver_repository"], "bytedance/bolt")
        self.assertEqual(inputs["driver_sha"], DRIVER)

    def test_pr_compile_and_source_artifact_require_compile_or_verified_reuse(self):
        build = workflow("build-test.yml")
        self.assertEqual(set(build["on"]), {"pull_request", "merge_group"})
        jobs = build["jobs"]
        compile_job = jobs["gluten-backend-compile"]
        self.assert_pinned_caller(compile_job, "native")
        self.assertEqual(compile_job["needs"], "reuse-check")
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
                for check_result in ("failure", "cancelled", "skipped"):
                    self.assertFalse(
                        condition(
                            source["if"],
                            status | {"needs.gluten-ci-check.result": check_result},
                        )
                    )

    def test_gluten_is_resolved_before_reuse_and_recorded_after_testing(self):
        jobs = workflow("build-test.yml")["jobs"]
        reuse = jobs["reuse-check"]
        steps = reuse["steps"]
        resolver = next(i for i, step in enumerate(steps) if step.get("id") == "gluten")
        lookup = next(i for i, step in enumerate(steps) if step.get("id") == "reuse")
        self.assertLess(resolver, lookup)
        self.assertIn("apache/gluten", steps[resolver]["run"])
        self.assertEqual(
            reuse["outputs"]["gluten_sha"], "${{ steps.gluten.outputs.sha }}"
        )
        self.assertIn(
            '--gluten-sha "${{ steps.gluten.outputs.sha }}"', steps[lookup]["run"]
        )
        record = next(
            step["run"]
            for step in jobs["source-result"]["steps"]
            if step.get("id") == "source"
        )
        self.assertIn(
            '--gluten-sha "${{ needs.reuse-check.outputs.gluten_sha }}"', record
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


class GlutenReuseEvidenceTest(unittest.TestCase):
    def test_reuse_requires_the_resolved_gluten_revision(self):
        now = dt.datetime.now(dt.timezone.utc)
        name = make_fingerprint("a" * 40, "b" * 40, "test")["artifact_name"]
        artifact = {
            "id": 11,
            "name": name,
            "created_at": queue.github_time(now),
            "workflow_run": {"id": 12},
        }
        run = {
            "status": "completed",
            "conclusion": "success",
            "event": "workflow_run",
            "path": queue.GATE_WORKFLOW_PATH,
            "repository": {"full_name": "bytedance/bolt"},
        }
        for evidence, expected in (
            ({}, False),
            ({"gluten_sha": "c" * 40}, True),
            ({"gluten_sha": "d" * 40}, False),
        ):
            with self.subTest(evidence=evidence):
                api = mock.Mock()
                api.request.side_effect = [{"artifacts": [artifact]}, run]
                api.artifact_json.return_value = evidence
                result = queue.find_reusable_result(
                    api,
                    "bytedance/bolt",
                    name,
                    queue.GATE_WORKFLOW_PATH,
                    24,
                    now=now,
                    gluten_sha="c" * 40,
                )
                self.assertEqual(result["hit"], expected)

    def test_gate_carries_tested_gluten_sha_without_resolving_main_again(self):
        merge, head, base, tree, gluten = (c * 40 for c in "abcde")
        config = queue.config_hash("test")
        runs = {
            name: {"id": i, "head_sha": head}
            for i, name in enumerate(queue.REQUIRED_WORKFLOWS, 1)
        }
        api = mock.Mock()
        api.request.return_value = {
            "parents": [{"sha": base}, {"sha": head}],
            "tree": {"sha": tree},
        }
        with mock.patch.object(
            queue,
            "find_source_evidence",
            side_effect=[
                {"merge_sha": merge, "config_hash": config, "gluten_sha": gluten},
                {"merge_sha": merge, "config_hash": config},
            ],
        ):
            result = queue.reusable_pr_result(api, "bytedance/bolt", runs, "test")
        self.assertEqual(result["gluten_sha"], gluten)
        api.request.assert_called_once_with(
            f"/repos/bytedance/bolt/git/commits/{merge}"
        )

    def test_build_source_evidence_loads_validated_gluten_revision(self):
        merge, gluten = "a" * 40, "b" * 40
        config = queue.config_hash("test")
        run = {"id": 10, "run_attempt": 2, "path": ".github/workflows/build-test.yml"}
        api = mock.Mock()
        api.request.return_value = {
            "artifacts": [
                {
                    "id": 11,
                    "name": queue.source_artifact_name(
                        "build-test.yml", 10, 2, merge, config
                    ),
                }
            ]
        }
        evidence = {
            "workflow": "build-test.yml",
            "source_run_id": "10",
            "source_run_attempt": 2,
            "merge_sha": merge,
            "config_hash": config,
            "gluten_sha": gluten,
        }
        api.artifact_json.return_value = evidence
        result = queue.find_source_evidence(api, "bytedance/bolt", run, {config})
        self.assertEqual(result["gluten_sha"], gluten)
        for field, value in (
            ("gluten_sha", "main"),
            ("merge_sha", "c" * 40),
            ("source_run_id", "99"),
            ("source_run_attempt", 1),
        ):
            api.artifact_json.return_value = evidence | {field: value}
            with self.subTest(field=field), self.assertRaises(ValueError):
                queue.find_source_evidence(api, "bytedance/bolt", run, {config})

    def test_record_source_persists_gluten_sha_and_legacy_calls_still_work(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.json"
            command = [
                sys.executable,
                str(ROOT / ".github/scripts/merge_queue_ci.py"),
                "record-source",
                "--workflow",
                "build-test.yml",
                "--event",
                "pull_request",
                "--run-id",
                "10",
                "--run-attempt",
                "2",
                "--merge-sha",
                "a" * 40,
                "--tree-sha",
                "b" * 40,
                "--base-sha",
                "c" * 40,
                "--config-hash",
                queue.config_hash("test"),
                "--output",
                str(output),
            ]
            subprocess.run(
                command + ["--gluten-sha", "d" * 40], check=True, capture_output=True
            )
            self.assertEqual(json.loads(output.read_text())["gluten_sha"], "d" * 40)
            subprocess.run(command, check=True, capture_output=True)
            self.assertNotIn("gluten_sha", json.loads(output.read_text()))

    def test_artifact_reader_bounds_and_parses_only_result_json(self):
        api = queue.GitHubApi("fixture")
        for name, body, success in (
            ("result.json", '{"gluten_sha":"abc"}', True),
            ("elsewhere/result.json", "{}", False),
            ("result.json", "[]", False),
            ("result.json", "x" * 65537, False),
        ):
            data = io.BytesIO()
            with zipfile.ZipFile(data, "w", zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(name, body)
            with (
                self.subTest(name=name, success=success),
                mock.patch.object(queue.urllib.request, "build_opener") as opener,
            ):
                opener.return_value.open.return_value.__enter__.return_value.read.return_value = data.getvalue()
                if success:
                    self.assertEqual(
                        api.artifact_json("bytedance/bolt", 11), json.loads(body)
                    )
                else:
                    with self.assertRaises(ValueError):
                        api.artifact_json("bytedance/bolt", 11)

    def test_artifact_redirect_drops_token_and_rejects_plain_http(self):
        request = urllib.request.Request(
            "https://api.github.com/repos/bytedance/bolt/actions/artifacts/11/zip",
            headers={"Authorization": "Bearer fixture"},
        )
        handler = queue.ArtifactRedirectHandler()
        redirected = handler.redirect_request(
            request, None, 302, "Found", {}, "https://blob.example/result.zip"
        )
        self.assertFalse(redirected.has_header("Authorization"))
        with self.assertRaises(ValueError):
            handler.redirect_request(
                request, None, 302, "Found", {}, "http://blob.example/result.zip"
            )
