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

import hashlib
import json
import os
import runpy
import subprocess
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from bolt_ci.commenter import Commenter, MARKER
from bolt_ci.config import Config, load_matrix
from bolt_ci.docker import Docker, Container, Workspaces, checkout
from bolt_ci.github import ApiError
from bolt_ci.listener import Listener
from bolt_ci.runner import Runner, collect_result, seal_native
from bolt_ci.state import State
from test_policy_listener import A, B, C, Api, pull


class WorkerDocker:
    def __init__(self):
        self.containers = {}
        self.launches = []
        self.lose_create_response = False
        self.fail_start = False
        self.isolated = True
        self.stopped = []

    def verify_network(self):
        return self.isolated

    def names(self, instance):
        return set(self.containers)

    def inspect(self, name):
        return self.containers.get(name)

    def create(self, task, workspaces, labels):
        name = task["container"]
        self.launches.append(task)
        self.containers[name] = Container(
            name, labels, "created", 0, False, hashlib.sha256(name.encode()).hexdigest()
        )
        if self.lose_create_response:
            self.lose_create_response = False
            from bolt_ci.docker import DockerError

            raise DockerError()

    def start(self, name):
        if self.fail_start:
            from bolt_ci.docker import DockerError

            raise DockerError()
        old = self.containers[name]
        self.containers[name] = replace(old, status="running")

    def stop(self, name):
        self.stopped.append(name)
        old = self.containers[name]
        self.containers[name] = replace(old, status="exited", exit_code=143)

    def logs(self, name, destination):
        destination.write_text("Private 192.0.2.8 /work/secret\n")

    def remove(self, name):
        self.containers.pop(name, None)


class Staging(Workspaces):
    def stage(self, task):
        for directory in ("source", "bolt", "reports", "cache", "native-out"):
            self.job(task).joinpath(directory).mkdir(parents=True)


class Comments:
    def __init__(self):
        self.posts = 0
        self.messages = {}
        self.denied = False
        self.lose_post = False
        self.after_write = lambda: None

    def viewer(self):
        return 701

    def comments(self, number):
        return list(self.messages.values())

    def post_comment(self, number, body):
        if self.denied:
            raise ApiError(403, 10)
        self.posts += 1
        self.messages[90] = {"id": 90, "user": {"id": 701}, "body": body}
        self.after_write()
        if self.lose_post:
            self.lose_post = False
            raise ApiError()
        return 90

    def update_comment(self, comment_id, body):
        if self.denied:
            raise ApiError(429, 10)
        self.messages[comment_id]["body"] = body
        self.after_write()


class MatrixTests(unittest.TestCase):
    @unittest.skipUnless(
        os.getenv("BOLT_TEST_HELPERS", str(Path(__file__).resolve().parents[4])),
        "requires Bolt helpers",
    )
    def test_canonical_full_matrix_and_reject_partial_or_substituted_profiles(self):
        directory = Path(
            os.getenv("BOLT_TEST_HELPERS", str(Path(__file__).resolve().parents[4]))
        ).resolve()
        matrix = load_matrix(directory)
        self.assertEqual(len(matrix), 39)
        self.assertEqual(
            len([entry for entry in matrix if entry["kind"] == "package"]), 12
        )
        scala = next(
            entry for entry in matrix if entry["id"] == "spark:3.5-scala213:spark-1"
        )
        self.assertEqual(scala["spark"], "3.5")
        self.assertIn("scala-2.13", scala["profiles"])
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            path = root / "scripts/gluten-ci/matrix.json"
            path.parent.mkdir(parents=True)
            for mutated in (matrix[:-1], [*matrix[:-1], matrix[-1] | {"profiles": []}]):
                path.write_text(json.dumps(mutated))
                with self.assertRaises(ValueError):
                    load_matrix(root)


class ServicesTests(unittest.TestCase):
    def setUp(self):
        work = tempfile.TemporaryDirectory()
        self.addCleanup(work.cleanup)
        root = Path(work.name).resolve()
        helper = root / "helper"
        ci = helper / "scripts/gluten-ci"
        ci.mkdir(parents=True)
        supplied = os.getenv(
            "BOLT_TEST_HELPERS", str(Path(__file__).resolve().parents[4])
        )
        self.matrix_path = ci / "matrix.json"
        matrix = [
            {
                "id": "native",
                "kind": "native",
                "jdk": 17,
                "spark": None,
                "stage": "native",
                "profiles": [],
            },
        ]
        self.settings = dict(
            repository="apache/gluten",
            host_data_dir=str(root),
            state_dir=str(root / "state"),
            helper_dir=str(helper),
            bolt_checkout=str(root / "engine"),
            driver_dir=str(root / "driver"),
            jdk_dirs={str(jdk): str(root / f"jdk{jdk}") for jdk in (8, 11, 17, 21, 25)},
            image="localhost:5000/bolt-ci@sha256:" + "a" * 64,
            bolt_sha=A,
            driver_sha=B,
            helper_sha=C,
            network="gluten-worker",
            network_preflight_file=str(root / "network.json"),
            read_token_file=str(root / "reader"),
            bot_token_file=str(root / "writer"),
            max_parallel=4,
        )
        for asset in (
            "scripts/gluten-ci/run.py",
            "scripts/gluten-ci/run-task.sh",
            "scripts/gluten-ci/ci-bolt-build.sh",
            "scripts/gluten-ci/ci-bolt-spark-env.sh",
        ):
            target = helper / asset
            if supplied:
                target.write_bytes((Path(supplied) / asset).read_bytes())
            else:
                target.touch()
        if supplied:
            self.matrix_path.write_bytes(
                (Path(supplied) / "scripts/gluten-ci/matrix.json").read_bytes()
            )
        else:
            self.matrix_path.write_text(json.dumps(matrix))
            pin = hashlib.sha256(self.matrix_path.read_bytes()).hexdigest()
            patcher = mock.patch("bolt_ci.config.MATRIX_SHA256", pin)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.config = Config.from_mapping(self.settings)
        self.db = State(
            self.config.state_dir,
            self.config.repository,
            self.config.matrix,
            self.config.fingerprint,
        )
        self.api = Api()
        self.api.current[1] = pull()
        self.db.observe(self.api.current[1], 1000, related=True)
        self.docker = WorkerDocker()
        self.staging = Staging(self.config)
        self.now = 1000
        self.comment_api = Comments()

    def runner(self):
        return Runner(
            self.config,
            self.db,
            self.api,
            self.docker,
            self.staging,
            clock=lambda: self.now,
        )

    def commenter(self):
        return Commenter(self.db, self.api, self.comment_api, clock=lambda: self.now)

    def complete(self, task, *, fail=False, invalid=False):
        job = self.staging.job(task)
        reports = job / "reports"
        identity = self.config.identity(task["head_sha"])
        (reports / "sources.json").write_text(json.dumps(identity))
        (reports / "native-build.log").write_text("compiler output\n")
        evidence = ["sources.json", "native-build.log"]
        if not fail:
            relative = "cpp/build/package/bolt/linux/amd64/libbolt_backend.so"
            library = job / "source" / relative
            library.parent.mkdir(parents=True)
            library.write_bytes(b"backend")
            package = dict(
                ref=f"bolt/ci-{A}#recipe", package_id="package", prev="revision"
            )
            graph = {
                "graph": {"nodes": {"1": package | {"name": "bolt", "context": "host"}}}
            }
            archive = job / "native-out/native.tar.gz"
            archive.write_bytes(b"native payload")
            archive_hash = hashlib.sha256(archive.read_bytes()).hexdigest()
            (archive.parent / "native-manifest.json").write_text(
                json.dumps(
                    {
                        **identity,
                        "schema": 1,
                        "mode": "compile",
                        "archive_sha256": archive_hash,
                    }
                )
            )
            for name, payload in (
                ("bolt-export.json", graph),
                ("gluten-install.json", graph),
                ("bolt-package.json", package),
                (
                    "compile-verification.json",
                    {
                        "schema": 1,
                        "bolt_sha": A,
                        "bolt_package": package,
                        "backend": {
                            "path": relative,
                            "size": library.stat().st_size,
                            "sha256": hashlib.sha256(library.read_bytes()).hexdigest(),
                        },
                        "archive_sha256": archive_hash,
                    },
                ),
            ):
                (reports / name).write_text(json.dumps(payload))
                evidence.append(name)
            (reports / "runtime-linkage.txt").write_text(
                f"/work/source/{relative}:\n"
                "\tlibc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x01)\n"
            )
            evidence.append("runtime-linkage.txt")
        if invalid:
            evidence.append("../../../secrets")
        (reports / "result.json").write_text(
            json.dumps(
                {
                    "schema": 1,
                    "task": task["task"],
                    "mode": "compile",
                    "status": "failure" if fail else "success",
                    "exit_code": int(fail),
                    "tests": dict(total=0, passed=0, failed=0, skipped=0),
                    "evidence": evidence,
                }
            )
        )
        name = task["container"]
        self.docker.containers[name] = replace(
            self.docker.containers[name], status="exited", exit_code=int(fail)
        )

    def test_compile_success_has_one_cell_and_a_compilation_comment(self):
        self.runner().tick()
        self.complete(self.db.active()[0])
        self.runner().tick()
        self.assertEqual(self.db.latest(1)["state"], "success")
        self.assertEqual([task["task"] for task in self.docker.launches], ["native"])
        self.commenter().tick()
        body = self.comment_api.messages[90]["body"]
        self.assertIn("Backend compilation succeeded", body)
        self.assertNotIn("Tests:", body)
        self.assertNotIn("Spark", body)

    def test_compile_rejects_a_legacy_full_report(self):
        self.runner().tick()
        active = self.db.active()[0]
        self.complete(active)
        result = self.staging.job(active) / "reports/result.json"
        report = json.loads(result.read_text())
        del report["mode"]
        report["tests"] = dict(total=1, passed=1, failed=0, skipped=0)
        (result.parent / "TEST-native.xml").write_text(
            "<testsuite><testcase/></testsuite>"
        )
        report["evidence"].append("TEST-native.xml")
        result.write_text(json.dumps(report))
        self.runner().tick()
        (task,) = self.db.task_rows(self.db.latest(1)["id"])
        self.assertEqual(task["category"], "evidence")

    def test_compile_rejects_zero_tests_without_verification(self):
        self.runner().tick()
        task = self.db.active()[0]
        self.complete(task)
        (self.staging.job(task) / "reports/compile-verification.json").unlink()
        self.runner().tick()
        self.assertEqual(
            self.db.task_rows(task["request_id"])[0]["category"], "evidence"
        )

    def test_compile_rejects_independent_manifest_and_archive_changes(self):
        self.runner().tick()
        task = self.db.active()[0]
        self.complete(task)
        job = self.staging.job(task)
        source = job / "native-out"
        destination = self.staging.artifact(task)
        manifest = source / "native-manifest.json"
        archive = source / "native.tar.gz"
        _, _, _, archive_hash = collect_result(self.config, task, job, 0)
        value = json.loads(manifest.read_text())
        for label, changed in (
            ("full mode", {key: val for key, val in value.items() if key != "mode"}),
            ("different source", value | {"source_sha": B}),
            ("different archive identity", value | {"archive_sha256": "1" * 64}),
        ):
            with self.subTest(label=label):
                manifest.write_text(json.dumps(changed))
                with self.assertRaises(ValueError):
                    seal_native(self.config, task, source, destination, archive_hash)
        manifest.write_text(json.dumps(value))
        archive.write_bytes(b"replaced archive")
        self.runner().tick()
        self.assertEqual(
            self.db.task_rows(task["request_id"])[0]["category"], "evidence"
        )

    def test_compile_rejects_empty_or_mismatched_backend_and_unresolved_linkage(self):
        self.runner().tick()
        task = self.db.active()[0]
        self.complete(task)
        job = self.staging.job(task)
        library = job / "source/cpp/build/package/bolt/linux/amd64/libbolt_backend.so"
        linkage = job / "reports/runtime-linkage.txt"
        for file, replacement in (
            (library, b""),
            (library, b"changed"),
            (linkage, b"libc.so.6 => not found\n"),
            (linkage, b"irrelevant.so:\n\tlibc.so.6 => /lib/libc.so.6 (0x01)\n"),
        ):
            with self.subTest(path=file.name, replacement=replacement):
                original = file.read_bytes()
                file.write_bytes(replacement)
                with self.assertRaises(ValueError):
                    collect_result(self.config, task, job, 0)
                file.write_bytes(original)

    @unittest.skipUnless(
        os.getenv("BOLT_TEST_HELPERS", str(Path(__file__).resolve().parents[4])),
        "requires Bolt helpers",
    )
    def test_compile_accepts_archive_and_verification_produced_by_gluten_helper(self):
        self.runner().tick()
        task = self.db.active()[0]
        self.complete(task)
        job = self.staging.job(task)
        source = job / "source"
        helper = runpy.run_path(
            str(
                Path(
                    os.getenv(
                        "BOLT_TEST_HELPERS", str(Path(__file__).resolve().parents[4])
                    )
                )
                / "scripts/gluten-ci/run.py"
            )
        )
        for relative in helper["NATIVE_LIBRARIES"]:
            path = source / relative
            if not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"library")
        runtime = job / "runtime"
        runtime.mkdir()
        verification = helper["verify_compile"](source, job / "reports", A)
        archive_hash = helper["create_native_archive"](
            job / "native-out",
            source,
            runtime,
            self.config.identity(task["head_sha"]),
            compile_only=True,
        )
        verification["archive_sha256"] = archive_hash
        (job / "reports/compile-verification.json").write_text(json.dumps(verification))
        self.runner().tick()
        self.assertEqual(self.db.latest(1)["state"], "success")

    def test_config_fingerprint_binds_mode_and_selected_graph(self):
        with mock.patch("bolt_ci.config.COMPILE_MODE", "full"):
            full = Config.from_mapping(self.settings)
        native = self.config.matrix[0] | {"jdk": 21}
        with mock.patch("bolt_ci.config.load_matrix", return_value=[native]):
            different_graph = Config.from_mapping(self.settings)
        self.assertNotEqual(self.config.fingerprint, full.fingerprint)
        self.assertNotEqual(self.config.fingerprint, different_graph.fingerprint)

    def test_compile_rejects_changed_consumed_package(self):
        self.runner().tick()
        task = self.db.active()[0]
        self.complete(task)
        graph = self.staging.job(task) / "reports/gluten-install.json"
        data = json.loads(graph.read_text())
        data["graph"]["nodes"]["1"]["package_id"] = "different-package"
        graph.write_text(json.dumps(data))
        self.runner().tick()
        self.assertEqual(
            self.db.task_rows(task["request_id"])[0]["category"], "evidence"
        )

    def test_upgrade_retires_old_active_publications_and_preserves_history(self):
        self.db.set_meta("baseline", 900)
        self.api.changed[1] = [{"filename": "cpp/bolt/file.cpp"}]
        self.runner().tick()
        old_task = self.db.active()[0]
        with self.db.transaction() as db:
            db.execute(
                "INSERT INTO tasks(request_id,ordinal,task,kind,jdk,state) VALUES(?,?,?,?,?,?)",
                (
                    old_task["request_id"],
                    1,
                    "package:spark35",
                    "package",
                    17,
                    "blocked",
                ),
            )
        previous = replace(self.config, fingerprint="e" * 64)
        self.config = previous
        self.db = State(
            previous.state_dir,
            previous.repository,
            previous.matrix,
            previous.fingerprint,
        )
        self.assertEqual(self.db.latest(1)["state"], "superseded")
        self.assertEqual(self.db.pending(self.now), [])
        self.runner().tick()
        self.assertEqual(self.docker.stopped, [old_task["container"]])
        self.assertEqual(
            [row["state"] for row in self.db.task_rows(old_task["request_id"])],
            ["not_run", "not_run"],
        )
        listener = Listener(self.db, self.api, clock=lambda: self.now)
        listener.tick()
        replacement = self.db.latest(1)
        self.assertEqual(replacement["attempt"], 2)
        self.assertEqual(replacement["bundle"], previous.fingerprint)
        self.assertEqual(
            [row["task"] for row in self.db.task_rows(replacement["id"])], ["native"]
        )
        listener.tick()
        self.assertEqual(self.db.latest(1), replacement)

    def test_restart_reconciles_ambiguous_create_and_preserves_safe_comment(self):
        self.docker.lose_create_response = True
        self.commenter().tick()
        self.assertIn(
            "Backend compilation is waiting", self.comment_api.messages[90]["body"]
        )
        self.runner().tick()
        self.commenter().tick()
        self.assertIn(
            "Backend compilation is running", self.comment_api.messages[90]["body"]
        )
        self.complete(self.db.active()[0], fail=True)
        self.runner().tick()
        self.assertEqual([task["task"] for task in self.docker.launches], ["native"])
        self.assertEqual(self.db.latest(1)["state"], "failure")
        self.commenter().tick()
        body = self.comment_api.messages[90]["body"]
        self.assertIn("Backend compilation failed (build)", body)
        self.assertNotIn("192.0.2.8", body)
        self.assertNotIn("/work/secret", body)
        self.assertEqual(self.comment_api.posts, 1)

    def test_base_tip_movement_preserves_running_compilation(self):
        self.db.set_meta("baseline", self.now)
        self.api.changed[1] = [{"filename": "cpp/bolt/file.cpp"}]
        listener = Listener(self.db, self.api, clock=lambda: self.now)
        original = self.api.current[1]
        self.runner().tick()
        native = self.db.active()[0]

        self.api.current[1] = replace(original, base_sha=B)
        listener.tick()
        self.runner().tick()
        self.assertEqual(self.docker.stopped, [])
        self.assertEqual(len(self.docker.launches), 1)

        self.complete(native)
        # Advance again without letting the listener record the new tip.
        self.api.current[1] = replace(original, base_sha=A)
        self.runner().tick()
        self.assertEqual(self.db.active(), [])
        request = self.db.latest(1)
        self.assertEqual(request["id"], native["request_id"])
        self.assertEqual(request["attempt"], 1)
        self.assertEqual(request["state"], "success")

        self.api.current[1] = replace(
            original, base_sha=B, updated=original.updated + 2
        )
        listener.tick()
        self.assertEqual(self.db.latest(1), request)
        self.commenter().tick()
        self.assertIn(
            "Backend compilation succeeded", self.comment_api.messages[90]["body"]
        )

    def test_completed_native_is_accepted_after_base_tip_movement_without_listener(
        self,
    ):
        self.runner().tick()
        native = self.db.active()[0]
        self.complete(native)
        self.api.current[1] = replace(self.api.current[1], base_sha=B)
        self.runner().tick()
        self.assertEqual(self.db.task_rows(native["request_id"])[0]["state"], "success")
        self.assertEqual(self.db.active(), [])

    def test_runner_rejects_completed_native_after_same_sha_retarget(self):
        self.runner().tick()
        native = self.db.active()[0]
        self.complete(native)
        self.api.current[1] = replace(self.api.current[1], base_ref="branch/3.5")
        self.runner().tick()
        self.assertEqual(self.db.latest(1)["state"], "superseded")
        self.assertEqual(self.db.task_rows(native["request_id"])[0]["state"], "not_run")
        self.assertEqual(len(self.docker.launches), 1)

    def test_runner_rejects_completed_native_from_replaced_head_repository(self):
        self.runner().tick()
        native = self.db.active()[0]
        self.complete(native)
        self.api.current[1] = replace(
            self.api.current[1], head_repo="other/contribution"
        )
        self.runner().tick()
        self.assertEqual(self.db.latest(1)["state"], "superseded")
        self.assertEqual(self.db.task_rows(native["request_id"])[0]["state"], "not_run")
        self.assertEqual(len(self.docker.launches), 1)

    def test_legacy_unbound_targets_are_replaced_without_losing_history(self):
        existing = self.api.current[1]
        finished = pull(number=2)
        queued = pull(number=3)
        baseline = pull(number=4)
        self.db.baseline((baseline,), 900)
        self.runner().tick()
        running_task = self.db.active()[0]
        self.db.observe(finished, self.now, related=True)
        completed_task = self.db.claim(self.now, 1800, 4)
        self.db.finish(completed_task, "failure", "test", None, self.now)
        self.db.observe(queued, self.now, related=True)
        self.db.ignore_comment(42)
        self.db.record_comment(2, 90)
        self.api.current = {
            pr.number: pr for pr in (existing, finished, queued, baseline)
        }
        self.api.changed = {
            pr: [{"filename": "cpp/bolt/file.cpp"}] for pr in self.api.current
        }
        with self.db.transaction() as db:
            # Reproduce the previous schema while retaining real in-flight rows.
            for table in ("pulls", "requests"):
                columns = {
                    row["name"] for row in db.execute(f"PRAGMA table_info({table})")
                }
                if "base_ref" in columns:
                    db.execute(f"ALTER TABLE {table} DROP COLUMN base_ref")

        def reopen_db():
            return State(
                self.config.state_dir,
                self.config.repository,
                self.config.matrix,
                self.config.fingerprint,
            )

        instance = self.db.instance
        self.db = reopen_db()
        self.assertEqual(self.db.instance, instance)
        self.assertEqual(self.db.latest(1)["state"], "superseded")
        self.assertEqual(self.db.latest(2)["state"], "failure")
        self.assertEqual(self.db.latest(3)["state"], "superseded")
        self.assertIsNone(self.db.latest(2)["base_ref"])
        self.assertEqual(self.db.status()["pending_comments"], 0)
        self.assertEqual(self.db.comment_id(2), 90)
        self.assertTrue(self.db.event_seen("comment", 42))
        self.runner().tick()
        self.assertEqual(self.docker.stopped, [running_task["container"]])
        self.assertEqual(len(self.docker.launches), 1)

        Listener(self.db, self.api, clock=lambda: self.now).tick()
        self.assertEqual(self.api.file_calls, 3)
        self.assertIsNone(self.db.latest(4))
        self.assertEqual(self.db.snapshots()[4]["base_ref"], "main")
        refreshed = [self.db.latest(pr) for pr in (1, 2, 3)]
        for request in refreshed:
            self.assertEqual(request["base_ref"], "main")
            self.assertEqual(request["state"], "queued")
            self.assertEqual(request["attempt"], 2)
        self.assertEqual(self.db.status()["pending_comments"], 3)
        self.db = reopen_db()
        self.assertEqual([self.db.latest(pr) for pr in (1, 2, 3)], refreshed)
        self.assertEqual(self.db.status()["pending_comments"], 3)
        self.assertEqual(
            self.db.task_rows(running_task["request_id"])[0]["state"], "not_run"
        )
        self.assertEqual(
            self.db.task_rows(completed_task["request_id"])[0]["state"], "failure"
        )

    def test_unsafe_evidence_path_fails_compilation(self):
        self.runner().tick()
        self.complete(self.db.active()[0], invalid=True)
        self.runner().tick()
        self.assertEqual(len(self.docker.launches), 1)
        (task,) = self.db.task_rows(self.db.latest(1)["id"])
        self.assertEqual((task["state"], task["category"]), ("failure", "evidence"))

    def test_failed_start_cannot_be_queue_timeout(self):
        self.docker.fail_start = True
        self.runner().tick()
        self.now += 1800
        self.runner().tick()
        self.assertEqual(self.db.latest(1)["state"], "failure")

    def test_missing_claimed_container_after_restart_is_interrupted(self):
        self.db.claim(self.now, 1800, 4)
        self.now += 1800
        self.runner().tick()
        self.assertEqual(self.db.latest(1)["state"], "failure")
        self.assertEqual(self.docker.launches, [])

    def test_network_receipt_loss_stops_running_and_refuses_new_work(self):
        self.runner().tick()
        self.docker.isolated = False
        self.runner().tick()
        self.assertEqual(len(self.docker.stopped), 1)
        self.assertEqual(self.db.latest(1)["state"], "failure")

    def test_ambiguous_post_and_api_denial_recover_without_duplicate_or_rebuild(self):
        self.comment_api.denied = True
        self.commenter().tick()
        self.assertEqual(self.db.status()["pending_comments"], 1)
        self.comment_api.denied = False
        self.comment_api.lose_post = True
        self.now += 60
        self.commenter().tick()
        self.now += 60
        self.commenter().tick()
        self.assertEqual(self.comment_api.posts, 1)
        self.assertEqual(self.db.status()["pending_comments"], 0)
        self.assertEqual(self.docker.launches, [])

    def test_completed_result_published_when_base_tip_changes_before_write(self):
        self.commenter().tick()
        self.runner().tick()
        self.complete(self.db.active()[0], fail=True)
        self.runner().tick()
        self.api.current[1] = replace(self.api.current[1], base_sha=B)
        self.commenter().tick()
        self.assertIn("Failed", self.comment_api.messages[90]["body"])
        self.assertEqual(self.db.status()["pending_comments"], 0)

    def test_comment_stays_current_when_base_tip_changes_during_write(self):
        def advance_base():
            self.api.current[1] = replace(self.api.current[1], base_sha=B)

        self.comment_api.after_write = advance_base
        self.commenter().tick()
        self.assertIn("Queued", self.comment_api.messages[90]["body"])

    def test_comment_neutralized_when_head_changes_during_write(self):
        def advance_head():
            self.api.current[1] = pull(sha=B)

        self.comment_api.after_write = advance_head
        self.commenter().tick()
        self.assertIn(
            "Awaiting results for current commit", self.comment_api.messages[90]["body"]
        )
        self.assertIn(B, self.comment_api.messages[90]["body"])

    def test_completed_result_neutralized_when_target_changes_during_write(self):
        self.commenter().tick()
        self.runner().tick()
        self.complete(self.db.active()[0], fail=True)
        self.runner().tick()
        self.assertEqual(self.db.latest(1)["state"], "failure")

        def retarget():
            self.api.current[1] = replace(self.api.current[1], base_ref="branch/3.5")

        self.comment_api.after_write = retarget
        self.commenter().tick()
        self.assertIn(
            "Awaiting results for current commit", self.comment_api.messages[90]["body"]
        )

    def test_only_own_marker_is_updated(self):
        self.comment_api.messages[12] = {"id": 12, "user": {"id": 3}, "body": MARKER}
        self.commenter().tick()
        self.assertEqual(self.comment_api.messages[12]["body"], MARKER)
        self.assertEqual(self.comment_api.posts, 1)

    def test_report_root_ancestor_symlink_is_rejected(self):
        self.runner().tick()
        task = self.db.active()[0]
        self.complete(task)
        reports = self.staging.job(task) / "reports"
        moved = reports.with_name("moved")
        reports.rename(moved)
        reports.symlink_to(moved)
        with self.assertRaises((OSError, ValueError)):
            collect_result(self.config, task, reports.parent, 0)
        self.runner().tick()
        self.assertEqual(self.db.latest(1)["state"], "failure")

    def test_docker_argv_uses_fixed_mounts_and_no_inherited_credentials(self):
        task = self.db.claim(self.now, 1800, 4)
        docker = Docker(self.config)
        args = docker.create_args(task, self.staging, {"io.test.label": "test"})
        joined = " ".join(args)
        for required in (
            "--compile-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--sysctl=net.ipv6.conf.all.disable_ipv6=1",
            "--network=gluten-worker",
            "--user=1000:1000",
            "--cpus=16",
            "--memory=50g",
            "target=/work/driver,readonly",
            "/opt/venv/bin",
            "GLUTEN_CI_DRIVER_SHA=" + B,
        ):
            self.assertIn(required, joined)
        for forbidden in (
            "--privileged",
            "docker.sock",
            str(self.config.state_dir),
            "TOKEN",
            str(self.config.read_token_file),
            "--env-file",
            "192.0.2.8",
        ):
            self.assertNotIn(forbidden, joined)

    def test_git_checkout_objects_are_independent(self):
        root = self.config.state_dir
        original = root / "origin"
        commands = [
            ["init", str(original)],
            ["-C", str(original), "config", "user.email", "example@example.test"],
            ["-C", str(original), "config", "user.name", "CI Test"],
        ]
        for args in commands:
            subprocess.run(["/usr/bin/git", *args], check=True, capture_output=True)
        (original / "file").write_text("engine")
        subprocess.run(["/usr/bin/git", "-C", str(original), "add", "file"], check=True)
        subprocess.run(
            ["/usr/bin/git", "-C", str(original), "commit", "-m", "test"],
            check=True,
            capture_output=True,
        )
        sha = subprocess.check_output(
            ["/usr/bin/git", "-C", str(original), "rev-parse", "HEAD"], text=True
        ).strip()
        target = root / "copy"
        from unittest.mock import patch

        with patch.dict(
            os.environ,
            {
                "GIT_DIR": str(root / "unrelated"),
                "GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": "core.hooksPath",
                "GIT_CONFIG_VALUE_0": str(root),
                "GH_TOKEN": "fictional-credential",
            },
        ):
            checkout(target, original.as_uri(), sha, local=True)
        self.assertEqual((target / "file").read_text(), "engine")
        self.assertFalse((target / ".git/objects/info/alternates").exists())
        source_inodes = {
            file.stat().st_ino
            for file in (original / ".git/objects").rglob("*")
            if file.is_file()
        }
        copied_inodes = {
            file.stat().st_ino
            for file in (target / ".git/objects").rglob("*")
            if file.is_file()
        }
        self.assertTrue(source_inodes and copied_inodes)
        self.assertFalse(source_inodes & copied_inodes)

    def test_maven_cache_promotes_only_independently_verified_downloads(self):
        from io import BytesIO

        from bolt_ci.cache import MavenCache

        root = self.config.state_dir
        seed, private = root / "seed", root / "private"
        seed.mkdir()
        good = "org/example/dependency/1.2/dependency-1.2.jar"
        bad = "org/example/corrupt/2.0/corrupt-2.0.pom"
        snapshot = "org/example/new/1.0-SNAPSHOT/new-1.0-SNAPSHOT.jar"
        internal = "org/apache/gluten/bundle/1.0/bundle-1.0.jar"
        linked = "org/example/linked/1.0/linked-1.0.jar"
        for relative in (good, bad, snapshot, internal, linked):
            (private / relative).parent.mkdir(parents=True)
            if relative == linked:
                (private / relative).symlink_to(private / good)
            else:
                (private / relative).write_bytes(b"download")
        requests = []

        class Checksums:
            def open(self, request, *, timeout):
                requests.append(request.full_url)
                match = good in request.full_url
                return BytesIO(
                    hashlib.sha256(b"download" if match else b"other")
                    .hexdigest()
                    .encode()
                )

        MavenCache(seed, opener=Checksums())(private)
        self.assertEqual((seed / good).read_bytes(), b"download")
        for relative in (bad, snapshot, internal, linked):
            self.assertFalse((seed / relative).exists())
        self.assertEqual(len(requests), 2)
        self.assertTrue(
            all(
                url.startswith("https://repo.maven.apache.org/maven2/")
                for url in requests
            )
        )

    def test_config_upgrade_fails_active_attempt_without_mixing_inputs(self):
        self.runner().tick()
        config = replace(self.config, fingerprint="e" * 64)
        Runner(
            config, self.db, self.api, self.docker, self.staging, clock=lambda: self.now
        ).tick()
        self.assertEqual(len(self.docker.launches), 1)
        self.assertEqual(len(self.docker.stopped), 1)
        self.assertEqual(self.db.latest(1)["state"], "failure")

    def test_terminal_workspace_cleanup_preserves_reports_and_sealed_native_artifact(
        self,
    ):
        self.runner().tick()
        task = self.db.active()[0]
        self.complete(task)
        self.runner().tick()
        root = self.staging.job(task)
        self.assertFalse((root / "source").exists())
        self.assertFalse((root / "cache").exists())
        self.assertTrue((root / "reports/result.json").is_file())
        self.assertTrue((root / "worker.log").is_file())
        self.assertTrue((self.staging.artifact(task) / "native.tar.gz").is_file())

    def test_claim_and_queue_expiry_are_mutually_exclusive(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier

        barrier = Barrier(2)

        def claim():
            barrier.wait()
            return self.db.claim(2799, 1800, 4)

        def expire():
            barrier.wait()
            self.db.expire(2800, 1800)

        with ThreadPoolExecutor(2) as executor:
            claimant = executor.submit(claim)
            expiry = executor.submit(expire)
            claimed = claimant.result()
            expiry.result()
        state = self.db.latest(1)["state"]
        if claimed:
            self.assertEqual(state, "queued")
            self.assertEqual(len(self.db.active()), 1)
        else:
            self.assertEqual(state, "queue_timeout")
            self.assertEqual(self.db.active(), [])

    def test_runtime_stop_decision_survives_scheduler_restart(self):
        self.runner().tick()
        task = self.db.active()[0]
        self.db.mark_stopping(task, "timeout")
        # Model termination after a durable stop decision. The process
        # happened to exit zero and wrote complete evidence before exiting.
        self.complete(task)
        self.runner().tick()
        self.assertEqual(self.db.latest(1)["state"], "failure")
        self.assertEqual(
            self.db.task_rows(task["request_id"])[0]["category"], "timeout"
        )
        self.assertEqual(len(self.docker.launches), 1)

    def test_container_replacement_with_same_labels_is_not_accepted(self):
        self.runner().tick()
        task = self.db.active()[0]
        self.complete(task)
        name = task["container"]
        self.docker.containers[name] = replace(
            self.docker.containers[name], id="9" * 64
        )
        self.runner().tick()
        self.assertEqual(self.db.latest(1)["state"], "failure")
        self.assertEqual(len(self.docker.launches), 1)

    def test_network_receipt_binds_boot_and_live_bridge_id(self):
        boot = self.config.state_dir / "boot-id"
        boot.write_text("boot-fixture")
        identity = "b" * 64
        receipt = dict(
            schema=1,
            network=self.config.network,
            network_id=identity,
            image=self.config.image,
            boot_id="boot-fixture",
            verified_at=self.now,
            metadata_blocked=True,
            control_blocked=True,
        )
        self.config.network_preflight_file.write_text(json.dumps(receipt))

        def command(args, **kwargs):
            self.assertEqual(
                args, ["/usr/bin/docker", "network", "inspect", "gluten-worker"]
            )
            self.assertEqual(
                kwargs["env"]["DOCKER_HOST"], "unix:///var/run/docker.sock"
            )
            self.assertNotIn("GH_TOKEN", kwargs["env"])
            return subprocess.CompletedProcess(
                args,
                0,
                json.dumps([{"Id": identity, "Driver": "bridge"}]).encode(),
                b"",
            )

        docker = Docker(
            self.config, run=command, clock=lambda: self.now, boot_id_path=boot
        )
        self.assertTrue(docker.verify_network())
        self.now += self.config.network_preflight_max_age_seconds + 1
        self.assertFalse(docker.verify_network())
        self.now = receipt["verified_at"]
        boot.write_text("another-boot")
        self.assertFalse(docker.verify_network())
        boot.write_text("boot-fixture")
        receipt["network_id"] = "c" * 64
        self.config.network_preflight_file.write_text(json.dumps(receipt))
        self.assertFalse(docker.verify_network())
