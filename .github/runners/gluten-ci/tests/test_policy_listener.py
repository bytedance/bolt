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

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from bolt_ci.commenter import summary
from bolt_ci.listener import Listener
from bolt_ci.policy import Pull, affected, command_sha
from bolt_ci.state import State

A = "a" * 40
B = "b" * 40
C = "c" * 40


def pull(sha=A, *, number=1, updated=1100, changed=1, open=True, base_ref="main"):
    return Pull(
        number,
        sha,
        "owner/contribution",
        C,
        "apache/gluten",
        base_ref,
        "contributor",
        changed,
        updated,
        open,
    )


class Api:
    def __init__(self):
        self.current = {}
        self.changed = {}
        self.history = {}
        self.feed = []
        self.race = None
        self.file_calls = 0
        self.event_calls = 0

    def open_pulls(self):
        return [pr for pr in self.current.values() if pr.open]

    def pull(self, number):
        return self.current[number]

    def files(self, number):
        self.file_calls += 1
        if self.race:
            snapshot, self.race = self.race, None
            self.current[number], self.changed[number] = snapshot
            return [{"filename": "docs/old.md"}]
        return self.changed[number]

    def events(self, number):
        self.event_calls += 1
        return self.history.get(number, [])

    def recent_comments(self, since):
        return [entry for entry in self.feed if entry["updated"] >= since]


class PolicyTests(unittest.TestCase):
    def test_whole_diff(self):
        unrelated = [
            {"filename": path}
            for path in (
                "docs/config.md",
                "README.md",
                "cpp/velox/Example.cpp",
                "backends-clickhouse/Test.scala",
                "gluten-flink/pom.xml",
                "ep/build-velox/build.sh",
            )
        ]
        self.assertFalse(affected(unrelated, len(unrelated)))
        self.assertTrue(affected(unrelated, len(unrelated) + 1))
        for path in (
            "gluten-core/pom.xml",
            "cpp/core/Example.cpp",
            "shims/pom.xml",
            "cpp/conanfile.py",
            "pom.xml",
            "new-module/cases.json",
        ):
            with self.subTest(path=path):
                files = unrelated + [{"filename": path, "status": "removed"}]
                self.assertTrue(affected(files, len(files)))
        self.assertTrue(
            affected(
                [
                    {
                        "filename": "docs/a.md",
                        "status": "renamed",
                        "previous_filename": "cpp/bolt/a.cpp",
                    }
                ],
                1,
            )
        )
        self.assertTrue(affected([{"filename": "docs/a.md", "status": "renamed"}], 1))
        self.assertTrue(affected(unrelated * 500, 3000))

    def test_empty_api_payload_is_conservatively_related(self):
        self.assertTrue(affected([], 0))

    def test_exact_commands(self):
        for body in (" Run Gluten Bolt CI ", "Run Gluten Bolt CI on x86", "/bolt ci"):
            self.assertEqual(command_sha(body), "")
        self.assertEqual(command_sha(f"/bolt ci {A}"), A)
        for body in (
            "> Run Gluten Bolt CI",
            "Run Gluten Clickhouse CI",
            "please run Gluten Bolt CI",
            "Run Gluten Bolt CI\nthanks",
            "/bolt ci aaaa",
        ):
            self.assertIsNone(command_sha(body))


class ListenerTests(unittest.TestCase):
    def setUp(self):
        work = tempfile.TemporaryDirectory()
        self.addCleanup(work.cleanup)
        self.db = State(Path(work.name), "apache/gluten")
        self.api = Api()
        self.time = 1000
        self.listener = Listener(
            self.db, self.api, ("maintainer",), clock=lambda: self.time
        )
        self.api.current[1] = pull(updated=950)
        self.api.changed[1] = [{"filename": "docs/change.md"}]
        self.listener.tick()
        self.time = 1200

    def comment(
        self, comment_id, body, login, *, created=1100, kind="User", user_id=None
    ):
        self.api.feed.append(
            dict(
                id=comment_id,
                pr=1,
                body=body,
                login=login,
                type=kind,
                user_id=user_id,
                created=created,
                updated=created,
            )
        )

    def test_baseline_and_authorized_manual_override_with_dedup(self):
        self.assertIsNone(self.db.latest(1))
        self.assertEqual(self.api.file_calls, 0)
        self.assertEqual(self.api.event_calls, 0)
        self.comment(1, "Run Gluten Bolt CI", "contributor", created=900)
        self.comment(2, "Run Gluten Bolt CI", "random-user")
        self.comment(3, "Run Gluten Bolt CI", "maintainer", kind="Bot")
        self.comment(4, f"/bolt ci {B}", "maintainer")
        self.listener.tick()
        self.assertIsNone(self.db.latest(1))
        self.api.feed[1]["body"] = "/bolt ci"
        self.api.feed[1]["login"] = "maintainer"
        self.listener.tick()
        self.assertIsNone(self.db.latest(1))
        self.comment(5, "/bolt ci", "contributor")
        self.listener.tick()
        self.assertEqual(self.db.latest(1)["reason"], "manual_override")
        self.comment(6, "/bolt ci", "maintainer")
        self.listener.tick()
        self.assertEqual(self.db.latest(1)["attempt"], 1)
        self.db.expire(1500, 1)
        self.listener.tick()
        self.assertEqual(self.db.latest(1)["attempt"], 1)
        self.comment(7, "Run Gluten Bolt CI", "maintainer")
        self.listener.tick()
        self.assertEqual(self.db.latest(1)["attempt"], 2)

    def test_author_can_rerun_from_status_publisher_account(self):
        self.db.set_meta("commenter_user", 701)
        status = summary(
            {"head_sha": A, "id": 1, "attempt": 1, "publication": "queued"}, []
        )
        self.comment(10, status, "contributor", user_id=701)
        self.listener.tick()
        self.assertIsNone(self.db.latest(1))

        self.comment(11, "Run Gluten Bolt CI", "contributor", user_id=701)
        self.listener.tick()
        request = self.db.latest(1)
        self.assertIsNotNone(request)
        self.assertEqual(request["reason"], "manual_override")

    def test_file_read_retries_after_head_race_then_supersedes(self):
        self.api.current[1] = pull(updated=1100, sha=B)
        self.api.race = (
            pull(sha=C, updated=1101, changed=2),
            [
                {"filename": "docs/last-commit.md"},
                {"filename": "cpp/bolt/file.cpp"},
            ],
        )
        self.listener.tick()
        self.assertEqual(self.db.latest(1)["head_sha"], C)
        self.assertEqual(self.api.file_calls, 2)
        self.api.current[1] = pull(updated=1110)
        self.api.changed[1] = [{"filename": "docs/only.md"}]
        self.listener.tick()
        self.assertEqual(self.db.latest(1)["state"], "not_applicable")

    def test_diff_read_retries_after_base_tip_race(self):
        original = pull(sha=B)
        self.api.current[1] = original
        self.api.race = (
            replace(original, base_sha=A),
            [{"filename": "cpp/bolt/file.cpp"}],
        )
        self.listener.tick()
        self.assertEqual(self.api.file_calls, 2)
        self.assertEqual(self.db.latest(1)["base_sha"], A)
        self.assertEqual(self.db.latest(1)["state"], "queued")

    def test_base_tip_movement_keeps_the_original_queue_window(self):
        original = pull(sha=B)
        self.api.current[1] = original
        self.api.changed[1] = [{"filename": "cpp/bolt/file.cpp"}]
        self.listener.tick()
        first = self.db.latest(1)
        self.time += 1200
        self.api.current[1] = replace(original, base_sha=A)
        self.api.changed[1] = [{"filename": "docs/only.md"}]
        self.listener.tick()
        self.assertEqual(self.db.latest(1), first)
        self.assertEqual(self.db.snapshots()[1]["base_sha"], A)
        self.assertEqual(self.api.file_calls, 1)
        self.db.expire(first["eligible"] + 1800, 1800)
        self.assertEqual(self.db.latest(1)["state"], "queue_timeout")

    def test_same_sha_retarget_retries_diff_and_supersedes_active_attempt(self):
        original = pull(sha=B)
        self.api.current[1] = original
        self.api.changed[1] = [{"filename": "cpp/bolt/file.cpp"}]
        self.listener.tick()
        first = self.db.claim(self.time, 1800, 4)
        self.api.current[1] = replace(original, base_ref="branch/3.5")
        self.api.race = (
            replace(original, base_ref="branch/3.4"),
            [{"filename": "cpp/bolt/file.cpp"}],
        )
        self.listener.tick()
        self.assertEqual(self.api.file_calls, 3)
        self.assertEqual(self.db.active()[0]["request_id"], first["request_id"])
        self.assertEqual(self.db.active()[0]["request_state"], "superseded")
        latest = self.db.latest(1)
        self.assertEqual(latest["head_sha"], B)
        self.assertEqual(latest["base_sha"], C)
        self.assertEqual(latest["base_ref"], "branch/3.4")
        self.assertEqual(latest["attempt"], 2)
        self.assertEqual(latest["state"], "queued")

    def test_legacy_baseline_learns_targets_without_backfilling_discussion(self):
        for number in range(2, 6):
            self.db.observe(pull(number=number, updated=950), self.time)
        with self.db.transaction() as db:
            db.execute("ALTER TABLE pulls DROP COLUMN base_ref")
            db.execute("ALTER TABLE requests DROP COLUMN base_ref")
        self.db = State(self.db.directory, "apache/gluten")
        self.listener = Listener(self.db, self.api, clock=lambda: self.time)
        self.assertTrue(
            all(row["base_ref"] is None for row in self.db.snapshots().values())
        )

        self.api.current = {
            number: pull(number=number, updated=1120) for number in range(1, 6)
        }
        self.api.current[3] = replace(self.api.current[3], base_sha=B)
        self.api.current[5] = replace(self.api.current[5], head_sha=B)
        for number in range(2, 6):
            self.api.changed[number] = [{"filename": "cpp/bolt/file.cpp"}]
        self.api.history[2] = [{"id": 20, "event": "reopened", "created": 950}]
        self.api.history[4] = [{"id": 40, "event": "reopened", "created": 1110}]
        self.comment(10, "Run Gluten Bolt CI", "contributor")
        self.listener.tick()

        for number, base_sha in ((2, C), (3, B)):
            with self.subTest(number=number, base_sha=base_sha):
                self.assertIsNone(self.db.latest(number))
                snapshot = self.db.snapshots()[number]
                self.assertEqual(snapshot["base_ref"], "main")
                self.assertEqual(snapshot["base_sha"], base_sha)
                self.assertEqual(snapshot["updated"], 1120)
        for number, reason in ((1, "manual_override"), (4, "reopened"), (5, "auto")):
            with self.subTest(number=number, reason=reason):
                request = self.db.latest(number)
                self.assertEqual(request["reason"], reason)
                self.assertEqual(request["head_sha"], self.api.current[number].head_sha)
                self.assertEqual(request["state"], "queued")
                self.assertEqual(request["attempt"], 1)
        self.assertTrue(self.db.event_seen("reopen", 40))
        self.assertEqual(self.api.event_calls, 5)
        self.assertEqual(self.api.file_calls, 2)

    def test_same_sha_reopens_once_and_old_events_are_ignored(self):
        self.api.current[1] = pull(updated=1100)
        self.api.changed[1] = [{"filename": "cpp/bolt/example.cpp"}]
        self.api.history[1] = [{"id": 1, "event": "reopened", "created": 950}]
        self.listener.tick()
        self.assertIsNone(self.db.latest(1))
        self.api.history[1].append({"id": 2, "event": "reopened", "created": 1110})
        self.api.current[1] = pull(updated=1120)
        self.listener.tick()
        self.assertEqual(self.db.latest(1)["attempt"], 1)
        self.db.expire(1300, 1)
        self.listener.tick()
        self.assertEqual(self.db.latest(1)["attempt"], 1)
        self.api.current[1] = pull(updated=1125, open=False)
        self.listener.tick()
        self.api.current[1] = pull(updated=1130)
        self.api.history[1].append({"id": 3, "event": "reopened", "created": 1130})
        self.listener.tick()
        self.assertEqual(self.db.latest(1)["attempt"], 2)

    def test_reopen_between_polls_supersedes_an_older_live_attempt(self):
        self.api.current[1] = pull(sha=B, updated=1100)
        self.api.changed[1] = [{"filename": "backends-bolt/Example.scala"}]
        self.listener.tick()
        first = self.db.latest(1)
        self.db.claim(self.time, 1800, 4)
        self.time = 1400
        self.api.current[1] = pull(sha=B, updated=1350)
        self.api.history[1] = [{"id": 100, "event": "reopened", "created": 1350}]
        self.listener.tick()
        self.assertEqual(self.db.latest(1)["attempt"], 2)
        self.assertEqual(self.db.active()[0]["request_state"], "superseded")
        self.assertEqual(self.db.active()[0]["request_id"], first["id"])

    def test_new_sha_and_reopen_coalesce_even_if_discovered_in_separate_polls(self):
        self.api.current[1] = pull(sha=B, updated=1100)
        self.api.changed[1] = [{"filename": "backends-bolt/Example.scala"}]
        self.listener.tick()
        self.db.expire(1220, 1)
        self.time = 1400
        self.api.current[1] = pull(sha=B, updated=1350)
        self.api.history[1] = [{"id": 101, "event": "reopened", "created": 1150}]
        self.listener.tick()
        self.assertEqual(self.db.latest(1)["attempt"], 1)
