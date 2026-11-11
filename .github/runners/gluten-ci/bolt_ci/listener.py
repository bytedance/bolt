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

"""Poll GitHub and enqueue only verified current PR identities."""

import logging
import time
from dataclasses import replace

from .github import ApiError
from .policy import affected, command_sha

COMMENT_OVERLAP = 300


class Listener:
    def __init__(self, state, api, maintainers=(), only_pr=None, clock=time.time):
        self.state = state
        self.api = api
        self.maintainers = {login.lower() for login in maintainers}
        self.only_pr = only_pr
        self.clock = clock

    def _allowed(self, number):
        return self.only_pr is None or self.only_pr == number

    def _classify(self, number):
        for _ in range(3):
            before = self.api.pull(number)
            if not before.open or not before.head_repo:
                return before, None
            try:
                files = self.api.files(number)
            except ApiError:
                files = None
            after = self.api.pull(number)
            if before.diff_identity == after.diff_identity and after.open:
                return after, affected(files, after.changed)
        raise ApiError()

    def enqueue(self, number, dry_run):
        if not self._allowed(number):
            raise ValueError("PR outside configured validation scope")
        with self.state.lock("listener") as locked:
            if not locked:
                raise RuntimeError("Listener is busy")
            pr, related = self._classify(number)
            if not pr.open or not pr.head_repo:
                raise ValueError("PR has no open, available head")
            result = {
                "pr": pr.number,
                "head_sha": pr.head_sha,
                "base_sha": pr.base_sha,
                "automatic": "queued" if related else "not_applicable",
                "operator": "queued",
                "tasks": len(self.state.matrix),
            }
            if not dry_run:
                request = self.state.observe(
                    pr, int(self.clock()), related=True, reason="operator"
                )
                if not request:
                    raise RuntimeError("PR changed during enqueue")
                result["run"] = request["id"]
                result["attempt"] = request["attempt"]
            return result

    def _automatic(self, listed, old, baseline, now):
        if not listed.head_repo:
            self.state.close(listed.number)
            return
        identity_changed = not listed.matches_execution(old) or not old["open"]
        updated = old is None or old["updated"] != listed.updated
        upgrade = old and old["open"] and old["needs_upgrade"]
        if (
            not identity_changed
            and not updated
            and not upgrade
            and old["base_sha"] == listed.base_sha
        ):
            return
        if not self._allowed(listed.number):
            self.state.observe(listed, now)
            return
        events = self.api.events(listed.number) if updated else []
        reopened = [
            (event["id"], event["created"])
            for event in events
            if event["event"] == "reopened"
            and event["created"] > baseline
            and not self.state.event_seen("reopen", event["id"])
        ]
        if (
            not reopened
            and old
            and old["open"]
            and old["base_ref"] is None
            and listed.head_sha == old["head_sha"]
            and listed.head_repo == old["head_repo"]
            and listed.base_repo == old["base_repo"]
            and self.state.latest(listed.number) is None
        ):
            # Learn the target for a legacy baseline entry without backfilling it.
            identity_changed = False
        if identity_changed or reopened or upgrade:
            verified, related = self._classify(listed.number)
            if not verified.head_repo:
                self.state.close(verified.number)
            else:
                # A newer updated_at may hide a reopen not returned in this event page.
                # Leave that event scan pending by storing the older update cursor.
                if verified.updated > listed.updated and old is not None:
                    verified = replace(verified, updated=listed.updated)
                self.state.observe(
                    verified,
                    now,
                    related=related,
                    reopened=reopened,
                    reason="config_upgrade" if upgrade and not reopened else "auto",
                )
        else:
            self.state.observe(listed, now)

    def _comments(self, baseline, now):
        last = int(self.state.meta("comments_cursor") or baseline)
        comments = self.api.recent_comments(max(baseline, last - COMMENT_OVERLAP))
        # Process all ID events before advancing the durable cursor. An API failure
        # leaves the cursor unchanged and already processed IDs are harmless.
        for comment in sorted(
            comments, key=lambda entry: (entry["created"], entry["id"])
        ):
            if not self._allowed(comment["pr"]) or comment["created"] <= baseline:
                continue
            if self.state.event_seen("comment", comment["id"]):
                continue
            # Generated status bodies cannot match the whole-comment command.
            # An authorized person may use the same account as the publisher.
            sha = command_sha(comment["body"])
            if (
                sha is None
                or comment["type"] != "User"
                or comment["updated"] != comment["created"]
            ):
                self.state.ignore_comment(comment["id"])
                continue
            try:
                pr = self.api.pull(comment["pr"])
            except ApiError as error:
                if error.status == 404:
                    self.state.ignore_comment(comment["id"])
                    continue
                raise
            authorized = comment["login"].lower() in self.maintainers | {
                pr.author.lower()
            }
            if (
                not pr.open
                or not pr.head_repo
                or not authorized
                or (sha and sha != pr.head_sha)
            ):
                self.state.ignore_comment(comment["id"])
                continue
            self.state.observe(
                pr, now, related=True, reason="manual_override", comment=comment["id"]
            )
        self.state.set_meta("comments_cursor", max(last, now))

    def tick(self):
        with self.state.lock("listener") as locked:
            if not locked:
                return
            now = int(self.clock())
            previous = self.state.snapshots()
            listed = {pr.number: pr for pr in self.api.open_pulls()}
            baseline = self.state.meta("baseline")
            if baseline is None:
                self.state.baseline(listed.values(), now)
                return
            baseline = int(baseline)
            for number, old in previous.items():
                if old["open"] and number not in listed:
                    try:
                        if not self.api.pull(number).open:
                            self.state.close(number)
                    except ApiError as error:
                        if error.status in {403, 429}:
                            raise
                        logging.warning("PR close verification deferred (%s)", error)
            for number, pr in listed.items():
                try:
                    self._automatic(pr, previous.get(number), baseline, now)
                except ApiError as error:
                    if error.status in {403, 429}:
                        raise
                    logging.warning("PR classification deferred (%s)", error)
            self._comments(baseline, now)
