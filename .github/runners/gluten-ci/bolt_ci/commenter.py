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

"""Deliver a single field-only status comment per PR from the durable outbox."""

import logging
import time

from .github import ApiError, positive
from .policy import valid_sha

MARKER = "<!-- gluten-bolt-external-ci:v1 -->"
TITLES = {
    "queued": "Queued",
    "running": "Running",
    "success": "Passed",
    "failure": "Failed",
    "queue_timeout": "Queue wait expired",
    "not_applicable": "Not applicable",
}
CATEGORIES = {
    "build": "build",
    "timeout": "runtime timeout",
    "interrupted": "interrupted",
    "infrastructure": "infrastructure",
    "evidence": "invalid evidence",
}


def summary(event, tasks):
    sha, run, attempt = event["head_sha"], event["id"], event["attempt"]
    if not valid_sha(sha) or not positive(run) or not positive(attempt):
        raise ValueError("Invalid publication identity")
    status = event["publication"]
    lines = [
        MARKER,
        f"**Gluten Bolt Backend Compilation — {TITLES[status]}**",
        f"Commit: `{sha}` | Run: `{run}` | Attempt: {attempt}",
    ]
    if status == "queued":
        lines.append("Backend compilation is waiting for an executor.")
    elif status == "running":
        lines.append("Backend compilation is running.")
    elif status == "queue_timeout":
        lines.append("The queue wait limit expired before backend compilation started.")
    elif status == "not_applicable":
        lines.append("No Bolt-related changes in the current PR diff.")
    elif status == "success":
        lines.append("Backend compilation succeeded.")
    else:
        failed = next((task for task in tasks if task["state"] == "failure"), {})
        category = CATEGORIES.get(failed.get("category"), "infrastructure")
        lines.append(f"Backend compilation failed ({category}).")
    if status in {"success", "failure"}:
        lines.append(f"Report: `bolt-{run}`")
    lines.append("To rerun, comment: `Run Gluten Bolt CI`")
    return "\n\n".join(lines)


class Commenter:
    def __init__(self, state, reader, writer, *, only_pr=None, clock=time.time):
        self.state = state
        self.reader = reader
        self.writer = writer
        self.only_pr = only_pr
        self.clock = clock
        self.viewer_id = None

    def _own(self, number):
        matches = []
        for comment in self.writer.comments(number):
            if not isinstance(comment, dict):
                continue
            user = comment.get("user")
            body = comment.get("body")
            if (
                isinstance(user, dict)
                and user.get("id") == self.viewer_id
                and positive(comment.get("id"))
                and isinstance(body, str)
                and MARKER in body.splitlines()
            ):
                matches.append(comment["id"])
        known = self.state.comment_id(number)
        return known if known in matches else min(matches, default=None)

    @staticmethod
    def _same(event, pr):
        return pr.open and pr.matches_execution(event)

    def _neutralize(self, comment_id, pr):
        if comment_id is not None and pr.open and valid_sha(pr.head_sha):
            body = f"{MARKER}\n\n**Gluten Bolt Backend Compilation**\n\nAwaiting results for current commit `{pr.head_sha}`."
            self.writer.update_comment(comment_id, body)

    def _write(self, event):
        number = event["pr"]
        if not self.state.current(event):
            return
        if self.viewer_id is None:
            self.viewer_id = self.writer.viewer()
        # Discover the marker even if the database lacks a comment ID. A previous
        # POST may have succeeded before an HTTP timeout or controller restart.
        owned = self._own(number)
        current = self.reader.pull(number)
        if not self.state.current(event):
            return
        if not self._same(event, current):
            self._neutralize(owned, current)
            self.state.delivered(event)
            return
        if owned is None and event["publication"] == "not_applicable":
            self.state.delivered(event)
            return
        body = summary(event, self.state.task_rows(event["id"]))
        self.state.publishing(number)
        if owned is None:
            owned = self.writer.post_comment(number, body)
        else:
            self.writer.update_comment(owned, body)
        self.state.record_comment(number, owned)
        after = self.reader.pull(number)
        latest = self.state.latest(number)
        if not self._same(event, after) or (latest and latest["id"] != event["id"]):
            self._neutralize(owned, after)
        self.state.delivered(event)

    def tick(self):
        for event in self.state.pending(int(self.clock()), self.only_pr):
            with self.state.lock(f"comment-{event['pr']}") as locked:
                if not locked:
                    continue
                try:
                    self._write(event)
                except ApiError as error:
                    wait = min(3600, 15 * 2 ** min(event["failures"], 8))
                    self.state.retry(
                        event, int(self.clock()) + max(wait, error.retry_after)
                    )
                    logging.warning("Comment delivery deferred (%s)", error)
