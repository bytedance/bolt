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

"""Durable queue, execution graph, and publication outbox."""

import fcntl
import json
import os
import sqlite3
import uuid
from contextlib import closing, contextmanager

from .policy import RULE_VERSION

IN_PROGRESS = ("queued", "running")
COUNTS = ("total", "passed", "failed", "skipped")
SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pulls(
    pr INTEGER PRIMARY KEY, head_sha TEXT NOT NULL, head_repo TEXT NOT NULL,
    base_sha TEXT NOT NULL, base_repo TEXT NOT NULL, base_ref TEXT,
    updated INTEGER NOT NULL, open INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events(
    kind TEXT NOT NULL, event INTEGER NOT NULL, PRIMARY KEY(kind,event)
);
CREATE TABLE IF NOT EXISTS requests(
    id INTEGER PRIMARY KEY, pr INTEGER NOT NULL, head_sha TEXT NOT NULL,
    head_repo TEXT NOT NULL, base_sha TEXT NOT NULL, base_repo TEXT NOT NULL,
    base_ref TEXT, attempt INTEGER NOT NULL, state TEXT NOT NULL, reason TEXT NOT NULL,
    rules INTEGER NOT NULL, bundle TEXT NOT NULL, eligible INTEGER NOT NULL,
    UNIQUE(pr,head_sha,attempt)
);
CREATE INDEX IF NOT EXISTS request_pr ON requests(pr,id);
CREATE TABLE IF NOT EXISTS tasks(
    request_id INTEGER NOT NULL REFERENCES requests(id), ordinal INTEGER NOT NULL,
    task TEXT NOT NULL, kind TEXT NOT NULL, jdk INTEGER NOT NULL,
    state TEXT NOT NULL, claimed INTEGER, container TEXT UNIQUE,
    docker_id TEXT, stop_reason TEXT, category TEXT, counts TEXT, cleaned INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(request_id,ordinal)
);
CREATE TABLE IF NOT EXISTS outbox(
    id INTEGER PRIMARY KEY, request_id INTEGER NOT NULL REFERENCES requests(id),
    pr INTEGER NOT NULL, state TEXT NOT NULL, retry_at INTEGER NOT NULL,
    failures INTEGER NOT NULL DEFAULT 0, delivered INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS outbox_pr ON outbox(pr,id);
CREATE TABLE IF NOT EXISTS comments(
    pr INTEGER PRIMARY KEY, remote_id INTEGER, attempted INTEGER NOT NULL DEFAULT 0
);
"""
TASK_JOIN = """SELECT t.*,r.pr,r.head_sha,r.head_repo,r.base_repo,r.base_ref,
    r.bundle,r.state AS request_state FROM tasks t JOIN requests r
    ON t.request_id=r.id"""


class State:
    def __init__(self, directory, repository, matrix=(), bundle=""):
        self.directory = directory
        self.matrix = list(matrix) or [{"id": "native", "kind": "native", "jdk": 17}]
        if len(self.matrix) != 1 or (self.matrix[0]["id"], self.matrix[0]["kind"]) != (
            "native",
            "native",
        ):
            raise ValueError("External PR requests require one native compile task")
        self.bundle = bundle
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.path = directory / "queue.sqlite3"
        with closing(self.connect()) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(SCHEMA)
        with self.transaction() as db:
            db.execute(
                "INSERT OR IGNORE INTO meta VALUES('repository',?)", (repository,)
            )
            db.execute(
                "INSERT OR IGNORE INTO meta VALUES('instance',?)", (uuid.uuid4().hex,)
            )
            if self._meta(db, "repository") != repository:
                raise ValueError("State directory belongs to another repository")
            self.instance = self._meta(db, "instance")
            self._migrate_target_refs(db)
            self._retire_incompatible_requests(db)

    def _retire_incompatible_requests(self, db):
        db.execute(
            """UPDATE tasks SET state='not_run' WHERE state IN ('blocked','queued')
            AND request_id IN (SELECT id FROM requests WHERE bundle!=?
                AND state IN ('queued','running'))""",
            (self.bundle,),
        )
        db.execute(
            """UPDATE requests SET state='superseded' WHERE bundle!=?
            AND state IN ('queued','running')""",
            (self.bundle,),
        )
        db.execute(
            """UPDATE outbox SET delivered=1 WHERE delivered=0 AND request_id IN
            (SELECT id FROM requests WHERE bundle!=?)""",
            (self.bundle,),
        )

    def _migrate_target_refs(self, db):
        columns = {row["name"] for row in db.execute("PRAGMA table_info(requests)")}
        if "base_ref" in columns:
            return
        db.execute("ALTER TABLE pulls ADD COLUMN base_ref TEXT")
        db.execute("ALTER TABLE requests ADD COLUMN base_ref TEXT")
        # Legacy target branches cannot be recovered from a commit SHA. Keep
        # history, but retire unbound work and publications before using the DB.
        affected = db.execute(
            "SELECT pr FROM requests WHERE state IN ('queued','running') "
            "UNION SELECT pr FROM outbox WHERE delivered=0"
        ).fetchall()
        for (pr,) in affected:
            self._supersede(db, pr)

    def connect(self):
        db = sqlite3.connect(self.path, isolation_level=None, timeout=20)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        return db

    @contextmanager
    def transaction(self):
        with closing(self.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            yield db

    @contextmanager
    def lock(self, name):
        path = self.directory / (name + ".lock")
        with os.fdopen(
            os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600), "r+"
        ) as stream:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                yield False
            else:
                yield True

    @staticmethod
    def _meta(db, key):
        row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def meta(self, key):
        with closing(self.connect()) as db:
            return self._meta(db, key)

    def set_meta(self, key, value):
        with self.transaction() as db:
            db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, str(value)))

    def _read(self, query, args=()):
        with closing(self.connect()) as db:
            return [dict(row) for row in db.execute(query, args)]

    def snapshots(self):
        return {
            row["pr"]: row
            for row in self._read(
                """SELECT p.*, (r.state='superseded' AND r.bundle!=?
                   AND r.head_sha=p.head_sha AND r.head_repo=p.head_repo
                   AND r.base_repo=p.base_repo AND r.base_ref=p.base_ref)
                   AS needs_upgrade
                FROM pulls p LEFT JOIN requests r ON r.id=(
                    SELECT MAX(n.id) FROM requests n WHERE n.pr=p.pr)""",
                (self.bundle,),
            )
        }

    @staticmethod
    def _save_pull(db, pr):
        db.execute(
            "INSERT OR REPLACE INTO pulls"
            "(pr,head_sha,head_repo,base_sha,base_repo,base_ref,updated,open) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (pr.number, *pr.diff_identity, pr.updated, pr.open),
        )

    def baseline(self, pulls, now):
        with self.transaction() as db:
            for pr in pulls:
                self._save_pull(db, pr)
            db.execute("INSERT INTO meta VALUES('baseline',?)", (str(now),))

    @staticmethod
    def _latest(db, pr):
        return db.execute(
            "SELECT * FROM requests WHERE pr=? ORDER BY id DESC LIMIT 1", (pr,)
        ).fetchone()

    def latest(self, pr):
        with closing(self.connect()) as db:
            row = self._latest(db, pr)
            return dict(row) if row else None

    @staticmethod
    def _event(db, kind, event):
        return db.execute(
            "INSERT OR IGNORE INTO events VALUES(?,?)", (kind, event)
        ).rowcount

    def event_seen(self, kind, event):
        return bool(
            self._read("SELECT 1 FROM events WHERE kind=? AND event=?", (kind, event))
        )

    @staticmethod
    def _publish(db, request_id, state, now):
        db.execute(
            "INSERT INTO outbox(request_id,pr,state,retry_at) "
            "SELECT id,pr,?,? FROM requests WHERE id=?",
            (state, now, request_id),
        )

    @staticmethod
    def _supersede(db, pr):
        db.execute(
            "UPDATE tasks SET state='not_run' WHERE state IN ('blocked','queued') AND "
            "request_id IN (SELECT id FROM requests WHERE pr=? AND state IN ('queued','running'))",
            (pr,),
        )
        db.execute(
            "UPDATE requests SET state='superseded' WHERE pr=? AND state IN ('queued','running')",
            (pr,),
        )
        db.execute("UPDATE outbox SET delivered=1 WHERE pr=? AND delivered=0", (pr,))

    def close(self, pr):
        with self.transaction() as db:
            db.execute("UPDATE pulls SET open=0 WHERE pr=?", (pr,))
            self._supersede(db, pr)

    def observe(
        self, pr, now, *, related=None, reason="auto", reopened=(), comment=None
    ):
        """Store a verified observation and its events in the enqueue transaction."""
        with self.transaction() as db:
            if comment is not None and not self._event(db, "comment", comment):
                return None
            previous = db.execute(
                "SELECT * FROM pulls WHERE pr=?", (pr.number,)
            ).fetchone()
            if previous and previous["updated"] > pr.updated:
                return None
            reopen_time = max(
                (
                    created
                    for event, created in reopened
                    if self._event(db, "reopen", event)
                ),
                default=None,
            )
            changed = not pr.matches_execution(previous) or not previous["open"]
            self._save_pull(db, pr)
            if not pr.open:
                self._supersede(db, pr.number)
                return None
            latest = self._latest(db, pr.number)
            same = pr.matches_execution(latest)
            if latest and not same:
                self._supersede(db, pr.number)
            upgrade = (
                reason == "config_upgrade"
                and same
                and latest["state"] == "superseded"
                and latest["bundle"] != self.bundle
            )
            if upgrade and latest["reason"] in ("manual_override", "operator"):
                related = True
                reason = latest["reason"]
            if related is None or not (
                upgrade
                or changed
                or reopen_time is not None
                or reason in ("manual_override", "operator")
            ):
                return None
            if same and reopen_time is not None:
                if previous and previous["open"] and latest["eligible"] >= reopen_time:
                    return dict(latest)
                # An observed or intervening closure invalidates even a live
                # attempt for this same source SHA if it predates the reopen.
                if latest["state"] in IN_PROGRESS:
                    self._supersede(db, pr.number)
            elif same and latest["state"] in IN_PROGRESS:
                return dict(latest)
            if reopen_time is not None and reason == "auto":
                reason = "reopened"
            attempt = db.execute(
                "SELECT COALESCE(MAX(attempt),0)+1 FROM requests WHERE pr=? AND head_sha=?",
                (pr.number, pr.head_sha),
            ).fetchone()[0]
            status = "queued" if related else "not_applicable"
            req = db.execute(
                "INSERT INTO requests(pr,head_sha,head_repo,base_sha,base_repo,base_ref,"
                "attempt,state,reason,rules,bundle,eligible) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    pr.number,
                    *pr.diff_identity,
                    attempt,
                    status,
                    reason,
                    RULE_VERSION,
                    self.bundle,
                    now,
                ),
            ).lastrowid
            if related:
                task = self.matrix[0]
                db.execute(
                    "INSERT INTO tasks(request_id,ordinal,task,kind,jdk,state) "
                    "VALUES(?,0,?,?,?,'queued')",
                    (req, task["id"], task["kind"], task["jdk"]),
                )
            if (
                related
                or db.execute(
                    "SELECT 1 FROM comments WHERE pr=? AND attempted=1", (pr.number,)
                ).fetchone()
            ):
                self._publish(db, req, status, now)
            return dict(self._latest(db, pr.number))

    def ignore_comment(self, event):
        with self.transaction() as db:
            self._event(db, "comment", event)

    def expire(self, now, timeout, only_pr=None):
        with self.transaction() as db:
            expired = db.execute(
                """SELECT r.id FROM requests r JOIN tasks t ON t.request_id=r.id
                WHERE r.state='queued' AND t.kind='native' AND t.state='queued'
                AND r.bundle=? AND r.eligible<=? AND (? IS NULL OR r.pr=?)""",
                (self.bundle, now - timeout, only_pr, only_pr),
            ).fetchall()
            for (request_id,) in expired:
                db.execute(
                    "UPDATE requests SET state='queue_timeout' WHERE id=?",
                    (request_id,),
                )
                db.execute(
                    "UPDATE tasks SET state='not_run' WHERE request_id=?", (request_id,)
                )
                self._publish(db, request_id, "queue_timeout", now)

    def claim(self, now, timeout, slots, only_pr=None):
        with self.transaction() as db:
            active = db.execute(
                "SELECT COUNT(*) FROM tasks WHERE state IN ('claimed','running')"
            ).fetchone()[0]
            if active >= slots:
                return None
            row = db.execute(
                TASK_JOIN
                + """ WHERE t.state='queued' AND r.state='queued' AND r.bundle=?
                    AND r.eligible>? AND (? IS NULL OR r.pr=?)
                    ORDER BY r.id,t.ordinal LIMIT 1""",
                (self.bundle, now - timeout, only_pr, only_pr),
            ).fetchone()
            if not row:
                return None
            container = (
                f"bolt-gluten-{self.instance[:12]}-{row['request_id']}-{row['ordinal']}"
            )
            db.execute(
                "UPDATE tasks SET state='claimed',claimed=?,container=? WHERE request_id=? AND ordinal=?",
                (now, container, row["request_id"], row["ordinal"]),
            )
            return dict(row) | {
                "state": "claimed",
                "claimed": now,
                "container": container,
            }

    def invalidate(self, task):
        with self.transaction() as db:
            latest = self._latest(db, task["pr"])
            if latest and latest["id"] == task["request_id"]:
                self._supersede(db, task["pr"])

    def cleanup_ready(self, names):
        names = tuple(names)
        bind = ",".join("?" for _ in names)
        return self._read(
            TASK_JOIN + " WHERE t.container IS NOT NULL "
            f"AND (t.cleaned=0 OR t.container IN ({bind})) "
            "AND t.state NOT IN ('claimed','running')",
            names,
        )

    def remember_container(self, task, docker_id):
        with self.transaction() as db:
            return (
                db.execute(
                    """UPDATE tasks SET docker_id=? WHERE container=?
                AND (docker_id IS NULL OR docker_id=?)""",
                    (docker_id, task["container"], docker_id),
                ).rowcount
                == 1
            )

    def mark_stopping(self, task, reason):
        with self.transaction() as db:
            db.execute(
                "UPDATE tasks SET stop_reason=? WHERE container=? AND stop_reason IS NULL",
                (reason, task["container"]),
            )

    def cleaned(self, task):
        with self.transaction() as db:
            db.execute(
                "UPDATE tasks SET cleaned=1 WHERE container=?", (task["container"],)
            )

    def active(self):
        return self._read(TASK_JOIN + " WHERE t.state IN ('claimed','running')")

    def current_task(self, task):
        return bool(
            self._read(
                """SELECT 1 FROM requests r JOIN pulls p ON p.pr=r.pr
            WHERE r.id=? AND r.state IN ('queued','running') AND p.open=1
            AND r.id=(SELECT MAX(n.id) FROM requests n WHERE n.pr=r.pr)""",
                (task["request_id"],),
            )
        )

    def task_rows(self, request_id):
        return self._read(
            "SELECT * FROM tasks WHERE request_id=? ORDER BY ordinal", (request_id,)
        )

    def mark_started(self, task, now):
        with self.transaction() as db:
            db.execute(
                "UPDATE tasks SET state='running' WHERE container=? AND state='claimed'",
                (task["container"],),
            )
            if db.execute(
                "UPDATE requests SET state='running' WHERE id=? AND state='queued'",
                (task["request_id"],),
            ).rowcount:
                self._publish(db, task["request_id"], "running", now)

    def finish(self, task, status, category, counts, now):
        with self.transaction() as db:
            request_id = task["request_id"]
            request = db.execute(
                "SELECT state FROM requests WHERE id=?", (request_id,)
            ).fetchone()
            if request[0] == "superseded":
                status, category, counts = "not_run", None, None
            if (
                not db.execute(
                    "UPDATE tasks SET state=?,category=?,counts=? WHERE container=? AND state IN ('claimed','running')",
                    (
                        status,
                        category,
                        json.dumps(counts) if counts else None,
                        task["container"],
                    ),
                ).rowcount
                or request[0] == "superseded"
            ):
                return
            result = "success" if status == "success" else "failure"
            db.execute("UPDATE requests SET state=? WHERE id=?", (result, request_id))
            self._publish(db, request_id, result, now)

    def pending(self, now, only_pr=None):
        return self._read(
            """SELECT o.id AS outbox_id,o.state AS publication,o.failures,r.*
            FROM outbox o JOIN requests r ON r.id=o.request_id
            WHERE o.delivered=0 AND r.bundle=? AND o.retry_at<=? AND (? IS NULL OR o.pr=?)
            AND o.id=(SELECT MAX(n.id) FROM outbox n WHERE n.pr=o.pr)
            ORDER BY o.id""",
            (self.bundle, now, only_pr, only_pr),
        )

    def current(self, event):
        with closing(self.connect()) as db:
            request = self._latest(db, event["pr"])
            row = db.execute(
                "SELECT open FROM pulls WHERE pr=?", (event["pr"],)
            ).fetchone()
            publication = db.execute(
                "SELECT id,delivered FROM outbox WHERE pr=? ORDER BY id DESC LIMIT 1",
                (event["pr"],),
            ).fetchone()
            return (
                request
                and row
                and row["open"]
                and publication
                and not publication["delivered"]
                and publication["id"] == event["outbox_id"]
                and request["id"] == event["id"]
                and request["bundle"] == self.bundle
                and request["state"] == event["publication"]
            )

    def delivered(self, event):
        with self.transaction() as db:
            db.execute(
                "UPDATE outbox SET delivered=1 WHERE pr=? AND id<=?",
                (event["pr"], event["outbox_id"]),
            )

    def retry(self, event, when):
        with self.transaction() as db:
            db.execute(
                "UPDATE outbox SET retry_at=?,failures=failures+1 WHERE id=?",
                (when, event["outbox_id"]),
            )

    def publishing(self, pr):
        with self.transaction() as db:
            db.execute(
                "INSERT OR IGNORE INTO comments(pr,attempted) VALUES(?,1)", (pr,)
            )

    def comment_id(self, pr):
        rows = self._read("SELECT remote_id FROM comments WHERE pr=?", (pr,))
        return rows[0]["remote_id"] if rows else None

    def record_comment(self, pr, remote_id):
        with self.transaction() as db:
            db.execute("INSERT OR REPLACE INTO comments VALUES(?,?,1)", (pr, remote_id))

    def status(self):
        return {
            "requests": self._read(
                "SELECT state,COUNT(*) AS count FROM requests GROUP BY state"
            ),
            "tasks": self._read(
                "SELECT state,COUNT(*) AS count FROM tasks GROUP BY state"
            ),
            "pending_comments": self._read(
                "SELECT COUNT(*) AS count FROM outbox WHERE delivered=0"
            )[0]["count"],
        }
