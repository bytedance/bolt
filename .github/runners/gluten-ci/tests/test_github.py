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

import json
import threading
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from bolt_ci.github import ApiError, GitHub


@contextmanager
def server(respond):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            status, headers, body = respond(self)
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as http:
        thread = threading.Thread(target=http.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{http.server_port}"
        finally:
            http.shutdown()
            thread.join()


class ApiTests(unittest.TestCase):
    def test_pull_retains_and_requires_target_ref(self):
        api = GitHub("apache/gluten")
        payload = {
            "number": 1,
            "user": {"login": "contributor"},
            "head": {"repo": {"full_name": "owner/contribution"}, "sha": "a" * 40},
            "base": {
                "repo": {"full_name": "apache/gluten"},
                "ref": "branch/3.5",
                "sha": "b" * 40,
            },
            "updated_at": "2026-11-09T12:00:00Z",
            "state": "open",
        }
        with patch.object(api, "_get", return_value=payload):
            self.assertEqual(getattr(api.pull(1), "base_ref", None), "branch/3.5")
            for invalid in (None, "", 42):
                with self.subTest(invalid=invalid), self.assertRaises(ApiError):
                    payload["base"]["ref"] = invalid
                    api.pull(1)
            del payload["base"]["ref"]
            with self.assertRaises(ApiError):
                api.pull(1)

    def test_follows_canonical_next_page_after_transient_error(self):
        calls = []

        def respond(handler):
            calls.append((handler.path, handler.headers.get("Authorization")))
            if len(calls) == 1:
                return 503, {}, {"message": "secret"}
            if len(calls) == 2:
                link = f'<{base}/repositories/501/pulls/1/files?page=2>; rel="next"'
                return 200, {"Link": link}, [{"filename": "docs/readme.md"}]
            return 200, {}, [{"filename": "cpp/bolt/a.cpp"}]

        with server(respond) as base:
            api = GitHub(
                "apache/gluten", "test-credential", base=base, sleep=lambda _: None
            )
            self.assertEqual(len(api.files(1)), 2)
        self.assertEqual(len(calls), 3)
        self.assertTrue(all(auth == "Bearer test-credential" for _, auth in calls))
        self.assertTrue(all("test-credential" not in path for path, _ in calls))

    def test_rejects_redirects_and_unsafe_pagination(self):
        def respond(handler):
            if handler.path.startswith("/repos"):
                return 200, {"Link": '<https://example.invalid/leak>; rel="next"'}, []
            return 302, {"Location": "https://example.invalid/leak"}, {}

        with server(respond) as base:
            api = GitHub("apache/gluten", "test-credential", base=base)
            with self.assertRaises(ApiError):
                api.files(1)
            with self.assertRaisesRegex(ApiError, "HTTP 302"):
                api.viewer()
