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

"""Bounded GitHub REST calls without credential redirects or raw API errors."""

import json
import re
import time
from datetime import datetime, timezone
from http.client import HTTPException
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .policy import LOGIN, Pull, repository, valid_sha

API = "https://api.github.com"
MAX_BODY = 32 * 1024 * 1024
NEXT = re.compile(r'<([^<>]+)>;\s*rel="next"')


class ApiError(RuntimeError):
    def __init__(self, status=None, retry_after=0):
        super().__init__(
            f"GitHub API HTTP {status}" if status else "GitHub API unavailable"
        )
        self.status = status
        self.retry_after = retry_after


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def timestamp(value):
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


def api_time(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def positive(value):
    return type(value) is int and 0 < value < 2**63


class GitHub:
    def __init__(self, repo, token="", *, opener=None, base=API, sleep=time.sleep):
        if not repository(repo):
            raise ValueError("Invalid watched repository")
        self.repo = repo
        self.prefix = "/repos/" + repo
        self.token = token
        self.opener = opener or build_opener(ProxyHandler({}), NoRedirect())
        self.base = base
        self.sleep = sleep

    def _request(self, method, route, body=None):
        data = json.dumps(body).encode() if body is not None else None
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "Bolt-Gluten-External-CI",
            "Content-Type": "application/json",
        }
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        request = Request(self.base + route, headers=headers, method=method, data=data)
        attempts = 3 if method == "GET" else 1
        for attempt in range(attempts):
            try:
                with self.opener.open(request, timeout=15) as response:
                    payload = response.read(MAX_BODY + 1)
                    if len(payload) > MAX_BODY:
                        raise ValueError()
                    return json.loads(payload), response.headers.get("Link", "")
            except HTTPError as error:
                status = error.code
                raw = error.headers.get("Retry-After", "")
                reset = error.headers.get("X-RateLimit-Reset", "")
                remaining = error.headers.get("X-RateLimit-Remaining")
                wait = (
                    int(raw)
                    if raw.isascii() and raw.isdecimal() and len(raw) < 8
                    else 0
                )
                if remaining == "0" and reset.isdecimal() and len(reset) < 13:
                    wait = max(wait, int(reset) - int(time.time()))
                error.close()
                last = ApiError(status, max(0, wait))
                if status not in {408, 429, 500, 502, 503, 504}:
                    break
            except (OSError, URLError, HTTPException, ValueError):
                last = ApiError()
            if attempt + 1 < attempts:
                if last.retry_after > 5:
                    break
                self.sleep(max(last.retry_after, 2**attempt))
        raise last from None

    def _get(self, route):
        return self._request("GET", route)[0]

    def _pages(self, path, **params):
        route = path + "?" + urlencode({"per_page": 100, **params})
        seen = set()
        for _ in range(1000):
            if route in seen:
                break
            seen.add(route)
            payload, link = self._request("GET", route)
            if not isinstance(payload, list):
                break
            yield from payload
            match = NEXT.search(link)
            if not match:
                return
            next_url = urlsplit(match[1])
            current = urlsplit(self.base)
            allowed_path = next_url.path == path
            if path.startswith(self.prefix + "/"):
                suffix = path[len(self.prefix) :]
                allowed_path |= (
                    re.fullmatch(
                        r"/repositories/[1-9][0-9]*" + re.escape(suffix), next_url.path
                    )
                    is not None
                )
            if (
                (next_url.scheme, next_url.netloc) != (current.scheme, current.netloc)
                or not allowed_path
                or next_url.fragment
                or not next_url.query
            ):
                break
            route = next_url.path + "?" + next_url.query
        raise ApiError()

    def _pull(self, value):
        try:
            head, base = value["head"], value["base"]
            number, author = value["number"], value["user"]["login"]
            state = value["state"]
            head_repo = head["repo"]["full_name"] if head["repo"] else ""
            base_repo, base_ref = base["repo"]["full_name"], base["ref"]
            changed = value.get("changed_files")
            if (
                not positive(number)
                or not valid_sha(head["sha"])
                or not valid_sha(base["sha"])
                or (head_repo and not repository(head_repo))
                or not repository(base_repo)
                or base_repo.lower() != self.repo.lower()
                or not isinstance(base_ref, str)
                or not base_ref
                or state not in {"open", "closed"}
                or not LOGIN.fullmatch(author)
                or (changed is not None and (type(changed) is not int or changed < 0))
            ):
                raise ValueError()
            return Pull(
                number,
                head["sha"],
                head_repo,
                base["sha"],
                base_repo,
                base_ref,
                author,
                changed,
                timestamp(value["updated_at"]),
                state == "open",
            )
        except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
            raise ApiError() from None

    def check(self):
        data = self._get(self.prefix)
        if (
            not isinstance(data, dict)
            or data.get("full_name", "").lower() != self.repo.lower()
        ):
            raise ApiError()

    def open_pulls(self):
        pulls = [
            self._pull(entry)
            for entry in self._pages(
                self.prefix + "/pulls", state="open", sort="created", direction="asc"
            )
        ]
        if any(not pr.open for pr in pulls) or len({pr.number for pr in pulls}) != len(
            pulls
        ):
            raise ApiError()
        return pulls

    def pull(self, number):
        if not positive(number):
            raise ApiError()
        pr = self._pull(self._get(f"{self.prefix}/pulls/{number}"))
        if pr.number != number:
            raise ApiError()
        return pr

    def files(self, number):
        return list(self._pages(f"{self.prefix}/pulls/{number}/files"))

    def events(self, number):
        result = []
        try:
            for entry in self._pages(f"{self.prefix}/issues/{number}/events"):
                if entry["event"] == "reopened":
                    if not positive(entry["id"]):
                        raise ValueError()
                    result.append(
                        {
                            "id": entry["id"],
                            "event": "reopened",
                            "created": timestamp(entry["created_at"]),
                        }
                    )
        except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
            raise ApiError() from None
        return result

    def recent_comments(self, since):
        result = []
        try:
            for entry in self._pages(
                f"{self.prefix}/issues/comments",
                sort="updated",
                direction="asc",
                since=api_time(since),
            ):
                issue = urlsplit(entry["issue_url"])
                current = urlsplit(self.base)
                match = re.fullmatch(
                    re.escape(self.prefix) + r"/issues/([1-9][0-9]{0,18})",
                    issue.path,
                    re.IGNORECASE,
                )
                if (issue.scheme, issue.netloc) != (
                    current.scheme,
                    current.netloc,
                ) or not match:
                    raise ValueError()
                user = entry["user"]
                if not positive(entry["id"]) or not positive(int(match[1])):
                    raise ValueError()
                result.append(
                    dict(
                        id=entry["id"],
                        pr=int(match[1]),
                        body=entry["body"],
                        login=user["login"] if user else "",
                        type=user["type"] if user else "",
                        created=timestamp(entry["created_at"]),
                        updated=timestamp(entry["updated_at"]),
                    )
                )
        except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
            raise ApiError() from None
        return result

    def viewer(self):
        viewer = self._get("/user")
        if not isinstance(viewer, dict) or not positive(viewer.get("id")):
            raise ApiError()
        return viewer["id"]

    def comments(self, number):
        return list(self._pages(f"{self.prefix}/issues/{number}/comments"))

    def post_comment(self, number, body):
        comment = self._request(
            "POST", f"{self.prefix}/issues/{number}/comments", {"body": body}
        )[0]
        if not isinstance(comment, dict) or not positive(comment.get("id")):
            raise ApiError()
        return comment["id"]

    def update_comment(self, comment_id, body):
        if not positive(comment_id):
            raise ApiError()
        self._request(
            "PATCH", f"{self.prefix}/issues/comments/{comment_id}", {"body": body}
        )
