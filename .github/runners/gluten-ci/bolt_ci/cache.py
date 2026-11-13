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

"""Promote verified release downloads from a private Maven cache."""

import fcntl
import hashlib
import os
import re
import tempfile
import time
from http.client import HTTPException
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

from .github import NoRedirect
from .runner import regular

CENTRAL = "https://repo.maven.apache.org/maven2/"
MAX_DOWNLOAD = 64 * 1024 * 1024
COORDINATE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,149}")


def release_artifact(relative):
    parts = relative.parts
    if (
        len(parts) < 4
        or parts[:3] == ("org", "apache", "gluten")
        or any(
            not COORDINATE.fullmatch(part) or "SNAPSHOT" in part.upper()
            for part in parts
        )
        or relative.suffix not in {".jar", ".pom"}
    ):
        return False
    artifact, version, filename = parts[-3:]
    prefix = f"{artifact}-{version}"
    return filename == prefix + relative.suffix or filename.startswith(prefix + "-")


class MavenCache:
    def __init__(self, seed, *, opener=None, clock=time.monotonic):
        self.seed = seed
        self.opener = opener or build_opener(ProxyHandler({}), NoRedirect())
        self.clock = clock

    def _checksum(self, relative, remaining):
        for algorithm, size in (("sha256", 64), ("sha1", 40)):
            url = CENTRAL + relative.as_posix() + "." + algorithm
            try:
                request = Request(
                    url, headers={"User-Agent": "Bolt-Gluten-External-CI"}
                )
                with self.opener.open(
                    request, timeout=max(0.1, min(remaining(), 1.0))
                ) as response:
                    value = response.read(513).strip()
                if re.fullmatch(rb"[0-9a-fA-F]{%d}" % size, value):
                    return algorithm, value.decode("ascii").lower()
                return None
            except HTTPError as error:
                status = error.code
                error.close()
                if status != 404 or remaining() <= 0:
                    return None
            except (URLError, OSError, HTTPException):
                return None
        return None

    def _promote(self, source, relative, remaining):
        destination = self.seed / relative
        if destination.exists() or destination.resolve() != destination:
            return False
        with regular(source, relative.as_posix(), MAX_DOWNLOAD) as stream:
            checksum = self._checksum(relative, remaining)
            if not checksum:
                return True
            algorithm, expected = checksum
            digest = hashlib.new(algorithm)
            with tempfile.NamedTemporaryFile(
                dir=self.seed, prefix=".download-"
            ) as temp:
                while data := stream.read(1024 * 1024):
                    if remaining() <= 0:
                        return True
                    digest.update(data)
                    temp.write(data)
                if digest.hexdigest() != expected:
                    return True
                temp.flush()
                os.fsync(temp.fileno())
                os.chmod(temp.name, 0o644)
                lock = self.seed / ".publish.lock"
                with os.fdopen(
                    os.open(lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600), "r+"
                ) as file:
                    fcntl.flock(file, fcntl.LOCK_EX)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        # Link the complete controller-owned temp file with no overwrite.
                        os.link(temp.name, destination)
                    except FileExistsError:
                        pass
        return True

    def __call__(self, source):
        if source.resolve() != source or not source.is_dir() or not self.seed.is_dir():
            return
        deadline = self.clock() + 4

        def remaining():
            return deadline - self.clock()

        requests = 0
        for directory, dirs, files in os.walk(source, followlinks=False):
            dirs.sort()
            for filename in sorted(files):
                if requests >= 16 or remaining() <= 0:
                    return
                relative = (Path(directory) / filename).relative_to(source)
                if release_artifact(relative):
                    try:
                        requests += self._promote(source, relative, remaining)
                    except (OSError, ValueError):
                        # A bad cache candidate cannot change the job result.
                        continue
