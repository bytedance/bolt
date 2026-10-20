#!/usr/bin/env python3
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

"""Configure Conan's source download cache and credentials.

Usage:
export CONAN_SOURCE_PASSWORD=${your_conan_password}
python scripts/configure-conan-sources-cache.py \
    --url ${your_conan_server_generic_repo_for_src_cache} \
    --user ${your_conan_user}

"""

import argparse
import json
import os
import secrets
import subprocess
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="Conan source repository URL")
    parser.add_argument("--user", required=True, help="Conan source repository user")
    args = parser.parse_args()
    args.password = os.environ.get("CONAN_SOURCE_PASSWORD")
    if not args.password:
        parser.error("CONAN_SOURCE_PASSWORD environment variable is required")
    return args


def conan_home() -> Path:
    result = subprocess.run(
        ["conan", "config", "home"],
        check=True,
        capture_output=True,
        text=True,
    )
    return Path(result.stdout.strip())


def configure_global_conf(home: Path, url: str) -> None:
    global_conf = home / "global.conf"
    settings = {
        "core.sources:download_cache": str(home / "source-backup"),
        "core.sources:download_urls": json.dumps([url, "origin"]),
    }
    lines = global_conf.read_text().splitlines() if global_conf.exists() else []
    lines = [line for line in lines if line.partition("=")[0].strip() not in settings]
    lines.extend(f"{key}={value}" for key, value in settings.items())
    global_conf.write_text("\n".join(lines) + "\n")


def configure_source_credentials(
    home: Path, url: str, user: str, password: str
) -> None:
    path = home / "source_credentials.json"
    content, expressions = load_source_credentials(path)
    credentials = content.setdefault("credentials", [])
    if not isinstance(credentials, list):
        raise ValueError("source_credentials.json: 'credentials' must be a list")

    updated = {"url": url, "user": user, "password": password}
    for index, credential in enumerate(credentials):
        if isinstance(credential, dict) and credential.get("url") == url:
            credentials[index] = updated
            break
    else:
        credentials.append(updated)

    text = json.dumps(content, indent=2) + "\n"
    for marker, (expression, was_quoted) in expressions.items():
        target = marker if was_quoted else json.dumps(marker)
        text = text.replace(target, expression)
    secure_write(path, text)


def load_source_credentials(
    path: Path,
) -> tuple[dict, dict[str, tuple[str, bool]]]:
    if not path.exists():
        return {"credentials": []}, {}

    text = path.read_text()
    if "{%" in text or "{#" in text:
        raise ValueError(
            "source_credentials.json: Jinja statements and comments cannot be "
            "updated safely"
        )

    expressions: dict[str, tuple[str, bool]] = {}
    masked = text
    offset = 0
    while True:
        start = masked.find("{{", offset)
        if start == -1:
            break
        end = masked.find("}}", start + 2)
        if end == -1:
            raise ValueError("source_credentials.json: unterminated Jinja expression")
        end += 2
        expression = masked[start:end]
        marker = f"__BOLT_JINJA_EXPRESSION_{len(expressions)}__"
        while marker in text:
            marker = f"{marker}_{secrets.token_hex(4)}"
        was_quoted = is_inside_json_string(masked, start)
        replacement = marker if was_quoted else json.dumps(marker)
        expressions[marker] = (expression, was_quoted)
        masked = f"{masked[:start]}{replacement}{masked[end:]}"
        offset = start + len(replacement)

    try:
        content = json.loads(masked)
    except json.JSONDecodeError as error:
        raise ValueError(f"source_credentials.json: invalid JSON: {error}") from error
    if not isinstance(content, dict):
        raise ValueError("source_credentials.json: top-level value must be an object")
    return content, expressions


def is_inside_json_string(text: str, position: int) -> bool:
    inside = False
    escaped = False
    for character in text[:position]:
        if escaped:
            escaped = False
        elif character == "\\":
            escaped = True
        elif character == '"':
            inside = not inside
    return inside


def secure_write(path: Path, text: str) -> None:
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = None
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        with os.fdopen(descriptor, "w") as output:
            descriptor = None
            output.write(text)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    except BaseException:
        if descriptor is not None:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
        raise


def main() -> None:
    args = parse_args()
    home = conan_home()
    configure_global_conf(home, args.url)
    configure_source_credentials(home, args.url, args.user, args.password)


if __name__ == "__main__":
    main()
