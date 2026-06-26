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


import argparse
import re
import sys
from pathlib import Path

POST_PATH = Path("doc/blog/_posts")
POST_FILENAME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})-([a-z0-9]+(?:-[a-z0-9]+)*)\.md$")
FRONT_MATTER_DELIMITER = "---"
DEFAULT_AUTHOR = "Bolt Community"
CORE_KEYS = ("layout", "title", "date", "author", "parent", "nav_order")
DISALLOWED_KEYS = {"has_children"}


class BlogPostError(Exception):
    pass


def discover_posts() -> list[Path]:
    if not POST_PATH.exists():
        return []
    return sorted(POST_PATH.glob("*.md"))


def title_from_slug(slug: str) -> str:
    return " ".join(part.capitalize() for part in slug.split("-"))


def split_front_matter(text: str) -> tuple[list[str], str]:
    lines = text.splitlines(keepends=True)
    if not lines or lines[0].strip() != FRONT_MATTER_DELIMITER:
        return [], text

    for index in range(1, len(lines)):
        if lines[index].strip() == FRONT_MATTER_DELIMITER:
            front_matter = [line.rstrip("\n") for line in lines[1:index]]
            body = "".join(lines[index + 1 :])
            return front_matter, body

    raise BlogPostError("front matter is opened but not closed")


def parse_front_matter(lines: list[str]) -> tuple[dict[str, str], list[str]]:
    values: dict[str, str] = {}
    order: list[str] = []

    for line in lines:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if ":" not in line:
            raise BlogPostError(f"unsupported front matter line: {line}")

        key, value = line.split(":", 1)
        key = key.strip()
        if not key:
            raise BlogPostError(f"empty front matter key: {line}")
        if key not in values:
            order.append(key)
        values[key] = value.strip()

    return values, order


def strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def unquote_string(value: str) -> str:
    if len(value) < 2 or value[0] != value[-1] or value[0] not in {'"', "'"}:
        return value

    unquoted = value[1:-1]
    if value[0] == "'":
        return unquoted.replace("''", "'")

    escapes = {
        '"': '"',
        "\\": "\\",
        "n": "\n",
        "r": "\r",
        "t": "\t",
    }
    decoded: list[str] = []
    index = 0
    while index < len(unquoted):
        char = unquoted[index]
        if char == "\\" and index + 1 < len(unquoted):
            escaped = unquoted[index + 1]
            decoded.append(escapes.get(escaped, char + escaped))
            index += 2
            continue
        decoded.append(char)
        index += 1
    return "".join(decoded)


def is_empty(value: str | None) -> bool:
    return value is None or unquote_string(value).strip() == ""


def quote_string(value: str) -> str:
    escaped = unquote_string(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def render_value(key: str, value: str) -> str:
    if key in {"title", "author"}:
        return quote_string(value)
    return value


def normalize_post(path: Path) -> tuple[bool, str]:
    match = POST_FILENAME_RE.match(path.name)
    if not match:
        raise BlogPostError("filename must match YYYY-MM-DD-lowercase-slug.md")

    filename_date, slug = match.groups()
    original = path.read_text(encoding="utf-8")
    front_matter_lines, body = split_front_matter(original)
    values, original_order = parse_front_matter(front_matter_lines)

    values["layout"] = "post"
    values["parent"] = "Blog"

    if is_empty(values.get("date")):
        values["date"] = filename_date
    elif not strip_quotes(values["date"]).startswith(filename_date):
        raise BlogPostError(
            f"front matter date {values['date']} does not match filename date {filename_date}"
        )

    if is_empty(values.get("title")):
        values["title"] = title_from_slug(slug)
    if is_empty(values.get("author")):
        values["author"] = DEFAULT_AUTHOR

    for key in DISALLOWED_KEYS:
        values.pop(key, None)

    if "nav_order" in values and not re.fullmatch(
        r"\d+", strip_quotes(values["nav_order"])
    ):
        raise BlogPostError("nav_order must be an integer when present")

    ordered_keys = [key for key in CORE_KEYS if key in values]
    ordered_keys.extend(
        key
        for key in original_order
        if key not in ordered_keys and key not in DISALLOWED_KEYS
    )

    rendered_front_matter = [FRONT_MATTER_DELIMITER]
    rendered_front_matter.extend(
        f"{key}: {render_value(key, values[key])}" for key in ordered_keys
    )
    rendered_front_matter.append(FRONT_MATTER_DELIMITER)

    normalized = "\n".join(rendered_front_matter) + "\n"
    if body and not body.startswith("\n"):
        normalized += "\n"
    normalized += body
    if not normalized.endswith("\n"):
        normalized += "\n"

    changed = normalized != original
    return changed, normalized


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Normalize Bolt blog post Markdown files."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="Markdown files to normalize. Defaults to doc/blog/_posts/*.md.",
    )
    parser.add_argument(
        "--check", action="store_true", help="Fail if any file would be changed."
    )
    args = parser.parse_args()

    paths = [Path(path) for path in args.paths] if args.paths else discover_posts()
    post_paths = [
        path for path in paths if path.suffix == ".md" and POST_PATH in path.parents
    ]

    changed_paths: list[Path] = []
    errors: list[str] = []

    for path in post_paths:
        try:
            changed, normalized = normalize_post(path)
        except BlogPostError as error:
            errors.append(f"{path}: {error}")
            continue

        if changed:
            changed_paths.append(path)
            if not args.check:
                path.write_text(normalized, encoding="utf-8")

    for error in errors:
        print(error, file=sys.stderr)

    if changed_paths and args.check:
        for path in changed_paths:
            print(f"{path}: blog post is not normalized", file=sys.stderr)
        return 1

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
