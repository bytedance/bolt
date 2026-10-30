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

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "normalize_blog_posts.py"
POST = Path("doc/blog/_posts/2026-10-19-example.md")


def post_bytes(title='"Plain title"', author='"Example"'):
    return (
        "---\n"
        "layout: post\n"
        f"title: {title}\n"
        "date: 2026-10-19\n"
        f"author: {author}\n"
        "parent: Blog\n"
        "---\n"
        "\n"
        "## Body\n"
        "\n"
        "Keep `code` and **Markdown**.\n"
        "\n"
        "Second paragraph.\n"
    ).encode("utf-8")


class NormalizeBlogPostsTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.path = self.root / POST
        self.path.parent.mkdir(parents=True)

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args, str(POST)],
            cwd=self.root,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def assert_round_trip(self, original, expected):
        self.path.write_bytes(original)
        first = self.run_cli()
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(self.path.read_bytes(), expected)
        check = self.run_cli("--check")
        self.assertEqual(check.returncode, 0, check.stderr)
        self.assertEqual(self.path.read_bytes(), expected)
        second = self.run_cli()
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(self.path.read_bytes(), expected)

    def assert_scalars(self, cases):
        for original, expected in cases:
            for key in ("title", "author"):
                with self.subTest(key=key, original=original):
                    self.assert_round_trip(
                        post_bytes(**{key: original}), post_bytes(**{key: expected})
                    )

    def test_escaped_newline(self):
        self.assert_scalars([(r'"Line one\nLine two"', r'"Line one\nLine two"')])

    def test_escaped_carriage_return(self):
        self.assert_scalars([(r'"Line one\rLine two"', r'"Line one\rLine two"')])

    def test_escaped_tab(self):
        self.assert_scalars(
            [
                (r'"Line one\tLine two"', r'"Line one\tLine two"'),
                ('"Line one\tLine two"', r'"Line one\tLine two"'),
            ]
        )

    def test_combined_escapes(self):
        self.assert_scalars(
            [
                (r'"A\nB\rC\tD\\nE\"F"', r'"A\nB\rC\tD\\nE\"F"'),
                (r'"A\r\nB"', r'"A\r\nB"'),
                (r'"A\\\n\"B\\\r\"C\\\t\"D"', r'"A\\\n\"B\\\r\"C\\\t\"D"'),
            ]
        )

    def test_literal_backslashes(self):
        self.assert_scalars(
            [
                (r'"A\\nB\\rC\\tD"', r'"A\\nB\\rC\\tD"'),
                (r"'A\nB\rC\tD'", r'"A\\nB\\rC\\tD"'),
                (r"A\nB\rC\tD", r'"A\\nB\\rC\\tD"'),
                (r'"Path ends in \\"', r'"Path ends in \\"'),
            ]
        )

    def test_quotes_and_text(self):
        self.assert_scalars(
            [
                ("Plain title", '"Plain title"'),
                ("'Writer''s title'", '"Writer\'s title"'),
                (r'"Say \"hello\" at C:\\tmp"', r'"Say \"hello\" at C:\\tmp"'),
                ("'博客 café 🚀'", '"博客 café 🚀"'),
                (r'"  A\nB\t  "', r'"  A\nB\t  "'),
            ]
        )

    def test_empty_values_keep_defaults(self):
        for scalar in ("", '""', "''", '"   "', r'"\n\r\t"'):
            with self.subTest(scalar=scalar):
                self.assert_round_trip(
                    post_bytes(title=scalar, author=scalar),
                    post_bytes(title='"Example"', author='"Bolt Community"'),
                )

    def test_missing_values_keep_defaults(self):
        original = post_bytes().replace(b'title: "Plain title"\n', b"")
        original = original.replace(b'author: "Example"\n', b"")
        self.assert_round_trip(
            original, post_bytes(title='"Example"', author='"Bolt Community"')
        )

    def test_check_does_not_write(self):
        original = post_bytes(title="'Needs normalization'")
        expected = post_bytes(title='"Needs normalization"')
        self.path.write_bytes(original)
        check = self.run_cli("--check")
        self.assertEqual(check.returncode, 1, check.stderr)
        self.assertIn("blog post is not normalized", check.stderr)
        self.assertEqual(self.path.read_bytes(), original)
        self.assert_round_trip(original, expected)


if __name__ == "__main__":
    unittest.main()
