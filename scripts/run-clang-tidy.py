#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
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
# --------------------------------------------------------------------------
# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0
#
# This file has been modified by ByteDance Ltd. and/or its affiliates on
# 2025-11-11.
#
# Original file was released under the Apache License 2.0,
# with the full license text available at:
#     http://www.apache.org/licenses/LICENSE-2.0
#
# This modified file is released under the same license.
# --------------------------------------------------------------------------

import argparse
import multiprocessing
import json
import re
import sys
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List


class Multimap(dict):
    def __setitem__(self, key, value):
        if key not in self:
            dict.__setitem__(self, key, [value])  # call super method to avoid recursion
        else:
            self[key].append(value)


def _truthy_env(name: str) -> bool:
    v = os.environ.get(name)
    if v is None:
        return False
    return v.lower() not in ("", "0", "false", "no")


def get_git_root() -> Optional[str]:
    return _git_stdout(["rev-parse", "--show-toplevel"])


def _run_git(args: List[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git"] + args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="ignore",
    )


def _git_stdout(args: List[str]) -> Optional[str]:
    proc = _run_git(args)
    if proc.returncode != 0:
        return None
    out = (proc.stdout or "").strip()
    return out if out else None


def _git_has_ref(ref: str) -> bool:
    proc = _run_git(["rev-parse", "--verify", "--quiet", ref])
    return proc.returncode == 0


def _git_upstream_ref() -> Optional[str]:
    # Example output: origin/main
    return _git_stdout(
        ["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"]
    )


def _git_merge_base(a: str, b: str) -> Optional[str]:
    return _git_stdout(["merge-base", a, b])


def _git_commits_ahead(base_ref: str, head_ref: str = "HEAD") -> Optional[int]:
    out = _git_stdout(["rev-list", "--count", f"{base_ref}..{head_ref}"])
    if out is None:
        return None
    try:
        return int(out)
    except ValueError:
        return None


def detect_local_base_ref(user_base_ref: Optional[str]) -> Optional[str]:
    """Return a base SHA to diff against for local runs.

    Prefers upstream/remote base, falls back to common branch names.
    Returns the merge-base SHA.
    """
    candidates: List[str] = []
    if user_base_ref:
        candidates.append(user_base_ref)
    else:
        upstream = _git_upstream_ref()
        if upstream:
            candidates.append(upstream)

        # Best-effort fallbacks for repos without upstream tracking.
        for ref in ("origin/main", "main", "origin/master", "master"):
            if _git_has_ref(ref):
                candidates.append(ref)

    for ref in candidates:
        mb = _git_merge_base("HEAD", ref)
        if mb:
            return mb
    return None


def merge_changed_lines(a: Multimap, b: Multimap) -> Multimap:
    out = Multimap()
    for k, ranges in a.items():
        for r in ranges:
            out[k] = r
    for k, ranges in b.items():
        for r in ranges:
            out[k] = r
    return out


def to_repo_rel(path: str, git_root: Optional[str]) -> str:
    """Convert path to repo-relative path when possible."""
    if not git_root:
        return os.path.normpath(path)
    abs_path = os.path.abspath(path) if not os.path.isabs(path) else path
    try:
        rel = os.path.relpath(abs_path, git_root)
        return os.path.normpath(rel)
    except ValueError:
        return os.path.normpath(path)


def to_repo_abs(repo_rel: str, git_root: Optional[str]) -> str:
    if not git_root:
        return os.path.abspath(repo_rel)
    return os.path.normpath(os.path.join(git_root, repo_rel))


def input_files(files):
    if len(files) == 1 and files[0] == "-":
        return [file.strip() for file in sys.stdin.readlines()]
    else:
        return files


def get_all_files(directory, extensions):
    files = []
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            if any(filename.endswith(ext) for ext in extensions):
                files.append(os.path.join(root, filename))
    return files


def git_diff_unified_zero(
    *,
    base_ref: Optional[str],
    head_ref: str = "HEAD",
    staged: bool = False,
) -> str:
    cmd = ["diff", "--text", "--no-color", "--unified=0"]
    if staged:
        cmd.append("--cached")
        if base_ref:
            cmd.append(base_ref)
    else:
        if base_ref:
            cmd.append(f"{base_ref}...{head_ref}")
        else:
            cmd.append(head_ref)

    proc = _run_git(cmd)
    if proc.returncode not in (0, 1):
        # git diff returns 1 for differences in some edge cases; treat non-(0,1) as fatal.
        raise RuntimeError(
            (proc.stderr or "").strip() or f"git diff failed: git {' '.join(cmd)}"
        )
    return proc.stdout or ""


def parse_changed_lines_from_diff(diff_text: str) -> Multimap:
    """Parse `git diff -U0` output and return {file -> [[start,end], ...]} for C/C++ files."""
    cur_file = ""
    changed_lines = Multimap()

    file_re = re.compile(r"^\+\+\+ b/(.*)$")
    cpp_file_re = re.compile(r"^\+\+\+ b/(.*(\.cc|\.cpp|\.cxx|\.c|\.h|\.hpp|\.hxx))$")
    hunk_re = re.compile(r"^@@ .*\+(\d+)(?:,(\d+))? @@")

    for line in diff_text.splitlines():
        line = line.rstrip("\n")

        if file_re.match(line):
            cur_file = ""

        m = cpp_file_re.match(line)
        if m:
            cur_file = m.group(1)
            continue

        if not cur_file:
            continue

        m = hunk_re.match(line)
        if not m:
            continue

        start_line = int(m.group(1))
        count = int(m.group(2)) if m.group(2) is not None else 1
        if count > 0:
            changed_lines[cur_file] = [start_line, start_line + count - 1]

    return changed_lines


def git_changed_lines(*, base_ref: Optional[str], staged: bool) -> Multimap:
    diff_text = git_diff_unified_zero(base_ref=base_ref, staged=staged)
    return parse_changed_lines_from_diff(diff_text)


def get_base_ref_from_github_event() -> Optional[str]:
    """Best-effort base SHA detection for PR, merge queue, and push workflows."""
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    event_name = os.environ.get("GITHUB_EVENT_NAME")
    if not event_path or not os.path.isfile(event_path):
        return None
    try:
        with open(event_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return None

    if event_name in ("pull_request", "pull_request_target", "pull_request_review"):
        return payload.get("pull_request", {}).get("base", {}).get("sha")

    if event_name == "merge_group":
        return payload.get("merge_group", {}).get("base_sha")

    if event_name == "push":
        return payload.get("before")

    return None


def normalize_path(path):
    """Normalize path to be relative to git root or absolute"""
    if os.path.isabs(path):
        return path
    return os.path.normpath(path)


def check_output(output, warnings_as_errors=False):
    """
    Check if the output contains errors (and warnings if strict mode is on).
    Returns True if passed (no blocking issues), False if failed.
    """
    if not output.strip():
        return True

    if warnings_as_errors:
        return re.search(r": (warning|error): ", output) is None

    return re.search(r": error: ", output) is None


def run_clang_tidy_batch(cmd_base, file_batch):
    """
    Worker function to run clang-tidy on a small batch of files.
    """
    full_cmd = cmd_base + file_batch
    try:
        proc = subprocess.run(
            full_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,  # Return strings, not bytes
            encoding="utf-8",
            errors="ignore",
        )
        return proc.returncode, proc.stdout, proc.stderr
    except Exception as e:
        return 1, "", str(e)


def process_gha_output(stdout):
    in_gha = os.environ.get("GITHUB_ACTIONS") is not None
    if in_gha and stdout:
        clang_tidy_pattern = (
            r"^(.*):(\d+):(\d+):\s+(error|warning):\s+(.*) $([a-z0-9,\-]+)$\s*$"
        )
        for stdout_line in stdout.split("\n"):
            m = re.match(clang_tidy_pattern, stdout_line)
            if m:
                file_path, line, col, severity, message, rule = m.groups()
                print(
                    f"::{severity} file={file_path},line={line},col={col},title={rule}::{message}"
                )


def tidy(args):
    extensions = (".cc", ".cpp", ".cxx", ".c", ".h", ".hpp", ".hxx")
    candidate_files = []
    # get file list to check
    if args.directory:
        print(f"Scanning directory: {args.directory} ...")
        candidate_files = get_all_files(args.directory, extensions)
    else:
        candidate_files = input_files(args.files)
        candidate_files = [f for f in candidate_files if f.endswith(extensions)]

    # Normalize candidate files (if provided) for later matching.
    git_root = get_git_root()
    candidate_files = [
        to_repo_rel(normalize_path(f), git_root) for f in candidate_files
    ]

    exclude_re = None
    if args.exclude:
        try:
            exclude_re = re.compile(args.exclude)
            before_count = len(candidate_files)
            # Filter files out if the regex matches anywhere in the path
            candidate_files = [f for f in candidate_files if not exclude_re.search(f)]

            excluded_count = before_count - len(candidate_files)
            if excluded_count > 0:
                print(
                    f"Excluded {excluded_count} files based on pattern '{args.exclude}'"
                )

            # Only early-exit if the caller provided an explicit file list.
            # When running under pre-commit with pass_filenames=false, we expect to
            # discover files via git diff later.
            if before_count > 0 and not candidate_files:
                print("All files were excluded by the filter pattern.")
                return 0

        except re.error as e:
            print(
                f"Error: Invalid regular expression for --exclude: {e}", file=sys.stderr
            )
            return 1

    build_path = args.p or os.getenv("BUILD_PATH", "_build/Release")
    if not build_path:
        if os.path.isfile("compile_commands.json"):
            build_path = "."
        else:
            print("Error: 'compile_commands.json' not found. Set BUILD_PATH or use -p.")
            return 1

    files_to_process = []  # type: List[str]
    line_filter_json = ""

    # Diff mode selection
    diff_mode = args.diff
    base_ref = args.base_ref

    # Backwards-compat: --commit means "diff against base ref" (not working tree).
    if args.commit:
        diff_mode = "base"
        base_ref = args.commit

    if diff_mode == "auto":
        if _truthy_env("GITHUB_ACTIONS") or _truthy_env("IN_CI"):
            base_ref = (
                base_ref
                or os.environ.get("CI_BASE_SHA")
                or get_base_ref_from_github_event()
            )
            diff_mode = "base" if base_ref else "staged"
        else:
            diff_mode = "local"

    if diff_mode == "staged":
        base_ref = base_ref or "HEAD"

    def _compute_changed_lines() -> Optional[Multimap]:
        try:
            if diff_mode == "staged":
                return git_changed_lines(base_ref=base_ref, staged=True)

            if diff_mode == "base":
                if not base_ref:
                    print(
                        "Error: --diff=base requires --base-ref (or CI_BASE_SHA in CI).",
                        file=sys.stderr,
                    )
                    return None
                base_map = git_changed_lines(base_ref=base_ref, staged=False)
                staged_map = git_changed_lines(base_ref="HEAD", staged=True)
                return merge_changed_lines(base_map, staged_map)

            if diff_mode == "local":
                # Local mode: lint all commits from merge-base(base, HEAD) plus any staged changes.
                base_sha = detect_local_base_ref(base_ref)

                # If there is no meaningful base (e.g. directly on main with no commits ahead),
                # fall back to only the last commit.
                commits_ahead = (
                    _git_commits_ahead(base_sha, "HEAD") if base_sha else None
                )
                if base_sha and commits_ahead == 0:
                    base_sha = None

                if not base_sha:
                    if _git_has_ref("HEAD^"):
                        base_sha = "HEAD^"

                base_map = (
                    git_changed_lines(base_ref=base_sha, staged=False)
                    if base_sha
                    else Multimap()
                )
                staged_map = git_changed_lines(base_ref="HEAD", staged=True)
                return merge_changed_lines(base_map, staged_map)

            if diff_mode == "none":
                return None

            print(f"Error: unknown diff mode '{diff_mode}'", file=sys.stderr)
            return None
        except Exception as e:
            print(f"Error computing changed lines via git diff: {e}", file=sys.stderr)
            return None

    changed_lines = _compute_changed_lines()
    if diff_mode in ("staged", "base", "local"):
        if changed_lines is None:
            return 1

        # If candidate files were provided, intersect them with diff.
        if candidate_files:
            for f in candidate_files:
                if f in changed_lines:
                    files_to_process.append(f)
        else:
            files_to_process = list(changed_lines.keys())

        if exclude_re is not None:
            files_to_process = [f for f in files_to_process if not exclude_re.search(f)]

        if not files_to_process:
            print("No changed C/C++ lines detected for clang-tidy.")
            return 0

        # Use absolute paths in --line-filter for better compatibility with compile DBs.
        final_map_abs = {}  # type: Dict[str, List[List[int]]]
        for f in files_to_process:
            abs_f = to_repo_abs(f, git_root)
            final_map_abs[abs_f] = changed_lines[f]
        line_filter_json = json.dumps(
            [{"name": key, "lines": value} for key, value in final_map_abs.items()]
        )

        # Also invoke clang-tidy with absolute paths to match compile_commands.json.
        files_to_process = [to_repo_abs(f, git_root) for f in files_to_process]
    else:
        # No diff mode: run on the provided candidate files.
        if not candidate_files:
            print("No valid C/C++ files found to check.")
            return 0
        files_to_process = [to_repo_abs(f, git_root) for f in candidate_files]
        line_filter_json = ""

    clang_tidy_bin = args.clang_tidy_binary

    cmd_base = [clang_tidy_bin]

    if args.config_file:
        cmd_base.append(f"--config-file={args.config_file}")
    else:
        cmd_base.append("--format-style=file")

    cmd_base.append(f"-p={build_path}")

    if args.fix:
        cmd_base.append("--fix")

    cmd_base.append("--quiet")

    if line_filter_json:
        cmd_base.append(f"--line-filter={line_filter_json}")

    cmd_base.append("--extra-arg=-Wno-unknown-warning-option")

    jobs = args.jobs if args.jobs else max(1, multiprocessing.cpu_count() // 2)
    chunk_size = 10
    file_chunks = [
        files_to_process[i : i + chunk_size]
        for i in range(0, len(files_to_process), chunk_size)
    ]
    print(
        f"Running {clang_tidy_bin} on {len(files_to_process)} files with {jobs} jobs (Batch size: {chunk_size})..."
    )
    final_exit_code = 0

    with ThreadPoolExecutor(max_workers=jobs) as executor:
        future_to_chunk = {
            executor.submit(run_clang_tidy_batch, cmd_base, chunk): chunk
            for chunk in file_chunks
        }

        for future in as_completed(future_to_chunk):
            code, stdout, stderr = future.result()
            is_failed = False

            if code != 0:
                is_failed = True
            if not check_output(stdout, args.warnings_as_errors):
                is_failed = True
            if is_failed:
                final_exit_code = 1

            if stdout.strip():
                process_gha_output(stdout)
                print(stdout, end="" if stdout.endswith("\n") else "\n")

            if stderr.strip():
                print(stderr, file=sys.stderr)
            if is_failed and args.fail_fast:
                print(
                    "\n[Fail Fast] Error detected. Stopping remaining tasks...",
                    file=sys.stderr,
                )

                try:
                    executor.shutdown(wait=False, cancel_futures=True)
                except TypeError:
                    executor.shutdown(wait=False)
                return 1

    return final_exit_code


def parse_args():
    parser = argparse.ArgumentParser(description="Clang Tidy Wrapper Script")

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--commit",
        help="Base git ref/SHA to diff against (legacy; prefer --diff=base --base-ref)",
    )
    group.add_argument(
        "--directory", "-d", help="Run recursively on all C/C++ files in this directory"
    )

    parser.add_argument(
        "--diff",
        choices=["auto", "local", "staged", "base", "none"],
        default="auto",
        help=(
            "Diff mode: auto (GitHub base in CI, local base + staged locally), "
            "local (merge-base to base branch + staged), staged (git diff --cached), "
            "base (git diff <base>...HEAD + staged), none (run on given files)."
        ),
    )
    parser.add_argument(
        "--base-ref",
        help=(
            "Base git ref/SHA to use with --diff=base. In GitHub Actions you can also set CI_BASE_SHA."
        ),
    )

    parser.add_argument("--fix", action="store_true", help="Automatically apply fixes")
    parser.add_argument(
        "-p", help="Path containing 'compile_commands.json' (build directory)"
    )
    parser.add_argument(
        "--config-file", help="Path to specific .clang-tidy configuration file"
    )
    parser.add_argument(
        "--exclude",
        default="_build/|tests/|.*Test\.cpp$|benchmark/|benchmarks/|.*Benchmark\.cpp$|.*Benchmarks\.cpp$|test/|.*\.pb\.cc$|.*\.pb\.h$",
        help="Regular expression to exclude files or paths (e.g. 'tests/|.*Test\.cpp$')",
    )
    parser.add_argument(
        "files",
        metavar="FILES",
        nargs="*",
        help="Specific files to process (space separated)",
    )

    parser.add_argument("-j", "--jobs", type=int, help="Parallel jobs")

    parser.add_argument(
        "--clang-tidy-binary",
        default="clang-tidy",
        help="Path to clang-tidy binary (default: clang-tidy)",
    )

    parser.add_argument(
        "--warnings-as-errors",
        action="store_true",
        help="Treat warnings as errors and exit with non-zero status",
    )

    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop processing immediately after the first error is found",
    )

    return parser.parse_args()


def main():
    try:
        sys.exit(tidy(parse_args()))
    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
