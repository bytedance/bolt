#!/usr/bin/env python3
"""Helpers for merge-queue CI reuse and stale-run reconciliation."""

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


API_VERSION = "2022-11-28"
ARTIFACT_PREFIX = "bolt-ci-result-v1"
REUSABLE_EVENTS = {"pull_request", "merge_group"}
ACTIVE_RUN_STATUSES = ("queued", "in_progress")
SHA_RE = re.compile(r"^[0-9a-f]{40,64}$")


class GitHubApiError(RuntimeError):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


class GitHubApi:
    def __init__(self, token, base_url=None):
        if not token:
            raise ValueError("A GitHub token is required")
        self.token = token
        self.base_url = (
            base_url or os.environ.get("GITHUB_API_URL") or "https://api.github.com"
        ).rstrip("/")

    def request(self, path, method="GET", payload=None):
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "X-GitHub-Api-Version": API_VERSION,
                "User-Agent": "bolt-merge-queue-ci",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read()
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            raise GitHubApiError(
                error.code,
                f"GitHub API {method} {path} failed with HTTP {error.code}: {body[:500]}",
            ) from error
        except urllib.error.URLError as error:
            raise GitHubApiError(
                None,
                f"GitHub API {method} {path} failed: {error.reason}",
            ) from error

        return json.loads(body) if body else None


def parse_github_time(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def utc_now():
    return dt.datetime.now(dt.timezone.utc)


def validate_repository(repository):
    if (
        not repository
        or repository.count("/") != 1
        or any(not part for part in repository.split("/"))
    ):
        raise ValueError(f"Invalid GitHub repository: {repository!r}")
    return repository


def validate_sha(value):
    if not SHA_RE.fullmatch(value):
        raise ValueError(f"Invalid Git SHA: {value!r}")
    return value


def append_github_output(path, values):
    if not path:
        return
    with open(path, "a", encoding="utf-8") as output:
        for key, value in values.items():
            output.write(f"{key}={value}\n")


def git_tree_sha():
    result = subprocess.run(
        ["git", "rev-parse", "HEAD^{tree}"],
        check=True,
        capture_output=True,
        text=True,
    )
    return validate_sha(result.stdout.strip())


def make_fingerprint(tree_sha, salt):
    tree_sha = validate_sha(tree_sha)
    config_hash = hashlib.sha256(
        f"{ARTIFACT_PREFIX}\0{salt}".encode("utf-8")
    ).hexdigest()[:20]
    return {
        "tree_sha": tree_sha,
        "config_hash": config_hash,
        "artifact_name": f"{ARTIFACT_PREFIX}-{tree_sha}-{config_hash}",
    }


def normalize_workflow_path(value):
    return (value or "").split("@", 1)[0]


def list_named_artifacts(api, repository, artifact_name):
    artifacts = []
    encoded_name = urllib.parse.quote(artifact_name, safe="")
    for page in range(1, 101):
        response = api.request(
            f"/repos/{repository}/actions/artifacts"
            f"?name={encoded_name}&per_page=100&page={page}"
        )
        batch = response.get("artifacts", [])
        artifacts.extend(batch)
        if len(batch) < 100:
            break
    return artifacts


def find_reusable_result(
    api,
    repository,
    artifact_name,
    workflow_path,
    max_age_hours,
    current_run_id=None,
    now=None,
):
    repository = validate_repository(repository)
    now = now or utc_now()
    cutoff = now - dt.timedelta(hours=max_age_hours)
    artifacts = sorted(
        list_named_artifacts(api, repository, artifact_name),
        key=lambda item: item.get("created_at", ""),
        reverse=True,
    )

    for artifact in artifacts:
        if artifact.get("expired"):
            continue
        created_at = parse_github_time(artifact["created_at"])
        if created_at < cutoff:
            continue
        source = artifact.get("workflow_run") or {}
        run_id = source.get("id")
        if not run_id or str(run_id) == str(current_run_id):
            continue

        run = api.request(f"/repos/{repository}/actions/runs/{run_id}")
        if run.get("status") != "completed" or run.get("conclusion") != "success":
            continue
        if run.get("event") not in REUSABLE_EVENTS:
            continue
        if normalize_workflow_path(run.get("path")) != normalize_workflow_path(
            workflow_path
        ):
            continue
        run_repository = (run.get("repository") or {}).get("full_name", "")
        if run_repository.lower() != repository.lower():
            continue

        return {
            "hit": True,
            "artifact_id": artifact["id"],
            "source_run_id": run_id,
            "source_run_url": run.get("html_url", ""),
            "created_at": artifact["created_at"],
        }

    return {
        "hit": False,
        "artifact_id": "",
        "source_run_id": "",
        "source_run_url": "",
        "created_at": "",
    }


def list_workflow_runs(api, repository, workflow, head_sha, event):
    workflow = urllib.parse.quote(workflow, safe="")
    query = urllib.parse.urlencode(
        {
            "head_sha": head_sha,
            "event": event,
            "per_page": 100,
        }
    )
    response = api.request(
        f"/repos/{repository}/actions/workflows/{workflow}/runs?{query}"
    )
    return response.get("workflow_runs", [])


def select_event_run(runs, gate_created_at, window_seconds):
    lower_bound = gate_created_at - dt.timedelta(seconds=window_seconds)
    upper_bound = gate_created_at + dt.timedelta(seconds=window_seconds)
    candidates = [
        run
        for run in runs
        if lower_bound <= parse_github_time(run["created_at"]) <= upper_bound
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda run: (
            parse_github_time(run["created_at"]),
            run.get("run_attempt", 1),
            run["id"],
        ),
    )


def wait_for_workflows(
    api,
    repository,
    workflows,
    head_sha,
    event,
    gate_run_id,
    timeout_seconds,
    poll_seconds,
    run_window_seconds,
    sleep=time.sleep,
):
    repository = validate_repository(repository)
    validate_sha(head_sha)
    gate_run = api.request(f"/repos/{repository}/actions/runs/{gate_run_id}")
    gate_created_at = parse_github_time(gate_run["created_at"])
    deadline = time.monotonic() + timeout_seconds
    last_state = None

    while True:
        states = {}
        for workflow in workflows:
            runs = list_workflow_runs(api, repository, workflow, head_sha, event)
            selected = select_event_run(runs, gate_created_at, run_window_seconds)
            if selected is None:
                states[workflow] = ("missing", None)
            else:
                states[workflow] = (
                    selected.get("status", "unknown"),
                    selected.get("conclusion"),
                )

        if states != last_state:
            print(json.dumps({"workflow_states": states}, sort_keys=True))
            last_state = states

        failures = {
            workflow: conclusion
            for workflow, (status, conclusion) in states.items()
            if status == "completed" and conclusion != "success"
        }
        if failures:
            raise RuntimeError(f"Required CI workflow failed: {failures}")

        if all(
            status == "completed" and conclusion == "success"
            for status, conclusion in states.values()
        ):
            return states

        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Timed out waiting for CI workflows after {timeout_seconds}s: {states}"
            )
        sleep(poll_seconds)


def list_live_merge_queue_shas(api, repository, base_branch):
    ref_prefix = f"refs/heads/gh-readonly-queue/{base_branch}/"
    encoded = urllib.parse.quote(
        f"heads/gh-readonly-queue/{base_branch}/",
        safe="/",
    )
    refs = api.request(f"/repos/{repository}/git/matching-refs/{encoded}")
    if not isinstance(refs, list):
        raise RuntimeError("GitHub matching-refs response was not a list")
    return {
        ref["object"]["sha"]
        for ref in refs
        if ref.get("ref", "").startswith(ref_prefix)
        and (ref.get("object") or {}).get("type") == "commit"
    }


def list_active_merge_group_runs(api, repository):
    runs_by_id = {}
    for status in ACTIVE_RUN_STATUSES:
        for page in range(1, 101):
            response = api.request(
                f"/repos/{repository}/actions/runs"
                f"?event=merge_group&status={status}&per_page=100&page={page}"
            )
            batch = response.get("workflow_runs", [])
            for run in batch:
                runs_by_id[run["id"]] = run
            if len(batch) < 100:
                break
    return list(runs_by_id.values())


def find_stale_runs(runs, live_shas, base_branch, grace_seconds, now=None):
    now = now or utc_now()
    branch_prefix = f"gh-readonly-queue/{base_branch}/"
    stale = []
    for run in runs:
        if run.get("event") != "merge_group":
            continue
        if run.get("status") not in ACTIVE_RUN_STATUSES:
            continue
        if not run.get("head_branch", "").startswith(branch_prefix):
            continue
        if run.get("head_sha") in live_shas:
            continue
        age = (now - parse_github_time(run["created_at"])).total_seconds()
        if age < grace_seconds:
            continue
        stale.append(run)
    return sorted(stale, key=lambda run: run["created_at"])


def reconcile_stale_runs(
    api,
    repository,
    base_branch,
    grace_seconds,
    mode,
    now=None,
):
    live_shas = list_live_merge_queue_shas(api, repository, base_branch)
    active_runs = list_active_merge_group_runs(api, repository)
    stale_runs = find_stale_runs(
        active_runs,
        live_shas,
        base_branch,
        grace_seconds,
        now=now,
    )
    cancelled = []
    raced = []

    for run in stale_runs:
        print(
            f"{'CANCEL' if mode == 'cancel' else 'STALE'} "
            f"run={run['id']} sha={run['head_sha']} "
            f"workflow={run.get('name', '')} url={run.get('html_url', '')}"
        )
        if mode != "cancel":
            continue
        try:
            api.request(
                f"/repos/{repository}/actions/runs/{run['id']}/cancel",
                method="POST",
            )
            cancelled.append(run["id"])
        except GitHubApiError as error:
            if error.status == 409:
                raced.append(run["id"])
                print(
                    f"::notice::Run {run['id']} finished before cancellation",
                    file=sys.stderr,
                )
                continue
            raise

    return {
        "mode": mode,
        "live_queue_shas": len(live_shas),
        "active_merge_group_runs": len(active_runs),
        "stale_runs": [run["id"] for run in stale_runs],
        "cancelled_runs": cancelled,
        "already_finished_runs": raced,
    }


def write_step_summary(path, summary):
    if not path:
        return
    with open(path, "a", encoding="utf-8") as output:
        output.write("## Merge Queue cleanup\n\n")
        for key, value in summary.items():
            output.write(f"- **{key.replace('_', ' ')}:** {value}\n")


def command_fingerprint(args):
    result = make_fingerprint(git_tree_sha(), args.salt)
    append_github_output(args.github_output, result)
    print(json.dumps(result, sort_keys=True))
    return 0


def command_find_reuse(args):
    miss = {
        "hit": "false",
        "source_run_id": "",
        "source_run_url": "",
    }
    if args.event != "merge_group":
        append_github_output(args.github_output, miss)
        print(json.dumps(miss, sort_keys=True))
        return 0

    try:
        api = GitHubApi(args.token)
        result = find_reusable_result(
            api,
            validate_repository(args.repository),
            args.artifact_name,
            args.workflow_path,
            args.max_age_hours,
            current_run_id=args.current_run_id,
        )
    except (GitHubApiError, KeyError, OSError, TypeError, ValueError) as error:
        print(
            f"::warning::CI reuse lookup failed; running full CI instead: {error}",
            file=sys.stderr,
        )
        result = miss

    output = {
        "hit": str(result["hit"]).lower(),
        "source_run_id": result.get("source_run_id", ""),
        "source_run_url": result.get("source_run_url", ""),
    }
    append_github_output(args.github_output, output)
    print(json.dumps(output, sort_keys=True))
    return 0


def command_wait(args):
    api = GitHubApi(args.token)
    states = wait_for_workflows(
        api,
        validate_repository(args.repository),
        args.workflow,
        validate_sha(args.head_sha),
        args.event,
        args.gate_run_id,
        args.timeout_seconds,
        args.poll_seconds,
        args.run_window_seconds,
    )
    print(json.dumps({"completed": states}, sort_keys=True))
    return 0


def command_record(args):
    payload = {
        "artifact_name": args.artifact_name,
        "tree_sha": validate_sha(args.tree_sha),
        "config_hash": args.config_hash,
        "run_id": args.run_id,
        "run_url": args.run_url,
        "created_at": utc_now().isoformat(),
    }
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(path)
    return 0


def command_cleanup(args):
    api = GitHubApi(args.token)
    summary = reconcile_stale_runs(
        api,
        validate_repository(args.repository),
        args.base_branch,
        args.grace_seconds,
        args.mode,
    )
    write_step_summary(args.step_summary, summary)
    print(json.dumps(summary, sort_keys=True))
    return 0


def build_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    fingerprint = subparsers.add_parser("fingerprint")
    fingerprint.add_argument("--salt", required=True)
    fingerprint.add_argument("--github-output")
    fingerprint.set_defaults(func=command_fingerprint)

    reuse = subparsers.add_parser("find-reuse")
    reuse.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    reuse.add_argument("--token", default=os.environ.get("GITHUB_TOKEN"))
    reuse.add_argument("--event", required=True)
    reuse.add_argument("--artifact-name", required=True)
    reuse.add_argument(
        "--workflow-path", default=".github/workflows/merge-queue-gate.yml"
    )
    reuse.add_argument("--max-age-hours", type=int, default=24)
    reuse.add_argument("--current-run-id", default=os.environ.get("GITHUB_RUN_ID"))
    reuse.add_argument("--github-output")
    reuse.set_defaults(func=command_find_reuse)

    wait = subparsers.add_parser("wait-for-workflows")
    wait.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    wait.add_argument("--token", default=os.environ.get("GITHUB_TOKEN"))
    wait.add_argument("--workflow", action="append", required=True)
    wait.add_argument("--head-sha", required=True)
    wait.add_argument("--event", required=True)
    wait.add_argument("--gate-run-id", default=os.environ.get("GITHUB_RUN_ID"))
    wait.add_argument("--timeout-seconds", type=int, default=5100)
    wait.add_argument("--poll-seconds", type=int, default=15)
    wait.add_argument("--run-window-seconds", type=int, default=900)
    wait.set_defaults(func=command_wait)

    record = subparsers.add_parser("record")
    record.add_argument("--artifact-name", required=True)
    record.add_argument("--tree-sha", required=True)
    record.add_argument("--config-hash", required=True)
    record.add_argument("--run-id", required=True)
    record.add_argument("--run-url", required=True)
    record.add_argument("--output", required=True)
    record.set_defaults(func=command_record)

    cleanup = subparsers.add_parser("cleanup")
    cleanup.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    cleanup.add_argument("--token", default=os.environ.get("GITHUB_TOKEN"))
    cleanup.add_argument("--base-branch", default="main")
    cleanup.add_argument("--grace-seconds", type=int, default=600)
    cleanup.add_argument("--mode", choices=("report", "cancel"), default="report")
    cleanup.add_argument(
        "--step-summary", default=os.environ.get("GITHUB_STEP_SUMMARY")
    )
    cleanup.set_defaults(func=command_cleanup)

    return parser


def main():
    args = build_parser().parse_args()
    try:
        return args.func(args)
    except (GitHubApiError, RuntimeError, TimeoutError, ValueError) as error:
        print(f"::error::{error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
