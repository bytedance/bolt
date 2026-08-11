#!/usr/bin/env python3
"""Helpers for merge-queue CI reuse, gating, and stale-run cleanup."""

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


API_VERSION = "2022-11-28"
RESULT_ARTIFACT_PREFIX = "bolt-ci-result-v2"
SOURCE_ARTIFACT_PREFIX = "bolt-ci-source-v1"
GATE_CHECK_NAME = "merge-queue-gate"
GATE_WORKFLOW_PATH = ".github/workflows/merge-queue-gate.yml"
REQUIRED_WORKFLOWS = ("build-test.yml", "pre-commit-checks.yml")
REUSABLE_EVENTS = {"pull_request", "merge_group"}
ACTIVE_RUN_STATUSES = (
    "queued",
    "in_progress",
    "waiting",
    "pending",
    "requested",
)
SHA_RE = re.compile(r"^[0-9a-f]{40,64}$")
CONFIG_HASH_RE = re.compile(r"^[0-9a-f]{20}$")


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
                f"GitHub API {method} {path} failed with HTTP {error.code}: "
                f"{body[:500]}",
            ) from error
        except urllib.error.URLError as error:
            raise GitHubApiError(
                None,
                f"GitHub API {method} {path} failed: {error.reason}",
            ) from error

        return json.loads(body) if body else None


def parse_github_time(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def github_time(value=None):
    value = value or dt.datetime.now(dt.timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


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


def validate_sha(value, field="Git SHA"):
    if not SHA_RE.fullmatch(value or ""):
        raise ValueError(f"Invalid {field}: {value!r}")
    return value


def validate_config_hash(value):
    if not CONFIG_HASH_RE.fullmatch(value or ""):
        raise ValueError(f"Invalid CI configuration hash: {value!r}")
    return value


def append_github_output(path, values):
    if not path:
        return
    with open(path, "a", encoding="utf-8") as output:
        for key, value in values.items():
            output.write(f"{key}={value}\n")


def write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def git_tree_sha():
    result = subprocess.run(
        ["git", "rev-parse", "HEAD^{tree}"],
        check=True,
        capture_output=True,
        text=True,
    )
    return validate_sha(result.stdout.strip(), "tree SHA")


def config_hash(salt):
    return hashlib.sha256(
        f"{RESULT_ARTIFACT_PREFIX}\0{salt}".encode("utf-8")
    ).hexdigest()[:20]


def make_fingerprint(tree_sha, base_sha, salt):
    tree_sha = validate_sha(tree_sha, "tree SHA")
    base_sha = validate_sha(base_sha, "base SHA")
    config = config_hash(salt)
    return {
        "tree_sha": tree_sha,
        "base_sha": base_sha,
        "config_hash": config,
        "artifact_name": (f"{RESULT_ARTIFACT_PREFIX}-{tree_sha}-{base_sha}-{config}"),
    }


def normalize_workflow_path(value):
    return (value or "").split("@", 1)[0]


def workflow_filename(value):
    return normalize_workflow_path(value).rsplit("/", 1)[-1]


def workflow_key(value):
    filename = workflow_filename(value)
    if filename == "build-test.yml":
        return "build-test"
    if filename == "pre-commit-checks.yml":
        return "pre-commit"
    raise ValueError(f"Unsupported CI workflow: {value!r}")


def source_artifact_name(workflow, run_id, run_attempt, merge_sha, config):
    return (
        f"{SOURCE_ARTIFACT_PREFIX}-{workflow_key(workflow)}-{run_id}-"
        f"{run_attempt}-{validate_sha(merge_sha, 'merge SHA')}-"
        f"{validate_config_hash(config)}"
    )


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


def list_run_artifacts(api, repository, run_id):
    artifacts = []
    for page in range(1, 101):
        response = api.request(
            f"/repos/{repository}/actions/runs/{run_id}/artifacts"
            f"?per_page=100&page={page}"
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
        if artifact.get("expired") or artifact.get("name") != artifact_name:
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
        if run.get("event") != "workflow_run":
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


def select_event_run(runs, source_created_at, window_seconds):
    lower_bound = source_created_at - dt.timedelta(seconds=window_seconds)
    upper_bound = source_created_at + dt.timedelta(seconds=window_seconds)
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


def correlated_ci_runs(
    api,
    repository,
    source_run,
    workflows=REQUIRED_WORKFLOWS,
    window_seconds=900,
):
    head_sha = validate_sha(source_run.get("head_sha"), "head SHA")
    event = source_run.get("event")
    if event not in REUSABLE_EVENTS:
        raise ValueError(f"Unsupported source event: {event!r}")
    source_created_at = parse_github_time(source_run["created_at"])
    correlated = {}
    for workflow in workflows:
        runs = list_workflow_runs(api, repository, workflow, head_sha, event)
        correlated[workflow] = select_event_run(
            runs,
            source_created_at,
            window_seconds,
        )
    return correlated


def check_state(runs):
    present = [run for run in runs.values() if run]
    if any(
        run.get("status") == "completed" and run.get("conclusion") != "success"
        for run in present
    ):
        return "completed", "failure"
    if len(present) == len(runs) and all(
        run.get("status") == "completed" and run.get("conclusion") == "success"
        for run in present
    ):
        return "completed", "success"
    return "in_progress", None


def check_summary(runs, validation_error=None):
    lines = []
    for workflow, run in runs.items():
        if not run:
            lines.append(f"- `{workflow}`: missing")
            continue
        state = run.get("conclusion") or run.get("status") or "unknown"
        url = run.get("html_url", "")
        lines.append(f"- [`{workflow}`]({url}): {state}")
    if validation_error:
        lines.extend(("", f"CI evidence rejected: `{validation_error}`"))
    return "\n".join(lines)


def upsert_gate_check(
    api,
    repository,
    head_sha,
    event,
    runs,
    status,
    conclusion,
    validation_error=None,
):
    external_id = f"bolt-merge-queue-gate:{event}:{head_sha}"
    query = urllib.parse.urlencode({"check_name": GATE_CHECK_NAME, "per_page": 100})
    response = api.request(f"/repos/{repository}/commits/{head_sha}/check-runs?{query}")
    existing = next(
        (
            check
            for check in response.get("check_runs", [])
            if check.get("external_id") == external_id
        ),
        None,
    )
    details_url = next(
        (
            run.get("html_url", "")
            for run in runs.values()
            if run and run.get("html_url")
        ),
        "",
    )
    payload = {
        "name": GATE_CHECK_NAME,
        "status": status,
        "external_id": external_id,
        "details_url": details_url,
        "output": {
            "title": (
                "CI evidence rejected"
                if validation_error
                else "Required CI passed"
                if conclusion == "success"
                else "Required CI failed"
                if conclusion == "failure"
                else "Waiting for required CI"
            ),
            "summary": check_summary(runs, validation_error),
        },
    }
    if status == "completed":
        payload["conclusion"] = conclusion
        payload["completed_at"] = github_time()

    if existing:
        return api.request(
            f"/repos/{repository}/check-runs/{existing['id']}",
            method="PATCH",
            payload=payload,
        )

    payload["head_sha"] = head_sha
    if status == "in_progress":
        payload["started_at"] = github_time()
    return api.request(
        f"/repos/{repository}/check-runs",
        method="POST",
        payload=payload,
    )


def find_source_evidence(api, repository, run, expected_config):
    workflow = workflow_filename(run.get("path"))
    prefix = (
        f"{SOURCE_ARTIFACT_PREFIX}-{workflow_key(workflow)}-{run['id']}-"
        f"{run.get('run_attempt', 1)}-"
    )
    matches = [
        artifact
        for artifact in list_run_artifacts(api, repository, run["id"])
        if not artifact.get("expired") and artifact.get("name", "").startswith(prefix)
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected one source evidence artifact for run {run['id']}, "
            f"found {len(matches)}"
        )
    suffix = matches[0]["name"][len(prefix) :]
    try:
        merge_sha, evidence_config = suffix.split("-", 1)
    except ValueError as error:
        raise RuntimeError(
            f"Malformed source evidence artifact: {matches[0]['name']}"
        ) from error
    validate_sha(merge_sha, "merge SHA")
    if evidence_config != expected_config:
        raise RuntimeError(
            f"CI configuration mismatch for run {run['id']}: "
            f"{evidence_config} != {expected_config}"
        )
    validate_config_hash(evidence_config)
    return {
        "artifact_id": matches[0]["id"],
        "merge_sha": merge_sha,
        "config_hash": evidence_config,
    }


def reusable_pr_result(api, repository, runs, salt):
    expected_config = config_hash(salt)
    evidences = {
        workflow: find_source_evidence(api, repository, run, expected_config)
        for workflow, run in runs.items()
    }
    merge_shas = {evidence["merge_sha"] for evidence in evidences.values()}
    if len(merge_shas) != 1:
        raise RuntimeError(f"CI workflows tested different merge commits: {merge_shas}")
    merge_sha = merge_shas.pop()
    commit = api.request(f"/repos/{repository}/git/commits/{merge_sha}")
    parents = [parent["sha"] for parent in commit.get("parents", [])]
    head_shas = {run.get("head_sha") for run in runs.values()}
    if len(head_shas) != 1:
        raise RuntimeError(f"CI workflows have different head SHAs: {head_shas}")
    head_sha = validate_sha(head_shas.pop(), "head SHA")
    if len(parents) != 2 or parents[1] != head_sha:
        raise RuntimeError(
            f"Merge commit {merge_sha} does not have expected PR head {head_sha}"
        )
    base_sha = validate_sha(parents[0], "base SHA")
    tree_sha = validate_sha((commit.get("tree") or {}).get("sha"), "tree SHA")
    result = make_fingerprint(tree_sha, base_sha, salt)
    result.update(
        {
            "event": "pull_request",
            "head_sha": head_sha,
            "merge_sha": merge_sha,
            "source_runs": {workflow: run["id"] for workflow, run in runs.items()},
            "created_at": github_time(),
        }
    )
    return result


def reconcile_gate(
    api,
    repository,
    source_run_id,
    salt,
    output,
    github_output=None,
):
    repository = validate_repository(repository)
    source_run = api.request(f"/repos/{repository}/actions/runs/{source_run_id}")
    if workflow_filename(source_run.get("path")) not in REQUIRED_WORKFLOWS:
        raise ValueError(f"Run {source_run_id} is not from a required CI workflow")
    runs = correlated_ci_runs(api, repository, source_run)
    status, conclusion = check_state(runs)
    head_sha = validate_sha(source_run.get("head_sha"), "head SHA")
    event = source_run.get("event")
    result = None
    validation_error = None
    if conclusion == "success" and event == "pull_request":
        try:
            result = reusable_pr_result(api, repository, runs, salt)
        except GitHubApiError:
            raise
        except (KeyError, RuntimeError, TypeError, ValueError) as error:
            status = "completed"
            conclusion = "failure"
            validation_error = str(error)

    check = upsert_gate_check(
        api,
        repository,
        head_sha,
        event,
        runs,
        status,
        conclusion,
        validation_error,
    )

    values = {
        "status": status,
        "conclusion": conclusion or "",
        "reusable": "false",
        "artifact_name": "",
        "check_url": check.get("html_url", ""),
    }
    if result:
        path = write_json(output, result)
        values["reusable"] = "true"
        values["artifact_name"] = result["artifact_name"]
        values["result_path"] = str(path)

    append_github_output(github_output, values)
    print(
        json.dumps(
            {
                "gate": values,
                "workflow_states": {
                    workflow: (
                        None
                        if run is None
                        else {
                            "id": run["id"],
                            "status": run.get("status"),
                            "conclusion": run.get("conclusion"),
                        }
                    )
                    for workflow, run in runs.items()
                },
            },
            sort_keys=True,
        )
    )
    return values


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
    result = make_fingerprint(git_tree_sha(), args.base_sha, args.salt)
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
        fingerprint = make_fingerprint(args.tree_sha, args.base_sha, args.salt)
        api = GitHubApi(args.token)
        result = find_reusable_result(
            api,
            validate_repository(args.repository),
            fingerprint["artifact_name"],
            args.workflow_path,
            args.max_age_hours,
            current_run_id=args.current_run_id,
        )
    except (
        GitHubApiError,
        KeyError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
    ) as error:
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


def command_record_source(args):
    payload = {
        "schema_version": 1,
        "workflow": workflow_filename(args.workflow),
        "event": args.event,
        "source_run_id": args.run_id,
        "source_run_attempt": args.run_attempt,
        "merge_sha": validate_sha(args.merge_sha, "merge SHA"),
        "tree_sha": validate_sha(args.tree_sha, "tree SHA"),
        "base_sha": validate_sha(args.base_sha, "base SHA"),
        "config_hash": validate_config_hash(args.config_hash),
        "created_at": github_time(),
    }
    artifact_name = source_artifact_name(
        args.workflow,
        args.run_id,
        args.run_attempt,
        args.merge_sha,
        args.config_hash,
    )
    path = write_json(args.output, payload)
    append_github_output(
        args.github_output,
        {"artifact_name": artifact_name, "result_path": str(path)},
    )
    print(json.dumps({"artifact_name": artifact_name, **payload}, sort_keys=True))
    return 0


def command_reconcile_gate(args):
    api = GitHubApi(args.token)
    reconcile_gate(
        api,
        validate_repository(args.repository),
        args.source_run_id,
        args.salt,
        args.output,
        github_output=args.github_output,
    )
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
    fingerprint.add_argument("--base-sha", required=True)
    fingerprint.add_argument("--github-output")
    fingerprint.set_defaults(func=command_fingerprint)

    reuse = subparsers.add_parser("find-reuse")
    reuse.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    reuse.add_argument("--token", default=os.environ.get("GITHUB_TOKEN"))
    reuse.add_argument("--event", required=True)
    reuse.add_argument("--tree-sha", required=True)
    reuse.add_argument("--base-sha", required=True)
    reuse.add_argument("--salt", required=True)
    reuse.add_argument("--workflow-path", default=GATE_WORKFLOW_PATH)
    reuse.add_argument("--max-age-hours", type=int, default=24)
    reuse.add_argument("--current-run-id", default=os.environ.get("GITHUB_RUN_ID"))
    reuse.add_argument("--github-output")
    reuse.set_defaults(func=command_find_reuse)

    source = subparsers.add_parser("record-source")
    source.add_argument("--workflow", required=True)
    source.add_argument("--event", required=True, choices=sorted(REUSABLE_EVENTS))
    source.add_argument("--run-id", required=True)
    source.add_argument("--run-attempt", required=True, type=int)
    source.add_argument("--merge-sha", required=True)
    source.add_argument("--tree-sha", required=True)
    source.add_argument("--base-sha", required=True)
    source.add_argument("--config-hash", required=True)
    source.add_argument("--output", required=True)
    source.add_argument("--github-output")
    source.set_defaults(func=command_record_source)

    gate = subparsers.add_parser("reconcile-gate")
    gate.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    gate.add_argument("--token", default=os.environ.get("GITHUB_TOKEN"))
    gate.add_argument("--source-run-id", required=True)
    gate.add_argument("--salt", required=True)
    gate.add_argument("--output", required=True)
    gate.add_argument("--github-output")
    gate.set_defaults(func=command_reconcile_gate)

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
    except (
        GitHubApiError,
        KeyError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
    ) as error:
        print(f"::error::{error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
