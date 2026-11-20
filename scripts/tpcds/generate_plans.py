#!/usr/bin/env python3
# Copyright (c) ByteDance Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate benchmark-ready Bolt TPC-DS plans through a Presto native worker.

The native worker must be configured to dump each received single-node bolt
plan as a JSON file. This tool submits the SQL files one at a time, detects the
new dump, validates its shape, writes it under the SQL stem, and cancels the
query as soon as the plan has been captured.
"""

import argparse
import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


VARIANT_QUERY_NUMBERS = {14, 23, 24, 39}
QUERY_NAMES = tuple(
    query_name
    for query_number in range(1, 100)
    for query_name in (
        (f"q{query_number}a", f"q{query_number}b")
        if query_number in VARIANT_QUERY_NUMBERS
        else (f"q{query_number}",)
    )
)

FileState = Tuple[int, int]
Snapshot = Dict[Path, FileState]


class PlanGenerationError(RuntimeError):
    """Reports a query submission or plan dump failure."""


def normalize_sql(sql: str) -> str:
    """Removes the statement terminator rejected by the HTTP query API."""
    sql = sql.strip()
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()
    if not sql:
        raise PlanGenerationError("SQL file is empty")
    return sql


def find_query_files(
    query_directory: Path, selected_queries: Sequence[str]
) -> List[Tuple[str, Path]]:
    """Finds and validates the requested TPC-DS SQL files."""
    if not query_directory.is_dir():
        raise PlanGenerationError(
            f"TPC-DS query directory does not exist: {query_directory}"
        )

    files: Dict[str, Path] = {}
    for path in query_directory.iterdir():
        if not path.is_file() or path.suffix.lower() != ".sql":
            continue
        query_name = path.stem.lower()
        if query_name in files:
            raise PlanGenerationError(
                f"Duplicate TPC-DS query name {query_name}: "
                f"{files[query_name]} and {path}"
            )
        files[query_name] = path

    if selected_queries:
        names = []
        for query_name in selected_queries:
            query_name = query_name.lower()
            if query_name not in QUERY_NAMES:
                raise PlanGenerationError(f"Unknown TPC-DS query: {query_name}")
            if query_name not in files:
                raise PlanGenerationError(
                    f"TPC-DS SQL file is missing for {query_name} in {query_directory}"
                )
            if query_name not in names:
                names.append(query_name)
    else:
        expected = set(QUERY_NAMES)
        actual = set(files)
        if actual != expected:
            missing = ", ".join(sorted(expected - actual)) or "none"
            extra = ", ".join(sorted(actual - expected)) or "none"
            raise PlanGenerationError(
                "TPC-DS query directory must contain the 103 SQL files; "
                f"missing: {missing}; extra: {extra}"
            )
        names = list(QUERY_NAMES)

    return [(name, files[name]) for name in names]


def snapshot_json_files(directory: Path) -> Snapshot:
    """Records JSON files so a subsequent native-worker dump can be found."""
    result = {}
    for path in directory.rglob("*.json"):
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        if path.is_file():
            result[path.resolve()] = (stat.st_mtime_ns, stat.st_size)
    return result


def node_names(plan: Mapping[str, object]) -> Iterable[str]:
    """Yields plan node names by following source edges."""
    name = plan.get("name")
    if isinstance(name, str):
        yield name
    sources = plan.get("sources", [])
    if isinstance(sources, list):
        for source in sources:
            if isinstance(source, dict):
                yield from node_names(source)


def validate_plan(plan: object, source: Path) -> Mapping[str, object]:
    """Checks that a dump is a complete local Bolt PlanNode tree."""
    if not isinstance(plan, dict):
        raise PlanGenerationError(f"Plan dump is not a JSON object: {source}")
    if not isinstance(plan.get("id"), str):
        raise PlanGenerationError(f"Plan dump has no string node id: {source}")
    if not isinstance(plan.get("name"), str) or not str(plan["name"]).endswith("Node"):
        raise PlanGenerationError(f"Plan dump has no PlanNode name: {source}")
    if not isinstance(plan.get("sources"), list):
        raise PlanGenerationError(f"Plan dump has no source list: {source}")

    names = set(node_names(plan))
    if "TableScanNode" not in names:
        raise PlanGenerationError(
            f"Plan dump contains no TableScanNode and is not a TPC-DS plan: {source}"
        )
    unsupported = names.intersection({"ExchangeNode", "RemoteSourceNode"})
    if unsupported:
        raise PlanGenerationError(
            "Plan dump is distributed rather than single-node; found "
            f"{', '.join(sorted(unsupported))} in {source}"
        )
    return plan


def changed_plans(
    dump_directory: Path, before: Snapshot
) -> List[Tuple[Path, Mapping[str, object]]]:
    """Returns complete PlanNode dumps created or changed after a snapshot."""
    plans = []
    for path, state in snapshot_json_files(dump_directory).items():
        if before.get(path) == state:
            continue
        try:
            with path.open(encoding="utf-8") as source:
                value = json.load(source)
            plan = validate_plan(value, path)
        except (OSError, json.JSONDecodeError, PlanGenerationError):
            continue
        plans.append((path, plan))
    return plans


def choose_plan(
    candidates: Sequence[Tuple[Path, Mapping[str, object]]], query_name: str
) -> Optional[Tuple[Path, Mapping[str, object]]]:
    """Selects one plan, allowing duplicate worker dumps of the same tree."""
    if not candidates:
        return None

    by_contents: Dict[str, List[Path]] = {}
    plans_by_contents: Dict[str, Mapping[str, object]] = {}
    for path, plan in candidates:
        contents = json.dumps(plan, sort_keys=True, separators=(",", ":"))
        by_contents.setdefault(contents, []).append(path)
        plans_by_contents[contents] = plan
    if len(by_contents) != 1:
        paths = ", ".join(str(path) for path, _ in candidates)
        raise PlanGenerationError(
            f"Multiple different plan dumps appeared while generating {query_name}: "
            f"{paths}. Use a dump directory dedicated to this generator."
        )

    contents, paths = next(iter(by_contents.items()))
    return paths[0], plans_by_contents[contents]


def write_plan(path: Path, plan: Mapping[str, object]) -> None:
    """Atomically writes a normalized PlanNode JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(plan, output, indent=2)
            output.write("\n")
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def parse_headers(values: Sequence[str]) -> Dict[str, str]:
    """Parses repeatable NAME=VALUE HTTP header options."""
    headers = {}
    for value in values:
        name, separator, header_value = value.partition("=")
        if not separator or not name.strip():
            raise PlanGenerationError(
                f"Invalid HTTP header {value!r}; expected NAME=VALUE"
            )
        headers[name.strip()] = header_value
    return headers


class PrestoClient:
    """Minimal client for the documented Presto statement HTTP protocol."""

    def __init__(
        self,
        server: str,
        user: str,
        catalog: str,
        schema: str,
        session_properties: Sequence[str],
        extra_headers: Mapping[str, str],
        timeout: float,
    ) -> None:
        self.statement_url = f"{server.rstrip('/')}/v1/statement"
        self.timeout = timeout
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "text/plain; charset=utf-8",
            "X-Presto-User": user,
            "X-Presto-Source": "bolt-tpcds-plan-generator",
            "X-Presto-Catalog": catalog,
            "X-Presto-Schema": schema,
            "X-Presto-Session": ",".join(session_properties),
            **extra_headers,
        }

    def _json_request(
        self,
        method: str,
        url: str,
        body: Optional[bytes] = None,
        trace_token: Optional[str] = None,
    ) -> Mapping[str, object]:
        headers = dict(self.headers)
        if trace_token:
            headers["X-Presto-Trace-Token"] = trace_token
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    return json.load(response)
            except urllib.error.HTTPError as error:
                if error.code == 503 and time.monotonic() < deadline:
                    time.sleep(0.1)
                    continue
                detail = error.read().decode("utf-8", errors="replace")
                raise PlanGenerationError(
                    f"Presto returned HTTP {error.code} for {method} {url}: {detail}"
                ) from error
            except (OSError, json.JSONDecodeError) as error:
                raise PlanGenerationError(
                    f"Presto request failed for {method} {url}: {error}"
                ) from error

    def submit(self, sql: str, trace_token: str) -> Mapping[str, object]:
        """Submits one SQL statement."""
        return self._json_request(
            "POST", self.statement_url, sql.encode("utf-8"), trace_token
        )

    def advance(self, next_uri: str) -> Mapping[str, object]:
        """Advances a query until the worker receives its plan."""
        return self._json_request("GET", next_uri)

    def cancel(self, next_uri: str) -> None:
        """Cancels a query after its plan has been dumped."""
        request = urllib.request.Request(
            next_uri, headers=self.headers, method="DELETE"
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout):
                return
        except urllib.error.HTTPError as error:
            if error.code not in (404, 410):
                detail = error.read().decode("utf-8", errors="replace")
                raise PlanGenerationError(
                    f"Failed to cancel Presto query: HTTP {error.code}: {detail}"
                ) from error
        except OSError as error:
            raise PlanGenerationError(
                f"Failed to cancel Presto query through {next_uri}: {error}"
            ) from error


def query_error(response: Mapping[str, object]) -> Optional[str]:
    """Extracts a readable error from a Presto QueryResults document."""
    error = response.get("error")
    if not isinstance(error, dict):
        return None
    message = error.get("message", "unknown Presto error")
    failure = error.get("failureInfo")
    if isinstance(failure, dict) and failure.get("message") != message:
        return f"{message}: {failure.get('message')}"
    return str(message)


def generate_one_plan(
    query_name: str,
    sql: str,
    output_path: Path,
    dump_directory: Path,
    client: PrestoClient,
    dump_timeout: float,
    poll_interval: float,
) -> Path:
    """Submits one query and saves the native worker's dumped PlanNode."""
    before = snapshot_json_files(dump_directory)
    response = client.submit(sql, query_name)
    next_uri = response.get("nextUri")
    deadline = time.monotonic() + dump_timeout

    try:
        while True:
            selected = choose_plan(changed_plans(dump_directory, before), query_name)
            if selected is not None:
                _, plan = selected
                write_plan(output_path, plan)
                return output_path

            error = query_error(response)
            if error:
                raise PlanGenerationError(
                    f"Presto failed while generating {query_name}: {error}"
                )
            if time.monotonic() >= deadline:
                query_id = response.get("id", "unknown")
                raise PlanGenerationError(
                    f"Timed out waiting for the native worker to dump {query_name} "
                    f"(Presto query {query_id}). Verify the worker plan-dump-dir "
                    f"points to {dump_directory}."
                )

            if isinstance(next_uri, str):
                response = client.advance(next_uri)
                next_uri = response.get("nextUri")
            else:
                time.sleep(poll_interval)
    finally:
        if isinstance(next_uri, str):
            client.cancel(next_uri)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--query-dir",
        required=True,
        type=Path,
        help="Directory containing q1.sql through q99.sql and a/b variants",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="Directory in which q1.json through q99.json are written",
    )
    parser.add_argument(
        "--dump-dir",
        required=True,
        type=Path,
        help="Directory configured as plan-dump-dir on the native worker",
    )
    parser.add_argument(
        "--server",
        default="http://127.0.0.1:8080",
        help="Presto coordinator URL (default: %(default)s)",
    )
    parser.add_argument("--user", default="bolt", help="Presto user")
    parser.add_argument("--catalog", default="hive", help="Presto catalog")
    parser.add_argument("--schema", default="tpcds", help="Presto schema")
    parser.add_argument(
        "--query",
        action="append",
        default=[],
        help="Generate only this query; may be repeated (default: all 103)",
    )
    parser.add_argument(
        "--session",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="Additional Presto session property; may be repeated",
    )
    parser.add_argument(
        "--header",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="Additional HTTP header; may be repeated",
    )
    parser.add_argument(
        "--dump-timeout",
        type=float,
        default=60.0,
        help="Seconds to wait for each worker plan dump (default: %(default)s)",
    )
    parser.add_argument(
        "--http-timeout",
        type=float,
        default=10.0,
        help="Seconds allowed for each HTTP request (default: %(default)s)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=0.1,
        help="Seconds between dump checks after a query ends (default: %(default)s)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing output plan files",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.dump_timeout <= 0 or args.http_timeout <= 0:
            raise PlanGenerationError("Timeout values must be positive")
        if args.poll_interval <= 0:
            raise PlanGenerationError("--poll-interval must be positive")

        query_files = find_query_files(args.query_dir, args.query)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        args.dump_dir.mkdir(parents=True, exist_ok=True)

        output_paths = {
            name: args.output_dir / f"{name}.json" for name, _ in query_files
        }
        existing = [path for path in output_paths.values() if path.exists()]
        if existing and not args.overwrite:
            raise PlanGenerationError(
                "Output plans already exist; pass --overwrite to replace them: "
                + ", ".join(str(path) for path in existing)
            )

        session_properties = ["single_node_execution_enabled=true"]
        session_properties.extend(args.session)
        client = PrestoClient(
            args.server,
            args.user,
            args.catalog,
            args.schema,
            session_properties,
            parse_headers(args.header),
            args.http_timeout,
        )

        for index, (query_name, query_path) in enumerate(query_files, start=1):
            print(f"[{index}/{len(query_files)}] Generating {query_name}", flush=True)
            sql = normalize_sql(query_path.read_text(encoding="utf-8"))
            path = generate_one_plan(
                query_name,
                sql,
                output_paths[query_name],
                args.dump_dir,
                client,
                args.dump_timeout,
                args.poll_interval,
            )
            print(f"[{index}/{len(query_files)}] Wrote {path}", flush=True)
        return 0
    except (OSError, PlanGenerationError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
