#!/usr/bin/env python3
#
# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

"""Standalone HTTP API backed by a real Bolt execution adapter."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import time
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[2]
API_ROOT = Path(__file__).resolve().parent
DATA_ROOT = API_ROOT / "data"
RUNS_ROOT = DATA_ROOT / "runs"
INDEX_PATH = DATA_ROOT / "index.json"

ADAPTER_BIN: Optional[Path] = None


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _load_index() -> Dict[str, Any]:
    if not INDEX_PATH.exists():
        return {"runs": {}, "capsules": {}}
    return _read_json(INDEX_PATH)


def _save_index(index: Dict[str, Any]) -> None:
    _write_json(INDEX_PATH, index)


def _new_run_id(prefix: str = "run") -> str:
    return f"{prefix}_{int(time.time())}_{uuid.uuid4().hex[:8]}"


def _artifact_url(base_url: str, run_id: str, artifact_name: str) -> str:
    return f"{base_url}/artifacts/{run_id}/{artifact_name}"


def _run_url(base_url: str, run_id: str) -> str:
    return f"{base_url}/runs/{run_id}"


def _capsule_url(base_url: str, capsule_id: str) -> str:
    return f"{base_url}/capsules/{capsule_id}"


def _build_base_url(handler: BaseHTTPRequestHandler) -> str:
    host = handler.headers.get("Host", f"127.0.0.1:{handler.server.server_port}")
    return f"http://{host}"


def _resolve_capsule(
    index: Dict[str, Any], payload: Dict[str, Any]
) -> Tuple[str, Dict[str, Any]]:
    if "capsule" in payload:
        capsule = payload["capsule"]
        capsule_id = str(
            capsule.get("query_id", f"inline_capsule_{uuid.uuid4().hex[:8]}")
        )
        return capsule_id, capsule

    capsule_id = payload.get("capsule_id")
    if not capsule_id:
        raise ValueError("request must include 'capsule_id' or inline 'capsule'")
    capsule_record = index.get("capsules", {}).get(capsule_id)
    if not capsule_record:
        raise KeyError(f"capsule not found: {capsule_id}")
    return capsule_id, _read_json(Path(capsule_record["path"]))


def _candidate_adapter_paths() -> List[Path]:
    return [
        Path(os.environ["BOLT_AGENT_EXECUTOR_BIN"])
        if "BOLT_AGENT_EXECUTOR_BIN" in os.environ
        else Path(),
        PROJECT_ROOT
        / "_build"
        / "Release"
        / "_build"
        / "Release"
        / "bolt"
        / "bolt_agent_runtime_executor",
        PROJECT_ROOT
        / "_build"
        / "Release"
        / "_build"
        / "Release"
        / "bolt_agent_runtime_executor",
        PROJECT_ROOT / "_build" / "Release" / "bolt_agent_runtime_executor",
        PROJECT_ROOT / "build" / "bolt_agent_runtime_executor",
    ]


def _resolve_adapter_bin(explicit: Optional[str]) -> Path:
    if explicit:
        path = Path(explicit)
        if path.exists():
            return path
        raise FileNotFoundError(f"adapter binary not found: {path}")

    for candidate in _candidate_adapter_paths():
        if candidate and candidate.exists():
            return candidate

    raise FileNotFoundError(
        "Bolt adapter binary not found. Build `bolt_agent_runtime_executor` "
        "or pass --adapter-bin /path/to/binary."
    )


def _aggregate_operator_summaries(
    trace_dir: Path, traced_nodes: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    summaries: List[Dict[str, Any]] = []
    for traced_node in traced_nodes:
        node_id = str(traced_node["plan_node_id"])
        node_name = traced_node.get("node_name", "")
        node_dir = trace_dir / node_id
        if not node_dir.exists():
            continue

        summary_files = list(node_dir.glob("*/*/op_trace_summary.json"))
        if not summary_files:
            continue

        aggregate = {
            "plan_node_id": node_id,
            "operator_type": node_name or "Unknown",
            "input_rows": 0,
            "input_bytes": 0,
            "raw_input_rows": 0,
            "raw_input_bytes": 0,
            "peak_memory_bytes": 0,
            "num_splits": 0,
        }
        for summary_file in summary_files:
            payload = _read_json(summary_file)
            aggregate["operator_type"] = payload.get(
                "opType", aggregate["operator_type"]
            )
            aggregate["input_rows"] += int(payload.get("inputRows", 0))
            aggregate["input_bytes"] += int(payload.get("inputBytes", 0))
            aggregate["raw_input_rows"] += int(payload.get("rawInputRows", 0))
            aggregate["raw_input_bytes"] += int(payload.get("rawInputBytes", 0))
            aggregate["peak_memory_bytes"] = max(
                aggregate["peak_memory_bytes"], int(payload.get("peakMemory", 0))
            )
            aggregate["num_splits"] += int(payload.get("numSplits", 0))
        summaries.append(aggregate)

    summaries.sort(
        key=lambda summary: (summary["peak_memory_bytes"], summary["input_bytes"]),
        reverse=True,
    )
    return summaries


def _build_critique(
    request_payload: Dict[str, Any],
    adapter_result: Dict[str, Any],
    operator_summaries: List[Dict[str, Any]],
) -> Dict[str, Any]:
    findings: List[Dict[str, Any]] = []
    global_findings: List[str] = []

    for summary in operator_summaries:
        op_type = summary["operator_type"]
        selectivity = 0.0
        if summary["raw_input_rows"]:
            selectivity = summary["input_rows"] / summary["raw_input_rows"]

        if "Aggregation" in op_type:
            strategy = (
                request_payload.get("plan", {})
                .get("aggregation", {})
                .get("strategy", "single")
            )
            findings.append(
                {
                    "plan_node_id": summary["plan_node_id"],
                    "operator_type": op_type,
                    "symptom": "aggregation_dominates_runtime",
                    "evidence": {
                        "input_rows": summary["input_rows"],
                        "peak_memory_bytes": summary["peak_memory_bytes"],
                        "strategy": strategy,
                    },
                    "suggested_actions": [
                        "retune_aggregation",
                        "increase_max_drivers",
                        "switch_to_partial_final"
                        if strategy == "single"
                        else "keep_partial_final",
                    ],
                }
            )
        if "FilterProject" in op_type and selectivity > 0.9:
            findings.append(
                {
                    "plan_node_id": summary["plan_node_id"],
                    "operator_type": op_type,
                    "symptom": "low_filter_selectivity",
                    "evidence": {
                        "raw_input_rows": summary["raw_input_rows"],
                        "input_rows": summary["input_rows"],
                    },
                    "suggested_actions": [
                        "tighten_filter",
                        "push_projection_earlier",
                    ],
                }
            )

    if adapter_result["runtime_ms"] > 50:
        global_findings.append("latency_above_interactive_budget")
    if operator_summaries:
        global_findings.append(
            f"dominant_operator:{operator_summaries[0]['operator_type']}"
        )

    recommended = []
    for finding in findings:
        for action in finding["suggested_actions"]:
            if action not in recommended:
                recommended.append(action)

    return {
        "query_id": adapter_result["query_id"],
        "status": "SUCCESS",
        "goal": request_payload.get("goal", "minimize_latency"),
        "dominant_operators": operator_summaries[:3],
        "operator_findings": findings,
        "global_findings": global_findings,
        "recommended_next_actions": recommended[:5],
    }


def _deep_merge(base: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _apply_patch_to_request(
    request_payload: Dict[str, Any], patch_payload: Dict[str, Any]
) -> Dict[str, Any]:
    patched = json.loads(json.dumps(request_payload))
    patch = patch_payload.get("patch", patch_payload)
    op = patch["op"]

    if op == "retune_aggregation":
        aggregation = dict(patched.get("plan", {}).get("aggregation", {}))
        aggregation["strategy"] = patch.get("strategy", "partial_final")
        patched.setdefault("plan", {})["aggregation"] = aggregation
        if "max_drivers" in patch:
            patched["max_drivers"] = patch["max_drivers"]
    elif op == "merge_request":
        patched = _deep_merge(patched, patch["updates"])
    else:
        raise ValueError(f"unsupported patch op: {op}")

    patched["query_id"] = f"{patched.get('query_id', 'bolt_agent_query')}_patched"
    return patched


class BoltExecutor:
    def __init__(self, adapter_bin: Path) -> None:
        self.adapter_bin = adapter_bin

    def run(self, request_payload: Dict[str, Any], run_dir: Path) -> Dict[str, Any]:
        request_path = run_dir / "adapter_request.json"
        trace_root = run_dir / "trace"
        if trace_root.exists():
            shutil.rmtree(trace_root)
        _write_json(request_path, request_payload)

        completed = subprocess.run(
            [
                str(self.adapter_bin),
                "--request",
                str(request_path),
                "--trace-root",
                str(trace_root),
            ],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )

        payload = json.loads(completed.stdout)
        payload["trace_root"] = str(trace_root)
        payload["task_trace_dir"] = payload.get("task_trace_dir", str(trace_root))
        return payload


def _store_run(
    index: Dict[str, Any],
    run_id: str,
    mode: str,
    request_payload: Dict[str, Any],
    result: Dict[str, Any],
    critique: Dict[str, Any],
    capsule: Dict[str, Any],
    base_url: str,
    replay_of: Optional[str] = None,
    base_capsule_id: Optional[str] = None,
) -> Dict[str, Any]:
    run_dir = RUNS_ROOT / run_id
    request_path = run_dir / "request.json"
    result_path = run_dir / "result.json"
    critique_path = run_dir / "critique.json"
    capsule_path = run_dir / "capsule.json"

    _write_json(request_path, request_payload)
    _write_json(result_path, result)
    _write_json(critique_path, critique)
    _write_json(capsule_path, capsule)

    summary = {
        "run_id": run_id,
        "mode": mode,
        "status": result.get("status", "UNKNOWN"),
        "query_id": result.get("query_id"),
        "runtime_ms": result.get("runtime_ms"),
        "peak_memory_bytes": result.get("peak_memory_bytes"),
        "dominant_operator": result.get("dominant_operator"),
        "created_at_unix": int(time.time()),
        "replay_of": replay_of,
        "base_capsule_id": base_capsule_id,
        "artifacts": {
            "request": _artifact_url(base_url, run_id, "request"),
            "result": _artifact_url(base_url, run_id, "result"),
            "critique": _artifact_url(base_url, run_id, "critique"),
            "capsule": _artifact_url(base_url, run_id, "capsule"),
        },
        "run_url": _run_url(base_url, run_id),
        "capsule_url": _capsule_url(base_url, run_id),
    }

    index["runs"][run_id] = {"path": str(run_dir), "summary": summary}
    index["capsules"][run_id] = {"run_id": run_id, "path": str(capsule_path)}
    _save_index(index)
    return summary


def _package_artifacts(
    request_payload: Dict[str, Any],
    adapter_result: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    operator_summaries = _aggregate_operator_summaries(
        Path(adapter_result["task_trace_dir"]),
        adapter_result.get("traced_nodes", []),
    )
    peak_memory = max(
        (summary["peak_memory_bytes"] for summary in operator_summaries), default=0
    )
    dominant_operator = (
        operator_summaries[0]["operator_type"] if operator_summaries else None
    )

    result = {
        "query_id": adapter_result["query_id"],
        "status": "SUCCESS",
        "row_count": adapter_result["row_count"],
        "rows": adapter_result.get("rows", []),
        "runtime_ms": adapter_result["runtime_ms"],
        "peak_memory_bytes": peak_memory,
        "dominant_operator": dominant_operator,
        "plan_string": adapter_result.get("plan_string"),
    }

    critique = _build_critique(request_payload, result, operator_summaries)

    capsule = {
        "capsule_version": 1,
        "query_id": adapter_result["query_id"],
        "request": request_payload,
        "result": result,
        "critique": critique,
        "trace_root": adapter_result["trace_root"],
        "task_trace_dir": adapter_result["task_trace_dir"],
        "traced_nodes": adapter_result.get("traced_nodes", []),
        "trace_summary": operator_summaries,
    }
    return result, critique, capsule


class AgentRuntimeApiHandler(BaseHTTPRequestHandler):
    server_version = "BoltAgentRuntimeAPI/0.2"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            if path == "/healthz":
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "status": "ok",
                        "adapter_bin": str(ADAPTER_BIN) if ADAPTER_BIN else None,
                    },
                )
                return

            if path.startswith("/runs/"):
                self._handle_get_run(path)
                return

            if path.startswith("/capsules/"):
                self._handle_get_capsule(path)
                return

            if path.startswith("/artifacts/"):
                self._handle_get_artifact(path)
                return

            self._send_json(
                HTTPStatus.NOT_FOUND, {"error": f"unknown endpoint: {path}"}
            )
        except Exception as exc:  # noqa: BLE001
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": "internal_server_error", "message": str(exc)},
            )

    def do_POST(self) -> None:
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            payload = self._read_request_json()

            if path == "/runs":
                self._handle_post_run(payload)
                return

            if path == "/replays":
                self._handle_post_replay(payload)
                return

            if path == "/patches":
                self._handle_post_patch(payload)
                return

            self._send_json(
                HTTPStatus.NOT_FOUND, {"error": f"unknown endpoint: {path}"}
            )
        except ValueError as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST, {"error": "bad_request", "message": str(exc)}
            )
        except KeyError as exc:
            self._send_json(
                HTTPStatus.NOT_FOUND, {"error": "not_found", "message": str(exc)}
            )
        except subprocess.CalledProcessError as exc:
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    "error": "adapter_failed",
                    "message": exc.stderr or str(exc),
                },
            )
        except Exception as exc:  # noqa: BLE001
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": "internal_server_error", "message": str(exc)},
            )

    def _read_request_json(self) -> Dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(content_length) if content_length > 0 else b"{}"
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _send_json(self, status: HTTPStatus, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_post_run(self, payload: Dict[str, Any]) -> None:
        if "input" not in payload or "plan" not in payload:
            raise ValueError("request must include 'input' and 'plan'")

        request_payload = {
            "query_id": payload.get(
                "query_id", f"bolt_agent_query_{uuid.uuid4().hex[:8]}"
            ),
            "input": payload["input"],
            "plan": payload["plan"],
            "goal": payload.get("goal", "minimize_latency"),
            "repeat_times": int(payload.get("repeat_times", 1)),
            "max_drivers": int(payload.get("max_drivers", 1)),
        }

        run_id = _new_run_id("run")
        run_dir = RUNS_ROOT / run_id
        adapter_result = BoltExecutor(ADAPTER_BIN).run(request_payload, run_dir)
        result, critique, capsule = _package_artifacts(request_payload, adapter_result)

        index = _load_index()
        base_url = _build_base_url(self)
        summary = _store_run(
            index=index,
            run_id=run_id,
            mode="run",
            request_payload=request_payload,
            result=result,
            critique=critique,
            capsule=capsule,
            base_url=base_url,
        )
        self._send_json(HTTPStatus.CREATED, summary)

    def _handle_post_replay(self, payload: Dict[str, Any]) -> None:
        index = _load_index()
        base_capsule_id, capsule = _resolve_capsule(index, payload)
        request_payload = capsule["request"]

        run_id = _new_run_id("replay")
        run_dir = RUNS_ROOT / run_id
        adapter_result = BoltExecutor(ADAPTER_BIN).run(request_payload, run_dir)
        result, critique, replay_capsule = _package_artifacts(
            request_payload, adapter_result
        )
        replay_capsule["replay_of"] = base_capsule_id

        base_url = _build_base_url(self)
        summary = _store_run(
            index=index,
            run_id=run_id,
            mode="replay",
            request_payload={"capsule_id": base_capsule_id, "request": request_payload},
            result=result,
            critique=critique,
            capsule=replay_capsule,
            base_url=base_url,
            replay_of=base_capsule_id,
        )
        self._send_json(HTTPStatus.CREATED, summary)

    def _handle_post_patch(self, payload: Dict[str, Any]) -> None:
        if "patch" not in payload:
            raise ValueError("request must include 'patch'")

        index = _load_index()
        base_capsule_id, capsule = _resolve_capsule(index, payload)
        patched_request = _apply_patch_to_request(capsule["request"], payload["patch"])

        run_id = _new_run_id("patch")
        run_dir = RUNS_ROOT / run_id
        adapter_result = BoltExecutor(ADAPTER_BIN).run(patched_request, run_dir)
        result, critique, patched_capsule = _package_artifacts(
            patched_request, adapter_result
        )
        patched_capsule["base_capsule_query_id"] = base_capsule_id
        patched_capsule["applied_patch"] = payload["patch"]

        base_url = _build_base_url(self)
        summary = _store_run(
            index=index,
            run_id=run_id,
            mode="patch",
            request_payload={"capsule_id": base_capsule_id, "patch": payload["patch"]},
            result=result,
            critique=critique,
            capsule=patched_capsule,
            base_url=base_url,
            base_capsule_id=base_capsule_id,
        )
        self._send_json(HTTPStatus.CREATED, summary)

    def _handle_get_run(self, path: str) -> None:
        run_id = path.split("/")[2]
        index = _load_index()
        record = index.get("runs", {}).get(run_id)
        if not record:
            raise KeyError(f"run not found: {run_id}")
        self._send_json(HTTPStatus.OK, record["summary"])

    def _handle_get_capsule(self, path: str) -> None:
        capsule_id = path.split("/")[2]
        index = _load_index()
        record = index.get("capsules", {}).get(capsule_id)
        if not record:
            raise KeyError(f"capsule not found: {capsule_id}")
        self._send_json(HTTPStatus.OK, _read_json(Path(record["path"])))

    def _handle_get_artifact(self, path: str) -> None:
        parts = path.split("/")
        if len(parts) < 4:
            raise ValueError(
                "artifact path must be /artifacts/{run_id}/{artifact_name}"
            )
        run_id = parts[2]
        artifact_name = parts[3]
        if artifact_name not in {"request", "result", "critique", "capsule"}:
            raise ValueError(f"unsupported artifact: {artifact_name}")
        artifact_path = RUNS_ROOT / run_id / f"{artifact_name}.json"
        if not artifact_path.exists():
            raise KeyError(f"artifact not found: {run_id}/{artifact_name}")
        self._send_json(HTTPStatus.OK, _read_json(artifact_path))


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Standalone Bolt agent runtime API")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind")
    parser.add_argument("--port", type=int, default=8088, help="Port to bind")
    parser.add_argument("--adapter-bin", help="Path to bolt_agent_runtime_executor")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    global ADAPTER_BIN

    args = parse_args(argv)
    ADAPTER_BIN = _resolve_adapter_bin(args.adapter_bin)

    RUNS_ROOT.mkdir(parents=True, exist_ok=True)
    if not INDEX_PATH.exists():
        _save_index({"runs": {}, "capsules": {}})

    server = ThreadingHTTPServer((args.host, args.port), AgentRuntimeApiHandler)
    print(
        f"Bolt Agent Runtime API listening on http://{args.host}:{args.port} "
        f"using adapter {ADAPTER_BIN}"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
