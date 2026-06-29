#!/usr/bin/env python3
#
# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

"""Standalone HTTP API backed by a real Bolt execution adapter."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import threading
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
INTERACTIVE_BUDGET_MS = 50
INDEX_LOCK = threading.RLock()


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _load_index() -> Dict[str, Any]:
    with INDEX_LOCK:
        if not INDEX_PATH.exists():
            return {"runs": {}, "capsules": {}}
        return _read_json(INDEX_PATH)


def _save_index(index: Dict[str, Any]) -> None:
    with INDEX_LOCK:
        tmp_path = INDEX_PATH.with_suffix(".tmp")
        _write_json(tmp_path, index)
        os.replace(tmp_path, INDEX_PATH)


def _new_run_id(prefix: str = "run") -> str:
    return f"{prefix}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"


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
    candidates: List[Path] = []
    env_candidate = os.environ.get("BOLT_AGENT_EXECUTOR_BIN")
    if env_candidate:
        candidates.append(Path(env_candidate))
    candidates.extend(
        [
            PROJECT_ROOT
            / "_build"
            / "Release"
            / "bolt"
            / "bolt_agent_runtime_executor",
            PROJECT_ROOT / "_build" / "Release" / "bolt_agent_runtime_executor",
            PROJECT_ROOT / "build" / "Release" / "bolt_agent_runtime_executor",
            PROJECT_ROOT / "build" / "bolt_agent_runtime_executor",
        ]
    )
    return candidates


def _resolve_adapter_bin(explicit: Optional[str]) -> Path:
    if explicit:
        path = Path(explicit)
        if path.is_file():
            return path
        raise FileNotFoundError(f"adapter binary not found: {path}")

    for candidate in _candidate_adapter_paths():
        if candidate.is_file():
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


def _default_operator_summary(
    plan_node_id: str,
    operator_type: str,
    request_payload: Dict[str, Any],
) -> Dict[str, Any]:
    input_rows = len(request_payload.get("input", {}).get("rows", [])) * int(
        request_payload.get("repeat_times", 1)
    )
    return {
        "plan_node_id": plan_node_id,
        "operator_type": operator_type,
        "input_rows": input_rows,
        "input_bytes": 0,
        "raw_input_rows": input_rows,
        "raw_input_bytes": 0,
        "peak_memory_bytes": 0,
        "num_splits": 0,
        "source": "plan_fallback",
    }


def _operator_priority(operator_type: str) -> int:
    priorities = {
        "Aggregation": 50,
        "PartialAggregation": 45,
        "TopN": 40,
        "OrderBy": 35,
        "FilterProject": 20,
        "Project": 10,
        "Values": 0,
    }
    return priorities.get(operator_type, 5)


def _fallback_operator_summaries(
    request_payload: Dict[str, Any],
    adapter_result: Dict[str, Any],
    operator_summaries: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if operator_summaries:
        return operator_summaries

    summaries: List[Dict[str, Any]] = []
    for traced_node in adapter_result.get("traced_nodes", []):
        summaries.append(
            _default_operator_summary(
                traced_node.get("plan_node_id", "?"),
                traced_node.get("node_name", "Unknown"),
                request_payload,
            )
        )

    if summaries:
        summaries.sort(
            key=lambda summary: (
                summary["peak_memory_bytes"],
                summary["input_bytes"],
                _operator_priority(summary["operator_type"]),
            ),
            reverse=True,
        )
        return summaries

    plan = adapter_result.get("plan_string", "")
    for idx, operator_type in enumerate(
        re.findall(r"--\s+([A-Za-z]+)(?:\[\d+\])?", plan), start=1
    ):
        summaries.append(
            _default_operator_summary(str(idx), operator_type, request_payload)
        )
    summaries.sort(
        key=lambda summary: _operator_priority(summary["operator_type"]),
        reverse=True,
    )
    return summaries


def _extract_aggregation_strategy(request_payload: Dict[str, Any]) -> str:
    return (
        request_payload.get("plan", {}).get("aggregation", {}).get("strategy", "single")
    )


def _build_plan_findings(
    request_payload: Dict[str, Any],
    adapter_result: Dict[str, Any],
    operator_summaries: List[Dict[str, Any]],
) -> Tuple[List[str], List[Dict[str, Any]], List[str], List[Dict[str, Any]]]:
    global_findings: List[str] = []
    findings: List[Dict[str, Any]] = []
    recommended: List[str] = []
    candidate_patches: List[Dict[str, Any]] = []
    plan = adapter_result.get("plan_string", "")
    strategy = _extract_aggregation_strategy(request_payload)

    if adapter_result["runtime_ms"] > INTERACTIVE_BUDGET_MS:
        global_findings.append("latency_above_interactive_budget")

    if operator_summaries:
        dominant = operator_summaries[0]["operator_type"]
        global_findings.append(f"dominant_operator:{dominant}")

    if "Aggregation" in plan or request_payload.get("plan", {}).get("aggregation"):
        findings.append(
            {
                "operator_type": "Aggregation",
                "symptom": "aggregation_present",
                "evidence": {
                    "strategy": strategy,
                    "max_drivers": request_payload.get("max_drivers", 1),
                    "runtime_ms": adapter_result["runtime_ms"],
                },
                "suggested_actions": (
                    ["retune_aggregation", "increase_max_drivers"]
                    if strategy == "single"
                    else ["compare_single_vs_partial_final"]
                ),
            }
        )
        if strategy == "single":
            candidate_patches.append(
                {
                    "patch": {
                        "op": "retune_aggregation",
                        "strategy": "partial_final",
                        "max_drivers": max(
                            2, request_payload.get("max_drivers", 1) * 2
                        ),
                    },
                    "reason": "Aggregation is present under single-stage execution.",
                }
            )

    if request_payload.get("plan", {}).get("filter"):
        findings.append(
            {
                "operator_type": "FilterProject",
                "symptom": "filter_present",
                "evidence": {
                    "filter": request_payload["plan"]["filter"],
                    "runtime_ms": adapter_result["runtime_ms"],
                },
                "suggested_actions": ["tighten_filter", "push_projection_earlier"],
            }
        )
    elif request_payload.get("input", {}).get("schema"):
        schema_names = [
            column["name"] for column in request_payload["input"].get("schema", [])
        ]
        if "active" in schema_names:
            candidate_patches.append(
                {
                    "patch": {
                        "op": "add_filter",
                        "expr": "active = true",
                    },
                    "reason": "Boolean active column present with no filter applied.",
                }
            )

    if "TopN" in plan or request_payload.get("plan", {}).get("order_by"):
        findings.append(
            {
                "operator_type": "TopN",
                "symptom": "ordered_result",
                "evidence": {
                    "order_by": request_payload.get("plan", {}).get("order_by", []),
                    "limit": request_payload.get("plan", {}).get("limit"),
                },
                "suggested_actions": ["reduce_limit", "drop_order_by_if_optional"],
            }
        )

    for finding in findings:
        for action in finding["suggested_actions"]:
            if action not in recommended:
                recommended.append(action)

    return global_findings, findings, recommended, candidate_patches


def _build_critique(
    request_payload: Dict[str, Any],
    adapter_result: Dict[str, Any],
    operator_summaries: List[Dict[str, Any]],
) -> Dict[str, Any]:
    findings: List[Dict[str, Any]] = []
    operator_summaries = _fallback_operator_summaries(
        request_payload, adapter_result, operator_summaries
    )
    global_findings, plan_findings, recommended, candidate_patches = (
        _build_plan_findings(request_payload, adapter_result, operator_summaries)
    )
    findings.extend(plan_findings)

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
        "candidate_patches": candidate_patches[:5],
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
    elif op == "change_parallelism":
        patched["max_drivers"] = int(patch["max_drivers"])
    elif op == "add_filter":
        plan = patched.setdefault("plan", {})
        if plan.get("filter"):
            raise ValueError("request already has filter; use replace_filter instead")
        plan["filter"] = patch["expr"]
    elif op == "replace_filter":
        patched.setdefault("plan", {})["filter"] = patch["expr"]
    elif op == "remove_filter":
        patched.setdefault("plan", {}).pop("filter", None)
    elif op == "change_projection":
        patched.setdefault("plan", {})["project"] = list(patch["columns"])
    elif op == "change_limit":
        patched.setdefault("plan", {})["limit"] = {
            "offset": int(patch.get("offset", 0)),
            "count": int(patch["count"]),
        }
    elif op == "change_order_by":
        patched.setdefault("plan", {})["order_by"] = list(patch["order_by"])
    elif op == "merge_request":
        patched = _deep_merge(patched, patch["updates"])
    else:
        raise ValueError(f"unsupported patch op: {op}")

    patched["query_id"] = f"{patched.get('query_id', 'bolt_agent_query')}_patched"
    return patched


def _normalize_v2_plan_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    request = payload.get("request", payload)
    if "input_tables" in payload and "input" not in request:
        tables = payload["input_tables"]
        if "input" not in tables:
            raise ValueError("v2 SQL currently requires input_tables.input")
        request = dict(request)
        request["input"] = tables["input"]
    if "input" not in request or "plan" not in request:
        raise ValueError("request must include 'input' and 'plan'")
    return {
        "query_id": request.get(
            "query_id",
            payload.get("query_id", f"bolt_agent_query_{uuid.uuid4().hex[:8]}"),
        ),
        "input": request["input"],
        "plan": request["plan"],
        "goal": request.get("goal", payload.get("goal", "minimize_latency")),
        "repeat_times": int(
            request.get("repeat_times", payload.get("repeat_times", 1))
        ),
        "max_drivers": int(request.get("max_drivers", payload.get("max_drivers", 1))),
        "constraints": payload.get("constraints", request.get("constraints", {})),
        "execution_hints": payload.get(
            "execution_hints", request.get("execution_hints", {})
        ),
    }


def _translate_v2_sql_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    expanded = dict(payload)
    if "input_tables" in payload and "input" not in payload:
        tables = payload["input_tables"]
        if "input" in tables:
            expanded["input"] = tables["input"]
    translated = _translate_sql_to_request(expanded)
    translated["constraints"] = payload.get("constraints", {})
    translated["execution_hints"] = payload.get("execution_hints", {})
    return translated


def _load_run_summary(index: Dict[str, Any], run_id: str) -> Dict[str, Any]:
    record = index.get("runs", {}).get(run_id)
    if not record:
        raise KeyError(f"run not found: {run_id}")
    return record["summary"]


def _load_run_artifact(run_id: str, artifact_name: str) -> Dict[str, Any]:
    artifact_path = RUNS_ROOT / run_id / f"{artifact_name}.json"
    if not artifact_path.exists():
        raise KeyError(f"artifact not found: {run_id}/{artifact_name}")
    return _read_json(artifact_path)


def _compare_runs_payload(base_run_id: str, candidate_run_id: str) -> Dict[str, Any]:
    base_result = _load_run_artifact(base_run_id, "result")
    candidate_result = _load_run_artifact(candidate_run_id, "result")
    base_critique = _load_run_artifact(base_run_id, "critique")
    candidate_critique = _load_run_artifact(candidate_run_id, "critique")

    same_rows = base_result.get("rows", []) == candidate_result.get("rows", [])
    base_runtime = int(base_result.get("runtime_ms", 0))
    candidate_runtime = int(candidate_result.get("runtime_ms", 0))
    delta = candidate_runtime - base_runtime

    if not same_rows:
        winner = "base"
        why = ["candidate_result_changed"]
    elif candidate_runtime < base_runtime:
        winner = "candidate"
        why = ["same_result", "lower_runtime"]
    elif candidate_runtime > base_runtime:
        winner = "base"
        why = ["same_result", "base_is_faster"]
    else:
        winner = "tie"
        why = ["same_result", "same_runtime"]

    base_dominant = base_critique.get("global_findings", [])
    candidate_dominant = candidate_critique.get("global_findings", [])
    if base_dominant != candidate_dominant:
        why.append("critique_changed")

    return {
        "base_run_id": base_run_id,
        "candidate_run_id": candidate_run_id,
        "same_rows": same_rows,
        "runtime_ms": {
            "base": base_runtime,
            "candidate": candidate_runtime,
            "delta": delta,
        },
        "winner": winner,
        "why": why,
        "base_dominant_operator": base_result.get("dominant_operator"),
        "candidate_dominant_operator": candidate_result.get("dominant_operator"),
    }


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

    with INDEX_LOCK:
        index = _load_index()
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
    operator_summaries = _fallback_operator_summaries(
        request_payload, adapter_result, operator_summaries
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


def _split_csv(expr: str) -> List[str]:
    parts: List[str] = []
    current: List[str] = []
    depth = 0
    in_single = False
    for ch in expr:
        if ch == "'" and (not current or current[-1] != "\\"):
            in_single = not in_single
        elif not in_single:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                parts.append("".join(current).strip())
                current = []
                continue
        current.append(ch)
    if current:
        parts.append("".join(current).strip())
    return [part for part in parts if part]


def _infer_literal_type(value: Any) -> str:
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "DOUBLE"
    return "VARCHAR"


def _parse_sql_literal(expr: str) -> Any:
    value = expr.strip()
    if re.fullmatch(r"(?i)true|false", value):
        return value.lower() == "true"
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if re.fullmatch(r"-?\d+\.\d+", value):
        return float(value)
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    raise ValueError(f"unsupported SQL literal: {expr}")


def _sql_identifier(expr: str) -> str:
    return expr.strip().strip('"')


def _translate_sql_to_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    sql = payload.get("sql", "").strip().rstrip(";")
    if not sql:
        raise ValueError("request must include non-empty 'sql'")

    match = re.match(
        r"(?is)^select\s+(?P<select>.+?)"
        r"(?:\s+from\s+(?P<from>[a-zA-Z_][\w]*))?"
        r"(?:\s+where\s+(?P<where>.+?))?"
        r"(?:\s+group\s+by\s+(?P<group_by>.+?))?"
        r"(?:\s+order\s+by\s+(?P<order_by>.+?))?"
        r"(?:\s+limit\s+(?P<limit>\d+))?$",
        sql,
    )
    if not match:
        raise ValueError(f"unsupported demo SQL: {sql}")

    select_expr = match.group("select").strip()
    from_name = match.group("from")
    where_expr = match.group("where")
    group_by_expr = match.group("group_by")
    order_by_expr = match.group("order_by")
    limit_expr = match.group("limit")

    request_payload = {
        "query_id": payload.get("query_id", f"sql_query_{uuid.uuid4().hex[:8]}"),
        "goal": payload.get("goal", "minimize_latency"),
        "repeat_times": int(payload.get("repeat_times", 1)),
        "max_drivers": int(payload.get("max_drivers", 1)),
    }

    if from_name:
        if from_name.lower() != "input":
            raise ValueError("demo SQL only supports FROM input")
        if "input" not in payload:
            raise ValueError("SQL queries with FROM input require an 'input' object")
        request_payload["input"] = payload["input"]
        plan: Dict[str, Any] = {}
        if where_expr:
            plan["filter"] = where_expr.strip()

        select_items = _split_csv(select_expr)
        group_keys = _split_csv(group_by_expr) if group_by_expr else []
        aggregate_items = [
            item
            for item in select_items
            if re.search(r"(?i)\b(sum|count|min|max|avg)\s*\(", item)
        ]
        non_aggregate_items = [
            item for item in select_items if item not in aggregate_items
        ]

        if aggregate_items or group_keys:
            plan["aggregation"] = {
                "group_by": [_sql_identifier(item) for item in group_keys],
                "aggregates": aggregate_items,
                "strategy": payload.get("aggregation_strategy", "single"),
            }
        elif select_items != ["*"]:
            plan["project"] = [_sql_identifier(item) for item in non_aggregate_items]

        if order_by_expr:
            plan["order_by"] = _split_csv(order_by_expr)
        if limit_expr:
            plan["limit"] = {"offset": 0, "count": int(limit_expr)}
        request_payload["plan"] = plan
        return request_payload

    select_items = _split_csv(select_expr)
    schema = []
    row = []
    projections = []
    for idx, item in enumerate(select_items, start=1):
        alias_match = re.match(r"(?is)^(.*?)\s+as\s+([a-zA-Z_][\w]*)$", item)
        expr = alias_match.group(1).strip() if alias_match else item.strip()
        column_name = alias_match.group(2) if alias_match else f"col_{idx}"
        value = _parse_sql_literal(expr)
        schema.append({"name": column_name, "type": _infer_literal_type(value)})
        row.append(value)
        projections.append(column_name)

    request_payload["input"] = {"schema": schema, "rows": [row]}
    request_payload["plan"] = {"project": projections}
    return request_payload


class AgentRuntimeApiHandler(BaseHTTPRequestHandler):
    server_version = "BoltAgentRuntimeAPI/0.3"

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

            if path.startswith("/v2/runs/"):
                self._handle_get_v2_run(path)
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

            if path == "/v2/sql/runs":
                self._handle_post_v2_sql_run(payload)
                return

            if path == "/v2/plans/runs":
                self._handle_post_v2_plan_run(payload)
                return

            if path == "/v2/runs/compare":
                self._handle_post_v2_compare(payload)
                return

            if path.startswith("/v2/runs/") and path.endswith("/replay"):
                self._handle_post_v2_replay(path)
                return

            if path.startswith("/v2/runs/") and path.endswith("/patch"):
                self._handle_post_v2_patch(path, payload)
                return

            if path == "/sql":
                self._handle_post_sql(payload)
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

        base_url = _build_base_url(self)
        summary = _store_run(
            run_id=run_id,
            mode="run",
            request_payload=request_payload,
            result=result,
            critique=critique,
            capsule=capsule,
            base_url=base_url,
        )
        self._send_json(HTTPStatus.CREATED, summary)

    def _handle_post_sql(self, payload: Dict[str, Any]) -> None:
        translated = _translate_sql_to_request(payload)
        self._handle_post_run(translated)

    def _handle_post_v2_sql_run(self, payload: Dict[str, Any]) -> None:
        translated = _translate_v2_sql_request(payload)
        self._handle_post_run(translated)

    def _handle_post_v2_plan_run(self, payload: Dict[str, Any]) -> None:
        normalized = _normalize_v2_plan_request(payload)
        self._handle_post_run(normalized)

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

    def _handle_post_v2_replay(self, path: str) -> None:
        run_id = path.split("/")[3]
        self._handle_post_replay({"capsule_id": run_id})

    def _handle_post_v2_patch(self, path: str, payload: Dict[str, Any]) -> None:
        run_id = path.split("/")[3]
        if "patch" not in payload:
            raise ValueError("request must include 'patch'")
        self._handle_post_patch({"capsule_id": run_id, "patch": payload["patch"]})

    def _handle_post_v2_compare(self, payload: Dict[str, Any]) -> None:
        base_run_id = payload.get("base_run_id")
        candidate_run_id = payload.get("candidate_run_id")
        if not base_run_id or not candidate_run_id:
            raise ValueError(
                "request must include 'base_run_id' and 'candidate_run_id'"
            )
        self._send_json(
            HTTPStatus.OK,
            _compare_runs_payload(base_run_id, candidate_run_id),
        )

    def _handle_get_run(self, path: str) -> None:
        run_id = path.split("/")[2]
        index = _load_index()
        record = index.get("runs", {}).get(run_id)
        if not record:
            raise KeyError(f"run not found: {run_id}")
        self._send_json(HTTPStatus.OK, record["summary"])

    def _handle_get_v2_run(self, path: str) -> None:
        parts = path.split("/")
        if len(parts) < 4:
            raise ValueError("v2 run path must be /v2/runs/{run_id}")
        run_id = parts[3]
        artifact_name = parts[4] if len(parts) > 4 and parts[4] else None
        index = _load_index()
        _load_run_summary(index, run_id)

        if artifact_name is None:
            self._send_json(HTTPStatus.OK, _load_run_summary(index, run_id))
            return
        if artifact_name not in {"request", "result", "critique", "capsule"}:
            raise ValueError(f"unsupported v2 run artifact: {artifact_name}")
        self._send_json(HTTPStatus.OK, _load_run_artifact(run_id, artifact_name))

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
