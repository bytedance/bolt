#!/usr/bin/env python3
#
# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

"""A lightweight demo of the Bolt Agent Runtime concept.

This script simulates an agent-facing execution loop:
1. run a plan and emit result + critique + replay capsule
2. replay a captured capsule
3. patch the captured plan and rerun analysis

It intentionally uses only the Python standard library so it can be run
without building Bolt or installing extra dependencies.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _stable_hash(parts: List[str]) -> int:
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def _ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


@dataclass
class OperatorMetrics:
    plan_node_id: str
    operator_type: str
    input_rows: int
    raw_input_rows: int
    input_bytes: int
    raw_input_bytes: int
    peak_memory_bytes: int
    wall_time_ms: int
    spill_bytes: int
    shuffle_bytes: int
    parallelism: int

    def to_summary(self) -> Dict[str, Any]:
        return {
            "plan_node_id": self.plan_node_id,
            "operator_type": self.operator_type,
            "input_rows": self.input_rows,
            "raw_input_rows": self.raw_input_rows,
            "input_bytes": self.input_bytes,
            "raw_input_bytes": self.raw_input_bytes,
            "peak_memory_bytes": self.peak_memory_bytes,
            "wall_time_ms": self.wall_time_ms,
            "spill_bytes": self.spill_bytes,
            "shuffle_bytes": self.shuffle_bytes,
            "parallelism": self.parallelism,
        }


class DemoRuntime:
    def __init__(self, memory_limit_bytes: int = 4 * 1024**3) -> None:
        self.memory_limit_bytes = memory_limit_bytes

    def run(
        self,
        plan: Dict[str, Any],
        splits: Optional[Dict[str, Any]],
        query_config: Optional[Dict[str, str]],
        connector_properties: Optional[Dict[str, str]],
        goal: str,
        seed: str,
    ) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        operators = self._compute_metrics(plan, seed)
        critique = self._build_critique(plan, operators, goal)
        result = self._build_result(plan, operators, critique)
        capsule = self._build_capsule(
            plan=plan,
            splits=splits or {},
            query_config=query_config or {},
            connector_properties=connector_properties or {},
            goal=goal,
            seed=seed,
            result=result,
            critique=critique,
            operators=operators,
        )
        return result, critique, capsule

    def _compute_metrics(
        self, plan: Dict[str, Any], seed: str
    ) -> List[OperatorMetrics]:
        operators: List[OperatorMetrics] = []
        for operator in plan.get("operators", []):
            operators.append(self._compute_operator_metrics(plan, operator, seed))
        return operators

    def _compute_operator_metrics(
        self, plan: Dict[str, Any], operator: Dict[str, Any], seed: str
    ) -> OperatorMetrics:
        op_type = operator["operator_type"]
        node_id = str(operator["plan_node_id"])
        raw_rows = int(operator.get("raw_input_rows", operator.get("input_rows", 0)))
        width_bytes = int(operator.get("row_width_bytes", 64))
        selectivity = float(operator.get("selectivity", 1.0))
        distinct_ratio = float(operator.get("distinct_ratio", 0.1))
        groups = max(1, int(raw_rows * distinct_ratio))
        partition_count = max(1, int(operator.get("partition_count", 1)))
        build_rows = int(operator.get("build_rows", raw_rows // 2))
        udf_cost = float(operator.get("udf_cpu_factor", 1.0))
        seed_bias = _stable_hash([seed, plan.get("query_id", "demo"), node_id]) % 11
        input_rows = max(1, int(raw_rows * selectivity))
        raw_bytes = raw_rows * width_bytes
        input_bytes = input_rows * width_bytes
        parallelism = max(1, int(operator.get("parallelism", partition_count)))

        peak_memory = 64 * 1024**2
        wall_time = 5
        spill_bytes = 0
        shuffle_bytes = 0

        if op_type == "TableScan":
            wall_time += input_rows // 1_500_000 + seed_bias
            peak_memory += min(input_bytes // 32, 512 * 1024**2)
        elif op_type == "Filter":
            wall_time += input_rows // 2_500_000 + seed_bias
            peak_memory += min(input_bytes // 64, 256 * 1024**2)
        elif op_type == "Project":
            wall_time += input_rows // 3_000_000 + seed_bias
            peak_memory += min(input_bytes // 128, 256 * 1024**2)
        elif op_type == "HashAggregation":
            grouping_factor = (
                1.4 if operator.get("mode", "single_phase") == "single_phase" else 0.65
            )
            peak_memory += int(
                groups * width_bytes * grouping_factor / max(partition_count, 1)
            )
            wall_time += int(input_rows / 1_200_000 * grouping_factor) + seed_bias
            shuffle_bytes = input_bytes if operator.get("requires_shuffle", True) else 0
            if peak_memory > self.memory_limit_bytes:
                spill_bytes = max(0, peak_memory - self.memory_limit_bytes)
                wall_time += spill_bytes // (256 * 1024**2) * 25
        elif op_type == "HashJoin":
            build_width = int(operator.get("build_row_width_bytes", width_bytes))
            peak_memory += build_rows * build_width // max(partition_count, 1)
            wall_time += input_rows // 900_000 + seed_bias
            shuffle_bytes = input_bytes + build_rows * build_width
            if peak_memory > self.memory_limit_bytes:
                spill_bytes = max(0, peak_memory - self.memory_limit_bytes)
                wall_time += spill_bytes // (256 * 1024**2) * 20
        elif op_type == "PythonNode":
            wall_time += int(input_rows / 800_000 * udf_cost) + seed_bias
            peak_memory += min(input_bytes // 16, 768 * 1024**2)
        elif op_type == "Exchange":
            shuffle_bytes = input_bytes
            wall_time += input_rows // 1_000_000 + seed_bias
            peak_memory += min(input_bytes // 64, 384 * 1024**2)
        else:
            wall_time += input_rows // 2_000_000 + seed_bias
            peak_memory += min(input_bytes // 64, 256 * 1024**2)

        return OperatorMetrics(
            plan_node_id=node_id,
            operator_type=op_type,
            input_rows=input_rows,
            raw_input_rows=raw_rows,
            input_bytes=input_bytes,
            raw_input_bytes=raw_bytes,
            peak_memory_bytes=peak_memory,
            wall_time_ms=wall_time,
            spill_bytes=spill_bytes,
            shuffle_bytes=shuffle_bytes,
            parallelism=parallelism,
        )

    def _build_critique(
        self, plan: Dict[str, Any], operators: List[OperatorMetrics], goal: str
    ) -> Dict[str, Any]:
        dominant = sorted(
            operators,
            key=lambda op: (op.wall_time_ms, op.peak_memory_bytes),
            reverse=True,
        )[:3]

        findings: List[Dict[str, Any]] = []
        global_findings: List[str] = []

        total_shuffle = sum(op.shuffle_bytes for op in operators)
        total_spill = sum(op.spill_bytes for op in operators)
        total_runtime = sum(op.wall_time_ms for op in operators)

        for op in dominant:
            symptoms: List[Tuple[str, List[str], Dict[str, Any]]] = []
            selectivity = _ratio(op.input_rows, op.raw_input_rows)

            if (
                op.operator_type == "HashAggregation"
                and op.peak_memory_bytes > self.memory_limit_bytes
            ):
                symptoms.append(
                    (
                        "high_peak_memory",
                        [
                            "increase_partition_count",
                            "rewrite_to_two_phase_aggregation",
                            "enable_spill",
                        ],
                        {
                            "peak_memory_bytes": op.peak_memory_bytes,
                            "memory_limit_bytes": self.memory_limit_bytes,
                        },
                    )
                )
            if op.operator_type == "HashAggregation" and op.shuffle_bytes > 0:
                symptoms.append(
                    (
                        "shuffle_heavy_aggregation",
                        [
                            "prefer_pre_aggregation_before_shuffle",
                            "increase_local_parallelism",
                        ],
                        {
                            "shuffle_bytes": op.shuffle_bytes,
                            "wall_time_ms": op.wall_time_ms,
                        },
                    )
                )
            if op.operator_type == "TableScan" and selectivity > 0.9:
                symptoms.append(
                    (
                        "low_filter_selectivity",
                        [
                            "check_filter_pushdown",
                            "consider_partition_pruning",
                        ],
                        {
                            "raw_input_rows": op.raw_input_rows,
                            "input_rows": op.input_rows,
                        },
                    )
                )
            if op.operator_type == "PythonNode":
                symptoms.append(
                    (
                        "vectorization_blocked_by_python",
                        [
                            "replace_python_node_with_native_operator",
                            "push_projection_before_python",
                        ],
                        {
                            "wall_time_ms": op.wall_time_ms,
                            "input_rows": op.input_rows,
                        },
                    )
                )
            if (
                op.operator_type == "HashJoin"
                and op.peak_memory_bytes > self.memory_limit_bytes * 0.7
            ):
                symptoms.append(
                    (
                        "join_build_side_large",
                        [
                            "switch_build_probe_sides",
                            "increase_partition_count",
                            "enable_spill",
                        ],
                        {
                            "peak_memory_bytes": op.peak_memory_bytes,
                            "memory_limit_bytes": self.memory_limit_bytes,
                        },
                    )
                )

            for symptom, suggestions, evidence in symptoms:
                findings.append(
                    {
                        "plan_node_id": op.plan_node_id,
                        "operator_type": op.operator_type,
                        "symptom": symptom,
                        "evidence": evidence,
                        "suggested_actions": suggestions,
                    }
                )

        if total_shuffle > sum(op.input_bytes for op in operators):
            global_findings.append("shuffle_dominated_runtime")
        if total_spill > 0:
            global_findings.append("spill_detected")
        if total_runtime > 250:
            global_findings.append("latency_above_interactive_budget")
        if goal == "minimize_latency" and total_spill > 0:
            global_findings.append("goal_conflict_memory_vs_latency")

        return {
            "query_id": plan.get("query_id", "demo_query"),
            "status": "SUCCESS",
            "goal": goal,
            "dominant_operators": [op.to_summary() for op in dominant],
            "operator_findings": findings,
            "global_findings": global_findings,
            "recommended_next_actions": self._recommend_next_actions(
                findings, global_findings
            ),
        }

    def _recommend_next_actions(
        self, findings: List[Dict[str, Any]], global_findings: List[str]
    ) -> List[str]:
        action_order = [
            "rewrite_to_two_phase_aggregation",
            "increase_partition_count",
            "enable_spill",
            "check_filter_pushdown",
            "consider_partition_pruning",
            "replace_python_node_with_native_operator",
        ]
        seen = set()
        recommendations: List[str] = []
        for action in action_order:
            for finding in findings:
                if (
                    action in finding.get("suggested_actions", [])
                    and action not in seen
                ):
                    recommendations.append(action)
                    seen.add(action)
        if (
            "latency_above_interactive_budget" in global_findings
            and "increase_local_parallelism" not in seen
        ):
            recommendations.append("increase_local_parallelism")
        return recommendations[:5]

    def _build_result(
        self,
        plan: Dict[str, Any],
        operators: List[OperatorMetrics],
        critique: Dict[str, Any],
    ) -> Dict[str, Any]:
        total_rows = operators[-1].input_rows if operators else 0
        total_runtime = sum(op.wall_time_ms for op in operators)
        peak_memory = max((op.peak_memory_bytes for op in operators), default=0)
        return {
            "query_id": plan.get("query_id", "demo_query"),
            "status": "SUCCESS",
            "row_count": total_rows,
            "runtime_ms": total_runtime,
            "peak_memory_bytes": peak_memory,
            "dominant_operator": critique["dominant_operators"][0]["operator_type"]
            if critique["dominant_operators"]
            else None,
        }

    def _build_capsule(
        self,
        plan: Dict[str, Any],
        splits: Dict[str, Any],
        query_config: Dict[str, str],
        connector_properties: Dict[str, str],
        goal: str,
        seed: str,
        result: Dict[str, Any],
        critique: Dict[str, Any],
        operators: List[OperatorMetrics],
    ) -> Dict[str, Any]:
        redacted_connector_properties = {
            key: (
                "masked"
                if any(
                    token in key.lower() for token in ("secret", "token", "password")
                )
                else value
            )
            for key, value in connector_properties.items()
        }
        return {
            "capsule_version": 1,
            "query_id": plan.get("query_id", "demo_query"),
            "goal": goal,
            "seed": seed,
            "plan": plan,
            "splits": splits,
            "query_config": query_config,
            "connector_properties": redacted_connector_properties,
            "result": result,
            "critique": critique,
            "trace_summary": [op.to_summary() for op in operators],
        }


def _load_request(plan_path: Path, args: argparse.Namespace) -> Dict[str, Any]:
    request = _read_json(plan_path)
    if args.splits:
        request["splits"] = _read_json(Path(args.splits))
    if args.query_config:
        request["query_config"] = _read_json(Path(args.query_config))
    if args.connector_properties:
        request["connector_properties"] = _read_json(Path(args.connector_properties))
    return request


def _apply_patch_to_plan(
    plan: Dict[str, Any], patch_payload: Dict[str, Any]
) -> Dict[str, Any]:
    patched = copy.deepcopy(plan)
    patch = patch_payload.get("patch", patch_payload)
    op = patch["op"]
    target = str(patch.get("target_plan_node_id", ""))

    if op == "replace_operator":
        replacement = patch["new_operator"]
        for index, operator in enumerate(patched.get("operators", [])):
            if str(operator["plan_node_id"]) == target:
                patched["operators"][index] = replacement
                break
        else:
            raise ValueError(f"target plan node not found: {target}")
    elif op == "update_operator":
        updates = patch["updates"]
        for operator in patched.get("operators", []):
            if str(operator["plan_node_id"]) == target:
                operator.update(updates)
                break
        else:
            raise ValueError(f"target plan node not found: {target}")
    elif op == "retune_aggregation":
        for operator in patched.get("operators", []):
            if (
                str(operator["plan_node_id"]) == target
                and operator["operator_type"] == "HashAggregation"
            ):
                operator["mode"] = patch.get("mode", "two_phase")
                operator["partition_count"] = int(
                    patch.get("partition_count", operator.get("partition_count", 1))
                )
                operator["parallelism"] = int(
                    patch.get("parallelism", operator.get("parallelism", 1))
                )
                break
        else:
            raise ValueError(f"hash aggregation target not found: {target}")
    else:
        raise ValueError(f"unsupported patch op: {op}")

    patched["query_id"] = f"{patched.get('query_id', 'demo_query')}_patched"
    return patched


def cmd_run(args: argparse.Namespace) -> int:
    request = _load_request(Path(args.plan), args)
    runtime = DemoRuntime(memory_limit_bytes=args.memory_limit_gb * 1024**3)
    result, critique, capsule = runtime.run(
        plan=request,
        splits=request.get("splits"),
        query_config=request.get("query_config"),
        connector_properties=request.get("connector_properties"),
        goal=args.goal,
        seed=args.seed,
    )
    if args.result_out:
        _write_json(Path(args.result_out), result)
    if args.critique_out:
        _write_json(Path(args.critique_out), critique)
    if args.capsule_out:
        _write_json(Path(args.capsule_out), capsule)
    print(json.dumps({"status": "SUCCESS", "result": result}, indent=2))
    return 0


def cmd_replay(args: argparse.Namespace) -> int:
    capsule = _read_json(Path(args.capsule))
    runtime = DemoRuntime(memory_limit_bytes=args.memory_limit_gb * 1024**3)
    result, critique, replay_capsule = runtime.run(
        plan=capsule["plan"],
        splits=capsule.get("splits"),
        query_config=capsule.get("query_config"),
        connector_properties=capsule.get("connector_properties"),
        goal=capsule.get("goal", "minimize_latency"),
        seed=capsule.get("seed", "replay-seed"),
    )
    replay_capsule["replay_of"] = capsule.get("query_id")
    if args.result_out:
        _write_json(Path(args.result_out), result)
    if args.trace_out:
        _write_json(
            Path(args.trace_out),
            {
                "query_id": replay_capsule["query_id"],
                "replay_of": replay_capsule["replay_of"],
                "trace_summary": replay_capsule["trace_summary"],
                "critique": critique,
            },
        )
    print(
        json.dumps(
            {
                "status": "SUCCESS",
                "replay_of": capsule.get("query_id"),
                "result": result,
            },
            indent=2,
        )
    )
    return 0


def cmd_patch(args: argparse.Namespace) -> int:
    capsule = _read_json(Path(args.capsule))
    patch_payload = _read_json(Path(args.patch))
    patched_plan = _apply_patch_to_plan(capsule["plan"], patch_payload)
    runtime = DemoRuntime(memory_limit_bytes=args.memory_limit_gb * 1024**3)
    result, critique, patched_capsule = runtime.run(
        plan=patched_plan,
        splits=capsule.get("splits"),
        query_config=capsule.get("query_config"),
        connector_properties=capsule.get("connector_properties"),
        goal=capsule.get("goal", "minimize_latency"),
        seed=capsule.get("seed", "patch-seed"),
    )
    patched_capsule["base_capsule_query_id"] = capsule.get("query_id")
    patched_capsule["applied_patch"] = patch_payload
    if args.result_out:
        _write_json(Path(args.result_out), result)
    if args.critique_out:
        _write_json(Path(args.critique_out), critique)
    if args.capsule_out:
        _write_json(Path(args.capsule_out), patched_capsule)
    print(
        json.dumps(
            {
                "status": "SUCCESS",
                "base_query_id": capsule.get("query_id"),
                "patched_query_id": patched_plan.get("query_id"),
                "result": result,
            },
            indent=2,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bolt Agent Runtime demo CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser(
        "run", help="run a plan and emit critique/capsule"
    )
    run_parser.add_argument("--plan", required=True, help="Path to the plan JSON")
    run_parser.add_argument("--splits", help="Path to split metadata JSON")
    run_parser.add_argument("--query-config", help="Path to query config JSON")
    run_parser.add_argument(
        "--connector-properties", help="Path to connector properties JSON"
    )
    run_parser.add_argument("--goal", default="minimize_latency", help="Execution goal")
    run_parser.add_argument(
        "--seed", default="demo-seed", help="Stable seed for replayability"
    )
    run_parser.add_argument(
        "--memory-limit-gb", type=int, default=4, help="Simulated memory limit"
    )
    run_parser.add_argument("--result-out", help="Where to write result JSON")
    run_parser.add_argument("--critique-out", help="Where to write critique JSON")
    run_parser.add_argument("--capsule-out", help="Where to write replay capsule JSON")
    run_parser.set_defaults(func=cmd_run)

    replay_parser = subparsers.add_parser("replay", help="replay a captured capsule")
    replay_parser.add_argument(
        "--capsule", required=True, help="Path to a capsule JSON"
    )
    replay_parser.add_argument(
        "--memory-limit-gb", type=int, default=4, help="Simulated memory limit"
    )
    replay_parser.add_argument("--result-out", help="Where to write replay result JSON")
    replay_parser.add_argument("--trace-out", help="Where to write replay trace JSON")
    replay_parser.set_defaults(func=cmd_replay)

    patch_parser = subparsers.add_parser(
        "patch", help="apply a patch to a captured capsule"
    )
    patch_parser.add_argument("--capsule", required=True, help="Path to a capsule JSON")
    patch_parser.add_argument("--patch", required=True, help="Path to a patch JSON")
    patch_parser.add_argument(
        "--memory-limit-gb", type=int, default=4, help="Simulated memory limit"
    )
    patch_parser.add_argument("--result-out", help="Where to write patched result JSON")
    patch_parser.add_argument(
        "--critique-out", help="Where to write patched critique JSON"
    )
    patch_parser.add_argument(
        "--capsule-out", help="Where to write patched capsule JSON"
    )
    patch_parser.set_defaults(func=cmd_patch)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
