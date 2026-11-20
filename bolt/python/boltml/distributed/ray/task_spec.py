# Copyright (c) ByteDance Ltd. and/or its affiliates
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

from dataclasses import dataclass, field
from typing import Any

from pybolt import exportToArrow, fileFormatToString

from .udf_spec import UdfRegistry
from .runtime_summary import RuntimeExecutionStage


def _emptyUdfRegistry() -> UdfRegistry:
    """Default ``UdfRegistry`` for ``StageTaskSpec.udfRegistry``.

    A module-level helper rather than a lambda so the dataclass
    default_factory shows a stable name in repr / pickle.
    """
    return UdfRegistry()


def _encode_expression(expr) -> dict[str, Any] | None:
    if hasattr(expr, "inner"):
        return _encode_expression(expr.inner())
    literal = expr.asLiteral() if hasattr(expr, "asLiteral") else None
    if literal is not None:
        return {"kind": "literal", "value": literal.value}
    if hasattr(expr, "name") and expr.__class__.__name__ == "FieldExpression":
        return {"kind": "field", "name": expr.name}
    if (
        expr.__class__.__name__ == "BooleanExpression"
        and getattr(expr, "rhs", None) is not None
    ):
        lhs = _encode_expression(expr.lhs)
        rhs = _encode_expression(expr.rhs)
        if lhs is None or rhs is None:
            return None
        return {"kind": "boolean_compare", "op": expr.op.value, "lhs": lhs, "rhs": rhs}
    return None


def _encode_source(source) -> dict[str, Any] | None:
    if source.kind == "values":
        return {
            "kind": source.kind,
            "table": exportToArrow(source.args[0]),
        }
    if source.kind == "tpch_read":
        table, columns, scaleFactor, numParts = source.args
        return {
            "kind": source.kind,
            "table": table.name if hasattr(table, "name") else table,
            "columns": tuple(columns),
            "scaleFactor": scaleFactor,
            "numParts": numParts,
        }
    if source.kind == "single_file_read":
        # KNOWN-BROKEN GATE: encoding this source kind flips leaf stages
        # to remoteCapable=True and they get dispatched to Ray workers.
        # Correctness is proven for most queries but TPC-H Q10 (4-way
        # join, unfiltered customer scan) produces wrong rows on this
        # path; the off-by-something has not been root-caused. Default
        # stays driver-local-leaf for safety. Set ``BOLTML_LEAF_REMOTE=1``
        # to opt into distributed leaves on shapes you have validated;
        # do not remove this gate without also fixing Q10. The matching
        # test ``test_leaf_remote_gate.py`` locks this behaviour so
        # accidental gate removal trips immediately.
        # See ``docs/boltml_pipeline.md`` § "Known correctness gates".
        import os

        if os.environ.get("BOLTML_LEAF_REMOTE") != "1":
            return None
        filePath, outputType, fileFormat = source.args
        return {
            "kind": source.kind,
            "output_names": tuple(outputType.names()),
            "output_types": tuple(
                str(outputType.childAt(i)) for i in range(len(outputType))
            ),
            "file_format": fileFormatToString(fileFormat),
            "file_path": filePath,
        }
    return None


def _encode_logical_plan(plan) -> dict[str, Any] | None:
    source = _encode_source(plan.source)
    if source is None:
        return None
    operations = _encode_logical_ops(plan.operations)
    if operations is None:
        return None
    return {"source": source, "operations": operations}


def _encode_logical_ops(operations) -> tuple[dict[str, Any], ...] | None:
    encoded = []
    for op in operations:
        if op.kind == "join":
            rhsPlan = _encode_logical_plan(op.args[0])
            if rhsPlan is None:
                return None
            encoded.append(
                {
                    "kind": "join",
                    "rhs_plan": rhsPlan,
                    "lhs_keys": tuple(op.args[1]),
                    "rhs_keys": tuple(op.args[2]),
                    "output_layout": tuple(op.args[3]),
                    "join_type": op.args[4].name
                    if hasattr(op.args[4], "name")
                    else str(op.args[4]),
                }
            )
            continue
        if op.kind == "aggregate":
            groupingKeys, aggregations = op.args
            encodedAggregations = []
            for expr in aggregations:
                fn = expr.inner()
                argumentNames = []
                for arg in fn.arguments:
                    if not hasattr(arg, "name"):
                        return None
                    argumentNames.append(arg.name)
                encodedAggregations.append(
                    {
                        "alias": expr.name,
                        "function": fn.name,
                        "arguments": tuple(argumentNames),
                    }
                )
            encoded.append(
                {
                    "kind": "aggregate",
                    "grouping_keys": tuple(groupingKeys),
                    "aggregations": tuple(encodedAggregations),
                }
            )
            continue
        if op.kind == "project":
            columns = []
            fieldsOnly = True
            for expr in op.args[0]:
                inner = expr.inner()
                if inner.__class__.__name__ != "FieldExpression":
                    fieldsOnly = False
                    break
                columns.append({"source": inner.name, "alias": expr.name})
            if fieldsOnly:
                encoded.append({"kind": "project_fields", "columns": tuple(columns)})
            else:
                # Project that carries arbitrary expressions (e.g. a transform
                # whose output is a FunctionExpression). We can't round-trip the
                # expression through the remote-op schema, but the real
                # execution uses stage.consumerSubstraitPlanBytes which has no
                # such limit. Emit a placeholder so the partition-parallel
                # gate knows this op is partition-local (every project is).
                encoded.append({"kind": "project_generic"})
            continue
        if op.kind == "filter":
            predicate = _encode_expression(op.args[0])
            if predicate is None or predicate.get("kind") != "boolean_compare":
                return None
            encoded.append(
                {
                    "kind": "filter_compare",
                    "op": predicate["op"],
                    "lhs": predicate["lhs"],
                    "rhs": predicate["rhs"],
                }
            )
            continue
        if op.kind == "order_by":
            encoded.append(
                {
                    "kind": "order_by",
                    "columns": tuple(
                        (name, order.name if hasattr(order, "name") else str(order))
                        for name, order in op.args[0]
                    ),
                }
            )
            continue
        if op.kind == "limit":
            encoded.append({"kind": "limit", "count": op.args[0], "offset": op.args[1]})
            continue
        return None
    return tuple(encoded)


@dataclass(frozen=True)
class StageTaskSpec:
    """Task-spec for a Ray-runnable stage. Every spec carries
    Substrait bytes; the runtime executes them via
    ``runStageSubstraitBytes``.
    """

    stageId: str
    executionKind: str
    taskKind: str = "leaf_producer"
    dependencies: tuple[str, ...] = ()
    inputPlaceholders: tuple["PlaceholderRead", ...] = ()  # noqa: F821
    executionId: str = ""
    attemptId: int = 1
    partitionId: int = 0
    partitionCount: int = 1
    partitioning: str = "singleton"
    inputPartitionCounts: tuple[int, ...] = ()
    inputPartitionings: tuple[str, ...] = ()
    executionMode: str = "staged_local"
    substraitPlanBytes: bytes | None = None
    expectedOutputNames: tuple[str, ...] = ()
    remoteCapable: bool = False
    fallbackReason: str | None = None
    # Opt-in leaf-stage parallelization. When the runtime fans
    # one logical leaf out across N parallel Ray tasks (via
    # ``BOLTML_LEAF_PARALLELISM > 1``), each task gets a different
    # ``leafShardIndex`` (0..leafShardCount-1) and inside
    # ``_execute_substrait_bytes`` filters input splits via
    # ``splits[index::count]``. Defaults of (0, 1) mean "one task per
    # leaf, processes every split" — the prior behaviour.
    #
    # Two correctness guards in the worker:
    #   * empty shard (all scan nodes received 0 splits after slicing)
    #     → return an empty Arrow table matching the plan output type
    #     instead of executing; avoids Bolt's kSerial assertion that
    #     every scan node receives ``noMoreSplits`` (only triggered
    #     for non-empty add lists).
    #   * task name uniqueness — the ``BoltTaskExecutor`` keeps a
    #     process-wide memory pool registry keyed by task name, so
    #     when N shards land in the same Ray worker the bare
    #     ``"RemoteWorkerTask"`` name collides; sharded runs append
    #     the index to make each task name unique.
    leafShardIndex: int = 0
    leafShardCount: int = 1
    # Driver-registered UDFs the remote worker must re-register on its
    # own pybolt singleton before executing ``substraitPlanBytes``. The
    # Substrait plan only carries function-extension references by name;
    # without re-registration the worker sees no implementation for
    # those names and execution fails in C++. Defaults to an empty
    # registry so non-Ray paths and tests that don't use UDFs are
    # unaffected. See ``boltml/ray/udf_spec.py``.
    udfRegistry: "UdfRegistry" = field(  # noqa: F821
        default_factory=lambda: _emptyUdfRegistry()
    )

    @staticmethod
    def fromStageNode(
        stage: "StageNode",  # noqa: F821
        executionMode: str = "staged_local",
    ) -> "StageTaskSpec":
        if not stage.inputPlaceholders:
            taskKind = (
                "partitioned_producer" if stage.partitionCount > 1 else "leaf_producer"
            )
        elif len(stage.inputPlaceholders) == 1:
            taskKind = "single_input_consumer"
        elif len(stage.inputPlaceholders) == 2:
            taskKind = "multi_input_consumer"
        else:
            taskKind = "consumer"

        # Every stage is Substrait-native: producer stages carry
        # ``producerSubstraitPlanBytes``; consumer stages carry
        # ``consumerSubstraitPlanBytes`` plus ``inputPlaceholders``.
        substraitPlanBytes = None
        expectedOutputNames = tuple(stage.outputNames)
        remoteCapable = False
        fallbackReason = None
        if stage.inputPlaceholders:
            substraitPlanBytes = stage.consumerSubstraitPlanBytes
            if substraitPlanBytes is None:
                fallbackReason = "consumer_stage_no_substrait"
            else:
                remoteCapable = True
        elif stage.producerSubstraitPlanBytes is not None:
            substraitPlanBytes = stage.producerSubstraitPlanBytes
            remoteCapable = True
            # The legacy ``_encode_source`` path enforces a
            # ``BOLTML_LEAF_REMOTE=1`` opt-in for the
            # ``single_file_read`` source kind because Q10 produced
            # wrong results on that LEGACY logical-IR path. The
            # Substrait producer path is a separate code path with
            # independent correctness coverage
            # (``test_tpcds_parity::testBootstrapSubsetSourceBacked
            # ParityLocalVsRemoteLeaf`` is the canonical
            # local-vs-remote diff check); the prior tests proved it
            # safe for single-file reads. Don't reapply the legacy
            # gate here.

        return StageTaskSpec(
            stageId=stage.stageId,
            executionKind="consumer" if stage.inputPlaceholders else "producer",
            taskKind=taskKind,
            dependencies=stage.dependencies,
            inputPlaceholders=stage.inputPlaceholders,
            partitionCount=stage.partitionCount,
            partitioning=stage.partitioning,
            inputPartitionCounts=tuple(
                placeholder.partitionCount for placeholder in stage.inputPlaceholders
            ),
            inputPartitionings=tuple(
                placeholder.partitioning for placeholder in stage.inputPlaceholders
            ),
            executionMode=executionMode,
            substraitPlanBytes=substraitPlanBytes,
            expectedOutputNames=expectedOutputNames,
            remoteCapable=remoteCapable,
            fallbackReason=fallbackReason,
        )


@dataclass(frozen=True)
class StageTaskResult:
    stageId: str
    attempt: int
    inputCount: int
    outputNames: tuple[str, ...]
    partitionCount: int = 1
    exchangeIds: tuple[str, ...] = ()
    transport: str | None = None
    retryReason: str | None = None
    remote: bool = False
    remoteTaskCount: int = 0
    workerId: str | None = None
    nodeId: str | None = None
    workerIds: tuple[str, ...] = ()
    nodeIds: tuple[str, ...] = ()
    fallbackReason: str | None = None
    # Profiling fields. ``startNs`` / ``endNs`` are ``time.monotonic_ns()``
    # readings from the driver around the stage's main work; ``publishStartNs``
    # / ``publishEndNs`` bracket the exchange-publish step (None for stages
    # that don't publish, e.g. terminal collect, or for multi-worker
    # partitioned-remote stages where the worker-side monotonic clocks
    # can't be combined into one timeline — see ``publishWallNs``).
    # ``publishWallNs`` is the sum of per-task ``end - start`` durations
    # across all tasks of a stage; safe to compare/aggregate because it's
    # a duration in nanoseconds, not a worker-clock timestamp. It's the
    # canonical "publish wall time" metric for multi-worker stages where
    # ``publishStartNs``/``publishEndNs`` are intentionally None.
    startNs: int | None = None
    endNs: int | None = None
    publishStartNs: int | None = None
    publishEndNs: int | None = None
    publishWallNs: int | None = None

    def toRuntimeExecutionStage(self) -> RuntimeExecutionStage:
        return RuntimeExecutionStage(
            stageId=self.stageId,
            attempt=self.attempt,
            inputCount=self.inputCount,
            outputNames=self.outputNames,
            partitionCount=self.partitionCount,
            exchangeIds=self.exchangeIds,
            transport=self.transport,
            retryReason=self.retryReason,
            remote=self.remote,
            remoteTaskCount=self.remoteTaskCount,
            workerId=self.workerId,
            nodeId=self.nodeId,
            workerIds=self.workerIds,
            nodeIds=self.nodeIds,
            fallbackReason=self.fallbackReason,
            startNs=self.startNs,
            endNs=self.endNs,
            publishStartNs=self.publishStartNs,
            publishEndNs=self.publishEndNs,
            publishWallNsExplicit=self.publishWallNs,
        )
