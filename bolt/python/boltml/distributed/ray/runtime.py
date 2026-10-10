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

from dataclasses import dataclass, replace
from enum import Enum
import os
import re
import shutil
import tempfile
import time
from uuid import uuid4

import pyarrow as pa
import ray

from ...dataframe import DataFrame
from ...logging import boltmlDebugEnabled, boltmlDebugLog, boltmlTraceLog
from .runtime_summary import RuntimeExecutionSummary
from .exchange_manager import AdaptiveExchangeManager
from .scheduler import RayLeafScheduler
from .task_spec import StageTaskSpec
from .task_spec import StageTaskResult


def _is_retryable_ray_error(error: BaseException) -> bool:
    """Return True for Ray *infrastructure* failures that warrant a retry.

    Distinguishes between:

    * ``WorkerCrashedError`` / ``ActorDiedError`` / ``ObjectLostError``
      / ``NodeDiedError`` (and similar) — the worker or its node went
      away, the data wasn't lost on disk yet, a fresh submission on a
      different worker has a real chance of succeeding.
    * ``RayTaskError`` — the user-supplied task body raised a Python
      exception inside the worker. Re-submitting will reproduce the
      same exception; we don't retry these. The wrapped cause is
      what the user actually needs to see.

    Anything we can't classify is treated as non-retryable so we don't
    mask programming bugs as "transient infra blips". Tests that want
    to inject a retryable failure should raise one of the Ray classes
    above (or ``TransientStageError`` for the synthetic test path).
    """
    try:
        from ray.exceptions import (
            ActorDiedError,
            ActorUnavailableError,
            NodeDiedError,
            ObjectLostError,
            OwnerDiedError,
            RayTaskError,
            WorkerCrashedError,
        )
    except ImportError:
        # Old Ray versions may not expose every class above. Anything we
        # can import is fine; the rest just won't be classified
        # retryable (and the import error itself is not retryable).
        return False

    if isinstance(error, RayTaskError):
        # User-task exception — retrying reproduces it. Fatal.
        return False
    return isinstance(
        error,
        (
            WorkerCrashedError,
            ActorDiedError,
            ActorUnavailableError,
            ObjectLostError,
            OwnerDiedError,
            NodeDiedError,
        ),
    )


def _dataframe_from_typed_arrow(arrow_table) -> "DataFrame":
    """Thin shim that delegates to the shared ``_typed_arrow`` helper.

    Historical name kept for in-process callers; new code should call
    :func:`._typed_arrow.dataframe_from_typed_arrow` directly. The
    canonical implementation lives there so the exchange load path
    and the remote-task return path decode bolt-type metadata
    identically.
    """
    from ._typed_arrow import dataframe_from_typed_arrow

    return dataframe_from_typed_arrow(arrow_table)


class TransientStageError(RuntimeError):
    pass


class RayExecutionMode(str, Enum):
    STAGED_LOCAL = "staged_local"
    REMOTE_LEAF = "remote_leaf"


class RayWorkerMode(str, Enum):
    TASK = "task"
    ACTOR = "actor"


class RayRolloutLevel(str, Enum):
    STAGED_LOCAL = "staged_local"
    REMOTE_LEAF = "remote_leaf"
    REMOTE_ANALYTICAL = "remote_analytical"


@dataclass(frozen=True)
class RayExecutionConfig:
    mode: RayExecutionMode = RayExecutionMode.STAGED_LOCAL
    workerMode: RayWorkerMode = RayWorkerMode.TASK
    rolloutLevel: RayRolloutLevel | None = None


def _rowVectorFromTable(table):
    """Materialise ``table`` (``pa.Table`` / ``pa.RecordBatch`` /
    ``pa.StructArray``) as a Bolt ``RowVector``.

    Used by ``_vectorizedBucketAssignments`` to feed pybolt's
    HashPartitionFunction binding without reaching back into the
    DataFrame / Substrait stack — we already have an Arrow value here
    and need the row vector view of it just for the hash computation.

    ``pa.Table.to_struct_array()`` returns a ``ChunkedArray`` (one chunk
    per table chunk); ``importFromArrow`` only accepts a single
    ``StructArray``. Combine chunks at the table level first and pull
    chunk 0 from the resulting single-chunk ChunkedArray. Routes the
    other Arrow flavours through ``importFromArrow`` directly.
    """
    import pyarrow as pa
    from pybolt import importFromArrow

    if isinstance(table, pa.Table):
        chunked = table.combine_chunks().to_struct_array()
        if isinstance(chunked, pa.ChunkedArray):
            chunked = (
                chunked.chunk(0)
                if chunked.num_chunks == 1
                else chunked.combine_chunks()
            )
        return importFromArrow(chunked)
    if isinstance(table, pa.RecordBatch):
        return importFromArrow(table.to_struct_array())
    return importFromArrow(table)


def _vectorizedBucketAssignments(table, columns, bucketCount: int, seed: int = 0):
    """Row → bucket assignment for boltml's exchange split.

    Wraps Bolt's ``HashPartitionFunction`` (via the
    ``pybolt.computePartitionIndices`` / ``computeRowHashes`` bindings)
    so the assignment is byte-identical to what Bolt's own exchange
    operators would produce for the same ``(rowType, keyChannels,
    numPartitions)``. Doing the hashing in C++ also drops the previous
    ``xxhash`` Python dep.

    Two partition shapes:

    * ``hash(col1, col2, ...)`` (``seed=0``): straight call into
      ``HashPartitionFunction``.
    * ``shuffle(seed=N)`` (``seed != 0``): boltml's driver-only
      reshuffle. Bolt has no native "shuffle with seed" partitioner,
      so we get the raw row hashes from Bolt's ``VectorHasher`` and
      mix the seed in numpy before bucketing. The hash itself still
      uses Bolt's ``bits::hashMix``; only the seed combination is
      Python-side.
    """
    import numpy as np
    import pybolt

    bucketCount = max(1, int(bucketCount))
    # 0-row tables fall out before the rowvec conversion: there are no
    # rows to bucket, and ``pa.Table.to_struct_array()`` raises
    # ``ArrowInvalid: cannot construct ChunkedArray from empty vector
    # and omitted type`` on a 0-chunk table because pyarrow can't
    # infer the chunk type. This case fires legitimately on the worker
    # side when a consumer task processes a partition that has zero
    # rows (e.g. an empty bucket left by an earlier hash split) and
    # then needs to re-publish for downstream stages.
    if len(table) == 0:
        return np.empty(0, dtype=np.int32)
    rowVec = _rowVectorFromTable(table)
    rowType = rowVec.dtype()
    names = list(rowType.names())
    keyChannels = [names.index(name) for name in columns]

    if seed == 0:
        # Pure hash partitioning: HashPartitionFunction returns the
        # bucket index per row directly. No Python-side mixing needed
        # — what we get back is what Bolt would produce.
        partitions = pybolt.computePartitionIndices(rowVec, keyChannels, bucketCount)
        return np.asarray(partitions, dtype=np.int32)

    # ``shuffle(seed=N)`` path: hash all key columns, XOR-salt with the
    # seed, then mod. XOR is a deterministic, reversible mix that
    # preserves the bit-distribution of the input hash and gives
    # different bucket assignments for different seeds — exactly the
    # property the ``df.shuffle(seed=N)`` API promises. We do this
    # combination in Python (rather than another C++ binding) because
    # Bolt has no seeded-partitioner concept; the only consistency we
    # need here is driver↔worker, both of which run this same helper.
    rawHashes = np.asarray(
        pybolt.computeRowHashes(rowVec, keyChannels), dtype=np.uint64
    )
    seeded = rawHashes ^ np.uint64(seed)
    return (seeded % np.uint64(bucketCount)).astype(np.int32, copy=False)


def _splitProducedResult(result, partitioning: str, partitionCount: int = 2):
    dataframe = DataFrame(result)
    # Attach Bolt type metadata before splitting so Table.filter preserves
    # the declared types for every partition and its exchange descriptor.
    from ._typed_arrow import (
        arrow_table_with_bolt_types,
        dataframe_from_typed_arrow,
    )

    table = arrow_table_with_bolt_types(dataframe)
    bucketCount = max(1, partitionCount)
    if len(table) <= 1 and bucketCount == 1:
        return (dataframe,)

    if partitioning.startswith("hash("):
        match = re.fullmatch(r"hash\((.*)\)", partitioning)
        if match is None:
            return (dataframe,)
        keys = [key.strip() for key in match.group(1).split(",") if key.strip()]
        if not keys:
            return (dataframe,)
        bucketAssignments = _vectorizedBucketAssignments(table, keys, bucketCount)
    elif partitioning.startswith("shuffle(seed="):
        match = re.fullmatch(r"shuffle\(seed=(\d+)\)", partitioning)
        if match is None:
            return (dataframe,)
        seed = int(match.group(1))
        bucketAssignments = _vectorizedBucketAssignments(
            table, list(table.schema.names), bucketCount, seed=seed
        )
    else:
        return (dataframe,)

    # Capture the original schema for constructing empty buckets.
    boltDtype = dataframe.dtype
    boltTypeStrs = tuple(str(boltDtype.childAt(i)) for i in range(len(boltDtype)))
    boltNames = tuple(boltDtype.names())
    partitions = []
    for bucket in range(bucketCount):
        mask = pa.array(bucketAssignments == bucket)
        partitionTable = table.filter(mask)
        if partitionTable.num_rows == 0:
            # Table.to_struct_array() can fail on zero-row tables. Build
            # the empty bucket with explicit Bolt types; use the Arrow
            # schema as a fallback for types the parser cannot restore.
            from ._typed_arrow import empty_dataframe_with_bolt_types

            try:
                partitions.append(
                    empty_dataframe_with_bolt_types(boltNames, boltTypeStrs)
                )
                continue
            except Exception:  # noqa: BLE001
                empty_struct = pa.StructArray.from_arrays(
                    [pa.array([], type=f.type) for f in partitionTable.schema],
                    fields=list(partitionTable.schema),
                )
                partitions.append(DataFrame(empty_struct))
        else:
            # Reconstruct non-empty buckets via the typed-Arrow decoder
            # so custom bolt types preserved on the schema metadata
            # (attached by ``arrow_table_with_bolt_types`` above) are
            # rebuilt instead of degrading to Arrow defaults. Falls
            # back to the descriptor's ``outputTypes`` via the
            # ``fallback_types`` arg when the metadata is absent (the
            # legacy descriptor path, kept for safety).
            partitions.append(
                dataframe_from_typed_arrow(partitionTable, fallback_types=boltTypeStrs)
            )
    return tuple(partitions)


class PartitionedRemoteFailure(Exception):
    """Carrier exception raised by ``_runPartitioned*`` methods when at
    least one shard failed but others may have published artifacts
    that need driver-side cleanup before the original error
    propagates.

    Attributes:
        original: the underlying exception from the failing shard.
        partialResults: tuple of (RemoteLeafResult | None) — same
            length as the original submission, with ``None`` for
            failed shards and the ``RemoteLeafResult`` for successful
            ones. The caller's cleanup path ingests these and calls
            ``exchangeManager.cleanup`` so stranded artifacts get
            unlinked / unregistered before the retry attempt starts
            a fresh batch.
    """

    def __init__(self, original: BaseException, partialResults: tuple):
        super().__init__(repr(original))
        self.original = original
        self.partialResults = partialResults


def _cleanup_partial_remote_publish(remoteResults, exchangeManager) -> None:
    """Best-effort cleanup of artifacts published by successful remote
    siblings when a fail-fast partitioned-remote batch had at least
    one shard fail.

    Without this, a failed shard would propagate via the driver's
    ``ray.get`` raise, and the siblings that already wrote parquet
    files / registered Ray objects would have those artifacts stranded
    (the driver never reaches the ``ingestDescriptors`` line that
    transfers ownership to ``exchangeManager``, so cleanup can't see
    them either). The retry path then re-publishes on a fresh
    ``attemptId`` and the leaked artifacts pile up.

    Strategy: ingest the siblings' descriptors as owned, then use
    ``cleanupDescriptors`` (scoped to just those descriptors) to
    remove their artifacts. The earlier implementation called
    ``cleanup(executionId=...)`` which is dangerous — it deletes
    every owned exchange for that execution, including upstream
    exchanges the retry still depends on. ``cleanupDescriptors``
    deletes only the failed batch's stage / exchange / object keys.

    Idempotent: descriptors that were already ingested are no-ops on
    re-ingest; missing files/objects are best-effort.
    """
    if exchangeManager is None:
        return
    all_descriptors = []
    for r in remoteResults:
        if r is None:
            continue
        descriptors = _remote_published_descriptors(r)
        if descriptors:
            try:
                exchangeManager.ingestDescriptors(descriptors, owned=True)
            except Exception:  # noqa: BLE001
                # Best-effort: missing/duplicate descriptors don't
                # block cleanup.
                pass
            all_descriptors.extend(descriptors)
    if not all_descriptors:
        return
    if not hasattr(exchangeManager, "cleanupDescriptors"):
        return
    try:
        exchangeManager.cleanupDescriptors(all_descriptors)
    except Exception:  # noqa: BLE001
        pass


def _mergeResolvedPartitions(inputs):
    if len(inputs) == 1:
        return inputs[0]
    # Preserve the declared Bolt types through concatenation by attaching
    # metadata to each input and using the shared decoder for the result.
    from ._typed_arrow import arrow_table_with_bolt_types, dataframe_from_typed_arrow

    tables = [arrow_table_with_bolt_types(dataframe) for dataframe in inputs]
    merged = pa.concat_tables(tables)
    fallback_types = [
        str(inputs[0].dtype.childAt(i)) for i in range(len(inputs[0].dtype))
    ]
    return dataframe_from_typed_arrow(merged, fallback_types=fallback_types)


def _group_descriptors_by_partition(descriptors, expectedPartitionCount: int):
    # Broadcast routing. When ALL descriptors come from a
    # broadcast exchange (partitioning starts with "broadcast" and
    # partitionCount==1), every consumer partition gets the SAME full
    # set. The broadcast producer wrote one logical output (1 file /
    # 1 object); each consumer task with partitionId K should read
    # that one descriptor regardless of K.
    if descriptors and all(
        d.partitionCount == 1 and (d.partitioning or "").startswith("broadcast")
        for d in descriptors
    ):
        broadcast = tuple(descriptors)
        return [broadcast for _ in range(max(1, expectedPartitionCount))]

    partitionCount = (
        max(
            expectedPartitionCount,
            *(descriptor.partitionCount for descriptor in descriptors),
        )
        if descriptors
        else expectedPartitionCount
    )
    partitionCount = max(1, partitionCount)
    descriptorsByPartition = {descriptor.partitionId: [] for descriptor in descriptors}
    for descriptor in descriptors:
        if descriptor.partitionCount != partitionCount:
            raise RuntimeError(
                f"Descriptor {descriptor.stageId}/{descriptor.exchangeId}/part-{descriptor.partitionId} "
                f"reported partition count {descriptor.partitionCount}, expected {partitionCount}."
            )
        if descriptor.partitionId < 0 or descriptor.partitionId >= partitionCount:
            raise RuntimeError(
                f"Descriptor {descriptor.stageId}/{descriptor.exchangeId} has out-of-range partition id {descriptor.partitionId}."
            )
        descriptorsByPartition.setdefault(descriptor.partitionId, []).append(descriptor)
    if descriptors:
        missingPartitions = [
            partitionId
            for partitionId in range(partitionCount)
            if partitionId not in descriptorsByPartition
        ]
        if missingPartitions:
            raise RuntimeError(
                f"Exchange descriptors are missing partition ids {tuple(missingPartitions)} for partition count {partitionCount}."
            )
    groups = []
    for partitionId in range(partitionCount):
        groups.append(tuple(descriptorsByPartition.get(partitionId, ())))
    return tuple(groups)


def _hash_partitioning(keys) -> str:
    return f"hash({','.join(keys)})"


def _substrait_has_non_local_suffix(substraitPlanBytes: bytes) -> bool:
    """True iff the consumer subtree's chain above the join/aggregate
    contains ops (sort, fetch, aggregate-without-keys) that need a
    global view of the data — i.e. ops that cannot be partition-parallel-
    decomposed and emit per-shard results.
    """
    if not substraitPlanBytes:
        return False
    from substrait.proto import plan as plan_pb2

    p = plan_pb2.Plan()
    try:
        p.ParseFromString(substraitPlanBytes)
    except Exception:
        return False
    if not p.relations or not p.relations[0].HasField("root"):
        return False
    rel = p.relations[0].root.input
    while True:
        kind = rel.WhichOneof("rel_type")
        if kind in _PARTITION_PARALLEL_SAFE_SUFFIX:
            inner = getattr(rel, kind)
            if not inner.HasField("input"):
                return False
            rel = inner.input
            continue
        if kind in (
            "aggregate",
            "hash_join",
            "join",
            "merge_join",
            "nested_loop_join",
            "cross",
        ):
            return False
        return True


def _substrait_consumer_root(substraitPlanBytes: bytes):
    """Walk past placeholder ReadRels in *substraitPlanBytes* and return the
    consumer's root operator (aggregate / hash_join / etc.). Returns
    ``None`` if the plan cannot be parsed or has no recognizable root.
    """
    if not substraitPlanBytes:
        return None
    from substrait.proto import plan as plan_pb2

    p = plan_pb2.Plan()
    try:
        p.ParseFromString(substraitPlanBytes)
    except Exception:
        return None
    if not p.relations or not p.relations[0].HasField("root"):
        return None
    rel = p.relations[0].root.input
    # Walk through any single-input wrappers (project/filter/fetch/sort)
    # to find the partition-parallel-relevant root.
    while True:
        kind = rel.WhichOneof("rel_type")
        if kind in (
            "aggregate",
            "hash_join",
            "join",
            "merge_join",
            "nested_loop_join",
            "cross",
        ):
            return rel
        if kind in ("project", "filter", "fetch", "sort", "extension_single", "expand"):
            inner = getattr(rel, kind)
            if inner.HasField("input"):
                rel = inner.input
                continue
        return None


def _hash_partitioning_from_field_indices(rel, field_indices):
    """Render ``hash(col1,col2)`` from ``base_schema.names`` + indices."""
    # We don't have schema names here without walking deeper; the caller
    # supplies them via the placeholder's outputNames.
    return None


def _hash_partitioning_keys(scattering: str) -> "tuple[str, ...] | None":
    """Parse ``"hash(a, b, c)"`` into ``("a", "b", "c")``.

    Returns ``None`` for any non-``hash(...)`` form (singleton,
    round-robin, broadcast, malformed) — used by the partition-parallel
    safety gate to refuse fan-out when the upstream partitioning isn't
    a hash on a known key tuple.
    """
    if not (scattering.startswith("hash(") and scattering.endswith(")")):
        return None
    inner = scattering[len("hash(") : -1].strip()
    if not inner:
        return ()
    return tuple(part.strip() for part in inner.split(",") if part.strip())


def _aggregate_grouping_key_names(
    consumer_root, input_column_names: "tuple[str, ...]"
) -> "tuple[str, ...] | None":
    """Resolve a Substrait ``AggregateRel``'s grouping field-references
    to column names in the aggregate's *input* row type.

    Handles the common case where the aggregate sits directly on top
    of the consumer's input placeholder. Returns ``None`` for anything
    we can't prove safe — e.g. a project/filter intervening between
    placeholder and aggregate that could rename or reorder columns
    (the conservative answer: treat as "can't prove equivalence" so
    the safety gate falls back to single-task execution).

    The grouping references come from one of two Substrait shapes:
    * ``aggregate.groupings[*].grouping_expressions`` (older), each a
      full ``Expression`` that must be a field selection.
    * ``aggregate.grouping_expressions`` + ``groupings[*]
      .expression_references`` (newer), where references are integer
      indices into ``grouping_expressions``.
    """
    if consumer_root.WhichOneof("rel_type") != "aggregate":
        return None
    agg = consumer_root.aggregate
    # Build the list of grouping expressions we need to resolve.
    expressions = []
    if list(agg.grouping_expressions):
        expressions = list(agg.grouping_expressions)
        # Each grouping selects a subset; for partition-parallel safety
        # we require that *every* grouping selects the same set of keys.
        # In practice BoltML lowering emits one Grouping per aggregate.
        for grouping in agg.groupings:
            for ref in grouping.expression_references:
                if not (0 <= ref < len(expressions)):
                    return None
        used = expressions
    else:
        # Old shape: grouping_expressions live inside each Grouping.
        seen = []
        for grouping in agg.groupings:
            for expr in grouping.grouping_expressions:
                seen.append(expr)
        used = seen

    names: list[str] = []
    for expr in used:
        if not expr.HasField("selection"):
            return None
        # Field selection — only direct-reference into the input row is
        # supported here (no struct projection, no masked references).
        sel = expr.selection
        if not sel.HasField("direct_reference"):
            return None
        ref = sel.direct_reference
        if not ref.HasField("struct_field"):
            return None
        idx = ref.struct_field.field
        if not (0 <= idx < len(input_column_names)):
            return None
        names.append(input_column_names[idx])
    return tuple(names)


def _consumer_is_trivial(taskSpec) -> bool:
    """True iff the consumer plan in ``taskSpec`` carries no aggregate
    or join — it's just a shuffle / sort / project / fetch chain over
    the input placeholders.

    Used by ``_remoteAllowed`` to keep trivial consumers off the
    Ray-task path. They can't be partition-parallel (``non_local_suffix``)
    AND have no real compute that benefits from running on a worker
    instead of the driver, so single-task remote dispatch is pure
    overhead vs. driver-local execution. The driver already has the
    exchange descriptors resolved at this point and can read the
    partitions in-process.

    Returns ``False`` for empty / unparsable plans (caller's other
    guards handle those — we just don't want to gate them here).
    """
    if not taskSpec.substraitPlanBytes:
        return False
    return _substrait_consumer_root(taskSpec.substraitPlanBytes) is None


def _partition_parallel_reason(taskSpec: StageTaskSpec) -> str | None:
    """Decide if a consumer-stage task can run partition-parallel.

    Every consumer task is Substrait-native; the decision delegates
    to ``_substrait_partition_parallel_reason``.
    """
    if not taskSpec.remoteCapable:
        return "remote_incapable"
    if taskSpec.partitionCount <= 1:
        return "single_partition"
    if taskSpec.taskKind not in ("single_input_consumer", "multi_input_consumer"):
        return "unsupported_task_kind"
    if not taskSpec.substraitPlanBytes:
        return "missing_substrait_bytes"
    return _substrait_partition_parallel_reason(taskSpec)


def _partition_parallel_safe(taskSpec: StageTaskSpec) -> bool:
    return _partition_parallel_reason(taskSpec) is None


_PARTITION_PARALLEL_SAFE_SUFFIX = {"project", "filter"}


def _substrait_partition_parallel_reason(taskSpec: StageTaskSpec) -> str | None:
    """Substrait-native equivalent of ``_partition_parallel_reason``.

    Parses ``taskSpec.substraitPlanBytes`` (the consumer subtree from
    ``SubstraitPlanDispatcher``) and decides partition-parallel safety
    using the same predicates the legacy code uses on ``remoteOps``.
    """
    if _substrait_has_non_local_suffix(taskSpec.substraitPlanBytes):
        # sort, fetch (limit), aggregate-without-keys etc. above the
        # join/aggregate require a global view — partition-parallel
        # would yield each shard's local sort, concatenated without a
        # final merge sort. Force single-task remote execution instead.
        return "non_local_suffix"
    rel = _substrait_consumer_root(taskSpec.substraitPlanBytes)
    if rel is None:
        return "unsupported_root"
    kind = rel.WhichOneof("rel_type")
    # Single-input consumer: aggregate with non-empty grouping keys.
    if taskSpec.taskKind == "single_input_consumer":
        if (
            len(taskSpec.inputPartitionings) != 1
            or len(taskSpec.inputPartitionCounts) != 1
        ):
            return "input_arity"
        if kind != "aggregate":
            return "unsupported_root"
        agg = rel.aggregate
        # Bolt's lowering puts grouping expressions in
        # AggregateRel.grouping_expressions (field 5) and references them
        # from a single Grouping in AggregateRel.groupings (field 3).
        if not agg.grouping_expressions:
            return "global_aggregate"
        if taskSpec.inputPartitionCounts[0] != taskSpec.partitionCount:
            return "partition_count_mismatch"
        # Equivalence check: the upstream hash partitioning must be on
        # the *same* keys as the aggregate's grouping. If a previous
        # stage hashed on key ``X`` but this aggregate groups on key
        # ``Y`` (with the same partition count), groups for ``Y`` are
        # split across partitions and a partition-parallel run would
        # concat without a final merge, producing duplicated/missing
        # groups. Refuse the fan-out and let the runtime fall back to
        # single-task execution where the global aggregate is safe.
        partition_keys = _hash_partitioning_keys(taskSpec.inputPartitionings[0])
        if partition_keys is None:
            return "non_hash_input_partitioning"
        input_names: tuple[str, ...] = ()
        if taskSpec.inputPlaceholders:
            input_names = tuple(taskSpec.inputPlaceholders[0].outputNames)
        grouping_names = _aggregate_grouping_key_names(rel, input_names)
        if grouping_names is None:
            return "cannot_prove_grouping_key_equivalence"
        if set(grouping_names) != set(partition_keys):
            return "grouping_partitioning_key_mismatch"
        return None
    # Multi-input consumer: hash_join (or join) with key arity > 0.
    if taskSpec.taskKind == "multi_input_consumer":
        if (
            len(taskSpec.inputPartitionings) != 2
            or len(taskSpec.inputPartitionCounts) != 2
        ):
            return "input_arity"
        if kind not in ("hash_join", "join", "merge_join"):
            return "unsupported_root"
        join = getattr(rel, kind)
        keys = list(getattr(join, "keys", ()))
        if not keys:
            # join with `expression` but no comparison keys cannot be
            # partition-parallelized
            return "missing_join_keys"
        if (
            taskSpec.inputPartitionCounts[0] != taskSpec.partitionCount
            or taskSpec.inputPartitionCounts[1] != taskSpec.partitionCount
        ):
            return "partition_count_mismatch"

        # Both inputs must be hash-partitioned on the EXACT join keys.
        # Equivalent check to the aggregate path's
        # ``grouping_partitioning_key_mismatch`` — arity alone is
        # insufficient because two different two-key partitionings
        # (``hash(a, b)`` vs ``hash(c, d)``) match on arity but route
        # rows differently. If the partitioning's key set doesn't
        # match the join's, partition-parallel runs produce wrong
        # results: matching rows on the keys could land in different
        # partitions and the join misses them.
        lhs_keys = _hash_partitioning_keys(taskSpec.inputPartitionings[0])
        rhs_keys = _hash_partitioning_keys(taskSpec.inputPartitionings[1])
        if lhs_keys is None or rhs_keys is None:
            return "non_hash_join_partitioning"
        if len(lhs_keys) != len(keys) or len(rhs_keys) != len(keys):
            return "join_key_arity_mismatch"

        # Resolve join key field-references to column names on each
        # side. Conservative: returns None for anything we can't
        # prove (non-selection key expressions, out-of-range field
        # indices, etc.) — fall back to single-task execution rather
        # than risk wrong partition routing.
        lhs_input_names: tuple[str, ...] = ()
        rhs_input_names: tuple[str, ...] = ()
        if len(taskSpec.inputPlaceholders) >= 2:
            lhs_input_names = tuple(taskSpec.inputPlaceholders[0].outputNames)
            rhs_input_names = tuple(taskSpec.inputPlaceholders[1].outputNames)
        lhs_join_keys = _join_key_names(keys, "left", lhs_input_names)
        rhs_join_keys = _join_key_names(keys, "right", rhs_input_names)
        if lhs_join_keys is None or rhs_join_keys is None:
            return "cannot_prove_join_key_equivalence"
        if set(lhs_keys) != set(lhs_join_keys) or set(rhs_keys) != set(rhs_join_keys):
            return "join_partitioning_key_mismatch"
        return None
    return "unsupported_task_kind"


def _join_key_names(
    key_messages, side: str, input_column_names: "tuple[str, ...]"
) -> "tuple[str, ...] | None":
    """Resolve a Substrait ``HashJoinRel.keys[*].{left,right}`` field-refs
    to column names on the named *side* (``"left"`` or ``"right"``).

    Each key message has ``left`` and ``right`` ``FieldReference``
    fields; we walk the side-specific one. Conservative: returns
    ``None`` on anything we can't prove (non-direct-reference,
    masked-reference, out-of-range index) so the partition-parallel
    safety gate falls back to single-task execution rather than
    risking wrong routing.
    """
    names: list[str] = []
    for key in key_messages:
        side_msg = getattr(key, side, None)
        if side_msg is None:
            return None
        if not side_msg.HasField("direct_reference"):
            return None
        ref = side_msg.direct_reference
        if not ref.HasField("struct_field"):
            return None
        idx = ref.struct_field.field
        if not (0 <= idx < len(input_column_names)):
            return None
        names.append(input_column_names[idx])
    return tuple(names)


def _grouping_key_count_from_plan_bytes(plan_bytes: "bytes | None") -> int:
    """Return the grouping-key count of the topmost grouped aggregate
    in *plan_bytes*, or 0 if there isn't one.

    Used by ``_merge_remote_results`` to decide whether the concatenated
    partition outputs need a stable key-sort. Local kSerial hash
    aggregate emits groups in insertion order; Ray hash-shuffle collects
    each partition's final-aggregate output in partition-hash order, so
    a plan-determined key-sort over the *gathered* result is the
    minimal fix that:

    * leaves the partition-parallel aggregate stage unchanged (no Sort
      injected into the plan, so the dispatcher still spawns N final-
      aggregate tasks rather than gathering to one);
    * doesn't alter the dataframe's stored ``PlanBuilder`` (plan-equality
      tests stay green);
    * matches local-mode surface ordering so users see consistent
      results regardless of executor.

    Recognised shapes (matches ``substrait_rules.sort_grouped_aggregate
    _output._find_grouped_aggregate``):

    * top-level ``aggregate``;
    * ``project(aggregate(...))`` (``DecomposeMeanBeforeExchange``'s
      avg-rewrite).

    Returns 0 for plans without a top-level grouped aggregate, plans
    with computed (non-field-ref) grouping keys, or any parse failure.
    Conservative: skip-not-crash keeps the runtime robust against
    plan-shape evolution.
    """
    if not plan_bytes:
        return 0
    try:
        from substrait.proto import plan as plan_pb2

        plan = plan_pb2.Plan()
        plan.ParseFromString(plan_bytes)
        if not plan.relations:
            return 0
        root = plan.relations[0].root.input
        kind = root.WhichOneof("rel_type")
        if kind == "aggregate":
            agg = root.aggregate
        elif kind == "project" and root.project.HasField("input"):
            inner = root.project.input
            if inner.WhichOneof("rel_type") != "aggregate":
                return 0
            agg = inner.aggregate
        else:
            return 0
        if not agg.grouping_expressions:
            return 0
        for ge in agg.grouping_expressions:
            if not ge.HasField("selection"):
                return 0
            sel = ge.selection
            if not sel.HasField("direct_reference"):
                return 0
            if not sel.direct_reference.HasField("struct_field"):
                return 0
        return len(agg.grouping_expressions)
    except Exception:  # noqa: BLE001
        return 0


def _merge_remote_results(remoteResults, taskSpec: "StageTaskSpec | None" = None):
    if len(remoteResults) == 1:
        return _dataframe_from_typed_arrow(remoteResults[0].table)._data_
    # Each remote table carries the same Bolt type metadata; retain the
    # first table's metadata when reconstructing the concatenated result.
    concatenated = pa.concat_tables(
        [remoteResult.table for remoteResult in remoteResults]
    )
    first_metadata = remoteResults[0].table.schema.metadata
    if first_metadata is not None:
        concatenated = concatenated.replace_schema_metadata(first_metadata)
    # Driver-side key-sort for grouped aggregate consumer stages.
    # See ``_grouping_key_count_from_plan_bytes`` for why we sort here
    # rather than in the substrait physical pipeline (would gather all
    # partitions to one task and destroy partition-parallelism, per
    # ``testRayExecutorRunsPartitionParallelRemoteGroupedConsumer``).
    n_keys = (
        _grouping_key_count_from_plan_bytes(taskSpec.substraitPlanBytes)
        if taskSpec is not None
        else 0
    )
    if n_keys > 0 and concatenated.num_rows > 1:
        sort_keys = [(concatenated.column_names[i], "ascending") for i in range(n_keys)]
        concatenated = concatenated.sort_by(sort_keys)
    return _dataframe_from_typed_arrow(concatenated)._data_


def _remote_result_metadata(remoteResults):
    workerIds = tuple(remoteResult.workerId for remoteResult in remoteResults)
    nodeIds = tuple(remoteResult.nodeId for remoteResult in remoteResults)
    # Per-task ``publishStartNs`` / ``publishEndNs`` come from each
    # worker's ``time.monotonic_ns()``; they are NOT comparable across
    # workers (different boot times). Two layered semantics:
    #
    # * ``publishWallNs`` is the bracket end-minus-start *per task*,
    #   summed across parallel tasks. This is a duration in
    #   nanoseconds and safe to compare/aggregate — represents the
    #   total worker wall time spent publishing for this stage.
    # * ``publishStartNs`` / ``publishEndNs`` are reported only when
    #   every task ran on the **same worker** (``workerIds`` collapses
    #   to one), because that's the only case where the worker
    #   monotonic clock can act as a single timeline. Otherwise both
    #   are ``None`` so downstream profile views don't accidentally
    #   place worker-clock timestamps on the same axis as the
    #   driver-side ``startNs`` / ``endNs`` brackets.
    publishStarts = [
        r.publishStartNs
        for r in remoteResults
        if getattr(r, "publishStartNs", None) is not None
    ]
    publishEnds = [
        r.publishEndNs
        for r in remoteResults
        if getattr(r, "publishEndNs", None) is not None
    ]
    publishWallNs = 0
    for r in remoteResults:
        start = getattr(r, "publishStartNs", None)
        end = getattr(r, "publishEndNs", None)
        if start is not None and end is not None and end >= start:
            publishWallNs += end - start
    distinctWorkers = len({wid for wid in workerIds if wid is not None})
    same_worker = distinctWorkers <= 1
    return {
        "remoteTaskCount": len(remoteResults),
        "workerId": workerIds[0] if workerIds else None,
        "nodeId": nodeIds[0] if nodeIds else None,
        "workerIds": workerIds,
        "nodeIds": nodeIds,
        "publishStartNs": (
            min(publishStarts) if publishStarts and same_worker else None
        ),
        "publishEndNs": (max(publishEnds) if publishEnds and same_worker else None),
        "publishWallNs": publishWallNs if publishWallNs > 0 else None,
    }


def _remote_field(remoteResult, name: str, default=None):
    if remoteResult is None:
        return default
    if isinstance(remoteResult, dict):
        return remoteResult.get(name, default)
    return getattr(remoteResult, name, default)


def _remote_output_names(result, remoteResult):
    if result is not None:
        names = getattr(result, "names", None)
        if callable(names):
            return tuple(names())
        if names is not None:
            return tuple(names)
        dtype = getattr(result, "dtype", None)
        if callable(dtype):
            dtype = dtype()
        if dtype is not None and hasattr(dtype, "names"):
            return tuple(dtype.names())
    outputNames = _remote_field(remoteResult, "outputNames", ())
    return tuple(outputNames)


def _remote_published_descriptors(remoteResult):
    return tuple(_remote_field(remoteResult, "publishedDescriptors", ()))


def _publish_stage_outputs(
    stage, result, exchangeManager, stageInputs, downstreamStages, attemptId: int = 1
):
    if exchangeManager is None:
        return []
    publishedDescriptors = []
    for placeholder in stageInputs.get(stage.stageId, []):
        partitions = (DataFrame(result),)
        if placeholder.partitionCount > 1:
            partitions = _splitProducedResult(
                result, placeholder.partitioning, placeholder.partitionCount
            )
        if boltmlDebugEnabled():
            sizes = []
            for p in partitions:
                try:
                    sizes.append(len(p.toArrow(pa.Table)))
                except Exception:
                    sizes.append(-1)
            boltmlDebugLog(
                "publish-audit",
                f"driver stage={stage.stageId}"
                f" exchange={placeholder.exchangeId}"
                f" partitioning={placeholder.partitioning}"
                f" partitionCount={placeholder.partitionCount}"
                f" per_partition_rows={sizes} total={sum(sizes)}",
            )
        try:
            if len(partitions) == 1:
                publishedDescriptors.extend(
                    exchangeManager.publish(
                        stage.stageId,
                        placeholder.exchangeId,
                        partitions[0],
                        placeholder.partitioning,
                        executionId=placeholder.executionId,
                        attemptId=attemptId,
                    )
                )
            else:
                publishedDescriptors.extend(
                    exchangeManager.publishPartitions(
                        stage.stageId,
                        placeholder.exchangeId,
                        partitions,
                        placeholder.partitioning,
                        executionId=placeholder.executionId,
                        attemptId=attemptId,
                    )
                )
        except Exception as error:
            raise RuntimeError(
                f"Stage {stage.stageId} failed to publish exchange {placeholder.exchangeId}: {error}"
            ) from error
    return publishedDescriptors


class RayRuntime:
    _planCounter = 0
    # Process-level guard so the stale-tempdir sweep runs at most once per
    # Python process even if the user constructs multiple ``RayRuntime``
    # instances (which is fine — the sweep is purely about reclaiming disk
    # left over by previous Python processes that died without ``finally``-
    # block cleanup).
    _staleTmpdirSweepDone: bool = False

    def __init__(
        self,
        maxStageRetries: int = 0,
        config: RayExecutionConfig | None = None,
    ):
        self._lastExecutionSummary: RuntimeExecutionSummary | None = None
        self._lastStageTaskResults: tuple[StageTaskResult, ...] = ()
        self._maxStageRetries = maxStageRetries
        self._config = config or RayExecutionConfig()
        self._scheduler = RayLeafScheduler(workerMode=self._config.workerMode)
        if not RayRuntime._staleTmpdirSweepDone:
            RayRuntime._sweepStaleExchangeTmpdirs()
            RayRuntime._staleTmpdirSweepDone = True

    @staticmethod
    def _sweepStaleExchangeTmpdirs(maxAgeSeconds: int = 3600) -> None:
        """Reclaim disk from leaked exchange tmpdirs whose owning driver
        process is proven dead.

        Each ``RayRuntime`` writes a small ``OWNER`` file inside its
        own tempdir (``OWNER=<hostname>:<pid>``) at construction time;
        the sweep reads that file on every candidate dir and:

        * If the OWNER file lists a different hostname, we can't probe
          the remote process from here. **Cross-host dirs are now
          never age-deleted** — without a cluster-level heartbeat we
          can't distinguish a live remote driver from a dead one.
          Cleanup of those leaks is the remote driver's
          responsibility (or operator/CI sweep tools).
        * If the OWNER lists this host, check whether the PID is
          alive (``os.kill(pid, 0)`` raises ``ProcessLookupError`` on
          dead, ``PermissionError`` on alive-but-foreign). Dead → safe
          to remove.
        * If there's no OWNER file at all (legacy dir from before
          this commit), fall back to the conservative age check —
          legacy dirs predate the OWNER protocol so we can't tell
          who owned them, but they're also bounded by the long
          ``maxAgeSeconds`` grace.

        Scans BOTH ``$TMPDIR`` AND any configured
        ``BOLTML_EXCHANGE_ROOT`` — without that, dirs created under
        a shared exchange root would leak forever since the original
        sweep only looked at the default tempdir.

        Idempotent and best-effort: errors during walk / rmtree are
        swallowed so the sweep can never break startup.
        """
        try:
            import socket

            scan_roots: list[str] = []
            scan_roots.append(tempfile.gettempdir())
            # Configured shared root. The runtime creates tempdirs
            # under this when ``BOLTML_EXCHANGE_ROOT`` is set; the
            # default sweep root would never reach them.
            exchange_root = os.environ.get("BOLTML_EXCHANGE_ROOT", "").strip()
            if exchange_root and exchange_root not in scan_roots:
                scan_roots.append(exchange_root)
            now = time.time()
            cutoff = now - maxAgeSeconds
            my_host = socket.gethostname()
            for scan_root in scan_roots:
                try:
                    entries = os.listdir(scan_root)
                except OSError:
                    continue
                for entry in entries:
                    if not entry.startswith("boltml-ray-runtime-"):
                        continue
                    full = os.path.join(scan_root, entry)
                    owner_path = os.path.join(full, "OWNER")
                    owner_alive = None  # tri-state
                    cross_host = False
                    try:
                        with open(owner_path, "r") as f:
                            owner_str = f.read().strip()
                        host, _, pid_str = owner_str.partition(":")
                        pid = int(pid_str)
                    except (OSError, ValueError):
                        owner_alive = None
                    else:
                        if host != my_host:
                            cross_host = True
                            owner_alive = None
                        else:
                            try:
                                os.kill(pid, 0)
                                owner_alive = True
                            except ProcessLookupError:
                                owner_alive = False
                            except PermissionError:
                                owner_alive = True
                            except OSError:
                                owner_alive = None
                    if owner_alive is True:
                        continue
                    if owner_alive is False:
                        boltmlDebugLog(
                            "runtime",
                            f"sweeping owner-dead exchange tmpdir {full}",
                        )
                        shutil.rmtree(full, ignore_errors=True)
                        continue
                    if cross_host:
                        # Don't age-delete a dir whose OWNER claims it
                        # belongs to a process on another host. We
                        # have no liveness signal we can trust;
                        # deleting could nuke an active remote
                        # driver's exchange files. The cluster
                        # operator (or a heartbeat-aware sweeper)
                        # owns this case.
                        continue
                    # No OWNER file → legacy dir. Conservative age
                    # fallback only.
                    try:
                        mtime = os.path.getmtime(full)
                    except OSError:
                        continue
                    if mtime < cutoff:
                        boltmlDebugLog(
                            "runtime",
                            f"sweeping legacy unowned exchange tmpdir {full} "
                            f"(age={(now - mtime) / 60:.1f}min)",
                        )
                        shutil.rmtree(full, ignore_errors=True)
        except Exception as exc:
            boltmlDebugLog(
                "runtime",
                f"stale-tempdir sweep failed (best-effort): {type(exc).__name__}: {exc}",
            )

    @property
    def executionMode(self) -> RayExecutionMode:
        return self._config.mode

    @property
    def rolloutLevel(self) -> RayRolloutLevel:
        if self._config.rolloutLevel is not None:
            return self._config.rolloutLevel
        if self.executionMode == RayExecutionMode.STAGED_LOCAL:
            return RayRolloutLevel.STAGED_LOCAL
        return RayRolloutLevel.REMOTE_ANALYTICAL

    def _remoteAllowed(self, stage, taskSpec) -> bool:
        if self.executionMode != RayExecutionMode.REMOTE_LEAF:
            return False
        if self.rolloutLevel == RayRolloutLevel.STAGED_LOCAL:
            return False
        if len(stage.inputPlaceholders) == 0:
            return taskSpec.remoteCapable and self.rolloutLevel in {
                RayRolloutLevel.REMOTE_LEAF,
                RayRolloutLevel.REMOTE_ANALYTICAL,
            }
        if not taskSpec.remoteCapable:
            return False
        if self.rolloutLevel != RayRolloutLevel.REMOTE_ANALYTICAL:
            return False
        # Trivial consumer guard: a consumer whose Substrait plan
        # contains no aggregate / join above the placeholder reads —
        # i.e. just a shuffle / sort / project / fetch chain — would
        # run as a single-task remote (no partition-parallel split
        # because ``_partition_parallel_reason`` returns
        # ``non_local_suffix`` for those shapes). Single-task remote
        # of a trivial consumer is strictly worse than driver-local:
        # it pays one Ray-task spawn, ships every input partition over
        # the exchange, and then runs the same code that the driver
        # could have run in-process. Fall back to local for those.
        # Real consumers (agg / join + suffix) still go remote so the
        # heavy compute stays off the driver.
        if _consumer_is_trivial(taskSpec):
            return False
        return True

    def _smallConsumerInputBytes(self, resolvedInputs) -> int | None:
        """Sum the on-disk size of every resolved input descriptor, or
        ``None`` if any descriptor is on a transport whose size we
        can't cheaply read (object store).

        ``resolvedInputs`` is a list of tuples-of-descriptors, one
        tuple per input placeholder; each tuple holds one descriptor
        per partition. For ``transport == "file"`` we read
        ``os.path.getsize`` (cheap, just stat). For ``transport ==
        "object"`` we'd have to ask Ray for the in-store object size,
        which is more expensive than the dispatch decision should be —
        so we conservatively return ``None`` (no opinion) and the
        caller dispatches as it would have without this rule.
        """
        total = 0
        for resolved in resolvedInputs:
            for descriptor in resolved:
                if descriptor.transport != "file":
                    return None
                try:
                    total += os.path.getsize(descriptor.path)
                except OSError:
                    return None
        return total

    def _shouldDispatchRemotely(self, stage, taskSpec, resolvedInputs) -> bool:
        """Wrap ``_remoteAllowed`` with a "tiny consumer input → driver
        collect" override.

        When the resolved descriptors total less than
        ``BOLTML_LOCAL_CONSUMER_THRESHOLD_BYTES`` (env var, **default
        0 = off**) AND the stage has any input (i.e. it's a consumer,
        not a leaf producer), routing to the local-fallback path is
        cheaper than spawning Ray tasks. The Ray task spawn alone
        takes O(100ms) per task across the cluster; for a final-agg
        consumer that processes ~300 partial-aggregated rows from 12
        partitions, the actual compute is microseconds.

        Default-off so existing semantics (every consumer dispatches
        remotely under REMOTE_LEAF + REMOTE_ANALYTICAL) are preserved.
        Production / harness configurations that want the optimization
        set the env var explicitly — typical value is ``1048576``
        (1 MiB), which captures the "300-row final agg" pattern this
        rule is designed for without affecting any consumer that
        actually has work to do.

        The harness-default is intentionally NOT 1 MiB because many
        unit tests exercise the remote-dispatch path with deliberately
        small data and assert ``stage.remote == True``; turning the
        gate on by default would convert those into spurious local-
        execution failures.
        """
        if not self._remoteAllowed(stage, taskSpec):
            return False
        if not stage.inputPlaceholders:
            # Leaf producer — never overridden by this gate.
            return True
        try:
            threshold = int(
                os.environ.get("BOLTML_LOCAL_CONSUMER_THRESHOLD_BYTES", "0")
            )
        except ValueError:
            threshold = 0
        if threshold <= 0:
            return True
        total = self._smallConsumerInputBytes(resolvedInputs)
        if total is None:
            return True
        if total < threshold:
            boltmlDebugLog(
                "runtime",
                f"stage {stage.stageId} small consumer input "
                f"bytes={total} < threshold={threshold}; routing to driver",
            )
            return False
        return True

    @staticmethod
    def _outputNames(result) -> tuple[str, ...]:
        names = getattr(result, "names", None)
        if callable(names):
            return tuple(names())
        if names is not None:
            return tuple(names)
        dtype = getattr(result, "dtype", None)
        if callable(dtype):
            dtype = dtype()
        if dtype is not None and hasattr(dtype, "names"):
            return tuple(dtype.names())
        raise TypeError(
            f"Cannot derive output names from result of type {type(result)}"
        )

    def runStageSubstraitBytes(
        self, substraitPlanBytes: bytes, taskName: str, *loadedInputs
    ):
        """Locally execute a stage whose plan is raw Substrait bytes.

        Used by the Substrait dispatcher path for stages emitted by
        ``SubstraitPlanDispatcher`` whose ``executionPlan`` is ``None``.
        ``substraitPlanBytes`` is either ``producerSubstraitPlanBytes``
        (no inputs) or ``consumerSubstraitPlanBytes`` (with placeholder
        ``ReadRel``s to be substituted from ``loadedInputs``). Reuses
        ``_execute_substrait_bytes`` so the local + remote paths converge
        on a single Substrait-execution implementation.
        """
        from .remote_worker import _execute_substrait_bytes

        boltmlDebugLog(
            "runtime",
            f"run local substrait stage task={taskName} inputs={len(loadedInputs)}",
        )
        return _execute_substrait_bytes(substraitPlanBytes, *loadedInputs)._data_

    def _runLeafStageRemotely(
        self, taskSpec, outputPlaceholders=(), publishConfig=None
    ):
        boltmlDebugLog("runtime", f"run remote leaf stage={taskSpec.stageId}")
        remoteResult = self._scheduler.execute(
            taskSpec, outputPlaceholders, publishConfig
        )
        result = (
            None
            if remoteResult.table is None
            else _dataframe_from_typed_arrow(remoteResult.table)._data_
        )
        return result, remoteResult

    def _runPartitionedLeafStageRemotely(
        self,
        taskSpec,
        shardCount: int,
        outputPlaceholders=(),
        publishConfig=None,
    ):
        """Fan one leaf-producer stage out across ``shardCount`` Ray tasks.

        Each task receives a copy of the same Substrait plan but a
        different ``leafShardIndex``; inside ``_execute_substrait_bytes``
        the splits are sliced ``round-robin`` so the union of all shards
        covers every split exactly once.

        Each task's published descriptors carry a distinct ``shardId``
        (= ``leafShardIndex``) so the consumer-side
        ``_group_descriptors_by_partition`` treats them as separate
        producer shards — same scheme used by the partitioned consumer
        path.

        For inputs whose total split count is < ``shardCount`` (e.g. a
        single-file parquet that ``convertSubstraitPlan`` turns into 1
        split), shards beyond the split count receive empty work and
        the worker short-circuits to an empty Arrow table — see the
        empty-shard guard in ``_execute_substrait_bytes``. This keeps
        the parallel path safe to opt into without first measuring the
        input's split count, at the cost of some wasted Ray dispatches
        for those degenerate inputs.

        Aggregate-correctness gate
        --------------------------
        If the leaf's plan tree contains an Aggregate operator, we
        force ``shardCount=1`` for this stage and dispatch a single
        Ray task instead. When a leaf's plan ends in (or contains) an
        Aggregate, the per-shard outputs are partial-aggregate state
        that needs explicit merging via a downstream operator. There
        is no such merging for single-stage leaves (e.g. TPC-H q6 =
        scan + filter + global SUM), so N shards produce N un-merged
        partial-sum rows and the result is wrong (q6 returned 4 rows
        with shardCount=4). Refusing fan-out for those plans keeps
        them correct at the cost of single-task throughput on that
        stage.
        """
        from .remote_worker import (
            _plan_contains_aggregate,
            _plan_contains_join,
            _plan_has_shardable_input,
            _plan_has_short_split_input,
        )
        from substrait.proto import plan as _plan

        if shardCount > 1 and taskSpec.substraitPlanBytes:
            _sp = _plan.Plan()
            _sp.ParseFromString(taskSpec.substraitPlanBytes)
            if _plan_contains_aggregate(_sp):
                boltmlDebugLog(
                    "runtime",
                    f"forcing leaf shardCount=1 for {taskSpec.stageId} -- "
                    f"plan contains Aggregate; fan-out would emit "
                    f"un-merged partial-aggregate rows",
                )
                # Fall back to single-task execution. The descriptor
                # publish path uses the same shardId scheme as the
                # un-fanned-out path (see remote_worker._publish_remote_outputs).
                return self._runLeafStageRemotely(
                    taskSpec, outputPlaceholders, publishConfig
                )
            # Under-sharded scan gate. If the leaf plan reads
            # any local_files source whose file count is < shardCount,
            # the round-robin slice in _execute_substrait_bytes leaves
            # late shards with empty splits for that node, triggering
            # the empty-shard short-circuit and dropping ~(N-1)/N of
            # the work for joins between under-sharded and fully-sharded
            # scans. q5/q7/q10/q21 at SF=10 multi-file with leafP=16 hit
            # this — under-sharded ``nation`` (1 file) joined with
            # ``lineitem`` (16 files) caused 87% revenue under-count for
            # q5. Refusing fan-out keeps the result correct.
            if _plan_has_short_split_input(_sp, shardCount):
                boltmlDebugLog(
                    "runtime",
                    f"forcing leaf shardCount=1 for {taskSpec.stageId} -- "
                    f"plan has a local_files scan with < {shardCount} files; "
                    f"fan-out would short-circuit empty shards and drop work",
                )
                return self._runLeafStageRemotely(
                    taskSpec, outputPlaceholders, publishConfig
                )
            # Cross-shard-join gate. Leaf-fan-out partitions each
            # scan node's splits round-robin by FILE INDEX. Joins inside
            # the leaf compute per-shard and assume the partitioning is
            # aligned with the join key -- which it is NOT (file index
            # is unrelated to join key). Each shard's join sees only
            # the file-aligned subset of both sides, dropping all
            # cross-shard matches. q5 at SF=10 multi-file with leafP=4
            # hit this: the leaf joins lineitem/orders/customer, all
            # multi-shard but partitioned by row position not customer/
            # order key, so the per-shard join lost ~3/4 of the matches
            # and revenue came back at 1/4 of golden.
            #
            # Refusing fan-out for any leaf containing a join keeps the
            # result correct. Future enhancement: detect broadcast-build
            # patterns (BroadcastSmallBuildSide) and allow fan-out when
            # the build side is replicated.
            if _plan_contains_join(_sp):
                boltmlDebugLog(
                    "runtime",
                    f"forcing leaf shardCount=1 for {taskSpec.stageId} -- "
                    f"plan contains a Join; fan-out would partition each "
                    f"side by file index (not join key) and drop cross-"
                    f"shard matches",
                )
                return self._runLeafStageRemotely(
                    taskSpec, outputPlaceholders, publishConfig
                )
            # Values-only leaf gate. ``virtual_table`` reads
            # carry their data in the plan literal, not as splits. With
            # ``leafShardCount=N`` the splits map is empty so the
            # round-robin slice is a no-op and every shard executes
            # the full plan, emitting N copies of the same data. The
            # downstream join then produces ``N * matches`` rows.
            # Refusing fan-out for these leaves keeps the result
            # correct (single Ray task that runs the Values plan once)
            # without sacrificing parallelism that wouldn't have
            # existed anyway -- a Values source is typically tiny
            # (max(rev) subquery result, IN-list filter table, etc.).
            if not _plan_has_shardable_input(_sp):
                boltmlDebugLog(
                    "runtime",
                    f"forcing leaf shardCount=1 for {taskSpec.stageId} -- "
                    f"plan has no shardable (local_files) inputs; "
                    f"fan-out would emit N duplicate rows from the Values source",
                )
                return self._runLeafStageRemotely(
                    taskSpec, outputPlaceholders, publishConfig
                )

        boltmlDebugLog(
            "runtime",
            f"run partitioned remote leaf stage={taskSpec.stageId} shards={shardCount}",
        )
        submitted = []
        for shardIdx in range(shardCount):
            shardSpec = replace(
                taskSpec,
                leafShardIndex=shardIdx,
                leafShardCount=shardCount,
            )
            submitted.append(
                self._scheduler.submit(shardSpec, outputPlaceholders, publishConfig)
            )
        remoteResults, _waitError = self._scheduler.waitWithPartialResults(submitted)
        if _waitError is not None:
            # Hand the partial results up so the caller (with
            # exchangeManager in scope) can clean up sibling artifacts
            # before the retry envelope sees the original error.
            raise PartitionedRemoteFailure(_waitError, remoteResults)
        result = (
            None
            if all(remoteResult.table is None for remoteResult in remoteResults)
            else _merge_remote_results(remoteResults, taskSpec)
        )
        metadata = _remote_result_metadata(remoteResults)
        metadata["publishedDescriptors"] = tuple(
            descriptor
            for remoteResult in remoteResults
            for descriptor in _remote_published_descriptors(remoteResult)
        )
        metadata["outputNames"] = (
            tuple(remoteResults[0].outputNames) if remoteResults else ()
        )
        return result, metadata

    def _runSingleInputConsumerStageRemotely(
        self, taskSpec, inputDescriptors, outputPlaceholders=(), publishConfig=None
    ):
        boltmlDebugLog(
            "runtime", f"run remote single-input consumer stage={taskSpec.stageId}"
        )
        remoteResult = self._scheduler.executeSingleInputConsumer(
            taskSpec,
            inputDescriptors,
            outputPlaceholders,
            publishConfig,
        )
        result = (
            None
            if remoteResult.table is None
            else _dataframe_from_typed_arrow(remoteResult.table)._data_
        )
        return result, remoteResult

    def _runPartitionedSingleInputConsumerStageRemotely(
        self, taskSpec, inputDescriptors, outputPlaceholders=(), publishConfig=None
    ):
        boltmlDebugLog(
            "runtime",
            f"run partitioned remote single-input consumer stage={taskSpec.stageId} partitions={taskSpec.partitionCount}",
        )
        partitionInputs = _group_descriptors_by_partition(
            inputDescriptors, taskSpec.partitionCount
        )
        submitted = [
            self._scheduler.submitSingleInputConsumer(
                replace(taskSpec, partitionId=partitionId),
                descriptors,
                outputPlaceholders,
                publishConfig,
                forceTaskMode=len(partitionInputs) > 1,
            )
            for partitionId, descriptors in enumerate(partitionInputs)
        ]
        remoteResults, _waitError = self._scheduler.waitWithPartialResults(submitted)
        if _waitError is not None:
            # Hand the partial results up so the caller (with
            # exchangeManager in scope) can clean up sibling artifacts
            # before the retry envelope sees the original error.
            raise PartitionedRemoteFailure(_waitError, remoteResults)
        result = (
            None
            if all(remoteResult.table is None for remoteResult in remoteResults)
            else _merge_remote_results(remoteResults, taskSpec)
        )
        metadata = _remote_result_metadata(remoteResults)
        metadata["publishedDescriptors"] = tuple(
            descriptor
            for remoteResult in remoteResults
            for descriptor in _remote_published_descriptors(remoteResult)
        )
        metadata["outputNames"] = (
            tuple(remoteResults[0].outputNames) if remoteResults else ()
        )
        return result, metadata

    def _runMultiInputConsumerStageRemotely(
        self, taskSpec, inputDescriptorGroups, outputPlaceholders=(), publishConfig=None
    ):
        boltmlDebugLog(
            "runtime", f"run remote multi-input consumer stage={taskSpec.stageId}"
        )
        remoteResult = self._scheduler.executeMultiInputConsumer(
            taskSpec,
            inputDescriptorGroups[0],
            inputDescriptorGroups[1],
            outputPlaceholders,
            publishConfig,
        )
        result = (
            None
            if remoteResult.table is None
            else _dataframe_from_typed_arrow(remoteResult.table)._data_
        )
        return result, remoteResult

    def _runPartitionedMultiInputConsumerStageRemotely(
        self, taskSpec, inputDescriptorGroups, outputPlaceholders=(), publishConfig=None
    ):
        boltmlDebugLog(
            "runtime",
            f"run partitioned remote multi-input consumer stage={taskSpec.stageId} partitions={taskSpec.partitionCount}",
        )
        lhsInputs = _group_descriptors_by_partition(
            inputDescriptorGroups[0], taskSpec.partitionCount
        )
        rhsInputs = _group_descriptors_by_partition(
            inputDescriptorGroups[1], taskSpec.partitionCount
        )
        if len(lhsInputs) != len(rhsInputs):
            raise RuntimeError(
                f"Stage {taskSpec.stageId} resolved mismatched partition groups: lhs={len(lhsInputs)} rhs={len(rhsInputs)}."
            )
        submitted = [
            self._scheduler.submitMultiInputConsumer(
                replace(taskSpec, partitionId=partitionId),
                lhsDescriptors,
                rhsDescriptors,
                outputPlaceholders,
                publishConfig,
                forceTaskMode=len(lhsInputs) > 1,
            )
            for partitionId, (lhsDescriptors, rhsDescriptors) in enumerate(
                zip(lhsInputs, rhsInputs)
            )
        ]
        remoteResults, _waitError = self._scheduler.waitWithPartialResults(submitted)
        if _waitError is not None:
            # Hand the partial results up so the caller (with
            # exchangeManager in scope) can clean up sibling artifacts
            # before the retry envelope sees the original error.
            raise PartitionedRemoteFailure(_waitError, remoteResults)
        result = (
            None
            if all(remoteResult.table is None for remoteResult in remoteResults)
            else _merge_remote_results(remoteResults, taskSpec)
        )
        metadata = _remote_result_metadata(remoteResults)
        metadata["publishedDescriptors"] = tuple(
            descriptor
            for remoteResult in remoteResults
            for descriptor in _remote_published_descriptors(remoteResult)
        )
        metadata["outputNames"] = (
            tuple(remoteResults[0].outputNames) if remoteResults else ()
        )
        return result, metadata

    def _withLocalStageRetries(self, work, label: str):
        """Shared retry envelope for local stage execution.

        *work* is a no-arg callable that produces the stage result. *label*
        is a debug-log string identifying the stage (e.g. ``f"local
        stage={stageId}"``). Returns ``(result, attempt, retryReason)``.

        Retries on ``TransientStageError`` (the synthetic test-only
        exception) AND on classified-retryable Ray failures
        (``WorkerCrashedError`` / ``ObjectLostError`` etc., per
        ``_is_retryable_ray_error``) — local consumer stages run
        ``exchangeManager.load`` and ``ray.get`` inside the *work*
        closure, so the same infrastructure failures the remote retry
        envelope already handles can surface here too. Anything else
        propagates immediately — masking programming bugs as
        "transient" is worse than failing loud.
        """
        attempt = 0
        retryReason = None
        while True:
            attempt += 1
            try:
                boltmlDebugLog("runtime", f"attempt {attempt} {label}")
                return work(), attempt, retryReason
            except TransientStageError as error:
                retryReason = type(error).__name__
                if attempt > self._maxStageRetries:
                    raise
            except Exception as error:  # noqa: BLE001
                if not _is_retryable_ray_error(error):
                    raise
                retryReason = type(error).__name__
                boltmlDebugLog(
                    "runtime",
                    f"retryable Ray failure on attempt {attempt} {label}: "
                    f"{retryReason}: {error}",
                )
                if attempt > self._maxStageRetries:
                    raise

    def _runSubstraitStageWithRetries(self, substraitPlanBytes, stageId, *loadedInputs):
        """Run a Substrait-bytes stage locally, wrapped in the standard
        local-stage retry envelope. Every stage is Substrait-native."""
        return self._withLocalStageRetries(
            lambda: self.runStageSubstraitBytes(
                substraitPlanBytes, stageId, *loadedInputs
            ),
            f"local substrait stage={stageId}",
        )

    def _runRemoteWithRetries(self, fn, exchangeManager=None):
        attempt = 0
        retryReason = None
        while True:
            attempt += 1
            try:
                boltmlDebugLog("runtime", f"attempt {attempt} remote execution")
                result, remoteResult = fn(attempt)
                return result, remoteResult, attempt, retryReason
            except PartitionedRemoteFailure as partial:
                # Stage-atomic publish: clean up the artifacts that
                # successful siblings already wrote before letting the
                # original error propagate / retry. Without this, the
                # retry attempt would race against stranded parquet
                # files / Ray objects from the failed attempt.
                _cleanup_partial_remote_publish(partial.partialResults, exchangeManager)
                # Re-raise the original error so the existing classifier
                # paths below decide retry vs propagate.
                error = partial.original
                if isinstance(error, TransientStageError):
                    retryReason = type(error).__name__
                    if attempt > self._maxStageRetries:
                        raise error from partial
                    continue
                if not _is_retryable_ray_error(error):
                    raise error from partial
                retryReason = type(error).__name__
                boltmlDebugLog(
                    "runtime",
                    f"retryable Ray failure on partitioned attempt {attempt}: "
                    f"{retryReason}: {error}",
                )
                if attempt > self._maxStageRetries:
                    raise error from partial
                continue
            except TransientStageError as error:
                retryReason = type(error).__name__
                if attempt > self._maxStageRetries:
                    raise
            except Exception as error:  # noqa: BLE001
                # Real Ray failures (RayTaskError wrapping a user
                # exception, worker crashes, actor death, object
                # eviction/loss, node failures) arrive here. Classify
                # them: retryable failures (worker crash, object lost,
                # node death — i.e. *infra* faults) re-submit with a
                # fresh attempt id; fatal failures (RayTaskError
                # wrapping a deterministic user-visible exception)
                # propagate immediately because retrying would just
                # reproduce the same error. Anything we can't classify
                # is treated as fatal so we don't mask bugs as
                # "transient".
                if not _is_retryable_ray_error(error):
                    raise
                retryReason = type(error).__name__
                boltmlDebugLog(
                    "runtime",
                    f"retryable Ray failure on attempt {attempt}: "
                    f"{retryReason}: {error}",
                )
                if attempt > self._maxStageRetries:
                    raise

    def runStageDag(
        self,
        stageDag: "StageDAG",  # noqa: F821
        exchangeManager: "ExchangeManager | None" = None,  # noqa: F821
        udfRegistry: "UdfRegistry | None" = None,  # noqa: F821
    ):
        # ``udfRegistry`` carries the UDFs the driver registered on the
        # ``RayExecutor`` so each remote worker can re-register them on
        # its own pybolt singleton before executing the Substrait plan.
        # Defaults to an empty registry for the no-UDF case (which is
        # most paths) so callers that don't have one don't need to
        # synthesize one.
        from .udf_spec import UdfRegistry

        if udfRegistry is None:
            udfRegistry = UdfRegistry()
        RayRuntime._planCounter += 1
        executionId = f"ray-exec-{RayRuntime._planCounter}-{uuid4().hex}"
        stageDag.validate()
        stageRecords = []
        stageInputs = {}
        stagePlaceholders = {}
        downstreamStages = {}
        ownedExchangeTmpDir: tempfile.TemporaryDirectory[str] | None = None
        for stage in stageDag.stages:
            for placeholder in stage.inputPlaceholders:
                scopedPlaceholder = replace(placeholder, executionId=executionId)
                stagePlaceholders.setdefault(stage.stageId, []).append(
                    scopedPlaceholder
                )
                stageInputs.setdefault(placeholder.sourceStageId, []).append(
                    scopedPlaceholder
                )
                downstreamStages.setdefault(placeholder.sourceStageId, []).append(stage)

        if (
            exchangeManager is None
            and stageInputs
            and self.executionMode == RayExecutionMode.REMOTE_LEAF
        ):
            # Allow env-var override of the exchange transport policy
            # so users can switch from default file-backed to Ray
            # object-store transport without code changes. Defaults
            # preserve current behaviour (file). Useful when stage-N
            # intermediates fit comfortably in the cluster's object
            # store memory and the parquet write/read cost dominates
            # the actual compute (TPC-H Q5's 120M-row intermediate
            # spent ~18s on file I/O alone, where object transport
            # would skip both the parquet encode/decode and the disk
            # roundtrip).
            from .exchange_manager import ExchangeTransportPolicy

            transport = os.environ.get("BOLTML_EXCHANGE_TRANSPORT", "file")
            # Track whether the row threshold was set explicitly so that
            # ``BOLTML_EXCHANGE_TRANSPORT=object`` alone forces object
            # transport (``maxRowsForObject=-1`` sentinel) rather than
            # silently flipping back to file at the
            # ``max_rows_for_object == 0`` policy branch. Explicit
            # threshold values still win.
            _max_rows_raw = os.environ.get("BOLTML_EXCHANGE_MAX_ROWS_OBJECT", "")
            max_rows_explicit = bool(_max_rows_raw.strip())
            try:
                max_rows = int(_max_rows_raw) if max_rows_explicit else 0
            except ValueError:
                max_rows = 0
                max_rows_explicit = False
            if transport == "object" and not max_rows_explicit:
                # "User asked for object transport without a threshold"
                # → always object.
                max_rows = -1
            # Multi-node safety check: file transport writes producer-local
            # absolute paths (``/tmp/boltml-ray-runtime-XXX/...``) and the
            # consumer reads them by path. On a single-node cluster every
            # node sees the same /tmp; on a multi-node Ray cluster the
            # producer's /tmp is unreachable from a consumer scheduled on
            # another node. ``BOLTML_EXCHANGE_ROOT`` declares the user has
            # provided a shared filesystem; without it, refuse file
            # transport on >1-node clusters and fall back to object.
            exchange_root = os.environ.get("BOLTML_EXCHANGE_ROOT", "").strip()
            if transport == "file" and not exchange_root:
                try:
                    alive_node_count = sum(1 for n in ray.nodes() if n.get("Alive"))
                except Exception:  # noqa: BLE001
                    alive_node_count = 1
                if alive_node_count > 1:
                    boltmlDebugLog(
                        "runtime",
                        f"multi-node Ray cluster ({alive_node_count} nodes) "
                        f"detected with file transport and no "
                        f"BOLTML_EXCHANGE_ROOT; auto-switching to object "
                        f"transport. Set BOLTML_EXCHANGE_ROOT=<shared-path> "
                        f"to opt back into file transport with a shared "
                        f"filesystem.",
                    )
                    transport = "object"
                    # Safety override beats user threshold: if the user
                    # had ``BOLTML_EXCHANGE_MAX_ROWS_OBJECT=0`` set
                    # explicitly, ``chooseTransport`` would still
                    # return "file" because ``maxRowsForObject == 0``
                    # means "object disabled". Force the always-object
                    # sentinel here so the cross-node-unsafe file
                    # transport doesn't sneak back in.
                    if max_rows <= 0:
                        max_rows = -1
            # Use ``BOLTML_EXCHANGE_ROOT`` as the parent of the
            # exchange tempdir when it's set. Without this the runtime
            # would check the env var (above) to decide that file
            # transport is *safe* on a multi-node cluster, then write
            # to the local /tmp anyway — the supposed shared filesystem
            # never gets used. With the wiring, file transport actually
            # lands on the declared shared mount and cross-node reads
            # succeed.
            if exchange_root:
                os.makedirs(exchange_root, exist_ok=True)
                ownedExchangeTmpDir = tempfile.TemporaryDirectory(
                    prefix="boltml-ray-runtime-",
                    dir=exchange_root,
                )
            else:
                ownedExchangeTmpDir = tempfile.TemporaryDirectory(
                    prefix="boltml-ray-runtime-"
                )
            # Stamp an OWNER file so ``_sweepStaleExchangeTmpdirs`` can
            # tell whether the dir belongs to a live process before
            # deleting. Without this, the sweep falls back to a 1h
            # age heuristic that can incorrectly delete another live
            # driver's exchange files on shared TMPDIR / exchange-root
            # deployments.
            try:
                import socket

                owner_path = os.path.join(ownedExchangeTmpDir.name, "OWNER")
                with open(owner_path, "w") as _owner_f:
                    _owner_f.write(f"{socket.gethostname()}:{os.getpid()}")
            except Exception:  # noqa: BLE001
                # Best-effort — without OWNER, the dir falls into the
                # legacy age-only fallback (still bounded by the 1h
                # grace period).
                pass
            policy = ExchangeTransportPolicy(
                preferredTransport=transport,
                maxRowsForObject=max_rows,
                # ``BOLTML_EXCHANGE_ROOT`` declares the file-transport
                # tempdir lives on a filesystem mounted on every Ray
                # node. The publish path uses this to skip stamping a
                # producer node id on file descriptors, so the
                # consumer-side cross-node guard accepts the read on
                # any node. Without this the shared-root write path
                # would be half-wired and cross-node reads would still
                # fail.
                sharedFilesystem=bool(exchange_root),
            )
            boltmlDebugLog(
                "runtime",
                f"exchange policy preferredTransport={transport} maxRowsForObject={max_rows} "
                f"exchangeRoot={exchange_root or '<tempdir>'} sharedFilesystem={bool(exchange_root)}",
            )
            exchangeManager = AdaptiveExchangeManager(
                ownedExchangeTmpDir.name, policy=policy
            )

        result = None
        plan_start_ns = time.monotonic_ns()
        try:
            for stage in stageDag.stages:
                # Per-stage profiling timestamp captured at loop top so it
                # includes any setup work (taskSpec construction, retry
                # loops, etc.). Paired with stage_end_ns at every
                # ``stageRecords.append`` site.
                stage_start_ns = time.monotonic_ns()
                boltmlDebugLog(
                    "runtime",
                    f"stage start id={stage.stageId} inputs={len(stage.inputPlaceholders)}",
                )
                if (
                    stage.producerSubstraitPlanBytes is not None
                    and not stage.inputPlaceholders
                ):
                    taskSpec = replace(
                        StageTaskSpec.fromStageNode(
                            stage, executionMode=self.executionMode.value
                        ),
                        executionId=executionId,
                        udfRegistry=udfRegistry,
                    )
                    boltmlTraceLog(
                        "runtime", f"task spec for {stage.stageId}: {taskSpec}"
                    )
                    remoteResult = None
                    fallbackReason = None
                    publishedDescriptors = []
                    outputPlaceholders = tuple(stageInputs.get(stage.stageId, ()))
                    publishConfig = (
                        None
                        if exchangeManager is None
                        else exchangeManager.publishConfig()
                    )
                    if self._remoteAllowed(stage, taskSpec):
                        # Opt-in leaf-stage parallelism via
                        # ``BOLTML_LEAF_PARALLELISM`` (default 1, prior
                        # behaviour). When N > 1 the runtime fans the
                        # leaf out across N parallel Ray tasks; each
                        # task processes ``splits[i::N]`` of the plan's
                        # input splits. For single-file inputs (which
                        # produce just one split) shards beyond the
                        # first short-circuit to an empty Arrow table
                        # — see the empty-shard guard in the worker.
                        try:
                            leaf_parallelism = max(
                                1,
                                int(os.environ.get("BOLTML_LEAF_PARALLELISM", "1")),
                            )
                        except ValueError:
                            leaf_parallelism = 1
                        if leaf_parallelism > 1:
                            boltmlDebugLog(
                                "runtime",
                                f"stage {stage.stageId} selected partitioned remote leaf "
                                f"execution shards={leaf_parallelism}",
                            )
                            result, remoteResult, attempt, retryReason = (
                                self._runRemoteWithRetries(
                                    lambda attemptId: self._runPartitionedLeafStageRemotely(
                                        replace(taskSpec, attemptId=attemptId),
                                        leaf_parallelism,
                                        outputPlaceholders,
                                        publishConfig,
                                    ),
                                    exchangeManager=exchangeManager,
                                )
                            )
                        else:
                            boltmlDebugLog(
                                "runtime",
                                f"stage {stage.stageId} selected remote leaf execution",
                            )
                            result, remoteResult, attempt, retryReason = (
                                self._runRemoteWithRetries(
                                    lambda attemptId: self._runLeafStageRemotely(
                                        replace(taskSpec, attemptId=attemptId),
                                        outputPlaceholders,
                                        publishConfig,
                                    ),
                                    exchangeManager=exchangeManager,
                                )
                            )
                    else:
                        if self.executionMode == RayExecutionMode.REMOTE_LEAF:
                            fallbackReason = (
                                "rollout_gate"
                                if self.rolloutLevel == RayRolloutLevel.STAGED_LOCAL
                                else taskSpec.fallbackReason
                            )
                            boltmlDebugLog(
                                "runtime",
                                f"stage {stage.stageId} falling back to local execution reason={fallbackReason}",
                            )
                        # Substrait-native producer: execute the
                        # producer's Substrait bytes locally via the
                        # same helper the remote worker uses, wrapped
                        # in the same retry envelope.
                        result, attempt, retryReason = (
                            self._runSubstraitStageWithRetries(
                                stage.producerSubstraitPlanBytes,
                                stage.stageId,
                            )
                        )
                    remotePublishedDescriptors = _remote_published_descriptors(
                        remoteResult
                    )
                    # Bracket the driver-local ``_publish_stage_outputs``
                    # call so leaf stages that fall back to local
                    # execution report publish timing too — previously
                    # only remote stages populated these fields and the
                    # local-fallback case dropped to None. Single-worker
                    # (the driver), so ``publishStartNs`` / End fields
                    # are clock-comparable and we don't need
                    # ``publishWallNsExplicit``.
                    local_publish_start = None
                    local_publish_end = None
                    if remotePublishedDescriptors:
                        exchangeManager.ingestDescriptors(
                            remotePublishedDescriptors, owned=True
                        )
                        publishedDescriptors.extend(remotePublishedDescriptors)
                    else:
                        local_publish_start = time.monotonic_ns()
                        publishedDescriptors.extend(
                            _publish_stage_outputs(
                                stage,
                                result,
                                exchangeManager,
                                stageInputs,
                                downstreamStages,
                                attempt,
                            )
                        )
                        local_publish_end = time.monotonic_ns()
                    producerTransport = None
                    if publishedDescriptors:
                        transports = {
                            descriptor.transport for descriptor in publishedDescriptors
                        }
                        producerTransport = (
                            sorted(transports)[0] if len(transports) == 1 else "mixed"
                        )
                    stage_end_ns = time.monotonic_ns()
                    stageRecords.append(
                        StageTaskResult(
                            stageId=stage.stageId,
                            attempt=attempt,
                            inputCount=0,
                            outputNames=_remote_output_names(result, remoteResult),
                            partitionCount=max(
                                1,
                                max(
                                    (
                                        descriptor.partitionCount
                                        for descriptor in publishedDescriptors
                                    ),
                                    default=0,
                                ),
                            ),
                            exchangeIds=tuple(
                                p.exchangeId for p in stageInputs.get(stage.stageId, [])
                            ),
                            transport=producerTransport,
                            retryReason=retryReason,
                            remote=remoteResult is not None,
                            remoteTaskCount=0
                            if remoteResult is None
                            else _remote_field(remoteResult, "remoteTaskCount", 1),
                            workerId=_remote_field(remoteResult, "workerId"),
                            nodeId=_remote_field(remoteResult, "nodeId"),
                            workerIds=tuple(
                                _remote_field(remoteResult, "workerIds", ())
                            ),
                            nodeIds=tuple(_remote_field(remoteResult, "nodeIds", ())),
                            fallbackReason=fallbackReason,
                            startNs=stage_start_ns,
                            endNs=stage_end_ns,
                            # Prefer driver-bracketed local timing when
                            # the local-fallback path published outputs;
                            # otherwise fall back to whatever the remote
                            # result reported (None if the stage didn't
                            # publish at all).
                            publishStartNs=(
                                local_publish_start
                                if local_publish_start is not None
                                else _remote_field(remoteResult, "publishStartNs")
                            ),
                            publishEndNs=(
                                local_publish_end
                                if local_publish_end is not None
                                else _remote_field(remoteResult, "publishEndNs")
                            ),
                            publishWallNs=_remote_field(remoteResult, "publishWallNs"),
                        )
                    )
                elif stage.inputPlaceholders:
                    if exchangeManager is None:
                        raise RuntimeError(
                            f"Stage {stage.stageId} requires an exchange manager to resolve placeholder inputs."
                        )
                    runtimeInputPlaceholders = tuple(
                        stagePlaceholders.get(stage.stageId, ())
                    )

                    # Wrap the resolve+validate loop in the same retry
                    # envelope that protects the local consumer
                    # compute path. ``AdaptiveExchangeManager.resolve``
                    # calls ``ray.get`` on the detached registry actor,
                    # so transient Ray failures (actor restart, worker
                    # death, etc.) can surface here. Without retry, we
                    # wrap the exception as a plain RuntimeError that
                    # aborts the plan; with retry, the same classifier
                    # the remote envelope uses gives infra failures a
                    # second chance before giving up.
                    def _resolve_placeholders():
                        resolved_pairs = []
                        for placeholder in runtimeInputPlaceholders:
                            boltmlDebugLog(
                                "runtime",
                                f"stage {stage.stageId} resolving placeholder "
                                f"{placeholder.sourceStageId}/{placeholder.exchangeId}",
                            )
                            try:
                                resolved = exchangeManager.resolve(placeholder)
                            except TransientStageError:
                                raise
                            except Exception as error:
                                if _is_retryable_ray_error(error):
                                    raise
                                raise RuntimeError(
                                    f"Stage {stage.stageId} failed to resolve "
                                    f"exchange {placeholder.exchangeId}: {error}"
                                ) from error
                            for descriptor in resolved:
                                try:
                                    placeholder.validateDescriptor(descriptor)
                                except Exception as error:
                                    raise RuntimeError(
                                        f"Stage {stage.stageId} received incompatible "
                                        f"descriptor for exchange "
                                        f"{placeholder.exchangeId}: {error}"
                                    ) from error
                            resolved_pairs.append(tuple(resolved))
                        return resolved_pairs

                    resolved_pairs_result, _, _ = self._withLocalStageRetries(
                        _resolve_placeholders,
                        f"placeholder resolve stage={stage.stageId}",
                    )
                    resolvedInputs = list(resolved_pairs_result)
                    descriptors = [d for tup in resolvedInputs for d in tup]
                    remoteResult = None
                    fallbackReason = None
                    taskSpec = replace(
                        StageTaskSpec.fromStageNode(
                            stage, executionMode=self.executionMode.value
                        ),
                        executionId=executionId,
                        inputPlaceholders=runtimeInputPlaceholders,
                        udfRegistry=udfRegistry,
                    )
                    boltmlTraceLog(
                        "runtime", f"task spec for {stage.stageId}: {taskSpec}"
                    )
                    outputPlaceholders = tuple(stageInputs.get(stage.stageId, ()))
                    publishConfig = (
                        None
                        if exchangeManager is None
                        else exchangeManager.publishConfig()
                    )
                    if self.executionMode == RayExecutionMode.REMOTE_LEAF:
                        if len(
                            runtimeInputPlaceholders
                        ) == 1 and self._shouldDispatchRemotely(
                            stage, taskSpec, resolvedInputs
                        ):
                            boltmlDebugLog(
                                "runtime",
                                f"stage {stage.stageId} selected remote single-input consumer execution",
                            )
                            partitionParallelReason = _partition_parallel_reason(
                                taskSpec
                            )
                            if partitionParallelReason is None:
                                remoteRunner = (  # noqa: E731
                                    lambda attemptId: self._runPartitionedSingleInputConsumerStageRemotely(
                                        replace(taskSpec, attemptId=attemptId),
                                        resolvedInputs[0],
                                        outputPlaceholders,
                                        publishConfig,
                                    )
                                )
                            else:
                                boltmlDebugLog(
                                    "runtime",
                                    f"stage {stage.stageId} using single-task remote aggregate execution reason={partitionParallelReason}",
                                )
                                remoteRunner = (  # noqa: E731
                                    lambda attemptId: self._runSingleInputConsumerStageRemotely(
                                        replace(taskSpec, attemptId=attemptId),
                                        resolvedInputs[0],
                                        outputPlaceholders,
                                        publishConfig,
                                    )
                                )
                            result, remoteResult, attempt, retryReason = (
                                self._runRemoteWithRetries(
                                    remoteRunner, exchangeManager=exchangeManager
                                )
                            )
                        elif len(
                            runtimeInputPlaceholders
                        ) == 2 and self._shouldDispatchRemotely(
                            stage, taskSpec, resolvedInputs
                        ):
                            boltmlDebugLog(
                                "runtime",
                                f"stage {stage.stageId} selected remote multi-input consumer execution",
                            )
                            partitionParallelReason = _partition_parallel_reason(
                                taskSpec
                            )
                            if partitionParallelReason is None:
                                remoteRunner = (  # noqa: E731
                                    lambda attemptId: self._runPartitionedMultiInputConsumerStageRemotely(
                                        replace(taskSpec, attemptId=attemptId),
                                        tuple(resolvedInputs),
                                        outputPlaceholders,
                                        publishConfig,
                                    )
                                )
                            else:
                                boltmlDebugLog(
                                    "runtime",
                                    f"stage {stage.stageId} using single-task remote join execution reason={partitionParallelReason}",
                                )
                                remoteRunner = (  # noqa: E731
                                    lambda attemptId: self._runMultiInputConsumerStageRemotely(
                                        replace(taskSpec, attemptId=attemptId),
                                        tuple(resolvedInputs),
                                        outputPlaceholders,
                                        publishConfig,
                                    )
                                )
                            result, remoteResult, attempt, retryReason = (
                                self._runRemoteWithRetries(
                                    remoteRunner, exchangeManager=exchangeManager
                                )
                            )
                        elif len(runtimeInputPlaceholders) in (1, 2):
                            # Both single-input and multi-input
                            # consumer paths execute the same way
                            # post-Cleanup-B: every consumer stage
                            # built by the Substrait dispatcher
                            # carries ``consumerSubstraitPlanBytes``;
                            # legacy stage construction (with
                            # ``executionPlan`` / ``planTemplate``
                            # / ``logicalOps``) is gone. The remote-
                            # local fallback path collapses to one
                            # branch.
                            #
                            # Wrap the whole consumer stage operation
                            # — exchange load, partition merge, AND
                            # compute — in the local-stage retry
                            # envelope. A transient hiccup in registry
                            # lookup / Ray object fetch / parquet read
                            # / typed-Arrow conversion would otherwise
                            # abort the plan with no retry, even
                            # though the same stage retried fresh would
                            # likely succeed. Outer ``retry_consumer``
                            # captures the env (descriptors,
                            # planBytes, stage id) and gets re-invoked
                            # on each attempt.
                            _resolvedInputs = resolvedInputs
                            _consumerBytes = stage.consumerSubstraitPlanBytes
                            _stageId = stage.stageId

                            def retry_consumer():
                                loadedInputs = [
                                    _mergeResolvedPartitions(
                                        tuple(
                                            exchangeManager.load(descriptor)
                                            for descriptor in resolved
                                        )
                                    )
                                    for resolved in _resolvedInputs
                                ]
                                return self.runStageSubstraitBytes(
                                    _consumerBytes,
                                    _stageId,
                                    *loadedInputs,
                                )

                            result, attempt, retryReason = self._withLocalStageRetries(
                                retry_consumer,
                                f"local substrait consumer stage={_stageId}",
                            )
                            # Distinguish the three reasons we landed in
                            # the local-fallback branch:
                            #   * rollout_gate: rollout level keeps us local
                            #   * small_consumer_input: bytes < threshold
                            #     (driver-collect path)
                            #   * task-spec fallbackReason / consumer_stage
                            if self.rolloutLevel != RayRolloutLevel.REMOTE_ANALYTICAL:
                                fallbackReason = "rollout_gate"
                            elif self._remoteAllowed(stage, taskSpec):
                                # _remoteAllowed says yes but we're here
                                # anyway → small-consumer override fired.
                                fallbackReason = "small_consumer_input"
                            else:
                                fallbackReason = (
                                    taskSpec.fallbackReason or "consumer_stage"
                                )
                    else:
                        # Same retry routing as the sibling consumer
                        # fallback above — wraps exchange load, merge,
                        # AND compute in the local-stage retry envelope
                        # so transient failures in any of those steps
                        # retry rather than aborting the plan.
                        _resolvedInputs = resolvedInputs
                        _consumerBytes = stage.consumerSubstraitPlanBytes
                        _stageId = stage.stageId

                        def retry_consumer():
                            loadedInputs = [
                                _mergeResolvedPartitions(
                                    tuple(
                                        exchangeManager.load(descriptor)
                                        for descriptor in resolved
                                    )
                                )
                                for resolved in _resolvedInputs
                            ]
                            return self.runStageSubstraitBytes(
                                _consumerBytes,
                                _stageId,
                                *loadedInputs,
                            )

                        result, attempt, retryReason = self._withLocalStageRetries(
                            retry_consumer,
                            f"local substrait consumer stage={_stageId}",
                        )
                    transport = None
                    if descriptors:
                        transports = {
                            descriptor.transport for descriptor in descriptors
                        }
                        transport = (
                            sorted(transports)[0] if len(transports) == 1 else "mixed"
                        )
                    remotePublishedDescriptors = _remote_published_descriptors(
                        remoteResult
                    )
                    # Same driver-local publish bracketing as the
                    # leaf-producer site above — see the comment there.
                    local_publish_start = None
                    local_publish_end = None
                    if remotePublishedDescriptors:
                        exchangeManager.ingestDescriptors(
                            remotePublishedDescriptors, owned=True
                        )
                        publishedDescriptors = list(remotePublishedDescriptors)
                    else:
                        local_publish_start = time.monotonic_ns()
                        publishedDescriptors = list(
                            _publish_stage_outputs(
                                stage,
                                result,
                                exchangeManager,
                                stageInputs,
                                downstreamStages,
                                attempt,
                            )
                        )
                        local_publish_end = time.monotonic_ns()
                    if publishedDescriptors:
                        publishedTransports = {
                            descriptor.transport for descriptor in publishedDescriptors
                        }
                        transport = (
                            sorted(publishedTransports)[0]
                            if len(publishedTransports) == 1
                            else "mixed"
                        )
                    stage_end_ns = time.monotonic_ns()
                    stageRecords.append(
                        StageTaskResult(
                            stageId=stage.stageId,
                            attempt=attempt,
                            inputCount=len(runtimeInputPlaceholders),
                            outputNames=_remote_output_names(result, remoteResult),
                            partitionCount=max(
                                1,
                                max(
                                    (
                                        descriptor.partitionCount
                                        for descriptor in publishedDescriptors
                                    ),
                                    default=0,
                                ),
                                *(
                                    placeholder.partitionCount
                                    for placeholder in runtimeInputPlaceholders
                                ),
                            ),
                            exchangeIds=tuple(
                                p.exchangeId for p in runtimeInputPlaceholders
                            ),
                            transport=transport,
                            retryReason=retryReason,
                            remote=remoteResult is not None,
                            remoteTaskCount=0
                            if remoteResult is None
                            else _remote_field(remoteResult, "remoteTaskCount", 1),
                            workerId=_remote_field(remoteResult, "workerId"),
                            nodeId=_remote_field(remoteResult, "nodeId"),
                            workerIds=tuple(
                                _remote_field(remoteResult, "workerIds", ())
                            ),
                            nodeIds=tuple(_remote_field(remoteResult, "nodeIds", ())),
                            fallbackReason=fallbackReason,
                            startNs=stage_start_ns,
                            endNs=stage_end_ns,
                            publishStartNs=(
                                local_publish_start
                                if local_publish_start is not None
                                else _remote_field(remoteResult, "publishStartNs")
                            ),
                            publishEndNs=(
                                local_publish_end
                                if local_publish_end is not None
                                else _remote_field(remoteResult, "publishEndNs")
                            ),
                            publishWallNs=_remote_field(remoteResult, "publishWallNs"),
                        )
                    )
        finally:
            if exchangeManager is not None:
                exchangeManager.cleanup(executionId=executionId)
            if ownedExchangeTmpDir is not None:
                ownedExchangeTmpDir.cleanup()
        plan_end_ns = time.monotonic_ns()
        self._lastStageTaskResults = tuple(stageRecords)
        self._lastExecutionSummary = RuntimeExecutionSummary(
            planId=f"ray-plan-{RayRuntime._planCounter}",
            stages=tuple(stage.toRuntimeExecutionStage() for stage in stageRecords),
            executionMode=self.executionMode.value,
            rolloutLevel=self.rolloutLevel.value,
            startNs=plan_start_ns,
            endNs=plan_end_ns,
        )
        boltmlDebugLog(
            "runtime",
            f"completed stage dag plan={self._lastExecutionSummary.planId} stages={self._lastExecutionSummary.stageIds}",
        )
        return result
