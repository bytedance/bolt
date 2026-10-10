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

from dataclasses import dataclass

import pyarrow as pa
import ray
from pybolt import (
    BigintType,
    BooleanType,
    DoubleType,
    IntegerType,
    RealType,
    RowType,
    SmallintType,
    TinyintType,
    VarcharType,
    VarbinaryType,
    fileFormatFromString,
)

from ...dataframe import DataFrame
from ...executor import LocalExecutor
from ...logging import boltmlDebugEnabled, boltmlDebugLog
from ...tpch import tpchTable
from .exchange_manager import (
    ExchangePartitionDescriptor,
    ExchangePublishConfig,
    loadExchangeDescriptor,
    publishExchangePartition,
)
from .task_spec import StageTaskSpec


@dataclass(frozen=True)
class RemoteLeafResult:
    table: pa.Table | None
    outputNames: tuple[str, ...]
    workerId: str
    nodeId: str
    publishedDescriptors: tuple[ExchangePartitionDescriptor, ...] = ()
    # ``time.monotonic_ns()`` brackets around ``_publish_remote_outputs``
    # so the driver can attribute "exchange publish" wall time to this
    # stage's ``StageTaskResult`` even when execution happened on a
    # remote worker. Values are from the *worker's* clock, not the
    # driver's, so they're useful as a *duration* (publishEnd-Start)
    # but not for cross-stage timeline alignment with driver-side
    # timestamps.
    publishStartNs: int | None = None
    publishEndNs: int | None = None


def _arrow_table_with_bolt_types(dataframe) -> "pa.Table":
    """Thin shim that delegates to the shared ``_typed_arrow`` helper.

    Historical name kept for in-process callers; new code should call
    :func:`._typed_arrow.arrow_table_with_bolt_types` directly. The
    canonical implementation lives there so the exchange publish path
    and the remote-task return path encode bolt-type metadata
    identically.
    """
    from ._typed_arrow import arrow_table_with_bolt_types

    return arrow_table_with_bolt_types(dataframe)


def _configure_bolt_logs_from_env() -> None:
    """Apply ``BOLTML_BOLT_LOG_LEVEL`` / ``BOLTML_BOLT_VLOG`` /
    ``BOLTML_BOLT_LOG_TO_STDERR`` to the running Ray worker's already-
    imported ``pybolt`` module on every remote-task entry.

    Ray reuses worker Python processes across jobs and across tasks
    inside a job. The first task imports ``pybolt`` and runs its
    ``PYBIND11_MODULE`` init, which reads the env vars *once*. If a
    subsequent job arrives with different ``BOLTML_BOLT_*`` settings
    (e.g. the user just opted into ``info``), the worker would still
    be running with the old config because the module body never
    re-executes. Re-applying the gflags setters per task is cheap (a
    handful of variable writes) and ensures Bolt's C++ ``LOG``/
    ``VLOG`` output reflects the *current* job's config — visible in
    the worker's stderr file under ``/tmp/ray/session_latest/logs/``
    and in ``ray logs <job-id>``.
    """
    import os

    try:
        import pybolt
    except Exception:
        return
    if not hasattr(pybolt, "setBoltLogLevel"):
        # Older pybolt build that didn't expose the setters; module
        # init already applied whatever env was visible at first
        # import. Best we can do.
        return
    if (lvl := os.environ.get("BOLTML_BOLT_LOG_LEVEL")) is not None:
        try:
            mapping = {
                "info": 0,
                "warning": 1,
                "warn": 1,
                "error": 2,
                "fatal": 3,
                "off": 2,
                "quiet": 2,
                "none": 2,
            }
            key = lvl.strip().lower()
            # ``mapping.get(key, int(key))`` would *eagerly* evaluate
            # ``int(key)`` even when the key is a named level like
            # ``"info"`` — the ValueError from that int() raise
            # propagates out of the get() call, gets caught by the
            # outer except, and the named level silently drops.
            # Compute the named level first; fall back to int() only
            # when the named lookup misses.
            if key in mapping:
                pybolt.setBoltLogLevel(mapping[key])
            else:
                pybolt.setBoltLogLevel(int(key))
        except Exception:
            pass
    if (v := os.environ.get("BOLTML_BOLT_VLOG")) is not None:
        try:
            pybolt.setBoltVLogLevel(int(v))
        except Exception:
            pass
    raw = os.environ.get("BOLTML_BOLT_LOG_TO_STDERR")
    if raw is not None:
        pybolt.setBoltLogToStderr(raw.strip().lower() in {"1", "true", "yes", "on"})
    elif os.environ.get("BOLTML_BOLT_LOG_LEVEL", "").strip().lower() in {
        "info",
        "0",
    } or (os.environ.get("BOLTML_BOLT_VLOG", "0").strip() not in {"", "0"}):
        # Auto-stderr when the user opted into INFO/VLOG so Ray
        # captures the C++ logs to its worker .err files.
        pybolt.setBoltLogToStderr(True)


def _register_udfs_from_task_spec(taskSpec) -> None:
    """Re-register driver-shipped UDFs on this worker's pybolt singleton.

    ``RayExecutor`` records every ``register{Vector,Aggregation}Function``
    call into a ``UdfRegistry``, ships it inside ``StageTaskSpec.udfRegistry``,
    and the worker calls this helper at the top of every
    ``executeRemote*`` entry point. Without this call the worker would
    have no implementation for the function names referenced in the
    Substrait plan and execution would fail in C++ at lookup time.

    Idempotent: re-registering the same name on pybolt overwrites the
    prior registration, so it's safe to invoke on every task even when
    the same Ray worker process serves multiple consecutive jobs.
    """
    registry = getattr(taskSpec, "udfRegistry", None)
    if not registry:
        return
    # Local import: keep ``remote_worker`` module-load cheap; we only
    # need this code path when at least one UDF is in flight.
    from .udf_spec import reregisterUdfsOnPybolt

    boltmlDebugLog(
        "remote-worker", f"re-registering {len(registry)} UDF(s) from driver"
    )
    reregisterUdfsOnPybolt(registry)


def _materialize_encoded_source(sourceSpec: dict) -> DataFrame:
    if sourceSpec["kind"] == "values":
        return DataFrame(sourceSpec["table"])
    if sourceSpec["kind"] == "tpch_read":
        from ...plan_builder.bolt import BoltPlanBuilderFactory

        factory = BoltPlanBuilderFactory()
        return DataFrame(
            factory.tpchGenerator(
                tpchTable(sourceSpec["table"]),
                scaleFactor=sourceSpec["scaleFactor"],
                numSplits=sourceSpec["numParts"],
            ),
            executor=LocalExecutor(),
            planFactory=factory,
        )
    if sourceSpec["kind"] == "single_file_read":
        typeMap = {
            "BIGINT": BigintType,
            "INTEGER": IntegerType,
            "DOUBLE": DoubleType,
            "REAL": RealType,
            "SMALLINT": SmallintType,
            "TINYINT": TinyintType,
            "BOOLEAN": BooleanType,
            "VARCHAR": VarcharType,
            "VARBINARY": VarbinaryType,
        }
        outputType = RowType(
            list(sourceSpec["output_names"]),
            [typeMap[name]() for name in sourceSpec["output_types"]],
        )
        # single_file_read — build a SubstraitPlanBuilder with a localFilesRel
        # read rel. The remote worker then layers remoteOps on top (via
        # _apply_remote_ops) and executes through BoltTaskExecutor.
        from substrait.proto import algebra
        from ...plan_builder.base import PlanSource
        from ...plan_builder.substrait import (
            SubstraitPlanBuilder,
            SubstraitPlanBuilderFactory,
        )
        from ...substrait.rel import localFilesRel

        factory = SubstraitPlanBuilderFactory()
        fileFormat = fileFormatFromString(sourceSpec["file_format"])
        rel = algebra.Rel(
            read=localFilesRel([sourceSpec["file_path"]], outputType, fileFormat)
        )
        builder = SubstraitPlanBuilder(
            rel,
            list(sourceSpec["output_names"]),
            PlanSource(
                "single_file_read", (sourceSpec["file_path"], outputType, fileFormat)
            ),
            factory,
            [],
        )
        return DataFrame(builder, executor=LocalExecutor(), planFactory=factory)
    raise NotImplementedError(
        f"remote leaf execution does not support source kind {sourceSpec['kind']}."
    )


def _materialize_encoded_plan(planSpec: dict) -> DataFrame:
    dataframe = _materialize_encoded_source(planSpec["source"])
    helperSpec = StageTaskSpec(
        stageId="encoded-plan",
        executionKind="producer",
        remoteOps=tuple(planSpec["operations"]),
    )
    return _apply_remote_ops(dataframe, helperSpec)


def _audit_read_rels(r, path: str = ""):
    """Walk the rel tree in the same order _traverse_and_replace_read does
    and debug-log every read rel's output schema.

    Routed through the ``boltml`` debug logger (component
    ``"subst-audit"``); no-op unless that logger is at ``DEBUG``. The
    ``path`` breadcrumb tracks join-side descent (`` L`` / `` R``)."""
    kind = r.WhichOneof("rel_type")
    if kind == "read":
        names = list(r.read.base_schema.names) if r.read.HasField("base_schema") else []
        boltmlDebugLog("subst-audit", f"{path} kind=read names={names}".strip())
    elif kind == "filter":
        _audit_read_rels(r.filter.input, path)
    elif kind == "project":
        _audit_read_rels(r.project.input, path)
    elif kind == "aggregate":
        _audit_read_rels(r.aggregate.input, path)
    elif kind == "sort":
        _audit_read_rels(r.sort.input, path)
    elif kind == "join":
        _audit_read_rels(r.join.left, path + " L")
        _audit_read_rels(r.join.right, path + " R")
    elif kind == "hash_join":
        _audit_read_rels(r.hash_join.left, path + " L")
        _audit_read_rels(r.hash_join.right, path + " R")
    elif kind == "merge_join":
        _audit_read_rels(r.merge_join.left, path + " L")
        _audit_read_rels(r.merge_join.right, path + " R")
    elif kind == "fetch":
        _audit_read_rels(r.fetch.input, path)
    elif kind == "extension_single":
        _audit_read_rels(r.extension_single.input, path)


def _traverse_and_replace_read(r, new_reads):
    kind = r.WhichOneof("rel_type")
    if kind == "read":
        if new_reads:
            new_r = new_reads.pop(0)
            r.CopyFrom(new_r)
    elif kind == "filter":
        _traverse_and_replace_read(r.filter.input, new_reads)
    elif kind == "project":
        _traverse_and_replace_read(r.project.input, new_reads)
    elif kind == "aggregate":
        _traverse_and_replace_read(r.aggregate.input, new_reads)
    elif kind == "sort":
        _traverse_and_replace_read(r.sort.input, new_reads)
    elif kind == "join":
        _traverse_and_replace_read(r.join.left, new_reads)
        _traverse_and_replace_read(r.join.right, new_reads)
    elif kind == "hash_join":
        _traverse_and_replace_read(r.hash_join.left, new_reads)
        _traverse_and_replace_read(r.hash_join.right, new_reads)
    elif kind == "merge_join":
        _traverse_and_replace_read(r.merge_join.left, new_reads)
        _traverse_and_replace_read(r.merge_join.right, new_reads)
    elif kind == "nested_loop_join":
        _traverse_and_replace_read(r.nested_loop_join.left, new_reads)
        _traverse_and_replace_read(r.nested_loop_join.right, new_reads)
    elif kind == "cross":
        _traverse_and_replace_read(r.cross.left, new_reads)
        _traverse_and_replace_read(r.cross.right, new_reads)
    elif kind == "set":
        for inp in r.set.inputs:
            _traverse_and_replace_read(inp, new_reads)
    elif kind == "fetch":
        _traverse_and_replace_read(r.fetch.input, new_reads)
    elif kind == "extension_single":
        _traverse_and_replace_read(r.extension_single.input, new_reads)
    elif kind == "extension_multi":
        for inp in r.extension_multi.inputs:
            _traverse_and_replace_read(inp, new_reads)


_TMP_INPUT_DIR = None


def _tmp_input_dir():
    global _TMP_INPUT_DIR
    if _TMP_INPUT_DIR is None:
        import atexit
        import shutil
        import tempfile

        _TMP_INPUT_DIR = tempfile.mkdtemp(prefix="boltml-remote-input-")
        # Per-call cleanup in ``_execute_substrait_bytes``
        # deletes individual spill files as soon as their consumer
        # task completes. This atexit hook is the safety net for
        # processes that crash mid-task (would otherwise orphan the
        # tempdir on disk -- a single SF=10 sweep accumulated ~19 GB
        # of orphaned spills before a "No space left on device" crash).
        atexit.register(shutil.rmtree, _TMP_INPUT_DIR, ignore_errors=True)
    return _TMP_INPUT_DIR


def _spill_dataframe_to_parquet(df, tag: str) -> tuple[str, object, object]:
    """Write a DataFrame's RowVector to a temp parquet and return (uri, rowType, fileFormat).

    Substitutes the read rel in a serialized consumer Substrait plan. Embedding
    the loaded data as a virtualTable literal is O(rows) in substraitLiteral's
    recursion and blows up on SF1. A one-shot parquet spill + localFilesRel
    keeps the plan small and lets Bolt's C++ parquet reader stream the data."""
    import os
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pybolt as pb

    tbl = df.toArrow(pa.Table)
    rowType = df._data_.dtype()
    # uuid4 (not id(df)) because Python reuses object ids after GC; on a
    # reused Ray worker process, sequential stages could hit the same spill
    # path, and a stale file cached by the C++ parquet reader would be read
    # instead of the fresh write. Seen as stage-4 shardId=0 producing 0 join
    # rows on Q3/Q10 despite ~2900 overlapping keys.
    import uuid

    path = os.path.join(_tmp_input_dir(), f"{tag}-{uuid.uuid4().hex}.parquet")
    # Write to a tempfile and atomic-rename so Bolt's reader never sees a
    # partial buffer (has been observed on Ray workers when the reader
    # opens the file before the writer's final flush).
    tmp = path + ".partial"
    pq.write_table(tbl, tmp)
    assert os.path.getsize(tmp) > 0, (
        f"empty parquet spill at {tmp} rows={len(tbl)} schema={tbl.schema}"
    )
    os.replace(tmp, path)
    return f"file://{path}", rowType, pb.FileFormat.PARQUET


def _execute_substrait_bytes(
    substraitPlanBytes,
    *dfs,
    leafShardIndex: int = 0,
    leafShardCount: int = 1,
):
    """Execute a Substrait plan in-process via ``BoltTaskExecutor``.

    ``leafShardIndex`` / ``leafShardCount``: when ``leafShardCount > 1``
    the runtime is fanning a single leaf out across N parallel Ray tasks;
    each task processes only ``splitList[leafShardIndex::leafShardCount]``
    of the plan's input splits. Defaults of (0, 1) are a no-op for
    consumer paths and the local-fallback path.

    Two correctness guards in the sharded case:

    * Empty shard short-circuit. If after slicing every scan node ends
      up with zero splits, return an empty Arrow table matching the
      plan's output schema rather than calling ``executor.execute``.
      Bolt's ``BoltTaskExecutor::execute`` only invokes
      ``task->noMoreSplits(planNodeId)`` when ``!splits.empty()``
      (see ``bolt/python/pybolt/executor.cpp``), so an empty
      ``addSplits`` would leave the kSerial scan waiting forever and
      Velox throws ``"Serial execution requires all splits to be added
      before calling Task::next()."`` We bypass execution entirely.
    * Per-shard task name. ``BoltTaskExecutor`` keys its child memory
      pool by task name; multiple shards landing in the same Ray
      worker process collide on the bare ``"RemoteWorkerTask"`` name
      (``"Child memory pool task.RemoteWorkerTask already exists in
      pybolt_root_pool"``). Sharded runs append the shard index to
      the name so each task is unique within a process.
    """
    import os
    from substrait.proto import plan, algebra
    from boltml.substrait.rel import localFilesRel
    from pybolt import convertSubstraitPlan, BoltTaskExecutor, PlanBuilder

    substraitPlan = plan.Plan()
    substraitPlan.ParseFromString(substraitPlanBytes)

    # Leaf tasks ship no input dataframes — the plan's own scan rels already
    # point at the right parquet files. Only consumer tasks (with exchange
    # inputs) need the read-rel substitution.
    #
    # Track spill URIs so we can delete them after this task
    # finishes. Without cleanup the per-process tempdir grows unbounded
    # (a single SF=10 multi-config sweep accumulated 19 GB of spill
    # parquets and triggered "No space left on device" mid-run).
    spill_paths: list[str] = []
    if dfs:
        if boltmlDebugEnabled():
            for idx, df in enumerate(dfs):
                tbl = df.toArrow(pa.Table)
                boltmlDebugLog(
                    "subst-audit",
                    f"input[{idx}] rows={len(tbl)} cols={tbl.column_names}",
                )
            _audit_read_rels(substraitPlan.relations[0].root.input, path="plan-read")
        new_reads = []
        for idx, df in enumerate(dfs):
            uri, rowType, fileFormat = _spill_dataframe_to_parquet(df, f"input{idx}")
            spill_paths.append(uri)
            rel = localFilesRel([uri], rowType, fileFormat)
            new_reads.append(algebra.Rel(read=rel))
        _traverse_and_replace_read(substraitPlan.relations[0].root.input, new_reads)

    planNode, splits = convertSubstraitPlan(substraitPlan.SerializeToString())

    # Slice splits per shard. ``round-robin`` (vs contiguous) keeps the
    # split count balanced when ``len(splitList) % leafShardCount != 0``.
    #
    # Per-node empty tracking: we need to short-circuit if ANY scan node
    # has a non-empty original split list but received zero splits after
    # sharding (e.g. ``shardCount=12`` over a 1-split table — only shard
    # 0 gets work; shards 1-11 must short-circuit). Adding an empty
    # split list for a node would leave that node's Velox kSerial scan
    # waiting forever for a ``noMoreSplits`` signal that
    # ``BoltTaskExecutor::execute`` only emits when ``!splits.empty()``
    # (see ``bolt/python/pybolt/executor.cpp``). The previous
    # implementation only checked the GLOBAL sharded total, which
    # missed leaves with mixed split-count nodes (e.g. q5: a leaf may
    # have one 12-split scan and one 1-split scan).
    sharded_splits = {}
    any_node_naturally_empty_after_shard = False
    if leafShardCount > 1:
        for nodeId, splitList in splits.items():
            sharded = splitList[leafShardIndex::leafShardCount]
            sharded_splits[nodeId] = sharded
            boltmlDebugLog(
                "remote-worker",
                f"leaf shard {leafShardIndex}/{leafShardCount} "
                f"node={nodeId} splits {len(splitList)} -> {len(sharded)}",
            )
            # If this node had splits but our shard got none, we must
            # short-circuit -- can't safely run the kSerial scan.
            if splitList and not sharded:
                any_node_naturally_empty_after_shard = True
    else:
        sharded_splits = splits

    # Empty-shard short-circuit. Fires when sharding reduced ANY
    # node's non-empty split set to empty for THIS shard. Plans with
    # zero splits in the original splits map (Values sources, where
    # data is in the plan literal) are NOT short-circuited -- only
    # shards that lost coverage of an originally-non-empty node.
    if leafShardCount > 1 and any_node_naturally_empty_after_shard:
        boltmlDebugLog(
            "remote-worker",
            f"leaf shard {leafShardIndex}/{leafShardCount} empty -> "
            "returning empty table without executing",
        )
        # Build an empty DataFrame using the typed-empty helper so
        # decimal and container types survive
        # the empty-shard short-circuit. The prior code mapped each
        # column through ``_bolt_type_to_arrow`` which only covered
        # primitives and degraded unknowns to ``binary`` — those
        # degraded types could then become authoritative on
        # ``ExchangePartitionDescriptor.outputTypes``. The C++ side
        # ``BaseVector::create<...>(type, 0, pool)`` route under
        # ``pybolt.rowVector(rowType)`` preserves every bolt type
        # exactly because it dispatches on the type kind C++-side.
        outputType = planNode.outputType()
        names = [outputType.nameOf(i) for i in range(len(outputType))]
        type_strs = [str(outputType.childAt(i)) for i in range(len(outputType))]
        try:
            from ._typed_arrow import empty_dataframe_with_bolt_types

            return empty_dataframe_with_bolt_types(names, type_strs)
        except Exception:  # noqa: BLE001
            # Fall back to the Arrow-default empty path if the bolt
            # type parser doesn't know how to construct one of these
            # types yet (e.g. an unsupported decimal precision).
            # Loud warning at parse-time elsewhere — here we keep
            # execution moving.
            fields = [
                pa.field(name, _bolt_type_to_arrow(outputType.childAt(i)))
                for i, name in enumerate(names)
            ]
            empty_table = pa.table(
                {f.name: pa.array([], type=f.type) for f in fields},
                schema=pa.schema(fields),
            )
            from boltml import DataFrame

            return DataFrame(empty_table)

    executor = BoltTaskExecutor()
    for nodeId, splitList in sharded_splits.items():
        executor.addSplits(nodeId, splitList)

    fragment = PlanBuilder(planNode).planFragment()
    if leafShardCount > 1:
        taskName = f"RemoteWorkerTask-shard-{leafShardIndex}-of-{leafShardCount}"
    else:
        taskName = "RemoteWorkerTask"
    try:
        res = executor.execute(fragment, taskName)
    finally:
        # Clean up the spill parquets we wrote at line ~310.
        # Bolt's parquet reader is done with the file by the time
        # ``executor.execute`` returns (kSerial execution completes
        # synchronously). Deleting in finally also covers the error
        # path where execute throws.
        for uri in spill_paths:
            local_path = (
                uri.removeprefix("file://") if uri.startswith("file://") else uri
            )
            try:
                os.remove(local_path)
            except OSError:
                pass  # best-effort; tempdir cleanup is the safety net
    from boltml import DataFrame

    if res is None:
        # ``BoltTaskExecutor::execute`` returns ``None`` when the plan
        # produced no rows (e.g. filter eliminated every input row).
        # Construct a schema-bearing empty RowVector here so consumers
        # downstream (exchange-publish ``outputNames`` / ``outputTypes``,
        # Arrow conversion, ``DataFrame.__len__`` / ``childAt``) keep
        # their type information rather than getting a schema-less
        # empty DataFrame. The C++ side intentionally returns ``None``
        # to preserve the original boltml contract (pybolt fuzzer
        # tests rely on it); the schema-bearing empty path lives in
        # Python at every distributed call site.
        from pybolt import rowVector

        res = rowVector(planNode.outputType())
    return DataFrame(res)


def _plan_contains_aggregate(substraitPlan) -> bool:
    """Return True iff the plan tree contains ANY AggregateRel.

    Used by the leaf-fan-out gate, which needs the broad ``any-aggregate``
    discriminator because the fan-out path has no in-stage merge step
    regardless of aggregate phase: a leaf whose plan contains any
    aggregate must run as a single task so the per-shard partials
    aren't left un-merged.
    """
    if not substraitPlan.relations:
        return False
    plan_rel = substraitPlan.relations[0]
    if not plan_rel.HasField("root"):
        return False
    return _rel_contains_aggregate(plan_rel.root.input)


def _plan_has_shardable_input(substraitPlan) -> bool:
    """Return True iff the plan reads from at least one ``local_files``
    source (a parquet/csv file scan).

    Used by the leaf-fan-out gate. The fan-out path slices the
    splits map across N tasks, but Substrait ``virtual_table`` sources
    (``DataFrame(arrow_table)``-style in-memory data) carry their data
    in the plan literal -- they have NO splits to slice. With
    ``leafShardCount=N``, every shard executes the full plan AND sees
    every Values row, producing N copies of the same data. The
    downstream join then emits ``N * (matches per partition)`` rows.

    Concrete bug this fixed: TPC-H q15 materializes ``rev`` once via
    ``toArrow`` then re-wraps as ``DataFrame(rev_table)`` so the same
    revenue values feed both the ``max()`` subquery and the equality
    filter (avoiding float-eq fragility). With ``LEAF=4``, each of 4
    leaf shards saw the full 1-row top-revenue Values table; the join
    with sharded ``supplier`` then emitted 4 duplicate output rows.
    Forcing ``shardCount=1`` for Values-only leaves gives the correct
    1-row result.

    Plans that mix Values with at least one ``local_files`` scan are
    treated as shardable -- the Values side replicates per shard, but
    so does the scan side, and the union typically has the right shape
    for downstream joins (the correctness depends on the join's role).
    The narrow ``no shardable input at all`` test catches the q15
    pattern without disturbing mixed-source plans.
    """
    if not substraitPlan.relations:
        return False
    plan_rel = substraitPlan.relations[0]
    if not plan_rel.HasField("root"):
        return False
    return _rel_has_local_files_read(plan_rel.root.input)


def _rel_has_local_files_read(rel) -> bool:
    """Walk a Substrait Rel tree, return True iff any ReadRel uses
    ``local_files``. ``virtual_table`` and ``named_table`` reads do
    NOT count as shardable inputs."""
    kind = rel.WhichOneof("rel_type")
    if kind == "read":
        return rel.read.HasField("local_files")
    # Single-input rels: recurse into ``input``.
    if kind in (
        "filter",
        "project",
        "fetch",
        "sort",
        "exchange",
        "expand",
        "write",
        "extension_single",
        "aggregate",
    ):
        inner = getattr(rel, kind)
        if inner.HasField("input"):
            return _rel_has_local_files_read(inner.input)
        return False
    # Dual-input rels (joins).
    if kind in (
        "hash_join",
        "merge_join",
        "nested_loop_join",
        "join",
        "cross",
    ):
        inner = getattr(rel, kind)
        if inner.HasField("left") and _rel_has_local_files_read(inner.left):
            return True
        if inner.HasField("right") and _rel_has_local_files_read(inner.right):
            return True
        return False
    # Multi-input rels (set ops).
    if kind == "set":
        for inp in rel.set.inputs:
            if _rel_has_local_files_read(inp):
                return True
        return False
    return False


def _plan_has_short_split_input(substraitPlan, shardCount: int) -> bool:
    """Return True iff the plan contains ANY ``local_files`` ReadRel
    whose file count is in ``(0, shardCount)`` — independent of where
    that read sits in the tree.

    Implementation note: this walks every ReadRel and returns True for
    any short-split read, even when the read is a single-table scan
    with no join. That's intentionally conservative — refusing fan-out
    on a no-join short-split scan trades a tiny bit of throughput for
    one fewer correctness escape valve. The companion gate
    ``_plan_contains_join`` exists precisely so the runtime can
    distinguish the two cases when a more permissive policy is wanted.

    Used by the leaf-fan-out gate. The fan-out path slices each scan
    node's split list round-robin across N shards. When a scan has
    fewer files than N (e.g. ``nation`` = 1 file vs ``shardCount=16``),
    only the first few shards receive splits and the remaining ones
    short-circuit to an empty Arrow table. For pure single-table scans
    that's correct (the union of all shards' rows = the full table).

    But when the leaf plan JOINS that under-sharded scan with a
    fully-sharded scan (the common TPC-H pattern: ``lineitem`` joined
    with ``nation``/``supplier``/``region``), the empty-shard short-
    circuit drops most of the work: shard k still has its lineitem
    slice, but if it has no nation rows, its empty-shard guard returns
    nothing — so only shard 0's contribution lands in the final result.
    Net effect: the global join uses ~1/N of the lineitem rows, and
    aggregates downstream are correspondingly under-counted.

    Concrete bug this gate prevents: TPC-H q5/q7/q10/q21 at SF=10 with
    ``BOLTML_LEAF_PARALLELISM>1`` and the multi-file dataset. q5's
    revenue came back ~470M off vs ~537M golden (87% under-counted),
    matching the (16-1)/16 = 94% expected loss almost exactly. Refusing
    fan-out when any participating scan is under-sharded keeps the
    result correct at the cost of single-task throughput on that stage.

    Note: a single-table scan with no join (e.g. q6 = scan + filter +
    SUM, gated separately by ``_plan_contains_aggregate``) does NOT
    actually hit the under-count path — the empty shards contribute
    zero rows the FINAL agg ignores. We still refuse fan-out for
    consistency; pair this with ``_plan_contains_join`` if you want a
    no-join carve-out.
    """
    if not substraitPlan.relations:
        return False
    plan_rel = substraitPlan.relations[0]
    if not plan_rel.HasField("root"):
        return False
    return _rel_has_short_split_input(plan_rel.root.input, shardCount)


def _rel_has_short_split_input(rel, shardCount: int) -> bool:
    """Walk a Substrait Rel tree, return True iff any ReadRel uses
    ``local_files`` with fewer files than ``shardCount``."""
    kind = rel.WhichOneof("rel_type")
    if kind == "read":
        if rel.read.HasField("local_files"):
            n_files = len(rel.read.local_files.items)
            return 0 < n_files < shardCount
        return False
    # Single-input rels: recurse into ``input``.
    if kind in (
        "filter",
        "project",
        "fetch",
        "sort",
        "exchange",
        "expand",
        "write",
        "extension_single",
        "aggregate",
    ):
        inner = getattr(rel, kind)
        if inner.HasField("input"):
            return _rel_has_short_split_input(inner.input, shardCount)
        return False
    # Dual-input rels (joins).
    if kind in (
        "hash_join",
        "merge_join",
        "nested_loop_join",
        "join",
        "cross",
    ):
        inner = getattr(rel, kind)
        if inner.HasField("left") and _rel_has_short_split_input(
            inner.left, shardCount
        ):
            return True
        if inner.HasField("right") and _rel_has_short_split_input(
            inner.right, shardCount
        ):
            return True
        return False
    # Multi-input rels (set ops).
    if kind == "set":
        for inp in rel.set.inputs:
            if _rel_has_short_split_input(inp, shardCount):
                return True
        return False
    return False


def _plan_contains_join(substraitPlan) -> bool:
    """Return True iff the plan tree contains any join Rel.

    Used by the leaf-fan-out gate. Leaf-fan-out partitions each scan node's
    splits round-robin by FILE INDEX, not by JOIN KEY. When a leaf plan
    contains a join between two multi-shard scans, each shard processes
    its own slice of both sides INDEPENDENTLY -- but because the slices
    are NOT co-partitioned by the join key, rows in one shard's left side
    that should match rows in another shard's right side are silently
    DROPPED (no shuffle exchange exists between the two scans within a
    single leaf).

    Concrete bug this fixes: TPC-H q5 at SF=10 multi-file with
    ``BOLTML_LEAF_PARALLELISM=4``. The leaf includes
    ``lineitem (16 files, 4 splits/shard) JOIN orders (8 files, 2/shard)
    JOIN customer (4 files, 1/shard)``. Each shard joins its own
    file-index-aligned slice, but file index does NOT match orderkey/
    custkey, so most cross-shard matches are missed. Revenue came back
    134M vs golden 537M -- exactly 1/4, matching the (N-1)/N drop for
    N=4 fan-out.

    Note: even if all participating scans are equally sharded
    (e.g. N=4 across 4-shard customer + 4-shard orders + 16-shard
    lineitem), the data inside each shard's file is partitioned by ROW
    POSITION, not by JOIN KEY. So the join still drops cross-shard
    matches. The only safe fan-out for join-containing leaves is when
    the data is explicitly hash-partitioned by the join key on disk
    (which BoltML's harness does not produce).

    Refusing fan-out for any join-containing leaf keeps the result
    correct at the cost of single-task throughput. Future optimization:
    detect ``BroadcastSmallBuildSide`` patterns and allow fan-out when
    the build side is broadcast (replicated to every shard).
    """
    if not substraitPlan.relations:
        return False
    plan_rel = substraitPlan.relations[0]
    if not plan_rel.HasField("root"):
        return False
    return _rel_contains_join(plan_rel.root.input)


def _rel_contains_join(rel) -> bool:
    """Walk a Substrait Rel tree, return True iff any join Rel is found."""
    kind = rel.WhichOneof("rel_type")
    if kind in (
        "hash_join",
        "merge_join",
        "nested_loop_join",
        "join",
        "cross",
    ):
        return True
    if kind in (
        "filter",
        "project",
        "fetch",
        "sort",
        "exchange",
        "expand",
        "write",
        "extension_single",
        "aggregate",
    ):
        inner = getattr(rel, kind)
        if inner.HasField("input"):
            return _rel_contains_join(inner.input)
        return False
    if kind == "set":
        for inp in rel.set.inputs:
            if _rel_contains_join(inp):
                return True
        return False
    return False


def _rel_contains_aggregate(rel, safe_phases=None) -> bool:
    kind = rel.WhichOneof("rel_type")
    if kind == "aggregate":
        # If safe_phases provided, only flag this aggregate as a hit
        # when its measures' phase is NOT in the safe set. (PARTIAL
        # aggregates whose downstream stage merges them are safe.)
        if safe_phases is not None:
            agg = rel.aggregate
            if agg.measures:
                phase = agg.measures[0].measure.phase
                if phase in safe_phases:
                    # Aggregate is safe; keep walking through children
                    # in case a downstream child contains an UNSAFE
                    # aggregate.
                    if agg.HasField("input"):
                        return _rel_contains_aggregate(
                            agg.input, safe_phases=safe_phases
                        )
                    return False
        return True
    # Single-input rels: recurse into ``input``.
    if kind in (
        "filter",
        "project",
        "fetch",
        "sort",
        "exchange",
        "expand",
        "write",
        "extension_single",
    ):
        inner = getattr(rel, kind)
        if inner.HasField("input"):
            return _rel_contains_aggregate(inner.input, safe_phases=safe_phases)
        return False
    # Dual-input rels (joins).
    if kind in (
        "hash_join",
        "merge_join",
        "nested_loop_join",
        "join",
        "cross",
    ):
        inner = getattr(rel, kind)
        if inner.HasField("left") and _rel_contains_aggregate(
            inner.left, safe_phases=safe_phases
        ):
            return True
        if inner.HasField("right") and _rel_contains_aggregate(
            inner.right, safe_phases=safe_phases
        ):
            return True
        return False
    # Multi-input rels (set ops).
    if kind == "set":
        for inp in rel.set.inputs:
            if _rel_contains_aggregate(inp):
                return True
        return False
    # Leaf rels (read, etc.) — no children, no aggregate.
    return False


def _bolt_type_to_arrow(boltType):
    """Map a pybolt type to its Arrow equivalent for empty-table fallback."""
    import pyarrow as pa

    name = type(boltType).__name__
    mapping = {
        "BigintType": pa.int64(),
        "IntegerType": pa.int32(),
        "SmallintType": pa.int16(),
        "TinyintType": pa.int8(),
        "DoubleType": pa.float64(),
        "RealType": pa.float32(),
        "BooleanType": pa.bool_(),
        "VarcharType": pa.string(),
        "VarbinaryType": pa.binary(),
        "TimestampType": pa.timestamp("us"),
    }
    if name in mapping:
        return mapping[name]
    # Fallback: bytes — keeps the empty table buildable even for types
    # the empty-shard path hasn't seen yet (which never has data anyway).
    return pa.binary()


def _apply_leaf_remote_ops(taskSpec: StageTaskSpec) -> DataFrame:
    """Execute a leaf stage's Substrait plan directly — no read-rel patching.

    The plan already has the scan pointing at the source parquet; there's no
    exchange input to splice in. ``leafShardIndex`` / ``leafShardCount``
    flow into the executor so when the runtime fans this leaf out across
    N parallel Ray tasks each task only handles its slice of splits.
    """
    if not taskSpec.substraitPlanBytes:
        raise RuntimeError(
            f"Stage {taskSpec.stageId} has no substraitPlanBytes. "
            "BoltML remote execution is Substrait-only."
        )
    return _execute_substrait_bytes(
        taskSpec.substraitPlanBytes,
        leafShardIndex=taskSpec.leafShardIndex,
        leafShardCount=taskSpec.leafShardCount,
    )


def _apply_remote_ops(df: DataFrame, taskSpec: StageTaskSpec) -> DataFrame:
    if not taskSpec.substraitPlanBytes:
        raise RuntimeError(
            f"Stage {taskSpec.stageId} has no substraitPlanBytes. "
            "BoltML remote execution is Substrait-only: every operator must run "
            "in C++ via BoltTaskExecutor. The Python IR fallback has been removed."
        )
    return _execute_substrait_bytes(taskSpec.substraitPlanBytes, df)


def _apply_remote_multi_input_ops(
    lhs: DataFrame,
    rhs: DataFrame,
    taskSpec: StageTaskSpec,
) -> DataFrame:
    if not taskSpec.substraitPlanBytes:
        raise RuntimeError(
            f"Stage {taskSpec.stageId} has no substraitPlanBytes. "
            "BoltML remote execution is Substrait-only: every operator must run "
            "in C++ via BoltTaskExecutor. The Python IR fallback has been removed."
        )
    return _execute_substrait_bytes(taskSpec.substraitPlanBytes, lhs, rhs)


def _materialize_leaf_task(taskSpec: StageTaskSpec) -> DataFrame:
    boltmlDebugLog("remote-worker", f"materialize leaf task stage={taskSpec.stageId}")
    if not taskSpec.substraitPlanBytes:
        raise RuntimeError(
            f"Stage {taskSpec.stageId} has no substraitPlanBytes; "
            "every remote leaf task is Substrait-native."
        )
    return _apply_leaf_remote_ops(taskSpec)


def _load_exchange_inputs(
    descriptors: tuple[ExchangePartitionDescriptor, ...],
) -> DataFrame:
    loaded = tuple(loadExchangeDescriptor(descriptor) for descriptor in descriptors)
    if len(loaded) == 1:
        return loaded[0]
    # Same typed-Arrow round-trip as the driver-side
    # ``_mergeResolvedPartitions``: ``loadExchangeDescriptor``
    # preserved bolt types on each individual descriptor, but plain
    # ``dataframe.toArrow(pa.Table) -> concat -> DataFrame`` drops
    # the tags again. Re-tag before concat, typed-rebuild after.
    from ._typed_arrow import arrow_table_with_bolt_types, dataframe_from_typed_arrow

    tables = [arrow_table_with_bolt_types(dataframe) for dataframe in loaded]
    merged = pa.concat_tables(tables)
    fallback_types = [
        str(loaded[0].dtype.childAt(i)) for i in range(len(loaded[0].dtype))
    ]
    return dataframe_from_typed_arrow(merged, fallback_types=fallback_types)


def _split_for_publish(
    dataframe: DataFrame, partitioning: str, partitionCount: int
) -> tuple[DataFrame, ...]:
    from .runtime import _splitProducedResult

    return _splitProducedResult(dataframe._data_, partitioning, partitionCount)


def _publish_remote_outputs(
    dataframe: DataFrame,
    outputPlaceholders,
    publishConfig: ExchangePublishConfig | None,
    taskSpec: StageTaskSpec,
) -> tuple[ExchangePartitionDescriptor, ...]:
    if not outputPlaceholders:
        return ()
    if publishConfig is None:
        raise RuntimeError(
            f"Stage {taskSpec.stageId} is missing publish config for remote exchange handoff."
        )
    published = []
    # Shard discriminator on each published descriptor: leaf tasks
    # fanned out across multiple Ray tasks all default to
    # ``partitionId=0`` and need ``leafShardIndex`` to disambiguate;
    # consumer tasks each carry a unique ``partitionId`` (their assigned
    # consumer-side partition) and use that.
    if taskSpec.leafShardCount > 1:
        shardId = str(taskSpec.leafShardIndex)
    else:
        shardId = str(taskSpec.partitionId)
    for placeholder in outputPlaceholders:
        partitions = _split_for_publish(
            dataframe, placeholder.partitioning, placeholder.partitionCount
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
                f"stage={taskSpec.stageId} shardId={shardId}"
                f" exchange={placeholder.exchangeId}"
                f" partitioning={placeholder.partitioning}"
                f" partitionCount={placeholder.partitionCount}"
                f" per_partition_rows={sizes} total={sum(sizes)}",
            )
        for partitionId, partition in enumerate(partitions):
            published.append(
                publishExchangePartition(
                    publishConfig,
                    taskSpec.executionId,
                    taskSpec.stageId,
                    placeholder.exchangeId,
                    partition,
                    placeholder.partitioning,
                    taskSpec.attemptId,
                    partitionId,
                    placeholder.partitionCount,
                    shardId=shardId,
                )
            )
    return tuple(published)


@ray.remote
def executeRemoteLeafTask(
    taskSpec: StageTaskSpec,
    outputPlaceholders=(),
    publishConfig: ExchangePublishConfig | None = None,
) -> RemoteLeafResult:
    _configure_bolt_logs_from_env()
    _register_udfs_from_task_spec(taskSpec)
    boltmlDebugLog(
        "remote-worker",
        f"execute leaf task stage={taskSpec.stageId} substraitBytes={len(taskSpec.substraitPlanBytes) if taskSpec.substraitPlanBytes else 0}",
    )
    import time as _t

    dataframe = _materialize_leaf_task(taskSpec)
    _pub_start = _t.monotonic_ns()
    publishedDescriptors = _publish_remote_outputs(
        dataframe, outputPlaceholders, publishConfig, taskSpec
    )
    _pub_end = _t.monotonic_ns()
    # H4 cache: convert to arrow once, reuse for both `table` and `outputNames`.
    # ``_arrow_table_with_bolt_types`` tags the Arrow schema metadata with each
    # column's original Bolt type for reconstruction on the driver.
    arrow_table = _arrow_table_with_bolt_types(dataframe)
    table = None if publishedDescriptors else arrow_table
    runtimeContext = ray.get_runtime_context()
    return RemoteLeafResult(
        table=table,
        outputNames=tuple(arrow_table.column_names),
        workerId=runtimeContext.get_worker_id(),
        nodeId=runtimeContext.get_node_id(),
        publishedDescriptors=publishedDescriptors,
        publishStartNs=_pub_start if publishedDescriptors else None,
        publishEndNs=_pub_end if publishedDescriptors else None,
    )


@ray.remote
def executeRemoteSingleInputConsumer(
    taskSpec: StageTaskSpec,
    inputDescriptors: tuple[ExchangePartitionDescriptor, ...],
    outputPlaceholders=(),
    publishConfig: ExchangePublishConfig | None = None,
) -> RemoteLeafResult:
    _configure_bolt_logs_from_env()
    _register_udfs_from_task_spec(taskSpec)
    boltmlDebugLog(
        "remote-worker",
        f"execute single-input consumer stage={taskSpec.stageId} substraitBytes={len(taskSpec.substraitPlanBytes) if taskSpec.substraitPlanBytes else 0}",
    )
    import time as _t

    dataframe = _apply_remote_ops(_load_exchange_inputs(inputDescriptors), taskSpec)
    _pub_start = _t.monotonic_ns()
    publishedDescriptors = _publish_remote_outputs(
        dataframe, outputPlaceholders, publishConfig, taskSpec
    )
    _pub_end = _t.monotonic_ns()
    # H4 cache: convert to arrow once, reuse for both `table` and `outputNames`.
    # ``_arrow_table_with_bolt_types`` tags the Arrow schema metadata with each
    # column's original Bolt type for reconstruction on the driver.
    arrow_table = _arrow_table_with_bolt_types(dataframe)
    table = None if publishedDescriptors else arrow_table
    runtimeContext = ray.get_runtime_context()
    return RemoteLeafResult(
        table=table,
        outputNames=tuple(arrow_table.column_names),
        workerId=runtimeContext.get_worker_id(),
        nodeId=runtimeContext.get_node_id(),
        publishedDescriptors=publishedDescriptors,
        publishStartNs=_pub_start if publishedDescriptors else None,
        publishEndNs=_pub_end if publishedDescriptors else None,
    )


@ray.remote
def executeRemoteMultiInputConsumer(
    taskSpec: StageTaskSpec,
    lhsDescriptors: tuple[ExchangePartitionDescriptor, ...],
    rhsDescriptors: tuple[ExchangePartitionDescriptor, ...],
    outputPlaceholders=(),
    publishConfig: ExchangePublishConfig | None = None,
) -> RemoteLeafResult:
    _configure_bolt_logs_from_env()
    _register_udfs_from_task_spec(taskSpec)
    boltmlDebugLog(
        "remote-worker",
        f"execute multi-input consumer stage={taskSpec.stageId} substraitBytes={len(taskSpec.substraitPlanBytes) if taskSpec.substraitPlanBytes else 0}",
    )
    if boltmlDebugEnabled():
        boltmlDebugLog(
            "subst-audit",
            f"multi-input consumer stage={taskSpec.stageId}"
            f" lhsSrc={[d.stageId + '/' + d.exchangeId for d in lhsDescriptors]}"
            f" rhsSrc={[d.stageId + '/' + d.exchangeId for d in rhsDescriptors]}"
            f" placeholders={[(p.sourceStageId, p.exchangeId, p.outputNames[:3]) for p in taskSpec.inputPlaceholders]}",
        )
    import time as _t

    dataframe = _apply_remote_multi_input_ops(
        _load_exchange_inputs(lhsDescriptors),
        _load_exchange_inputs(rhsDescriptors),
        taskSpec,
    )
    _pub_start = _t.monotonic_ns()
    publishedDescriptors = _publish_remote_outputs(
        dataframe, outputPlaceholders, publishConfig, taskSpec
    )
    _pub_end = _t.monotonic_ns()
    # H4 cache: convert to arrow once, reuse for both `table` and `outputNames`.
    # ``_arrow_table_with_bolt_types`` tags the Arrow schema metadata with each
    # column's original Bolt type for reconstruction on the driver.
    arrow_table = _arrow_table_with_bolt_types(dataframe)
    table = None if publishedDescriptors else arrow_table
    runtimeContext = ray.get_runtime_context()
    return RemoteLeafResult(
        table=table,
        outputNames=tuple(arrow_table.column_names),
        workerId=runtimeContext.get_worker_id(),
        nodeId=runtimeContext.get_node_id(),
        publishedDescriptors=publishedDescriptors,
        publishStartNs=_pub_start if publishedDescriptors else None,
        publishEndNs=_pub_end if publishedDescriptors else None,
    )


@ray.remote
class RemoteExecutionActor:
    def executeLeafTask(
        self,
        taskSpec: StageTaskSpec,
        outputPlaceholders=(),
        publishConfig: ExchangePublishConfig | None = None,
    ) -> RemoteLeafResult:
        return executeRemoteLeafTask._function(
            taskSpec, outputPlaceholders, publishConfig
        )

    def executeSingleInputConsumer(
        self,
        taskSpec: StageTaskSpec,
        inputDescriptors: tuple[ExchangePartitionDescriptor, ...],
        outputPlaceholders=(),
        publishConfig: ExchangePublishConfig | None = None,
    ) -> RemoteLeafResult:
        return executeRemoteSingleInputConsumer._function(
            taskSpec, inputDescriptors, outputPlaceholders, publishConfig
        )

    def executeMultiInputConsumer(
        self,
        taskSpec: StageTaskSpec,
        lhsDescriptors: tuple[ExchangePartitionDescriptor, ...],
        rhsDescriptors: tuple[ExchangePartitionDescriptor, ...],
        outputPlaceholders=(),
        publishConfig: ExchangePublishConfig | None = None,
    ) -> RemoteLeafResult:
        return executeRemoteMultiInputConsumer._function(
            taskSpec, lhsDescriptors, rhsDescriptors, outputPlaceholders, publishConfig
        )
