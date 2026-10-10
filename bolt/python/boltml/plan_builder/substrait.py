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

from typing import Optional

from substrait.proto import algebra, plan

from pybolt import (
    BoltType,
    FileFormat,
    JoinType,
    RowType,
    RowVector,
    SortOrder,
    fromList,
    rowVector,
    convertSubstraitPlan,
)
from pybolt import PlanBuilder as PyboltPlanBuilder

try:
    from pybolt import makePaimonExtensionTable
except ImportError:
    makePaimonExtensionTable = None

from ..substrait.extensions import extensionRegister
from ..substrait.rel import (
    aggregateRel,
    fetchRel,
    filterRel,
    joinRel,
    localFilesRel,
    pythonRel,
    projectRel,
    tpchExtensionTableRel,
    writeRel,
    shuffleRel,
    sortRel,
    virtualTable,
)
from ..expression.field import FieldExpression
from .base import LogicalPlanBuilder, LogicalPlanBuilderFactory, PlanBuilder
from .base import LogicalOp, LogicalPlan, PlanSource, renderLogicalPlan
from .bolt import BoltPlanBuilder, BoltPlanBuilderFactory


# ---------------------------------------------------------------------------
# Engine-level routing helpers
# ---------------------------------------------------------------------------

# Logical-plan source kinds that ``SubstraitPlanBuilder.toSubstraitPlan``
# cannot represent in Substrait today. Mirrors the explicit dispatch in
# ``SubstraitPlanBuilderFactory.fromLogicalPlan`` (any ``source.kind`` not
# in the supported set there raises ``NotImplementedError`` and is therefore
# non-lowerable). Kept as a module-level constant rather than re-derived
# from the dispatch so that the engine-level routing is a cheap, pure
# predicate (no factory construction, no exception catching).
#
# Currently empty: every BoltML logical source kind is Substrait-
# lowerable. ``tpch_read`` lowers to a Substrait ``ReadRel`` carrying a
# ``TpchExtensionTable`` payload (handled C++-side in
# ``SubstraitToBoltPlan.cpp`` via
# ``readRel.extension_table().detail().Is<TpchExtensionTable>()``).
_NON_SUBSTRAIT_LOWERABLE_SOURCES: frozenset[str] = frozenset()


# Spill-cleanup state for ``SubstraitPlanBuilderFactory._fromDataViaParquetSpill``.
#
# ``fromData`` writes the spill files to ``$TMPDIR/boltml_virtual_table_spill/``
# (process-shared). Without explicit cleanup these accumulate forever, since
# the process holds no other reference once the substrait plan referencing the
# file completes. We register a single ``atexit`` hook the first time a spill
# is requested in this process; the hook removes the whole spill root on
# graceful shutdown. ``ignore_errors=True`` keeps the hook safe to invoke
# more than once and survives partial cleanup by other processes sharing the
# same ``TMPDIR`` (each spill file has a uuid-suffixed name so cross-process
# collisions are negligible).
#
# This handles the GRACEFUL exit case. Hard kill (SIGKILL, OOM-killer, container
# stop) bypasses atexit; orphaned spills from those events accumulate until the
# tmpfs is GC'd by the OS or a startup sweep removes them. Both Ray exchange
# tmpdirs and these spill dirs share the same fragility — see RayRuntime's
# stale-tempdir sweep at process start for the recovery-time mechanism.
_SPILL_CLEANUP_REGISTERED: bool = False


def _virtual_table_spill_root() -> str:
    """Return the per-process spill directory, registering an ``atexit``
    cleanup hook on first call. Idempotent.

    Spill location precedence:

    1. ``BOLTML_VIRTUAL_TABLE_SPILL_ROOT`` (explicit override).
    2. ``BOLTML_EXCHANGE_ROOT`` (the same shared-filesystem env var
       the Ray exchange code uses). When the operator has declared a
       shared mount for exchange transport, it makes sense to reuse
       it for the virtual-table spill so Ray workers on other nodes
       can also read the parquet via ``file://``.
    3. ``tempfile.gettempdir()`` (the historical default). This is
       only safe in single-node deployments — on a multi-node Ray
       cluster the worker reading the ``file://`` URI would land on
       a different node from the driver that wrote it.

    The runtime detects multi-node + no shared root in
    ``RayRuntime.runStageDag`` and switches the *exchange* transport
    to ``object``; the same gate ought to apply here, but virtual
    tables are constructed at plan-build time (before any Ray
    cluster check). Operators in multi-node deployments must set
    ``BOLTML_EXCHANGE_ROOT`` (or
    ``BOLTML_VIRTUAL_TABLE_SPILL_ROOT``) to a shared mount; without
    that, large ``fromData()`` inputs degrade to driver-local
    ``file://`` paths that remote leaf workers can't read.
    A warning is emitted on first spill in multi-node mode without a
    shared root so the misconfiguration is visible.
    """
    import atexit
    import logging
    import os
    import shutil
    import tempfile

    global _SPILL_CLEANUP_REGISTERED
    explicit = os.environ.get("BOLTML_VIRTUAL_TABLE_SPILL_ROOT", "").strip()
    exchange_root = os.environ.get("BOLTML_EXCHANGE_ROOT", "").strip()
    if explicit:
        root = os.path.join(explicit, "boltml_virtual_table_spill")
    elif exchange_root:
        root = os.path.join(exchange_root, "boltml_virtual_table_spill")
    else:
        root = os.path.join(tempfile.gettempdir(), "boltml_virtual_table_spill")
        # Best-effort: if Ray is initialized and we can see >1 node,
        # the operator is using a multi-node cluster but hasn't
        # declared a shared root. Surface the misconfiguration once
        # — silently spilling to /tmp gives remote-leaf workers
        # FileNotFoundError on a path that exists on the driver but
        # not on them. The warning fires per-process, not per-call.
        global _SPILL_MULTINODE_WARNED
        if not _SPILL_MULTINODE_WARNED:
            try:
                import ray

                if ray.is_initialized():
                    alive = sum(1 for n in ray.nodes() if n.get("Alive"))
                    if alive > 1:
                        logging.getLogger(__name__).warning(
                            "boltml: spilling large fromData() to %s on a "
                            "multi-node Ray cluster without a declared "
                            "shared filesystem. Remote-leaf workers on "
                            "other nodes cannot read this path. Set "
                            "BOLTML_EXCHANGE_ROOT (or "
                            "BOLTML_VIRTUAL_TABLE_SPILL_ROOT) to a "
                            "path mounted on every Ray node, or keep "
                            "large fromData() inputs on the driver only.",
                            root,
                        )
                        _SPILL_MULTINODE_WARNED = True
            except Exception:  # noqa: BLE001
                pass
    os.makedirs(root, exist_ok=True)
    if not _SPILL_CLEANUP_REGISTERED:
        atexit.register(shutil.rmtree, root, ignore_errors=True)
        _SPILL_CLEANUP_REGISTERED = True
    return root


_SPILL_MULTINODE_WARNED: bool = False


def plan_is_substrait_lowerable(logicalPlan: LogicalPlan) -> bool:
    """Return True iff *logicalPlan* (and every join right-hand side) can be
    lowered to a Substrait ``plan.Plan`` by
    ``SubstraitPlanBuilderFactory.fromLogicalPlan(...).toSubstraitPlan()``.

    ``_NON_SUBSTRAIT_LOWERABLE_SOURCES`` is currently empty, so this
    predicate returns True for every BoltML logical plan whose ops
    chain stays inside the supported op set. The constant + this
    predicate are preserved as the registry's zero-value rather than
    deleted: future non-lowerable source kinds (if any) plug in here
    without changing call sites.

    Recurses into the right-hand side of every ``join`` op (left-hand
    side is the chain we are walking). A plan with no ops is
    lowerable iff its own source kind is supported.
    """
    if logicalPlan.source.kind in _NON_SUBSTRAIT_LOWERABLE_SOURCES:
        return False
    for op in logicalPlan.operations:
        if op.kind == "join":
            rhs_plan = op.args[0]
            if not plan_is_substrait_lowerable(rhs_plan):
                return False
    return True


class SubstraitPlanBuilder(LogicalPlanBuilder):
    def __init__(
        self,
        rel: Optional[algebra.Rel],
        fieldNames: list[str],
        source: PlanSource,
        factory: "SubstraitPlanBuilderFactory",
        operations: list[LogicalOp] | None = None,
    ):
        # Normalize a "lying" single-file PlanSource against a multi-file
        # Read Rel.
        #
        # Background: callers (notably the TPC-H harness ``_table``) used to
        # construct a SubstraitPlanBuilder with a multi-shard ReadRel that
        # carried every parquet path AND a PlanSource labelled
        # ``single_file_read`` with only the *first* URI. The lie was
        # invisible until ``.copy()`` triggered ``cloneWithLogicalPlan``,
        # which rebuilds the plan from the PlanSource — dropping 7/8 shards
        # and silently undercounting every aggregate by exactly the shard
        # ratio. TPC-H Q11 (HAVING subquery on a copied de_ps) and Q21/Q22
        # (anti-join via ``.copy()``) were all victims.
        #
        # Fix lives here, not in the caller, because:
        # - the Rel is the authoritative description of what's actually
        #   read; the PlanSource is a lossy summary used for clone-replay.
        # - normalising at the leaf construction means every downstream
        #   operator (``.filter``, ``.join``, ``.transform``) inherits the
        #   correct source without needing to know about this concern.
        # - applies in every config (local, hybrid, distributed; any value
        #   of ``BOLTML_LEAF_PARALLELISM``; single-file or multi-file)
        #   because we operate on the plan structure, not on execution mode.
        if (
            rel is not None
            and rel.HasField("read")
            and rel.read.HasField("local_files")
            and source is not None
            and source.kind == "single_file_read"
            and len(rel.read.local_files.items) > 1
        ):
            allUris = tuple(item.uri_file for item in rel.read.local_files.items)
            # source.args for single_file_read is ``(filePath, outputType,
            # fileFormat)``; for local_files_read it's
            # ``(filePathsTuple, outputType, fileFormat)``. Reuse the
            # outputType + fileFormat from the existing source.
            if len(source.args) == 3:
                _, outputType, fileFormat = source.args
                source = PlanSource(
                    "local_files_read", (allUris, outputType, fileFormat)
                )
        self.__rel = rel
        self.__fieldNames = fieldNames
        self.__source = source
        self.__factory = factory
        self.__operations = operations or []
        # Cached output of ``boltPlan()`` plus the (rel-id, op-count)
        # fingerprint of the builder state that produced it. Every
        # mutator reassigns ``__rel`` to a fresh ``algebra.Rel`` and
        # appends to ``__operations``, so ``(id(__rel), len(__operations))``
        # is a sufficient invalidator: when the fingerprint changes,
        # the cache is stale. The cache is held inside the
        # ``BoltPlanBuilder`` itself, which keeps the source ``__rel``
        # reachable through ``self.__rel`` for the duration of the
        # cached entry — so id reuse via GC is not a risk.
        self.__cached_bolt_plan: Optional[BoltPlanBuilder] = None
        self.__cached_bolt_plan_key: Optional[tuple[int, int]] = None

    def boltPlan(self) -> BoltPlanBuilder:
        cache_key = (id(self.__rel), len(self.__operations))
        if (
            self.__cached_bolt_plan is not None
            and self.__cached_bolt_plan_key == cache_key
        ):
            return self.__cached_bolt_plan
        substraitPlan = self.toSubstraitPlan()
        if substraitPlan is None:
            result = BoltPlanBuilderFactory().empty()
        else:
            planNode, splits = convertSubstraitPlan(substraitPlan.SerializeToString())
            result = BoltPlanBuilder(
                PyboltPlanBuilder(planNode),
                splits,
                self.__source,
                BoltPlanBuilderFactory(),
                list(self.__operations),
            )
        self.__cached_bolt_plan = result
        self.__cached_bolt_plan_key = cache_key
        return result

    def dtype(self) -> RowType:
        return self.boltPlan().dtype()

    def logicalText(self) -> str:
        return renderLogicalPlan(self.logicalPlan())

    def logicalPlan(self) -> LogicalPlan:
        return LogicalPlan(self.__source, tuple(self.__operations))

    def cloneWithLogicalPlan(self, logicalPlan: LogicalPlan) -> "SubstraitPlanBuilder":
        return self.__factory.fromLogicalPlan(logicalPlan)

    def substraitPlan(self) -> "SubstraitPlanBuilder":
        # Already Substrait-backed.
        return self

    def optimize(self) -> "SubstraitPlanBuilder":
        """Run the logical Substrait optimizer; return a new plain
        ``SubstraitPlanBuilder`` over the optimized plan.

        The original logical source/operations are preserved on the result so
        ``explain`` (un-optimized "Logical Plan" view) and clone-replay stay
        correct, and the result is a *plain* builder so its own ``optimize``
        is the only place the optimizer runs (no accidental double-optimize on
        the materialisation path).
        """
        from ..optimizer import SubstraitOptimizerPipeline

        loweredPlan = self.toSubstraitPlan()
        if loweredPlan is None:
            return SubstraitPlanBuilderFactory().empty()
        optimizedPlan = SubstraitOptimizerPipeline().optimize(loweredPlan)
        root = optimizedPlan.relations[0].root
        logical = self.logicalPlan()
        return SubstraitPlanBuilder(
            root.input,
            list(root.names),
            logical.source,
            SubstraitPlanBuilderFactory(),
            list(logical.operations),
        )

    @property
    def rel(self) -> Optional[algebra.Rel]:
        return self.__rel

    @property
    def fieldNames(self) -> list[str]:
        return list(self.__fieldNames)

    def toSubstraitPlan(self) -> Optional[plan.Plan]:
        """Wrap the cached ``__rel`` in a complete Substrait ``Plan`` proto.

        Wraps the current root ``Rel`` in ``RelRoot`` / ``PlanRel`` / ``Plan``
        and populates ``extensions`` with the registered function/type
        anchors so the C++ engine can resolve every ``function_reference``
        in the tree. Returns ``None`` for an empty builder (no rel) so
        callers can short-circuit to an empty plan.

        We use ``extensionRegister.getAll()`` (not ``find(__rel)``) because
        ``find`` walks ``ListFields`` recursively but does not unpack
        ``google.protobuf.Any`` payloads — anchors buried inside an
        Any-packed extension detail (e.g. an ``EmbeddedFunction``
        reference) would silently be omitted. Unused declarations are
        harmless: Substrait consumers index by anchor and ignore
        unreferenced ones.
        """
        if self.__rel is None:
            return None
        rootRel = algebra.RelRoot(input=self.__rel, names=list(self.__fieldNames))
        planRel = plan.PlanRel(root=rootRel)
        return plan.Plan(relations=[planRel], extensions=extensionRegister.getAll())

    def serializeToBytes(self) -> bytes:
        substraitPlan = self.toSubstraitPlan()
        if substraitPlan is None:
            raise ValueError("SubstraitPlanBuilder has no rel to serialize.")
        return substraitPlan.SerializeToString()

    def project(
        self,
        expressions: list["NamedExpression"],  # noqa: F821
    ) -> "Self":  # noqa: F821
        rel = algebra.Rel(project=projectRel(self.__rel, expressions))
        self.__rel = rel
        self.__fieldNames = [e.name for e in expressions]
        self.__operations.append(LogicalOp("project", (tuple(expressions),)))
        return self

    def aggregate(
        self,
        keys: list["FieldExpression"],  # noqa: F821
        expressions: list["NamedExpression"],  # noqa: F821
    ) -> "Self":  # noqa: F821
        rel = algebra.Rel(aggregate=aggregateRel(self.__rel, keys, expressions))
        self.__rel = rel
        self.__fieldNames = [k.name for k in keys] + [e.name for e in expressions]
        self.__operations.append(
            LogicalOp(
                "aggregate",
                (tuple(k.name for k in keys), tuple(expressions)),
            )
        )
        return self

    def limit(self, count: int, offset: int) -> "Self":  # noqa: F821
        rel = algebra.Rel(fetch=fetchRel(self.__rel, count, offset))
        self.__rel = rel
        self.__operations.append(LogicalOp("limit", (count, offset)))
        return self

    def orderBy(
        self,
        order: list[tuple["FieldExpression", SortOrder]],  # noqa: F821
    ) -> "Self":  # noqa: F821
        rel = algebra.Rel(sort=sortRel(self.__rel, order))
        self.__rel = rel
        self.__operations.append(
            LogicalOp(
                "order_by",
                (tuple((k.name if hasattr(k, "name") else k, v) for k, v in order),),
            )
        )
        return self

    def shuffle(self, seed: int) -> "Self":  # noqa: F821
        if self.__rel is None:
            raise ValueError("Shuffle requires an input relation")

        ext_rel = shuffleRel(self.__rel, seed)
        self.__rel = algebra.Rel(extension_single=ext_rel)
        self.__operations.append(LogicalOp("shuffle", (seed,)))
        return self

    def filter(self, expression: "BooleanExpression") -> "Self":  # noqa: F821
        rel = algebra.Rel(filter=filterRel(self.__rel, expression))
        self.__rel = rel
        self.__operations.append(LogicalOp("filter", (expression,)))
        return self

    def join(
        self,
        rhs: "Self",  # noqa: F821
        lhsKeys: list["FieldExpression"],  # noqa: F821
        rhsKeys: list["FieldExpression"],  # noqa: F821
        outputLayout: list[str],
        joinType: JoinType,
    ) -> "Self":  # noqa F821
        leftOutputNames = tuple(self.dtype().names())
        rightOutputNames = tuple(rhs.dtype().names())
        rel = algebra.Rel(
            hash_join=joinRel(
                self.__rel, rhs.__rel, lhsKeys, rhsKeys, outputLayout, joinType
            )
        )
        self.__rel = rel
        self.__fieldNames = outputLayout
        self.__operations.append(
            LogicalOp(
                "join",
                (
                    rhs.logicalPlan(),
                    tuple(k.name if hasattr(k, "name") else k for k in lhsKeys),
                    tuple(k.name if hasattr(k, "name") else k for k in rhsKeys),
                    tuple(outputLayout),
                    joinType,
                    leftOutputNames,
                    rightOutputNames,
                ),
            )
        )
        return self

    def python(
        self, outputType: BoltType, function: callable, *args, **kwargs
    ) -> "Self":  # noqa: F821
        if self.__rel is None:
            raise ValueError("Python extension requires an input relation")

        ext_rel = pythonRel(self.__rel, function, outputType, *args, **kwargs)
        self.__rel = algebra.Rel(extension_single=ext_rel)
        self.__fieldNames = outputType.names()
        self.__operations.append(
            LogicalOp("python", (outputType, function, args, tuple(kwargs.items())))
        )
        return self

    def catalogWrite(
        self,
        directoryPath: str,
        fileFormat: FileFormat,
        partitionBy: list[str],
        numBuckets: int,
        bucketBy: list[str],
        aggregations: list[str],
    ) -> "Self":  # noqa: F821
        if self.__rel is None:
            raise ValueError("catalogWrite requires an input relation")

        wrel = writeRel(
            self.__rel, directoryPath, fileFormat, partitionBy, numBuckets, bucketBy
        )
        self.__rel = algebra.Rel(write=wrel)
        self.__operations.append(
            LogicalOp(
                "catalog_write",
                (
                    directoryPath,
                    fileFormat,
                    tuple(partitionBy),
                    numBuckets,
                    tuple(bucketBy),
                    tuple(aggregations),
                ),
            )
        )
        return self


class SubstraitPlanBuilderFactory(LogicalPlanBuilderFactory):
    def fromLogicalPlan(self, logicalPlan: LogicalPlan) -> SubstraitPlanBuilder:
        class _DTypeHolder:
            def __init__(self, dtype: RowType):
                self.dtype = dtype

        def _fields(builder: PlanBuilder, names: list[str]) -> list[FieldExpression]:
            holder = _DTypeHolder(builder.dtype())
            return [FieldExpression(name, holder) for name in names]

        source = logicalPlan.source
        if source.kind == "empty":
            builder = self.empty()
        elif source.kind == "values":
            builder = self.fromData(source.args[0])
        elif source.kind == "single_file_read":
            builder = self.fromSingleFileRead(*source.args)
        elif source.kind == "local_files_read":
            filePaths, outputType, fileFormat = source.args
            builder = self.fromLocalFiles(list(filePaths), outputType, fileFormat)
        elif source.kind == "catalog_read":
            builder = self.fromCatalog(*source.args)
        elif source.kind == "tpch_read":
            # ``source.args`` is built by ``boltml.tpch.TpchTable.read`` as
            # ``(table_enum, columns, scaleFactor, numParts)`` where
            # ``table_enum`` is the ``pybolt.TpchTable`` instance — its
            # ``.name`` is the canonical table name accepted by
            # ``makeTpchExtensionTable`` (and matched in C++ via
            # ``tpch::fromTableName``). ``BoltPlanBuilderFactory`` accepts
            # the prefixed column names because ``tpchTableScan`` strips
            # the prefix internally; the Substrait extension expects the
            # canonical TPC-H column names ("nationkey" / "name" / etc.),
            # which are what ``boltml.tpch.<TableClass>.colNames`` carries.
            tableEnum, columnNames, scaleFactor, numParts = source.args
            tableName = tableEnum.name if hasattr(tableEnum, "name") else str(tableEnum)
            builder = self.fromTpchRead(
                tableName, list(columnNames), scaleFactor, numParts
            )
        else:
            raise NotImplementedError(f"Unsupported logical plan source: {source.kind}")

        for op in logicalPlan.operations:
            if op.kind == "project":
                builder.project(list(op.args[0]))
            elif op.kind == "aggregate":
                builder.aggregate(_fields(builder, list(op.args[0])), list(op.args[1]))
            elif op.kind == "limit":
                builder.limit(op.args[0], op.args[1])
            elif op.kind == "order_by":
                builder.orderBy(
                    [
                        (field, order)
                        for field, (_, order) in zip(
                            _fields(builder, [name for name, _ in op.args[0]]),
                            op.args[0],
                        )
                    ]
                )
            elif op.kind == "shuffle":
                builder.shuffle(op.args[0])
            elif op.kind == "filter":
                builder.filter(op.args[0])
            elif op.kind == "join":
                rhs = self.fromLogicalPlan(op.args[0])
                builder.join(
                    rhs,
                    _fields(builder, list(op.args[1])),
                    _fields(rhs, list(op.args[2])),
                    list(op.args[3]),
                    op.args[4],
                )
            elif op.kind == "python":
                builder.python(op.args[0], op.args[1], *op.args[2], **dict(op.args[3]))
            elif op.kind == "catalog_write":
                builder.catalogWrite(
                    op.args[0],
                    op.args[1],
                    list(op.args[2]),
                    op.args[3],
                    list(op.args[4]),
                    list(op.args[5]),
                )
            else:
                raise NotImplementedError(f"Unsupported logical op: {op.kind}")
        return builder

    def empty(self) -> PlanBuilder:
        return SubstraitPlanBuilder(None, [], PlanSource("empty", ()), self, [])

    # Threshold above which ``fromData`` spills the RowVector to a temp
    # parquet and routes through ``fromLocalFiles`` instead of building a
    # Substrait ``virtual_table`` literal. The virtualTable encoder is
    # O(rows * columns) in pure-Python proto manipulation
    # (see substrait/rel.py:virtualTable -- per-row substraitLiteral call
    # plus per-cell fromList/Expression construction); past a few thousand
    # rows it becomes minutes-slow and looks like a hang. The parquet
    # round-trip preserves byte-identical semantics (the data is
    # materialized once either way) but moves the per-row cost to
    # pyarrow's vectorized writer + Bolt's C++ parquet reader.
    #
    # Override via ``BOLTML_VIRTUAL_TABLE_MAX_ROWS`` env var.
    _DEFAULT_VIRTUAL_TABLE_MAX_ROWS: int = 1000

    def fromData(self, data: RowVector) -> PlanBuilder:
        import os

        try:
            max_rows = int(
                os.environ.get(
                    "BOLTML_VIRTUAL_TABLE_MAX_ROWS",
                    str(self._DEFAULT_VIRTUAL_TABLE_MAX_ROWS),
                )
            )
        except ValueError:
            max_rows = self._DEFAULT_VIRTUAL_TABLE_MAX_ROWS

        if len(data) > max_rows:
            return self._fromDataViaParquetSpill(data)

        rel = virtualTable(data)
        if not isinstance(rel, algebra.Rel):
            rel = algebra.Rel(read=rel)
        return SubstraitPlanBuilder(
            rel,
            data.dtype().names(),
            PlanSource("values", (data,)),
            self,
            [],
        )

    def _fromDataViaParquetSpill(self, data: RowVector) -> PlanBuilder:
        """Spill ``data`` to a temp parquet and return a PlanBuilder
        backed by ``fromLocalFiles``. Used by ``fromData`` to avoid the
        O(rows*cols) Python proto cost of the virtualTable encoder for
        large RowVectors (TPC-H q15's 50K-row revenue subquery was the
        canonical victim — it took >5 min to lower a virtual_table that
        the parquet path materializes in <1 sec).

        Each call writes a fresh ``persist_<uuid>.parquet`` so
        independent invocations don't collide. The ``PlanSource`` is
        ``local_files_read`` carrying the file path, so cloning via
        ``cloneWithLogicalPlan`` correctly reconstructs the read.
        """
        import os
        import pyarrow as pa
        import pyarrow.parquet as pq
        import uuid
        from pybolt import exportToArrow

        tmp_root = _virtual_table_spill_root()
        path = os.path.join(tmp_root, f"vt_{uuid.uuid4().hex}.parquet")

        # Materialize the RowVector to an Arrow table and write parquet.
        arrow_struct = exportToArrow(data)
        if isinstance(arrow_struct, pa.StructArray):
            arrow_table = pa.Table.from_struct_array(arrow_struct)
        else:
            arrow_table = arrow_struct
        # Atomic write so a concurrent reader never sees a partial buffer.
        tmp_path = path + ".partial"
        pq.write_table(arrow_table, tmp_path)
        os.replace(tmp_path, path)

        return self.fromLocalFiles(
            [f"file://{path}"],
            data.dtype(),
            FileFormat.PARQUET,
        )

    def fromTpchRead(
        self,
        tableName: str,
        columnNames: list[str],
        scaleFactor: float,
        numSplits: int,
    ) -> PlanBuilder:
        """Build a SubstraitPlanBuilder over a TPC-H ExtensionTable read.

        Lets the Substrait pipeline lower TPC-H scans end-to-end. The
        resulting ``ReadRel.extension_table.detail`` is a
        ``substrait.bolt.TpchExtensionTable`` Any payload, which
        ``SubstraitToBoltPlan.cpp`` recognizes and converts into the
        TPC-H connector splits + table handle. ``columnNames`` are the
        canonical (unprefixed) TPC-H column names — see
        ``boltml.tpch.<TableClass>.colNames``.
        """
        return SubstraitPlanBuilder(
            algebra.Rel(
                read=tpchExtensionTableRel(
                    tableName, columnNames, scaleFactor, numSplits
                )
            ),
            list(columnNames),
            PlanSource(
                "tpch_read",
                (tableName, tuple(columnNames), scaleFactor, numSplits),
            ),
            self,
            [],
        )

    def tpchGenerator(
        self,
        table: "TpchTable",  # noqa: F821
        scaleFactor: int = 1,
        numSplits: int = 1,
    ) -> PlanBuilder:
        """Mirror ``BoltPlanBuilderFactory.tpchGenerator`` — produce a
        Substrait-pipeline-ready builder over a TPC-H table.

        Accepts the same ``pybolt.TpchTable`` enum the legacy factory
        does. Routes through ``fromTpchRead`` which builds the
        ``TpchExtensionTable`` ReadRel.
        """
        tableName = table.name if hasattr(table, "name") else str(table)
        columnNames = list(getattr(table, "colNames", []))
        if not columnNames:
            raise ValueError(f"tpchGenerator: TpchTable {tableName!r} has no colNames")
        return self.fromTpchRead(tableName, columnNames, scaleFactor, numSplits)

    def fromSingleFileRead(
        self,
        filePath: str,
        outputType: RowType,
        fileFormat: FileFormat,
    ) -> PlanBuilder:
        return SubstraitPlanBuilder(
            algebra.Rel(read=localFilesRel([filePath], outputType, fileFormat)),
            outputType.names(),
            PlanSource("single_file_read", (filePath, outputType, fileFormat)),
            self,
            [],
        )

    def fromLocalFiles(
        self,
        filePaths: "list[str]",
        outputType: RowType,
        fileFormat: FileFormat,
    ) -> PlanBuilder:
        """Build a PlanBuilder that reads from a list of local files.

        Multi-file analogue of ``fromSingleFileRead``. The PlanSource
        carries the FULL ``filePaths`` tuple so that cloning via
        ``cloneWithLogicalPlan`` reconstructs the same multi-file
        ReadRel — without this, callers that needed multi-file reads
        had to hand-construct ``SubstraitPlanBuilder`` with the
        ``local_files`` ReadRel + a PlanSource lying about being
        ``single_file_read``, which silently dropped all but the first
        URI on every clone (TPC-H q21 was the canonical victim — its
        ``has_other.copy()`` cloned the upstream lineitem read but
        only saw ``part-0000.parquet`` instead of all 16 shards,
        under-counting the anti-join by ~15/16).
        """
        return SubstraitPlanBuilder(
            algebra.Rel(read=localFilesRel(list(filePaths), outputType, fileFormat)),
            outputType.names(),
            PlanSource("local_files_read", (tuple(filePaths), outputType, fileFormat)),
            self,
            [],
        )

    def fromCatalog(
        self,
        tableName: str,
        outputType: RowType,
        partitionKeys: list[str],
        parameters: dict[str, str],
        splits: list["CatalogSplit"],  # noqa: F821
    ) -> PlanBuilder:
        if len(splits) == 0:
            data = rowVector(
                outputType.names(),
                [fromList([], outputType.childAt(i)) for i in range(outputType.size())],
            )
            return self.fromData(data)

        from ..catalog.hive import HiveSplit
        from ..catalog.paimon import PaimonSplit

        # Ensure all splits are of a supported and consistent type.
        if not all((isinstance(s, (HiveSplit, PaimonSplit)) for s in splits)):
            raise NotImplementedError(
                "Substrait plan does not support non-hive splits."
            )

        fileFormat = splits[0].fileFormat
        if not all((s.fileFormat == fileFormat for s in splits)):
            raise ValueError(
                "Substrait plan does not support heterogeneous file formats"
            )

        # Hive splits: build LocalFiles ReadRel.
        if isinstance(splits[0], HiveSplit):
            filePaths = [s.filePath for s in splits]
            return SubstraitPlanBuilder(
                algebra.Rel(read=localFilesRel(filePaths, outputType, fileFormat)),
                outputType.names(),
                PlanSource(
                    "catalog_read",
                    (tableName, outputType, partitionKeys, parameters, tuple(splits)),
                ),
                self,
                [],
            )

        # Paimon splits: use bound makePaimonExtensionTable
        if isinstance(splits[0], PaimonSplit):
            if makePaimonExtensionTable is None:
                raise NotImplementedError(
                    "Substrait plan does not support Paimon catalog reads in this runtime."
                )
            boltSplits = [s.toBoltSplit() for s in splits]
            read_bytes = makePaimonExtensionTable(
                outputType,
                fileFormat,
                boltSplits,
                parameters,
            )
            read_rel = algebra.ReadRel()
            read_rel.ParseFromString(read_bytes)
            return SubstraitPlanBuilder(
                algebra.Rel(read=read_rel),
                outputType.names(),
                PlanSource(
                    "catalog_read",
                    (tableName, outputType, partitionKeys, parameters, tuple(splits)),
                ),
                self,
                [],
            )

        raise NotImplementedError(
            f"Substrait plan does not support catalog split type {type(splits[0])}."
        )
