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

from typing import Callable
from random import getrandbits

from pybolt import PlanBuilder as BoltPlan
from pybolt import (
    BoltType,
    FileFormat,
    JoinType,
    RowType,
    RowVector,
    SortOrder,
)
from pybolt import Split as BoltSplit

from ..tpch import TpchTable

from .base import LogicalPlanBuilder, LogicalPlanBuilderFactory, PlanBuilder
from .base import LogicalOp, LogicalPlan, PlanSource, renderLogicalPlan


class BoltPlanBuilder(LogicalPlanBuilder):
    def __init__(
        self,
        plan: BoltPlan,
        splits: dict[int, list[BoltSplit]],
        source: PlanSource,
        factory: "BoltPlanBuilderFactory",
        operations: list[LogicalOp] | None = None,
    ):
        if not isinstance(plan, BoltPlan):
            raise TypeError(f"Invalid bolt plan type: {type(plan)}")
        self.__pb = plan
        self.__splits = splits
        self.__source = source
        self.__factory = factory
        self.__operations = operations or []

    def plan(self) -> BoltPlan:
        return self.__pb

    def splits(self) -> dict[int, list[BoltSplit]]:
        return self.__splits

    def boltPlan(self) -> "BoltPlanBuilder":
        return self

    def explain(self, executor=None, **pipelineOverrides) -> str:
        # BoltPlanBuilder is the direct-local-execution shortcut and does not
        # own Substrait attributes. A faithful explain would need to lower the
        # Bolt plan back to Substrait in C++ (not available); explain is only
        # offered on the Substrait / Optimized builders. (#79)
        raise NotImplementedError(
            "explain() is not available on BoltPlanBuilder. Use the default "
            "(Substrait/Optimized) plan builder to explain a plan."
        )

    def dtype(self) -> RowType:
        return self.__pb.planNode().outputType()

    def logicalText(self) -> str:
        return renderLogicalPlan(self.logicalPlan())

    def logicalPlan(self) -> LogicalPlan:
        return LogicalPlan(self.__source, tuple(self.__operations))

    def cloneWithLogicalPlan(self, logicalPlan: LogicalPlan) -> "BoltPlanBuilder":
        return self.__factory.fromLogicalPlan(logicalPlan)

    def toSubstraitPlan(self):
        """Lower the captured ``LogicalPlan`` to a Substrait ``Plan`` proto.

        ``BoltPlanBuilder`` doesn't carry an in-flight Substrait ``Rel`` of
        its own — operations were applied directly to the C++ ``BoltPlan``.
        We replay the captured ``LogicalOp`` log into a fresh
        ``SubstraitPlanBuilder`` to recover the equivalent Substrait tree,
        then ask that builder for its Substrait ``Plan``. This keeps the
        ``toSubstraitPlan()`` interface uniform across both builders so
        downstream code (``boltml.execute.execute`` / optimizer /
        dispatcher) doesn't have to branch on builder type.

        Long-term, when the C++ Substrait converter covers every
        Bolt logical-plan node (currently a few extension paths still
        require this Python-side replay), we can remove this method
        and route Bolt-builder users through the converter directly.
        """
        # Local import to avoid a builder.bolt → builder.substrait cycle
        # at module-import time.
        from .substrait import SubstraitPlanBuilderFactory

        return (
            SubstraitPlanBuilderFactory()
            .fromLogicalPlan(self.logicalPlan())
            .toSubstraitPlan()
        )

    def substraitPlan(self) -> "SubstraitPlanBuilder":  # noqa: F821
        """Lower the captured logical plan to a ``SubstraitPlanBuilder`` by
        replaying the ``LogicalOp`` log — the same recovery as
        ``toSubstraitPlan()`` but returning the builder, not the proto."""
        from .substrait import SubstraitPlanBuilderFactory

        return SubstraitPlanBuilderFactory().fromLogicalPlan(self.logicalPlan())

    def project(
        self,
        expressions: list["NamedExpression"],  # noqa: F821
    ) -> "Self":  # noqa: F821
        self.__pb.project([e.expr() for e in expressions])
        self.__operations.append(LogicalOp("project", (tuple(expressions),)))
        return self

    def aggregate(
        self,
        keys: list["FieldExpression"],  # noqa: F821
        expressions: list["NamedExpression"],  # noqa: F821
    ) -> "Self":  # noqa: F821
        expressionDefs = tuple(expressions)
        expressions = [e.expr() for e in expressions]
        self.__pb.singleAggregation(
            [k.name if hasattr(k, "name") else k for k in keys], expressions, []
        )
        self.__operations.append(
            LogicalOp(
                "aggregate",
                (
                    tuple(k.name if hasattr(k, "name") else k for k in keys),
                    expressionDefs,
                ),
            )
        )
        return self

    def limit(self, count: int, offset: int) -> "Self":  # noqa: F821
        self.__pb.limit(offset, count, False)
        self.__operations.append(LogicalOp("limit", (count, offset)))
        return self

    def orderBy(self, order: list[tuple["FieldExpression", SortOrder]]) -> "Self":  # noqa F821
        exprs = [f"{k.name if hasattr(k, 'name') else k} {v}" for k, v in order]
        self.__pb.orderBy(exprs, False)
        self.__operations.append(
            LogicalOp(
                "order_by",
                (tuple((k.name if hasattr(k, "name") else k, v) for k, v in order),),
            )
        )
        return self

    def shuffle(self, seed: int) -> "Self":  # noqa: F821
        self.__pb.localShuffle(seed)
        self.__operations.append(LogicalOp("shuffle", (seed,)))
        return self

    def filter(self, expression: "BooleanExpression") -> "Self":  # noqa: F821
        self.__pb.filter(expression.expr())
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
        rhs = rhs.boltPlan()
        rightOutputNames = tuple(rhs.dtype().names())
        self.__splits |= rhs.splits()
        self.__pb.hashJoin(
            [k.name if hasattr(k, "name") else k for k in lhsKeys],
            [k.name if hasattr(k, "name") else k for k in rhsKeys],
            rhs.__pb.planNode(),
            filter="",
            outputLayout=outputLayout,
            joinType=joinType,
            nullAware=False,
        )
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
        self,
        outputType: BoltType,
        function: Callable,
        *args,
        **kwargs,
    ) -> "Self":  # noqa: F821
        self.__pb.pythonPlanNode(
            function.__name__, outputType, function, *args, **kwargs
        )
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
        self.__pb.tableWrite(
            directoryPath,
            fileFormat,
            partitionBy,
            numBuckets,
            bucketBy,
            aggregations,
        )
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


class BoltPlanBuilderFactory(LogicalPlanBuilderFactory):
    def __init__(self, nodeIdGen: Callable[[], int] = lambda: getrandbits(30)):
        self.__nodeIdGen = nodeIdGen

    def __newPlanBuilder(self) -> BoltPlan:
        planStartId = self.__nodeIdGen()
        return BoltPlan(planStartId)

    def fromLogicalPlan(self, logicalPlan: LogicalPlan) -> BoltPlanBuilder:
        source = logicalPlan.source
        if source.kind == "empty":
            builder = self.empty().boltPlan()
        elif source.kind == "values":
            builder = self.fromData(source.args[0]).boltPlan()
        elif source.kind == "substrait_plan":
            # A BoltPlanBuilder rooted at an already-lowered Substrait
            # plan (used by ``boltml.execute.execute`` to feed a
            # post-optimizer plan into the in-process ``eval`` path).
            # Re-run ``convertSubstraitPlan`` to rebuild the same C++
            # planNode + splits the original builder wrapped.
            (planBytes,) = source.args
            return self.fromSubstraitPlan(planBytes)
        elif source.kind == "single_file_read":
            builder = self.fromSingleFileRead(*source.args).boltPlan()
        elif source.kind == "local_files_read":
            filePaths, outputType, fileFormat = source.args
            builder = self.fromLocalFiles(
                list(filePaths), outputType, fileFormat
            ).boltPlan()
        elif source.kind == "catalog_read":
            builder = self.fromCatalog(*source.args).boltPlan()
        elif source.kind == "tpch_read":
            tableName, columnNames, scaleFactor, numParts = source.args
            pb = self.__newPlanBuilder().tpchTableScan(
                tableName, list(columnNames), scaleFactor
            )
            splits = {pb.planNode().id(): BoltSplit.tpch(numParts)}
            builder = BoltPlanBuilder(pb, splits, source, self, [])
        else:
            raise ValueError(f"Unsupported logical plan source: {source.kind}")

        for op in logicalPlan.operations:
            if op.kind == "project":
                builder.project(list(op.args[0]))
            elif op.kind == "aggregate":
                builder.aggregate(list(op.args[0]), list(op.args[1]))
            elif op.kind == "limit":
                builder.limit(op.args[0], op.args[1])
            elif op.kind == "order_by":
                builder.orderBy(list(op.args[0]))
            elif op.kind == "shuffle":
                builder.shuffle(op.args[0])
            elif op.kind == "filter":
                builder.filter(op.args[0])
            elif op.kind == "join":
                rhs = self.fromLogicalPlan(op.args[0])
                builder.join(
                    rhs,
                    list(op.args[1]),
                    list(op.args[2]),
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
                raise ValueError(f"Unsupported logical op: {op.kind}")
        return builder

    def empty(self) -> PlanBuilder:
        return BoltPlanBuilder(
            self.__newPlanBuilder(), {}, PlanSource("empty", ()), self, []
        )

    def fromSubstraitPlan(self, planBytes: bytes) -> "BoltPlanBuilder":
        """Build a BoltPlanBuilder rooted at an already-lowered Substrait
        plan.

        Used by ``boltml.execute.execute`` so an executor that exposes
        ``eval(planBuilder)`` (and not ``evalSubstraitPlan``) can still
        run the post-optimizer Substrait plan: the driver converts the
        plan to ``(planNode, splits)`` via the C++ ``convertSubstraitPlan``
        helper, wraps them in a ``BoltPlanBuilder``, and hands that to
        ``executor.eval``. Replaces the bespoke ``evalSubstraitPlan``
        method that LocalExecutor used to ship — the in-process eval
        path is now a single funnel through ``boltPlan() →
        executor.execute(fragment)``.

        The captured ``PlanSource`` keeps the raw bytes so
        ``cloneWithLogicalPlan`` can rebuild an identical builder by
        re-running the conversion. Empty operations log — the Substrait
        plan already encodes everything.
        """
        from pybolt import convertSubstraitPlan

        planNode, splits = convertSubstraitPlan(planBytes)
        pb = BoltPlan(planNode)
        return BoltPlanBuilder(
            pb,
            splits,
            PlanSource("substrait_plan", (planBytes,)),
            self,
            [],
        )

    def fromData(self, data: RowVector) -> PlanBuilder:
        pb = self.__newPlanBuilder()
        pb.values([data])
        return BoltPlanBuilder(pb, {}, PlanSource("values", (data,)), self, [])

    def fromSingleFileRead(
        self,
        filePath: str,
        outputType: RowType,
        fileFormat: FileFormat,
    ) -> PlanBuilder:
        pb = self.__newPlanBuilder()
        pb.tableRead(filePath, outputType, [])
        splits = [BoltSplit.hive(filePath, fileFormat, {})]
        return BoltPlanBuilder(
            pb,
            {pb.planNode().id(): splits},
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
        """Build a BoltPlanBuilder reading from a list of local files.

        Multi-file analogue of ``fromSingleFileRead``. The PlanSource
        carries the FULL ``filePaths`` tuple so cloning preserves all
        URIs (see substrait.py:fromLocalFiles for context).
        """
        pb = self.__newPlanBuilder()
        # tableRead's tableName is just a label here; use the first path
        # as a representative (the actual splits drive the read).
        firstPath = filePaths[0] if filePaths else ""
        pb.tableRead(firstPath, outputType, [])
        splits = [BoltSplit.hive(p, fileFormat, {}) for p in filePaths]
        return BoltPlanBuilder(
            pb,
            {pb.planNode().id(): splits},
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
        pb = self.__newPlanBuilder()
        pb.tableRead(tableName, outputType, partitionKeys, parameters)
        return BoltPlanBuilder(
            pb,
            {pb.planNode().id(): [s.toBoltSplit() for s in splits]},
            PlanSource(
                "catalog_read",
                (tableName, outputType, partitionKeys, parameters, tuple(splits)),
            ),
            self,
            [],
        )

    def tpchGenerator(
        self, table: "TpchTable", scaleFactor: int = 1, numSplits: int = 1
    ) -> PlanBuilder:  # noqa: F821
        """
        Make a plan builder generating its input data from a TPCH generator.
        """
        pb = self.__newPlanBuilder()
        pb.tpchTableScan(table.boltEnum, table.colNames, scaleFactor)
        splits: list[BoltSplit] = BoltSplit.tpch(numSplits)
        return BoltPlanBuilder(
            pb,
            {pb.planNode().id(): splits},
            # Store the boltEnum (not the TpchTable class): the clone path
            # (cloneWithLogicalPlan, source.kind == "tpch_read") feeds
            # ``source.args[0]`` straight into ``tpchTableScan``, which takes
            # the enum, and ``_encode_source`` reads ``.name`` off it (both
            # the enum and the class expose ``.name`` as the canonical lower-
            # case table name, so distributed encoding is unchanged).
            PlanSource(
                "tpch_read", (table.boltEnum, table.colNames, scaleFactor, numSplits)
            ),
            self,
            [],
        )
