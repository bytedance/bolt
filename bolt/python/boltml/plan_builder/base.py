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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable

from pybolt import RowType, RowVector, JoinType, FileFormat, BoltType, SortOrder


@dataclass(frozen=True)
class PlanSource:
    kind: str
    args: tuple[Any, ...]


@dataclass(frozen=True)
class LogicalOp:
    kind: str
    args: tuple[Any, ...]


@dataclass(frozen=True)
class LogicalPlan:
    source: PlanSource
    operations: tuple[LogicalOp, ...]


def _formatLogicalSource(source: PlanSource) -> str:
    if source.kind == "values":
        return "Values"
    if source.kind in ("single_file_read", "catalog_read"):
        return "TableRead"
    if source.kind == "tpch_read":
        return f"TpchTableScan[{source.args[0]}]"
    if source.kind == "empty":
        return "Empty"
    return source.kind


def _formatLogicalOp(op: LogicalOp) -> list[str]:
    if op.kind == "project":
        names = ", ".join(expr.name for expr in op.args[0])
        return [f"Project[{names}]"]
    if op.kind == "aggregate":
        names = ", ".join([*op.args[0], *[expr.name for expr in op.args[1]]])
        return [f"Aggregate[{names}]"]
    if op.kind == "limit":
        count, offset = op.args
        return [f"Limit[offset={offset}, count={count}]"]
    if op.kind == "order_by":
        names = ", ".join(f"{name} {order}" for name, order in op.args[0])
        return [f"OrderBy[{names}]"]
    if op.kind == "shuffle":
        return [f"Shuffle[seed={op.args[0]}]"]
    if op.kind == "filter":
        return [f"Filter[{op.args[0].expr()}]"]
    if op.kind == "join":
        rhsPlan, lhsKeys, rhsKeys, _, joinType, *_ = op.args
        lines = [
            f"HashJoin[{joinType.name}]({', '.join(lhsKeys)} = {', '.join(rhsKeys)})"
        ]
        lines.extend(
            f"  rhs::{line}" for line in renderLogicalPlan(rhsPlan).splitlines()
        )
        return lines
    if op.kind == "python":
        return [f"Python[{op.args[1].__name__}]"]
    if op.kind == "catalog_write":
        return ["TableWrite"]
    return [op.kind]


def renderLogicalPlan(plan: LogicalPlan) -> str:
    lines = [_formatLogicalSource(plan.source)]
    for op in plan.operations:
        lines.extend(_formatLogicalOp(op))
    return "\n".join(lines)


class PlanBuilder(ABC):
    @abstractmethod
    def boltPlan(self) -> "BoltPlanBuilder":  # noqa: F821
        """
        Return a bolt plan equivalent.
        """
        ...

    def dtype(self) -> RowType:
        """
        Get the output type of the last added plan node.
        """
        return self.boltPlan().dtype()


class LogicalPlanBuilder(PlanBuilder):
    """Logical plan-building layer: lowers to Substrait, optimizes, renders,
    clones, and carries the DataFrame build operations
    (``project``/``filter``/``join``/…).

    The :class:`PlanBuilder` base above is intentionally minimal — just the
    execution contract (``boltPlan``). Everything that manipulates or lowers a
    *logical* plan lives here, so the shipping concrete builders
    (``SubstraitPlanBuilder``, ``BoltPlanBuilder``) inherit from this class.
    """

    @abstractmethod
    def logicalText(self) -> str:
        """
        Return a deterministic textual rendering of the logical plan.
        """
        ...

    def explain(self, executor=None) -> str:
        """Full explain text (logical, optimized logical, physical, stage DAG).

        ``executor`` is optional and is consulted only to (a) source the
        physical optimizer + dispatcher used to render the Physical Plan /
        Stage DAG sections, and (b) append its ``lastExecutionSummary`` when
        present. The logical optimizer is owned by the plan builder
        (``SubstraitPlanBuilder.optimize()``), not passed in here.
        """
        from ..execute import explain as _explain

        return _explain(self, executor=executor)

    @abstractmethod
    def toSubstraitPlan(self):
        """Lower this builder to a Substrait ``Plan`` proto (or ``None``)."""
        ...

    @abstractmethod
    def substraitPlan(self) -> "SubstraitPlanBuilder":  # noqa: F821
        """Lower this builder to a ``SubstraitPlanBuilder`` (the Substrait-backed
        logical plan). For a ``SubstraitPlanBuilder`` this returns ``self``."""
        ...

    def optimize(self) -> "SubstraitPlanBuilder":  # noqa: F821
        """Return a logically-optimized ``SubstraitPlanBuilder``.

        Default lowers to Substrait and optimizes there
        (``self.substraitPlan().optimize()``); ``SubstraitPlanBuilder``
        overrides this to run the logical optimizer on itself and return a
        new optimized ``SubstraitPlanBuilder``.
        """
        return self.substraitPlan().optimize()

    @abstractmethod
    def logicalPlan(self) -> LogicalPlan:
        """
        Return the logical plan description used by the optimizer.
        """
        ...

    @abstractmethod
    def cloneWithLogicalPlan(self, logicalPlan: LogicalPlan) -> "Self":  # noqa F821
        """
        Rebuild a plan builder from the provided logical plan.
        """
        ...

    def copy(self) -> "PlanBuilder":
        """Return an independent ``PlanBuilder`` sharing no mutable state.

        Default: structural clone by replaying the captured
        :class:`LogicalPlan` through ``cloneWithLogicalPlan`` (both
        shipping builders support this). A subclass that can't be
        structurally cloned should override with a materialise-and-rewrap
        fallback via ``PlanBuilderFactory.fromData``.
        """
        return self.cloneWithLogicalPlan(self.logicalPlan())

    @abstractmethod
    def project(
        self,
        expressions: list["NamedExpression"],  # noqa: F821
    ) -> "Self":  # noqa: F821
        """
        Add a project node to the plan with expressions to evaluate.
        """
        ...

    @abstractmethod
    def aggregate(
        self,
        keys: list["FieldExpression"],  # noqa: F821
        expressions: list["NamedExpression"],  # noqa: F821
    ) -> "Self":  # noqa: F821
        """
        Add an aggregation node to the plan with expressions to evaluate.
        """
        ...

    @abstractmethod
    def limit(self, count: int, offset: int) -> "Self":  # noqa: F821
        """
        Add a limit node to limit the output size to `count` rows starting at
        `offset` from the first row.
        """
        ...

    @abstractmethod
    def orderBy(self, order: list[tuple["FieldExpression", SortOrder]]) -> "Self":  # noqa: F821
        """
        Add an orderBy node to the plan with ordering expressions to evaluate.
        """
        ...

    @abstractmethod
    def shuffle(self, seed: int) -> "Self":  # noqa: F821
        """
        Add a shuffle node to the plan to shuffle rows.
        """
        ...

    @abstractmethod
    def filter(self, expression: "BooleanExpression") -> "Self":  # noqa: F821
        """
        Add a filter node to the plan with a filter expression.
        """
        ...

    @abstractmethod
    def join(
        self,
        rhs: "Self",  # noqa: F821
        lhsKeys: list["FieldExpression"],  # noqa: F821
        rhsKeys: list["FieldExpression"],  # noqa: F821
        outputLayout: list[str],
        joinType: JoinType,
    ) -> "Self":  # noqa: F821
        """
        Add a join node to the plan.
        """
        ...

    @abstractmethod
    def python(
        self, outputType: BoltType, function: Callable, *args, **kwargs
    ) -> "Self":  # noqa: F821
        """
        Add a plan node to execute a function with `args` and `kwargs`.
        """
        ...

    @abstractmethod
    def catalogWrite(
        self,
        directoryPath: str,
        fileFormat: FileFormat,
        partitionBy: list[str],
        numBuckets: int,
        bucketBy: list[str],
        aggregations: list[str],
    ) -> "Self":  # noqa: F821
        """
        Add a plan node writing to a catalog.
        """
        ...


class PlanBuilderFactory(ABC):
    @abstractmethod
    def empty(self) -> PlanBuilder:
        """
        Create a new empty plan builder.
        """
        ...

    @abstractmethod
    def fromData(self, data: RowVector) -> PlanBuilder:
        """
        Make a plan builder from raw data.
        """
        ...

    @abstractmethod
    def fromSingleFileRead(
        self,
        filePath: str,
        outputType: RowType,
        fileFormat: FileFormat,
    ) -> PlanBuilder:
        """
        Make a plan builder with a starting plan node to read a data file.
        """
        ...

    @abstractmethod
    def fromCatalog(
        self,
        tableName: str,
        outputType: RowType,
        partitionKeys: list[str],
        parameters: dict[str, str],
        splits: list["CatalogSplit"],  # noqa: F821
    ) -> PlanBuilder:
        """
        Make a plan builder with a starting plan node to read a table from a
        catalog.
        """
        ...

    @abstractmethod
    def tpchGenerator(
        self,
        table: "TpchTable",  # noqa: F821
        scaleFactor: int = 1,
        numSplits: int = 1,
    ) -> PlanBuilder:  # noqa: F821
        """
        Make a plan builder generating its input data from a TPCH generator.
        """
        ...


class LogicalPlanBuilderFactory(PlanBuilderFactory):
    """A ``PlanBuilderFactory`` that yields :class:`LogicalPlanBuilder`
    instances — the factory type the :class:`~boltml.dataframe.DataFrame`
    accepts.

    Every shipping factory (``SubstraitPlanBuilderFactory``,
    ``BoltPlanBuilderFactory``) produces a ``LogicalPlanBuilder`` subclass, so
    they inherit from this layer (mirroring ``LogicalPlanBuilder`` on the
    builder side). The construction protocol itself lives on the
    ``PlanBuilderFactory`` base above.
    """
