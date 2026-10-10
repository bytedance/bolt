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
from enum import Enum
import os
import tempfile
from typing import Callable

import pyarrow as pa

from ...dataframe import DataFrame
from ...executor import LocalExecutor
from ...executor.base import Executor
from ...executor.ray import RayExecutor
from ...function.aggregation import Sum
from ...distributed.ray.exchange_manager import AdaptiveExchangeManager
from ...distributed.ray.exchange_manager import ExchangeTransportPolicy
from ...distributed.ray.exchange_manager import FileExchangeManager
from ...distributed.ray.runtime import RayExecutionConfig, RayExecutionMode
from ...plan_builder.base import PlanBuilderFactory
from ...plan_builder.substrait import SubstraitPlanBuilderFactory
from ...tpch import tpchTable


class TpchExecutionMode(str, Enum):
    LOCAL_OPTIMIZED = "local_optimized"
    LOCAL_NO_OPT = "local_no_opt"
    REMOTE_LEAF_OPTIMIZED = "remote_leaf_optimized"
    REMOTE_LEAF_NO_OPT = "remote_leaf_no_opt"
    STAGED_FILE_OPTIMIZED = "staged_file_optimized"
    STAGED_FILE_NO_OPT = "staged_file_no_opt"
    STAGED_OBJECT_OPTIMIZED = "staged_object_optimized"
    STAGED_OBJECT_NO_OPT = "staged_object_no_opt"

    @property
    def optimize(self) -> bool:
        return self in {
            TpchExecutionMode.LOCAL_OPTIMIZED,
            TpchExecutionMode.REMOTE_LEAF_OPTIMIZED,
            TpchExecutionMode.STAGED_FILE_OPTIMIZED,
            TpchExecutionMode.STAGED_OBJECT_OPTIMIZED,
        }

    @property
    def distributed(self) -> bool:
        return self in {
            TpchExecutionMode.REMOTE_LEAF_OPTIMIZED,
            TpchExecutionMode.REMOTE_LEAF_NO_OPT,
            TpchExecutionMode.STAGED_FILE_OPTIMIZED,
            TpchExecutionMode.STAGED_FILE_NO_OPT,
            TpchExecutionMode.STAGED_OBJECT_OPTIMIZED,
            TpchExecutionMode.STAGED_OBJECT_NO_OPT,
        }

    @property
    def executionMode(self) -> RayExecutionMode:
        if self in {
            TpchExecutionMode.REMOTE_LEAF_OPTIMIZED,
            TpchExecutionMode.REMOTE_LEAF_NO_OPT,
        }:
            return RayExecutionMode.REMOTE_LEAF
        return RayExecutionMode.STAGED_LOCAL

    @property
    def transport(self) -> str | None:
        if self in {
            TpchExecutionMode.STAGED_FILE_OPTIMIZED,
            TpchExecutionMode.STAGED_FILE_NO_OPT,
        }:
            return "file"
        if self in {
            TpchExecutionMode.STAGED_OBJECT_OPTIMIZED,
            TpchExecutionMode.STAGED_OBJECT_NO_OPT,
        }:
            return "object"
        return None


@dataclass(frozen=True)
class TpchQuerySpec:
    name: str
    tableNames: tuple[str, ...]
    queryBuilder: Callable[[dict[str, DataFrame]], DataFrame]
    minDistributedStages: int
    ordered: bool = True
    expectedStagedFailure: str | None = None


def _customer_projection_limit(tables: dict[str, DataFrame]) -> DataFrame:
    return (
        tables["customer"]
        .select(["custkey", "nationkey"])
        .orderBy("custkey")
        .select(count=5)
    )


def _orders_filter_projection_limit(tables: dict[str, DataFrame]) -> DataFrame:
    orders = tables["orders"]
    return (
        orders.filter(orders["totalprice"] > 1000.0)
        .select(["orderkey", "custkey", "totalprice"])
        .orderBy("orderkey")
        .select(count=5)
    )


def _orders_groupby_revenue(tables: dict[str, DataFrame]) -> DataFrame:
    return (
        tables["orders"]
        .select(["custkey", "totalprice"])
        .groupBy("custkey")
        .aggregate(revenue=Sum("totalprice"))
        .orderBy("custkey")
        .select(count=5)
    )


def _customer_nation_region_join(tables: dict[str, DataFrame]) -> DataFrame:
    nation = tables["nation"].rename(name="nation_name")
    region = tables["region"].rename(name="region_name")
    return (
        tables["customer"]
        .join(nation, {"nationkey"})
        .join(region, {"regionkey"})
        .select(["custkey", "nation_name", "region_name"])
        .orderBy("custkey")
        .select(count=10)
    )


def _orders_customer_nation_aggregate(tables: dict[str, DataFrame]) -> DataFrame:
    nation = tables["nation"].rename(name="nation_name")
    return (
        tables["orders"]
        .join(tables["customer"], {"custkey"})
        .join(nation, {"nationkey"})
        .select(["nation_name", "totalprice"])
        .groupBy("nation_name")
        .aggregate(revenue=Sum("totalprice"))
        .orderBy("nation_name")
        .select(count=5)
    )


PHASED_TPCH_QUERY_SUBSETS: dict[str, tuple[TpchQuerySpec, ...]] = {
    "phase1": (
        TpchQuerySpec(
            name="customer_projection_limit",
            tableNames=("customer",),
            queryBuilder=_customer_projection_limit,
            minDistributedStages=1,
        ),
        TpchQuerySpec(
            name="orders_filter_projection_limit",
            tableNames=("orders",),
            queryBuilder=_orders_filter_projection_limit,
            minDistributedStages=1,
        ),
    ),
    "phase2": (
        TpchQuerySpec(
            name="customer_projection_limit_transport_baseline",
            tableNames=("customer",),
            queryBuilder=_customer_projection_limit,
            minDistributedStages=1,
        ),
        TpchQuerySpec(
            name="orders_filter_projection_limit_transport_baseline",
            tableNames=("orders",),
            queryBuilder=_orders_filter_projection_limit,
            minDistributedStages=1,
        ),
    ),
    "phase3": (
        TpchQuerySpec(
            name="orders_groupby_revenue",
            tableNames=("orders",),
            queryBuilder=_orders_groupby_revenue,
            minDistributedStages=2,
        ),
    ),
    "phase4": (
        TpchQuerySpec(
            name="customer_nation_region_join",
            tableNames=("customer", "nation", "region"),
            queryBuilder=_customer_nation_region_join,
            minDistributedStages=3,
        ),
        TpchQuerySpec(
            name="orders_customer_nation_aggregate",
            tableNames=("orders", "customer", "nation"),
            queryBuilder=_orders_customer_nation_aggregate,
            minDistributedStages=3,
        ),
    ),
}


@dataclass
class TpchExecutionParams:
    """Executor + plan-builder factory for a harness query run.

    The harness previously returned a ``LocalEngine`` bundling these;
    the Engine class has since been removed in favour of free
    ``execute``/``explain`` functions, so the harness ships the bundle
    directly. The logical optimizer now lives on the plan builder
    (``SubstraitPlanBuilder.optimize()``) and is intrinsic to
    materialisation, so there is no separate ``optimize`` vs ``no-opt``
    toggle — every run optimizes.
    """

    executor: Executor
    planFactory: PlanBuilderFactory

    def asDataFrameKwargs(self) -> dict:
        return {
            "executor": self.executor,
            "planFactory": self.planFactory,
        }


class TpchParityHarness:
    def __init__(self, scaleFactor: float = 0.01, numParts: int | None = None):
        self._scaleFactor = scaleFactor
        # ``numParts`` is the number of TPC-H scan splits — the single-file
        # (1) vs multi-file (>=2) dimension of the correctness matrix. Defaults
        # to 2; override with ``BOLTML_TPCH_NUM_PARTS`` so the matrix can sweep
        # the file layout without editing the parity test.
        self._numParts = (
            numParts
            if numParts is not None
            else int(os.environ.get("BOLTML_TPCH_NUM_PARTS", "2"))
        )

    def engine(
        self, mode: TpchExecutionMode, tmpDir: str | None = None
    ) -> TpchExecutionParams:
        # Optimization is intrinsic to materialisation now: the executor's
        # eval path runs ``plan.optimize()``, so the plan-builder factory no
        # longer carries an optimizer schedule. NOTE: ``mode.optimize`` no
        # longer toggles optimization (both modes optimize on execute); a true
        # no-optimize mode would need an executor-level opt-out — left as a
        # follow-up under the substrait-canonical architecture.
        planFactory = SubstraitPlanBuilderFactory()
        if not mode.distributed:
            executor = LocalExecutor()
        elif mode in {
            TpchExecutionMode.REMOTE_LEAF_OPTIMIZED,
            TpchExecutionMode.REMOTE_LEAF_NO_OPT,
        }:
            executor = RayExecutor(config=RayExecutionConfig(mode=mode.executionMode))
        elif mode.transport == "object":
            if tmpDir is None:
                raise ValueError("tmpDir is required for staged object execution mode.")
            executor = RayExecutor(
                exchangeManager=AdaptiveExchangeManager(
                    tmpDir,
                    policy=ExchangeTransportPolicy(
                        preferredTransport="object",
                        maxRowsForObject=100_000,
                    ),
                )
            )
        else:
            if tmpDir is None:
                raise ValueError("tmpDir is required for staged file execution mode.")
            executor = RayExecutor(exchangeManager=FileExchangeManager(tmpDir))
        return TpchExecutionParams(
            executor=executor,
            planFactory=planFactory,
        )

    def tpch(self, table: str, engine: TpchExecutionParams) -> DataFrame:
        # Build leaf reads through the run's own factory so the plan is built
        # by the configured factory (the inherited ``tpchGenerator`` routes
        # through ``fromTpchRead``). Optimization is intrinsic to
        # materialisation now — ``plan.optimize()`` applies the logical
        # optimizer regardless of which builder produced the plan.
        pb = engine.planFactory.tpchGenerator(
            tpchTable(table), scaleFactor=self._scaleFactor, numSplits=self._numParts
        )
        return DataFrame(pb, **engine.asDataFrameKwargs())

    def tables(
        self, engine: TpchExecutionParams, tableNames: tuple[str, ...]
    ) -> dict[str, DataFrame]:
        return {name: self.tpch(name, engine) for name in tableNames}

    def runQuery(
        self,
        spec: TpchQuerySpec,
        mode: TpchExecutionMode,
        tmpDir: str | None = None,
    ) -> tuple[DataFrame, TpchExecutionParams]:
        engine = self.engine(mode, tmpDir=tmpDir)
        return spec.queryBuilder(self.tables(engine, spec.tableNames)), engine

    def assertFileParity(self, testCase, spec: TpchQuerySpec) -> None:
        expected = self.assertLocalParity(testCase, spec)

        with (
            tempfile.TemporaryDirectory() as stagedOptimizedTmpDir,
            tempfile.TemporaryDirectory() as stagedNoOptTmpDir,
        ):
            stagedOptimized, stagedOptimizedEngine = self.runQuery(
                spec,
                TpchExecutionMode.STAGED_FILE_OPTIMIZED,
                tmpDir=stagedOptimizedTmpDir,
            )
            stagedNoOpt, stagedNoOptEngine = self.runQuery(
                spec,
                TpchExecutionMode.STAGED_FILE_NO_OPT,
                tmpDir=stagedNoOptTmpDir,
            )
            testCase.assertEqual(
                self.normalizeResult(expected, ordered=spec.ordered),
                self.normalizeResult(stagedOptimized, ordered=spec.ordered),
            )
            testCase.assertEqual(
                self.normalizeResult(expected, ordered=spec.ordered),
                self.normalizeResult(stagedNoOpt, ordered=spec.ordered),
            )
            testCase.assertGreaterEqual(
                len(stagedOptimizedEngine.executor.lastExecutionSummary.stages),
                spec.minDistributedStages,
            )
            testCase.assertGreaterEqual(
                len(stagedNoOptEngine.executor.lastExecutionSummary.stages),
                spec.minDistributedStages,
            )
            for engine in (stagedOptimizedEngine, stagedNoOptEngine):
                self.assertDistributedSummary(
                    testCase,
                    engine.executor.lastExecutionSummary,
                    expectedTransport="file",
                )

    def assertObjectParity(self, testCase, spec: TpchQuerySpec) -> None:
        expected = self.assertLocalParity(testCase, spec)

        with (
            tempfile.TemporaryDirectory() as stagedOptimizedTmpDir,
            tempfile.TemporaryDirectory() as stagedNoOptTmpDir,
        ):
            stagedOptimized, stagedOptimizedEngine = self.runQuery(
                spec,
                TpchExecutionMode.STAGED_OBJECT_OPTIMIZED,
                tmpDir=stagedOptimizedTmpDir,
            )
            stagedNoOpt, stagedNoOptEngine = self.runQuery(
                spec,
                TpchExecutionMode.STAGED_OBJECT_NO_OPT,
                tmpDir=stagedNoOptTmpDir,
            )
            testCase.assertEqual(
                self.normalizeResult(expected, ordered=spec.ordered),
                self.normalizeResult(stagedOptimized, ordered=spec.ordered),
            )
            testCase.assertEqual(
                self.normalizeResult(expected, ordered=spec.ordered),
                self.normalizeResult(stagedNoOpt, ordered=spec.ordered),
            )
            testCase.assertGreaterEqual(
                len(stagedOptimizedEngine.executor.lastExecutionSummary.stages),
                spec.minDistributedStages,
            )
            testCase.assertGreaterEqual(
                len(stagedNoOptEngine.executor.lastExecutionSummary.stages),
                spec.minDistributedStages,
            )
            for engine in (stagedOptimizedEngine, stagedNoOptEngine):
                summary = engine.executor.lastExecutionSummary
                testCase.assertTrue(
                    any(stage.transport == "object" for stage in summary.stages)
                )
                self.assertDistributedSummary(
                    testCase, summary, expectedTransport="object"
                )

    def assertLocalParity(self, testCase, spec: TpchQuerySpec) -> DataFrame:
        expected, _ = self.runQuery(spec, TpchExecutionMode.LOCAL_OPTIMIZED)
        localNoOpt, _ = self.runQuery(spec, TpchExecutionMode.LOCAL_NO_OPT)
        testCase.assertEqual(
            self.normalizeResult(expected, ordered=spec.ordered),
            self.normalizeResult(localNoOpt, ordered=spec.ordered),
        )
        return expected

    def assertFileFailureClassification(self, testCase, spec: TpchQuerySpec) -> None:
        if spec.expectedStagedFailure is None:
            raise ValueError(
                f"Query {spec.name} does not declare an expected staged failure."
            )
        for mode in (
            TpchExecutionMode.STAGED_FILE_OPTIMIZED,
            TpchExecutionMode.STAGED_FILE_NO_OPT,
        ):
            with tempfile.TemporaryDirectory() as tmpDir:
                result, _ = self.runQuery(spec, mode, tmpDir=tmpDir)
                with testCase.assertRaises(Exception) as raised:
                    self.normalizeResult(result, ordered=spec.ordered)
                testCase.assertEqual(
                    self.classifyFailure(raised.exception),
                    spec.expectedStagedFailure,
                )

    def assertRemoteLeafParity(self, testCase, spec: TpchQuerySpec) -> None:
        expected = self.assertLocalParity(testCase, spec)

        remoteOptimized, remoteOptimizedEngine = self.runQuery(
            spec,
            TpchExecutionMode.REMOTE_LEAF_OPTIMIZED,
        )
        remoteNoOpt, remoteNoOptEngine = self.runQuery(
            spec,
            TpchExecutionMode.REMOTE_LEAF_NO_OPT,
        )

        testCase.assertEqual(
            self.normalizeResult(expected, ordered=spec.ordered),
            self.normalizeResult(remoteOptimized, ordered=spec.ordered),
        )
        testCase.assertEqual(
            self.normalizeResult(expected, ordered=spec.ordered),
            self.normalizeResult(remoteNoOpt, ordered=spec.ordered),
        )

        for engine in (remoteOptimizedEngine, remoteNoOptEngine):
            summary = engine.executor.lastExecutionSummary
            testCase.assertEqual(summary.executionMode, RayExecutionMode.REMOTE_LEAF)
            for stage in summary.stages:
                testCase.assertTrue(stage.remote)
                testCase.assertIsNotNone(stage.workerId)
                testCase.assertIsNone(stage.fallbackReason)
            self.assertDistributedSummary(testCase, summary, expectedTransport="file")

    @staticmethod
    def assertDistributedSummary(
        testCase, summary, expectedTransport: str | None = None
    ) -> None:
        if expectedTransport is not None:
            for stage in summary.stages:
                if stage.transport is not None:
                    testCase.assertEqual(stage.transport, expectedTransport)
        for stage in summary.stages:
            if stage.inputCount > 0:
                testCase.assertEqual(stage.partitionCount, 12)

    @staticmethod
    def _normalizeScalar(value):
        if value is None:
            return ("none", None)
        if isinstance(value, float):
            return ("float", round(value, 2))
        if isinstance(value, bytes):
            return ("bytes", value.hex())
        if isinstance(value, list):
            return ("list", tuple(TpchParityHarness._normalizeScalar(v) for v in value))
        if isinstance(value, dict):
            return (
                "dict",
                tuple(
                    (key, TpchParityHarness._normalizeScalar(val))
                    for key, val in sorted(value.items())
                ),
            )
        return (type(value).__name__, value)

    @staticmethod
    def normalizeResult(dataframe: DataFrame, ordered: bool = False):
        table = dataframe.toArrow(pa.Table)
        schema = tuple((field.name, str(field.type)) for field in table.schema)
        rows = tuple(
            tuple(
                TpchParityHarness._normalizeScalar(row[name])
                for name in table.column_names
            )
            for row in table.to_pylist()
        )
        if not ordered:
            rows = tuple(sorted(rows))
        return schema, rows

    @staticmethod
    def classifyFailure(error: Exception) -> str:
        if isinstance(error, KeyError):
            return "transport"
        if isinstance(error, NotImplementedError):
            message = str(error)
            if "1-input or 2-input join consumers" in message:
                return "binding"
            if "single-partition exchange descriptors" in message:
                return "unsupported-shape"
            return "unsupported-shape"
        message = str(error).lower()
        if "retry" in message or "transient" in message:
            return "retry"
        if "exchange" in message or "transport" in message or "publish" in message:
            return "transport"
        if (
            "bind" in message
            or "loaded inputs" in message
            or "plan template" in message
        ):
            return "binding"
        return "planner"
