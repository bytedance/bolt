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

from typing import Any, Callable

from pybolt import BoltType, RowVector

from ..logging import boltmlDebugLog
from ..distributed.ray.exchange_manager import ExchangeManager
from ..distributed.ray import RayExecutionConfig, RayRuntime
from .base import Executor
from .local import LocalExecutor
from ..distributed.dispatcher import SubstraitPlanDispatcher
from ..distributed.ray.udf_spec import UdfRegistry, UdfSpec, _serializeBoltType
from ..optimizer import SubstraitPhysicalOptimizerPipeline


class RayExecutor(Executor):
    _numEval = 0

    def __init__(
        self,
        runtime: RayRuntime | None = None,
        exchangeManager: ExchangeManager | None = None,
        config: RayExecutionConfig | None = None,
        physical: SubstraitPhysicalOptimizerPipeline | None = None,
        dispatcher: SubstraitPlanDispatcher | None = None,
    ):
        self.__runtime = runtime or RayRuntime(config=config)
        self.__exchangeManager = exchangeManager
        # UDFs registered via ``registerVectorFunction`` /
        # ``registerAggregationFunction`` are recorded here on the driver
        # and shipped on every ``runStageDag`` call inside the
        # ``StageTaskSpec`` so remote workers can re-register them on
        # their own pybolt singleton before executing the plan.
        # Without propagation, the worker sees a Substrait function
        # extension reference (e.g. ``my_udf:i64``) for which no
        # implementation is registered and fails deep in C++.
        self.__udfRegistry: UdfRegistry = UdfRegistry()
        # Driver-side LocalExecutor purely for the side-effect of
        # registering the UDF on the driver's pybolt singleton too —
        # boltml's plan compilation / type inference / explain rendering
        # all run on the driver and need to look the function up by
        # name. We don't dispatch any compute through this executor;
        # the actual work goes through ``self.__runtime`` below.
        self.__driverPyboltRegistrar: LocalExecutor = LocalExecutor()
        # Distributed-only concerns. The *physical* optimizer and the stage
        # dispatcher live on this executor (not in ``execute``/``DataFrame``),
        # so the in-process ``LocalExecutor`` path never touches them. ``eval``
        # runs the full distributed pipeline: take the logically-optimized
        # Substrait from the builder -> physical optimize -> split into a
        # stage DAG -> ``evalStageDag``. Both are injectable so callers can
        # pin behaviour (e.g. ``defaultPartitionCount``) on the executor that
        # owns them, rather than on the DataFrame / ``execute``.
        self.__physical = physical or SubstraitPhysicalOptimizerPipeline()
        self.__dispatcher = dispatcher or SubstraitPlanDispatcher()

    def eval(self, plan: "PlanBuilder") -> RowVector:  # noqa: F821
        """Physical-optimize + dispatch a plan into a stage DAG and run it.

        Applies the builder-owned logical optimizer via ``plan.optimize()``
        and lowers the result to Substrait, then runs the distributed-only
        physical optimizer, splits the result into a stage DAG, and evaluates
        it. The logical optimizer lives on the builder
        (``SubstraitPlanBuilder.optimize()``); the physical optimizer +
        dispatcher are owned by this executor.
        """
        optimizedSubstrait = plan.optimize().toSubstraitPlan()
        physicalPlan = self.__physical.optimize(optimizedSubstrait)
        stageDag = self.__dispatcher.split(physicalPlan)
        return self.evalStageDag(stageDag)

    def evalStageDag(self, stageDag: "StageDAG") -> RowVector:  # noqa: F821
        executionMode = getattr(
            getattr(self.__runtime, "executionMode", None), "value", "unknown"
        )
        boltmlDebugLog(
            "ray-executor",
            f"executing stage dag stages={[stage.stageId for stage in stageDag.stages]} "
            f"mode={executionMode} udfs={len(self.__udfRegistry)}",
        )
        return self.__runtime.runStageDag(
            stageDag,
            self.__exchangeManager,
            udfRegistry=self.__udfRegistry,
        )

    @property
    def lastExecutionSummary(self):
        return self.__runtime._lastExecutionSummary

    @property
    def lastStageTaskResults(self):
        return self.__runtime._lastStageTaskResults

    @property
    def executionMode(self):
        return self.__runtime.executionMode

    @property
    def physicalPipeline(self) -> SubstraitPhysicalOptimizerPipeline:
        """The physical optimizer this executor runs in ``eval``.

        Exposed so ``explain`` can render the Physical Plan / Stage DAG that
        *this* executor would actually produce (e.g. a pinned
        ``defaultPartitionCount``) rather than a fresh default pipeline.
        """
        return self.__physical

    @property
    def dispatcher(self) -> SubstraitPlanDispatcher:
        """The stage dispatcher this executor runs in ``eval`` (see
        ``physicalPipeline``)."""
        return self.__dispatcher

    @property
    def udfRegistry(self) -> UdfRegistry:
        """Read-only view of UDFs registered on this executor.

        Exposed primarily for tests / debugging. The registry is shipped
        to remote workers automatically on each ``evalStageDag`` call.
        """
        return self.__udfRegistry

    def registerVectorFunction(
        self,
        function: Callable,
        functionName: str,
        outputType: BoltType,
        numArgs: int,
        defaultArgs: list[Any],
        mapBatch: bool,
    ):
        # Register on the driver's pybolt so plan compilation /
        # explain / driver-local fallback paths can resolve the name.
        self.__driverPyboltRegistrar.registerVectorFunction(
            function,
            functionName,
            outputType,
            numArgs,
            defaultArgs,
            mapBatch,
        )
        # Capture for ship-to-workers propagation. Type goes as a
        # canonical string; pybolt BoltType bindings don't pickle, so
        # storing the live object would fail Ray's cloudpickle check.
        self.__udfRegistry = self.__udfRegistry.appended(
            UdfSpec(
                kind="vector",
                function=function,
                functionName=functionName,
                outputTypeStr=_serializeBoltType(outputType),
                numArgs=numArgs,
                defaultArgs=tuple(defaultArgs),
                mapBatch=mapBatch,
            )
        )

    def registerAggregationFunction(
        self,
        function: "Aggregator",  # noqa: F821
        functionName: str,
        outputType: BoltType,
        numArgs: int,
    ):
        self.__driverPyboltRegistrar.registerAggregationFunction(
            function,
            functionName,
            outputType,
            numArgs,
        )
        self.__udfRegistry = self.__udfRegistry.appended(
            UdfSpec(
                kind="aggregation",
                function=function,
                functionName=functionName,
                outputTypeStr=_serializeBoltType(outputType),
                numArgs=numArgs,
                defaultArgs=(),
                mapBatch=False,
            )
        )
