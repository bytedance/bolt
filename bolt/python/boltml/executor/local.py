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

from pybolt import (
    BoltTaskExecutor,
    BoltType,
    RowVector,
    registerPythonAggregator,
    registerPythonFunction,
    rowVector,
)

from .base import Executor


class LocalExecutor(Executor):
    """
    `Executor` implementation running on the local system.
    """

    _numEval = 0

    def __init__(self):
        self.__executor = BoltTaskExecutor()

    def eval(self, plan: "PlanBuilder") -> RowVector:  # noqa: F821
        LocalExecutor._numEval += 1
        # Optimization is a builder method now (not baked into boltPlan): run
        # the logical Substrait optimizer once on the materialisation path.
        boltPlan = plan.optimize().boltPlan()
        for nodeId, splitList in boltPlan.splits().items():
            self.__executor.addSplits(nodeId, splitList)
        result = self.__executor.execute(
            boltPlan.plan().planFragment(),
            f"LocalExecutor-Task-{LocalExecutor._numEval}",
        )
        # execute() returns None when no rows are produced (e.g. a filter
        # eliminates every row); hand higher layers a schema-bearing empty
        # RowVector so column lookups / Arrow conversion still work.
        return result if result is not None else rowVector(boltPlan.dtype())

    def registerVectorFunction(
        self,
        function: Callable,
        functionName: str,
        outputType: BoltType,
        numArgs: int,
        defaultArgs: list[Any],
        mapBatch: bool,
    ):
        registerPythonFunction(
            function,
            functionName,
            outputType,
            numArgs,
            defaultArgs,
            mapBatch,
        )

    def registerAggregationFunction(
        self,
        function: type["Aggregator"],  # noqa: F821
        functionName: str,
        outputType: BoltType,
        numArgs: int,
    ):
        registerPythonAggregator(function, functionName, outputType, numArgs)
