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
from typing import Any, Callable

from pybolt import (
    BoltType,
    RowVector,
)


class Executor(ABC):
    """Executor protocol: run a materialised plan and register Python UDFs.

    A concrete executor exposes an execution entry point that the
    materialisation path dispatches through to run a plan and return a
    ``RowVector``, plus methods to register user Python vector / aggregation
    functions. Execution-strategy details — in particular the distributed
    stage-DAG dispatch — live on the relevant subclass (``RayExecutor``),
    not in this base protocol.
    """

    @abstractmethod
    def eval(self, plan: "PlanBuilder") -> RowVector:  # noqa: F821
        """
        Evaluate a materialised plan and return the resulting ``RowVector``.
        """
        ...

    @abstractmethod
    def registerVectorFunction(
        self,
        function: Callable,
        functionName: str,
        outputType: BoltType,
        numArgs: int,
        defaultArgs: list[Any],
        mapBatch: bool,
    ):
        """
        Register a user python function as a vector udf.
        """
        ...

    @abstractmethod
    def registerAggregationFunction(
        self,
        function: type["Aggregator"],  # noqa: F821
        functionName: str,
        outputType: BoltType,
        numArgs: int,
    ):
        """
        Register a user python aggregation function.
        """
        ...
