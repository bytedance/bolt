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

from .stage_dag import StageDAG


class Dispatcher(ABC):
    """Cuts an optimized plan into a ``StageDAG`` that the runtime
    executes. The only implementation is
    ``SubstraitPlanDispatcher``, which consumes a Substrait ``Plan``
    proto. The ABC is kept as the contract third-party dispatchers
    extend.
    """

    @abstractmethod
    def split(self, plan) -> StageDAG:  # noqa: F821
        """Cut *plan* into a ``StageDAG``."""
        ...
