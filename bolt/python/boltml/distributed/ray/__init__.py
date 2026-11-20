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

from __future__ import annotations

from typing import TYPE_CHECKING


__all__ = [
    "AdaptiveExchangeManager",
    "ExchangeManager",
    "ExchangePartitionDescriptor",
    "ExchangeTransportPolicy",
    "FileExchangeManager",
    "RuntimeHeuristics",
    "RayExecutionConfig",
    "RayExecutionMode",
    "RayRolloutLevel",
    "RayWorkerMode",
    "RayRuntime",
    "StageTaskResult",
    "StageTaskSpec",
]


if TYPE_CHECKING:
    from .exchange_manager import AdaptiveExchangeManager as AdaptiveExchangeManager
    from .exchange_manager import ExchangeManager as ExchangeManager
    from .exchange_manager import (
        ExchangePartitionDescriptor as ExchangePartitionDescriptor,
    )
    from .exchange_manager import ExchangeTransportPolicy as ExchangeTransportPolicy
    from .exchange_manager import FileExchangeManager as FileExchangeManager
    from .heuristics import RuntimeHeuristics as RuntimeHeuristics
    from .runtime import RayExecutionConfig as RayExecutionConfig
    from .runtime import RayExecutionMode as RayExecutionMode
    from .runtime import RayRolloutLevel as RayRolloutLevel
    from .runtime import RayRuntime as RayRuntime
    from .runtime import RayWorkerMode as RayWorkerMode
    from .task_spec import StageTaskResult as StageTaskResult
    from .task_spec import StageTaskSpec as StageTaskSpec


def __getattr__(name: str):
    if name in {
        "AdaptiveExchangeManager",
        "ExchangeManager",
        "ExchangePartitionDescriptor",
        "ExchangeTransportPolicy",
        "FileExchangeManager",
    }:
        from . import exchange_manager

        return getattr(exchange_manager, name)
    if name == "RuntimeHeuristics":
        from .heuristics import RuntimeHeuristics

        return RuntimeHeuristics
    if name in {
        "RayExecutionConfig",
        "RayExecutionMode",
        "RayRolloutLevel",
        "RayWorkerMode",
        "RayRuntime",
    }:
        from . import runtime

        return getattr(runtime, name)
    if name in {"StageTaskResult", "StageTaskSpec"}:
        from . import task_spec

        return getattr(task_spec, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
