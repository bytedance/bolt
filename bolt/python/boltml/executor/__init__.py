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

from .base import Executor as Executor
from .local import LocalExecutor as LocalExecutor

# RayExecutor is intentionally NOT re-exported from this base package: it
# pulls in the Ray runtime (its module tree does ``import ray`` via
# exchange_manager / runtime / scheduler), so re-exporting it here would leak
# a distributed-only dependency into the base ``boltml.executor`` namespace
# and the eager ``import boltml`` path. Import it directly from its submodule:
#     from boltml.executor.ray import RayExecutor
