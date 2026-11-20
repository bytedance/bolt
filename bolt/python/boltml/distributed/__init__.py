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

"""Distributed-execution components.

* ``boltml.distributed.dispatcher`` — Substrait-plan dispatcher that
  cuts a physical plan into a ``StageDAG`` at every ``ExchangeRel``.
  Owned by the distributed executor: ``RayExecutor.eval`` physical-
  optimizes the plan and then splits it with this dispatcher. The
  in-process ``LocalExecutor`` path skips it entirely (the C++ engine
  handles local shuffles inside a single PlanFragment).
* ``boltml.distributed.ray`` — Ray runtime that executes a
  ``StageDAG`` across remote workers, including the exchange-transport
  layer (file + object) and the per-task scheduling helpers.

This module deliberately does NOT eagerly import either subpackage:
``boltml.distributed.dispatcher.stage_dag`` imports
``boltml.distributed.ray.task_spec``, and the ray package
transitively pulls in ``boltml.dataframe`` → ``boltml.execute`` →
back to ``boltml.distributed.dispatcher``. Eagerly initialising both
at package load triggers a partial-import cycle. Callers use the
fully-qualified path (``from boltml.distributed.dispatcher import
X``) which auto-loads on first use without the up-front cycle.
"""
