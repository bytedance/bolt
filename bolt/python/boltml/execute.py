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

"""Free functions ``execute`` and ``explain``.

Optimization ownership lives with the plan builder and the executor, not
here (review #74/#73): the *logical* Substrait optimizer is owned by the
plan builder (``SubstraitPlanBuilder.optimize()``, reached through the
``LogicalPlanBuilder.optimize()`` default), and the *physical* optimizer +
dispatcher are owned by the distributed executor (``RayExecutor.eval``).
``execute`` therefore just dispatches to ``executor.eval(plan)``; each
executor decides its own strategy. ``explain`` renders the full pipeline
for inspection.
"""

from __future__ import annotations

import os
from typing import Optional

from pybolt import RowVector

from .logging import boltmlDebugLog
from .distributed.dispatcher import StageDAG, SubstraitPlanDispatcher
from .executor.base import Executor
from .optimizer import (
    SubstraitPhysicalOptimizerPipeline,
)
from .plan_builder.base import PlanBuilder


_DEAD_SUBSTRAIT_KILLSWITCH_VARS: tuple[str, ...] = (
    "BOLTML_SUBSTRAIT",
    "BOLTML_SUBSTRAIT_OPTIMIZER",
    "BOLTML_SUBSTRAIT_PHYSICAL",
    "BOLTML_SUBSTRAIT_DISPATCH",
)

FALSY_FLAG_VALUES: frozenset[str] = frozenset({"0", "false", "no", "off"})


def _refuseLegacyKillswitch() -> None:
    """Raise if a retired Substrait kill-switch env var is set falsy.

    The dual-pipeline driver has been retired; ``execute`` always lowers
    through Substrait. Silently honouring ``BOLTML_SUBSTRAIT=0`` would
    turn the assumption "set it to roll back" into a footgun.
    """
    offending: list[str] = []
    for var in _DEAD_SUBSTRAIT_KILLSWITCH_VARS:
        raw = os.environ.get(var, "")
        if raw.strip().lower() in FALSY_FLAG_VALUES:
            offending.append(f"{var}={raw!r}")
    if not offending:
        return
    raise RuntimeError(
        "Refusing to start: legacy Substrait kill-switch env var(s) set "
        "to a falsy value: " + ", ".join(offending) + ". The dual-pipeline "
        "engine has been retired; ``execute`` unconditionally lowers and "
        "optimizes through Substrait. Unset these env vars to silence this "
        "error."
    )


def execute(plan: PlanBuilder, executor: Executor) -> RowVector:
    """Run a ``PlanBuilder`` on the given executor.

    Dispatches to ``executor.eval(plan)``. The executor owns its execution
    strategy: ``LocalExecutor`` lowers the (already logically-optimized) plan
    and runs it in-process; ``RayExecutor`` physical-optimizes + dispatches it
    into a stage DAG. The logical optimizer is owned by the plan builder
    (``SubstraitPlanBuilder.optimize()``), so this function neither optimizes
    nor lowers.
    """
    _refuseLegacyKillswitch()
    boltmlDebugLog(
        "execute",
        f"starting execute logical_plan={plan.logicalPlan().source.kind}",
    )
    if not hasattr(executor, "eval"):
        raise RuntimeError(
            f"execute: executor of type {type(executor).__name__} does not "
            "implement eval(planBuilder); every executor must provide one."
        )
    return executor.eval(plan)


def explain(plan: PlanBuilder, *, executor: Optional[Executor] = None) -> str:
    """Full explain text: logical, optimized logical, physical, stage DAG.

    The optimized-logical view comes from the plan's own logical optimizer
    (``plan.optimize()``, a ``SubstraitPlanBuilder``). The physical plan + stage DAG are
    rendered with the *executor's own* physical optimizer + dispatcher when it
    exposes them (so a pinned ``defaultPartitionCount`` etc. shows up), falling
    back to the default distributed pipeline otherwise — so the Physical Plan
    section reflects what this executor would actually produce on Ray. Appends
    the executor's most-recent ``lastExecutionSummary`` when present.
    """
    from .substrait.explain import (
        render_logical_plan_text,
        render_physical_plan_text,
    )

    logicalPlanText = plan.logicalText()
    optimizedSubstrait = plan.optimize().toSubstraitPlan()
    physical = (
        getattr(executor, "physicalPipeline", None)
        or SubstraitPhysicalOptimizerPipeline()
    )
    dispatcher = getattr(executor, "dispatcher", None) or SubstraitPlanDispatcher()
    substraitPlan = physical.optimize(optimizedSubstrait)
    stageDag: StageDAG = dispatcher.split(substraitPlan)
    optimizedLogicalText = render_logical_plan_text(optimizedSubstrait)
    physicalText = render_physical_plan_text(
        stageDag, render_logical_plan_text(substraitPlan)
    )
    stageDagText = stageDag.explain()
    text = (
        f"Logical Plan:\n{logicalPlanText}\n\n"
        f"Optimized Logical Plan:\n{optimizedLogicalText}\n\n"
        f"Physical Plan:\n{physicalText}\n\n"
        f"Stage DAG:\n{stageDagText}"
    )
    runtimeSummary = (
        getattr(executor, "lastExecutionSummary", None)
        if executor is not None
        else None
    )
    if runtimeSummary is not None:
        text += f"\n\nRuntime Summary:\n{runtimeSummary.explain()}"
    return text
