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

"""Substrait-Plan-based logical optimizer pipeline.

Canonical contract
------------------
``SubstraitOptimizerPipeline.optimize(plan: plan_pb2.Plan) -> plan_pb2.Plan``.
Substrait in, Substrait out. The pipeline has **zero** dependency on the
legacy ``optimizer.pipeline`` module: its rule schedule, fixpoint loop, and
change detection are driven entirely by the Substrait rules in
``optimizer.substrait_rules``. ``LogicalOp`` types do not appear anywhere
in this module — Substrait only.

The pipeline assumes its caller has already verified the input ``Plan`` is
something it can rewrite (use ``plan_is_substrait_lowerable`` from
``plan_builder.substrait`` to gate at the engine level). It does not look
for non-Substrait-lowerable sources itself; if given an empty plan it
returns it unchanged.

The legacy ``OptimizerPipeline`` has been retired; this pipeline is the
only path. Setting any ``BOLTML_SUBSTRAIT*`` env var to a falsy value
now raises a ``RuntimeError`` at the top of ``boltml.execute.execute``
(see ``execute._refuseLegacyKillswitch``) instead of silently ignoring
the request.
"""

from __future__ import annotations

import warnings
from typing import Tuple

from substrait.proto import plan as plan_pb2

from ..logging import boltmlDebugLog
from .substrait_rules import (
    EnsureSmallerOnRight,
    InnerDedupToSemi,
    JoinReorder,
    MergeProjects,
    PruneUnusedColumns,
    PushAggThroughJoin,
    PushFilterThroughProjects,
    Rule,
    RewriteLimits,
)


def _plans_equal(a: plan_pb2.Plan, b: plan_pb2.Plan) -> bool:
    """Return True iff two Substrait ``Plan`` protos are byte-equal.

    Used as the fixpoint termination check. ``SerializeToString`` on the
    Substrait Python bindings is deterministic for a given message graph
    (proto3 default field ordering), so two structurally identical plans
    serialize to identical bytes.
    """
    return a.SerializeToString() == b.SerializeToString()


def _default_schedule() -> Tuple[Rule, ...]:
    """Return the canonical Substrait rule schedule.

    Order:
      1. ``JoinReorder``               — global cost-based brute-force
         enumeration of join trees. Runs first so downstream rules
         see the rewritten join shape; the rewrite synthesizes new
         prefix-Project chains that the pushdown / pruning passes
         then optimize further.
      2. ``EnsureSmallerOnRight``      — local fallback that swaps a
         single join's inputs when the LHS is significantly larger
         than the RHS. Runs immediately after ``JoinReorder`` so it
         sees only joins that ``JoinReorder`` could not extract or
         reshape (e.g. q18, where a schema-tracking edge case in the
         full-rebuild path leaves the source-order join intact).
      3. ``PushAggThroughJoin``        — push partial aggregation below
         an inner join (AggregateJoinTranspose). Runs after
         ``JoinReorder`` so it sees the post-reorder join shape and
         before pruning so the partial aggs land before columns get
         dropped from the join sub-tree.
      4. ``PushFilterThroughProjects`` — predicates closer to scans.
      5. ``PruneUnusedColumns``        — drop unreferenced output
         columns top-down. Runs after predicate pushdown so the new
         projects landed by the pushdown are already in place when
         we compute the requirement set; runs before
         ``MergeProjects`` so the pruner has explicit
         per-output-column expressions to reason about (a merged
         project hides its constituent expressions).
      6. ``InnerDedupToSemi``          — rewrite an inner-join +
         dedup-groupby into a proper semi-join. Runs after
         ``JoinReorder`` and ``PushAggThroughJoin`` so it sees the
         post-rewrite join shape, and after ``PruneUnusedColumns``
         so a count() measure that's downstream-unused has already
         been dropped (which is the precondition for the rewrite —
         see ``inner_dedup_to_semi.py`` for why we conservatively
         skip when count measures are still present).
      7. ``MergeProjects``             — coalesce adjacent projects.
      8. ``RewriteLimits``             — fetch coalescing.
      9. ``MergeProjects``             — fuse pairs limit rewrite
         may have re-exposed.

    Each rule is independent and stateless; one instance per slot is
    fine because rules do not retain per-call state.
    """
    return (
        # NOTE: ``PushSemiJoinIntoAggregate`` is intentionally NOT in this
        # schedule. The rule itself is correct on the Substrait side and
        # targets correlated subqueries (q17, q18) where the agg input is
        # huge and the join key set is small, but the rewritten plan
        # currently fails Bolt's ``SubstraitToBoltPlan.cpp:1299`` hint
        # output_name validation ("Invalid hint output name r_thresh for
        # hash join"). The new SemiJoin and the rewritten Aggregate need
        # explicit ``RelCommon.Hint.output_names`` set to whatever the C++
        # side expects to see propagated from the original Aggregate's
        # output schema. Until that hint plumbing is in place, leaving the
        # rule out of the default schedule prevents
        # ``BOLTML_PUSH_SEMI_INTO_AGG=1`` from accidentally tripping the
        # C++ validator. Re-add this entry once the hint propagation is
        # audited and the rule is verified end-to-end.
        JoinReorder(),
        EnsureSmallerOnRight(),
        PushAggThroughJoin(),
        PushFilterThroughProjects(),
        PruneUnusedColumns(),
        InnerDedupToSemi(),
        MergeProjects(),
        RewriteLimits(),
        MergeProjects(),
    )


class SubstraitOptimizerPipeline:
    """Logical optimizer over Substrait ``Plan`` protobufs.

    Parameters
    ----------
    maxIterations:
        Upper bound on rewrite iterations before declaring fixpoint.
        Matches the legacy ``OptimizerPipeline`` default (``8``); raise
        for adversarial plans, lower only for tests.
    schedule:
        Optional override for the rule schedule. Default is the canonical
        schedule documented on ``_default_schedule``.

    Notes
    -----
    This pipeline takes a Substrait ``Plan`` directly; no ``LogicalOp``
    types leak in. ``boltml.execute.execute`` lowers a ``PlanBuilder``
    via ``toSubstraitPlan()`` and feeds the result here.
    """

    def __init__(
        self,
        maxIterations: int = 8,
        schedule: Tuple[Rule, ...] | None = None,
    ) -> None:
        self.__maxIterations = maxIterations
        self.__schedule = schedule if schedule is not None else _default_schedule()

    def optimize(self, plan: plan_pb2.Plan) -> plan_pb2.Plan:
        """Rewrite *plan* to a fixpoint and return the rewritten ``Plan``.

        Pure: *plan* is not mutated; the returned ``Plan`` is a fresh proto
        owned by the caller. Reaches fixpoint when one full pass over the
        rule schedule produces a Substrait ``Plan`` byte-equal to its input.
        Stops at ``maxIterations`` if the schedule is not contractive (a
        defensive bound; well-formed rules converge in O(1) iterations).

        An empty plan (``len(plan.relations) == 0``) is returned unchanged
        because there is no root ``Rel`` to walk; rules are no-ops on it.
        """
        current = plan_pb2.Plan()
        current.CopyFrom(plan)

        if not current.relations:
            return current

        boltmlDebugLog(
            "substrait_optimizer",
            f"start relations={len(current.relations)}",
        )

        for iteration in range(self.__maxIterations):
            iter_input = current
            for rule in self.__schedule:
                rewritten = rule.apply(current)
                if not _plans_equal(rewritten, current):
                    boltmlDebugLog(
                        "substrait_optimizer",
                        f"iter={iteration} rule={rule.name} produced change",
                    )
                    current = rewritten
            if _plans_equal(current, iter_input):
                boltmlDebugLog(
                    "substrait_optimizer",
                    f"fixpoint after iter={iteration}",
                )
                return current

        # Hitting the cap means the rule schedule is not contractive on
        # this plan — some pair of rules is either toggling the plan
        # back and forth or making forward progress that exceeds eight
        # passes. Either case is suspicious for a well-formed schedule
        # (most rules converge in O(1) iterations). We emit BOTH a
        # debug log (cheap, off by default) and a Python ``warnings``
        # warning (visible by default, captured by ``-W error`` in
        # tests) so cap exhaustion is detectable in production
        # operation, not just under the debug-logs flag.
        boltmlDebugLog(
            "substrait_optimizer",
            f"hit maxIterations={self.__maxIterations}; returning current plan",
        )
        warnings.warn(
            f"SubstraitOptimizerPipeline hit maxIterations="
            f"{self.__maxIterations} without reaching fixpoint; returning "
            f"the in-progress plan. The rule schedule is not contractive "
            f"on this input — set BOLTML_DEBUG_LOGS=1 to see per-iteration "
            f"rule activity, or pass a larger ``maxIterations`` if the plan "
            f"genuinely needs more passes.",
            RuntimeWarning,
            stacklevel=2,
        )
        return current
