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

"""Substrait-based physical optimizer pipeline.

A ``Plan -> Plan`` pipeline that inserts ``ExchangeRel`` nodes and
splits aggregates so the dispatcher can cut a stage DAG at every
exchange boundary. Differentially equivalent to the legacy
``optimizer.physical.PhysicalOptimizerPipeline`` on the scattering
string + partition count + producer-side schema for every TPC-H query,
but emits a Substrait ``Plan`` rather than a Python ``PhysicalPlan``
dataclass.

Selection
---------
The legacy ``PhysicalOptimizerPipeline`` has been retired; this is the
only physical pipeline. ``BOLTML_SUBSTRAIT_PHYSICAL`` is no longer a
selector — there is no alternative pipeline to choose. Setting it (or
any other ``BOLTML_SUBSTRAIT*`` var) to a falsy value is refused at
``boltml.execute.execute`` (see ``execute._refuseLegacyKillswitch``)
rather than silently honoured.
"""

from __future__ import annotations

from typing import Tuple

from substrait.proto import plan as plan_pb2

from ..logging import boltmlDebugLog
from .substrait_rules import Rule
from .substrait_rules.add_exchanges import AddExchanges
from .substrait_rules.broadcast_small_build_side import BroadcastSmallBuildSide
from .substrait_rules.partial_agg_before_exchange import PartialAggBeforeExchange
from .substrait_rules.remove_redundant_exchanges import RemoveRedundantExchanges


def _default_schedule(defaultPartitionCount: int) -> Tuple[Rule, ...]:
    """Return the canonical rule schedule for the physical optimizer.

    Schedule:

    1. ``AddExchanges`` — inserts an ``ExchangeRel`` above the input of
       every grouped aggregate, hash join, and shuffle.
    2. ``PartialAggBeforeExchange`` — splits each ``Aggregate(Exchange)``
       pair into ``Aggregate[final](Exchange(Aggregate[partial]))`` so
       the shuffle moves pre-aggregated rows. Decomposable measures
       only (sum, count, min, max).
    3. ``RemoveRedundantExchanges`` — elides an outer hash exchange
       whose distribution is already established by an inner hash
       exchange of the same shape (after ``PushAggThroughJoin``
       pushed a partial-agg below a join, AddExchanges materializes
       both the post-aggregate shuffle and the pre-existing pre-aggregate
       shuffle; only the inner one does real work).
    4. ``BroadcastSmallBuildSide`` — opt-in
       (``BOLTML_BROADCAST_BUILD_BYTES`` env var, default 0 = off):
       flips a HashJoin's right-side ``ExchangeRel`` from
       ``scatter_by_fields`` to ``broadcast`` when the right scan totals
       fewer than the threshold bytes. Runs after AddExchanges so the
       right-side ExchangeRel exists.
    """
    return (
        AddExchanges(defaultPartitionCount=defaultPartitionCount),
        PartialAggBeforeExchange(),
        RemoveRedundantExchanges(),
        BroadcastSmallBuildSide(),
    )


class SubstraitPhysicalOptimizerPipeline:
    """Physical optimizer over Substrait ``Plan`` protobufs.

    Parameters
    ----------
    defaultPartitionCount:
        Partition count plumbed into every inserted ``ExchangeRel``.
        Mirrors ``PhysicalOptimizerPipeline(defaultPartitionCount=...)``
        so the two pipelines are configured identically when wired
        side-by-side. Coerced to ``max(1, ...)`` by ``AddExchanges``
        itself; a zero/negative value cannot reach the dispatcher.
    schedule:
        Optional override for the rule schedule. Defaults to the
        canonical schedule above (``AddExchanges`` plus the four
        downstream rules).

    Notes
    -----
    Unlike ``PhysicalOptimizerPipeline``, this pipeline is Substrait-in /
    Substrait-out and does not consume or produce a ``PlanBuilder`` /
    ``PhysicalPlan``. The dispatcher cuts the stage DAG out of the
    inserted ``ExchangeRel`` nodes downstream; this pipeline is
    consumed both by tests and by the engine-level adapter that
    materialises a ``PhysicalPlan`` for downstream stages.
    """

    def __init__(
        self,
        defaultPartitionCount: int = 12,
        schedule: Tuple[Rule, ...] | None = None,
    ) -> None:
        self.__defaultPartitionCount = max(1, defaultPartitionCount)
        self.__schedule = (
            schedule
            if schedule is not None
            else _default_schedule(self.__defaultPartitionCount)
        )

    @property
    def defaultPartitionCount(self) -> int:
        """Return the partition count this pipeline configures rules with."""
        return self.__defaultPartitionCount

    def optimize(self, plan: plan_pb2.Plan) -> plan_pb2.Plan:
        """Apply every rule in the schedule once and return the result.

        Pure: *plan* is not mutated. Each rule is invoked in order; the
        output of rule ``i`` is the input of rule ``i+1``. No fixpoint
        loop is needed: every rule in the canonical schedule is
        idempotent (``AddExchanges`` refuses to wrap an input that is
        already an ``ExchangeRel``; ``PartialAggBeforeExchange`` only
        fires on single-phase aggregates; ``RemoveRedundantExchanges``
        and ``BroadcastSmallBuildSide`` are gated on shape predicates
        that disappear after one pass).
        An empty plan is returned unchanged because there is no root
        ``Rel`` to walk; the rules are no-ops on it.
        """
        current = plan_pb2.Plan()
        current.CopyFrom(plan)
        if not current.relations:
            return current
        boltmlDebugLog(
            "substrait_physical_optimizer",
            f"start relations={len(current.relations)} "
            f"partition_count={self.__defaultPartitionCount}",
        )
        for rule in self.__schedule:
            current = rule.apply(current)
            boltmlDebugLog(
                "substrait_physical_optimizer",
                f"applied rule={rule.name}",
            )
        return current
