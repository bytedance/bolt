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

"""Unit + structural tests for ``PartialAggBeforeExchange``.

The rule splits ``Aggregate(Exchange(...))`` into
``Aggregate[final](Exchange(Aggregate[partial](...)))`` so the shuffle
moves pre-aggregated rows. Tests verify:

  * Structural shape after the rule (top agg phase = INTERMEDIATE_TO_RESULT,
    new partial agg below the exchange with phase = INITIAL_TO_INTERMEDIATE,
    exchange's scatter_by_fields renumbered to point at partial-agg's K
    columns).
  * No-op cases (global agg, distinct, non-decomposable measure, no
    exchange below the agg).
  * Idempotency under repeated apply.
  * End-to-end execution agreement: pipeline-with-this-rule produces
    the same row contents as the pipeline-without (the in-process
    LocalEngine, sufficient for behavioral parity even though the
    rule's PERFORMANCE benefit is only realized in distributed mode).
"""

from __future__ import annotations

import unittest

import pyarrow as pa
from substrait.proto import algebra, plan as plan_pb2

from ..dataframe import DataFrame
from ..function.aggregation import Mean, Sum, Count
from ..optimizer.substrait_physical_pipeline import (
    SubstraitPhysicalOptimizerPipeline,
)
from ..optimizer.substrait_rules.add_exchanges import AddExchanges
from ..optimizer.substrait_rules.partial_agg_before_exchange import (
    PartialAggBeforeExchange,
)
from ..plan_builder.substrait import SubstraitPlanBuilderFactory


def _build_substrait(df: DataFrame) -> plan_pb2.Plan:
    return df._planBuilder_.toSubstraitPlan()


def _physical(p: plan_pb2.Plan) -> plan_pb2.Plan:
    return SubstraitPhysicalOptimizerPipeline().optimize(p)


def _add_exchanges_only(p: plan_pb2.Plan) -> plan_pb2.Plan:
    """Run ONLY AddExchanges (skip PartialAggBeforeExchange) for diff tests."""
    return AddExchanges().apply(p)


def _find_rel(rel: algebra.Rel, kind: str) -> algebra.Rel | None:
    """Pre-order search for the first Rel of *kind*; ``None`` if absent."""
    if rel.WhichOneof("rel_type") == kind:
        return rel
    cur_kind = rel.WhichOneof("rel_type")
    if cur_kind in (
        "filter",
        "project",
        "fetch",
        "aggregate",
        "sort",
        "extension_single",
        "exchange",
        "expand",
        "write",
    ):
        inner = getattr(rel, cur_kind)
        if inner.HasField("input"):
            return _find_rel(inner.input, kind)
    elif cur_kind in ("hash_join", "merge_join", "nested_loop_join", "join", "cross"):
        inner = getattr(rel, cur_kind)
        if inner.HasField("left"):
            f = _find_rel(inner.left, kind)
            if f is not None:
                return f
        if inner.HasField("right"):
            return _find_rel(inner.right, kind)
    return None


def _all_aggregates(rel: algebra.Rel) -> list[algebra.AggregateRel]:
    """Pre-order list of every AggregateRel in *rel*'s tree."""
    out: list[algebra.AggregateRel] = []

    def _walk(r: algebra.Rel) -> None:
        kind = r.WhichOneof("rel_type")
        if kind == "aggregate":
            out.append(r.aggregate)
            if r.aggregate.HasField("input"):
                _walk(r.aggregate.input)
            return
        if kind in (
            "filter",
            "project",
            "fetch",
            "sort",
            "extension_single",
            "exchange",
            "expand",
            "write",
        ):
            inner = getattr(r, kind)
            if inner.HasField("input"):
                _walk(inner.input)
        elif kind in (
            "hash_join",
            "merge_join",
            "nested_loop_join",
            "join",
            "cross",
        ):
            inner = getattr(r, kind)
            if inner.HasField("left"):
                _walk(inner.left)
            if inner.HasField("right"):
                _walk(inner.right)

    _walk(rel)
    return out


# ---------------------------------------------------------------------------
# Structural tests
# ---------------------------------------------------------------------------


class TestPartialAggBeforeExchangeStructural(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = SubstraitPlanBuilderFactory()

    def test_grouped_sum_splits_into_partial_then_final(self) -> None:
        df = DataFrame(
            {"grp": ["a", "a", "b"], "value": [1, 2, 3]},
            planFactory=self.factory,
        )
        df.groupBy("grp").aggregate(total=Sum("value"))

        out = _physical(_build_substrait(df))
        aggs = _all_aggregates(out.relations[0].root.input)
        # Two aggregates now: top = final, bottom = partial.
        self.assertEqual(len(aggs), 2)
        top, bottom = aggs[0], aggs[1]

        # Top is INTERMEDIATE_TO_RESULT; bottom is INITIAL_TO_INTERMEDIATE.
        self.assertEqual(
            top.measures[0].measure.phase,
            algebra.AggregationPhase.AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT,
        )
        self.assertEqual(
            bottom.measures[0].measure.phase,
            algebra.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE,
        )

        # The exchange sits between them.
        self.assertEqual(top.input.WhichOneof("rel_type"), "exchange")
        ex = top.input.exchange
        self.assertEqual(ex.input.WhichOneof("rel_type"), "aggregate")

        # Exchange's scatter_by_fields points at position 0 (the K
        # column at the partial agg's output).
        self.assertEqual(ex.WhichOneof("exchange_kind"), "scatter_by_fields")
        self.assertEqual(len(ex.scatter_by_fields.fields), 1)
        self.assertEqual(
            ex.scatter_by_fields.fields[0].direct_reference.struct_field.field,
            0,
        )

    def test_grouped_count_splits(self) -> None:
        df = DataFrame(
            {"grp": ["a", "a", "b"], "v": [1, 2, 3]}, planFactory=self.factory
        )
        df.groupBy("grp").aggregate(n=Count("v"))
        out = _physical(_build_substrait(df))
        aggs = _all_aggregates(out.relations[0].root.input)
        self.assertEqual(len(aggs), 2)
        # Both partial and final retain the count function (Bolt's
        # CountAggregate handles the addIntermediateResults internally).
        self.assertEqual(
            aggs[0].measures[0].measure.phase,
            algebra.AggregationPhase.AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT,
        )
        self.assertEqual(
            aggs[1].measures[0].measure.phase,
            algebra.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE,
        )

    def test_global_aggregate_does_not_split(self) -> None:
        # No grouping → no exchange below → ``PartialAggBeforeExchange``
        # is a no-op. Global aggregates have no grouping keys to scatter
        # on, so they stay single-phase through the whole pipeline.
        df = DataFrame({"v": [1, 2, 3]}, planFactory=self.factory)
        df.groupBy().aggregate(total=Sum("v"))
        # Add exchanges first (the rule's precondition), then test that
        # PartialAggBeforeExchange ALONE doesn't split a global aggregate.
        with_exchanges = AddExchanges().apply(_build_substrait(df))
        out = PartialAggBeforeExchange().apply(with_exchanges)
        aggs = _all_aggregates(out.relations[0].root.input)
        # Just the original single-phase agg; no partial appears.
        self.assertEqual(len(aggs), 1)
        self.assertEqual(
            aggs[0].measures[0].measure.phase,
            algebra.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT,
        )

    def test_non_decomposable_measure_does_not_split(self) -> None:
        # Mean is not decomposable in this allowlist; rule must skip.
        df = DataFrame(
            {"grp": ["a", "a", "b"], "v": [1, 2, 3]}, planFactory=self.factory
        )
        df.groupBy("grp").aggregate(avg=Mean("v"))
        out = _physical(_build_substrait(df))
        aggs = _all_aggregates(out.relations[0].root.input)
        self.assertEqual(len(aggs), 1)

    def test_addexchanges_only_no_partial_inserted(self) -> None:
        # Sanity: confirm AddExchanges alone doesn't insert a partial agg.
        df = DataFrame(
            {"grp": ["a", "a", "b"], "v": [1, 2, 3]}, planFactory=self.factory
        )
        df.groupBy("grp").aggregate(total=Sum("v"))
        out = _add_exchanges_only(_build_substrait(df))
        aggs = _all_aggregates(out.relations[0].root.input)
        self.assertEqual(len(aggs), 1)

    def test_idempotent_under_repeated_apply(self) -> None:
        df = DataFrame({"grp": ["a"], "v": [1]}, planFactory=self.factory)
        df.groupBy("grp").aggregate(total=Sum("v"))
        rule = PartialAggBeforeExchange()
        once_input = AddExchanges().apply(_build_substrait(df))
        once = rule.apply(once_input)
        twice = rule.apply(once)
        self.assertEqual(once.SerializeToString(), twice.SerializeToString())

    def test_partition_count_preserved(self) -> None:
        df = DataFrame({"grp": ["a"], "v": [1]}, planFactory=self.factory)
        df.groupBy("grp").aggregate(total=Sum("v"))
        out = SubstraitPhysicalOptimizerPipeline(defaultPartitionCount=7).optimize(
            _build_substrait(df)
        )
        ex = _find_rel(out.relations[0].root.input, "exchange")
        self.assertIsNotNone(ex)
        self.assertEqual(ex.exchange.partition_count, 7)

    def test_scattering_alias_preserved(self) -> None:
        # The dispatcher reads the "hash(col,col)" alias verbatim; the
        # rule must not mangle it.
        df = DataFrame({"grp": ["a"], "v": [1]}, planFactory=self.factory)
        df.groupBy("grp").aggregate(total=Sum("v"))
        out = _physical(_build_substrait(df))
        ex = _find_rel(out.relations[0].root.input, "exchange")
        self.assertIsNotNone(ex)
        self.assertEqual(ex.exchange.common.hint.alias, "hash(grp)")


# ---------------------------------------------------------------------------
# Behavioral tests (LocalEngine)
# ---------------------------------------------------------------------------


class TestPartialAggBeforeExchangeBehavioral(unittest.TestCase):
    """End-to-end: a DataFrame whose groupBy goes through the physical
    optimizer (which now includes ``PartialAggBeforeExchange``) yields
    the same rows as the equivalent reference computation.

    Local-mode execution doesn't actually shuffle rows, but the
    partial-then-final split still runs through Bolt's kPartial /
    kFinal aggregate code paths — so this exercises the rule's
    correctness independently of Ray.
    """

    def _result_dict(self, df: DataFrame, key: str, value: str) -> dict:
        rows = df.toArrow(pa.Table).to_pylist()
        return {row[key]: row[value] for row in rows}

    def test_grouped_sum_matches_unoptimized(self) -> None:
        df = DataFrame(
            {
                "grp": ["a", "a", "b", "c", "c", "c"],
                "v": [1, 2, 3, 4, 5, 6],
            },
            planFactory=SubstraitPlanBuilderFactory(),
        )
        result = df.groupBy("grp").aggregate(total=Sum("v"))
        # Expected sums: a=3, b=3, c=15.
        self.assertEqual(
            self._result_dict(result, "grp", "total"),
            {"a": 3, "b": 3, "c": 15},
        )

    def test_grouped_count_matches_unoptimized(self) -> None:
        df = DataFrame(
            {
                "grp": ["a", "a", "b", "c", "c", "c"],
                "v": [1, 2, 3, 4, 5, 6],
            },
            planFactory=SubstraitPlanBuilderFactory(),
        )
        result = df.groupBy("grp").aggregate(n=Count("v"))
        self.assertEqual(
            self._result_dict(result, "grp", "n"),
            {"a": 2, "b": 1, "c": 3},
        )

    def test_grouped_sum_of_computed_arg_matches_unoptimized(self) -> None:
        # Force an intermediate Project: the Sum's argument is a
        # computed expression (v*v), not a direct field-ref.
        df = DataFrame(
            {
                "grp": ["a", "a", "b"],
                "v": [2.0, 3.0, 4.0],
            },
            planFactory=SubstraitPlanBuilderFactory(),
        )
        result = (
            df.transform(vsq=lambda f: f["v"] * f["v"])
            .groupBy("grp")
            .aggregate(total=Sum("vsq"))
        )
        # Expected: a = 4 + 9 = 13; b = 16.
        self.assertEqual(
            self._result_dict(result, "grp", "total"),
            {"a": 13.0, "b": 16.0},
        )


if __name__ == "__main__":
    unittest.main()
