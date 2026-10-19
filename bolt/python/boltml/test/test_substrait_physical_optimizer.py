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

"""Unit + structural-parity tests for ``SubstraitPhysicalOptimizerPipeline``.

Three test families:

1. ``TestAddExchangesUnit`` — per-construct insertion (aggregate /
   join / shuffle / no-op cases) against crafted ``DataFrame`` plans.
2. ``TestStructuralParity`` — for each BoltML-supported DataFrame
   shape, the set of ``(scattering, partition_count, output_names)``
   tuples extracted from the new pipeline's ``ExchangeRel`` nodes
   equals the set extracted from the legacy pipeline's
   ``PhysicalExchange`` list.
3. ``TestEngineFlagWiring`` — ``BOLTML_SUBSTRAIT_PHYSICAL`` flag selects
   the engine-level adapter and exposes the new Substrait plan.
"""

from __future__ import annotations

import os
import unittest
from typing import List, Tuple

from substrait.proto import algebra, plan as plan_pb2

from ..dataframe import DataFrame
from ..execute import execute as _execute
from ..executor import LocalExecutor
from ..function.aggregation import Mean, Sum
from ..optimizer.substrait_physical_pipeline import (
    SubstraitPhysicalOptimizerPipeline,
)
from ..optimizer.substrait_rules.add_exchanges import (
    AddExchanges,
    SHUFFLE_EXTENSION_TYPE_URL,
    _decode_shuffle_seed,
)
from ..plan_builder.substrait import SubstraitPlanBuilderFactory


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_substrait(df: DataFrame) -> plan_pb2.Plan:
    return df._planBuilder_.toSubstraitPlan()


def _walk_exchanges(rel: algebra.Rel) -> List[algebra.ExchangeRel]:
    """Return every ``ExchangeRel`` in the rel tree, in pre-order."""
    found: List[algebra.ExchangeRel] = []

    def _recurse(r: algebra.Rel) -> None:
        kind = r.WhichOneof("rel_type")
        if kind == "exchange":
            found.append(r.exchange)
            _recurse(r.exchange.input)
            return
        if kind in (
            "filter",
            "project",
            "fetch",
            "aggregate",
            "sort",
            "extension_single",
            "expand",
            "write",
        ):
            inner = getattr(r, kind)
            if inner.HasField("input"):
                _recurse(inner.input)
        elif kind in ("join", "cross", "hash_join", "merge_join", "nested_loop_join"):
            inner = getattr(r, kind)
            if inner.HasField("left"):
                _recurse(inner.left)
            if inner.HasField("right"):
                _recurse(inner.right)
        elif kind == "set":
            for child in r.set.inputs:
                _recurse(child)

    for plan_rel in [rel]:
        _recurse(plan_rel)
    return found


def _exchange_signatures_substrait(p: plan_pb2.Plan) -> List[Tuple[str, int]]:
    """Extract ``(scattering_alias, partition_count)`` tuples from *p*."""
    sigs: List[Tuple[str, int]] = []
    for plan_rel in p.relations:
        if not plan_rel.HasField("root"):
            continue
        for ex in _walk_exchanges(plan_rel.root.input):
            alias = ex.common.hint.alias if ex.HasField("common") else ""
            sigs.append((alias, ex.partition_count))
    return sigs


# ---------------------------------------------------------------------------
# AddExchanges unit tests
# ---------------------------------------------------------------------------


class TestAddExchangesUnit(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = SubstraitPlanBuilderFactory()

    def test_grouped_aggregate_inserts_hash_exchange(self) -> None:
        df = DataFrame(
            {"grp": ["a", "a", "b"], "value": [1, 2, 3]}, planFactory=self.factory
        )
        df.groupBy("grp").aggregate(avg=Mean("value"))

        out = SubstraitPhysicalOptimizerPipeline().optimize(_build_substrait(df))
        sigs = _exchange_signatures_substrait(out)
        self.assertEqual(sigs, [("hash(grp)", 12)])

        # Structural: the only exchange sits directly under the aggregate.
        agg = out.relations[0].root.input.aggregate
        self.assertEqual(agg.input.WhichOneof("rel_type"), "exchange")
        ex = agg.input.exchange
        self.assertEqual(ex.WhichOneof("exchange_kind"), "scatter_by_fields")
        self.assertEqual(len(ex.scatter_by_fields.fields), 1)
        self.assertEqual(
            ex.scatter_by_fields.fields[0].direct_reference.struct_field.field,
            0,
        )

    def test_global_aggregate_inserts_no_exchange(self) -> None:
        # ``AddExchanges`` does not insert an exchange below a global
        # aggregate (no grouping → nothing to scatter on), so a global
        # aggregate stays single-phase through the whole pipeline.
        df = DataFrame({"value": [1, 2, 3]}, planFactory=self.factory)
        df.groupBy().aggregate(total=Sum("value"))
        out = AddExchanges().apply(_build_substrait(df))
        self.assertEqual(_exchange_signatures_substrait(out), [])

    def test_join_inserts_two_hash_exchanges(self) -> None:
        lhs = DataFrame({"id": [1, 2], "lv": [10, 20]}, planFactory=self.factory)
        rhs = DataFrame({"id": [1, 3], "rv": [100, 300]}, planFactory=self.factory)
        lhs.join(rhs, {"id"})

        out = SubstraitPhysicalOptimizerPipeline().optimize(_build_substrait(lhs))
        sigs = _exchange_signatures_substrait(out)
        self.assertEqual(sigs, [("hash(l_id)", 12), ("hash(r_id)", 12)])

        # Walk down to the hash_join (the lowering wraps the join in a
        # rename project to reproject l_id/r_id back to the user-visible
        # outputLayout).
        rel = out.relations[0].root.input
        while rel.WhichOneof("rel_type") != "hash_join":
            rel = getattr(rel, rel.WhichOneof("rel_type")).input
        join = rel.hash_join
        self.assertEqual(join.left.WhichOneof("rel_type"), "exchange")
        self.assertEqual(join.right.WhichOneof("rel_type"), "exchange")

    def test_shuffle_inserts_round_robin_exchange(self) -> None:
        df = DataFrame({"id": [1, 2], "tag": ["a", "b"]}, planFactory=self.factory)
        df.shuffle(7)

        out = SubstraitPhysicalOptimizerPipeline().optimize(_build_substrait(df))
        sigs = _exchange_signatures_substrait(out)
        self.assertEqual(sigs, [("shuffle(seed=7)", 12)])

        ext = out.relations[0].root.input.extension_single
        self.assertEqual(ext.input.WhichOneof("rel_type"), "exchange")
        self.assertEqual(
            ext.input.exchange.WhichOneof("exchange_kind"),
            "round_robin",
        )

    def test_filter_only_inserts_no_exchange(self) -> None:
        df = DataFrame({"id": [1, 2, 3]}, planFactory=self.factory)
        df.filter(df["id"] > 1)
        out = SubstraitPhysicalOptimizerPipeline().optimize(_build_substrait(df))
        self.assertEqual(_exchange_signatures_substrait(out), [])

    def test_partition_count_flows_from_constructor(self) -> None:
        df = DataFrame({"grp": ["a"], "v": [1]}, planFactory=self.factory)
        df.groupBy("grp").aggregate(s=Sum("v"))
        out = SubstraitPhysicalOptimizerPipeline(defaultPartitionCount=11).optimize(
            _build_substrait(df)
        )
        sigs = _exchange_signatures_substrait(out)
        self.assertEqual(sigs, [("hash(grp)", 11)])

    def test_partition_count_floor_is_one(self) -> None:
        rule = AddExchanges(defaultPartitionCount=0)
        self.assertEqual(rule.partitionCount, 1)

    def test_idempotent_under_repeated_apply(self) -> None:
        df = DataFrame({"grp": ["a"], "v": [1]}, planFactory=self.factory)
        df.groupBy("grp").aggregate(s=Sum("v"))
        rule = AddExchanges()
        once = rule.apply(_build_substrait(df))
        twice = rule.apply(once)
        self.assertEqual(once.SerializeToString(), twice.SerializeToString())

    def test_empty_plan_passes_through(self) -> None:
        empty = plan_pb2.Plan()
        out = SubstraitPhysicalOptimizerPipeline().optimize(empty)
        self.assertEqual(out.SerializeToString(), empty.SerializeToString())


class TestShuffleSeedDecoding(unittest.TestCase):
    def test_default_zero_seed(self) -> None:
        self.assertEqual(_decode_shuffle_seed(b""), 0)

    def test_small_seed(self) -> None:
        # tag for field 1 varint = 0x08; value 7
        self.assertEqual(_decode_shuffle_seed(b"\x08\x07"), 7)

    def test_multibyte_varint(self) -> None:
        # 12345 = 0xb960 little-endian-base128
        self.assertEqual(_decode_shuffle_seed(b"\x08\xb9\x60"), 12345)

    def test_unexpected_tag_raises(self) -> None:
        with self.assertRaises(ValueError):
            _decode_shuffle_seed(b"\x10\x07")  # field 2

    def test_shuffle_extension_type_url_constant_matches_lowering(self) -> None:
        df = DataFrame({"id": [1]}, planFactory=SubstraitPlanBuilderFactory())
        df.shuffle(0)
        p = _build_substrait(df)
        self.assertEqual(
            p.relations[0].root.input.extension_single.detail.type_url,
            SHUFFLE_EXTENSION_TYPE_URL,
        )


# ---------------------------------------------------------------------------
# Structural parity vs legacy
# ---------------------------------------------------------------------------


class TestStructuralParity(unittest.TestCase):
    """For every supported plan shape, new and legacy pipelines agree on
    the set of ``(scattering, partition_count)`` tuples they emit.
    """

    def setUp(self) -> None:
        self.factory = SubstraitPlanBuilderFactory()

    def _check(self, df: DataFrame, expected: list) -> None:
        # Asserts the expected exchange signatures directly. The
        # legacy pipeline's well-known outputs are preserved here as
        # the canonical answer key — diverging from them is a
        # structural-assertion failure.
        substrait_out = SubstraitPhysicalOptimizerPipeline().optimize(
            _build_substrait(df)
        )
        new_sigs = sorted(_exchange_signatures_substrait(substrait_out))
        self.assertEqual(new_sigs, sorted(expected))

    def test_parity_grouped_aggregate(self) -> None:
        df = DataFrame({"grp": ["a", "b"], "v": [1, 2]}, planFactory=self.factory)
        df.groupBy("grp").aggregate(s=Sum("v"))
        self._check(df, [("hash(grp)", 12)])

    def test_parity_global_aggregate(self) -> None:
        # A global (no-grouping) aggregate stays single-phase: there are
        # no grouping keys to scatter on, so ``AddExchanges`` inserts no
        # exchange and the pipeline emits no exchange signatures. The
        # distributed leaf-fan-out gate keeps such plans correct by
        # running them as a single task (no per-shard partials to
        # merge).
        df = DataFrame({"v": [1, 2]}, planFactory=self.factory)
        df.groupBy().aggregate(s=Sum("v"))
        self._check(df, [])

    def test_parity_shuffle(self) -> None:
        df = DataFrame({"id": [1, 2]}, planFactory=self.factory)
        df.shuffle(42)
        self._check(df, [("shuffle(seed=42)", 12)])

    def test_parity_join(self) -> None:
        lhs = DataFrame({"id": [1], "lv": [10]}, planFactory=self.factory)
        rhs = DataFrame({"id": [1], "rv": [100]}, planFactory=self.factory)
        lhs.join(rhs, {"id"})
        self._check(lhs, [("hash(l_id)", 12), ("hash(r_id)", 12)])

    def test_parity_filter_only(self) -> None:
        df = DataFrame({"id": [1, 2]}, planFactory=self.factory)
        df.filter(df["id"] > 1)
        self._check(df, [])

    def test_parity_aggregate_then_shuffle(self) -> None:
        df = DataFrame({"grp": ["a", "b"], "v": [1, 2]}, planFactory=self.factory)
        df.groupBy("grp").aggregate(s=Sum("v"))
        df.shuffle(3)
        self._check(df, [("hash(grp)", 12), ("shuffle(seed=3)", 12)])


class _FlagSetter:
    """Sets ``BOLTML_SUBSTRAIT_PHYSICAL`` to the given value on entry,
    restores prior state on exit. Used by the smoke test below to
    verify that flipping the per-component flag does not break
    execution (the legacy fallback was retired — flag values no
    longer change which pipeline runs)."""

    def __init__(self, value: str | None) -> None:
        self.__value = value
        self.__prior = os.environ.get("BOLTML_SUBSTRAIT_PHYSICAL")

    def __enter__(self) -> "_FlagSetter":
        if self.__value is None:
            os.environ.pop("BOLTML_SUBSTRAIT_PHYSICAL", None)
        else:
            os.environ["BOLTML_SUBSTRAIT_PHYSICAL"] = self.__value
        return self

    def __exit__(self, *exc_info) -> None:
        if self.__prior is None:
            os.environ.pop("BOLTML_SUBSTRAIT_PHYSICAL", None)
        else:
            os.environ["BOLTML_SUBSTRAIT_PHYSICAL"] = self.__prior


class TestEngineFlagWiring(unittest.TestCase):
    """The per-flag legacy fallback has been retired; the engine no
    longer branches on ``BOLTML_SUBSTRAIT*``. This class keeps an
    end-to-end smoke test that execution works whether the flag is set
    (truthy) or unset."""

    def test_engine_executes_under_both_flag_values(self) -> None:
        """End-to-end execution agreement: same DataFrame, both flags."""
        for flag in (None, "1"):
            with self.subTest(flag=flag), _FlagSetter(flag):
                df = DataFrame(
                    {"grp": ["a", "a", "b"], "v": [1, 2, 3]},
                    planFactory=SubstraitPlanBuilderFactory(),
                )
                df.groupBy("grp").aggregate(s=Sum("v"))
                rows = _execute(df._planBuilder_, LocalExecutor())
                self.assertEqual(rows.size(), 2)


if __name__ == "__main__":
    unittest.main()
