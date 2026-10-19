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

"""Unit + structural tests for ``RemoveRedundantExchanges``.

The rule elides an outer hash ``ExchangeRel`` whose distribution is
already established by an inner hash ``ExchangeRel`` of the same shape,
with only distribution-preserving rels (Project, Aggregate-with-key-in-
grouping, Filter) in between. The inner exchange is preserved (it does
the real shuffle); the chain between is preserved (it does real work
on the data); only the outer wrapper is dropped.
"""

from __future__ import annotations

import unittest

from substrait.proto import algebra, plan as plan_pb2, type as type_pb2

from ..optimizer.substrait_rules.remove_redundant_exchanges import (
    RemoveRedundantExchanges,
)


def _direct_field(idx: int) -> algebra.Expression:
    return algebra.Expression(
        selection=algebra.Expression.FieldReference(
            direct_reference=algebra.Expression.ReferenceSegment(
                struct_field=algebra.Expression.ReferenceSegment.StructField(
                    field=idx,
                ),
            ),
            root_reference=algebra.Expression.FieldReference.RootReference(),
        )
    )


def _read_rel(names: list[str]) -> algebra.Rel:
    """A minimal ReadRel with named output_names for a base table."""
    return algebra.Rel(
        read=algebra.ReadRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=names),
            ),
            base_schema=type_pb2.NamedStruct(names=names),
        )
    )


def _exchange(
    input_rel: algebra.Rel, key_field: int, partition_count: int = 12
) -> algebra.Rel:
    """Build a hash-by-``key_field`` ExchangeRel."""
    return algebra.Rel(
        exchange=algebra.ExchangeRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(alias=f"hash(field-{key_field})"),
            ),
            input=input_rel,
            partition_count=partition_count,
            scatter_by_fields=algebra.ExchangeRel.ScatterFields(
                fields=[_direct_field(key_field).selection],
            ),
        )
    )


def _project(
    input_rel: algebra.Rel,
    expressions: list[algebra.Expression],
    output_names: list[str],
) -> algebra.Rel:
    return algebra.Rel(
        project=algebra.ProjectRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=output_names),
            ),
            input=input_rel,
            expressions=expressions,
        )
    )


def _wrap_in_root(rel: algebra.Rel, names: list[str]) -> plan_pb2.Plan:
    p = plan_pb2.Plan()
    p.relations.add().root.CopyFrom(algebra.RelRoot(input=rel, names=names))
    return p


def _count_exchanges(rel: algebra.Rel) -> int:
    n = 0
    if rel.WhichOneof("rel_type") == "exchange":
        n += 1
    kind = rel.WhichOneof("rel_type")
    if kind in (
        "filter",
        "project",
        "fetch",
        "aggregate",
        "sort",
        "exchange",
        "expand",
        "write",
        "extension_single",
    ):
        inner = getattr(rel, kind)
        if inner.HasField("input"):
            n += _count_exchanges(inner.input)
    elif kind in ("hash_join", "merge_join", "nested_loop_join", "join", "cross"):
        inner = getattr(rel, kind)
        if inner.HasField("left"):
            n += _count_exchanges(inner.left)
        if inner.HasField("right"):
            n += _count_exchanges(inner.right)
    return n


class TestRemoveRedundantExchanges(unittest.TestCase):
    def test_outer_exchange_over_project_over_inner_exchange_elided(self) -> None:
        # Pattern: Exchange[hash f0] -> Project[passthrough f0] -> Exchange[hash f0] -> read
        # The two exchanges hash by the same field index so the outer is
        # redundant; Project just passes f0 through unchanged.
        read = _read_rel(["k", "v"])
        inner_ex = _exchange(read, key_field=0)
        proj = _project(
            inner_ex,
            [_direct_field(0), _direct_field(1)],
            ["k", "v"],
        )
        outer_ex = _exchange(proj, key_field=0)
        before = _wrap_in_root(outer_ex, ["k", "v"])
        self.assertEqual(_count_exchanges(before.relations[0].root.input), 2)

        after = RemoveRedundantExchanges().apply(before)
        # Outer exchange dropped, project + inner exchange preserved.
        self.assertEqual(_count_exchanges(after.relations[0].root.input), 1)
        # Result root's input is the project chain (the outer exchange's input).
        root = after.relations[0].root.input
        self.assertEqual(root.WhichOneof("rel_type"), "project")
        self.assertEqual(root.project.input.WhichOneof("rel_type"), "exchange")

    def test_unmatched_partition_count_keeps_outer(self) -> None:
        # Outer 12 / inner 4 — partition counts disagree; rule must NOT
        # elide (the outer's downstream consumer expects 12 buckets, the
        # inner gives 4).
        read = _read_rel(["k", "v"])
        inner_ex = _exchange(read, key_field=0, partition_count=4)
        proj = _project(
            inner_ex,
            [_direct_field(0), _direct_field(1)],
            ["k", "v"],
        )
        outer_ex = _exchange(proj, key_field=0, partition_count=12)
        before = _wrap_in_root(outer_ex, ["k", "v"])

        after = RemoveRedundantExchanges().apply(before)
        # Both exchanges remain.
        self.assertEqual(_count_exchanges(after.relations[0].root.input), 2)

    def test_unmatched_key_keeps_outer(self) -> None:
        # Outer hashes by field 1, inner by field 0 — different keys;
        # the outer is doing real work, must not elide.
        read = _read_rel(["k", "v"])
        inner_ex = _exchange(read, key_field=0)
        proj = _project(
            inner_ex,
            [_direct_field(0), _direct_field(1)],
            ["k", "v"],
        )
        outer_ex = _exchange(proj, key_field=1)
        before = _wrap_in_root(outer_ex, ["k", "v"])

        after = RemoveRedundantExchanges().apply(before)
        self.assertEqual(_count_exchanges(after.relations[0].root.input), 2)

    def test_no_inner_exchange_leaves_outer_alone(self) -> None:
        # Just one exchange in the plan — nothing to elide against.
        read = _read_rel(["k", "v"])
        outer_ex = _exchange(read, key_field=0)
        before = _wrap_in_root(outer_ex, ["k", "v"])

        after = RemoveRedundantExchanges().apply(before)
        self.assertEqual(_count_exchanges(after.relations[0].root.input), 1)

    def test_idempotent(self) -> None:
        # Apply twice; second pass is a no-op (output byte-equal).
        read = _read_rel(["k", "v"])
        inner_ex = _exchange(read, key_field=0)
        proj = _project(
            inner_ex,
            [_direct_field(0), _direct_field(1)],
            ["k", "v"],
        )
        outer_ex = _exchange(proj, key_field=0)
        before = _wrap_in_root(outer_ex, ["k", "v"])

        rule = RemoveRedundantExchanges()
        once = rule.apply(before)
        twice = rule.apply(once)
        self.assertEqual(once.SerializeToString(), twice.SerializeToString())

    def test_project_with_computed_expression_keeps_outer(self) -> None:
        # Project's f0 column is f0+f1 (computed) — the chain doesn't
        # provably pass the key column through, so the outer must stay.
        from substrait.proto import algebra as alg

        read = _read_rel(["k", "v"])
        inner_ex = _exchange(read, key_field=0)
        # Build a fake scalar function expression (the function ref doesn't
        # need to resolve — the rule bails on any non-direct-field-ref).
        computed = alg.Expression(
            scalar_function=alg.Expression.ScalarFunction(
                function_reference=42,
                arguments=[
                    alg.FunctionArgument(value=_direct_field(0)),
                    alg.FunctionArgument(value=_direct_field(1)),
                ],
            )
        )
        proj = _project(inner_ex, [computed, _direct_field(1)], ["k", "v"])
        outer_ex = _exchange(proj, key_field=0)
        before = _wrap_in_root(outer_ex, ["k", "v"])

        after = RemoveRedundantExchanges().apply(before)
        # Both exchanges remain.
        self.assertEqual(_count_exchanges(after.relations[0].root.input), 2)


if __name__ == "__main__":
    unittest.main()
