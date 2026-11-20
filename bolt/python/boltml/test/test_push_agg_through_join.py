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

"""Unit tests for ``PushAggThroughJoin`` (AggregateJoinTranspose).

The rule pushes a partial aggregation below an inner HashJoin, so the
post-join rows count is reduced before the join shuffle. The full
end-to-end behaviour is exercised by TPC-H q3/q5/q9. These tests
exercise the boundary conditions that those queries do not light up
individually.
"""

from __future__ import annotations

import unittest

from substrait.proto import (
    algebra,
    extensions as ext_pb2,
    plan as plan_pb2,
    type as type_pb2,
)

from ..optimizer.substrait_rules.push_agg_through_join import (
    PushAggThroughJoin,
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
    return algebra.Rel(
        read=algebra.ReadRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=names),
            ),
            base_schema=type_pb2.NamedStruct(names=names),
        )
    )


def _wrap_in_root(
    rel: algebra.Rel,
    names: list[str],
    extensions: list[ext_pb2.SimpleExtensionDeclaration] | None = None,
) -> plan_pb2.Plan:
    p = plan_pb2.Plan()
    p.relations.add().root.CopyFrom(algebra.RelRoot(input=rel, names=names))
    for d in extensions or []:
        p.extensions.add().CopyFrom(d)
    return p


def _ext_decl(anchor: int, name: str) -> ext_pb2.SimpleExtensionDeclaration:
    return ext_pb2.SimpleExtensionDeclaration(
        extension_function=ext_pb2.SimpleExtensionDeclaration.ExtensionFunction(
            function_anchor=anchor,
            name=name,
        ),
    )


def _global_agg_over(input_rel: algebra.Rel, output_names: list[str]) -> algebra.Rel:
    """Build a single-measure global aggregate (no grouping)."""
    return algebra.Rel(
        aggregate=algebra.AggregateRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=output_names),
            ),
            input=input_rel,
            measures=[
                algebra.AggregateRel.Measure(
                    measure=algebra.AggregateFunction(
                        function_reference=1,
                        arguments=[
                            algebra.FunctionArgument(value=_direct_field(0)),
                        ],
                        output_type=type_pb2.Type(i64=type_pb2.Type.I64()),
                        phase=algebra.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT,
                    ),
                ),
            ],
        )
    )


def _hash_join(
    left: algebra.Rel,
    right: algebra.Rel,
    output_names: list[str],
    join_type: int = algebra.HashJoinRel.JoinType.JOIN_TYPE_INNER,
) -> algebra.Rel:
    return algebra.Rel(
        hash_join=algebra.HashJoinRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=output_names),
            ),
            left=left,
            right=right,
            keys=[
                algebra.ComparisonJoinKey(
                    left=_direct_field(0).selection,
                    right=_direct_field(0).selection,
                    comparison=algebra.ComparisonJoinKey.ComparisonType(
                        simple=algebra.ComparisonJoinKey.SIMPLE_COMPARISON_TYPE_EQ,
                    ),
                ),
            ],
            type=join_type,
        )
    )


class TestPushAggThroughJoin(unittest.TestCase):
    def test_purity_input_not_mutated(self) -> None:
        # Global agg over a join over two reads -- the rule's gate should
        # bail (no grouping_expressions) but it must not mutate input.
        left = _read_rel(["a_k", "a_v"])
        right = _read_rel(["b_k", "b_v"])
        join = _hash_join(left, right, ["a_k", "a_v", "b_k", "b_v"])
        agg = _global_agg_over(join, ["s"])
        before = _wrap_in_root(agg, ["s"], extensions=[_ext_decl(1, "sum")])
        snapshot = before.SerializeToString()
        PushAggThroughJoin().apply(before)
        self.assertEqual(before.SerializeToString(), snapshot)

    def test_idempotence_on_global_agg(self) -> None:
        left = _read_rel(["a_k", "a_v"])
        right = _read_rel(["b_k", "b_v"])
        join = _hash_join(left, right, ["a_k", "a_v", "b_k", "b_v"])
        agg = _global_agg_over(join, ["s"])
        before = _wrap_in_root(agg, ["s"], extensions=[_ext_decl(1, "sum")])
        rule = PushAggThroughJoin()
        once = rule.apply(before)
        twice = rule.apply(once)
        self.assertEqual(once.SerializeToString(), twice.SerializeToString())

    def test_global_agg_skipped(self) -> None:
        # An aggregate with no grouping_expressions -- the rule is a no-op
        # by design (a global aggregate has no grouping keys to push
        # through the join, so it stays single-phase).
        left = _read_rel(["a_k", "a_v"])
        right = _read_rel(["b_k", "b_v"])
        join = _hash_join(left, right, ["a_k", "a_v", "b_k", "b_v"])
        agg = _global_agg_over(join, ["s"])
        before = _wrap_in_root(agg, ["s"], extensions=[_ext_decl(1, "sum")])
        snapshot = before.SerializeToString()
        after = PushAggThroughJoin().apply(before)
        self.assertEqual(after.SerializeToString(), snapshot)

    def test_non_inner_join_skipped(self) -> None:
        # LEFT join below grouped agg -- rule's match requires
        # JOIN_TYPE_INNER. Anything else stays put.
        left = _read_rel(["a_k", "a_v"])
        right = _read_rel(["b_k", "b_v"])
        join = _hash_join(
            left,
            right,
            ["a_k", "a_v", "b_k", "b_v"],
            join_type=algebra.HashJoinRel.JoinType.JOIN_TYPE_LEFT,
        )
        # Grouped aggregate (with grouping_expressions) directly above
        # the LEFT join. The rule's gate should refuse the rewrite.
        grouped_agg = algebra.Rel(
            aggregate=algebra.AggregateRel(
                common=algebra.RelCommon(
                    direct=algebra.RelCommon.Direct(),
                    hint=algebra.RelCommon.Hint(output_names=["a_k", "s"]),
                ),
                input=join,
                grouping_expressions=[_direct_field(0)],
                groupings=[
                    algebra.AggregateRel.Grouping(
                        expression_references=[0],
                    ),
                ],
                measures=[
                    algebra.AggregateRel.Measure(
                        measure=algebra.AggregateFunction(
                            function_reference=1,
                            arguments=[
                                algebra.FunctionArgument(value=_direct_field(1)),
                            ],
                            output_type=type_pb2.Type(i64=type_pb2.Type.I64()),
                            phase=algebra.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT,
                        ),
                    ),
                ],
            )
        )
        before = _wrap_in_root(
            grouped_agg, ["a_k", "s"], extensions=[_ext_decl(1, "sum")]
        )
        snapshot = before.SerializeToString()
        after = PushAggThroughJoin().apply(before)
        self.assertEqual(after.SerializeToString(), snapshot)


if __name__ == "__main__":
    unittest.main()
