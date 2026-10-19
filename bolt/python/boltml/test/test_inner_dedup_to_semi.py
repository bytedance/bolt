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

"""Unit tests for ``InnerDedupToSemi``.

The rule rewrites an inner-join + dedup-groupby (group by all the LHS
keys, no measures) into a proper semi-join. End-to-end this is what
makes TPC-H q21 fast — the EXISTS sub-pattern compiles to inner+dedup
and the rewrite gives the C++ engine a SemiJoin to execute directly
instead of materialising the dedup intermediate.
"""

from __future__ import annotations

import unittest

from substrait.proto import (
    algebra,
    extensions as ext_pb2,
    plan as plan_pb2,
    type as type_pb2,
)

from ..optimizer.substrait_rules.inner_dedup_to_semi import InnerDedupToSemi


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


def _dedup_agg(
    input_rel: algebra.Rel,
    group_field: int,
    output_names: list[str],
) -> algebra.Rel:
    """Build a dedup aggregate (group by, no measures) over ``input_rel``."""
    return algebra.Rel(
        aggregate=algebra.AggregateRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=output_names),
            ),
            input=input_rel,
            grouping_expressions=[_direct_field(group_field)],
            groupings=[
                algebra.AggregateRel.Grouping(
                    expression_references=[0],
                ),
            ],
        )
    )


def _wrap_in_root(rel: algebra.Rel, names: list[str]) -> plan_pb2.Plan:
    p = plan_pb2.Plan()
    p.relations.add().root.CopyFrom(algebra.RelRoot(input=rel, names=names))
    return p


class TestInnerDedupToSemi(unittest.TestCase):
    def test_purity_input_not_mutated(self) -> None:
        left = _read_rel(["a_k", "a_v"])
        right = _read_rel(["b_k", "b_v"])
        join = _hash_join(left, right, ["a_k", "a_v", "b_k", "b_v"])
        # Group by ``a_k`` (field 0) — that's an LHS-only key, the
        # canonical dedup pattern.
        agg = _dedup_agg(join, group_field=0, output_names=["a_k"])
        before = _wrap_in_root(agg, ["a_k"])
        snapshot = before.SerializeToString()
        InnerDedupToSemi().apply(before)
        self.assertEqual(before.SerializeToString(), snapshot)

    def test_idempotence(self) -> None:
        left = _read_rel(["a_k", "a_v"])
        right = _read_rel(["b_k", "b_v"])
        join = _hash_join(left, right, ["a_k", "a_v", "b_k", "b_v"])
        agg = _dedup_agg(join, group_field=0, output_names=["a_k"])
        before = _wrap_in_root(agg, ["a_k"])
        rule = InnerDedupToSemi()
        once = rule.apply(before)
        twice = rule.apply(once)
        self.assertEqual(once.SerializeToString(), twice.SerializeToString())

    def test_global_aggregate_skipped(self) -> None:
        # No grouping_expressions — the rule explicitly bails (a global
        # aggregate has no grouping keys, so there's nothing to dedup).
        left = _read_rel(["a_k", "a_v"])
        right = _read_rel(["b_k", "b_v"])
        join = _hash_join(left, right, ["a_k", "a_v", "b_k", "b_v"])
        # Aggregate with no grouping_expressions and one measure.
        agg = algebra.Rel(
            aggregate=algebra.AggregateRel(
                common=algebra.RelCommon(
                    direct=algebra.RelCommon.Direct(),
                    hint=algebra.RelCommon.Hint(output_names=["s"]),
                ),
                input=join,
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
        ext_decl = ext_pb2.SimpleExtensionDeclaration(
            extension_function=ext_pb2.SimpleExtensionDeclaration.ExtensionFunction(
                function_anchor=1,
                name="sum",
            ),
        )
        before = _wrap_in_root(agg, ["s"])
        before.extensions.add().CopyFrom(ext_decl)
        snapshot = before.SerializeToString()
        after = InnerDedupToSemi().apply(before)
        self.assertEqual(after.SerializeToString(), snapshot)

    def test_aggregate_with_measures_skipped(self) -> None:
        # Group by a_k WITH a measure (Sum) — this is no longer a "pure
        # dedup", so the rule must NOT rewrite to a semi-join. The
        # measure would be lost.
        left = _read_rel(["a_k", "a_v"])
        right = _read_rel(["b_k", "b_v"])
        join = _hash_join(left, right, ["a_k", "a_v", "b_k", "b_v"])
        agg = algebra.Rel(
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
        ext_decl = ext_pb2.SimpleExtensionDeclaration(
            extension_function=ext_pb2.SimpleExtensionDeclaration.ExtensionFunction(
                function_anchor=1,
                name="sum",
            ),
        )
        before = _wrap_in_root(agg, ["a_k", "s"])
        before.extensions.add().CopyFrom(ext_decl)
        snapshot = before.SerializeToString()
        after = InnerDedupToSemi().apply(before)
        self.assertEqual(after.SerializeToString(), snapshot)


if __name__ == "__main__":
    unittest.main()
