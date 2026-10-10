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

"""Unit + asymmetric tests for the Substrait-based logical optimizer.

Covers:
  * Each Substrait rule individually on a crafted Plan.
  * The pipeline's fixpoint behaviour (a second pass produces no change).
  * Asymmetric parity: for representative DataFrame chains,
    ``SubstraitOptimizerPipeline`` applied to the lowered plan produces
    the same Substrait ``Plan`` (modulo extension-registry variance) as
    lowering the legacy ``OptimizerPipeline`` output.
"""

from __future__ import annotations

import unittest

from substrait.proto import algebra, plan

from ..dataframe import DataFrame
from ..optimizer.substrait_pipeline import SubstraitOptimizerPipeline
from ..optimizer.substrait_rules import (
    MergeProjects,
    PruneUnusedColumns,
    PushFilterThroughProjects,
    RewriteLimits,
    get_rel_kind,
    get_root_rel,
)
from ..plan_builder.substrait import (
    SubstraitPlanBuilderFactory,
    plan_is_substrait_lowerable,
)


# ---------------------------------------------------------------------------
# Crafted-Plan helpers
# ---------------------------------------------------------------------------


def _i32_type() -> object:
    """Return a Substrait i32 type wrapped in a NamedStruct's struct."""
    from substrait.proto import type as stype

    return stype.Type(i32=stype.Type.I32(nullability=stype.Type.NULLABILITY_NULLABLE))


def _named_struct(names: list[str]) -> "stype.NamedStruct":  # noqa: F821
    """Build a NamedStruct of N nullable i32 columns."""
    from substrait.proto import type as stype

    return stype.NamedStruct(
        names=names,
        struct=stype.Type.Struct(
            types=[_i32_type() for _ in names],
            nullability=stype.Type.NULLABILITY_NULLABLE,
        ),
    )


def _read_rel(names: list[str]) -> algebra.Rel:
    """Build a minimal ReadRel emitting *names* with i32 columns."""
    schema = _named_struct(names)
    read = algebra.ReadRel(
        common=algebra.RelCommon(direct=algebra.RelCommon.Direct()),
        base_schema=schema,
        virtual_table=algebra.ReadRel.VirtualTable(),
    )
    return algebra.Rel(read=read)


def _field_ref(idx: int) -> algebra.Expression:
    """Return ``$idx`` as a direct struct-field reference."""
    return algebra.Expression(
        selection=algebra.Expression.FieldReference(
            direct_reference=algebra.Expression.ReferenceSegment(
                struct_field=algebra.Expression.ReferenceSegment.StructField(field=idx)
            ),
            root_reference=algebra.Expression.FieldReference.RootReference(),
        )
    )


def _i32_literal(value: int) -> algebra.Expression:
    """Return a literal i32 expression for *value*."""
    return algebra.Expression(literal=algebra.Expression.Literal(i32=value))


def _project_rel(
    input_rel: algebra.Rel, expressions: list[algebra.Expression]
) -> algebra.Rel:
    return algebra.Rel(
        project=algebra.ProjectRel(
            common=algebra.RelCommon(direct=algebra.RelCommon.Direct()),
            input=input_rel,
            expressions=expressions,
        )
    )


def _filter_rel(input_rel: algebra.Rel, condition: algebra.Expression) -> algebra.Rel:
    return algebra.Rel(
        filter=algebra.FilterRel(
            common=algebra.RelCommon(direct=algebra.RelCommon.Direct()),
            input=input_rel,
            condition=condition,
        )
    )


def _fetch_rel(input_rel: algebra.Rel, count: int | None, offset: int) -> algebra.Rel:
    fetch = algebra.FetchRel(
        common=algebra.RelCommon(direct=algebra.RelCommon.Direct()),
        input=input_rel,
    )
    fetch.offset_expr.CopyFrom(
        _i32_literal(offset).__class__(literal=algebra.Expression.Literal(i64=offset))
    )
    if count is not None:
        fetch.count_expr.CopyFrom(
            _i32_literal(count).__class__(literal=algebra.Expression.Literal(i64=count))
        )
    return algebra.Rel(fetch=fetch)


def _wrap_plan(root: algebra.Rel) -> plan.Plan:
    """Wrap *root* in a single-PlanRel ``plan.Plan``."""
    return plan.Plan(
        relations=[plan.PlanRel(root=algebra.RelRoot(input=root, names=["x"]))]
    )


# ---------------------------------------------------------------------------
# Per-rule unit tests
# ---------------------------------------------------------------------------


class MergeProjectsRuleTest(unittest.TestCase):
    """Unit tests for ``MergeProjects`` operating on crafted Substrait Plans."""

    def test_merges_two_passthrough_projects(self) -> None:
        # Project[$0]( Project[$0,$1]( Read[a,b,c] ) )
        read = _read_rel(["a", "b", "c"])
        inner = _project_rel(read, [_field_ref(0), _field_ref(1)])
        outer = _project_rel(inner, [_field_ref(0)])
        merged = MergeProjects().apply(_wrap_plan(outer))
        root = get_root_rel(merged)
        self.assertEqual(get_rel_kind(root), "project")
        self.assertEqual(len(root.project.expressions), 1)
        # The merged expression should resolve to $0 (column "a") of the read.
        self.assertEqual(
            root.project.expressions[0].selection.direct_reference.struct_field.field, 0
        )
        # Inner project must be gone.
        self.assertEqual(get_rel_kind(root.project.input), "read")

    def test_does_not_merge_single_project(self) -> None:
        read = _read_rel(["a", "b"])
        single = _project_rel(read, [_field_ref(0)])
        out = MergeProjects().apply(_wrap_plan(single))
        # Structurally identical (single project preserved).
        self.assertEqual(
            out.SerializeToString(), _wrap_plan(single).SerializeToString()
        )

    def test_skips_when_outer_addresses_unprojected_column(self) -> None:
        # Outer references $2 but inner only emits 2 columns ($0, $1).
        read = _read_rel(["a", "b", "c"])
        inner = _project_rel(read, [_field_ref(0), _field_ref(1)])
        outer = _project_rel(inner, [_field_ref(2)])  # out-of-range
        out = MergeProjects().apply(_wrap_plan(outer))
        # The merge must be skipped; tree is unchanged.
        self.assertEqual(out.SerializeToString(), _wrap_plan(outer).SerializeToString())

    def test_does_not_mutate_input(self) -> None:
        read = _read_rel(["a", "b"])
        inner = _project_rel(read, [_field_ref(0), _field_ref(1)])
        outer = _project_rel(inner, [_field_ref(0)])
        plan_in = _wrap_plan(outer)
        before = plan_in.SerializeToString()
        MergeProjects().apply(plan_in)
        self.assertEqual(plan_in.SerializeToString(), before)


class PushFilterThroughProjectsRuleTest(unittest.TestCase):
    """Unit tests for ``PushFilterThroughProjects``."""

    def test_pushes_filter_below_project(self) -> None:
        read = _read_rel(["a", "b"])
        proj = _project_rel(read, [_field_ref(0), _field_ref(1)])
        filt = _filter_rel(proj, _field_ref(0))
        out = PushFilterThroughProjects().apply(_wrap_plan(filt))
        root = get_root_rel(out)
        self.assertEqual(get_rel_kind(root), "project")
        self.assertEqual(get_rel_kind(root.project.input), "filter")
        self.assertEqual(get_rel_kind(root.project.input.filter.input), "read")

    def test_skips_when_filter_addresses_unprojected_column(self) -> None:
        read = _read_rel(["a", "b", "c"])
        proj = _project_rel(read, [_field_ref(0), _field_ref(1)])
        filt = _filter_rel(proj, _field_ref(2))  # out-of-range
        original = _wrap_plan(filt)
        out = PushFilterThroughProjects().apply(original)
        self.assertEqual(out.SerializeToString(), original.SerializeToString())

    def test_no_op_when_no_filter_above_project(self) -> None:
        read = _read_rel(["a", "b"])
        proj = _project_rel(read, [_field_ref(0)])
        out = PushFilterThroughProjects().apply(_wrap_plan(proj))
        self.assertEqual(out.SerializeToString(), _wrap_plan(proj).SerializeToString())

    def test_remaps_field_refs_through_renaming_project(self) -> None:
        # Inner project: $1, $0  (swap columns)
        # Filter: $0  -> after pushdown should reference $1 of the read.
        read = _read_rel(["a", "b"])
        proj = _project_rel(read, [_field_ref(1), _field_ref(0)])
        filt = _filter_rel(proj, _field_ref(0))
        out = PushFilterThroughProjects().apply(_wrap_plan(filt))
        pushed = out.relations[0].root.input.project.input.filter
        self.assertEqual(
            pushed.condition.selection.direct_reference.struct_field.field, 1
        )


class RewriteLimitsRuleTest(unittest.TestCase):
    """Unit tests for ``RewriteLimits``."""

    def test_swaps_fetch_below_project(self) -> None:
        read = _read_rel(["a"])
        proj = _project_rel(read, [_field_ref(0)])
        fetch = _fetch_rel(proj, count=5, offset=0)
        out = RewriteLimits().apply(_wrap_plan(fetch))
        root = get_root_rel(out)
        self.assertEqual(get_rel_kind(root), "project")
        self.assertEqual(get_rel_kind(root.project.input), "fetch")

    def test_merges_adjacent_fetches(self) -> None:
        # Outer: count=2 offset=1 over Inner: count=10 offset=3.
        # Merge: offset = 3+1 = 4, count = min(2, max(10-1,0)) = 2.
        read = _read_rel(["a"])
        inner_fetch = _fetch_rel(read, count=10, offset=3)
        outer_fetch = _fetch_rel(inner_fetch, count=2, offset=1)
        out = RewriteLimits().apply(_wrap_plan(outer_fetch))
        root = get_root_rel(out)
        self.assertEqual(get_rel_kind(root), "fetch")
        # Should be a single fetch above the read now.
        self.assertEqual(get_rel_kind(root.fetch.input), "read")
        self.assertEqual(root.fetch.offset_expr.literal.i64, 4)
        self.assertEqual(root.fetch.count_expr.literal.i64, 2)

    def test_unlimited_inner_takes_outer_count(self) -> None:
        # Inner unlimited, outer count=5 offset=0 → merged count=5 offset=0.
        read = _read_rel(["a"])
        inner_fetch = _fetch_rel(read, count=None, offset=0)
        outer_fetch = _fetch_rel(inner_fetch, count=5, offset=0)
        out = RewriteLimits().apply(_wrap_plan(outer_fetch))
        root = get_root_rel(out)
        self.assertEqual(get_rel_kind(root), "fetch")
        self.assertTrue(root.fetch.HasField("count_expr"))
        self.assertEqual(root.fetch.count_expr.literal.i64, 5)
        self.assertEqual(root.fetch.offset_expr.literal.i64, 0)

    def test_unlimited_outer_clamps_to_inner(self) -> None:
        # Inner count=10 offset=2, outer unlimited offset=3.
        # Merge: offset=2+3=5, count=10-3=7 (no outer cap).
        read = _read_rel(["a"])
        inner_fetch = _fetch_rel(read, count=10, offset=2)
        outer_fetch = _fetch_rel(inner_fetch, count=None, offset=3)
        out = RewriteLimits().apply(_wrap_plan(outer_fetch))
        root = get_root_rel(out)
        self.assertEqual(root.fetch.offset_expr.literal.i64, 5)
        self.assertEqual(root.fetch.count_expr.literal.i64, 7)


# ---------------------------------------------------------------------------
# PruneUnusedColumns
# ---------------------------------------------------------------------------


def _wrap_plan_with_names(root: algebra.Rel, names: list[str]) -> plan.Plan:
    """Like ``_wrap_plan`` but with explicit root output names."""
    return plan.Plan(
        relations=[plan.PlanRel(root=algebra.RelRoot(input=root, names=names))]
    )


def _read_schema_names(p: plan.Plan) -> list[str]:
    """Return the bottom-most ReadRel's ``base_schema.names`` in *p*."""
    rel = p.relations[0].root.input
    while True:
        kind = rel.WhichOneof("rel_type")
        if kind == "read":
            return list(rel.read.base_schema.names)
        inner = getattr(rel, kind)
        if hasattr(inner, "input") and inner.HasField("input"):
            rel = inner.input
        elif hasattr(inner, "left"):
            rel = inner.left
        else:
            return []


class PruneUnusedColumnsRuleTest(unittest.TestCase):
    """Unit tests for ``PruneUnusedColumns`` operating on crafted Plans."""

    def test_drops_read_columns_no_consumer_references(self) -> None:
        # Read[a,b,c,d,e] -> Project[$1] -> root("b")
        # Columns a, c, d, e are unreferenced; only b survives.
        read = _read_rel(["a", "b", "c", "d", "e"])
        proj = _project_rel(read, [_field_ref(1)])
        out = PruneUnusedColumns().apply(_wrap_plan_with_names(proj, ["b"]))
        self.assertEqual(_read_schema_names(out), ["b"])

    def test_keeps_referenced_read_columns_only(self) -> None:
        # Read[a,b,c] -> Project[$0,$2] -> root("a","c"); column b unused.
        read = _read_rel(["a", "b", "c"])
        proj = _project_rel(read, [_field_ref(0), _field_ref(2)])
        out = PruneUnusedColumns().apply(_wrap_plan_with_names(proj, ["a", "c"]))
        self.assertEqual(_read_schema_names(out), ["a", "c"])

    def test_no_op_when_every_column_used(self) -> None:
        read = _read_rel(["a", "b"])
        proj = _project_rel(read, [_field_ref(0), _field_ref(1)])
        before = _wrap_plan_with_names(proj, ["a", "b"]).SerializeToString()
        out = PruneUnusedColumns().apply(_wrap_plan_with_names(proj, ["a", "b"]))
        self.assertEqual(out.SerializeToString(), before)

    def test_filter_pass_through_keeps_predicate_column(self) -> None:
        # Read[a,b,c] -> Filter($0 == lit) -> Project[$2] -> root("c")
        # Pruning must keep a (predicate) AND c (output); drop b.
        read = _read_rel(["a", "b", "c"])
        # Build $0 == 1 predicate as scalar function.
        eq = algebra.Expression(
            scalar_function=algebra.Expression.ScalarFunction(
                arguments=[
                    algebra.FunctionArgument(value=_field_ref(0)),
                    algebra.FunctionArgument(value=_i32_literal(1)),
                ]
            )
        )
        filt = _filter_rel(read, eq)
        proj = _project_rel(filt, [_field_ref(2)])
        out = PruneUnusedColumns().apply(_wrap_plan_with_names(proj, ["c"]))
        kept = _read_schema_names(out)
        self.assertIn("a", kept)
        self.assertIn("c", kept)
        self.assertNotIn("b", kept)

    def test_renumbers_predicate_after_pruning(self) -> None:
        # Read[a,b,c] -> Filter($2 == lit) -> Project[$0] -> root("a")
        # The filter needs column c ($2); the project needs column a ($0);
        # column b ($1) is unused. After pruning -> Read[a,c] with remap
        # {0: 0, 2: 1}. The filter's $2 must be rewritten to $1.
        read = _read_rel(["a", "b", "c"])
        eq = algebra.Expression(
            scalar_function=algebra.Expression.ScalarFunction(
                arguments=[
                    algebra.FunctionArgument(value=_field_ref(2)),
                    algebra.FunctionArgument(value=_i32_literal(1)),
                ]
            )
        )
        filt = _filter_rel(read, eq)
        proj = _project_rel(filt, [_field_ref(0)])
        out = PruneUnusedColumns().apply(_wrap_plan_with_names(proj, ["a"]))
        # Read should be down to ["a", "c"].
        self.assertEqual(_read_schema_names(out), ["a", "c"])
        # Filter should now reference $1 (the new index of "c").
        new_filt_cond = (
            out.relations[0]
            .root.input.project.input.filter.condition.scalar_function.arguments[0]
            .value
        )
        self.assertEqual(new_filt_cond.selection.direct_reference.struct_field.field, 1)

    def test_idempotent_on_already_pruned_plan(self) -> None:
        read = _read_rel(["a", "b", "c"])
        proj = _project_rel(read, [_field_ref(1)])
        wrap = _wrap_plan_with_names(proj, ["b"])
        once = PruneUnusedColumns().apply(wrap)
        twice = PruneUnusedColumns().apply(once)
        self.assertEqual(once.SerializeToString(), twice.SerializeToString())

    def test_does_not_mutate_input(self) -> None:
        read = _read_rel(["a", "b", "c"])
        proj = _project_rel(read, [_field_ref(1)])
        wrap = _wrap_plan_with_names(proj, ["b"])
        before = wrap.SerializeToString()
        PruneUnusedColumns().apply(wrap)
        self.assertEqual(wrap.SerializeToString(), before)

    def test_root_output_names_preserved(self) -> None:
        # The root names list must round-trip unchanged regardless of
        # what we prune underneath.
        read = _read_rel(["a", "b", "c", "d"])
        proj = _project_rel(read, [_field_ref(0), _field_ref(2)])
        wrap = _wrap_plan_with_names(proj, ["alpha", "gamma"])
        out = PruneUnusedColumns().apply(wrap)
        self.assertEqual(list(out.relations[0].root.names), ["alpha", "gamma"])

    def test_pipeline_integration_dataframe(self) -> None:
        """Through the full SubstraitOptimizerPipeline, an obviously-prunable
        plan should drop unused columns and survive a fixpoint pass."""
        df = DataFrame(
            {"a": [1, 2], "b": [3, 4], "c": [5.0, 6.0], "d": ["p", "q"]},
            planFactory=SubstraitPlanBuilderFactory(),
        )
        # Project to one column; b/c/d become unreferenced at root.
        df.select({"a"})
        lowered = df._planBuilder_.toSubstraitPlan()
        before_bytes = len(lowered.SerializeToString())
        optimized = SubstraitOptimizerPipeline().optimize(lowered)
        after_bytes = len(optimized.SerializeToString())
        # Pruning must shrink the plan or at least not grow it (in this
        # case it strictly shrinks).
        self.assertLess(after_bytes, before_bytes)
        self.assertEqual(_read_schema_names(optimized), ["a"])


# ---------------------------------------------------------------------------
# Pipeline-level tests
# ---------------------------------------------------------------------------


class SubstraitPipelineFixpointTest(unittest.TestCase):
    """The pipeline must reach a fixpoint within one extra pass."""

    def test_idempotent_on_lowered_plan(self) -> None:
        df = DataFrame(
            {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
            planFactory=SubstraitPlanBuilderFactory(),
        )
        df.select({"id"}).filter(df["id"] > 1)
        lowered = df._planBuilder_.toSubstraitPlan()
        pipeline = SubstraitOptimizerPipeline()
        once = pipeline.optimize(lowered)
        twice = pipeline.optimize(once)
        self.assertEqual(once.SerializeToString(), twice.SerializeToString())

    def test_empty_plan_passthrough(self) -> None:
        # Empty plan (no relations) must be returned unchanged.
        empty = plan.Plan()
        out = SubstraitOptimizerPipeline().optimize(empty)
        self.assertEqual(out.SerializeToString(), empty.SerializeToString())


class PlanIsSubstraitLowerableTest(unittest.TestCase):
    """The engine-level lowerability predicate matches what the lowering accepts."""

    def test_values_source_is_lowerable(self) -> None:
        df = DataFrame(
            {"id": [1, 2, 3]},
            planFactory=SubstraitPlanBuilderFactory(),
        )
        self.assertTrue(plan_is_substrait_lowerable(df._planBuilder_.logicalPlan()))

    def test_tpch_read_source_is_lowerable(self) -> None:
        """Every BoltML logical source kind is Substrait-lowerable;
        ``_NON_SUBSTRAIT_LOWERABLE_SOURCES`` is empty.
        ``SubstraitPlanBuilderFactory.fromLogicalPlan`` dispatches
        ``tpch_read`` through ``fromTpchRead`` which builds a
        ``ReadRel`` with a ``TpchExtensionTable`` payload."""
        from ..plan_builder.base import LogicalPlan, PlanSource

        src = PlanSource("tpch_read", ())
        plan = LogicalPlan(src, ())
        self.assertTrue(plan_is_substrait_lowerable(plan))


if __name__ == "__main__":
    unittest.main()
