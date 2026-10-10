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

"""Merge two adjacent ``ProjectRel`` nodes into one."""

from __future__ import annotations

from typing import Optional

from substrait.proto import algebra, plan

from ._expression import collect_field_indices, substitute_field_refs
from .base import (
    Rule,
    get_rel_kind,
    rewrite_plan_root,
)


class MergeProjects(Rule):
    """Fuse ``ProjectRel(ProjectRel(X))`` into a single ``ProjectRel``.

    BoltML's ``ProjectRel`` uses *replacement* semantics — the project's
    output columns are exactly its ``expressions`` list (the
    ``RelCommon.Hint.output_names`` mirrors that). So the inner project's
    output column ``i`` is ``inner.expressions[i]``. Merging the two
    projects means substituting every direct field-reference ``i`` in the
    outer's expressions with ``inner.expressions[i]``.

    Invariants preserved:
      * The merged ``ProjectRel`` keeps the outer project's
        ``RelCommon`` (output names + emit), so its output schema is
        identical to the original outer project.
      * If any outer expression references a column outside
        ``range(len(inner.expressions))``, the rule conservatively skips
        the merge (returns ``None``); BoltML's replacement-style project
        would not have emitted such a column anyway, and merging would
        produce an out-of-range field-ref.
    """

    name: str = "merge_projects"

    def apply(self, p: plan.Plan) -> plan.Plan:
        """Apply the merge bottom-up across every ``Rel`` in the root tree.

        Returns a fresh ``Plan``; *p* is not mutated.
        """
        return rewrite_plan_root(p, _merge_one, bottom_up=True)


def _is_trivial_inner(expr: algebra.Expression) -> bool:
    """A ``Project`` expression is *trivial* iff inlining it in place of
    a field-ref is cost-free and side-effect-free.

    Only plain selections (field references) and literals qualify.
    Function calls, casts, if-then, switch, singular-or-list, and any
    nested expression are non-trivial: duplicating them through
    substitution would (a) re-execute potentially impure user code
    such as Python UDFs and (b) inflate compute cost when the inner
    project was the natural CSE point.
    """
    kind = expr.WhichOneof("rex_type")
    return kind in ("selection", "literal")


def _merge_one(rel: algebra.Rel) -> Optional[algebra.Rel]:
    if get_rel_kind(rel) != "project":
        return None
    outer = rel.project
    if get_rel_kind(outer.input) != "project":
        return None
    inner = outer.input.project

    # The ``join_reorder`` rule stamps a marker alias on its
    # ``_post_unprefix_project`` output so subsequent passes can detect
    # "this hash_join below is already optimal, do not re-rewrite it".
    # Merging through the marker drops it and the next ``join_reorder``
    # pass would re-extract the rebuilt subgraph, accumulate another
    # prefix layer on every leaf, and oscillate forever. Refuse to
    # merge whenever either project carries the marker.
    _MARKER = "boltml_join_reorder_output"
    for proj in (outer, inner):
        if (
            proj.HasField("common")
            and proj.common.HasField("hint")
            and proj.common.hint.alias == _MARKER
        ):
            return None

    inner_width = len(inner.expressions)
    valid_range = range(inner_width)

    # Refuse to merge if any outer expression references a column the
    # inner project did not emit. Pure check, no substitution attempted.
    for outer_expr in outer.expressions:
        for idx in collect_field_indices(outer_expr):
            if idx not in valid_range:
                return None

    # Count how many times each inner output is referenced across all
    # outer expressions. If the same inner index is referenced more
    # than once AND its inner expression is non-trivial (i.e. anything
    # other than a plain field-ref or literal — a function call, cast,
    # case, etc.), substituting in place would duplicate that
    # expression in the merged project. That breaks two contracts at
    # once:
    #
    #   * Correctness for impure UDFs. BoltML registers Python
    #     ``vectorFunction`` / ``mapBatchFunction`` callbacks that
    #     receive the input vector by reference; many of them mutate
    #     it in place (e.g. ``def addAb(a, b=3): a[i] = a[i] + b``)
    #     and return the same buffer. The pre-merge plan evaluated
    #     such a UDF exactly once per row (the second project just
    #     read the field), so the in-place mutation worked. Merging
    #     duplicates the call into both ``c0`` and the captured
    #     ``result`` slot, so the second call observes the already-
    #     mutated buffer and applies the UDF twice. Substrait specs
    #     project expressions as pure, but the BoltML public API
    #     does not force purity on Python UDFs, and silently changing
    #     observed outputs for the impure ones is a correctness
    #     regression.
    #
    #   * Common-subexpression cost. Even for genuinely pure inner
    #     expressions, substituting an N-cost function call into K
    #     outer references inflates work to N*K. The inner project
    #     was already the natural CSE point; collapsing it loses
    #     that.
    #
    # Bail out conservatively in either case — the un-merged
    # two-project shape is correct and the optimizer is free to find
    # a smarter form in a later rule if it can prove purity.
    ref_counts: dict[int, int] = {}
    for outer_expr in outer.expressions:
        for idx in collect_field_indices(outer_expr):
            ref_counts[idx] = ref_counts.get(idx, 0) + 1
    for idx, count in ref_counts.items():
        if count > 1 and not _is_trivial_inner(inner.expressions[idx]):
            return None

    def _lookup(idx: int) -> Optional[algebra.Expression]:
        if idx < 0 or idx >= inner_width:
            return None
        return inner.expressions[idx]

    new_expressions = [
        substitute_field_refs(outer_expr, _lookup) for outer_expr in outer.expressions
    ]

    merged = algebra.ProjectRel()
    merged.CopyFrom(outer)
    merged.input.CopyFrom(inner.input)
    del merged.expressions[:]
    merged.expressions.extend(new_expressions)
    return algebra.Rel(project=merged)
