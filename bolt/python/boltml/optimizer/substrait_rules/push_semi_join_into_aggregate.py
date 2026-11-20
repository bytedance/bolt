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

"""Push a semi-join filter into an aggregate's input.

This is the classical "AggregateSemiJoinPushdown" rewrite for
correlated subqueries (TPC-H q17, q18, q20) where one side of an
inner join is an Aggregate that groups over a much larger input.
The other side typically has a small filtered set of join keys; the
aggregate is wastefully aggregating over key values the join will
later drop.

Pattern recognised
------------------

::

    HashJoinRel[INNER]
      left:  X                           # small filtered side
      right: AggregateRel
              grouping_expressions: [G1, ..., Gn]   # all direct field refs
              measures:             [M1, ...]
              input: Y                   # big base table
      keys:  [(X.k1 = agg.G_i1), ...]    # all agg-side keys are
                                          # grouping_expressions

Or symmetrically with the Aggregate on the LEFT.

Rewrite
-------

::

    HashJoinRel[INNER]                   # outer join unchanged
      left:  X
      right: AggregateRel                # same grouping & measures
              input:
                HashJoinRel[LEFT_SEMI]   # NEW: filter Y to keys in X
                  left:  Y
                  right: ProjectRel(X, only join-key columns)
                  keys:  [(Y.G_i1 = X.k1), ...]

The aggregate now only sees Y rows whose join keys appear in X.
Same final result -- the keys that get dropped after the outer join
are the same ones the new semi-join filters out before aggregation.
For TPC-H q17 with X = ~2K filtered parts and Y = 60M lineitem rows,
the aggregate's input drops from ~60M rows to ~120K (matched parts'
worth), a ~500x reduction.

Conditions checked
------------------

1. The outer join is ``hash_join`` of ``JOIN_TYPE_INNER``.
2. The outer join has at least one key.
3. Exactly one side of the join has an Aggregate as its direct
   input (no Project chain on top in this v1 -- extended in a
   follow-up).
4. All join keys reference direct field references on both sides.
5. All keys on the agg-side reference columns that are themselves
   ``grouping_expressions`` of the aggregate (with each grouping
   itself a direct field reference, so we can translate to the
   aggregate's input's column space).
6. The aggregate has at least one measure (otherwise it's a pure
   group-by-distinct -- ``InnerDedupToSemi`` handles that more
   directly by removing the aggregate entirely).

Opt-in
------

This rule is gated by ``BOLTML_PUSH_SEMI_INTO_AGG`` (default 0 = off)
while it ships in the experimental tier. Set to 1 to enable.

Empirical (planned)
-------------------

TPC-H SF=10, local 8-CPU cluster:

  Query  Without rule    With rule
  q17    ~133s          target ~10-20s   (correlated avg subquery)
  q18    ~127s          target ~10-20s   (correlated SUM subquery)

Rule lives in the LOGICAL pipeline, AFTER ``PushAggThroughJoin``
(so the agg shape is settled) and AFTER ``EnsureSmallerOnRight``
(so we know which side is "small"). Runs BEFORE
``PushFilterThroughProjects`` so the new semi-join is already in
place when filters get pushed.
"""

from __future__ import annotations

import os
from typing import List, Optional, Tuple

from substrait.proto import algebra, plan

from ...logging import boltmlDebugLog
from ._expression import direct_field_index
from .base import Rule, get_rel_kind, rewrite_plan_root


def _push_semi_into_agg_enabled() -> bool:
    """Read ``BOLTML_PUSH_SEMI_INTO_AGG`` env var (default 0 = off)."""
    return os.environ.get("BOLTML_PUSH_SEMI_INTO_AGG", "0").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _make_field_ref(idx: int) -> algebra.Expression:
    """Build a fresh top-level direct field reference Expression."""
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


def _hint_output_names(rel: algebra.Rel) -> Tuple[str, ...]:
    """Return ``RelCommon.Hint.output_names`` of *rel* as a tuple, or ``()``."""
    kind = get_rel_kind(rel)
    if kind is None:
        return ()
    inner = getattr(rel, kind)
    if not hasattr(inner, "HasField") or not inner.HasField("common"):
        return ()
    if not inner.common.HasField("hint"):
        return ()
    return tuple(inner.common.hint.output_names)


def _make_field_reference(idx: int) -> algebra.Expression.FieldReference:
    """Build a fresh top-level direct ``FieldReference`` proto (no
    surrounding Expression). Used for ``ComparisonJoinKey.{left,right}``
    which carries FieldReference directly."""
    return algebra.Expression.FieldReference(
        direct_reference=algebra.Expression.ReferenceSegment(
            struct_field=algebra.Expression.ReferenceSegment.StructField(
                field=idx,
            ),
        ),
        root_reference=algebra.Expression.FieldReference.RootReference(),
    )


def _agg_directly_under(rel: algebra.Rel) -> Optional[algebra.AggregateRel]:
    """Return the Aggregate iff *rel* is an AggregateRel directly. Returns
    None if *rel* is anything else (project, exchange, ...)."""
    if get_rel_kind(rel) == "aggregate":
        return rel.aggregate
    return None


def _walk_projects_to_aggregate(
    rel: algebra.Rel,
) -> Tuple[Optional[algebra.AggregateRel], List[algebra.ProjectRel]]:
    """Walk *rel* through any chain of Project rels and return
    ``(aggregate, [projects])`` where ``projects`` is OUTERMOST-FIRST
    (first entry is the project directly under the join, last entry is
    the project sitting on top of the aggregate).

    Returns ``(None, [])`` if the chain ends in something that isn't an
    Aggregate. Does NOT enforce that projects are pure passthrough --
    callers are responsible for translating join-key columns through
    each project's expressions, and the per-key passthrough check
    happens column-by-column in ``_translate_keys_through_projects``.
    """
    projects: List[algebra.ProjectRel] = []
    cur = rel
    while True:
        kind = get_rel_kind(cur)
        if kind == "aggregate":
            return cur.aggregate, projects
        if kind != "project":
            return None, []
        proj = cur.project
        projects.append(proj)
        if not proj.HasField("input"):
            return None, []
        cur = proj.input


def _translate_keys_through_projects(
    keys: List[int],
    projects: List[algebra.ProjectRel],
) -> Optional[List[int]]:
    """Translate join-key column indices through a chain of Project rels.

    *keys* are positions in the OUTERMOST project's output (i.e. what
    the join sees). *projects* is outermost-first (matching the return
    of ``_walk_projects_to_aggregate``).

    For each key, walk OUTERMOST-INWARD: at each project, look up
    ``project.expressions[key_idx]`` and require it to be a direct
    field reference; replace ``key_idx`` with the referenced index in
    ``project.input``'s output space. After all projects are walked,
    the index points into the AGGREGATE's output column space.

    Returns None if any key's expression at any layer isn't a direct
    field reference (the rule can't safely push through a computed
    column).

    Note that we DON'T require ALL of a project's expressions to be
    field-refs -- only the ones touched by join keys matter. So a
    project that produces e.g. ``[passthrough_partkey, 0.2 * avg_q]``
    is fine when the join key only references position 0
    (passthrough_partkey).
    """
    # Substrait Project semantics: when no emit_kind is set or it's
    # "direct", the project's output is just its expressions list (not
    # input + expressions). Boltml's projectRel sets common.direct, so
    # the output column count equals len(expressions).
    out: List[int] = []
    for key in keys:
        cur_idx = key
        for proj in projects:
            if cur_idx < 0 or cur_idx >= len(proj.expressions):
                return None
            expr = proj.expressions[cur_idx]
            idx = direct_field_index(expr)
            if idx is None:
                return None  # join key flows through a computed expression
            cur_idx = idx
        out.append(cur_idx)
    return out


def _all_groupings_are_field_refs(
    agg: algebra.AggregateRel,
) -> Optional[List[int]]:
    """Return [field_index for each grouping expression], or None if any
    grouping is not a plain direct field reference.

    Handles two Substrait formats:
    1. Modern: ``agg.grouping_expressions = [exprs]``,
       ``agg.groupings[0].expression_references = [indices into above]``.
       (Boltml uses this -- see ``boltml/substrait/rel.py:aggregateRel``.)
    2. Legacy: ``agg.groupings[0].grouping_expressions = [exprs]`` inline,
       ``agg.grouping_expressions = []``.
    """
    if not agg.groupings:
        return None
    if len(agg.groupings) != 1:
        # Substrait allows multiple grouping sets (GROUPING SETS / CUBE /
        # ROLLUP); we only handle the simple single-group case.
        return None
    grouping = agg.groupings[0]

    # Modern format: pull expressions from agg.grouping_expressions via
    # the grouping's expression_references list.
    if grouping.expression_references and not grouping.grouping_expressions:
        out: List[int] = []
        for ref in grouping.expression_references:
            if ref < 0 or ref >= len(agg.grouping_expressions):
                return None
            expr = agg.grouping_expressions[ref]
            idx = direct_field_index(expr)
            if idx is None:
                return None
            out.append(idx)
        return out

    # Legacy format: expressions inline in the grouping.
    if grouping.grouping_expressions:
        out = []
        for expr in grouping.grouping_expressions:
            idx = direct_field_index(expr)
            if idx is None:
                return None
            out.append(idx)
        return out

    return None


def _field_ref_index(fr) -> Optional[int]:
    """Return the field index of a Substrait ``FieldReference``, or None
    if it isn't a plain top-level ``struct_field`` reference.

    ``HashJoinRel.keys`` (new ``ComparisonJoinKey`` format) carries
    ``FieldReference`` directly rather than wrapping it in ``Expression``.
    """
    if not fr.HasField("direct_reference"):
        return None
    ref = fr.direct_reference
    if ref.WhichOneof("reference_type") != "struct_field":
        return None
    if ref.struct_field.HasField("child"):
        return None
    return ref.struct_field.field


def _keys_are_simple(
    keys,
) -> Optional[List[Tuple[int, int]]]:
    """Return [(left_field_idx, right_field_idx) for each ComparisonJoinKey],
    or None if any key isn't a plain field-ref equality."""
    out: List[Tuple[int, int]] = []
    for k in keys:
        # ComparisonJoinKey.{left,right} are FieldReference (not Expression)
        left_idx = _field_ref_index(k.left)
        right_idx = _field_ref_index(k.right)
        if left_idx is None or right_idx is None:
            return None
        # Only handle equality joins (the typical hash-join shape).
        # ComparisonJoinKey.comparison is a wrapper Message with a
        # ``simple`` enum field; eq is SIMPLE_COMPARISON_TYPE_EQ.
        if k.HasField("comparison"):
            cmp = k.comparison
            if cmp.WhichOneof("inner_type") != "simple":
                return None
            if cmp.simple != algebra.ComparisonJoinKey.SIMPLE_COMPARISON_TYPE_EQ:
                return None
        out.append((left_idx, right_idx))
    return out


def _translate_join_keys_through_aggregate(
    agg_side_keys: List[int],
    grouping_field_indices: List[int],
) -> Optional[List[int]]:
    """For each join-side key index (a column position in agg's output),
    return the corresponding column position in agg's input.

    Substrait Aggregate output column ordering: grouping_expressions
    come first (positions 0..len(groupings)-1), then measures. So a
    join key referring to position i (where i < len(groupings)) maps
    to agg_input column ``grouping_field_indices[i]``.

    Returns None if any key refers to a column that is NOT a grouping
    expression (e.g. an aggregate measure -- can't push because the
    measure value doesn't exist before aggregation).
    """
    n_groupings = len(grouping_field_indices)
    out: List[int] = []
    for k in agg_side_keys:
        if k < 0 or k >= n_groupings:
            return None  # key references a measure output, not a grouping
        out.append(grouping_field_indices[k])
    return out


def _build_distinct_keys_project(
    side: algebra.Rel,
    side_key_indices: List[int],
    output_names: List[str],
) -> algebra.Rel:
    """Build a Project that selects only the join-key columns from *side*.

    The semi-join's right side only needs these columns to filter the
    big-side input. We do NOT add a DISTINCT here -- LEFT_SEMI naturally
    produces each LHS row at most once regardless of how many RHS
    matches exist. So duplicates on the small side cost a few extra
    hash-table inserts but not extra output rows.

    Hint ``output_names`` must match the number of expressions and is
    used by the C++ converter for column-name plumbing across the
    SemiJoin -> Project -> Aggregate -> outer-join chain.
    """
    project = algebra.ProjectRel()
    project.input.CopyFrom(side)
    for idx in side_key_indices:
        project.expressions.append(_make_field_ref(idx))
    # Boltml convention (see ``boltml/substrait/rel.py:projectRel``):
    # set ``common.direct`` so the project's output is its expressions
    # only (not input + expressions). Hint output_names mirror the
    # column names the LHS of the new SemiJoin uses for the same
    # positions, so the C++ join schema-builder finds matching names.
    project.common.CopyFrom(
        algebra.RelCommon(
            direct=algebra.RelCommon.Direct(),
            hint=algebra.RelCommon.Hint(output_names=output_names),
        )
    )
    return algebra.Rel(project=project)


def _try_push_semi_join_into_aggregate(rel: algebra.Rel) -> Optional[algebra.Rel]:
    """Match the agg-on-right hash_join pattern and rewrite.

    Returns None if the pattern doesn't match.
    """
    if get_rel_kind(rel) != "hash_join":
        return None
    join = rel.hash_join
    if join.type != algebra.HashJoinRel.JoinType.JOIN_TYPE_INNER:
        return None
    if not join.keys:
        return None
    if join.HasField("post_join_filter"):
        return None
    if not join.HasField("left") or not join.HasField("right"):
        return None

    # Walk through any project chain on each side to find an Aggregate.
    right_agg, right_projects = _walk_projects_to_aggregate(join.right)
    left_agg, left_projects = _walk_projects_to_aggregate(join.left)
    if right_agg is None and left_agg is None:
        return None
    if right_agg is not None and left_agg is not None:
        # Both sides aggregate -- ambiguous; skip
        return None

    if right_agg is not None:
        agg = right_agg
        agg_projects = right_projects
        other_side_rel = join.left
        agg_is_right = True
    else:
        agg = left_agg
        agg_projects = left_projects
        other_side_rel = join.right
        agg_is_right = False

    if not agg.measures:
        # Pure dedup -- InnerDedupToSemi handles this case
        return None
    if not agg.HasField("input"):
        return None

    # Idempotency guard: if agg.input is already a LEFT_SEMI hash_join,
    # this rule has already pushed a semi-join into this aggregate on a
    # prior iteration. Don't fire again -- otherwise we'd stack
    # arbitrarily many redundant semi-joins.
    if get_rel_kind(agg.input) == "hash_join":
        inner_join = agg.input.hash_join
        if inner_join.type == algebra.HashJoinRel.JoinType.JOIN_TYPE_LEFT_SEMI:
            return None

    grouping_field_indices = _all_groupings_are_field_refs(agg)
    if grouping_field_indices is None:
        return None

    simple_keys = _keys_are_simple(list(join.keys))
    if simple_keys is None:
        return None

    # Extract the agg-side and other-side key indices from each pair
    if agg_is_right:
        agg_side_keys = [right_idx for (_, right_idx) in simple_keys]
        other_side_keys = [left_idx for (left_idx, _) in simple_keys]
    else:
        agg_side_keys = [left_idx for (left_idx, _) in simple_keys]
        other_side_keys = [right_idx for (_, right_idx) in simple_keys]

    # Translate agg-side keys (positions in outermost project's output)
    # back through the project chain to get the aggregate's output index.
    # Only the join-key COLUMNS need to be passthrough field-refs --
    # other expressions in the same project are fine to be computed.
    agg_output_keys = _translate_keys_through_projects(agg_side_keys, agg_projects)
    if agg_output_keys is None:
        return None

    # Translate agg output keys to positions in the agg's INPUT column space.
    input_side_keys = _translate_join_keys_through_aggregate(
        agg_output_keys, grouping_field_indices
    )
    if input_side_keys is None:
        return None

    # Get the agg's input column names from its hint output_names.
    # We need these to set the new SemiJoin's hint output_names so the
    # C++ SubstraitToBoltPlan converter selects kLeftSemiFilter (which
    # requires output_names size to equal LHS column count, see
    # InnerDedupToSemi for the same pattern).
    agg_input_names = list(_hint_output_names(agg.input))
    if not agg_input_names:
        # Without input names we can't safely set the SemiJoin hint;
        # bail rather than produce a plan that fails C++ validation.
        return None

    # Build the new semi-join: agg.input LEFT_SEMI other_side.project(keys)
    # The RHS Project's output names MUST differ from any LHS column
    # name -- Bolt's join schema validator rejects duplicate names
    # across left/right (PlanNode.cpp:1039). Use a synthetic
    # ``__bcastkey_<n>`` prefix that won't collide with any user column.
    semi_join = algebra.HashJoinRel()
    semi_join.left.CopyFrom(agg.input)
    rhs_synthetic_names = [f"__semikey_{i}" for i in range(len(other_side_keys))]
    semi_join.right.CopyFrom(
        _build_distinct_keys_project(
            other_side_rel,
            other_side_keys,
            rhs_synthetic_names,
        )
    )
    semi_join.type = algebra.HashJoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
    # LEFT_SEMI emits LHS columns only. Hint output_names must match.
    semi_join.common.CopyFrom(
        algebra.RelCommon(
            direct=algebra.RelCommon.Direct(),
            hint=algebra.RelCommon.Hint(output_names=agg_input_names),
        )
    )

    # Build keys for the semi-join: input_side_keys (LHS, agg's input)
    # = positions [0..len(other_side_keys)-1] (RHS, projected keys only).
    # ComparisonJoinKey carries FieldReference directly (not Expression).
    for i, lhs_idx in enumerate(input_side_keys):
        key = algebra.ComparisonJoinKey()
        key.left.CopyFrom(_make_field_reference(lhs_idx))
        key.right.CopyFrom(_make_field_reference(i))
        key.comparison.simple = algebra.ComparisonJoinKey.SIMPLE_COMPARISON_TYPE_EQ
        semi_join.keys.append(key)

    # Build the rewritten aggregate with the semi-join as input
    new_agg = algebra.AggregateRel()
    new_agg.CopyFrom(agg)
    new_agg.ClearField("input")
    new_agg.input.CopyFrom(algebra.Rel(hash_join=semi_join))

    # Build the new agg-side: the project chain on top of the original
    # aggregate is preserved verbatim; only the innermost (the aggregate
    # itself) is swapped out for ``new_agg``. ``agg_projects`` is
    # outermost-first: ``agg_projects[0]`` sat directly under the join
    # and ``agg_projects[-1]`` sat directly on the original aggregate.
    # Rebuild bottom-up: start with new_agg, then wrap each project
    # innermost-first (i.e. iterate ``agg_projects`` in REVERSE).
    new_agg_side = algebra.Rel(aggregate=new_agg)
    for proj in reversed(agg_projects):
        rebuilt = algebra.ProjectRel()
        rebuilt.CopyFrom(proj)
        rebuilt.ClearField("input")
        rebuilt.input.CopyFrom(new_agg_side)
        new_agg_side = algebra.Rel(project=rebuilt)

    # Build the new outer join with the rewritten agg side
    new_join = algebra.HashJoinRel()
    new_join.CopyFrom(join)
    if agg_is_right:
        new_join.ClearField("right")
        new_join.right.CopyFrom(new_agg_side)
    else:
        new_join.ClearField("left")
        new_join.left.CopyFrom(new_agg_side)

    boltmlDebugLog(
        "push_semi_join_into_aggregate",
        f"pushed semi-join into aggregate: agg_side="
        f"{'right' if agg_is_right else 'left'} "
        f"groupings={len(grouping_field_indices)} keys={len(simple_keys)}",
    )

    return algebra.Rel(hash_join=new_join)


class PushSemiJoinIntoAggregate(Rule):
    """Push a semi-join filter into the input of an aggregate that's a
    direct child of an inner hash join. Pure: input ``Plan`` is not
    mutated. Idempotent (after rewrite, the agg's input is no longer
    the original raw scan, so the pattern won't re-match).
    """

    name: str = "push_semi_join_into_aggregate"

    def apply(self, p: plan.Plan) -> plan.Plan:
        if not _push_semi_into_agg_enabled():
            out = plan.Plan()
            out.CopyFrom(p)
            return out
        return rewrite_plan_root(p, _try_push_semi_join_into_aggregate, bottom_up=True)
