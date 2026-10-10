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

"""Rewrite ``inner-join + dedup-groupby`` into a proper semi-join.

Pattern recognised
------------------

::

    AggregateRel                                # K-only group-by, no measures
      grouping_expressions: [a1, a2, ..., aN]   # all direct field refs
      measures: []   OR  [count() / count(*)]   # purely a deduplication
      input: ProjectRel*                        # zero or more passthroughs
        HashJoinRel
          type: INNER
          keys: equi-join only
          left:  side L
          right: side R

When ALL non-join-key grouping keys come from a single side (call it
"A"), the inner-join + dedup-groupby is semantically equivalent to a
semi-join of A against the OTHER side ("B"). The inner-join materialises
every matching ``(L, R)`` pair (M:N row blow-up) and the groupBy then
collapses the duplicates; a semi-join streams A and emits each A row
at most once when a B match is found — same result, fewer rows.

This is the pattern q4 emits today: ``orders.join(lineitem, key=orderkey)
.groupBy(o_orderkey, o_orderpriority).count()``. Both group keys come
from ``orders``; ``o_orderkey`` is also the join key (so it's available
on both sides), but ``o_orderpriority`` lives only on the orders side.
A = orders, B = lineitem; rewrite to ``orders.semi_join(lineitem,
key=orderkey)``.

Output direction
----------------

* All A-side columns + A is on the LEFT  → ``JOIN_TYPE_LEFT_SEMI``.
  The substrait→bolt converter at ``SubstraitToBoltPlan.cpp:1154``
  picks ``kLeftSemiFilter`` when the join's hint output_names count
  equals the LHS column count, so we set the new join's output_names
  to exactly the LHS hint names.
* A-side on the RIGHT → ``JOIN_TYPE_RIGHT_SEMI`` analogously, with
  output_names equal to RHS hint names → ``kRightSemiFilter``.

Schema preservation
-------------------

The aggregate's hint output_names list and the original aggregate's
column ordering are preserved by wrapping the new semi-join in a
Project that re-stamps each kept column under its original name.
Downstream consumers (sort, limit, RelRoot) see the same schema.

Conditions checked
------------------

1. Aggregate's measures list must be empty, or contain only ``count``
   measures (single-arg or count-star). Anything else (sum/min/max/
   avg/...) is real aggregation; semi-join would change semantics.
2. Aggregate ``common.emit_kind`` must be unset or ``direct``.
3. Aggregate must have at least one grouping_expression (zero would
   be a global agg; not the pattern).
4. Each grouping_expression must be a direct field-ref into the
   join's output (after walking through the optional passthrough
   project chain).
5. Each intermediate Project must be passthrough (every expression
   is a direct field-ref) and have ``direct`` emit.
6. The join must be ``hash_join`` of ``JOIN_TYPE_INNER``.
7. The join must have at least one key, and every key must be a
   simple equality on direct field-refs into LHS / RHS.
8. The join's hint output_names must use Bolt's ``l_``/``r_``
   prefix scheme; we use the prefix to attribute each join output
   column to a side. This is how every BoltML join is built (the
   prefix is added by the dataframe layer's join wrapper); rejecting
   joins without it keeps us from misclassifying weird hand-built
   plans.
9. The join must not have a ``post_join_filter`` (we'd need to
   prove the filter only references A-side columns).

Side attribution
----------------

For each grouping key (after substituting through the project chain),
look up its column in the join's output_names by index. Strip the
``l_`` / ``r_`` prefix and look up the resulting canonical name on
each side's output_names:

* canonical name appears on left only      → A-only column from L
* canonical name appears on right only     → A-only column from R
* canonical name appears on both AND that
  pair is a join key                       → "either" (we can pick
                                              the side whose other
                                              non-key columns dictate)

If all non-"either" attributions point to ONE side, we proceed with
A = that side. If columns split across both sides (e.g. group key
``o_orderpriority`` from R and ``l_quantity`` from L), the pattern
can't be a semi-join — we'd need to keep columns from both sides
and a semi-join only outputs one side. Skip.

If all group keys are "either" (every group key is a join key),
default A = LEFT. The pattern is degenerate — the dedup groups on
join keys only; the semi-join keeps the same key-bearing side without
materializing the joined columns.

Idempotency
-----------

After a successful rewrite the aggregate is gone, so a second pass
can't match the same shape. The rule is bottom-up, so each fixpoint
iteration sees the post-rewrite tree.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from substrait.proto import algebra, plan

from ...logging import boltmlDebugLog
from ._expression import (
    direct_field_index,
    is_direct_field_ref,
    substitute_field_refs,
)
from .base import Rule, get_rel_kind, rewrite_plan_root


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def _strip_lr_prefix(name: str) -> str:
    """Strip a single leading ``l_`` or ``r_`` prefix if present."""
    if name.startswith("l_") or name.startswith("r_"):
        return name[2:]
    return name


def _build_extension_lookup(p: plan.Plan) -> Dict[int, str]:
    """Return ``{function_anchor: name}`` for every extension_function in *p*."""
    out: Dict[int, str] = {}
    for ext in p.extensions:
        if ext.WhichOneof("mapping_type") == "extension_function":
            out[ext.extension_function.function_anchor] = ext.extension_function.name
    return out


def _agg_function_uri_name(
    measure: algebra.AggregateRel.Measure,
    extension_lookup: Dict[int, str],
) -> Optional[str]:
    """Return the unprefixed substrait function name for *measure*."""
    if not measure.HasField("measure"):
        return None
    full = extension_lookup.get(measure.measure.function_reference)
    if full is None:
        return None
    if ":" in full:
        return full.split(":", 1)[0]
    return full


# ---------------------------------------------------------------------------
# Rule
# ---------------------------------------------------------------------------


class InnerDedupToSemi(Rule):
    """Rewrite an inner-join + dedup-groupby into a proper semi-join.

    Pure: input ``Plan`` is not mutated. Idempotent: a successful rewrite
    consumes the matched aggregate, so a re-run on the output produces
    a byte-equal Plan.
    """

    name: str = "inner_dedup_to_semi"

    def apply(self, p: plan.Plan) -> plan.Plan:
        ext_lookup = _build_extension_lookup(p)

        def _try(rel: algebra.Rel) -> Optional[algebra.Rel]:
            return _try_rewrite(rel, ext_lookup)

        return rewrite_plan_root(p, _try, bottom_up=True)


# ---------------------------------------------------------------------------
# Match + rewrite
# ---------------------------------------------------------------------------


def _try_rewrite(
    rel: algebra.Rel,
    ext_lookup: Dict[int, str],
) -> Optional[algebra.Rel]:
    """Match the pattern; return rewritten Rel or ``None`` to leave unchanged."""
    if get_rel_kind(rel) != "aggregate":
        return None
    agg = rel.aggregate

    # Need at least one grouping. Global aggregations are not the pattern.
    if not agg.grouping_expressions:
        return None

    # Reject a non-direct emit envelope.
    if agg.HasField("common") and agg.common.WhichOneof("emit_kind") not in (
        None,
        "direct",
    ):
        return None

    # Measures: empty OR all count() (with no per-measure filter / distinct
    # / sort). Anything that does real aggregation forbids the rewrite.
    count_measure_indices: List[int] = []
    for i, m in enumerate(agg.measures):
        if m.HasField("filter"):
            return None
        nm = _agg_function_uri_name(m, ext_lookup)
        if nm is None or nm != "count":
            return None
        if (
            m.measure.invocation
            == algebra.AggregateFunction.AGGREGATION_INVOCATION_DISTINCT
        ):
            return None
        if len(m.measure.sorts) > 0:
            return None
        # Phase must be "complete" — this rule fires before any other
        # multi-phase decomposition has touched the agg.
        if m.measure.phase not in (
            algebra.AggregationPhase.AGGREGATION_PHASE_UNSPECIFIED,
            algebra.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT,
        ):
            return None
        count_measure_indices.append(i)

    # Walk the optional passthrough Project chain between the aggregate
    # and the join. Each project's expressions must be plain field-refs
    # so we can substitute the agg's grouping_expressions back to the
    # join's output coordinate space without losing column identity.
    child = agg.input
    project_chain: List[algebra.ProjectRel] = []
    while get_rel_kind(child) == "project":
        proj = child.project
        if proj.HasField("common") and proj.common.WhichOneof("emit_kind") not in (
            None,
            "direct",
        ):
            return None
        for e in proj.expressions:
            if not is_direct_field_ref(e):
                return None
        project_chain.append(proj)
        child = proj.input

    # The next Rel must be a hash_join.
    if get_rel_kind(child) != "hash_join":
        return None
    hj_rel = child
    hj = hj_rel.hash_join

    if hj.type != algebra.HashJoinRel.JoinType.JOIN_TYPE_INNER:
        return None
    if hj.HasField("post_join_filter"):
        return None
    if not hj.keys:
        return None
    if hj.HasField("common") and hj.common.WhichOneof("emit_kind") not in (
        None,
        "direct",
    ):
        return None

    # Validate join keys: simple equality on direct field-refs only.
    join_key_lhs_indices: set[int] = set()
    join_key_rhs_indices: set[int] = set()
    for k in hj.keys:
        l_idx = _key_field_index(k.left)
        r_idx = _key_field_index(k.right)
        if l_idx is None or r_idx is None:
            return None
        if k.HasField("comparison"):
            cmp = k.comparison
            if cmp.WhichOneof("inner_type") != "simple":
                return None
            if cmp.simple != algebra.ComparisonJoinKey.SIMPLE_COMPARISON_TYPE_EQ:
                return None
        join_key_lhs_indices.add(l_idx)
        join_key_rhs_indices.add(r_idx)

    # Need hint output_names to attribute join columns to sides.
    join_names = _hint_output_names(hj_rel)
    if not join_names:
        return None
    left_names = _hint_output_names(hj.left)
    right_names = _hint_output_names(hj.right)
    if not left_names or not right_names:
        return None
    if len(join_names) > len(left_names) + len(right_names):
        return None

    left_name_set = set(left_names)
    right_name_set = set(right_names)

    # For each join output column index, resolve to ('L'/'R'/'EITHER',
    # side_idx). 'EITHER' means the canonical column name is a join key
    # present on both sides, so consuming a group key referring to it
    # could come from either side. For 'L' / 'R' the column is exclusive
    # to that side at the canonical name level.
    join_idx_attribution: List[Tuple[str, int]] = []
    for jn in join_names:
        canon = _strip_lr_prefix(jn)
        if jn.startswith("l_") and jn in left_name_set:
            l_idx = left_names.index(jn)
            # Is this also a join key? If so, the "canonical" key column
            # exists on the right too; mark as EITHER so a group-key on
            # this column doesn't force A = LEFT.
            if l_idx in join_key_lhs_indices:
                # Find the matching RHS join key index.
                rhs_match = _find_paired_rhs_key(hj.keys, l_idx)
                if rhs_match is not None:
                    join_idx_attribution.append(("EITHER", l_idx))
                    continue
            join_idx_attribution.append(("L", l_idx))
        elif jn.startswith("r_") and jn in right_name_set:
            r_idx = right_names.index(jn)
            if r_idx in join_key_rhs_indices:
                lhs_match = _find_paired_lhs_key(hj.keys, r_idx)
                if lhs_match is not None:
                    join_idx_attribution.append(("EITHER", r_idx))
                    continue
            join_idx_attribution.append(("R", r_idx))
        elif canon in left_name_set and not jn.startswith("r_"):
            join_idx_attribution.append(("L", left_names.index(canon)))
        elif canon in right_name_set and not jn.startswith("l_"):
            join_idx_attribution.append(("R", right_names.index(canon)))
        else:
            # Untrackable column; bail.
            return None

    # Substitute each grouping_expression all the way through the
    # project chain (outermost-to-innermost) so it lands as a direct
    # field-ref into the JOIN's output.
    grouping_in_join_space: List[int] = []
    for g in agg.grouping_expressions:
        sub = _substitute_through_chain(g, project_chain)
        if sub is None:
            return None
        if not is_direct_field_ref(sub):
            return None
        idx = direct_field_index(sub)
        if idx is None or not (0 <= idx < len(join_idx_attribution)):
            return None
        grouping_in_join_space.append(idx)

    # Determine the A side: collect non-'EITHER' attributions of the
    # group keys. If they all live on one side, A = that side. If they
    # split across both sides, the rewrite is impossible (a semi-join
    # only emits A's columns; we'd lose B's group-key contributions).
    sides_seen: set[str] = set()
    for join_idx in grouping_in_join_space:
        side, _ = join_idx_attribution[join_idx]
        if side != "EITHER":
            sides_seen.add(side)
    if len(sides_seen) > 1:
        # Split — q4 is NOT this case (both ``o_orderkey`` and
        # ``o_orderpriority`` resolve to RHS or EITHER), but other
        # plans might emit it. Skip.
        return None
    if len(sides_seen) == 0:
        # All group keys are join keys (degenerate case). Default A=LEFT.
        a_side = "L"
    else:
        a_side = next(iter(sides_seen))

    # Establish the new join type and the A side's output_names list.
    if a_side == "L":
        new_join_type = algebra.HashJoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
        a_names = left_names
    else:
        new_join_type = algebra.HashJoinRel.JoinType.JOIN_TYPE_RIGHT_SEMI
        a_names = right_names

    # The original aggregate's output column names — we need to preserve
    # this exact list and ordering downstream.
    original_agg_names = list(_hint_output_names(rel))
    if not original_agg_names:
        # Without an output_names hint we can't rebuild the rename project
        # safely. Be conservative and bail.
        return None

    # The original aggregate's output is:
    #   [grouping_expression_0, grouping_expression_1, ..., count_measure_0, ...]
    # Both group keys and count columns are positionally aligned with
    # ``original_agg_names``. After the rewrite, count columns go away
    # (we're a semi-join now — it emits each A row once, no aggregation).
    # That means we can't honour an aggregate that has count measures
    # AND something downstream references them. Conservatively: only
    # accept the case where there are zero count measures, OR all count
    # measure outputs are at positions that we can simply DROP from the
    # rewrite output (the wrapping rename-project just doesn't include
    # them in the final emit).
    #
    # The Project rewrite below selects the kept columns by position;
    # downstream sees a NARROWER schema if count columns are dropped.
    # That changes the visible width — which can break a parent that
    # references the count column by index. So if count measures exist,
    # we have to verify the parent (the Rel above this aggregate) does
    # NOT exist or doesn't reference them. We don't have that context
    # here (this is a node-local rewrite), so be safe:
    if count_measure_indices:
        # Skip when count measures are present; PruneUnusedColumns will
        # remove them on the next iteration if they're truly unused, and
        # then we fire next time. (q4 takes this path.)
        return None

    # Build the new join with the same children and keys, but type
    # switched and output_names equal to the A side's full output_names.
    # The C++ converter sees output_names_size == leftType.size() (or
    # rightType.size()) and selects the *Filter* variant.
    new_keys = []
    for k in hj.keys:
        new_key = algebra.ComparisonJoinKey()
        new_key.CopyFrom(k)
        new_keys.append(new_key)

    new_hj_common = algebra.RelCommon(
        direct=algebra.RelCommon.Direct(),
        hint=algebra.RelCommon.Hint(output_names=list(a_names)),
    )
    new_left = algebra.Rel()
    new_left.CopyFrom(hj.left)
    new_right = algebra.Rel()
    new_right.CopyFrom(hj.right)

    new_join = algebra.Rel(
        hash_join=algebra.HashJoinRel(
            common=new_hj_common,
            left=new_left,
            right=new_right,
            type=new_join_type,
            keys=new_keys,
        )
    )

    # Wrap in a Project that selects the original group-key columns in
    # their original order and re-stamps them under the original
    # aggregate's output_names. Each group key was attributed to a
    # specific side index above; the new join's output column order
    # is exactly ``a_names``, so the side index IS the new join column
    # index for A-side columns. For 'EITHER' columns the side index in
    # ``join_idx_attribution`` is the A-side index by construction
    # (we recorded l_idx for EITHER on the LHS branch and r_idx on the
    # RHS branch); but the chosen ``a_side`` may differ from the side
    # we recorded. For an EITHER column the canonical name appears on
    # both sides — translate to the A side via name lookup.
    rename_exprs: List[algebra.Expression] = []
    a_name_to_idx: Dict[str, int] = {nm: i for i, nm in enumerate(a_names)}
    for join_idx in grouping_in_join_space:
        side, side_idx = join_idx_attribution[join_idx]
        if side == a_side:
            new_idx = side_idx
        elif side == "EITHER":
            # Look up the canonical column name on the A side.
            jn = join_names[join_idx]
            canon = _strip_lr_prefix(jn)
            # Try canonical name then prefixed name.
            if canon in a_name_to_idx:
                new_idx = a_name_to_idx[canon]
            elif jn in a_name_to_idx:
                new_idx = a_name_to_idx[jn]
            else:
                # The "either" side recorded one side's index; for the
                # OPPOSITE side, look up via the paired join key.
                if a_side == "L":
                    paired = _find_paired_lhs_key(hj.keys, side_idx)
                else:
                    paired = _find_paired_rhs_key(hj.keys, side_idx)
                if paired is None:
                    return None
                new_idx = paired
        else:
            # Side != a_side and side != EITHER — should have been
            # rejected by the sides_seen check above. Defensive guard.
            return None
        rename_exprs.append(_make_field_ref(new_idx))

    rename_proj = algebra.Rel(
        project=algebra.ProjectRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(
                    output_names=list(original_agg_names),
                ),
            ),
            input=new_join,
            expressions=rename_exprs,
        )
    )

    boltmlDebugLog(
        "inner_dedup_to_semi",
        f"rewrote: a_side={a_side} keys={len(hj.keys)} "
        f"groupings={len(agg.grouping_expressions)} "
        f"join_out_width={len(join_names)}->{len(a_names)} "
        f"new_join_type={'LEFT_SEMI' if a_side == 'L' else 'RIGHT_SEMI'}",
    )
    return rename_proj


# ---------------------------------------------------------------------------
# Helpers (cont.)
# ---------------------------------------------------------------------------


def _key_field_index(
    ref: algebra.Expression.FieldReference,
) -> Optional[int]:
    """Return the top-level struct-field index of *ref*, or None for non-direct."""
    if not ref.HasField("direct_reference"):
        return None
    seg = ref.direct_reference
    if seg.WhichOneof("reference_type") != "struct_field":
        return None
    if seg.struct_field.HasField("child"):
        return None
    return seg.struct_field.field


def _find_paired_rhs_key(
    keys: List[algebra.ComparisonJoinKey], lhs_idx: int
) -> Optional[int]:
    """For an LHS join-key field index, return the paired RHS field index.

    Returns ``None`` if no key references that LHS index.
    """
    for k in keys:
        key_lhs = _key_field_index(k.left)
        key_rhs = _key_field_index(k.right)
        if key_lhs == lhs_idx and key_rhs is not None:
            return key_rhs
    return None


def _find_paired_lhs_key(
    keys: List[algebra.ComparisonJoinKey], rhs_idx: int
) -> Optional[int]:
    """For an RHS join-key field index, return the paired LHS field index."""
    for k in keys:
        key_lhs = _key_field_index(k.left)
        key_rhs = _key_field_index(k.right)
        if key_rhs == rhs_idx and key_lhs is not None:
            return key_lhs
    return None


def _substitute_through_chain(
    expr: algebra.Expression,
    project_chain: List[algebra.ProjectRel],
) -> Optional[algebra.Expression]:
    """Walk the project chain outermost-to-innermost, substituting field refs.

    Each project's expression at output position ``i`` is the substitute for
    a field-ref ``i`` at that project's OUTPUT level. Walking in order from
    outermost to innermost translates an expression at the agg level (which
    sees the OUTERMOST project's output) all the way down to the join's
    output level.

    Returns ``None`` if any field index falls outside its project's width.
    """
    cur = expr
    for proj in project_chain:
        proj_width = len(proj.expressions)
        idx = direct_field_index(cur)
        if idx is None:
            # Not a direct field-ref — substitute_field_refs handles
            # nested expressions transparently, but we still validate
            # any contained refs are in range.
            cur = substitute_field_refs(
                cur,
                lambda i, _exprs=proj.expressions, _w=proj_width: (
                    _exprs[i] if 0 <= i < _w else None
                ),
            )
            continue
        if not (0 <= idx < proj_width):
            return None
        cur = substitute_field_refs(
            cur,
            lambda i, _exprs=proj.expressions, _w=proj_width: (
                _exprs[i] if 0 <= i < _w else None
            ),
        )
    return cur
