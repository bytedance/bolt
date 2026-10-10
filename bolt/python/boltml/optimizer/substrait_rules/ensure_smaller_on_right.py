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

"""Local fallback that swaps a join's two inputs so the smaller side ends up
on the right (build) side of Bolt's HashJoin.

Implementation strategy
-----------------------
After the swap, we keep each input rel exactly as it was: the inner
prefix Project's ``hint.output_names`` continue to use the prefixes
the original side had (so the rel that was the LHS keeps its ``l_<...>``
prefix names even though it now occupies the RIGHT slot, and vice
versa). This is intentional. Bolt's ``SubstraitToBoltPlan.cpp`` looks
at each input rel's row type to compute the join's natural output
schema (the concatenation of left + right column names), and the
hint then selects/reorders columns from that concat. Because the
original output hint already contains ``[l_<lhs_cols>, r_<rhs_nonkey_cols>]``
and those same names still exist in the post-swap natural concat (the
inner prefix Projects weren't touched), we can KEEP the original
output hint unchanged and let the C++ resolve names by lookup.
The wrapping re-emit Project at the top is then a structural no-op
(its hint is already what parents want), so we can elide it entirely.

Why
---
Bolt's ``HashJoinRel`` builds the hash table on the RIGHT input
(``HashBuild.cpp:97``) and probes from the LEFT. The build side dictates
the hash-table memory footprint: 60M rows on the build side bloats memory
and tanks performance. ``JoinReorder`` (the global cost-based brute-force
rule) is the primary defense: at every join in its rewritten tree it forces
the smaller side onto the build (right) input. But ``JoinReorder`` requires
the entire join sub-graph to be reorderable — pure INNER, with estimable
leaf cardinalities, no ``post_join_filter``, etc. When extraction fails
(e.g. q18, where the schema-tracking logic in JoinReorder's full-rebuild
path can't bind a join key after walking through aggregate-filter-rename
chains), the original source-order shape is left intact. If the user wrote
``small.join(big)``, Bolt builds the hash table over the 60M-row big side.
This rule fixes that as a local fallback: walk every ``HashJoinRel``, and
if the LEFT input is significantly larger than the RIGHT, swap them.

Scope
-----
* Only ``hash_join`` rels with ``JOIN_TYPE_INNER`` are eligible — semi /
  anti / outer joins encode left-vs-right asymmetry in their semantics.
* Only swaps when ``right_card > left_card * 2``. The 2x threshold
  avoids flapping on noisy estimates (a swap that puts a 100k-row build
  side in place of a 90k-row build side does not move the needle and
  may regress if the estimate was wrong). Bolt's ``HashJoin`` builds on
  the RIGHT, so the bad case is "big on right" — that's what triggers
  the swap.
* Cardinality is estimated by walking down through Project / Filter /
  Aggregate / nested join rels to leaf parquet reads. Reuses the
  ``_estimate_leaf_cardinality`` helper from ``join_reorder.py`` and
  extends it to traverse nested ``hash_join`` (using
  ``min(left, right)`` as the join-output estimate — daft's no-stats
  fallback).
* If either side's cardinality cannot be estimated, the swap is skipped:
  swapping based on noise is worse than leaving the original order alone.

What this rule does NOT do
--------------------------
* It does not enumerate alternative join orders (that's
  ``JoinReorder``'s job; this rule runs strictly *after* it as a
  fallback for cases that rule could not handle).
* It does not flow filters or stats across joins — only direct walk
  through the local rel tree.
* It does not modify the join's output schema. The original
  ``output_names`` hint is preserved verbatim — see
  "Implementation strategy" above for why.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

from substrait.proto import algebra, plan as plan_pb2

from ...logging import boltmlDebugLog
from .base import Rule, get_rel_kind
from .join_reorder import (
    _estimate_leaf_cardinality as _join_reorder_estimate,
    _hint_output_names,
)


# Minimum size ratio (right / left) above which we trigger a swap.
# Conservative — avoids swapping on noisy estimates while still
# catching pathological orderings (q18's 40,000x ratio trips it
# instantly).
_SWAP_RATIO_THRESHOLD: float = 2.0

# Minimum absolute right-side cardinality below which we don't swap
# regardless of ratio. Rationale: the swap moves a derived (joined or
# aggregated) sub-tree onto the BUILD side, replacing a flat scan that
# the executor can pipeline-parallelize well. When the original
# build-side scan is "small" (say <= ~30M rows), the parallel-scan
# build is FASTER than waiting for an upstream join, even though the
# resulting build table is smaller. The swap also indirectly perturbs
# ``JoinReorder`` (its second-pass enumeration sees a different input
# shape and may pick a different — sometimes worse — global order).
#
# Empirical thresholds (TPC-H SF=10):
#   * Below ~20M rows on the original build: swap regresses (q3 6M,
#     q7/q9 15M all neutral or worse).
#   * 60M rows on the original build (q18 lineitem): swap drops wall
#     from 91s to 71s, a 22% improvement.
#
# Pick 30M as a defensive floor: comfortably above the 15M of q7/q9
# (which we want to leave alone) and well below 60M of q18 (which
# the rule must catch). Future stat-based rule could remove this
# floor by accurately predicting build pipeline serialization.
_MIN_SWAP_RIGHT_CARDINALITY: float = 3.0e7


# ---------------------------------------------------------------------------
# Cardinality estimation extended to walk through nested joins
# ---------------------------------------------------------------------------


def _estimate_cardinality(rel: algebra.Rel) -> Optional[float]:
    """Return an estimated row count for *rel*, walking through joins.

    Walks through Project / Filter / Aggregate / Sort / Fetch / Exchange /
    HashJoin / MergeJoin / Join down to parquet ``Read`` rels. The leaf
    case (parquet ``Read``, ``Filter`` selectivity heuristic) reuses
    ``join_reorder._estimate_leaf_cardinality`` to avoid divergence
    from ``JoinReorder``'s cost model.

    Recurses through ``project`` / ``filter`` / ``aggregate`` /
    ``sort`` / ``fetch`` / ``exchange`` / ``extension_single``
    explicitly here (rather than delegating to the join_reorder helper)
    because their inputs may themselves be ``hash_join`` rels — which
    the join_reorder helper does NOT handle (it expects a leaf
    sub-tree of project/filter above a single Read). Without explicit
    recursion, we'd return None for any join sub-tree wrapped in a
    Project (which is exactly what JoinReorder's rewritten output looks
    like, and what Bolt's join builder always produces around its
    inputs).

    Returns ``None`` if any sub-rel is a kind we don't know how to
    estimate (e.g. ``extension_table`` reads, ``virtual_table`` reads,
    ``set`` ops). The caller treats unknown cardinalities as "do not
    swap this join" — better to leave the source order alone than
    rearrange based on noise.
    """
    kind = get_rel_kind(rel)
    if kind is None:
        return None
    if kind in ("hash_join", "merge_join", "join"):
        inner = getattr(rel, kind)
        l_card = _estimate_cardinality(inner.left)
        r_card = _estimate_cardinality(inner.right)
        if l_card is None or r_card is None:
            return None
        # Output of an inner equi-join on a PK-FK pair is bounded above by
        # the FK-side cardinality (i.e. max(l, r) for star-shape queries).
        # But without distinct-count stats, daft uses min(l, r) as a
        # neutral fallback. We mirror that — over-estimating helps prefer
        # actually-small sides for build placement.
        return min(l_card, r_card)
    # Recurse through pass-through-shaped rels ourselves so we can
    # descend into nested hash_joins (which the join_reorder helper
    # treats as None).
    if kind == "project":
        return _estimate_cardinality(rel.project.input)
    if kind == "sort":
        return _estimate_cardinality(rel.sort.input)
    if kind == "fetch":
        return _estimate_cardinality(rel.fetch.input)
    if kind == "exchange":
        return _estimate_cardinality(rel.exchange.input)
    if kind == "extension_single":
        return _estimate_cardinality(rel.extension_single.input)
    if kind == "filter":
        # Reuse the join_reorder helper here — it applies the
        # range-predicate selectivity heuristic. Walk down to the
        # filter's input ourselves so the helper sees a leaf.
        child = _estimate_cardinality(rel.filter.input)
        if child is None:
            return None
        # Mirror the helper's selectivity calculation. We can't call the
        # helper directly because its `filter` branch only handles a
        # filter whose input is itself estimable by it (a leaf chain).
        from .join_reorder import (
            _expr_has_range_predicate,
            _FILTER_RANGE_SELECTIVITY,
        )

        sel = (
            _FILTER_RANGE_SELECTIVITY
            if _expr_has_range_predicate(rel.filter.condition)
            else 1.0
        )
        return max(1.0, child * sel)
    if kind == "aggregate":
        # Aggregations collapse rows. The output row count is bounded
        # above by the input row count (each input row contributes to
        # at most one group). Without distinct-count stats we don't
        # know how much it actually collapses — so we use the input
        # cardinality as a conservative upper bound for swap decisions.
        #
        # Note: ``join_reorder._estimate_leaf_cardinality`` caps at
        # 10000 here because it wants aggregates to look "small" so
        # they get picked up onto the build side. That heuristic helps
        # global enumeration but hurts pairwise swap decisions —
        # capping at 10000 fooled the rule into swapping a partial-
        # aggregate input (post ``PushAggThroughJoin``) onto the
        # build side when its actual output (e.g. 1.5M distinct
        # orderkeys for q3's lineitem) is much larger than the
        # build-side dimension table. We use the input row count
        # instead to avoid that false positive.
        child = _estimate_cardinality(rel.aggregate.input)
        if child is None:
            return None
        return child
    # Leaf cases (read, etc.) — defer to the join_reorder helper which
    # handles parquet metadata reads and rejects extension/virtual.
    return _join_reorder_estimate(rel)


# ---------------------------------------------------------------------------
# Helpers for the swap
# ---------------------------------------------------------------------------


def _swap_join_keys(
    keys: List[algebra.ComparisonJoinKey],
) -> List[algebra.ComparisonJoinKey]:
    """Return a new list of join keys with each key's left/right swapped.

    The comparison type is preserved verbatim. ``ComparisonJoinKey``'s
    ``ComparisonType`` only encodes equality-flavored comparisons (EQ,
    IS_NOT_DISTINCT_FROM, MIGHT_EQUAL — see ``algebra.proto`` line
    760), all of which are symmetric — swapping operands does not
    change the semantics. There is no LT/GT/etc. to mirror.
    """
    swapped: List[algebra.ComparisonJoinKey] = []
    for k in keys:
        new_key = algebra.ComparisonJoinKey()
        new_key.CopyFrom(k)
        # Snapshot the original left ref before overwriting it.
        old_left = algebra.Expression.FieldReference()
        old_left.CopyFrom(k.left)
        new_key.left.CopyFrom(k.right)
        new_key.right.CopyFrom(old_left)
        swapped.append(new_key)
    return swapped


def _validate_original_hint(
    original: Tuple[str, ...],
    n_left_old: int,
    n_right_old: int,
    n_keys: int,
) -> bool:
    """Sanity-check that the join's original hint matches Bolt's standard
    ``[l_<lhs_cols>, r_<rhs_nonkey_cols>]`` shape.

    If it does, the hint can be reused verbatim after the swap (the
    inner prefix Projects of each input retain their original ``l_``/``r_``
    names regardless of which slot they sit in). If it doesn't (e.g. a
    later optimizer pass already rewrote the hint), we abort the swap
    rather than risk producing a malformed plan.
    """
    expected_len = n_left_old + (n_right_old - n_keys)
    if len(original) != expected_len:
        return False
    for nm in original[:n_left_old]:
        if not nm.startswith("l_"):
            return False
    for nm in original[n_left_old:]:
        if not nm.startswith("r_"):
            return False
    return True


# ---------------------------------------------------------------------------
# Top-level walker
# ---------------------------------------------------------------------------


def _try_swap(rel: algebra.Rel) -> Optional[algebra.Rel]:
    """If *rel* is a swap-eligible HashJoinRel and the RHS build is too big, swap.

    Returns the input-swapped HashJoinRel (with ``output_names`` hint
    preserved verbatim — see module docstring for why this is sound),
    or ``None`` to leave the rel alone.
    """
    if get_rel_kind(rel) != "hash_join":
        return None
    jr = rel.hash_join
    # Only swap pure INNER joins. Semi / anti / outer encode left-vs-right
    # asymmetry; swapping changes the semantics.
    if jr.type != algebra.HashJoinRel.JoinType.JOIN_TYPE_INNER:
        return None
    if jr.HasField("post_join_filter"):
        # post_join_filter expressions are indexed against the join's
        # output column layout; the swap would shift those indices.
        # Skip rather than rewrite the filter (out of scope for V1).
        return None
    if not jr.keys:
        return None

    l_card = _estimate_cardinality(jr.left)
    r_card = _estimate_cardinality(jr.right)
    if l_card is None or r_card is None:
        return None
    # Bolt builds the hash table on the RIGHT input. The smaller side
    # should be on the RIGHT (small build = small memory + fast probe).
    # We swap only when (a) the right side is significantly LARGER than
    # the left — i.e., the current placement forces a big build —
    # AND (b) the right side is large enough in absolute terms that
    # the build cost dominates pipeline-parallel scan throughput
    # (see ``_MIN_SWAP_RIGHT_CARDINALITY`` for the empirical threshold).
    if r_card <= l_card * _SWAP_RATIO_THRESHOLD:
        return None
    if r_card < _MIN_SWAP_RIGHT_CARDINALITY:
        return None

    # Snapshot the original output_names before swapping.
    original_names = _hint_output_names(rel)
    if not original_names:
        return None

    # Validate the original hint shape so we can reuse it verbatim.
    l_names = _hint_output_names(jr.left)
    r_names = _hint_output_names(jr.right)
    if not l_names or not r_names:
        return None
    n_left_old = len(l_names)
    n_right_old = len(r_names)
    n_keys = len(jr.keys)
    if not _validate_original_hint(original_names, n_left_old, n_right_old, n_keys):
        return None

    # Snapshot the inputs and keys before mutating anything.
    old_left = algebra.Rel()
    old_left.CopyFrom(jr.left)
    old_right = algebra.Rel()
    old_right.CopyFrom(jr.right)
    new_keys = _swap_join_keys(list(jr.keys))

    # Build the swapped join: inputs swapped, keys swapped, original
    # hint preserved verbatim.
    #
    # Why we keep the original hint: the inner prefix Projects of each
    # input retain their old ``l_``/``r_`` prefix names regardless of
    # which slot they sit in (we only swap the input slots, not the
    # children themselves). So the join's natural output (concatenation
    # of left + right column names; see SubstraitToBoltPlan.cpp:1276) now
    # contains all of the OLD RHS columns first (still named ``r_<...>``)
    # then all of the OLD LHS columns (still named ``l_<...>``). The
    # original hint already references columns by these prefixed names —
    # ``l_<lhs_col>`` and ``r_<rhs_nonkey_col>`` — and those names still
    # exist in the post-swap natural concat (just at different
    # positions). Bolt's name-based hint resolution (lines 1287-1300)
    # finds them by name lookup, so the hint resolves correctly without
    # any rewrite. This makes the output schema of the swapped join
    # IDENTICAL to the original — no wrapping Project needed.
    new_join = algebra.HashJoinRel()
    new_join.CopyFrom(jr)
    new_join.left.CopyFrom(old_right)
    new_join.right.CopyFrom(old_left)
    del new_join.keys[:]
    new_join.keys.extend(new_keys)
    # output_names stays unchanged (it's already the correct subset of
    # the post-swap natural concat).

    boltmlDebugLog(
        "ensure_smaller_on_right",
        f"swapping join: l_card={l_card:.0f} r_card={r_card:.0f} "
        f"(r/l ratio={r_card / max(l_card, 1):.1f}x)",
    )
    return algebra.Rel(hash_join=new_join)


def _walk(rel: algebra.Rel) -> algebra.Rel:
    """Bottom-up walk: swap eligible joins after their children are walked.

    Walking bottom-up means a child join has already had a chance to
    be swapped before the parent considers itself. A swap is purely
    local — it does not re-walk children — so this single bottom-up
    pass converges (and the rule is idempotent on a second pipeline
    iteration: the smaller side already on the right means
    ``r_card <= l_card * threshold`` and the rule no-ops).
    """
    out = algebra.Rel()
    out.CopyFrom(rel)
    kind = get_rel_kind(out)
    if kind is None:
        return out

    # Recurse into children first (bottom-up).
    if kind == "read":
        return out
    inner = getattr(out, kind)
    if kind in (
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
        if inner.HasField("input"):
            new_child = _walk(inner.input)
            inner.input.CopyFrom(new_child)
    elif kind in ("join", "cross", "hash_join", "merge_join", "nested_loop_join"):
        if inner.HasField("left"):
            new_left = _walk(inner.left)
            inner.left.CopyFrom(new_left)
        if inner.HasField("right"):
            new_right = _walk(inner.right)
            inner.right.CopyFrom(new_right)
    elif kind == "set":
        for i in range(len(inner.inputs)):
            new_child = _walk(inner.inputs[i])
            inner.inputs[i].CopyFrom(new_child)

    # Now try the local swap on this rel.
    rewritten = _try_swap(out)
    if rewritten is not None:
        return rewritten
    return out


# ---------------------------------------------------------------------------
# Rule
# ---------------------------------------------------------------------------


class EnsureSmallerOnRight(Rule):
    """Swap a join's two inputs if the left is significantly larger.

    Pure: the input ``Plan`` is not mutated. Idempotent: the rule produces
    a stable plan after one pass — once the smaller side is on the
    right, ``l_card <= r_card * 2`` holds and the rule no-ops on the
    second pass.

    Designed as a *fallback* to ``JoinReorder``: it should run AFTER
    that rule has had its chance, and only fix joins JoinReorder
    couldn't reorganize (e.g. cases where its full-tree extraction
    failed schema-tracking checks).
    """

    name: str = "ensure_smaller_on_right"

    def apply(self, p: plan_pb2.Plan) -> plan_pb2.Plan:
        out = plan_pb2.Plan()
        out.CopyFrom(p)
        for plan_rel in out.relations:
            if not plan_rel.HasField("root"):
                continue
            if not plan_rel.root.HasField("input"):
                continue
            new_root = _walk(plan_rel.root.input)
            plan_rel.root.input.CopyFrom(new_root)
        return out
