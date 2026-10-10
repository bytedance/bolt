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

"""Push a partial aggregation below an inner ``HashJoinRel``.

This is the classical "AggregateJoinTranspose" rewrite. Given:

::

    Aggregate[K, M(args)]                        (kSingle)
      Project*[exprs]              (optional, all expressions)
        HashJoin[J](L, R)          (INNER, no post_join_filter)

it rewrites to:

::

    Aggregate[K, final(M)]                       (kFinal)
      Project*[exprs]
        HashJoin[J](
          Aggregate[K_L ∪ J_L, partial(M_L)](L),    (kPartial)
          Aggregate[K_R ∪ J_R, partial(M_R)](R),    (kPartial)
        )

so the join sees fewer rows when the LHS / RHS contains a high-multiplicity
"fact" relationship that the group-by collapses anyway.

Why decomposable phases work in Bolt
------------------------------------

Bolt's executor implements the four-phase Velox-style aggregation contract
(``kPartial`` / ``kIntermediate`` / ``kFinal`` / ``kSingle``) at
``bolt/exec/Aggregate.h`` and ``bolt/exec/HashAggregation.h``. The
Substrait-to-Bolt converter at ``bolt/substrait/SubstraitToBoltPlan.cpp``
already maps Substrait's ``AggregationPhase`` enum to the matching
``AggregationNode::Step`` (``INITIAL_TO_INTERMEDIATE → kPartial``,
``INTERMEDIATE_TO_RESULT → kFinal``). So nothing needs to change C++-side;
this rule lives entirely in Python.

Decomposable function allowlist
-------------------------------

Only ``sum``, ``count``, ``min``, ``max`` are pushed through. These are the
classical decomposable aggregates whose intermediate type equals the
result type (in Bolt's TPC-H coverage where all numerics are float64 or
int64 — see ``bolt/functions/prestosql/aggregates/SumAggregate.cpp``,
``CountAggregate.cpp``). This lets us keep the partial measure's
``output_type`` byte-equal to the original (no per-function intermediate
type lookup), which would otherwise require importing the Bolt function
signature registry.

Anything else (``avg``/``mean``, ``stddev``, ``median``, distinct ``count``,
boolean ``and``/``or``) is rejected outright. The rule fails fast on
unrecognised aggregate URIs rather than silently skipping individual
measures — a single non-decomposable measure poisons the whole rewrite.

Cross-side measure rejection
----------------------------

Each measure's argument expressions are walked; the field-references they
reach are bucketed per-side via the project chain that translates them
back to the join's ``l_``/``r_``-prefixed output. A measure that touches
columns from BOTH sides (e.g. ``sum(L.a * R.b)``) cannot be pushed —
the partial agg on either side wouldn't have all the inputs. Skipped.

Other guards
------------

* Only ``HashJoinRel`` of ``JOIN_TYPE_INNER`` with no ``post_join_filter``.
* Empty ``grouping_expressions`` (a global agg) — final still re-aggregates
  everything, no benefit. Skipped.
* The Project chain between Aggregate and HashJoin can be any number of
  pure projects; expressions are substituted through the chain so the
  attribution of each measure-arg field to a join-output column is exact.
* The ``RelCommon.Hint.output_names`` of the join must be present and
  use Bolt's standard ``l_``/``r_`` prefix scheme so we can attribute
  each output column to a side.

Cost-driven skips
-----------------

Even when the structural guards pass, we skip when the rewrite is
unlikely to pay off:

* A side gets NO measures (its only role would be a DISTINCT) — that
  side is fed straight into the new join unchanged. If neither side
  gets measures, the whole rewrite is skipped.
* A measure-receiving side's immediate child is itself an Aggregate
  (TPC-H Q18's ``HAVING SUM(l_quantity) > 300`` lineitem-side pattern).
  Pushing on top of an existing partial-agg is double-aggregation
  with no row reduction.
* All measure args are direct field-refs (no scalar computation).
  Pushing those just adds a partial-agg step; the real win of this
  rewrite is moving the per-row arithmetic of ``sum(a*b)``-style
  expressions below the join so it runs on the side's input rather
  than the join's typically-larger output.

Output schema preservation
--------------------------

The rewritten root is byte-equivalent in shape to the original Aggregate's
output: same ``RelCommon.Hint.output_names``, same column ordering (by
construction — the final agg's measures are at the same positions as the
original's). Downstream consumers see no schema change.

Idempotency
-----------

After rewriting, the pattern ``Aggregate(Project(HashJoin))`` no longer
matches because the inner aggregations don't fit the (Aggregate(Project(Join)))
shape (they sit directly above the join leaves). A re-run produces a
byte-equal Plan, so the fixpoint loop in ``SubstraitOptimizerPipeline``
converges in one extra iteration.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple

from substrait.proto import algebra, plan

from ...logging import boltmlDebugLog
from ._expression import (
    collect_field_indices,
    direct_field_index,
    is_direct_field_ref,
    substitute_field_refs,
)
from .base import Rule, get_rel_kind, rewrite_plan_root


# ---------------------------------------------------------------------------
# Allowlist
# ---------------------------------------------------------------------------

# Aggregation function URIs that are decomposable into partial+final phases
# AND whose Bolt-side intermediate type equals their final output type for
# every input type used in TPC-H (float64 / int64). See
# ``bolt/functions/prestosql/aggregates/SumAggregate.cpp`` (intermediateType
# matches returnType for double / bigint signatures) and
# ``CountAggregate.cpp`` (intermediateType is always bigint).
#
# IMPORTANT: do NOT add ``avg``, ``mean``, ``stddev``, ``median``,
# ``count_distinct``, ``bool_and``, ``bool_or`` here. ``avg`` is decomposable
# but its intermediate type is a struct (sum, count) whose substrait
# representation we'd have to synthesize from scratch. The rest are either
# not decomposable or have intermediate types that differ from the final
# type and would need the same special handling.
_DECOMPOSABLE_AGG_NAMES: frozenset[str] = frozenset({"sum", "count", "min", "max"})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_field_ref(idx: int) -> algebra.Expression:
    """Build a fresh ``Expression`` for a top-level direct field reference."""
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


def _strip_uniform_prefix(name: str) -> str:
    """Strip a leading ``l_`` or ``r_`` prefix if present, else return unchanged."""
    if name.startswith("l_") or name.startswith("r_"):
        return name[2:]
    return name


def _agg_function_uri_name(
    measure: algebra.AggregateRel.Measure,
    extension_lookup: Dict[int, str],
) -> Optional[str]:
    """Return the unprefixed Substrait function name for *measure*.

    Looks up the measure's ``function_reference`` in ``extension_lookup``
    (anchor → URI string of form ``"name:arg_sig"``), and returns the
    part before the first ``:``. Returns ``None`` if the anchor is not
    in the lookup.
    """
    if not measure.HasField("measure"):
        return None
    anchor = measure.measure.function_reference
    full = extension_lookup.get(anchor)
    if full is None:
        return None
    if ":" in full:
        return full.split(":", 1)[0]
    return full


def _build_extension_lookup(p: plan.Plan) -> Dict[int, str]:
    """Return ``{function_anchor: name_string}`` for every extension in *p*."""
    out: Dict[int, str] = {}
    for ext in p.extensions:
        kind = ext.WhichOneof("mapping_type")
        if kind == "extension_function":
            out[ext.extension_function.function_anchor] = ext.extension_function.name
    return out


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


# ---------------------------------------------------------------------------
# Rule
# ---------------------------------------------------------------------------


class PushAggThroughJoin(Rule):
    """Push partial aggregation below an inner HashJoin (AggregateJoinTranspose).

    Pure: input ``Plan`` is not mutated. Idempotent: a second run over the
    rule's output produces a byte-equal Plan.
    """

    name: str = "push_agg_through_join"

    def apply(self, p: plan.Plan) -> plan.Plan:
        ext_lookup = _build_extension_lookup(p)

        def _try(rel: algebra.Rel) -> Optional[algebra.Rel]:
            return _try_push(rel, ext_lookup)

        return rewrite_plan_root(p, _try, bottom_up=True)


# ---------------------------------------------------------------------------
# Match + rewrite
# ---------------------------------------------------------------------------


def _try_push(
    rel: algebra.Rel,
    ext_lookup: Dict[int, str],
) -> Optional[algebra.Rel]:
    """Match the pattern; return rewritten Rel or ``None`` to leave unchanged."""
    if get_rel_kind(rel) != "aggregate":
        return None
    agg = rel.aggregate

    # Skip global aggregations — final re-aggregates everything anyway.
    if not agg.grouping_expressions:
        return None

    # Reject anything weird in the aggregate envelope.
    if agg.HasField("common") and agg.common.WhichOneof("emit_kind") not in (
        None,
        "direct",
    ):
        return None

    # ``measures`` must be non-empty; an aggregate with no measures is
    # effectively a DISTINCT and pushdown isn't beneficial.
    if not agg.measures:
        return None

    # Validate every measure: in allowlist, single-side, no measure-level
    # filter, simple structure.
    for m in agg.measures:
        if m.HasField("filter"):
            return None
        nm = _agg_function_uri_name(m, ext_lookup)
        if nm is None or nm not in _DECOMPOSABLE_AGG_NAMES:
            return None
        # Reject distinct (the Substrait `invocation` field would set
        # AGGREGATION_INVOCATION_DISTINCT).
        if (
            m.measure.invocation
            == algebra.AggregateFunction.AGGREGATION_INVOCATION_DISTINCT
        ):
            return None
        # Reject sorted aggregates (rare but possible).
        if len(m.measure.sorts) > 0:
            return None
        # Phase must be the "complete" phase. If the rule already ran on
        # this site, the phase will be INTERMEDIATE_TO_RESULT and we must
        # not re-push.
        if m.measure.phase not in (
            algebra.AggregationPhase.AGGREGATION_PHASE_UNSPECIFIED,
            algebra.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT,
        ):
            return None

    # Walk through 0 or more intermediate ProjectRels until we hit either
    # the join or something else. Each project may transform expressions
    # so we accumulate a chain — innermost project first — that any
    # expression at the agg level can be substituted through to reach
    # the join's output coordinate space.
    child = agg.input
    project_chain: List[algebra.ProjectRel] = []
    while get_rel_kind(child) == "project":
        # Skip if the project has emit (we can't reason about it).
        if child.project.HasField("common") and child.project.common.WhichOneof(
            "emit_kind"
        ) not in (None, "direct"):
            return None
        project_chain.append(child.project)
        child = child.project.input
    join_holder = child

    if get_rel_kind(join_holder) != "hash_join":
        return None
    hj_rel = join_holder
    hj = hj_rel.hash_join

    # Only INNER joins, no post_join_filter, must have keys (no cross product).
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

    # Need the join's output_names to attribute columns to sides via the
    # ``l_``/``r_`` prefix scheme.
    join_names = _hint_output_names(hj_rel)
    if not join_names:
        return None
    # Need each side's output_names too.
    left_names = _hint_output_names(hj.left)
    right_names = _hint_output_names(hj.right)
    if not left_names or not right_names:
        return None

    # Each name in the join's hint must be uniquely attributable to a side.
    # Bolt's prefix scheme guarantees the prefix; we double-check.
    join_idx_to_side: List[Tuple[str, int]] = []  # ('L'/'R', side_idx)
    for jn in join_names:
        if jn.startswith("l_") and jn in set(left_names):
            join_idx_to_side.append(("L", left_names.index(jn)))
        elif jn.startswith("r_") and jn in set(right_names):
            join_idx_to_side.append(("R", right_names.index(jn)))
        else:
            # Untrackable column; bail.
            return None

    # Each join key must use direct field refs into LHS / RHS; collect the
    # join key positions on each side.
    lhs_key_positions: List[int] = []
    rhs_key_positions: List[int] = []
    for k in hj.keys:
        l_idx = _key_field_index(k.left)
        r_idx = _key_field_index(k.right)
        if l_idx is None or r_idx is None:
            return None
        if not (0 <= l_idx < len(left_names) and 0 <= r_idx < len(right_names)):
            return None
        # Reject non-equality comparisons.
        if k.HasField("comparison"):
            cmp = k.comparison
            if cmp.WhichOneof("inner_type") != "simple":
                return None
            if cmp.simple != algebra.ComparisonJoinKey.SIMPLE_COMPARISON_TYPE_EQ:
                return None
        lhs_key_positions.append(l_idx)
        rhs_key_positions.append(r_idx)

    # Translate every grouping_expression and every measure-argument field
    # reference back to a (side, side_idx) pair.
    #
    # Grouping expressions in the original aggregate refer to columns of
    # ``agg.input``. If a project sits between, those refs are to the
    # project's outputs, which we substitute back to the project's inputs
    # (= join's outputs) via the project's expression list. If no project,
    # they directly reference join outputs.
    #
    # We require every grouping expression to be a direct field reference
    # AT THIS LEVEL after substitution — non-direct group keys are rare
    # but not free to push (they'd have to be evaluated on one side only).
    # If a project sits between, we substitute the projection expression
    # in place of the field-ref, then check.
    def _substitute_through_project(
        e: algebra.Expression,
    ) -> Optional[algebra.Expression]:
        """Walk the project chain inside-out, substituting field refs.

        ``project_chain`` is ordered outermost-to-innermost (the way we
        descended from the agg). Each project's expression at output
        position ``i`` is the substitute for a field-ref ``i`` at that
        project's OUTPUT level. To translate an expression at the agg
        level (which sees the OUTERMOST project's output) all the way
        down to the JOIN's output level, we substitute through each
        project in order from outermost to innermost.
        """
        cur = e
        for proj in project_chain:
            proj_width = len(proj.expressions)
            for idx in collect_field_indices(cur):
                if not (0 <= idx < proj_width):
                    return None
            cur = substitute_field_refs(
                cur,
                lambda idx, _exprs=proj.expressions, _w=proj_width: (
                    _exprs[idx] if 0 <= idx < _w else None
                ),
            )
        return cur

    # For each grouping expression, after substitution, must be a direct
    # field ref into the join's output. We then look up the side via
    # join_idx_to_side.
    grouping_substituted: List[algebra.Expression] = []
    for g in agg.grouping_expressions:
        sub = _substitute_through_project(g)
        if sub is None:
            return None
        if not is_direct_field_ref(sub):
            # Computed group key — would require pushing the computation
            # to one side. Skip for V1.
            return None
        grouping_substituted.append(sub)

    grouping_side_positions: List[Tuple[str, int]] = []
    for sub in grouping_substituted:
        idx = direct_field_index(sub)
        if idx is None or not (0 <= idx < len(join_idx_to_side)):
            return None
        grouping_side_positions.append(join_idx_to_side[idx])

    # For each measure, after substitution of arguments, collect referenced
    # field indices in the JOIN's output space, attribute each to a side,
    # and require that all references for one measure come from the SAME
    # side. ``count(*)`` (an aggregate with no value arguments, or one
    # argument that's a constant) lands on neither side — we associate it
    # with the LHS by default since count of partials sums to total count.
    #
    # We also need substituted arguments — these expressions become the
    # arguments of the partial-agg measures.
    measure_side: List[str] = []  # 'L', 'R', or 'NONE' for global count
    measure_arg_exprs: List[List[algebra.Expression]] = []  # post-substitution args
    for m in agg.measures:
        sides_seen: Set[str] = set()
        sub_args: List[algebra.Expression] = []
        for arg in m.measure.arguments:
            kind = arg.WhichOneof("arg_type")
            if kind != "value":
                # Non-value arg (type / enum) — don't try.
                return None
            sub = _substitute_through_project(arg.value)
            if sub is None:
                return None
            sub_args.append(sub)
            for idx in collect_field_indices(sub):
                if not (0 <= idx < len(join_idx_to_side)):
                    return None
                sides_seen.add(join_idx_to_side[idx][0])
        if len(sides_seen) > 1:
            # Cross-side measure (e.g. sum(L.a * R.b)) — cannot push.
            return None
        elif len(sides_seen) == 1:
            measure_side.append(next(iter(sides_seen)))
        else:
            # ``count(*)`` style — no field refs. Default to LHS.
            measure_side.append("L")
        measure_arg_exprs.append(sub_args)

    # All checks passed. Build the rewrite.

    # LHS partial-agg group keys: the LHS-side grouping refs ∪ the LHS
    # join-key positions. Preserve original order: grouping keys first
    # (in their original order), then any LHS join keys not already
    # present in the grouping.
    lhs_kept_positions: List[int] = []
    seen_lhs: Set[int] = set()
    for side, side_idx in grouping_side_positions:
        if side == "L" and side_idx not in seen_lhs:
            lhs_kept_positions.append(side_idx)
            seen_lhs.add(side_idx)
    for jk in lhs_key_positions:
        if jk not in seen_lhs:
            lhs_kept_positions.append(jk)
            seen_lhs.add(jk)
    # CRITICAL invariant: every join key must be in the partial-agg's
    # grouping. Otherwise the partial collapses rows that the join would
    # have kept apart, and the final agg loses information.
    assert all(jk in seen_lhs for jk in lhs_key_positions), (
        f"LHS partial agg missing join key: lhs_kept={lhs_kept_positions} "
        f"lhs_join_keys={lhs_key_positions}"
    )

    rhs_kept_positions: List[int] = []
    seen_rhs: Set[int] = set()
    for side, side_idx in grouping_side_positions:
        if side == "R" and side_idx not in seen_rhs:
            rhs_kept_positions.append(side_idx)
            seen_rhs.add(side_idx)
    for jk in rhs_key_positions:
        if jk not in seen_rhs:
            rhs_kept_positions.append(jk)
            seen_rhs.add(jk)
    assert all(jk in seen_rhs for jk in rhs_key_positions), (
        f"RHS partial agg missing join key: rhs_kept={rhs_kept_positions} "
        f"rhs_join_keys={rhs_key_positions}"
    )

    # Group measures by side.
    lhs_measure_indices: List[int] = []
    rhs_measure_indices: List[int] = []
    for i, side in enumerate(measure_side):
        if side == "L":
            lhs_measure_indices.append(i)
        else:
            rhs_measure_indices.append(i)

    # Build LHS partial agg.
    # Field refs in measure args address LHS columns in the join's
    # left-side coordinate system already (we established this above by
    # walking through the project + join attribution). Renumber from
    # join-output index to side-index.
    def _to_side_index_remap(side: str) -> Dict[int, int]:
        """Remap join-output field index -> side-side field index."""
        out: Dict[int, int] = {}
        for j_idx, (s, s_idx) in enumerate(join_idx_to_side):
            if s == side:
                out[j_idx] = s_idx
        return out

    lhs_remap = _to_side_index_remap("L")
    rhs_remap = _to_side_index_remap("R")

    # Only push to a side if the side gets at least one measure. A
    # measure-less partial agg is a DISTINCT that often doesn't reduce
    # rows (the side's join key + group keys might already be unique
    # per row); doing it adds latency without throughput. Without a
    # partial agg the side just feeds its raw rows to the new join
    # and the final agg above the join handles the row-count collapse.
    #
    # If NEITHER side gets measures we skip the rewrite entirely; this
    # is the all-group-keys-on-one-side, no-measures case which doesn't
    # exist in TPC-H and would be a no-op.
    if not lhs_measure_indices and not rhs_measure_indices:
        return None

    # Don't push down to a side whose immediate child is already an
    # aggregate. That side has been pre-aggregated (TPC-H Q18's
    # ``HAVING SUM(l_quantity) > 300`` lineitem-side pattern); pushing
    # an additional partial agg there just re-aggregates already-collapsed
    # rows and adds latency without throughput.
    def _side_skips_due_to_inner_agg(side_rel: algebra.Rel) -> bool:
        cur = side_rel
        # Walk through the prefix-Project chain that BoltML's join builder
        # always emits (up to 2 projects) to look at the actual side input.
        for _ in range(3):
            ck = get_rel_kind(cur)
            if ck == "aggregate":
                return True
            if ck == "project":
                cur = cur.project.input
                continue
            break
        return False

    if lhs_measure_indices and _side_skips_due_to_inner_agg(hj.left):
        return None
    if rhs_measure_indices and _side_skips_due_to_inner_agg(hj.right):
        return None

    # Heuristic: only push down a measure whose argument is a COMPUTED
    # expression (not just a direct field-ref). Pushing a computed
    # expression below the join saves the cost of evaluating it on the
    # join's output — usually more rows than the side's input. Pushing
    # a direct-field measure (``Sum(l_quantity)``) doesn't save
    # arithmetic; it just adds a partial-agg step whose row reduction
    # may be undone if a downstream join filters into a small set
    # (e.g. TPC-H Q18: a HAVING-style pre-aggregate is filtered down to
    # ~600 orderkeys before the second lineitem scan, so pushing the
    # partial sum on full 6M lineitem before the join is pure overhead).
    has_computed_measure_arg = False
    for sub_args in measure_arg_exprs:
        for arg in sub_args:
            if not is_direct_field_ref(arg):
                has_computed_measure_arg = True
                break
        if has_computed_measure_arg:
            break
    if not has_computed_measure_arg:
        return None

    if lhs_measure_indices:
        lhs_partial = _build_partial_agg(
            side_rel=hj.left,
            side_names=left_names,
            kept_positions=lhs_kept_positions,
            measure_indices=lhs_measure_indices,
            original_measures=list(agg.measures),
            substituted_args_per_measure=measure_arg_exprs,
            side_remap=lhs_remap,
            side_label="L",
        )
    else:
        # No partial agg on LHS — but we still need the LHS's output
        # column ordering exposed at ``new_lhs_names`` matching what
        # ``_build_join`` expects (= ``left_names`` verbatim, since we
        # haven't reduced any columns). The hash_join's prefix-Project
        # wrapper will take care of column reordering.
        lhs_partial = (hj.left, left_names)

    if rhs_measure_indices:
        rhs_partial = _build_partial_agg(
            side_rel=hj.right,
            side_names=right_names,
            kept_positions=rhs_kept_positions,
            measure_indices=rhs_measure_indices,
            original_measures=list(agg.measures),
            substituted_args_per_measure=measure_arg_exprs,
            side_remap=rhs_remap,
            side_label="R",
        )
    else:
        rhs_partial = (hj.right, right_names)

    if lhs_partial is None or rhs_partial is None:
        return None

    new_lhs_rel, new_lhs_names = lhs_partial
    new_rhs_rel, new_rhs_names = rhs_partial

    # Build the new join. The LHS partial-agg outputs (when applied) are
    # ``[lhs_kept_positions, lhs_measure_indices]`` in that order. The
    # join keys move with their positions in lhs_kept_positions. Same
    # for RHS. The new join's keys must point at the new partial-agg
    # output columns where the join keys now live.
    #
    # When a side has NO partial agg (passthrough), the side's original
    # column order is preserved and the join keys remain at their
    # ``lhs_key_positions`` / ``rhs_key_positions`` indices.
    if lhs_measure_indices:
        new_lhs_key_indices = [lhs_kept_positions.index(k) for k in lhs_key_positions]
    else:
        new_lhs_key_indices = list(lhs_key_positions)
    if rhs_measure_indices:
        new_rhs_key_indices = [rhs_kept_positions.index(k) for k in rhs_key_positions]
    else:
        new_rhs_key_indices = list(rhs_key_positions)

    new_join_rel = _build_join(
        new_lhs_rel,
        new_lhs_names,
        new_rhs_rel,
        new_rhs_names,
        new_lhs_key_indices,
        new_rhs_key_indices,
    )
    if new_join_rel is None:
        return None

    new_join, new_join_canonical_names = new_join_rel

    # Now we need to wrap the new join in:
    #  1) a Project that mirrors the ORIGINAL Project (if any), translating
    #     references from new-join-output positions to the same positions
    #     the original project used. Because the original project's expressions
    #     reference fields in the join's output, and the new join's output
    #     names mirror the OLD join's (we kept the same canonical column
    #     identities by construction), we can reuse the project's expressions
    #     unchanged but rewrite their field-refs through a name-based remap.
    #  2) a Final aggregate above the project, with the same K and final-phase
    #     measures whose arguments reference the partial-agg output columns
    #     now propagated through the project.
    #
    # The easiest way: build the Project's expressions by re-mapping the
    # original project's field-refs from "old-join-output index" to
    # "new-join-output index" via column-name matching.

    # If a side wasn't partial-aggregated, ALL of its columns flow through
    # the new join. Update kept_positions accordingly so the rebuild
    # logic's column-attribution map covers them.
    if not lhs_measure_indices:
        lhs_kept_positions_for_rebuild = list(range(len(left_names)))
    else:
        lhs_kept_positions_for_rebuild = lhs_kept_positions
    if not rhs_measure_indices:
        rhs_kept_positions_for_rebuild = list(range(len(right_names)))
    else:
        rhs_kept_positions_for_rebuild = rhs_kept_positions

    rebuilt = _rebuild_above_join(
        new_join=new_join,
        new_join_names=new_join_canonical_names,
        project_chain=project_chain,
        original_agg=agg,
        grouping_substituted=grouping_substituted,
        measure_side=measure_side,
        lhs_kept_positions=lhs_kept_positions_for_rebuild,
        rhs_kept_positions=rhs_kept_positions_for_rebuild,
        join_idx_to_side=join_idx_to_side,
        join_names=join_names,
        left_names=left_names,
        right_names=right_names,
    )
    if rebuilt is None:
        return None

    n_measures_pushed = len(lhs_measure_indices) + len(rhs_measure_indices)
    boltmlDebugLog(
        "push_agg_through_join",
        f"pushed: keys={len(agg.grouping_expressions)} "
        f"measures={n_measures_pushed} "
        f"lhs_keep={len(lhs_kept_positions)} rhs_keep={len(rhs_kept_positions)}",
    )
    return rebuilt


# ---------------------------------------------------------------------------
# Subroutines: build partial agg, build join, build final agg
# ---------------------------------------------------------------------------


def _build_partial_agg(
    side_rel: algebra.Rel,
    side_names: Tuple[str, ...],
    kept_positions: List[int],
    measure_indices: List[int],
    original_measures: List[algebra.AggregateRel.Measure],
    substituted_args_per_measure: List[List[algebra.Expression]],
    side_remap: Dict[int, int],
    side_label: str,
) -> Optional[Tuple[algebra.Rel, Tuple[str, ...]]]:
    """Construct the partial-agg sub-tree for one side.

    Returns ``(partial_agg_rel, output_names)`` where ``output_names`` is
    the ``RelCommon.Hint.output_names`` of the new aggregate (canonical
    side names + measure names).

    Bolt's ``HashAggregation`` (see ``bolt/exec/AggregateInfo.cpp:99-104``)
    only accepts ``FieldAccessTypedExpr``, ``ConstantTypedExpr``, or
    ``LambdaTypedExpr`` as measure arguments — NOT computed expressions.
    So whenever a measure's argument is anything other than a direct
    field-ref, we wrap the side input in an INTERMEDIATE PROJECT that
    pre-computes the expression as a named column, and the partial agg
    then reads that column by field-ref.
    """
    # Renumber every measure argument from join-output indices to side
    # indices via side_remap. This gives expressions in the side's
    # coordinate system.
    renum_args_per_measure: List[List[algebra.Expression]] = []
    for orig_idx in measure_indices:
        sub_args = substituted_args_per_measure[orig_idx]
        renum_args = [
            substitute_field_refs(
                e,
                lambda i, _r=side_remap: (_make_field_ref(_r[i]) if i in _r else None),
            )
            for e in sub_args
        ]
        renum_args_per_measure.append(renum_args)

    # Determine whether we need an intermediate Project: any arg that's
    # not already a direct field-ref forces it.
    needs_project = any(
        not is_direct_field_ref(arg)
        for renum_args in renum_args_per_measure
        for arg in renum_args
    )

    # Build the project's expressions (or skip if not needed).
    if needs_project:
        # Project layout: [kept_positions' side columns] then [each computed
        # measure-arg as a separate column]. Direct field-ref args get
        # mapped through the kept-positions block (no need for a fresh
        # column). Non-direct args get a fresh column.
        proj_exprs: List[algebra.Expression] = []
        proj_names: List[str] = []
        # First emit the kept side columns at their positions.
        for p in kept_positions:
            proj_exprs.append(_make_field_ref(p))
            proj_names.append(side_names[p])

        # Track per-arg location after projection: either an index into
        # kept_positions (= proj_exprs index for that key) or a fresh
        # appended index.
        kept_pos_to_proj_idx: Dict[int, int] = {
            p: i for i, p in enumerate(kept_positions)
        }
        # Walk each measure's args, decide where they live in the project.
        new_arg_indices: List[List[int]] = []  # per measure, per arg
        for m_local_idx, renum_args in enumerate(renum_args_per_measure):
            arg_idxs: List[int] = []
            for arg in renum_args:
                fidx = direct_field_index(arg)
                if fidx is not None and fidx in kept_pos_to_proj_idx:
                    arg_idxs.append(kept_pos_to_proj_idx[fidx])
                else:
                    # Fresh column at end of project.
                    new_pos = len(proj_exprs)
                    proj_exprs.append(arg)
                    proj_names.append(
                        f"__boltml_arg_{side_label}_"
                        f"{measure_indices[m_local_idx]}_{len(arg_idxs)}"
                    )
                    arg_idxs.append(new_pos)
            new_arg_indices.append(arg_idxs)

        # Build the project.
        project_rel = algebra.Rel(
            project=algebra.ProjectRel(
                common=algebra.RelCommon(
                    direct=algebra.RelCommon.Direct(),
                    hint=algebra.RelCommon.Hint(output_names=list(proj_names)),
                ),
                input=side_rel,
                expressions=proj_exprs,
            )
        )

        # Group keys are now at positions [0..len(kept_positions)).
        agg_input = project_rel
        grouping_exprs = [_make_field_ref(i) for i in range(len(kept_positions))]
        side_kept_names = [side_names[p] for p in kept_positions]

        # Build measures referencing the project's columns by index.
        new_measures: List[algebra.AggregateRel.Measure] = []
        measure_out_names: List[str] = []
        for m_local_idx, orig_idx in enumerate(measure_indices):
            orig = original_measures[orig_idx]
            arg_idxs = new_arg_indices[m_local_idx]
            new_func = algebra.AggregateFunction()
            new_func.CopyFrom(orig.measure)
            del new_func.arguments[:]
            for ai in arg_idxs:
                new_func.arguments.append(
                    algebra.FunctionArgument(value=_make_field_ref(ai))
                )
            new_func.phase = (
                algebra.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE
            )
            new_measures.append(algebra.AggregateRel.Measure(measure=new_func))
            measure_out_names.append(f"__boltml_partial_{side_label}_{orig_idx}")
    else:
        # All args are direct field-refs — no intermediate project needed.
        agg_input = side_rel
        grouping_exprs = [_make_field_ref(p) for p in kept_positions]
        side_kept_names = [side_names[p] for p in kept_positions]

        new_measures = []
        measure_out_names = []
        for m_local_idx, orig_idx in enumerate(measure_indices):
            orig = original_measures[orig_idx]
            renum_args = renum_args_per_measure[m_local_idx]
            new_func = algebra.AggregateFunction()
            new_func.CopyFrom(orig.measure)
            del new_func.arguments[:]
            for ne in renum_args:
                new_func.arguments.append(algebra.FunctionArgument(value=ne))
            new_func.phase = (
                algebra.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE
            )
            new_measures.append(algebra.AggregateRel.Measure(measure=new_func))
            measure_out_names.append(f"__boltml_partial_{side_label}_{orig_idx}")

    groupings = [
        algebra.AggregateRel.Grouping(
            expression_references=list(range(len(kept_positions)))
        )
    ]
    out_names = side_kept_names + measure_out_names

    common = algebra.RelCommon(
        direct=algebra.RelCommon.Direct(),
        hint=algebra.RelCommon.Hint(output_names=out_names),
    )

    new_agg = algebra.AggregateRel(
        common=common,
        input=agg_input,
        groupings=groupings,
        measures=new_measures,
        grouping_expressions=grouping_exprs,
    )
    return algebra.Rel(aggregate=new_agg), tuple(out_names)


def _build_join(
    lhs_rel: algebra.Rel,
    lhs_names: Tuple[str, ...],
    rhs_rel: algebra.Rel,
    rhs_names: Tuple[str, ...],
    lhs_key_indices: List[int],
    rhs_key_indices: List[int],
) -> Optional[Tuple[algebra.Rel, Tuple[str, ...]]]:
    """Build a HashJoinRel that consumes the two partial-agg sides.

    Both sides are pre-wrapped in ``l_``/``r_`` prefix Projects so the
    join's output_names follow Bolt's standard scheme (LHS keys+nonkeys
    prefixed, RHS non-keys prefixed). Returns the (Rel, output_canonical
    names) — the canonical names strip the prefix so the caller can build
    above-join logic without prefix juggling.
    """
    lhs_width = len(lhs_names)
    rhs_width = len(rhs_names)

    # Reorder LHS: keys first, then non-keys. Same for RHS.
    lhs_key_set = set(lhs_key_indices)
    lhs_inner_order = list(lhs_key_indices) + [
        i for i in range(lhs_width) if i not in lhs_key_set
    ]
    rhs_key_set = set(rhs_key_indices)
    rhs_inner_order = list(rhs_key_indices) + [
        i for i in range(rhs_width) if i not in rhs_key_set
    ]

    lhs_inner_names = [lhs_names[i] for i in lhs_inner_order]
    rhs_inner_names = [rhs_names[i] for i in rhs_inner_order]

    lhs_inner_proj = algebra.Rel(
        project=algebra.ProjectRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=list(lhs_inner_names)),
            ),
            input=lhs_rel,
            expressions=[_make_field_ref(i) for i in lhs_inner_order],
        )
    )
    rhs_inner_proj = algebra.Rel(
        project=algebra.ProjectRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=list(rhs_inner_names)),
            ),
            input=rhs_rel,
            expressions=[_make_field_ref(i) for i in rhs_inner_order],
        )
    )

    lhs_outer_names = [f"l_{nm}" for nm in lhs_inner_names]
    rhs_outer_names = [f"r_{nm}" for nm in rhs_inner_names]

    lhs_outer_proj = algebra.Rel(
        project=algebra.ProjectRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=lhs_outer_names),
            ),
            input=lhs_inner_proj,
            expressions=[_make_field_ref(i) for i in range(len(lhs_inner_names))],
        )
    )
    rhs_outer_proj = algebra.Rel(
        project=algebra.ProjectRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=rhs_outer_names),
            ),
            input=rhs_inner_proj,
            expressions=[_make_field_ref(i) for i in range(len(rhs_inner_names))],
        )
    )

    n_keys = len(lhs_key_indices)
    keys = []
    for i in range(n_keys):
        keys.append(
            algebra.ComparisonJoinKey(
                left=algebra.Expression.FieldReference(
                    direct_reference=algebra.Expression.ReferenceSegment(
                        struct_field=algebra.Expression.ReferenceSegment.StructField(
                            field=i,
                        ),
                    ),
                    root_reference=algebra.Expression.FieldReference.RootReference(),
                ),
                right=algebra.Expression.FieldReference(
                    direct_reference=algebra.Expression.ReferenceSegment(
                        struct_field=algebra.Expression.ReferenceSegment.StructField(
                            field=i,
                        ),
                    ),
                    root_reference=algebra.Expression.FieldReference.RootReference(),
                ),
                comparison=algebra.ComparisonJoinKey.ComparisonType(
                    simple=algebra.ComparisonJoinKey.SIMPLE_COMPARISON_TYPE_EQ,
                ),
            )
        )

    # Hint output_names: full LHS prefixed names, then RHS non-key prefixed
    # names. Bolt drops the duplicate r_<key> at the boundary.
    join_hint_names = list(lhs_outer_names)
    for i in range(n_keys, len(rhs_outer_names)):
        join_hint_names.append(rhs_outer_names[i])

    # Canonical (prefix-stripped) names for downstream logic.
    canonical_names = [_strip_uniform_prefix(nm) for nm in join_hint_names]

    join_rel = algebra.Rel(
        hash_join=algebra.HashJoinRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=join_hint_names),
            ),
            left=lhs_outer_proj,
            right=rhs_outer_proj,
            type=algebra.HashJoinRel.JoinType.JOIN_TYPE_INNER,
            keys=keys,
        )
    )
    return join_rel, tuple(canonical_names)


def _rebuild_above_join(
    new_join: algebra.Rel,
    new_join_names: Tuple[str, ...],
    project_chain: List[algebra.ProjectRel],
    original_agg: algebra.AggregateRel,
    grouping_substituted: List[algebra.Expression],
    measure_side: List[str],
    lhs_kept_positions: List[int],
    rhs_kept_positions: List[int],
    join_idx_to_side: List[Tuple[str, int]],
    join_names: Tuple[str, ...],
    left_names: Tuple[str, ...],
    right_names: Tuple[str, ...],
) -> Optional[algebra.Rel]:
    """Build the Project (if any) and Final-Agg above the new join.

    The original Project's expressions referenced the OLD join's output
    columns. The new join's output is laid out differently (different
    column count and order). We fix this by:

    1. Building a column-name map from the new join's canonical names
       (prefix-stripped) to their indices.
    2. For each original-join column referenced by the project /
       aggregate, find its NEW canonical name. Group keys keep their
       original column name (we kept the same names through partial-agg
       output). Partial-agg measure outputs get the synthesized
       ``__boltml_partial_{side}_{i}`` names.
    3. Rewrite the project's expressions and the agg's measure args to
       use new-join field indices.
    """
    # Map: canonical new-join name -> index in the new join's output.
    name_to_new_idx: Dict[str, int] = {}
    for i, nm in enumerate(new_join_names):
        if nm in name_to_new_idx:
            # Duplicate canonical name (shouldn't happen since we
            # synthesize unique names for measures). Bail.
            return None
        name_to_new_idx[nm] = i

    # For each OLD join output index, compute the corresponding NEW
    # join output index. Group-key columns retained on either side keep
    # their original (canonical, unprefixed) name; we look them up in
    # name_to_new_idx. Pure non-grouping non-measure columns from the
    # old join are NOT carried through; if the original project / agg
    # references them, we have to bail.
    #
    # Build the (old_join_idx -> new_join_idx) map by walking
    # join_idx_to_side and picking up the column's name from each side's
    # original schema.
    old_idx_to_new_idx: Dict[int, int] = {}
    for old_idx, (side, side_idx) in enumerate(join_idx_to_side):
        # The new partial-agg's output names for kept columns are exactly
        # the side's hint names (verbatim). The new join builds prefix-
        # Projects that wrap each column with ``l_`` / ``r_`` and then
        # the canonical layer strips one prefix. Net effect: the canonical
        # name of a new-join column originally from side X is exactly
        # the X-side's original hint name. So we look up by side_name.
        if side == "L":
            kept = lhs_kept_positions
            side_name = left_names[side_idx]
        else:
            kept = rhs_kept_positions
            side_name = right_names[side_idx]
        if side_idx not in kept:
            # Not kept by partial agg; original consumer can't see it.
            continue
        new_idx = name_to_new_idx.get(side_name)
        if new_idx is None:
            return None
        old_idx_to_new_idx[old_idx] = new_idx

    # The intervening project chain (if any) is intentionally DROPPED here.
    #
    # In the original plan, the project COMPUTES expressions like
    # ``revenue = l_extendedprice * (1 - l_discount)``. The aggregate then
    # SUMs revenue. After pushdown, the LHS partial agg evaluates
    # ``sum(l_extendedprice * (1 - l_discount))`` partially — the agg's
    # arguments are the substituted, full expression. So the partial-agg
    # output is the partial sum directly.
    #
    # The intervening project, originally producing the ``revenue`` column,
    # is no longer needed — the partial agg already turned that expression
    # into a column. The final agg consumes the partial-agg output
    # directly (after the join).
    #
    # The original project's GROUP-KEY columns (e.g. ``l_orderkey``,
    # ``o_orderdate``) are kept verbatim by both the partial agg and the
    # new join — they still flow through under the same canonical names.
    # The final-agg's grouping expressions reference those columns
    # directly in the new join's output.

    final_grouping_exprs: List[algebra.Expression] = []
    for g_sub in grouping_substituted:
        # ``g_sub`` is the original grouping expression after walking
        # through the project chain — it's a direct field ref into the
        # OLD join's output (we already validated this in _try_push).
        old_join_idx = direct_field_index(g_sub)
        if old_join_idx is None:
            return None
        new_idx = old_idx_to_new_idx.get(old_join_idx)
        if new_idx is None:
            return None
        final_grouping_exprs.append(_make_field_ref(new_idx))

    # Build final measures. For each ORIGINAL measure index i, find the
    # matching partial-agg output column on the appropriate side. The
    # column is named ``__boltml_partial_{side}_{i}``.
    final_measures: List[algebra.AggregateRel.Measure] = []
    for orig_idx, m in enumerate(original_agg.measures):
        side = measure_side[orig_idx]
        partial_col_name = f"__boltml_partial_{side}_{orig_idx}"
        new_idx = name_to_new_idx.get(partial_col_name)
        if new_idx is None:
            return None
        # Build a final-phase measure that consumes the partial column.
        # The function reference, output_type stay the same. count's
        # final phase still uses the count function name; Bolt's
        # ``Aggregate::create`` with the original name + step=kFinal
        # internally handles the partial-merging by calling
        # ``addIntermediateResults``.
        new_func = algebra.AggregateFunction()
        new_func.CopyFrom(m.measure)
        del new_func.arguments[:]
        new_func.arguments.append(
            algebra.FunctionArgument(value=_make_field_ref(new_idx))
        )
        new_func.phase = (
            algebra.AggregationPhase.AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT
        )
        final_measures.append(algebra.AggregateRel.Measure(measure=new_func))

    final_groupings = [
        algebra.AggregateRel.Grouping(
            expression_references=list(range(len(final_grouping_exprs)))
        )
    ]

    # Hint output_names: same as the original aggregate (group key names +
    # measure names) so downstream consumers see the same schema.
    original_agg_names = list(_hint_output_names(algebra.Rel(aggregate=original_agg)))

    final_common = algebra.RelCommon(
        direct=algebra.RelCommon.Direct(),
        hint=algebra.RelCommon.Hint(output_names=original_agg_names),
    )

    final_agg = algebra.AggregateRel(
        common=final_common,
        input=new_join,
        groupings=final_groupings,
        measures=final_measures,
        grouping_expressions=final_grouping_exprs,
    )
    final_agg_rel = algebra.Rel(aggregate=final_agg)

    # Bolt's HashAggregation derives the output column NAME for each
    # grouping key from the input field's name (not from the agg's hint
    # output_names). After pushdown, the input is a new hash_join with
    # rebuilt prefix-Project layers, so the grouping keys carry mangled
    # names like ``l_l_l_orderkey`` instead of the original ``l_orderkey``.
    # Wrap the final agg in a passthrough Project that re-emits each
    # column under the ORIGINAL name. This preserves the schema visible
    # to the parent consumer (sort, limit, RelRoot) without needing any
    # downstream renaming.
    rename_project = algebra.Rel(
        project=algebra.ProjectRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=original_agg_names),
            ),
            input=final_agg_rel,
            expressions=[_make_field_ref(i) for i in range(len(original_agg_names))],
        )
    )
    return rename_project
