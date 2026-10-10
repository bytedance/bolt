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

"""Insert a partial aggregation below an inserted ``ExchangeRel``.

This is the classical "partial aggregation push-down before shuffle"
rewrite, applied AFTER ``AddExchanges`` has positioned an ``ExchangeRel``
above the input of every grouped aggregate. Given:

::

    Aggregate[K, M(args)]                        (kSingle)
      Exchange[hash by K]                        (inserted by AddExchanges)
        chain_input

it rewrites to:

::

    Aggregate[K, final(M)]                       (kFinal)
      Exchange[hash by K]                        (now atop tiny pre-aggregated rows)
        Aggregate[K, partial(M)]                 (kPartial)
          chain_input

so that the shuffle moves only one row per (key, partition) pair instead
of one row per input row. For a single-join probe at SF=10 (lineitem(60M)
⨝ part(2M) → groupBy(brand) over 25 distinct brands and 12 partitions),
this collapses the 12M-row pre-shuffle output to ~300 rows.

Why this is a separate rule from ``PushAggThroughJoin``
-------------------------------------------------------

``PushAggThroughJoin`` moves a partial aggregate BELOW a join. It only
fires when the aggregate's grouping keys overlap the join keys
substantively, and it requires a particular ``Aggregate(Project(Join))``
shape with the Project's expressions pushable per-side. Many real
queries (TPC-H Q1, Q5, Q14, Q18 — and any standalone ``groupBy``
without a join) don't match that pattern but still benefit hugely from
a partial-agg before the shuffle. This rule is the simpler, more
broadly-applicable counterpart that runs in the physical pipeline
(after ``AddExchanges``) and fires whenever ``Aggregate → Exchange``
appears with decomposable measures.

When the two rules combine
~~~~~~~~~~~~~~~~~~~~~~~~~~

If ``PushAggThroughJoin`` already split the original aggregate into
``Aggregate[kFinal] → Project → HashJoin(Aggregate[kPartial], ...)``,
then ``AddExchanges`` wraps the kFinal's input with an ``ExchangeRel``,
giving ``Aggregate[kFinal] → Exchange → ...``. This rule's phase
guard (must be ``UNSPECIFIED`` or ``INITIAL_TO_RESULT``) prevents
re-firing on the kFinal — the partial work is already below the join,
where it's even more useful. So the two rules layer cleanly without
double-aggregation.

Decomposable function allowlist
-------------------------------

Reuses the same ``_DECOMPOSABLE_AGG_NAMES`` set as ``PushAggThroughJoin``:
``sum``, ``count``, ``min``, ``max``. These are the classical
decomposable aggregates whose intermediate type equals the result type
in Bolt's TPC-H coverage (float64 / int64). See
``push_agg_through_join.py``'s docstring for the rationale on excluding
``avg``/``mean``, ``stddev``, ``median``, distinct ``count``,
``bool_and``/``bool_or``.

Computed measure args
---------------------

Bolt's ``HashAggregation`` (see ``bolt/exec/AggregateInfo.cpp``) only
accepts ``FieldAccessTypedExpr``, ``ConstantTypedExpr``, or
``LambdaTypedExpr`` as measure arguments — NOT computed expressions.
Whenever a measure's argument is anything other than a direct field-ref,
this rule wraps the partial agg's input in an INTERMEDIATE Project that
pre-computes the expression as a named column, and the partial agg
then reads that column by field-ref. Same pattern as
``PushAggThroughJoin._build_partial_agg``.

Other guards
------------

* The aggregate's input must be an ``ExchangeRel`` (this rule's whole
  point is repositioning the shuffle relative to the partial agg; if
  there's no shuffle, partial+final has no benefit and just adds work).
* The aggregate's phase must be ``UNSPECIFIED`` or
  ``INITIAL_TO_RESULT`` — never ``INTERMEDIATE_TO_RESULT`` (already
  split) or ``INITIAL_TO_INTERMEDIATE`` (partial form, can't split).
* Empty grouping (global agg) is skipped — ``AddExchanges`` doesn't
  insert an exchange for those, so the input wouldn't match anyway,
  but the explicit skip keeps the rule's contract self-evident.
* No measure-level ``filter``, sorted measures, or distinct invocation —
  same restrictions as ``PushAggThroughJoin``.

Schema preservation
-------------------

The rewritten root has the same output column names and types as the
original aggregate by construction:

  * Final agg's grouping keys reference the partial agg's K columns
    (positions ``0..len(K)``), which carry the same names as the
    original input columns.
  * Final agg's measures reference the partial agg's M columns at
    positions ``len(K)..len(K)+len(M)`` and copy the original
    AggregateFunction (function name, output_type), so the function
    output type matches.
  * The original aggregate's ``RelCommon.Hint.output_names`` is copied
    to the final agg's hint verbatim. A passthrough Project above the
    final agg re-emits each column under the original hint name to
    paper over Bolt's HashAggregation deriving grouping-key output
    names from input field names rather than from the hint.

Idempotency
-----------

After rewriting, the top aggregate's phase is
``INTERMEDIATE_TO_RESULT``, which fails the rule's phase guard on a
second run. The output is byte-equal under repeated application.

Local-mode no-op
----------------

This rule only fires when the aggregate's input is an ``ExchangeRel``,
which only ``AddExchanges`` inserts. The local-mode optimizer pipeline
doesn't run ``AddExchanges`` (no shuffle path), so this rule is a
no-op in local mode by construction.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from substrait.proto import algebra, plan

from ...logging import boltmlDebugLog
from ._expression import (
    direct_field_index,
    is_direct_field_ref,
)
from .base import Rule, get_rel_kind, rewrite_plan_root
from ...substrait.rel import rel_output_names as _rel_output_names
from .push_agg_through_join import (
    _DECOMPOSABLE_AGG_NAMES,
    _agg_function_uri_name,
    _build_extension_lookup,
    _hint_output_names,
    _make_field_ref,
)


class PartialAggBeforeExchange(Rule):
    """Split ``Aggregate(Exchange(input))`` into partial-then-final around
    the exchange. Pure: input ``Plan`` is not mutated. Idempotent.
    """

    name: str = "partial_agg_before_exchange"

    def apply(self, p: plan.Plan) -> plan.Plan:
        ext_lookup = _build_extension_lookup(p)

        def _try(rel: algebra.Rel) -> Optional[algebra.Rel]:
            return _try_split(rel, ext_lookup)

        return rewrite_plan_root(p, _try, bottom_up=True)


# ---------------------------------------------------------------------------
# Match + rewrite
# ---------------------------------------------------------------------------


def _try_split(
    rel: algebra.Rel,
    ext_lookup: Dict[int, str],
) -> Optional[algebra.Rel]:
    """Match the pattern; return rewritten Rel or ``None`` to leave unchanged."""
    if get_rel_kind(rel) != "aggregate":
        return None
    agg = rel.aggregate

    # Skip global aggregations — no exchange beneath them anyway.
    if not agg.grouping_expressions:
        return None

    # Reject anything weird in the aggregate envelope (a non-direct emit
    # could prune columns we depend on).
    if agg.HasField("common") and agg.common.WhichOneof("emit_kind") not in (
        None,
        "direct",
    ):
        return None

    # Need at least one measure — a measure-less Aggregate is a DISTINCT,
    # and partial+final DISTINCT just doubles work without reducing rows.
    if not agg.measures:
        return None

    # Validate every measure: in allowlist, no filter, no sorts, no
    # distinct, phase is the "complete" phase.
    for m in agg.measures:
        if m.HasField("filter"):
            return None
        nm = _agg_function_uri_name(m, ext_lookup)
        if nm is None or nm not in _DECOMPOSABLE_AGG_NAMES:
            return None
        if (
            m.measure.invocation
            == algebra.AggregateFunction.AGGREGATION_INVOCATION_DISTINCT
        ):
            return None
        if len(m.measure.sorts) > 0:
            return None
        if m.measure.phase not in (
            algebra.AggregationPhase.AGGREGATION_PHASE_UNSPECIFIED,
            algebra.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT,
        ):
            return None

    # The input must be an ExchangeRel — the whole point of this rule is
    # to reposition the shuffle relative to the partial-agg work.
    if not agg.HasField("input") or get_rel_kind(agg.input) != "exchange":
        return None
    exchange_rel = agg.input
    exchange = exchange_rel.exchange

    # Defensive: the exchange's existing output schema must match the
    # input schema (exchange just routes rows, doesn't reshape). If it
    # were doing anything fancy, we'd refuse to splice through it.
    if exchange.HasField("common") and exchange.common.WhichOneof("emit_kind") not in (
        None,
        "direct",
    ):
        return None

    # All checks passed. Build the partial agg over the exchange's input.
    partial_agg_rel, partial_output_names = _build_partial_agg(
        agg=agg,
        agg_input=exchange.input,
    )
    if partial_agg_rel is None:
        return None

    # Build a new exchange that scatters atop the partial agg's K
    # columns (positions 0..len(K)). The scattering alias (the human-
    # readable hash(name,name) string) is preserved verbatim from the
    # existing exchange because the column names at the K positions are
    # the same (the partial agg propagates them through).
    new_exchange = _rebuild_exchange_above_partial(
        original_exchange=exchange,
        partial_agg_rel=partial_agg_rel,
        n_grouping_keys=len(agg.grouping_expressions),
    )

    # Build the final agg above the exchange. Grouping keys reference
    # exchange-output positions [0..len(K)); measures reference
    # [len(K)..len(K)+len(M)).
    final_agg_rel = _build_final_agg(
        agg=agg,
        new_exchange_rel=new_exchange,
        partial_output_names=partial_output_names,
    )

    # Wrap in a passthrough Project so the original ``output_names`` are
    # the visible schema even if Bolt's HashAggregation derives names
    # for grouping keys from input field names. This mirrors the safety
    # wrapper in push_agg_through_join's _rebuild_above_join.
    original_agg_names = list(_hint_output_names(rel))
    if not original_agg_names:
        # Without a hint we can't restore names; the schema-derivation
        # path in Bolt is the source of truth for this rel's output
        # names, which is fine — emit just the final agg without a
        # rename project.
        rebuilt_root = final_agg_rel
    else:
        rebuilt_root = algebra.Rel(
            project=algebra.ProjectRel(
                common=algebra.RelCommon(
                    direct=algebra.RelCommon.Direct(),
                    hint=algebra.RelCommon.Hint(output_names=original_agg_names),
                ),
                input=final_agg_rel,
                expressions=[
                    _make_field_ref(i) for i in range(len(original_agg_names))
                ],
            )
        )

    boltmlDebugLog(
        "partial_agg_before_exchange",
        f"split: keys={len(agg.grouping_expressions)} measures={len(agg.measures)}",
    )
    return rebuilt_root


# ---------------------------------------------------------------------------
# Subroutines
# ---------------------------------------------------------------------------


def _build_partial_agg(
    agg: algebra.AggregateRel,
    agg_input: algebra.Rel,
) -> Tuple[Optional[algebra.Rel], Tuple[str, ...]]:
    """Build the partial-aggregate sub-tree.

    Returns ``(partial_agg_rel, output_names)`` where ``output_names``
    is the partial agg's ``RelCommon.Hint.output_names`` (= K names
    followed by synthetic ``__boltml_partial_<i>`` names).

    If any measure's argument is a computed expression (not a direct
    field-ref), wraps *agg_input* in an intermediate Project that
    pre-computes the expression as a named column. The partial agg
    then reads those columns by field-ref.
    """
    # Resolve K-column names from the input schema. ``_hint_output_names``
    # only inspects the immediate rel, but the agg's input may be a
    # transparent Filter / Fetch / Sort that doesn't carry its own hint
    # — its schema is its child's. Use ``rel_output_names`` so we walk
    # through transparent rels and pick up the schema-providing rel's
    # hint (or a Read's ``base_schema``). Without this, a query like
    # ``Aggregate(Filter(Read))`` would assign ``$0`` to the grouping
    # key, mismatch the runtime descriptor's output names (which carry
    # the real ``"col"``), and trip ``PlaceholderRead.validateDescriptor``
    # at the consumer stage. ``$idx`` remains the fall-through for the
    # genuinely schema-less case (extension/set rels with no hint).
    input_hint = _rel_output_names(agg_input)

    def _name_at(idx: int) -> str:
        if 0 <= idx < len(input_hint):
            return input_hint[idx]
        return f"${idx}"

    # Grouping keys must be direct field refs (they are, by construction
    # — AddExchanges already validated this when emitting the exchange's
    # scatter_by_fields).
    grouping_field_indices: List[int] = []
    for g in agg.grouping_expressions:
        idx = direct_field_index(g)
        if idx is None:
            return None, ()
        grouping_field_indices.append(idx)

    k_names = [_name_at(i) for i in grouping_field_indices]

    # Decide if we need an intermediate Project for computed measure
    # args. Bolt's HashAggregation only accepts direct field-refs as
    # measure arguments.
    needs_project = False
    for m in agg.measures:
        for arg in m.measure.arguments:
            if arg.WhichOneof("arg_type") != "value":
                # Type/enum args not supported in this rewrite (would
                # have been caught upstream too — defensive).
                return None, ()
            if not is_direct_field_ref(arg.value):
                needs_project = True
                break
        if needs_project:
            break

    if needs_project:
        # Project layout: K columns first (in K order), then one column
        # per non-direct-ref measure argument. Direct-ref args are
        # mapped to whatever K column they happen to coincide with, or
        # to a fresh column if they don't (we keep the original input
        # column for the arg in those cases).
        proj_exprs: List[algebra.Expression] = []
        proj_names: List[str] = []
        for k_idx, k_nm in zip(grouping_field_indices, k_names):
            proj_exprs.append(_make_field_ref(k_idx))
            proj_names.append(k_nm)
        kept_pos_to_proj_idx: Dict[int, int] = {
            k_idx: i for i, k_idx in enumerate(grouping_field_indices)
        }

        # For each measure-arg, decide its position in the project.
        # Direct-ref args that already match a K column reuse that
        # column; others (computed or referencing non-K input columns)
        # get a fresh column appended.
        measure_arg_proj_indices: List[List[int]] = []
        for m_idx, m in enumerate(agg.measures):
            arg_idxs: List[int] = []
            for arg_local_idx, arg in enumerate(m.measure.arguments):
                e = arg.value
                fidx = direct_field_index(e)
                if fidx is not None and fidx in kept_pos_to_proj_idx:
                    arg_idxs.append(kept_pos_to_proj_idx[fidx])
                else:
                    new_pos = len(proj_exprs)
                    proj_exprs.append(e)
                    proj_names.append(f"__boltml_arg_{m_idx}_{arg_local_idx}")
                    arg_idxs.append(new_pos)
            measure_arg_proj_indices.append(arg_idxs)

        partial_input = algebra.Rel(
            project=algebra.ProjectRel(
                common=algebra.RelCommon(
                    direct=algebra.RelCommon.Direct(),
                    hint=algebra.RelCommon.Hint(output_names=list(proj_names)),
                ),
                input=agg_input,
                expressions=proj_exprs,
            )
        )

        # Grouping keys are now at positions [0..len(K)).
        partial_grouping_exprs = [_make_field_ref(i) for i in range(len(k_names))]

        # Build measures whose args reference the project's columns.
        partial_measures: List[algebra.AggregateRel.Measure] = []
        for m_idx, m in enumerate(agg.measures):
            new_func = algebra.AggregateFunction()
            new_func.CopyFrom(m.measure)
            del new_func.arguments[:]
            for ai in measure_arg_proj_indices[m_idx]:
                new_func.arguments.append(
                    algebra.FunctionArgument(value=_make_field_ref(ai))
                )
            new_func.phase = (
                algebra.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE
            )
            partial_measures.append(algebra.AggregateRel.Measure(measure=new_func))
    else:
        # No project needed; the partial agg consumes agg_input
        # directly. Grouping_expressions reference the same K indices
        # as the original aggregate.
        partial_input = agg_input
        partial_grouping_exprs = [_make_field_ref(i) for i in grouping_field_indices]
        partial_measures = []
        for m in agg.measures:
            new_func = algebra.AggregateFunction()
            new_func.CopyFrom(m.measure)
            new_func.phase = (
                algebra.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE
            )
            partial_measures.append(algebra.AggregateRel.Measure(measure=new_func))

    measure_names = [f"__boltml_partial_{i}" for i in range(len(agg.measures))]
    out_names = list(k_names) + measure_names

    partial_agg = algebra.AggregateRel(
        common=algebra.RelCommon(
            direct=algebra.RelCommon.Direct(),
            hint=algebra.RelCommon.Hint(output_names=out_names),
        ),
        input=partial_input,
        groupings=[
            algebra.AggregateRel.Grouping(
                expression_references=list(range(len(k_names)))
            )
        ],
        measures=partial_measures,
        grouping_expressions=partial_grouping_exprs,
    )
    return algebra.Rel(aggregate=partial_agg), tuple(out_names)


def _rebuild_exchange_above_partial(
    original_exchange: algebra.ExchangeRel,
    partial_agg_rel: algebra.Rel,
    n_grouping_keys: int,
) -> algebra.Rel:
    """Build a new ``ExchangeRel`` whose ``input`` is the partial agg.

    Preserves the original ``partition_count`` and ``RelCommon.Hint.alias``
    (the human-readable scattering string the dispatcher reads). The
    ``scatter_by_fields`` indices are renumbered to point at positions
    ``0..n_grouping_keys`` in the partial agg's output, since that's
    where the K columns live after the partial-agg projection.
    """
    new_ex = algebra.ExchangeRel()
    new_ex.CopyFrom(original_exchange)
    new_ex.input.CopyFrom(partial_agg_rel)
    # Update scatter_by_fields to point at the partial agg's K columns
    # (positions 0..n_grouping_keys) rather than the original input's.
    if new_ex.WhichOneof("exchange_kind") == "scatter_by_fields":
        del new_ex.scatter_by_fields.fields[:]
        for i in range(n_grouping_keys):
            new_ex.scatter_by_fields.fields.append(
                algebra.Expression.FieldReference(
                    direct_reference=algebra.Expression.ReferenceSegment(
                        struct_field=algebra.Expression.ReferenceSegment.StructField(
                            field=i,
                        ),
                    ),
                    root_reference=algebra.Expression.FieldReference.RootReference(),
                )
            )
    return algebra.Rel(exchange=new_ex)


def _build_final_agg(
    agg: algebra.AggregateRel,
    new_exchange_rel: algebra.Rel,
    partial_output_names: Tuple[str, ...],
) -> algebra.Rel:
    """Build the final-aggregate atop the new exchange.

    Final agg's grouping keys reference exchange-output positions
    ``[0..len(K))``; measures reference ``[len(K)..len(K)+len(M))``.
    Each measure's function (URI, output_type) is copied from the
    original; phase is set to ``INTERMEDIATE_TO_RESULT``; arguments
    are replaced with a single field-ref to the corresponding partial
    column.

    The final agg's ``Hint.output_names`` is set to
    ``[K_names, original_measure_hint_names]`` if the original agg had
    measure names; otherwise it falls through to the partial column
    names. Either way the outer rename Project (built by the caller)
    re-emits under the original aggregate's names.
    """
    n_keys = len(agg.grouping_expressions)
    n_measures = len(agg.measures)

    final_grouping_exprs = [_make_field_ref(i) for i in range(n_keys)]

    final_measures: List[algebra.AggregateRel.Measure] = []
    for m_idx, m in enumerate(agg.measures):
        new_func = algebra.AggregateFunction()
        new_func.CopyFrom(m.measure)
        del new_func.arguments[:]
        new_func.arguments.append(
            algebra.FunctionArgument(value=_make_field_ref(n_keys + m_idx))
        )
        new_func.phase = (
            algebra.AggregationPhase.AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT
        )
        final_measures.append(algebra.AggregateRel.Measure(measure=new_func))

    # Carry the original aggregate's hint output_names if present;
    # otherwise reuse the partial agg's output names (K + synthetic
    # __boltml_partial_<i>).
    original_hint = _hint_output_names(algebra.Rel(aggregate=agg))
    if original_hint and len(original_hint) == n_keys + n_measures:
        out_names = list(original_hint)
    else:
        out_names = list(partial_output_names)

    final_agg = algebra.AggregateRel(
        common=algebra.RelCommon(
            direct=algebra.RelCommon.Direct(),
            hint=algebra.RelCommon.Hint(output_names=out_names),
        ),
        input=new_exchange_rel,
        groupings=[
            algebra.AggregateRel.Grouping(expression_references=list(range(n_keys)))
        ],
        measures=final_measures,
        grouping_expressions=final_grouping_exprs,
    )
    return algebra.Rel(aggregate=final_agg)
