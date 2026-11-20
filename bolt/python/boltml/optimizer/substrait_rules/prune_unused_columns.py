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

"""Drop unused output columns from every Rel that supports it.

This is the canonical *projection pruning* / *column pruning* optimizer
rule. The pipeline previously had **only** predicate pushdown
(``PushFilterThroughProjects``) and the rewrite duals
(``MergeProjects``, ``RewriteLimits``); the field-flow direction was
unhandled, so unused columns rode all the way through joins and
aggregates. On TPC-H Q5 that meant the post-6-way-join intermediate
carried ~30 string columns that the final group-and-sum never reads,
which then overflowed Bolt's 2 GB Arrow string-buffer limit at
``bolt/vector/arrow/Bridge.cpp:1055`` (``bufSize <
int32_t::max``). With this rule, the same plan keeps only the ~9
columns Q5 actually consumes.

Algorithm
---------

A single recursive ``_prune(rel, required_outputs) -> (new_rel,
new_width)`` pass that:

1. Walks top-down: each node is asked which of its outputs the parent
   actually needs (``required_outputs``).
2. Computes which of *its own input* columns it needs (its parent's
   requirements + any field references inside its own
   expressions / conditions / sort fields / join keys / grouping
   exprs / measures).
3. Recurses on each child with that input requirement, gets back a
   rewritten child with potentially fewer output columns and a remap
   ``old_input_index -> new_input_index``.
4. Renumbers every field reference inside the current Rel's own
   expressions to use the child's new column ordering.
5. Drops any of the current Rel's own output positions the parent
   doesn't need (e.g. a project's expressions list, an aggregate's
   measures list).

The output column ordering is preserved as a *subsequence* of the
original — kept positions stay in their relative order. That keeps
rewriting upstream references to "compact a sparse selection"
arithmetic instead of arbitrary permutation.

Conservative fallbacks
----------------------

The rule refuses to prune through a Rel kind it does not fully
understand. In that case the recursion stops at that boundary: the
Rel is kept verbatim, its child is asked for *all* its columns, and
no remap is propagated upward. Currently:

* ``read``        — prunes ``base_schema`` columns when no parent uses them.
* ``project``     — drops unused output expressions and renumbers.
* ``filter``      — pass-through (predicate refs added to required set).
* ``aggregate``   — drops unused measures; keeps every grouping key
  (groupings define the row identity, dropping them changes
  semantics).
* ``hash_join`` / ``join`` / ``merge_join`` — splits required outputs
  per side, adds each side's join-key references, recurses
  independently, reassembles.
* ``sort`` / ``fetch`` — pass-through; sort keys' refs are added to
  the required set so they survive the prune.
* ``extension_single`` (shuffle / python UDF) / ``set`` /
  ``exchange`` / ``write`` / ``expand`` — opaque; treated as
  no-prune barriers (child is asked for all its columns).
* ``RelCommon.emit.output_mapping`` — opaque. Any non-``direct``
  emit on a Rel disables pruning for that Rel (the existing
  output_mapping already encodes a projection we don't want to
  conflict with).

The root output is always preserved exactly: the rule never drops
columns that ``RelRoot.names`` exposes, and it does not reorder
them.
"""

from __future__ import annotations

from typing import Optional

from substrait.proto import algebra, plan, type as type_pb2

from ...logging import boltmlDebugLog

from ._expression import collect_field_indices, substitute_field_refs
from .base import Rule, get_rel_kind


# ---------------------------------------------------------------------------
# Helpers: field references + "direct emit" check
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


def _renumber(expr: algebra.Expression, remap: dict[int, int]) -> algebra.Expression:
    """Renumber every direct field-reference index in *expr* via *remap*.

    Indices not in *remap* are left in place — that means the caller
    refused to prune across this expression and the field will be
    address out-of-range; callers must therefore include every
    referenced index in *remap* before calling.
    """

    def lookup(idx: int) -> Optional[algebra.Expression]:
        new_idx = remap.get(idx)
        if new_idx is None:
            return None
        return _make_field_ref(new_idx)

    return substitute_field_refs(expr, lookup)


def _has_direct_emit(common: algebra.RelCommon) -> bool:
    """Return ``True`` iff the Rel uses ``direct`` emit (no output_mapping)."""
    return common.WhichOneof("emit_kind") in (None, "direct")


def _struct_field_index(ref: algebra.Expression.FieldReference) -> Optional[int]:
    """Return the top-level struct-field index of *ref* if it is one, else None."""
    if ref.WhichOneof("reference_type") != "direct_reference":
        return None
    seg = ref.direct_reference
    if seg.WhichOneof("reference_type") != "struct_field":
        return None
    if seg.struct_field.HasField("child"):
        return None
    return seg.struct_field.field


# ---------------------------------------------------------------------------
# Output-width helper (used only for the no-prune fallback)
# ---------------------------------------------------------------------------


def _output_width(rel: algebra.Rel) -> int:
    """Compute the number of output columns a Rel produces.

    Used when a non-``direct`` emit or unhandled Rel kind forces the
    pruner to give up: we still need to tell the parent what the full
    width is so it can pass through every column.
    """
    kind = get_rel_kind(rel)
    if kind == "read":
        # Prefer the named-struct length (with names); fall back to
        # the type list (e.g. virtual_table built without names);
        # last-resort fall back to ``RelCommon.Hint.output_names`` for
        # reads whose schema lives C++-side (extension_table reads
        # like TPC-H carry an empty base_schema but populate the hint).
        n = len(rel.read.base_schema.names)
        if n:
            return n
        n = len(rel.read.base_schema.struct.types)
        if n:
            return n
        if (
            rel.read.HasField("common")
            and rel.read.common.HasField("hint")
            and len(rel.read.common.hint.output_names) > 0
        ):
            return len(rel.read.common.hint.output_names)
        return 0
    if kind == "project":
        return len(rel.project.expressions)
    if kind == "filter":
        return _output_width(rel.filter.input)
    if kind == "aggregate":
        return len(rel.aggregate.grouping_expressions) + len(rel.aggregate.measures)
    if kind in ("hash_join", "join", "merge_join", "nested_loop_join"):
        inner = getattr(rel, kind)
        # BoltML compresses join output via ``RelCommon.Hint.output_names``
        # (e.g. drops the redundant ``r_<key>`` for an equi-join), so the
        # actual width is ``len(hint.output_names)`` when the hint is
        # present, NOT ``left_width + right_width``. Without this, every
        # parent index past ``len(hint)`` would be silently skipped and
        # the pruner's per-side requirement set would miss real
        # references.
        if (
            inner.HasField("common")
            and inner.common.HasField("hint")
            and len(inner.common.hint.output_names) > 0
        ):
            return len(inner.common.hint.output_names)
        return _output_width(inner.left) + _output_width(inner.right)
    if kind in ("sort", "fetch"):
        return _output_width(getattr(rel, kind).input)
    if kind == "extension_single":
        # Conservative default: assume identity (true for shuffle).
        return _output_width(rel.extension_single.input)
    if kind == "exchange":
        return _output_width(rel.exchange.input)
    if kind == "expand":
        return _output_width(rel.expand.input)
    if kind == "set":
        # All inputs of a set op share the same width.
        if rel.set.inputs:
            return _output_width(rel.set.inputs[0])
        return 0
    if kind == "write":
        return _output_width(rel.write.input)
    raise ValueError(f"_output_width: unhandled Rel kind {kind!r}")


# ---------------------------------------------------------------------------
# Hint pruning helper
# ---------------------------------------------------------------------------


def _prune_hint_output_names(
    common: algebra.RelCommon, kept_indices: list[int]
) -> None:
    """Trim ``common.hint.output_names`` to the kept indices, in order.

    Some Rel builders (project, aggregate, join, read of TPC-H ext
    table) carry a ``RelCommon.Hint.output_names`` matching their
    output schema. The dispatcher's
    ``BOLTML_HINT_PROVIDING_REL_KINDS`` registry depends on the hint
    matching the actual output. When we drop output positions, the
    hint must drop in lockstep.
    """
    if not common.HasField("hint"):
        return
    if not common.hint.output_names:
        return
    original = list(common.hint.output_names)
    if len(original) == 0:
        return
    new_names = [original[i] for i in kept_indices if 0 <= i < len(original)]
    del common.hint.output_names[:]
    common.hint.output_names.extend(new_names)


# ---------------------------------------------------------------------------
# Core: _prune
# ---------------------------------------------------------------------------


def _prune(
    rel: algebra.Rel, required_outputs: set[int]
) -> tuple[algebra.Rel, dict[int, int]]:
    """Return ``(rewritten_rel, output_remap)`` where:

    * ``rewritten_rel`` produces output columns that are a (possibly
      compacted) subsequence of the original Rel's output.
    * ``output_remap`` maps each ``old_idx in required_outputs`` to its
      ``new_idx`` in the rewritten Rel's output. Keys are exactly the
      contents of ``required_outputs`` that survive the prune; missing
      keys mean the Rel could not be pruned through and the parent
      must keep using the original positions.

    Pure: the input ``rel`` is never mutated; the returned ``rel`` is
    always a fresh proto.
    """
    kind = get_rel_kind(rel)
    if kind is None:
        # Defensive: empty Rel → no-op.
        out = algebra.Rel()
        out.CopyFrom(rel)
        return out, {}

    # If the Rel uses a non-trivial emit, do not prune through it.
    common = _common_of(rel)
    if common is not None and not _has_direct_emit(common):
        return _no_prune(rel)

    if kind == "read":
        return _prune_read(rel, required_outputs)
    if kind == "project":
        return _prune_project(rel, required_outputs)
    if kind == "filter":
        return _prune_filter(rel, required_outputs)
    if kind == "aggregate":
        return _prune_aggregate(rel, required_outputs)
    if kind in ("hash_join", "join", "merge_join"):
        return _prune_hash_join(rel, required_outputs)
    if kind == "sort":
        return _prune_sort(rel, required_outputs)
    if kind == "fetch":
        return _prune_fetch(rel, required_outputs)

    # Anything else (extension_single, set, exchange, expand, write,
    # nested_loop_join) is treated as opaque.
    return _no_prune(rel)


def _common_of(rel: algebra.Rel) -> Optional[algebra.RelCommon]:
    kind = get_rel_kind(rel)
    if kind is None:
        return None
    inner = getattr(rel, kind)
    if not hasattr(inner, "common"):
        return None
    if not inner.HasField("common"):
        return None
    return inner.common


def _no_prune(rel: algebra.Rel) -> tuple[algebra.Rel, dict[int, int]]:
    """Return *rel* unchanged with an identity remap covering its full width.

    Used when the Rel kind is unhandled, the Rel uses a non-direct
    emit, or any other "give up" case. The parent will pass through
    every column unmodified.
    """
    out = algebra.Rel()
    out.CopyFrom(rel)
    width = _output_width(out)
    return out, {i: i for i in range(width)}


# ---------------------------------------------------------------------------
# Per-Rel pruning
# ---------------------------------------------------------------------------


def _prune_read(
    rel: algebra.Rel, required_outputs: set[int]
) -> tuple[algebra.Rel, dict[int, int]]:
    """Drop ``base_schema`` columns the parent doesn't need.

    Only safe for ``virtual_table`` and ``local_files`` reads where the
    output schema is exactly ``base_schema``. Extension-table reads
    (``ReadRel.extension_table``) build their schema in C++ from the
    table handle, not from ``base_schema`` — those carry an empty
    ``base_schema`` and we can't prune them from the Python side.
    """
    rd = rel.read
    width = _output_width(rel)

    # Special-case: extension-table reads (e.g. TPC-H tables built
    # C++-side via ``makeTpchExtensionTable``) carry an empty
    # ``base_schema`` because the C++ converter materialises the row
    # type from the extension's TableHandle. We can't trim
    # ``base_schema`` here — it would be a no-op in C++ — but we *can*
    # wrap the read in a Project that selects only the required
    # columns. The C++ side still emits the full row, but the Project
    # immediately drops the unused columns before any join sees them.
    # On TPC-H Q5 this is the difference between dragging ~30 wide
    # string columns through a 6-way join (which overflows Bolt's
    # 2 GB Arrow string buffer at the final ``exportStrings`` step)
    # and keeping only the ~9 columns the query actually consumes.
    # The ``RelCommon.Hint.output_names`` carried on the read tells
    # us what each index is named; we mirror that on the Project.
    if rd.HasField("extension_table"):
        if not rd.HasField("common"):
            return _no_prune(rel)
        names = list(rd.common.hint.output_names)
        if not names:
            return _no_prune(rel)
        ext_width = len(names)
        kept = sorted(i for i in required_outputs if 0 <= i < ext_width)
        if not kept or len(kept) == ext_width:
            return _no_prune(rel)

        kept_names = [names[i] for i in kept]
        project_exprs = [_make_field_ref(i) for i in kept]
        boltmlDebugLog(
            "prune_extension_read",
            f"trimmed {ext_width}->{len(kept)}: keeping {kept_names}",
        )
        new_project = algebra.Rel(
            project=algebra.ProjectRel(
                common=algebra.RelCommon(
                    direct=algebra.RelCommon.Direct(),
                    hint=algebra.RelCommon.Hint(output_names=kept_names),
                ),
                input=rel,
                expressions=project_exprs,
            )
        )
        remap = {old: new for new, old in enumerate(kept)}
        return new_project, remap

    # Bail-out: empty schema with no extension_table — nothing to prune.
    if width == 0:
        return _no_prune(rel)

    kept = sorted(i for i in required_outputs if 0 <= i < width)
    if len(kept) == width:
        # Nothing to drop.
        out = algebra.Rel()
        out.CopyFrom(rel)
        return out, {i: i for i in range(width)}

    new_rel = algebra.Rel()
    new_rel.CopyFrom(rel)
    new_read = new_rel.read

    # Trim the named-struct schema in lockstep.
    old_names = list(new_read.base_schema.names)
    old_types = list(new_read.base_schema.struct.types)
    new_names = [old_names[i] for i in kept] if old_names else []
    new_types = [old_types[i] for i in kept] if old_types else []

    new_read.base_schema.Clear()
    new_read.base_schema.names.extend(new_names)
    new_read.base_schema.struct.CopyFrom(
        type_pb2.Type.Struct(
            types=new_types,
            type_variation_reference=rd.base_schema.struct.type_variation_reference,
            nullability=rd.base_schema.struct.nullability,
        )
    )

    # If the read carries an output-names hint, trim it too.
    if new_read.HasField("common"):
        _prune_hint_output_names(new_read.common, kept)

    if new_read.HasField("virtual_table"):
        vt = new_read.virtual_table
        new_values = []
        for row in vt.values:
            if row.fields:
                assert len(row.fields) == width, (
                    f"virtual_table row has {len(row.fields)} fields "
                    f"but read width is {width}"
                )
                kept_fields = [row.fields[i] for i in kept]
            else:
                kept_fields = []
            new_values.append(algebra.Expression.Literal.Struct(fields=kept_fields))
        del vt.values[:]
        vt.values.extend(new_values)

        new_exprs = []
        for row in vt.expressions:
            if width == 0 or not row.fields:
                kept_fields = []
            else:
                assert len(row.fields) % width == 0, (
                    f"virtual_table expression has {len(row.fields)} fields "
                    f"but read width is {width}; layout is not column-major batched"
                )
                batch_size = len(row.fields) // width
                kept_fields = []
                for c in kept:
                    if c * batch_size + batch_size <= len(row.fields):
                        kept_fields.extend(
                            row.fields[c * batch_size : (c + 1) * batch_size]
                        )
            new_struct = algebra.Expression.Nested.Struct(fields=kept_fields)
            new_exprs.append(new_struct)
        del vt.expressions[:]
        vt.expressions.extend(new_exprs)

    remap = {old: new for new, old in enumerate(kept)}
    return new_rel, remap


def _prune_project(
    rel: algebra.Rel, required_outputs: set[int]
) -> tuple[algebra.Rel, dict[int, int]]:
    """Drop unused output expressions; recurse on input with the trimmed needs.

    BoltML project semantics are *replacement*: output column ``i`` is
    ``project.expressions[i]``. Required upstream outputs select which
    expressions survive; the surviving expressions' field references
    become the input requirements.
    """
    proj = rel.project
    width = len(proj.expressions)
    kept = sorted(i for i in required_outputs if 0 <= i < width)

    # If nothing survived, force at least one column through (a project
    # with zero expressions confuses Bolt's row-vector machinery and
    # would also produce an empty hint). Fall back to no-prune.
    if not kept:
        return _no_prune(rel)

    surviving_exprs = [proj.expressions[i] for i in kept]

    # What does the input have to provide?
    input_required: set[int] = set()
    for e in surviving_exprs:
        input_required |= collect_field_indices(e)

    # When the surviving expressions are all literal-or-constant (no
    # field references) we MUST NOT prune the input down to zero
    # columns. The input still drives the row count of the project --
    # Velox executes a ``Project [literal] over Values [width=0,
    # 2 rows]`` as a single-row ConstantVector instead of 2 rows of
    # the constant, silently collapsing ``df["c0"] = 2`` from N rows
    # to 1 row. Force at least one column through; ``_no_prune`` on
    # the input keeps every column it currently has and preserves
    # the row-count-driving behaviour. The project's renumbered
    # expressions are still all literals so the unused input column
    # is harmlessly carried through and discarded by the project.
    if not input_required:
        new_input, input_remap = _no_prune(proj.input)
    else:
        new_input, input_remap = _prune(proj.input, input_required)

    # Renumber field refs inside surviving expressions to use the
    # input's new ordering. Anything we required must be in the remap.
    if not input_required.issubset(input_remap.keys()):
        # The child refused to prune (returned identity). Then
        # input_remap is identity for full width — just renumber via
        # identity (no-op) and proceed.
        identity = input_remap
    else:
        identity = input_remap
    renumbered_exprs = [_renumber(e, identity) for e in surviving_exprs]

    new_rel = algebra.Rel()
    new_rel.CopyFrom(rel)
    new_proj = new_rel.project
    new_proj.input.CopyFrom(new_input)
    del new_proj.expressions[:]
    new_proj.expressions.extend(renumbered_exprs)
    if new_proj.HasField("common"):
        _prune_hint_output_names(new_proj.common, kept)

    out_remap = {old: new for new, old in enumerate(kept)}
    return new_rel, out_remap


def _prune_filter(
    rel: algebra.Rel, required_outputs: set[int]
) -> tuple[algebra.Rel, dict[int, int]]:
    """Filter passes columns through; predicate refs add to input requirements."""
    filt = rel.filter
    width = _output_width(rel)
    # Filter's outputs == its input's outputs. The parent's required
    # set + the predicate's referenced indices is what we need from
    # the input.
    cond_refs = collect_field_indices(filt.condition)
    needed = (set(required_outputs) & set(range(width))) | cond_refs

    new_input, input_remap = _prune(filt.input, needed)
    rewritten_cond = _renumber(filt.condition, input_remap)

    new_rel = algebra.Rel()
    new_rel.CopyFrom(rel)
    new_filt = new_rel.filter
    new_filt.input.CopyFrom(new_input)
    new_filt.condition.CopyFrom(rewritten_cond)

    # Output schema = trimmed input. Kept positions in original-order
    # subsequence are exactly the keys of input_remap that are also in
    # required_outputs.
    out_remap = {
        old: input_remap[old] for old in sorted(required_outputs) if old in input_remap
    }
    return new_rel, out_remap


def _prune_aggregate(
    rel: algebra.Rel, required_outputs: set[int]
) -> tuple[algebra.Rel, dict[int, int]]:
    """Drop unused measures; keep every grouping key.

    AggregateRel output schema = ``grouping_expressions ++ measures``.
    Output index ``i < n_groupings`` is the i-th grouping key; index
    ``i >= n_groupings`` is measure ``(i - n_groupings)``. Grouping
    keys define row identity — dropping one would change the result —
    so the rule keeps them all even when the parent doesn't reference
    them. (A subsequent ``MergeProjects`` pass can prune them again
    by stacking a project above.)
    """
    agg = rel.aggregate
    n_groups = len(agg.grouping_expressions)
    n_measures = len(agg.measures)

    # Decide which measures survive.
    measure_kept = sorted(
        i - n_groups for i in required_outputs if n_groups <= i < n_groups + n_measures
    )
    surviving_measures = [agg.measures[i] for i in measure_kept]

    # Compute input requirements: every field ref inside surviving
    # grouping expressions + every field ref inside each surviving
    # measure's arguments + each measure's filter (if present).
    input_required: set[int] = set()
    for g in agg.grouping_expressions:
        input_required |= collect_field_indices(g)
    for m in surviving_measures:
        if m.HasField("measure"):
            for arg in m.measure.arguments:
                if arg.WhichOneof("arg_type") == "value":
                    input_required |= collect_field_indices(arg.value)
        if m.HasField("filter"):
            input_required |= collect_field_indices(m.filter)

    new_input, input_remap = _prune(agg.input, input_required)

    # Renumber: every field ref in grouping_expressions and in each
    # surviving measure's arguments / filter must use input_remap.
    new_grouping_exprs = [_renumber(g, input_remap) for g in agg.grouping_expressions]

    new_measures = []
    for m in surviving_measures:
        new_m = algebra.AggregateRel.Measure()
        new_m.CopyFrom(m)
        if new_m.HasField("measure"):
            for arg in new_m.measure.arguments:
                if arg.WhichOneof("arg_type") == "value":
                    arg.value.CopyFrom(_renumber(arg.value, input_remap))
        if new_m.HasField("filter"):
            new_m.filter.CopyFrom(_renumber(new_m.filter, input_remap))
        new_measures.append(new_m)

    new_rel = algebra.Rel()
    new_rel.CopyFrom(rel)
    new_agg = new_rel.aggregate
    new_agg.input.CopyFrom(new_input)
    del new_agg.grouping_expressions[:]
    new_agg.grouping_expressions.extend(new_grouping_exprs)
    del new_agg.measures[:]
    new_agg.measures.extend(new_measures)

    # The aggregate's output schema is grouping_keys ++ kept_measures.
    # Trim hint output_names to match.
    kept_indices: list[int] = list(range(n_groups)) + [
        n_groups + i for i in measure_kept
    ]
    if new_agg.HasField("common"):
        _prune_hint_output_names(new_agg.common, kept_indices)

    # Grouping references rebuilt: the proto has
    # ``Grouping.expression_references`` indexing into
    # ``grouping_expressions``. We didn't drop any grouping
    # expressions, so the references remain valid.

    out_remap: dict[int, int] = {}
    new_idx = 0
    for old_idx in kept_indices:
        if old_idx in required_outputs or old_idx < n_groups:
            out_remap[old_idx] = new_idx
        new_idx += 1
    return new_rel, out_remap


def _prune_hash_join(
    rel: algebra.Rel, required_outputs: set[int]
) -> tuple[algebra.Rel, dict[int, int]]:
    """Recurse into both sides at full width; don't change the join's interface.

    BoltML's ``HashJoinRel`` compresses its output via the
    ``RelCommon.Hint.output_names`` list — a ``join({"c"})`` against
    inputs ``[c, lhs]`` and ``[c, rhs]`` emits ``[l_c, l_lhs, r_rhs]``
    (the duplicate ``r_c`` join key is dropped at the join's
    boundary). The hint's positions are not a simple
    ``[left_columns, right_columns]`` concatenation, so a parent
    field-ref index ``i`` cannot be trivially translated to a
    ``(side, side_index)`` pair without re-deriving BoltML's
    name-prefixing scheme.

    Rather than reimplement the prefixing here, the rule:

    1. Asks each side for its **full** output width (via
       ``set(range(side_width))``). This guarantees the side's
       interface to the join stays unchanged — no column the join's
       hint references gets dropped.
    2. Recursing with the full-width requirement still lets each
       side prune its **own** subtree: the side's projects, reads,
       aggregates can drop columns whose surviving references are a
       subset of that side's outputs.
    3. Returns an identity remap to the parent — every original
       output position survives at the same index, so the parent's
       field-references remain valid without renumbering.

    The rule loses the opportunity to prune unused JOIN OUTPUT
    columns (e.g. a join exposing ``r_rhs`` whose downstream
    consumer never reads it) — but that's a far smaller win than
    pruning the join's INPUTS, which this rule still does
    transitively through the recursive descent into each side.
    """
    kind = get_rel_kind(rel)
    inner = getattr(rel, kind)

    left_width = _output_width(inner.left)
    right_width = _output_width(inner.right)
    width = _output_width(rel)

    # Try the *aware* path: use the hint output_names on the join and
    # on each side to compute what each side actually needs to provide.
    # BoltML's join renames every column via ``l_*`` / ``r_*`` prefixes
    # (see ``DataFrame.join`` lines ~999-1004), so the prefix tells us
    # the source side and the stripped name is the side's column name.
    # The join's hint may be a strict subset of (LHS' + RHS') because
    # duplicate join keys are dropped — we still match each kept name
    # back to its side's index.
    # ``common`` lives on each *inner* Rel proto (HashJoinRel,
    # ProjectRel, ReadRel, ...), not on the outer ``Rel`` envelope —
    # that's a oneof-of-sub-message. Look up via the side's inner.
    def _hint_names(side_rel: algebra.Rel) -> list[str]:
        sk = get_rel_kind(side_rel)
        if sk is None:
            return []
        side_inner = getattr(side_rel, sk)
        if not hasattr(side_inner, "HasField") or not side_inner.HasField("common"):
            return []
        return list(side_inner.common.hint.output_names)

    join_names = (
        list(inner.common.hint.output_names) if inner.HasField("common") else []
    )
    left_names = _hint_names(inner.left)
    right_names = _hint_names(inner.right)

    aware_ok = (
        len(join_names) == width
        and len(left_names) == left_width
        and len(right_names) == right_width
        and not inner.HasField("post_join_filter")  # too complex to renumber safely
    )
    boltmlDebugLog(
        "prune_join",
        f"width={width} left_width={left_width} right_width={right_width} "
        f"join_names={len(join_names)} left_names={len(left_names)} "
        f"right_names={len(right_names)} aware_ok={aware_ok} "
        f"required={sorted(required_outputs)[:8]}{'...' if len(required_outputs) > 8 else ''}",
    )

    if aware_ok:
        # Build the per-side requirement set from parent's needs +
        # the join's own key references. BoltML's HashJoinRel does
        # NOT use the stock substrait ``left_keys``/``right_keys``
        # repeated FieldReference fields — those stay empty. Instead
        # it populates a custom ``keys`` field: a list of
        # ``KeyExpression`` records, each with ``left`` (FieldRef into
        # LHS), ``right`` (FieldRef into RHS), and ``comparison``.
        # Without reading from ``inner.keys`` we'd never include the
        # join's actual key columns in either side's required set,
        # the LHS/RHS would prune them away, and the C++ executor
        # would assert "Right side join key not found in right side
        # output".
        left_req: set[int] = set()
        right_req: set[int] = set()
        for kref in inner.left_keys:
            left_req |= collect_field_indices(kref)
        for kref in inner.right_keys:
            right_req |= collect_field_indices(kref)
        if hasattr(inner, "keys"):
            for kpair in inner.keys:
                if hasattr(kpair, "left") and kpair.HasField("left"):
                    left_req |= collect_field_indices(
                        algebra.Expression(selection=kpair.left)
                    )
                if hasattr(kpair, "right") and kpair.HasField("right"):
                    right_req |= collect_field_indices(
                        algebra.Expression(selection=kpair.right)
                    )

        # The join's hint output_names inherit the side's
        # *already-prefixed* names (BoltML's ``DataFrame.join`` does
        # the prefix rename on each side BEFORE the join, so when the
        # join's hint quotes 'l_orderkey' it's the literal name on
        # the left input — not 'orderkey' with an l_ added). Match
        # directly against each side's hint, no prefix stripping.
        # Tie-break: if the same name appears on both sides (which
        # BoltML's prefix scheme makes very rare), prefer the side
        # whose prefix matches the leading char of the name.
        unmappable = False
        unmappable_name = None
        for idx in required_outputs:
            if not (0 <= idx < width):
                continue
            nm = join_names[idx]
            in_left = nm in left_names
            in_right = nm in right_names
            if in_left and in_right:
                # Use prefix as a tiebreaker.
                if nm.startswith("l_"):
                    left_req.add(left_names.index(nm))
                else:
                    right_req.add(right_names.index(nm))
            elif in_left:
                left_req.add(left_names.index(nm))
            elif in_right:
                right_req.add(right_names.index(nm))
            else:
                unmappable = True
                unmappable_name = nm
                break

        if unmappable:
            boltmlDebugLog(
                "prune_join",
                f"BAIL unmappable name={unmappable_name!r} "
                f"left_names_sample={left_names[:4]} right_names_sample={right_names[:4]} "
                f"join_names_sample={join_names[:4]}",
            )

        if not unmappable:
            new_left, left_remap = _prune(inner.left, left_req)
            new_right, right_remap = _prune(inner.right, right_req)

            # All of left_keys / right_keys must still be expressible
            # under the remap (they were in left_req / right_req, so
            # they should be in the remap, but be defensive). The
            # ``HashJoinRel.left_keys`` proto is
            # ``repeated Expression.FieldReference`` — wrap each in
            # an Expression so ``_renumber`` (which expects an
            # Expression) can walk it, then peel the selection back
            # out.
            try:
                new_left_keys = [
                    _renumber(algebra.Expression(selection=kref), left_remap).selection
                    for kref in inner.left_keys
                ]
                new_right_keys = [
                    _renumber(algebra.Expression(selection=kref), right_remap).selection
                    for kref in inner.right_keys
                ]
                # Renumber the BoltML-specific ``keys`` list too. Each
                # entry's ``.left`` / ``.right`` FieldReference points
                # into the now-renumbered LHS / RHS row types.
                new_keys_pairs = []
                if hasattr(inner, "keys"):
                    for kpair in inner.keys:
                        new_kp = type(kpair)()
                        new_kp.CopyFrom(kpair)
                        if kpair.HasField("left"):
                            renumbered_left = _renumber(
                                algebra.Expression(selection=kpair.left), left_remap
                            ).selection
                            new_kp.left.CopyFrom(renumbered_left)
                        if kpair.HasField("right"):
                            renumbered_right = _renumber(
                                algebra.Expression(selection=kpair.right), right_remap
                            ).selection
                            new_kp.right.CopyFrom(renumbered_right)
                        new_keys_pairs.append(new_kp)
            except KeyError as e:
                # Renumbering failed; fall back.
                boltmlDebugLog(
                    "prune_join",
                    f"BAIL renumber_keyerror missing_key={e} "
                    f"left_remap={sorted(left_remap)[:6]} right_remap={sorted(right_remap)[:6]}",
                )
                pass
            else:
                # Compute new join hint output_names by dropping any
                # name whose source column was dropped from its side.
                # Same direct-match logic as above: the join's hint
                # name appears verbatim in the side's hint.
                kept_join_indices: list[int] = []
                kept_join_names: list[str] = []
                for j_idx, nm in enumerate(join_names):
                    in_left = nm in left_names
                    in_right = nm in right_names
                    keep = False
                    if in_left and in_right:
                        if nm.startswith("l_"):
                            keep = left_names.index(nm) in left_remap
                        else:
                            keep = right_names.index(nm) in right_remap
                    elif in_left:
                        keep = left_names.index(nm) in left_remap
                    elif in_right:
                        keep = right_names.index(nm) in right_remap
                    else:
                        # No side claims this name — keep as-is.
                        keep = True
                    if keep:
                        kept_join_indices.append(j_idx)
                        kept_join_names.append(nm)

                # Every parent-required index MUST survive (we put its
                # source column into the side's required set above).
                # If somehow it didn't, fall back.
                missing = [
                    idx for idx in required_outputs if idx not in kept_join_indices
                ]
                if missing:
                    boltmlDebugLog(
                        "prune_join",
                        f"BAIL kept_check_failed missing={missing[:6]} "
                        f"kept_indices_sample={kept_join_indices[:8]} "
                        f"left_remap_size={len(left_remap)} right_remap_size={len(right_remap)}",
                    )
                if all(idx in kept_join_indices for idx in required_outputs):
                    boltmlDebugLog(
                        "prune_join",
                        f"AWARE PRUNED width {width}->{len(kept_join_indices)} "
                        f"left_width {left_width}->{_output_width(new_left)} "
                        f"right_width {right_width}->{_output_width(new_right)} "
                        f"left_req={sorted(left_req)} right_req={sorted(right_req)} "
                        f"kept_names={kept_join_names[:6]}{'...' if len(kept_join_names) > 6 else ''}",
                    )
                    new_rel = algebra.Rel()
                    new_rel.CopyFrom(rel)
                    new_inner = getattr(new_rel, kind)
                    new_inner.left.CopyFrom(new_left)
                    new_inner.right.CopyFrom(new_right)
                    del new_inner.left_keys[:]
                    new_inner.left_keys.extend(new_left_keys)
                    del new_inner.right_keys[:]
                    new_inner.right_keys.extend(new_right_keys)
                    # And the BoltML-specific keys list.
                    if hasattr(new_inner, "keys"):
                        del new_inner.keys[:]
                        new_inner.keys.extend(new_keys_pairs)
                    if new_inner.HasField("common"):
                        del new_inner.common.hint.output_names[:]
                        new_inner.common.hint.output_names.extend(kept_join_names)

                    out_remap = {old: new for new, old in enumerate(kept_join_indices)}
                    return new_rel, out_remap

    # Safe fallback: recurse at full width, identity remap.
    new_left, _ = _prune(inner.left, set(range(left_width)))
    new_right, _ = _prune(inner.right, set(range(right_width)))
    if _output_width(new_left) != left_width or _output_width(new_right) != right_width:
        return _no_prune(rel)

    new_rel = algebra.Rel()
    new_rel.CopyFrom(rel)
    new_inner = getattr(new_rel, kind)
    new_inner.left.CopyFrom(new_left)
    new_inner.right.CopyFrom(new_right)
    out_remap = {i: i for i in range(width)}
    return new_rel, out_remap


def _prune_sort(
    rel: algebra.Rel, required_outputs: set[int]
) -> tuple[algebra.Rel, dict[int, int]]:
    """Sort passes columns through; sort-key refs add to input requirements."""
    sort = rel.sort
    width = _output_width(rel)

    sort_refs: set[int] = set()
    for sf in sort.sorts:
        sort_refs |= collect_field_indices(sf.expr)
    needed = (set(required_outputs) & set(range(width))) | sort_refs

    new_input, input_remap = _prune(sort.input, needed)
    new_sorts = []
    for sf in sort.sorts:
        new_sf = algebra.SortField()
        new_sf.CopyFrom(sf)
        new_sf.expr.CopyFrom(_renumber(sf.expr, input_remap))
        new_sorts.append(new_sf)

    new_rel = algebra.Rel()
    new_rel.CopyFrom(rel)
    new_sort = new_rel.sort
    new_sort.input.CopyFrom(new_input)
    del new_sort.sorts[:]
    new_sort.sorts.extend(new_sorts)

    out_remap = {
        old: input_remap[old] for old in sorted(required_outputs) if old in input_remap
    }
    return new_rel, out_remap


def _prune_fetch(
    rel: algebra.Rel, required_outputs: set[int]
) -> tuple[algebra.Rel, dict[int, int]]:
    """Fetch passes columns through; no extra refs."""
    fetch = rel.fetch
    width = _output_width(rel)
    needed = set(required_outputs) & set(range(width))

    new_input, input_remap = _prune(fetch.input, needed)

    new_rel = algebra.Rel()
    new_rel.CopyFrom(rel)
    new_rel.fetch.input.CopyFrom(new_input)

    out_remap = {
        old: input_remap[old] for old in sorted(required_outputs) if old in input_remap
    }
    return new_rel, out_remap


# ---------------------------------------------------------------------------
# Rule
# ---------------------------------------------------------------------------


class PruneUnusedColumns(Rule):
    """Drop unused output columns from every Rel that supports it.

    Top-down requirement propagation, bottom-up rewrite. The
    ``RelRoot``'s output schema is preserved exactly: every column
    listed in ``RelRoot.names`` is treated as required at the root.
    Inside the plan, columns that no upstream consumer references are
    pruned from reads, projects, joins, and aggregates.

    Pure: the input ``Plan`` is not mutated. Idempotent: a second run
    over the rule's output produces a byte-equal Plan, so the
    fixpoint loop in ``SubstraitOptimizerPipeline`` converges in a
    single extra iteration.
    """

    name: str = "prune_unused_columns"

    def apply(self, p: plan.Plan) -> plan.Plan:
        out = plan.Plan()
        out.CopyFrom(p)
        for plan_rel in out.relations:
            if not plan_rel.HasField("root"):
                continue
            if not plan_rel.root.HasField("input"):
                continue
            root_rel = plan_rel.root.input
            n = _output_width(root_rel)
            required = set(range(n))
            new_root, _ = _prune(root_rel, required)
            plan_rel.root.input.CopyFrom(new_root)
        return out
