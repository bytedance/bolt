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

"""Substrait-Plan / StageDAG → explain text renderers.

Consumed by ``boltml.execute.explain`` to render the "Optimized
Logical Plan" and "Physical Plan" sections of ``DataFrame.explain()``
directly from the Substrait pipeline's outputs.

Two public functions:

* ``render_logical_plan_text(plan)`` — walks an optimized Substrait
  ``Plan`` and produces text matching the legacy
  ``renderLogicalPlan`` format (``Filter[<expr>]``, ``Project[a, b]``,
  ``Limit[offset=N, count=M]``, ``HashJoin[...]``, etc.) so the
  existing ``test_optimizer.py`` assertions about op ordering and
  count work unchanged.
* ``render_physical_plan_text(stage_dag, base_text)`` — walks the
  ``StageDAG``'s placeholders to render the legacy
  ``PhysicalPlan.explain`` shape (``Exchanges[N]`` count followed by
  one ``Exchange[i]: <kind> id=... partitioning=... partitions=...
  output=... route=src->dst`` line per exchange).

Both renderers preserve the public explain text contract so
``df.explain()`` output continues to match what the legacy chain
produced for the same DataFrame.
"""

from __future__ import annotations

from typing import List

from substrait.proto import algebra, plan as plan_pb2

from .rel import rel_output_names


def render_logical_plan_text(substraitPlan: plan_pb2.Plan | None) -> str:
    """Render *substraitPlan* as legacy-format logical plan text.

    Walks the rel tree top-down (input first, then current rel) so
    the leaf source appears first and the outermost rel last —
    matching the order ``renderLogicalPlan`` produces from a
    ``LogicalPlan``'s ``operations`` chain. Returns an empty string
    for a ``None`` or empty plan, mirroring the legacy
    ``optimizedPlan.explain()`` behaviour for an empty
    ``LogicalPlan``.
    """
    if substraitPlan is None or len(substraitPlan.relations) == 0:
        return ""
    root = substraitPlan.relations[0].root.input
    lines: List[str] = []
    _render_rel(root, lines)
    return "\n".join(lines)


def render_physical_plan_text(
    stageDag,
    base_text: str,
) -> str:  # noqa: ANN001
    """Render the StageDAG's exchanges as legacy ``PhysicalPlan`` text.

    *base_text* is the text to render before the exchange list (the
    legacy code uses ``executionPlan.explain()`` here — i.e. the
    optimized logical plan text). The output shape matches
    ``optimizer.physical.PhysicalPlan.explain``:

    ::

        {base_text}
        Exchanges[{N}]
        Exchange[0]: {kind} id={exchange_id} partitioning={p} partitions={c} output={names_csv} route={src}->{dst} placeholder=PlaceholderRead[{src}/{exchange_id}]
        ...

    Each placeholder across all consumer stages contributes one
    ``Exchange[i]`` line, in stage-then-placeholder order. The
    Substrait dispatcher does not distinguish ``repartition`` /
    ``broadcast`` / ``gather`` kinds in the StageDAG today (every
    BoltML exchange is a repartition by hash or round-robin), so
    ``kind`` is always ``"repartition"``. If broadcast joins land,
    extend the kind derivation here.
    """
    lines = [base_text] if base_text else []
    exchanges: List[tuple] = []
    for stage in stageDag.stages:
        for placeholder in stage.inputPlaceholders:
            exchanges.append((stage, placeholder))
    lines.append(f"Exchanges[{len(exchanges)}]")
    for index, (consumer, placeholder) in enumerate(exchanges):
        kind = "repartition"
        parts = [
            f"Exchange[{index}]: {kind}",
            f"id={placeholder.exchangeId}",
            f"partitioning={placeholder.partitioning}",
            f"partitions={placeholder.partitionCount}",
        ]
        if placeholder.outputNames:
            parts.append(f"output={','.join(placeholder.outputNames)}")
        parts.append(f"route={placeholder.sourceStageId}->{consumer.stageId}")
        parts.append(
            f"placeholder=PlaceholderRead[{placeholder.sourceStageId}/{placeholder.exchangeId}]"
        )
        lines.append(" ".join(parts))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Rel → text walker (logical-plan shape)
# ---------------------------------------------------------------------------

# Type URL the BoltML lowering stamps on the LocalShuffleExtension's
# ExtensionSingleRel.detail. Mirrors
# ``optimizer.substrait_rules.add_exchanges.SHUFFLE_EXTENSION_TYPE_URL``;
# duplicated here to avoid a dispatcher → optimizer import.
_SHUFFLE_EXTENSION_TYPE_URL = "type.googleapis.com/substrait.bolt.LocalShuffleExtension"


def _render_rel(rel: algebra.Rel, lines: List[str]) -> None:
    """Append legacy-format text lines for *rel* (and its inputs)."""
    kind = rel.WhichOneof("rel_type")
    if kind is None:
        return
    if kind == "read":
        lines.append(_render_read(rel.read))
        return
    if kind in ("filter", "project", "fetch", "sort", "aggregate", "exchange"):
        inner = getattr(rel, kind)
        _render_rel(inner.input, lines)
        line = _render_single_input_rel(kind, inner)
        if line:
            lines.append(line)
        return
    if kind == "extension_single":
        _render_rel(rel.extension_single.input, lines)
        line = _render_extension_single(rel.extension_single)
        if line:
            lines.append(line)
        return
    if kind in ("hash_join", "join", "merge_join", "cross", "nested_loop_join"):
        join = getattr(rel, kind)
        _render_rel(join.left, lines)
        line = _render_join(kind, join)
        if line:
            lines.append(line)
        # Right side is the rhs subplan; render with the same "rhs::"
        # indent the legacy ``renderLogicalPlan`` uses for joins.
        rhs_lines: List[str] = []
        _render_rel(join.right, rhs_lines)
        for r in rhs_lines:
            lines.append(f"  rhs::{r}")
        return
    # Fallback: emit the rel kind verbatim. Consistent with the legacy
    # ``_formatLogicalOp`` final ``return [op.kind]`` branch.
    lines.append(kind)


def _render_read(read: algebra.ReadRel) -> str:
    """Map a ReadRel back to the legacy source label."""
    if read.HasField("virtual_table"):
        return "Values"
    if read.HasField("local_files"):
        return "TableRead"
    if read.HasField("named_table"):
        # Placeholder ReadRel (substituted at exchange cuts) uses
        # ``named_table``. Keep its label short and parseable.
        names = list(read.named_table.names)
        return f"Read[{','.join(names)}]" if names else "Read"
    return "Read"


def _render_single_input_rel(kind: str, inner) -> str:  # noqa: ANN001
    """Render a single-input rel (filter/project/fetch/sort/aggregate/exchange)."""
    names = rel_output_names_from_inner(kind, inner)
    if kind == "filter":
        return "Filter[?]"  # condition rendering is non-trivial; placeholder OK
    if kind == "project":
        return f"Project[{', '.join(names)}]" if names else "Project"
    if kind == "fetch":
        offset = _fetch_int(inner, "offset")
        count = _fetch_int(inner, "count")
        return f"Limit[offset={offset}, count={count}]"
    if kind == "sort":
        cols = []
        for sf in inner.sorts:
            # Render direction name using SortField.SortDirection enum.
            direction = algebra.SortField.SortDirection.Name(sf.direction)
            cols.append(direction)
        return f"OrderBy[{', '.join(cols)}]" if cols else "OrderBy"
    if kind == "aggregate":
        keys = []
        for grouping in inner.groupings:
            for ref in grouping.expression_references:
                keys.append(f"$grp{ref}")
        if names:
            cols = ", ".join(names)
        else:
            cols = ", ".join(keys)
        return f"Aggregate[{cols}]"
    if kind == "exchange":
        # Render exchange wrappers compactly; the per-exchange info
        # appears in the Physical Plan section.
        return "Exchange"
    return kind


def _render_extension_single(ext) -> str:  # noqa: ANN001
    """Render an ExtensionSingleRel — currently only ``LocalShuffleExtension``."""
    if ext.detail.type_url == _SHUFFLE_EXTENSION_TYPE_URL:
        # The seed lives inside the ``Any`` payload; the AddExchanges
        # rule decodes it via ``_decode_shuffle_seed``. For explain
        # text we omit the seed (keeps the renderer extension-payload-
        # opaque); the Physical Plan section's
        # ``partitioning=shuffle(seed=N)`` carries the seed.
        return "Shuffle"
    # Other extension rels include Python UDFs and similar. Render as
    # a generic Extension marker; legacy text used Python[fn] for the
    # python case but the function name lives behind the extension
    # registry — keep this conservative.
    return "Extension"


def _render_join(kind: str, join) -> str:  # noqa: ANN001
    """Render a join rel as ``HashJoin[...]`` matching the legacy text."""
    join_type_name = ""
    if hasattr(join, "type"):
        # JoinRel.JoinType / HashJoinRel.JoinType have the same enum
        # shape (UNSPECIFIED / INNER / OUTER / ...).
        try:
            join_type_name = type(join).JoinType.Name(join.type)
        except (ValueError, AttributeError):
            join_type_name = ""
    label_map = {"hash_join": "HashJoin", "merge_join": "MergeJoin"}
    label = label_map.get(kind, "Join")
    suffix = f"[{join_type_name}]" if join_type_name else ""
    return f"{label}{suffix}"


def rel_output_names_from_inner(kind: str, inner) -> tuple:  # noqa: ANN001
    """Read ``inner.common.hint.output_names`` if present.

    Wrapper that takes the already-unwrapped ``inner`` (e.g.
    ``rel.project``, ``rel.aggregate``) instead of the outer
    ``algebra.Rel`` — saves ``getattr`` round-trips inside the
    walker. Falls back to ``()`` when the hint is absent.
    """
    if not inner.HasField("common"):
        return ()
    if not inner.common.HasField("hint"):
        return ()
    return tuple(inner.common.hint.output_names)


def _fetch_int(fetch, name: str) -> int:  # noqa: ANN001
    """Extract the i32/i64 literal from FetchRel.{offset,count}_expr.

    Mirrors the lowering shape in ``substrait/rel.py``:fetchRel which
    wraps the int as ``algebra.Expression.Literal``.
    """
    expr = getattr(fetch, f"{name}_expr")
    if not expr.HasField("literal"):
        return 0
    lit = expr.literal
    for field_name in ("i64", "i32"):
        if lit.HasField(field_name):
            return getattr(lit, field_name)
    return 0


# Re-export the names helper so callers can derive output names from a
# top-level rel without importing ``rel_output_names`` separately.
__all__ = [
    "render_logical_plan_text",
    "render_physical_plan_text",
    "rel_output_names",
]
