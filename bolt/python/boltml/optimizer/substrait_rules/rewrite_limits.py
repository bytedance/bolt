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

"""Rewrite ``FetchRel`` chains: push through ``ProjectRel``, merge adjacents."""

from __future__ import annotations

from typing import Optional

from substrait.proto import algebra, plan

from ._expression import is_direct_field_ref
from .base import (
    Rule,
    get_rel_kind,
    rewrite_plan_root,
)


class RewriteLimits(Rule):
    """Rewrite limit chains in two complementary ways (one rule, one pass).

    1. ``FetchRel(ProjectRel(X))`` → ``ProjectRel(FetchRel(X))`` when the
       project contains only field references and literals.
       Swapping keeps the limit closer to the data so the project does
       less work; this also exposes adjacent ``Fetch(Fetch(...))`` for
       step (2).

    2. ``FetchRel(FetchRel(X))`` → single ``FetchRel`` whose
       ``(count, offset)`` is the algebraic merge of the two, or an
       always-false filter when the merged range is empty.
       Given outer ``LIMIT a OFFSET b`` and inner ``LIMIT c OFFSET d``:

           merged_offset = b + d
           merged_count  = (
               c - b              if outer count is unlimited
               min(a, max(c-b, 0)) if both have counts
               a                  if inner count is unlimited
           )

       This mirrors the Python ``_limit_merge`` helper in
       ``optimizer/pipeline.py``.

    Both transforms run bottom-up in a single pass, so a swap that exposes
    an adjacent-fetch site is picked up immediately by the parent visit.
    """

    name: str = "rewrite_limits"

    def apply(self, p: plan.Plan) -> plan.Plan:
        """Apply both transforms bottom-up. Returns a fresh ``Plan``."""
        return rewrite_plan_root(p, _rewrite_one, bottom_up=True)


def _rewrite_one(rel: algebra.Rel) -> Optional[algebra.Rel]:
    if get_rel_kind(rel) != "fetch":
        return None

    inner_kind = get_rel_kind(rel.fetch.input)
    if inner_kind == "project":
        return _swap_fetch_project(rel)
    if inner_kind == "fetch":
        return _merge_adjacent_fetches(rel)
    return None


# ---------------------------------------------------------------------------
# (1) Fetch over Project: swap.
# ---------------------------------------------------------------------------


def _swap_fetch_project(rel: algebra.Rel) -> Optional[algebra.Rel]:
    """Rewrite ``Fetch(Project(X))`` to ``Project(Fetch(X))``.

    Field references keep their input column space across the swap.
    Computed expressions may contain batch-dependent or stateful Python UDFs,
    so preserving the row count alone does not make the swap safe.
    """
    fetch = rel.fetch
    proj = fetch.input.project
    if any(
        not is_direct_field_ref(expr) and expr.WhichOneof("rex_type") != "literal"
        for expr in proj.expressions
    ):
        return None

    new_fetch = algebra.FetchRel()
    new_fetch.CopyFrom(fetch)
    new_fetch.input.CopyFrom(proj.input)

    new_project = algebra.ProjectRel()
    new_project.CopyFrom(proj)
    new_project.input.CopyFrom(algebra.Rel(fetch=new_fetch))

    return algebra.Rel(project=new_project)


# ---------------------------------------------------------------------------
# (2) Fetch over Fetch: merge.
# ---------------------------------------------------------------------------


def _fetch_bound(fetch: algebra.FetchRel, field: str, default: int) -> Optional[int]:
    """Read a scalar or literal bound; return ``None`` if it is unknown."""
    mode = fetch.WhichOneof(f"{field}_mode")
    if mode is None:
        return default
    if mode == field:
        return getattr(fetch, field)
    expr = getattr(fetch, mode)
    if expr.WhichOneof("rex_type") != "literal":
        return None
    lit = expr.literal
    kind = lit.WhichOneof("literal_type")
    if kind not in ("i32", "i64"):
        return None
    return getattr(lit, kind)


def _merge_counts(
    outer_count: Optional[int],
    outer_offset: int,
    inner_count: Optional[int],
    inner_offset: int,
) -> Optional[tuple[Optional[int], int]]:
    """Compute the merged ``(count, offset)`` of two adjacent fetches.

    Returns ``None`` when the merge cannot be performed (e.g. negative
    offsets). The math mirrors ``optimizer/pipeline.py::_limit_merge``.
    """
    if outer_offset < 0 or inner_offset < 0:
        return None
    merged_offset = inner_offset + outer_offset
    if inner_count is None:
        merged_count: Optional[int] = outer_count
    else:
        remaining = max(inner_count - outer_offset, 0)
        merged_count = remaining if outer_count is None else min(remaining, outer_count)
    return merged_count, merged_offset


def _merge_adjacent_fetches(rel: algebra.Rel) -> Optional[algebra.Rel]:
    outer = rel.fetch
    inner = outer.input.fetch
    # Merging must not discard the inner output projection.
    if inner.common.HasField("emit"):
        return None

    outer_count = _fetch_bound(outer, "count", -1)
    outer_offset = _fetch_bound(outer, "offset", 0)
    inner_count = _fetch_bound(inner, "count", -1)
    inner_offset = _fetch_bound(inner, "offset", 0)
    if None in (outer_count, outer_offset, inner_count, inner_offset):
        return None
    merged = _merge_counts(
        None if outer_count < 0 else outer_count,
        outer_offset,
        None if inner_count < 0 else inner_count,
        inner_offset,
    )
    if merged is None:
        return None
    merged_count, merged_offset = merged

    if merged_count == 0:
        # Bolt's native LimitNode requires a strictly positive count.
        empty = algebra.FilterRel(
            input=inner.input,
            condition=algebra.Expression(
                literal=algebra.Expression.Literal(boolean=False)
            ),
        )
        if outer.HasField("common"):
            empty.common.CopyFrom(outer.common)
        return algebra.Rel(filter=empty)

    new_fetch = algebra.FetchRel()
    # Preserve the outer common (output names / hints, if any).
    new_fetch.CopyFrom(outer)
    new_fetch.input.CopyFrom(inner.input)

    # Reset offset/count to the merged values.
    new_fetch.ClearField("offset_mode")
    new_fetch.ClearField("count_mode")
    new_fetch.offset_expr.CopyFrom(
        algebra.Expression(literal=algebra.Expression.Literal(i64=merged_offset))
    )
    if merged_count is not None:
        new_fetch.count_expr.CopyFrom(
            algebra.Expression(literal=algebra.Expression.Literal(i64=merged_count))
        )

    return algebra.Rel(fetch=new_fetch)
