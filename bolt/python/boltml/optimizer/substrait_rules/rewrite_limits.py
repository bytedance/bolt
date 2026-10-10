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

from .base import (
    Rule,
    get_rel_kind,
    rewrite_plan_root,
)


# A FetchRel without a count_expr means "no upper bound" (take everything
# after offset). The Python pipeline represents that with ``count is None``;
# in Substrait the count_expr field is simply not set.
_UNLIMITED: Optional[int] = None


class RewriteLimits(Rule):
    """Rewrite limit chains in two complementary ways (one rule, one pass).

    1. ``FetchRel(ProjectRel(X))`` → ``ProjectRel(FetchRel(X))``.
       Swapping keeps the limit closer to the data so the project does
       less work; this also exposes adjacent ``Fetch(Fetch(...))`` for
       step (2).

    2. ``FetchRel(FetchRel(X))`` → single ``FetchRel`` whose
       ``(count, offset)`` is the algebraic merge of the two.
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


def _swap_fetch_project(rel: algebra.Rel) -> algebra.Rel:
    """Rewrite ``Fetch(Project(X))`` to ``Project(Fetch(X))``.

    BoltML projects are row-preserving and replacement-style with no
    field-ref shift across the swap, so the limit can be applied to the
    project's input directly. Field-references inside the project's
    ``expressions`` are unchanged because the input column space is the
    same on both sides of the swap.
    """
    fetch = rel.fetch
    proj = fetch.input.project

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


def _fetch_offset(fetch: algebra.FetchRel) -> int:
    """Read a non-negative offset from a ``FetchRel``.

    Substrait stores offset in ``offset_expr`` as a signed 64-bit literal;
    a missing field means zero.
    """
    if not fetch.HasField("offset_expr"):
        return 0
    expr = fetch.offset_expr
    if expr.WhichOneof("rex_type") != "literal":
        return 0
    lit = expr.literal
    if lit.WhichOneof("literal_type") != "i64":
        return 0
    return lit.i64


def _fetch_count(fetch: algebra.FetchRel) -> Optional[int]:
    """Read the count from a ``FetchRel`` (``None`` means unlimited).

    A missing ``count_expr`` or a negative value is treated as
    "unlimited", matching BoltML's ``fetchRel`` constructor in
    ``boltml/substrait/rel.py``.
    """
    if not fetch.HasField("count_expr"):
        return _UNLIMITED
    expr = fetch.count_expr
    if expr.WhichOneof("rex_type") != "literal":
        return _UNLIMITED
    lit = expr.literal
    if lit.WhichOneof("literal_type") != "i64":
        return _UNLIMITED
    if lit.i64 < 0:
        return _UNLIMITED
    return lit.i64


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

    merged = _merge_counts(
        _fetch_count(outer),
        _fetch_offset(outer),
        _fetch_count(inner),
        _fetch_offset(inner),
    )
    if merged is None:
        return None
    merged_count, merged_offset = merged

    new_fetch = algebra.FetchRel()
    # Preserve the outer common (output names / hints, if any).
    new_fetch.CopyFrom(outer)
    new_fetch.input.CopyFrom(inner.input)

    # Reset offset/count to the merged values.
    new_fetch.ClearField("offset_expr")
    new_fetch.ClearField("count_expr")
    new_fetch.offset_expr.CopyFrom(
        algebra.Expression(literal=algebra.Expression.Literal(i64=merged_offset))
    )
    if merged_count is not None:
        new_fetch.count_expr.CopyFrom(
            algebra.Expression(literal=algebra.Expression.Literal(i64=merged_count))
        )

    return algebra.Rel(fetch=new_fetch)
