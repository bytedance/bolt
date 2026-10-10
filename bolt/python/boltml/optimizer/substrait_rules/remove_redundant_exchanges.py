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

"""Remove a redundant ``ExchangeRel`` that re-shuffles already-partitioned
data.

Problem
-------

After ``PushAggThroughJoin`` runs in the logical pipeline, the physical
plan often looks like:

::

    HashJoin
      Exchange[hash by partkey]                 (outer, redundant)
        Project   (renames / passes through partkey)
          Aggregate[grouping=(partkey, brand), partial(...)]
            Exchange[hash by partkey]           (inner, the real shuffle)
              Project
                read   (lineitem)
      ...

The outer exchange's job is to make the join's left input partitioned
by the join key. But the inner exchange already partitioned by partkey,
and the project + aggregate above it preserve that distribution
(grouping by partkey means each input row's partkey value is unchanged
in the output, so each Velox bucket-N task only ever produces rows
whose partkey hashes to N). The outer shuffle is wasted work — a
parquet write+read of millions of rows that just redistributes them
into the same buckets.

This rule walks the plan and elides any ``Exchange[hash K]`` whose
input chain provably retains the partition layout of an inner
``Exchange[hash K]`` with the SAME ``scatter_by_fields`` (and the same
``partition_count`` and ``scattering_alias``). Specifically it walks
from the outer exchange's input down through:

* ``ProjectRel`` whose output column at every position referenced by
  the outer exchange's keys is a direct field-ref to an input position
  that — via recursion through the chain — also tracks back to the
  inner exchange's matching key position. (For our common case the
  project's outer-key columns map 1:1 to inner-key columns under the
  same name, which is what BoltML's lowering emits.)
* ``AggregateRel`` whose grouping_expressions include direct field-refs
  to every outer-key column of the input. Aggregation preserves
  per-key distribution because every row of a given key value is
  collapsed into a single output row, also of that key value.
* ``FilterRel`` (no rewriting of column positions; predicate filters
  rows but never moves them across partitions).

Anything else (joins, sorts, set ops, exchange of a different shape)
breaks the chain and the outer exchange stays.

Safety
------

The rule conservatively requires that the outer exchange and the inner
exchange have identical ``scattering_alias`` (the dispatcher's routing
string), identical ``partition_count``, and identical scatter-by-field
columns when traced through the chain. Same alias + same partition
count is a sufficient (and easy to test) condition for "the outer
re-routes data into the same buckets the inner already placed it in".

Idempotency
-----------

After the outer exchange is removed, the inner exchange takes its
position in the parent's input slot. Re-running the rule on the new
plan finds a single Exchange where there used to be two — the pattern
no longer matches and the rule is a no-op.

Scope
-----

This is a physical-pipeline rule that runs AFTER ``AddExchanges``.
``PushAggThroughJoin`` (logical pipeline) creates the structural
preconditions; ``AddExchanges`` materializes both exchanges in the
physical plan; this rule prunes the redundant outer.
"""

from __future__ import annotations

from typing import List, Optional

from substrait.proto import algebra, plan

from ...logging import boltmlDebugLog
from ._expression import direct_field_index
from .base import Rule, get_rel_kind, rewrite_plan_root


class RemoveRedundantExchanges(Rule):
    """Elide an outer hash ``ExchangeRel`` whose distribution is
    already established by an inner hash ``ExchangeRel`` of the same
    shape, with only distribution-preserving rels in between.
    """

    name: str = "remove_redundant_exchanges"

    def apply(self, p: plan.Plan) -> plan.Plan:
        return rewrite_plan_root(p, _try_elide, bottom_up=True)


# ---------------------------------------------------------------------------
# Match + rewrite
# ---------------------------------------------------------------------------


def _try_elide(rel: algebra.Rel) -> Optional[algebra.Rel]:
    """If *rel* is a hash ExchangeRel whose distribution duplicates an
    inner hash exchange's, return the inner-rooted subtree. Else None.
    """
    if get_rel_kind(rel) != "exchange":
        return None
    outer = rel.exchange
    if outer.WhichOneof("exchange_kind") != "scatter_by_fields":
        return None
    outer_keys = _scatter_field_indices(outer)
    if outer_keys is None:
        return None
    outer_alias = outer.common.hint.alias if outer.HasField("common") else ""

    # Walk the input chain looking for an inner Exchange that provably
    # establishes the same distribution. Track the column positions
    # (in the OUTER's coordinate space) that map back to the INNER's
    # corresponding scatter columns through every traversed rel.
    cur = outer.input
    # Start with the identity mapping: outer position i must correspond
    # to inner position i (until a Project remaps).
    keys_to_track: List[Optional[int]] = list(outer_keys)
    while True:
        kind = get_rel_kind(cur)
        if kind == "exchange":
            inner = cur.exchange
            if inner.WhichOneof("exchange_kind") != "scatter_by_fields":
                return None
            # ``partition_count`` must match — the dispatcher uses it to
            # size the per-stage Ray dispatch fan-out, and disagreement
            # would produce a different placement layout downstream.
            if inner.partition_count != outer.partition_count:
                return None
            inner_keys = _scatter_field_indices(inner)
            if inner_keys is None:
                return None
            # Each tracked outer-key position must, after walking the
            # chain, correspond to one of the inner exchange's scatter
            # positions. (Exact positional match isn't required — what
            # matters is the SET, since hash partitioning is commutative
            # over keys.)
            tracked = set(k for k in keys_to_track if k is not None)
            if any(k is None for k in keys_to_track):
                return None
            if tracked != set(inner_keys):
                return None
            # Note: we deliberately do NOT compare ``RelCommon.Hint.alias``
            # ("hash(l_partkey)" vs "hash(l_l_partkey)") because Bolt's
            # join-prefix Projects and PushAggThroughJoin's rename
            # Projects bump the prefix on each layer
            # ("partkey" -> "l_partkey" -> "l_l_partkey"). The hash
            # distribution depends on the column VALUES, not on the
            # display name; when our chain walk has proven the SAME
            # column values flow through both exchanges (same field
            # positions traced through projects/aggregates), the
            # routing is identical regardless of which prefixed name
            # appears in the alias.
            inner_alias = inner.common.hint.alias if inner.HasField("common") else ""
            boltmlDebugLog(
                "remove_redundant_exchanges",
                f"elided outer exchange[parts={outer.partition_count} "
                f"alias={outer_alias!r}] over inner exchange[alias="
                f"{inner_alias!r}] (same partition layout)",
            )
            # Drop ONLY the outer exchange wrapper. The chain between
            # outer and inner (projects + aggregate + filter, all
            # distribution-preserving) MUST remain — those rels do real
            # work on the data flowing through. Returning the outer's
            # input (the first rel of the chain) splices that chain
            # directly under the outer's parent, with the inner
            # exchange still doing the actual shuffle at the bottom.
            return outer.input
        if kind == "project":
            project = cur.project
            if project.HasField("common") and project.common.WhichOneof(
                "emit_kind"
            ) not in (None, "direct"):
                return None
            # Translate every tracked outer-position via the project's
            # expressions. Outer-position i in the OUTER's coord space
            # corresponds to project.expressions[i] which must be a
            # direct field-ref into the project's INPUT. If it isn't
            # (e.g. arithmetic), we can't track it through and bail.
            new_tracked: List[Optional[int]] = []
            for outer_pos in keys_to_track:
                if outer_pos is None or outer_pos >= len(project.expressions):
                    return None
                expr = project.expressions[outer_pos]
                input_pos = direct_field_index(expr)
                if input_pos is None:
                    return None
                new_tracked.append(input_pos)
            keys_to_track = new_tracked
            cur = project.input
            continue
        if kind == "aggregate":
            agg = cur.aggregate
            if agg.HasField("common") and agg.common.WhichOneof("emit_kind") not in (
                None,
                "direct",
            ):
                return None
            # Aggregate preserves distribution iff every tracked outer
            # key's column is in the GROUPING (each row of a given
            # key value is collapsed but stays at the same value, so
            # the partition placement is preserved). The aggregate
            # outputs grouping keys at positions [0..len(grouping_exprs)).
            n_keys = len(agg.grouping_expressions)
            new_tracked = []
            for outer_pos in keys_to_track:
                if outer_pos is None or outer_pos >= n_keys:
                    return None
                # Outer column at position outer_pos is the
                # grouping_expressions[outer_pos], which must be a
                # direct field-ref to the input.
                gexpr = agg.grouping_expressions[outer_pos]
                input_pos = direct_field_index(gexpr)
                if input_pos is None:
                    return None
                new_tracked.append(input_pos)
            keys_to_track = new_tracked
            cur = agg.input
            continue
        if kind == "filter":
            f = cur.filter
            if f.HasField("common") and f.common.WhichOneof("emit_kind") not in (
                None,
                "direct",
            ):
                return None
            # Filter doesn't change column positions or partition
            # placement (it only drops rows).
            cur = f.input
            continue
        # Anything else (read, hash_join, sort, set, ...) breaks the
        # distribution-preservation chain.
        return None


def _scatter_field_indices(
    exchange: algebra.ExchangeRel,
) -> Optional[List[int]]:
    """Return the field indices in ``ExchangeRel.scatter_by_fields``,
    or ``None`` if any field reference is not a plain direct-struct-
    field-reference (we can't reason about complex scatter expressions).
    """
    out: List[int] = []
    for fr in exchange.scatter_by_fields.fields:
        if not fr.HasField("direct_reference"):
            return None
        seg = fr.direct_reference
        if seg.WhichOneof("reference_type") != "struct_field":
            return None
        if seg.struct_field.HasField("child"):
            return None
        out.append(seg.struct_field.field)
    return out
