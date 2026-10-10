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

"""Cost-based join reordering via global brute-force enumeration.

This is a Python port of daft's ``BruteForceJoinOrderer``
(``Daft/src/daft-logical-plan/src/optimization/rules/reorder_joins/
brute_force_join_order.rs``). The earlier per-join greedy V1 swap rule
(``join_reorder.v1.bak.py``) regressed Q5 SF=1 by 28% and OOMed
Q5 SF=10 because a local swap cannot fix a globally-bad shape: it
could put the 60M lineitem on the BUILD side and crash.

Algorithm
---------

1.  Walk the Substrait Plan tree top-down. At each ``HashJoinRel``
    root that is part of a *maximal pure-INNER* sub-graph, extract:

    * **Leaves**: the relations underneath the join sub-graph. Bolt
      wraps each side of every ``HashJoinRel`` in two ``ProjectRel``s
      (one to reorder columns putting the join key first, one to
      prefix every column with ``l_`` or ``r_`` to disambiguate
      the join's output). Walking through these two projects gets
      us back to either another ``HashJoinRel`` (recurse) or any
      other rel kind (a leaf).
    * **Edges**: each ``HashJoinRel.keys`` entry pairs a left and
      right field reference whose column names (after stripping the
      ``l_``/``r_`` prefix) tell us which leaves are joined and on
      which columns.

2.  Build a join graph (relations as nodes, equality predicates as
    edges, with key-equivalence inference: if A.x = B.x and B.x =
    C.x then A.x = C.x).

3.  Estimate per-leaf cardinalities by reading parquet footers
    (``pyarrow.parquet.read_metadata().num_rows`` summed across
    files). A leaf wrapped in a ``FilterRel`` whose condition
    structurally looks like a range comparison against a literal
    (the V1 heuristic; lifted unchanged) gets a flat 0.1
    selectivity factor.

4.  Brute-force enumerate every partition of the leaf-set into
    (left, right) subsets, recursively cost each, and pick the
    minimum. The cost model:

        out_cardinality = left_card * right_card / domain_size
        cost            = left_cost + right_cost + out_cardinality
        domain_size     = min(left_card, right_card)   # daft fallback

    At every join, the smaller side is forced to the LEFT (Bolt's
    HashJoin builds on RHS — verified by ``HashBuild.cpp:97`` and
    ``add_exchanges._join_key_field_indices``).

5.  Rebuild the sub-tree from the optimal order. Each new join
    re-creates the two ``ProjectRel`` prefix wrappers Bolt expects.
    The output schema of the rewritten root is forced to match the
    original via a top-level ``ProjectRel`` reorder.

Scope (V2)
----------

* Only sub-graphs of ``HashJoinRel`` (Bolt's physical join rel
  kind) of ``JOIN_TYPE_INNER`` with no ``post_join_filter`` and no
  cross product are considered. Anything else (LEFT / OUTER /
  SEMI / ANTI / MARK) is preserved untouched.
* Cardinality estimation falls back to skipping the rewrite if a
  leaf's parquet metadata cannot be read. Better to leave the
  plan alone than to swap based on noise.
* Maximum 10 leaves per sub-graph (2^10 = 1024 subsets, ~1M
  partitions to enumerate); a larger sub-graph is left unchanged.
  TPC-H tops out at q5/q9 with 6 relations.

What this rule does NOT do
--------------------------

* It does not consider distinct-count statistics from parquet
  column stats (daft does — see ``join_graph.rs::JoinAdjList::
  add_bidirectional_edge``). The ``min(left, right)`` fallback
  daft uses without column stats is what we use throughout. This
  is the V2 simplification; revisit when adding pq.ColumnChunk
  stats reading.
* It does not flow filters across joins. The selectivity heuristic
  (0.1 for "looks like a range predicate") is applied at the leaf
  it directly wraps; if a date filter sits above an aggregate
  above a join, it will not propagate down. Out of scope for V2.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pyarrow.parquet as pq

from substrait.proto import algebra, plan as plan_pb2

from ...logging import boltmlDebugLog
from ._expression import is_direct_field_ref, direct_field_index
from .base import Rule, get_rel_kind


# Selectivity fallback used when a leaf's filter expression looks like a
# range comparison against a literal. Mirrors V1's value (0.1). Daft
# uses a much richer model based on accumulated stats across the plan;
# port that in a follow-up. The 0.1 is conservative enough to not
# promote a junk leaf to "smallest", but small enough to differentiate
# an unfiltered fact table from a filtered dim.
_FILTER_RANGE_SELECTIVITY: float = 0.1

# Cap on the number of leaves we'll enumerate. Brute force is O(3^n) in
# subset partitions (each subset has 3 states: in left, in right, in
# neither); 10 leaves is ~59049 partitions — runs in milliseconds.
# TPC-H queries have at most ~6 leaves (q5, q9). Larger sub-graphs are
# left as-is — we'd need a DP-on-connected-subsets algorithm to scale.
_MAX_LEAVES: int = 10


# ---------------------------------------------------------------------------
# Cardinality estimation
# ---------------------------------------------------------------------------


def _read_parquet_row_count(uri: str) -> Optional[int]:
    """Return the row count in a parquet file referenced by *uri*.

    Accepts ``file://`` URIs and bare paths. Returns ``None`` on any
    failure — the caller treats unknown cardinalities as "this rule
    must not touch this sub-tree".
    """
    if uri.startswith("file://"):
        path = urlparse(uri).path
    else:
        path = uri
    try:
        return int(pq.read_metadata(path).num_rows)
    except Exception as exc:  # noqa: BLE001
        boltmlDebugLog(
            "join_reorder",
            f"parquet metadata read failed for {path!r}: {type(exc).__name__}: {exc}",
        )
        return None


def _read_rel_row_count(read: algebra.ReadRel) -> Optional[int]:
    """Estimate the row count of a ``ReadRel``.

    Handles ``local_files`` reads by summing parquet-footer row
    counts across files. Returns ``None`` for ``extension_table``,
    ``virtual_table``, or ``named_table`` reads — those carry their
    schema C++-side and we cannot stat them from Python.
    """
    if read.HasField("local_files"):
        total = 0
        for f in read.local_files.items:
            uri = f.uri_file or f.uri_path or f.uri_folder
            if not uri:
                return None
            n = _read_parquet_row_count(uri)
            if n is None:
                return None
            total += n
        return total
    return None


def _expr_has_range_predicate(expr: algebra.Expression) -> bool:
    """Return True iff *expr* structurally looks like a range comparison.

    Match shape: a ``scalar_function`` with two value arguments where
    one is a literal and the other is a field reference. Recurses into
    nested scalar functions (e.g. ``and(lt(...), gt(...))``). This is
    the V1 heuristic — coarse but effective for TPC-H date-range
    filters.
    """
    which = expr.WhichOneof("rex_type")
    if which is None:
        return False
    if which == "scalar_function":
        value_args = [
            arg.value
            for arg in expr.scalar_function.arguments
            if arg.WhichOneof("arg_type") == "value"
        ]
        if len(value_args) == 2:
            kinds = {a.WhichOneof("rex_type") for a in value_args}
            if "literal" in kinds and "selection" in kinds:
                return True
        for v in value_args:
            if _expr_has_range_predicate(v):
                return True
        return False
    if which == "if_then":
        for clause in expr.if_then.ifs:
            if clause.HasField("if") and _expr_has_range_predicate(
                getattr(clause, "if")
            ):
                return True
            if clause.HasField("then") and _expr_has_range_predicate(clause.then):
                return True
        if expr.if_then.HasField("else"):
            if _expr_has_range_predicate(getattr(expr.if_then, "else")):
                return True
        return False
    if which == "cast":
        return _expr_has_range_predicate(expr.cast.input)
    if which == "singular_or_list":
        if _expr_has_range_predicate(expr.singular_or_list.value):
            return True
        for opt in expr.singular_or_list.options:
            if _expr_has_range_predicate(opt):
                return True
        return False
    return False


def _estimate_leaf_cardinality(rel: algebra.Rel) -> Optional[float]:
    """Return an estimated row count for a *leaf* sub-tree, or None.

    A leaf is a sub-tree of (potentially) ``Project``/``Filter`` above
    a ``Read``. Walking rules:

    * ``read``     -> parquet footer; ``None`` for non-file reads.
    * ``filter``   -> child * (0.1 if range predicate else 1.0).
    * ``project``  -> child (projects don't change row count).
    * ``sort``/``fetch``/``exchange``/``extension_single`` -> child.

    Anything else returns ``None`` and the caller skips the rewrite
    over this leaf.
    """
    kind = get_rel_kind(rel)
    if kind is None:
        return None
    if kind == "read":
        n = _read_rel_row_count(rel.read)
        return float(n) if n is not None else None
    if kind == "filter":
        child = _estimate_leaf_cardinality(rel.filter.input)
        if child is None:
            return None
        sel = (
            _FILTER_RANGE_SELECTIVITY
            if _expr_has_range_predicate(rel.filter.condition)
            else 1.0
        )
        return max(1.0, child * sel)
    if kind == "project":
        return _estimate_leaf_cardinality(rel.project.input)
    if kind == "sort":
        return _estimate_leaf_cardinality(rel.sort.input)
    if kind == "fetch":
        return _estimate_leaf_cardinality(rel.fetch.input)
    if kind == "exchange":
        return _estimate_leaf_cardinality(rel.exchange.input)
    if kind == "extension_single":
        return _estimate_leaf_cardinality(rel.extension_single.input)
    if kind == "aggregate":
        # Aggregations collapse rows; pessimistic bound = a small constant.
        child = _estimate_leaf_cardinality(rel.aggregate.input)
        if child is None:
            return None
        return min(child, 10000.0)
    return None


# ---------------------------------------------------------------------------
# Output-name + Project-walking helpers
# ---------------------------------------------------------------------------


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


def _is_pure_passthrough_project(rel: algebra.Rel) -> bool:
    """Return True iff *rel* is a Project whose every expression is a direct field ref.

    These are the "rename + reorder" Projects Bolt's join builder
    emits. Any computed expression (cast, scalar_function, ...)
    disqualifies the Project from being walked through.
    """
    if get_rel_kind(rel) != "project":
        return False
    for e in rel.project.expressions:
        if not is_direct_field_ref(e):
            return False
    return True


def _peel_one_prefix_project(
    rel: algebra.Rel,
) -> Tuple[algebra.Rel, Tuple[str, ...]]:
    """Peel exactly one outer ``l_``/``r_`` prefix Project off *rel*.

    Bolt's ``DataFrame.join`` always wraps each side in one outer
    passthrough Project whose every output name carries a uniform
    ``l_`` or ``r_`` prefix. This helper:

    * If *rel* IS that Project — peel it and return
      ``(rel.project.input, names_with_prefix_stripped)`` where the
      stripped names are taken from the prefix Project's
      ``hint.output_names`` AND already permuted via the prefix
      Project's expression list to match the leaf's column order.
    * Otherwise return ``(rel, rel.hint_names)``.

    The names returned are the leaf candidate's column names (one per
    column in the leaf's natural order). They are reliable even when
    the leaf is, say, a Filter without its own hint output_names
    (Bolt doesn't always include an inner reorder Project — when all
    columns are already in the right order, Bolt skips the reorder
    Project and the prefix Project's input is the bare relation).
    """
    if not _is_pure_passthrough_project(rel):
        return rel, _hint_output_names(rel)
    outer_names = _hint_output_names(rel)
    if not outer_names or not all(
        nm.startswith("l_") or nm.startswith("r_") for nm in outer_names
    ):
        # Not the prefix Project; treat *rel* as a leaf candidate.
        return rel, outer_names
    leaf = rel.project.input
    # Strip the prefix from each output name, then permute to match
    # the leaf's column order. The prefix Project's expression at
    # output position p is a direct field ref to leaf column q; so
    # leaf column q is named (after stripping prefix from outer_names[p]).
    leaf_own = _hint_output_names(leaf)
    if leaf_own:
        # Leaf has its own hint. Trust it (it might already include a
        # rename, which we want to honor).
        return leaf, leaf_own
    # Synthesize from the prefix Project. Determine leaf width.
    # If we can't determine leaf width, fall back to using outer_names
    # in expression order (might be misaligned, but it's the best we have).
    leaf_width = _leaf_width(leaf)
    if leaf_width <= 0:
        # Use outer ordering directly (assume identity mapping).
        return leaf, tuple(_strip_uniform_prefix(nm) for nm in outer_names)
    # Build leaf-position -> name table.
    out_names = ["" for _ in range(leaf_width)]
    for p, nm in enumerate(outer_names):
        if not (0 <= p < len(rel.project.expressions)):
            continue
        leaf_idx = direct_field_index(rel.project.expressions[p])
        if leaf_idx is None or not (0 <= leaf_idx < leaf_width):
            continue
        if not out_names[leaf_idx]:
            out_names[leaf_idx] = _strip_uniform_prefix(nm)
    return leaf, tuple(out_names)


def _leaf_width(rel: algebra.Rel) -> int:
    """Return the output column count of a non-Project leaf rel.

    Best-effort. Used by ``_peel_one_prefix_project`` to size the
    leaf-name array when the leaf doesn't carry its own hint.
    """
    kind = get_rel_kind(rel)
    if kind is None:
        return 0
    if kind == "read":
        n = len(rel.read.base_schema.names)
        if not n and rel.read.HasField("common") and rel.read.common.HasField("hint"):
            n = len(rel.read.common.hint.output_names)
        if not n:
            n = len(rel.read.base_schema.struct.types)
        return n
    if kind == "filter":
        return _leaf_width(rel.filter.input)
    if kind == "fetch":
        return _leaf_width(rel.fetch.input)
    if kind == "sort":
        return _leaf_width(rel.sort.input)
    if kind == "project":
        return len(rel.project.expressions)
    if kind == "exchange":
        return _leaf_width(rel.exchange.input)
    if kind == "extension_single":
        return _leaf_width(rel.extension_single.input)
    return 0


def _find_nested_hash_join(rel: algebra.Rel) -> Optional[algebra.Rel]:
    """Walk through any number of pure-passthrough Projects looking for a hash_join.

    Returns the first hash_join encountered, or ``None`` if a non-Project
    rel that isn't a hash_join is hit first. Used after
    ``_peel_one_prefix_project`` to detect the "nested join" case where
    Bolt's reorder Project sits between the prefix Project and a deeper
    hash_join.
    """
    cur = rel
    while True:
        kind = get_rel_kind(cur)
        if kind == "hash_join":
            return cur
        if kind != "project":
            return None
        if not _is_pure_passthrough_project(cur):
            return None
        cur = cur.project.input


def _strip_uniform_prefix(name: str) -> str:
    """Strip a leading ``l_`` or ``r_`` prefix if present, else return unchanged."""
    if name.startswith("l_") or name.startswith("r_"):
        return name[2:]
    return name


# ---------------------------------------------------------------------------
# Join graph
# ---------------------------------------------------------------------------


@dataclass
class _Leaf:
    """A relation that participates in the join sub-graph being rewritten.

    ``rel``: the substrait Rel sub-tree (its hint output_names give the
    column ordering this leaf produces).
    ``names``: the output column names of ``rel`` (immutable copy of
    the hint's ``output_names``, with no ``l_``/``r_`` prefix).
    ``cardinality``: estimated row count.
    """

    rel: algebra.Rel
    names: Tuple[str, ...]
    cardinality: float

    def __hash__(self) -> int:
        return id(self.rel)


@dataclass
class _Edge:
    """An equi-join edge between two leaves, on named columns.

    The sides are unordered; the edge is symmetric.
    """

    left_leaf_id: int
    left_col: str
    right_leaf_id: int
    right_col: str


class _Graph:
    """Join graph: leaves + equi-join edges, with key-equivalence inference.

    Equivalence sets: if edges A.x=B.x and B.x=C.x exist, then
    A.x=C.x is inferred (they share a single equivalence-set id).
    The total domain of an equivalence set is the cardinality of
    the smallest member leaf — daft's "no-stats" fallback.
    """

    def __init__(self) -> None:
        self.leaves: List[_Leaf] = []
        self.edges: List[_Edge] = []
        # (leaf_id, col_name) -> equivalence_set_id
        self._eq_set: Dict[Tuple[int, str], int] = {}
        # equivalence_set_id -> domain (min cardinality of members)
        self._eq_domain: Dict[int, float] = {}
        self._next_eq_id: int = 0

    def add_leaf(self, leaf: _Leaf) -> int:
        leaf_id = len(self.leaves)
        self.leaves.append(leaf)
        return leaf_id

    def add_edge(self, edge: _Edge) -> None:
        # Merge / extend equivalence sets.
        l_key = (edge.left_leaf_id, edge.left_col)
        r_key = (edge.right_leaf_id, edge.right_col)
        l_set = self._eq_set.get(l_key)
        r_set = self._eq_set.get(r_key)
        l_card = self.leaves[edge.left_leaf_id].cardinality
        r_card = self.leaves[edge.right_leaf_id].cardinality
        new_domain = min(l_card, r_card)
        if l_set is not None and r_set is not None:
            if l_set != r_set:
                # Merge the two sets into the lower id.
                merged = min(l_set, r_set)
                old = max(l_set, r_set)
                for k, v in list(self._eq_set.items()):
                    if v == old:
                        self._eq_set[k] = merged
                merged_domain = min(self._eq_domain[merged], self._eq_domain[old])
                self._eq_domain[merged] = min(merged_domain, new_domain)
                del self._eq_domain[old]
            else:
                self._eq_domain[l_set] = min(self._eq_domain[l_set], new_domain)
        elif l_set is not None:
            self._eq_set[r_key] = l_set
            self._eq_domain[l_set] = min(self._eq_domain[l_set], new_domain)
        elif r_set is not None:
            self._eq_set[l_key] = r_set
            self._eq_domain[r_set] = min(self._eq_domain[r_set], new_domain)
        else:
            eq_id = self._next_eq_id
            self._next_eq_id += 1
            self._eq_set[l_key] = eq_id
            self._eq_set[r_key] = eq_id
            self._eq_domain[eq_id] = new_domain
        self.edges.append(edge)

    def shared_edges(
        self, left_set: int, right_set: int
    ) -> List[Tuple[int, str, int, str]]:
        """Return edges crossing the partition (left_set, right_set).

        Each entry is ``(left_leaf_id, left_col, right_leaf_id, right_col)``
        with the leaf ids ordered consistent with the partition (left
        leaf in left_set, right leaf in right_set). At most one edge
        per equivalence set is returned (a "minimum spanning" subset),
        matching daft's ``get_connections``.
        """
        seen_eq_ids: set[int] = set()
        crossing: List[Tuple[int, str, int, str]] = []
        for e in self.edges:
            l_in_left = (1 << e.left_leaf_id) & left_set
            r_in_right = (1 << e.right_leaf_id) & right_set
            l_in_right = (1 << e.left_leaf_id) & right_set
            r_in_left = (1 << e.right_leaf_id) & left_set
            if l_in_left and r_in_right:
                eq_id = self._eq_set[(e.left_leaf_id, e.left_col)]
                if eq_id in seen_eq_ids:
                    continue
                seen_eq_ids.add(eq_id)
                crossing.append(
                    (e.left_leaf_id, e.left_col, e.right_leaf_id, e.right_col)
                )
            elif l_in_right and r_in_left:
                eq_id = self._eq_set[(e.left_leaf_id, e.left_col)]
                if eq_id in seen_eq_ids:
                    continue
                seen_eq_ids.add(eq_id)
                crossing.append(
                    (e.right_leaf_id, e.right_col, e.left_leaf_id, e.left_col)
                )
        return crossing

    def shared_domain(self, left_set: int, right_set: int) -> float:
        """Return the product of equivalence-set domain sizes shared between
        the two halves of the partition. If no edge crosses, returns 1.0
        (the caller should also have checked ``shared_edges`` is non-empty
        to avoid building a cross product).
        """
        seen_eq_ids: set[int] = set()
        td = 1.0
        for e in self.edges:
            l_in_left = (1 << e.left_leaf_id) & left_set
            r_in_right = (1 << e.right_leaf_id) & right_set
            l_in_right = (1 << e.left_leaf_id) & right_set
            r_in_left = (1 << e.right_leaf_id) & left_set
            if (l_in_left and r_in_right) or (l_in_right and r_in_left):
                eq_id = self._eq_set[(e.left_leaf_id, e.left_col)]
                if eq_id in seen_eq_ids:
                    continue
                seen_eq_ids.add(eq_id)
                td *= self._eq_domain[eq_id]
        return max(td, 1.0)


# ---------------------------------------------------------------------------
# Sub-graph extraction
# ---------------------------------------------------------------------------


def _extract_subgraph(
    root: algebra.Rel, graph: _Graph
) -> Optional[List[Tuple[Tuple[int, str], Tuple[int, str]]]]:
    """Walk *root* (a HashJoinRel) and add every leaf and join edge to *graph*.

    Returns a list of pending edges as ``((left_leaf_id, left_unprefixed_col),
    (right_leaf_id, right_unprefixed_col))`` tuples — the caller adds them
    to the graph after all leaves are registered. Returns ``None`` if any
    sub-tree disqualifies the rewrite (non-INNER join, post_join_filter,
    cross product, or unestimable cardinality).

    The walker assumes Bolt's join lowering convention: each side of a
    HashJoinRel is wrapped in (up to) two pure-passthrough Projects
    that prefix every column with ``l_`` or ``r_``. The leaf is what
    sits below that chain.
    """
    pending_edges: List[Tuple[Tuple[int, str], Tuple[int, str]]] = []
    # leaves we have already seen by `id(rel)` — to canonicalize a
    # leaf shared by multiple joins (rare but possible).
    leaf_ids_by_rel: Dict[int, int] = {}

    def _process_join(jr: algebra.HashJoinRel) -> Optional[Tuple[List[int], List[int]]]:
        """Process a HashJoinRel. Return per-side leaf id lists if OK, else None.

        Each side may itself be another HashJoinRel sub-graph; in
        that case we recurse. Otherwise the side is a leaf relation.
        """
        # Disqualifiers.
        if jr.type != algebra.HashJoinRel.JoinType.JOIN_TYPE_INNER:
            return None
        if jr.HasField("post_join_filter"):
            return None
        if not jr.keys:
            # Cross product — we don't reorder cross products; daft skips them too.
            return None
        # Each key must have a direct field ref on each side.
        for k in jr.keys:
            l_idx = _key_field_index(k.left)
            r_idx = _key_field_index(k.right)
            if l_idx is None or r_idx is None:
                return None

        # Walk through each side's prefix-Project chain to find the leaf
        # (or the next nested HashJoinRel). Returns either a recursion
        # result (per-side leaf ids) or a single new leaf id.
        def _process_side(side_rel: algebra.Rel) -> Optional[List[int]]:
            leaf_rel, leaf_names = _peel_one_prefix_project(side_rel)
            # Check if leaf_rel actually wraps a nested hash_join below
            # zero or more passthrough Projects. If so, recurse on it.
            nested = _find_nested_hash_join(leaf_rel)
            if nested is not None:
                return _process_join(nested.hash_join)
            # It's a leaf relation. Estimate its cardinality.
            if not leaf_names:
                # No usable hint — bail.
                return None
            card = _estimate_leaf_cardinality(leaf_rel)
            if card is None:
                return None
            # Canonicalize by Rel identity.
            existing = leaf_ids_by_rel.get(id(leaf_rel))
            if existing is not None:
                return [existing]
            leaf = _Leaf(rel=leaf_rel, names=tuple(leaf_names), cardinality=card)
            leaf_id = graph.add_leaf(leaf)
            leaf_ids_by_rel[id(leaf_rel)] = leaf_id
            return [leaf_id]

        left_leaves = _process_side(jr.left)
        if left_leaves is None:
            return None
        right_leaves = _process_side(jr.right)
        if right_leaves is None:
            return None
        left_leaf_set = set(left_leaves)
        right_leaf_set = set(right_leaves)

        # Map each key (left_idx, right_idx) into a (leaf_id, col_name)
        # pair on each side. The key indices are positions in the
        # immediately-above prefix-Project's output. To map them to a
        # leaf column name we walk through the same chain we just
        # peeled off, tracking the index translation. The
        # ``allowed_leaves`` set scopes the leaf lookup so a column
        # name that exists on multiple leaves (e.g. the join-key
        # column ``n_nationkey`` exists on every relation that joins
        # against nation) gets resolved to the leaf actually under
        # this side of the join.
        def _key_to_leaf_column(
            side_rel: algebra.Rel, idx: int, allowed_leaves: set
        ) -> Optional[Tuple[int, str]]:
            """Translate a key field index into ``(leaf_id, leaf_col_name)``.

            Match shape: peel exactly one outer prefix Project (if
            present), translating the index through it. The result
            lands on either:

            * A non-Project rel (= a directly-registered leaf) — match
              index against that leaf's hint.
            * A passthrough Project chain leading to a nested hash_join
              — match the index against the same Project's hint, get
              the column name (stripping any internal prefix), then
              find the leaf in ``allowed_leaves`` that exposes that
              column name.
            * A passthrough Project chain leading to a direct leaf —
              follow the chain through the indices, then match against
              the deepest non-Project rel.
            """
            # Use _peel_one_prefix_project to get the leaf candidate
            # AND its (possibly synthesized) names. Then translate the
            # join-key index through the prefix Project's expression
            # list to get the leaf's column index.
            cur = side_rel
            cur_idx = idx
            cur_hint = _hint_output_names(cur)
            if (
                _is_pure_passthrough_project(cur)
                and cur_hint
                and all(nm.startswith("l_") or nm.startswith("r_") for nm in cur_hint)
            ):
                exprs = list(cur.project.expressions)
                if not (0 <= cur_idx < len(exprs)):
                    return None
                inner_idx = direct_field_index(exprs[cur_idx])
                if inner_idx is None:
                    return None
                cur = cur.project.input
                cur_idx = inner_idx

            # Now `cur` is the leaf candidate (after one prefix peel).
            # Look up the leaf's name list — prefer the already-registered
            # leaf's `_Leaf.names` (which may have been synthesized when
            # the leaf doesn't carry its own hint, e.g. FilterRel).
            lid_for_leaf = leaf_ids_by_rel.get(id(cur))
            if lid_for_leaf is not None:
                leaf_names = graph.leaves[lid_for_leaf].names
            else:
                leaf_names = _hint_output_names(cur)

            # Branch: nested hash_join below or direct leaf.
            nested = _find_nested_hash_join(cur)
            if nested is not None:
                # The leaf for this side lives inside the nested
                # sub-graph. The target column name comes from `cur`'s
                # hint at index `cur_idx` (which is the same index
                # the deeper join's output_names lists).
                if not leaf_names or not (0 <= cur_idx < len(leaf_names)):
                    return None
                target_col = leaf_names[cur_idx]
                clean_col = _strip_uniform_prefix(target_col)
                for lid in allowed_leaves:
                    lf = graph.leaves[lid]
                    if clean_col in lf.names:
                        return (lid, clean_col)
                return None

            # Direct leaf case. Use the leaf's registered name list.
            if not (0 <= cur_idx < len(leaf_names)):
                return None
            target_col = leaf_names[cur_idx]
            if not target_col:
                return None
            if lid_for_leaf is None or lid_for_leaf not in allowed_leaves:
                return None
            return (lid_for_leaf, target_col)

        for k in jr.keys:
            l_idx = _key_field_index(k.left)
            r_idx = _key_field_index(k.right)
            assert l_idx is not None and r_idx is not None  # checked above
            l_pair = _key_to_leaf_column(jr.left, l_idx, left_leaf_set)
            r_pair = _key_to_leaf_column(jr.right, r_idx, right_leaf_set)
            if l_pair is None or r_pair is None:
                return None
            pending_edges.append((l_pair, r_pair))

        return left_leaves + right_leaves

    if get_rel_kind(root) != "hash_join":
        return None
    leaves = _process_join(root.hash_join)
    if leaves is None:
        return None
    return pending_edges


def _key_field_index(ref: algebra.Expression.FieldReference) -> Optional[int]:
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
# Brute-force enumeration
# ---------------------------------------------------------------------------


@dataclass
class _OrderTree:
    """A join order: either a single leaf id, or a join of two sub-trees."""

    is_leaf: bool
    leaf_id: int = -1
    left: Optional["_OrderTree"] = None
    right: Optional["_OrderTree"] = None
    # Output column names produced by this sub-tree (in their layout
    # order). For a leaf, this is the leaf's `_Leaf.names`. For a
    # join, it's the concatenation of left + right output names with
    # the right side's join-key columns dropped.
    output_names: Tuple[str, ...] = ()


def _brute_force(graph: _Graph) -> Optional[_OrderTree]:
    """Enumerate all join orderings of ``graph.leaves`` and return the cheapest.

    Mirrors daft's ``BruteForceJoinOrderer``. Memoizes by available-set
    bitmask over the leaves. At every join, the smaller side goes LEFT
    (Bolt's HashJoin builds on right). Cross products are skipped.
    Returns ``None`` if the graph is disconnected (no valid order).
    """
    n = len(graph.leaves)
    if n == 0:
        return None
    if n > _MAX_LEAVES:
        return None

    # memo[available_set] -> (cost, cardinality, _OrderTree) or None if unreachable
    memo: Dict[int, Optional[Tuple[float, float, _OrderTree]]] = {}

    def _solve(available: int) -> Optional[Tuple[float, float, _OrderTree]]:
        if available in memo:
            return memo[available]
        # Single-leaf base case.
        if bin(available).count("1") == 1:
            leaf_id = (available & -available).bit_length() - 1
            leaf = graph.leaves[leaf_id]
            tree = _OrderTree(
                is_leaf=True,
                leaf_id=leaf_id,
                output_names=leaf.names,
            )
            res = (0.0, leaf.cardinality, tree)
            memo[available] = res
            return res

        # Enumerate all non-empty proper subsets of `available`.
        best: Optional[Tuple[float, float, _OrderTree]] = None
        # Subset enumeration: the "Gosper" subset trick. Start at the
        # lowest non-empty subset and walk through them.
        sub = (available - 1) & available
        while sub > 0:
            comp = available ^ sub
            # Avoid evaluating both (A,B) and (B,A) — only handle each
            # unordered partition once.
            if sub < comp:
                # Connectivity check: at least one edge crosses (sub, comp).
                edges = graph.shared_edges(sub, comp)
                if edges:
                    left_res = _solve(sub)
                    right_res = _solve(comp)
                    if left_res is not None and right_res is not None:
                        l_cost, l_card, l_tree = left_res
                        r_cost, r_card, r_tree = right_res
                        # Force smaller-on-RIGHT. Bolt's HashJoin builds
                        # the hash table on the RIGHT input
                        # (HashBuild.cpp: ``joinNode_->sources()[1]``,
                        # ``rightKeys()``). The build side dictates the
                        # hash-table memory footprint; smaller build =
                        # smaller table = avoids OOM at SF=10. The
                        # probe (LEFT) is streamed and adds no memory.
                        if l_card < r_card:
                            l_tree, r_tree = r_tree, l_tree
                            l_card, r_card = r_card, l_card
                        domain = graph.shared_domain(sub, comp)
                        # Cap the divisor at min(l_card, r_card). For multi-key
                        # joins on PK-FK pairs, ``shared_domain`` multiplies
                        # per-equivalence-set domains and can vastly exceed the
                        # true distinct-tuple count. E.g. for q9's
                        # lineitem(60M) ⨝ partsupp(8M) on (partkey, suppkey),
                        # the per-key product is 200k × 100k = 2e10 but the true
                        # distinct (partkey,suppkey) count is just 8M. Capping
                        # at min-side gives PK-FK semantics: out ≥ max-side.
                        domain = min(domain, min(l_card, r_card))
                        out_card = max(1.0, (l_card * r_card) / max(domain, 1.0))
                        cost = l_cost + r_cost + out_card
                        # Build combined output_names: left ++ right with
                        # join-key columns of the right side removed.
                        join_keys_right = {
                            right_col for (_l_id, _l_col, _r_id, right_col) in edges
                        }
                        combined: List[str] = []
                        combined.extend(l_tree.output_names)
                        for nm in r_tree.output_names:
                            if nm not in join_keys_right:
                                combined.append(nm)
                        tree = _OrderTree(
                            is_leaf=False,
                            left=l_tree,
                            right=r_tree,
                            output_names=tuple(combined),
                        )
                        if best is None or cost < best[0]:
                            best = (cost, out_card, tree)
            sub = (sub - 1) & available
        memo[available] = best
        return best

    full = (1 << n) - 1
    res = _solve(full)
    if res is None:
        return None
    return res[2]


# ---------------------------------------------------------------------------
# Plan re-builder
# ---------------------------------------------------------------------------


def _make_field_ref(idx: int) -> algebra.Expression:
    """Build a fresh top-level direct field-reference Expression."""
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


def _build_prefix_project(
    side_rel: algebra.Rel,
    side_names: Tuple[str, ...],
    join_keys: List[str],
    prefix: str,
) -> Tuple[algebra.Rel, Tuple[str, ...], Tuple[str, ...]]:
    """Wrap *side_rel* in two Projects matching Bolt's join-side convention.

    Inner Project: reorders columns so the join keys come first, with
    *unprefixed* names in the hint. Bolt's executor needs the join-key
    fields at the head of the side's output.
    Outer Project: passes columns through unchanged, but renames every
    output to ``<prefix><name>``.

    *side_names* is the canonical (unprefixed) name of each input
    column at *side_rel*'s actual rel-output positions; *join_keys*
    is the subset to put first.

    Returns ``(wrapped_rel, prefixed_names, canonical_names)`` where
    ``prefixed_names`` is the outer rel's hint output_names (with
    prefix) and ``canonical_names`` is the same length list of
    unprefixed names — these track each column's canonical identity
    for parent-join key matching.
    """
    name_to_idx = {nm: i for i, nm in enumerate(side_names)}
    keys_in_order = [k for k in join_keys if k in name_to_idx]
    nonkeys_in_order = [nm for nm in side_names if nm not in set(keys_in_order)]
    inner_order = keys_in_order + nonkeys_in_order

    inner_exprs = [_make_field_ref(name_to_idx[nm]) for nm in inner_order]
    inner_proj = algebra.Rel(
        project=algebra.ProjectRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=list(inner_order)),
            ),
            input=side_rel,
            expressions=inner_exprs,
        )
    )

    outer_names = [f"{prefix}{nm}" for nm in inner_order]
    outer_exprs = [_make_field_ref(i) for i in range(len(inner_order))]
    outer_proj = algebra.Rel(
        project=algebra.ProjectRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=outer_names),
            ),
            input=inner_proj,
            expressions=outer_exprs,
        )
    )

    return outer_proj, tuple(outer_names), tuple(inner_order)


def _build_subtree(
    tree: _OrderTree, graph: _Graph
) -> Tuple[algebra.Rel, Tuple[str, ...]]:
    """Recursively materialize *tree* as a Bolt-flavored HashJoinRel sub-tree.

    Returns ``(rel, output_canonical_names)``. The ``rel`` is a fresh
    sub-tree built either from a leaf (returned verbatim) or by joining
    two sub-trees with fresh prefix-Project wrappers. The
    ``output_canonical_names`` is a tuple giving each output column's
    *unprefixed* canonical name in the rel's actual layout order:

    * For a leaf, this is ``leaf.names`` (already unprefixed).
    * For a join, this is the concatenation
      ``[left_canonical_names ++ right_canonical_names_minus_join_keys]``,
      matching the layout of Bolt's HashJoinRel output (which physically
      emits ``[l_keys, l_nonkeys, r_nonkeys]`` after prefixing — but
      we expose canonical names so a parent join can match them
      against join-graph edges without prefix juggling).

    The actual column at output index ``i`` of the rel will be named
    with an ``l_``/``r_`` prefix in the rel's hint, but the
    ``output_canonical_names[i]`` strips those prefixes uniformly so
    the parent's edge-matching code can use canonical names.
    """
    if tree.is_leaf:
        leaf = graph.leaves[tree.leaf_id]
        out = algebra.Rel()
        out.CopyFrom(leaf.rel)
        return out, leaf.names

    # Recurse on children first.
    l_rel, l_names = _build_subtree(tree.left, graph)
    r_rel, r_names = _build_subtree(tree.right, graph)

    # Find the crossing edges in the join graph.
    left_set = _collect_leaves(tree.left)
    right_set = _collect_leaves(tree.right)

    seen_eq_ids: set[int] = set()
    crossing_pairs: List[Tuple[str, str]] = []
    # For matching, work with the canonical (unprefixed) names of each
    # sub-tree's output. A column name on the LEFT side may not literally
    # match the LEFT edge endpoint's column name if a deeper join's
    # equivalence-set inference connected a renamed pair — fall back to
    # equivalence-set membership when direct name match fails.
    l_name_set = set(l_names)
    r_name_set = set(r_names)
    for e in graph.edges:
        l_in_left = (1 << e.left_leaf_id) & left_set
        r_in_right = (1 << e.right_leaf_id) & right_set
        l_in_right = (1 << e.left_leaf_id) & right_set
        r_in_left = (1 << e.right_leaf_id) & left_set
        if l_in_left and r_in_right:
            eq_id = graph._eq_set[(e.left_leaf_id, e.left_col)]
            if eq_id in seen_eq_ids:
                continue
            l_col_in_subtree = (
                e.left_col
                if e.left_col in l_name_set
                else _find_in_eq_set(graph, eq_id, left_set, l_name_set)
            )
            r_col_in_subtree = (
                e.right_col
                if e.right_col in r_name_set
                else _find_in_eq_set(graph, eq_id, right_set, r_name_set)
            )
            if l_col_in_subtree is not None and r_col_in_subtree is not None:
                crossing_pairs.append((l_col_in_subtree, r_col_in_subtree))
                seen_eq_ids.add(eq_id)
        elif l_in_right and r_in_left:
            eq_id = graph._eq_set[(e.left_leaf_id, e.left_col)]
            if eq_id in seen_eq_ids:
                continue
            l_col_in_subtree = (
                e.right_col
                if e.right_col in l_name_set
                else _find_in_eq_set(graph, eq_id, left_set, l_name_set)
            )
            r_col_in_subtree = (
                e.left_col
                if e.left_col in r_name_set
                else _find_in_eq_set(graph, eq_id, right_set, r_name_set)
            )
            if l_col_in_subtree is not None and r_col_in_subtree is not None:
                crossing_pairs.append((l_col_in_subtree, r_col_in_subtree))
                seen_eq_ids.add(eq_id)

    if not crossing_pairs:
        raise RuntimeError(
            "JoinReorder: brute-force produced a partition with no shared "
            "join key; this is a bug in _brute_force"
        )

    # Build prefix-Project wrappers for each side.
    left_join_keys = [lcol for (lcol, _rcol) in crossing_pairs]
    right_join_keys = [rcol for (_lcol, rcol) in crossing_pairs]
    l_wrapped, l_pref_names, l_canon = _build_prefix_project(
        l_rel, l_names, left_join_keys, "l_"
    )
    r_wrapped, r_pref_names, r_canon = _build_prefix_project(
        r_rel, r_names, right_join_keys, "r_"
    )

    # Construct the join key-pair list. After the prefix Project, each
    # side has its join keys at positions [0..len(keys)). So both
    # the left and right key indices are simply ``i`` for the i-th key.
    keys = []
    for i in range(len(crossing_pairs)):
        kpair = algebra.ComparisonJoinKey(
            left=algebra.Expression.FieldReference(
                direct_reference=algebra.Expression.ReferenceSegment(
                    struct_field=algebra.Expression.ReferenceSegment.StructField(
                        field=i
                    ),
                ),
                root_reference=algebra.Expression.FieldReference.RootReference(),
            ),
            right=algebra.Expression.FieldReference(
                direct_reference=algebra.Expression.ReferenceSegment(
                    struct_field=algebra.Expression.ReferenceSegment.StructField(
                        field=i
                    ),
                ),
                root_reference=algebra.Expression.FieldReference.RootReference(),
            ),
            comparison=algebra.ComparisonJoinKey.ComparisonType(
                simple=algebra.ComparisonJoinKey.SIMPLE_COMPARISON_TYPE_EQ,
            ),
        )
        keys.append(kpair)

    # Build the join's hint output_names = full LHS prefixed names
    # followed by RHS non-key prefixed names.
    hint_names: List[str] = []
    hint_names.extend(l_pref_names)
    for i in range(len(keys), len(r_pref_names)):
        hint_names.append(r_pref_names[i])

    # Compute the parallel canonical (unprefixed) name list. We
    # propagate canonical names through the join: every LHS column
    # keeps its canonical name; every RHS non-key column keeps its
    # canonical name.
    canonical_out: List[str] = []
    canonical_out.extend(l_canon)
    for i in range(len(keys), len(r_canon)):
        canonical_out.append(r_canon[i])

    join_rel = algebra.Rel(
        hash_join=algebra.HashJoinRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(
                    output_names=hint_names,
                    # Stamp the marker on the rebuilt HashJoin itself so
                    # that ``_try_optimize_subtree`` can recognize "this
                    # subgraph was already rewritten by us" even when
                    # other rules (``push_filter_through_projects``,
                    # ``prune_unused_columns``) move things around and
                    # the post_unprefix_project is no longer the
                    # immediate parent. Marker on the project alone is
                    # not enough; ``_walk`` must also reject re-entry
                    # at the join level.
                    alias=_BOLTML_JOIN_REORDER_MARKER,
                ),
            ),
            left=l_wrapped,
            right=r_wrapped,
            type=algebra.HashJoinRel.JoinType.JOIN_TYPE_INNER,
            keys=keys,
        )
    )

    return join_rel, tuple(canonical_out)


def _find_in_eq_set(
    graph: _Graph, eq_id: int, allowed_set: int, allowed_names: set
) -> Optional[str]:
    """Return any column name in ``allowed_names`` that's in equivalence set ``eq_id``.

    Used by ``_build_subtree`` to find a column on a sub-tree's output
    that's known to be join-equivalent with the requested edge endpoint
    even if it's not literally the same name (e.g. an intervening
    rename moved ``c_nationkey`` to be exposed as ``n_nationkey``).
    """
    for (leaf_id, col_name), this_eq_id in graph._eq_set.items():
        if this_eq_id != eq_id:
            continue
        if not ((1 << leaf_id) & allowed_set):
            continue
        if col_name in allowed_names:
            return col_name
    return None


def _collect_leaves(tree: _OrderTree) -> int:
    """Return the bitmask of leaf-ids contained in *tree*."""
    if tree.is_leaf:
        return 1 << tree.leaf_id
    return _collect_leaves(tree.left) | _collect_leaves(tree.right)


def _post_unprefix_project(
    join_rel: algebra.Rel,
    canonical_names: Tuple[str, ...],
    target_hint_names: Tuple[str, ...],
) -> algebra.Rel:
    """Wrap *join_rel* in a Project that re-emits the original schema.

    *join_rel* has its physical column ordering described by
    ``canonical_names`` (unprefixed). The original sub-tree's parent
    referenced columns by ``target_hint_names`` (the original prefixed
    output_names hint of the HashJoinRel root we just replaced).

    Strategy: each ``target_hint_names[i]`` is matched against
    ``canonical_names`` after stripping any ``l_``/``r_`` prefix from
    the target. The project then selects the corresponding column.
    """
    canon_to_idx: Dict[str, int] = {nm: i for i, nm in enumerate(canonical_names)}

    expressions = []
    for tname in target_hint_names:
        # The target name might be prefixed (it came from the original
        # join's hint output_names which Bolt produces with prefixes).
        # Or it might be unprefixed if the sub-tree's parent was already
        # a ProjectRel that strips prefixes.
        if tname in canon_to_idx:
            expressions.append(_make_field_ref(canon_to_idx[tname]))
            continue
        stripped = _strip_uniform_prefix(tname)
        if stripped in canon_to_idx:
            expressions.append(_make_field_ref(canon_to_idx[stripped]))
            continue
        raise RuntimeError(
            f"JoinReorder: rebuilt sub-tree missing target column {tname!r} "
            f"(canonical names sample: {canonical_names[:6]})"
        )

    return algebra.Rel(
        project=algebra.ProjectRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(
                    output_names=list(target_hint_names),
                    # Mark this Project as join_reorder's own output so
                    # subsequent passes skip the immediate ``hash_join``
                    # below it. Without this marker, every pass through
                    # the rule pipeline re-extracts the rebuilt subgraph,
                    # treats canonical leaf names as if they were still
                    # raw TPC-H ``l_<col>`` names, and accumulates one
                    # extra prefix layer per iteration (q17 oscillates
                    # forever between this rule and ``MergeProjects``).
                    alias=_BOLTML_JOIN_REORDER_MARKER,
                ),
            ),
            input=join_rel,
            expressions=expressions,
        )
    )


# Marker stamped on every ``_post_unprefix_project`` so ``_walk`` can
# distinguish join_reorder's own output from a generic Project on top
# of a HashJoin. See ``_post_unprefix_project`` for why.
_BOLTML_JOIN_REORDER_MARKER: str = "boltml_join_reorder_output"


def _is_join_reorder_marker(rel: algebra.Rel) -> bool:
    """Return True iff *rel* is a Project carrying our marker alias."""
    if get_rel_kind(rel) != "project":
        return False
    if not rel.project.HasField("common"):
        return False
    if not rel.project.common.HasField("hint"):
        return False
    return rel.project.common.hint.alias == _BOLTML_JOIN_REORDER_MARKER


# ---------------------------------------------------------------------------
# Top-level walker
# ---------------------------------------------------------------------------


def _try_optimize_subtree(rel: algebra.Rel) -> Optional[algebra.Rel]:
    """If *rel* is a HashJoinRel that we can re-order, return the rewritten Rel.

    Returns ``None`` if any of the following holds:
      * *rel* is not a HashJoinRel.
      * The sub-graph contains a non-INNER join, post_join_filter, or
        cross product anywhere.
      * Any leaf's cardinality cannot be estimated.
      * The sub-graph has fewer than 2 leaves (1 leaf = nothing to
        reorder; daft also requires >= 3 but with 2 leaves a swap is
        also a valid output, so we allow 2 here for completeness).
      * The graph is disconnected (would require a cross product).
      * The optimal order computed equals (structurally) the input
        order — no point in rewriting.
      * More than _MAX_LEAVES leaves.
    """
    if get_rel_kind(rel) != "hash_join":
        return None

    # Idempotence: if the HashJoin already carries our marker alias,
    # this subtree was rebuilt by a previous run of join_reorder. Skip
    # re-extraction to avoid the prefix-accumulation / fixpoint
    # divergence that motivated the marker (see
    # ``_post_unprefix_project`` and ``_walk`` for the fuller story).
    if (
        rel.hash_join.HasField("common")
        and rel.hash_join.common.HasField("hint")
        and rel.hash_join.common.hint.alias == _BOLTML_JOIN_REORDER_MARKER
    ):
        return None

    graph = _Graph()
    pending = _extract_subgraph(rel, graph)
    if pending is None:
        return None

    n = len(graph.leaves)
    if n < 2 or n > _MAX_LEAVES:
        return None

    # Add all extracted edges to the graph (now that all leaves are
    # registered with stable ids).
    for l_pair, r_pair in pending:
        edge = _Edge(
            left_leaf_id=l_pair[0],
            left_col=l_pair[1],
            right_leaf_id=r_pair[0],
            right_col=r_pair[1],
        )
        graph.add_edge(edge)

    if not graph.edges:
        return None

    order = _brute_force(graph)
    if order is None:
        return None

    # Capture the original sub-tree's output schema.
    original_names = _hint_output_names(rel)
    if not original_names:
        return None

    # Build the rewritten sub-tree.
    try:
        new_subtree, new_names = _build_subtree(order, graph)
    except RuntimeError as exc:
        boltmlDebugLog("join_reorder", f"build_subtree failed: {exc}")
        return None

    # Idempotence check: if the rebuilt sub-tree is byte-equal to the
    # input HashJoin, the rule is already at fixpoint on this rel.
    if new_subtree.SerializeToString() == rel.SerializeToString():
        return None

    # Force the rewritten sub-tree's output to match the original
    # schema by stripping prefixes and reordering via a top-level
    # Project.
    try:
        out = _post_unprefix_project(new_subtree, new_names, original_names)
    except RuntimeError as exc:
        boltmlDebugLog("join_reorder", f"post_project failed: {exc}")
        return None

    # Diagnostic: emit the chosen order so we can verify q5 picks the right shape.
    boltmlDebugLog(
        "join_reorder",
        f"rewrote {n}-way join: {_describe_tree(order, graph)}",
    )
    return out


def _describe_tree(t: _OrderTree, graph: _Graph) -> str:
    """Render a join order tree as a string for debugging."""
    if t.is_leaf:
        leaf = graph.leaves[t.leaf_id]
        # Try to find a representative leaf name (e.g. the read URI's
        # filename, or just the first column name).
        kind = get_rel_kind(leaf.rel)
        if kind == "read" and leaf.rel.read.HasField("local_files"):
            for f in leaf.rel.read.local_files.items:
                uri = f.uri_file or f.uri_path or f.uri_folder
                if uri:
                    return f"{uri.rsplit('/', 1)[-1]}({int(leaf.cardinality)})"
        # Walk down to a Read for the URI.
        cur = leaf.rel
        for _ in range(5):
            if get_rel_kind(cur) == "read":
                if cur.read.HasField("local_files"):
                    for f in cur.read.local_files.items:
                        uri = f.uri_file or f.uri_path or f.uri_folder
                        if uri:
                            return f"{uri.rsplit('/', 1)[-1]}({int(leaf.cardinality)})"
                break
            ck = get_rel_kind(cur)
            if ck == "filter":
                cur = cur.filter.input
            elif ck == "project":
                cur = cur.project.input
            else:
                break
        return f"<leaf id={t.leaf_id}>({int(leaf.cardinality)})"
    return f"({_describe_tree(t.left, graph)} ⨝ {_describe_tree(t.right, graph)})"


def _walk(rel: algebra.Rel) -> algebra.Rel:
    """Top-down walk: try to optimize each Rel as a join sub-graph root.

    A rewritten sub-tree replaces the original. We do NOT recurse
    into the rewritten sub-tree to look for more INNER join roots:
    every leaf in the rewritten tree is the same substrait Rel as
    in the original (we preserved them verbatim during rewrite); the
    only new join structure inside is what brute-force just emitted,
    and it's already optimal. Re-running the rule would either find
    the same order again (no-op) or trigger the fixpoint loop to
    iterate uselessly.

    However, we DO recurse into the rewritten sub-tree's *leaves*
    (which are pre-existing rel sub-trees) — they may contain
    nested INNER joins below a non-INNER boundary that our
    extraction stopped at. Concretely we only stop recursion at
    ``read`` rels.
    """
    out = algebra.Rel()
    out.CopyFrom(rel)

    # If this Project is the marker we stamped on a previous run's
    # ``_post_unprefix_project`` output, treat the immediate
    # ``hash_join`` below it as already-optimal: skip
    # ``_try_optimize_subtree`` on it and recurse only into the leaves
    # of the rewritten subtree. Without this guard, every fixpoint pass
    # would re-extract the rebuilt subgraph, mistake the canonical leaf
    # names produced by the previous pass for raw ``l_<col>`` names,
    # and accumulate one extra prefix layer (q17 is the canonical
    # repro: it oscillated against ``MergeProjects`` for the full
    # ``maxIterations`` cap).
    if _is_join_reorder_marker(out):
        new_inp = _recurse_into_leaves_only(out.project.input)
        out.project.input.CopyFrom(new_inp)
        return out

    rewritten = _try_optimize_subtree(out)
    if rewritten is not None:
        # The rewritten root is a Project (the post-unprefix wrapper)
        # whose input is the rebuilt HashJoinRel chain. We do not
        # recurse into the chain (it's already optimal). But there
        # may be leaves below it that contain further reorderable
        # sub-graphs — we recurse only into those leaves.
        return _recurse_into_leaves_only(rewritten)

    return _recurse_children(out)


def _recurse_into_leaves_only(rel: algebra.Rel) -> algebra.Rel:
    """Recurse into the original-leaf sub-trees of a freshly-rewritten root.

    The rewritten root is a Project whose input is a chain of fresh
    HashJoinRels. We walk down to the bottom of each fresh
    HashJoinRel chain, and at every leaf rel we recurse into its
    children via the standard ``_walk``.
    """
    # The rewritten output is always a Project wrapping a chain of
    # HashJoinRels with leaves below. Walk through Project / HashJoin
    # nodes (don't try to re-optimize them — they're already optimal).
    out = algebra.Rel()
    out.CopyFrom(rel)
    kind = get_rel_kind(out)
    if kind == "project":
        # Recurse into the project's input (the join chain).
        new_inp = _recurse_into_leaves_only(out.project.input)
        out.project.input.CopyFrom(new_inp)
        return out
    if kind == "hash_join":
        # Recurse into both sides — each side is either another
        # join (recurse further) or a leaf (call _walk to descend).
        # We don't try to re-optimize this hash_join itself.
        new_left = _recurse_into_leaves_only(out.hash_join.left)
        new_right = _recurse_into_leaves_only(out.hash_join.right)
        out.hash_join.left.CopyFrom(new_left)
        out.hash_join.right.CopyFrom(new_right)
        return out
    # Reached a non-join, non-project rel — this is a leaf of the
    # rewritten sub-tree. From here, normal _walk handles it.
    return _walk(out)


def _recurse_children(rel: algebra.Rel) -> algebra.Rel:
    """Apply ``_walk`` to every child of *rel*, return the rewritten *rel*."""
    out = algebra.Rel()
    out.CopyFrom(rel)
    kind = get_rel_kind(out)
    if kind is None:
        return out
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
    return out


# ---------------------------------------------------------------------------
# Rule
# ---------------------------------------------------------------------------


class JoinReorder(Rule):
    """Reorder joins in a Substrait Plan via brute-force enumeration.

    Pure: the input ``Plan`` is not mutated. Idempotent: a second run
    over the rule's output produces a byte-equal Plan when the first
    run picks the optimal order, so the fixpoint loop in
    ``SubstraitOptimizerPipeline`` converges.
    """

    name: str = "join_reorder"

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
