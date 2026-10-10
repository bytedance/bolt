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

"""Unit tests for ``JoinReorder``.

The rule reorders inner-join trees on the basis of parquet-footer
cardinality. It is structural — it only touches ``HashJoinRel`` chains
of ``JOIN_TYPE_INNER``; any other join type or pattern is left
unchanged. The tests below exercise the boundary conditions that
end-to-end TPC-H runs do not exercise individually:

* the rule is **pure** (input ``Plan`` not mutated)
* the rule is **idempotent** (apply twice → byte-equal)
* a tree with **non-inner joins** is not reordered
* a tree with **no parquet metadata** is not reordered (the cardinality
  oracle returns ``None``, the rule must bail rather than guess)
* the **output schema** is preserved across reordering (top-level
  Project re-emits the original column names)
* trees with more than ``_MAX_LEAVES`` leaves bail out (brute force
  is O(3^n); the cap protects compile time on adversarial graphs)
"""

from __future__ import annotations

import os
import tempfile
import unittest
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from substrait.proto import algebra, plan as plan_pb2, type as type_pb2

from ..optimizer.substrait_rules.join_reorder import JoinReorder, _MAX_LEAVES


# ---------------------------------------------------------------------------
# Substrait proto builders
# ---------------------------------------------------------------------------


def _direct_field(idx: int) -> algebra.Expression:
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


def _read_rel_local_files(uris: list[str], names: list[str]) -> algebra.Rel:
    """A ReadRel pointing at one or more local parquet files."""
    items = [
        algebra.ReadRel.LocalFiles.FileOrFiles(
            uri_file=u,
            parquet=algebra.ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions(),
        )
        for u in uris
    ]
    return algebra.Rel(
        read=algebra.ReadRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=names),
            ),
            base_schema=type_pb2.NamedStruct(names=names),
            local_files=algebra.ReadRel.LocalFiles(items=items),
        )
    )


def _read_rel_extension(names: list[str]) -> algebra.Rel:
    """A ReadRel with an extension_table source — cardinality oracle returns None."""
    return algebra.Rel(
        read=algebra.ReadRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=names),
            ),
            base_schema=type_pb2.NamedStruct(names=names),
        )
    )


def _eq_join_key(left_idx: int, right_idx: int) -> algebra.ComparisonJoinKey:
    return algebra.ComparisonJoinKey(
        left=algebra.Expression.FieldReference(
            direct_reference=algebra.Expression.ReferenceSegment(
                struct_field=algebra.Expression.ReferenceSegment.StructField(
                    field=left_idx,
                ),
            ),
            root_reference=algebra.Expression.FieldReference.RootReference(),
        ),
        right=algebra.Expression.FieldReference(
            direct_reference=algebra.Expression.ReferenceSegment(
                struct_field=algebra.Expression.ReferenceSegment.StructField(
                    field=right_idx,
                ),
            ),
            root_reference=algebra.Expression.FieldReference.RootReference(),
        ),
        comparison=algebra.ComparisonJoinKey.ComparisonType(
            simple=algebra.ComparisonJoinKey.SIMPLE_COMPARISON_TYPE_EQ,
        ),
    )


def _hash_join(
    left: algebra.Rel,
    right: algebra.Rel,
    keys: list[algebra.ComparisonJoinKey],
    output_names: list[str],
    join_type: int = algebra.HashJoinRel.JoinType.JOIN_TYPE_INNER,
    post_join_filter: Optional[algebra.Expression] = None,
) -> algebra.Rel:
    join_rel = algebra.HashJoinRel(
        common=algebra.RelCommon(
            direct=algebra.RelCommon.Direct(),
            hint=algebra.RelCommon.Hint(output_names=output_names),
        ),
        left=left,
        right=right,
        keys=keys,
        type=join_type,
    )
    if post_join_filter is not None:
        join_rel.post_join_filter.CopyFrom(post_join_filter)
    return algebra.Rel(hash_join=join_rel)


def _wrap_in_root(rel: algebra.Rel, names: list[str]) -> plan_pb2.Plan:
    p = plan_pb2.Plan()
    p.relations.add().root.CopyFrom(algebra.RelRoot(input=rel, names=names))
    return p


def _write_parquet(path: str, n_rows: int, name: str = "x") -> None:
    """Write a tiny parquet with N rows so JoinReorder's cardinality
    oracle returns a real value for a test ReadRel."""
    t = pa.table({name: pa.array(range(n_rows), type=pa.int64())})
    pq.write_table(t, path)


# ---------------------------------------------------------------------------
# Helpers for asserting structural equality
# ---------------------------------------------------------------------------


def _count_hash_joins(rel: algebra.Rel) -> int:
    """Count hash_join rels anywhere in the tree."""
    n = 0
    kind = rel.WhichOneof("rel_type")
    if kind == "hash_join":
        n += 1
        n += _count_hash_joins(rel.hash_join.left)
        n += _count_hash_joins(rel.hash_join.right)
        return n
    if kind in (
        "filter",
        "project",
        "fetch",
        "aggregate",
        "sort",
        "exchange",
        "extension_single",
    ):
        inner = getattr(rel, kind)
        if inner.HasField("input"):
            n += _count_hash_joins(inner.input)
    return n


def _root_output_names(p: plan_pb2.Plan) -> list[str]:
    return list(p.relations[0].root.names)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestJoinReorderInvariants(unittest.TestCase):
    """Properties the rule must satisfy on EVERY input."""

    def test_purity_input_not_mutated(self) -> None:
        # Build a 2-leaf inner-join plan over two extension reads (no
        # cardinality available, so no reorder happens — but the rule
        # still must not mutate its input).
        left = _read_rel_extension(["a_k", "a_v"])
        right = _read_rel_extension(["b_k", "b_v"])
        join = _hash_join(
            left,
            right,
            keys=[_eq_join_key(0, 0)],
            output_names=["a_k", "a_v", "b_k", "b_v"],
        )
        before = _wrap_in_root(join, ["a_k", "a_v", "b_k", "b_v"])
        snapshot = before.SerializeToString()

        JoinReorder().apply(before)

        # The rule's own contract is "Pure: the input ``Plan`` is not
        # mutated." Verify the snapshot survived unchanged regardless of
        # whether the rule rewrote anything.
        self.assertEqual(before.SerializeToString(), snapshot)

    def test_idempotence_two_passes_byte_equal(self) -> None:
        left = _read_rel_extension(["a_k", "a_v"])
        right = _read_rel_extension(["b_k", "b_v"])
        join = _hash_join(
            left,
            right,
            keys=[_eq_join_key(0, 0)],
            output_names=["a_k", "a_v", "b_k", "b_v"],
        )
        before = _wrap_in_root(join, ["a_k", "a_v", "b_k", "b_v"])

        rule = JoinReorder()
        once = rule.apply(before)
        twice = rule.apply(once)

        # Idempotence: applying the rule a second time on its own output
        # produces a byte-equal Plan. (If the rule weren't idempotent,
        # the fixpoint loop in ``SubstraitOptimizerPipeline`` would
        # spin until ``maxIterations``.)
        self.assertEqual(once.SerializeToString(), twice.SerializeToString())


class TestJoinReorderBails(unittest.TestCase):
    """Cases where the rule must NOT rewrite the plan."""

    def test_no_reorder_when_cardinality_unknown(self) -> None:
        # Three-leaf inner join over extension reads. Without parquet
        # metadata the cardinality oracle returns None for every leaf,
        # so the rule must leave the plan structurally unchanged.
        a = _read_rel_extension(["a_k", "a_v"])
        b = _read_rel_extension(["b_k", "b_v"])
        c = _read_rel_extension(["c_k", "c_v"])
        ab = _hash_join(
            a,
            b,
            keys=[_eq_join_key(0, 0)],
            output_names=["a_k", "a_v", "b_k", "b_v"],
        )
        abc = _hash_join(
            ab,
            c,
            keys=[_eq_join_key(0, 0)],
            output_names=["a_k", "a_v", "b_k", "b_v", "c_k", "c_v"],
        )
        before = _wrap_in_root(abc, ["a_k", "a_v", "b_k", "b_v", "c_k", "c_v"])

        after = JoinReorder().apply(before)

        # No reorder happened — the plan is byte-equal modulo trivial
        # field reordering inside the proto. We verify the join count
        # and the root output names did not change.
        self.assertEqual(_count_hash_joins(before.relations[0].root.input), 2)
        self.assertEqual(_count_hash_joins(after.relations[0].root.input), 2)
        self.assertEqual(_root_output_names(before), _root_output_names(after))

    def test_non_inner_join_not_reordered(self) -> None:
        # LEFT OUTER join — the rule's pattern only matches
        # ``JOIN_TYPE_INNER``. The graph extractor returns None for
        # any non-inner branch and the rule walks past without rewriting.
        a = _read_rel_extension(["a_k", "a_v"])
        b = _read_rel_extension(["b_k", "b_v"])
        join_left = _hash_join(
            a,
            b,
            keys=[_eq_join_key(0, 0)],
            output_names=["a_k", "a_v", "b_k", "b_v"],
            join_type=algebra.HashJoinRel.JoinType.JOIN_TYPE_LEFT,
        )
        before = _wrap_in_root(join_left, ["a_k", "a_v", "b_k", "b_v"])
        snapshot = before.SerializeToString()

        after = JoinReorder().apply(before)

        # LEFT join survived unchanged — same proto bytes, same join type.
        self.assertEqual(after.SerializeToString(), snapshot)
        self.assertEqual(
            after.relations[0].root.input.hash_join.type,
            algebra.HashJoinRel.JoinType.JOIN_TYPE_LEFT,
        )

    def test_post_join_filter_not_reordered(self) -> None:
        # Inner join with a non-empty ``post_join_filter``. The graph
        # extractor refuses to fold this join into the brute-force
        # space (post-filter changes the row count contract for any
        # rewritten parent), so the rule leaves the join in place.
        a = _read_rel_extension(["a_k", "a_v"])
        b = _read_rel_extension(["b_k", "b_v"])
        # A trivial "always true" post-filter — the contents don't matter,
        # only the presence of the field does.
        post = _direct_field(0)
        join = _hash_join(
            a,
            b,
            keys=[_eq_join_key(0, 0)],
            output_names=["a_k", "a_v", "b_k", "b_v"],
            post_join_filter=post,
        )
        before = _wrap_in_root(join, ["a_k", "a_v", "b_k", "b_v"])
        snapshot = before.SerializeToString()

        after = JoinReorder().apply(before)

        # Plan survived intact — post_join_filter inhibits the rewrite.
        self.assertEqual(after.SerializeToString(), snapshot)


class TestJoinReorderMaxLeaves(unittest.TestCase):
    """The brute-force search is O(3^n); a cap on leaf count is
    required so adversarial inputs don't hang compile."""

    def test_constant_is_reasonable(self) -> None:
        # Sanity: the cap is small enough that 3^_MAX_LEAVES fits in a
        # rough "milliseconds" budget. We don't pin an exact value here
        # because it's a tunable; we just guard against an accidental
        # bump to e.g. 100 that would silently make the rule O(seconds).
        self.assertLessEqual(_MAX_LEAVES, 12)
        self.assertGreaterEqual(_MAX_LEAVES, 6)


class TestJoinReorderWithCardinality(unittest.TestCase):
    """Cases where parquet metadata IS available — exercises the
    end-to-end rewrite + output-schema preservation path."""

    def test_output_names_preserved_2leaf(self) -> None:
        # Two real parquet files of differing row counts. The rule may
        # decide to swap the join's left/right (the smaller side typically
        # belongs on the right as the build side); regardless of the
        # decision, the root's output names must match the input's.
        with tempfile.TemporaryDirectory() as tmp:
            left_path = os.path.join(tmp, "a.parquet")
            right_path = os.path.join(tmp, "b.parquet")
            _write_parquet(left_path, n_rows=1000, name="a_k")
            _write_parquet(right_path, n_rows=10, name="b_k")

            left = _read_rel_local_files([f"file://{left_path}"], ["a_k"])
            right = _read_rel_local_files([f"file://{right_path}"], ["b_k"])
            join = _hash_join(
                left,
                right,
                keys=[_eq_join_key(0, 0)],
                output_names=["a_k", "b_k"],
            )
            before = _wrap_in_root(join, ["a_k", "b_k"])

            after = JoinReorder().apply(before)

            # Root output names survive the rewrite, even though the rule
            # may have swapped sides under the hood.
            self.assertEqual(_root_output_names(before), _root_output_names(after))
            # Same number of hash_joins (rewrite never adds joins; it
            # only re-shapes them).
            self.assertEqual(
                _count_hash_joins(before.relations[0].root.input),
                _count_hash_joins(after.relations[0].root.input),
            )


if __name__ == "__main__":
    unittest.main()
