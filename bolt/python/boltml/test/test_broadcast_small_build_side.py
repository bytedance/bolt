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

"""Unit tests for ``BroadcastSmallBuildSide``.

The rule replaces a hash exchange over a small right-side scan with a
broadcast exchange when the total bytes of the right subtree fall below
``BOLTML_BROADCAST_BUILD_BYTES``. Opt-in: env var default 0 means
"leave plan unchanged."
"""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from substrait.proto import algebra, plan as plan_pb2, type as type_pb2

from ..optimizer.substrait_rules.broadcast_small_build_side import (
    BroadcastSmallBuildSide,
)


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


def _exchange_hash(input_rel: algebra.Rel, key_field: int) -> algebra.Rel:
    return algebra.Rel(
        exchange=algebra.ExchangeRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(alias=f"hash(field-{key_field})"),
            ),
            input=input_rel,
            partition_count=12,
            scatter_by_fields=algebra.ExchangeRel.ScatterFields(
                fields=[_direct_field(key_field).selection],
            ),
        )
    )


def _hash_join(left: algebra.Rel, right: algebra.Rel) -> algebra.Rel:
    return algebra.Rel(
        hash_join=algebra.HashJoinRel(
            common=algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=["a", "b"]),
            ),
            left=left,
            right=right,
            keys=[
                algebra.ComparisonJoinKey(
                    left=_direct_field(0).selection,
                    right=_direct_field(0).selection,
                    comparison=algebra.ComparisonJoinKey.ComparisonType(
                        simple=algebra.ComparisonJoinKey.SIMPLE_COMPARISON_TYPE_EQ,
                    ),
                ),
            ],
            type=algebra.HashJoinRel.JoinType.JOIN_TYPE_INNER,
        )
    )


def _wrap_in_root(rel: algebra.Rel, names: list[str]) -> plan_pb2.Plan:
    p = plan_pb2.Plan()
    p.relations.add().root.CopyFrom(algebra.RelRoot(input=rel, names=names))
    return p


def _write_parquet(path: str, n_rows: int, name: str = "x") -> None:
    t = pa.table({name: pa.array(range(n_rows), type=pa.int64())})
    pq.write_table(t, path)


class TestBroadcastSmallBuildSide(unittest.TestCase):
    def test_purity_input_not_mutated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "small.parquet")
            _write_parquet(path, n_rows=10)
            left = _read_rel_local_files([f"file://{path}"], ["a"])
            right = _read_rel_local_files([f"file://{path}"], ["b"])
            join = _hash_join(left, _exchange_hash(right, 0))
            before = _wrap_in_root(join, ["a", "b"])
            snapshot = before.SerializeToString()
            with patch.dict(os.environ, {"BOLTML_BROADCAST_BUILD_BYTES": "10000000"}):
                BroadcastSmallBuildSide().apply(before)
            self.assertEqual(before.SerializeToString(), snapshot)

    def test_idempotence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "small.parquet")
            _write_parquet(path, n_rows=10)
            left = _read_rel_local_files([f"file://{path}"], ["a"])
            right = _read_rel_local_files([f"file://{path}"], ["b"])
            join = _hash_join(left, _exchange_hash(right, 0))
            before = _wrap_in_root(join, ["a", "b"])
            with patch.dict(os.environ, {"BOLTML_BROADCAST_BUILD_BYTES": "10000000"}):
                rule = BroadcastSmallBuildSide()
                once = rule.apply(before)
                twice = rule.apply(once)
                self.assertEqual(once.SerializeToString(), twice.SerializeToString())

    def test_opt_in_default_off_leaves_plan_unchanged(self) -> None:
        # No env var set (or 0) -- the rule must produce a byte-equal copy
        # even when a join with a small right side could in principle be
        # rewritten. This is the "default off" contract.
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "small.parquet")
            _write_parquet(path, n_rows=10)
            left = _read_rel_local_files([f"file://{path}"], ["a"])
            right = _read_rel_local_files([f"file://{path}"], ["b"])
            join = _hash_join(left, _exchange_hash(right, 0))
            before = _wrap_in_root(join, ["a", "b"])
            with patch.dict(os.environ, {"BOLTML_BROADCAST_BUILD_BYTES": "0"}):
                after = BroadcastSmallBuildSide().apply(before)
            # Right-side stays a scatter_by_fields exchange; broadcast oneof
            # not picked.
            right_after = after.relations[0].root.input.hash_join.right
            self.assertEqual(right_after.WhichOneof("rel_type"), "exchange")
            self.assertEqual(
                right_after.exchange.WhichOneof("exchange_kind"),
                "scatter_by_fields",
            )

    def test_above_threshold_no_rewrite(self) -> None:
        # Build a parquet whose on-disk size comfortably exceeds the
        # threshold -- the rule must leave the plan as a hash exchange.
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "big.parquet")
            _write_parquet(path, n_rows=100_000)
            actual_bytes = os.path.getsize(path)
            self.assertGreater(actual_bytes, 0)
            threshold = max(1, actual_bytes // 4)  # well below file size

            left = _read_rel_local_files([f"file://{path}"], ["a"])
            right = _read_rel_local_files([f"file://{path}"], ["b"])
            join = _hash_join(left, _exchange_hash(right, 0))
            before = _wrap_in_root(join, ["a", "b"])
            with patch.dict(
                os.environ, {"BOLTML_BROADCAST_BUILD_BYTES": str(threshold)}
            ):
                after = BroadcastSmallBuildSide().apply(before)
            right_after = after.relations[0].root.input.hash_join.right
            self.assertEqual(
                right_after.exchange.WhichOneof("exchange_kind"),
                "scatter_by_fields",
                msg="right side >= threshold must stay as scatter_by_fields",
            )


if __name__ == "__main__":
    unittest.main()
