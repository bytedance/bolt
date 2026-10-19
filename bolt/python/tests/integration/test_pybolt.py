# Copyright (c) ByteDance Ltd. and/or its affiliates.
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

"""Exercise the installed native package without importing source-tree helpers."""

import unittest

import numpy as np
import pyarrow as pa
import pybolt


class NativeExecutionTests(unittest.TestCase):
    def testVectorsThroughFilterProjectAndAggregation(self):
        source = pybolt.rowVector(
            ["key", "value"],
            [
                pybolt.fromList(["a", "b", "a", "b"]),
                pybolt.fromList([1, 2, 3, None], pybolt.BigintType()),
            ],
        )
        plan = (
            pybolt.PlanBuilder()
            .values([source])
            .filter("value > 1")
            .project(["key", "value * CAST(2 AS BIGINT) AS doubled"])
            .singleAggregation(
                groupingKeys=["key"], aggregates=["sum(doubled) AS total"]
            )
            .orderBy(["key ASC"], isPartial=False)
        )
        result = pybolt.BoltTaskExecutor().execute(plan.planFragment(), self.id())
        self.assertEqual(list(result.childAt("key")), ["a", "b"])
        self.assertEqual(list(result.childAt("total")), [6, 4])

    def testHashJoinExecutesBothInputs(self):
        left = pybolt.rowVector(["id"], [pybolt.fromList([1, 2, 3])])
        right = pybolt.rowVector(
            ["right_id", "label"],
            [pybolt.fromList([2, 3]), pybolt.fromList(["b", "c"])],
        )
        build = pybolt.PlanBuilder(0).values([right]).planNode()
        plan = (
            pybolt.PlanBuilder(100)
            .values([left])
            .hashJoin(
                leftKeys=["id"],
                rightKeys=["right_id"],
                build=build,
                filter="",
                outputLayout=["id", "label"],
                joinType=pybolt.JoinType.kInner,
                nullAware=False,
            )
            .orderBy(["id ASC"], isPartial=False)
        )
        result = pybolt.BoltTaskExecutor().execute(plan.planFragment(), self.id())
        self.assertEqual(
            pybolt.exportToArrow(result).to_pylist(),
            [{"id": 2, "label": "b"}, {"id": 3, "label": "c"}],
        )

    def testNativeCountStarIncludesNullRows(self):
        source = pybolt.rowVector(
            ["value"], [pybolt.fromList([1, None, 3], pybolt.BigintType())]
        )
        plan = (
            pybolt.PlanBuilder()
            .values([source])
            .singleAggregation(
                groupingKeys=[],
                aggregates=["count(*) AS rows", "count(value) AS nonnull"],
            )
        )
        result = pybolt.BoltTaskExecutor().execute(plan.planFragment(), self.id())
        self.assertEqual(list(result.childAt("rows")), [3])
        self.assertEqual(list(result.childAt("nonnull")), [2])


class VectorInteropTests(unittest.TestCase):
    def testNullableBinaryArrowRoundTrip(self):
        values = [b"\x00\xffbinary", None, b""]
        vector = pybolt.fromList(values, pybolt.VarbinaryType())
        arrow = pybolt.exportToArrow(vector)
        self.assertEqual(arrow.type, pa.binary())
        self.assertEqual(arrow.to_pylist(), values)
        restored = pybolt.importFromArrow(arrow)
        self.assertEqual(restored.dtype(), pybolt.VarbinaryType())
        self.assertEqual(list(restored), values)

    def testArrowStructRoundTripPreservesNamesAndNulls(self):
        source = pa.StructArray.from_arrays(
            [pa.array([1, None, 3], type=pa.int64()), pa.array(["a", "b", None])],
            names=["id", "name"],
        )
        vector = pybolt.importFromArrow(source)
        self.assertEqual(vector.dtype().names(), ["id", "name"])
        self.assertTrue(pybolt.exportToArrow(vector).equals(source))

    def testNumpyTensorBridge(self):
        for dtype, elementType in (
            (np.int32, pybolt.IntegerType()),
            (np.float32, pybolt.RealType()),
        ):
            with self.subTest(dtype=dtype):
                source = np.arange(12, dtype=dtype).reshape(2, 2, 3)
                vector = pybolt.fromNumpy(source, elementType, [2, 3])
                self.assertEqual(vector.dtype(), pybolt.ArrayType(elementType))
                actual = vector.to_numpy(shape=[2, 3])
                self.assertEqual(actual.dtype, source.dtype)
                np.testing.assert_array_equal(actual, source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
