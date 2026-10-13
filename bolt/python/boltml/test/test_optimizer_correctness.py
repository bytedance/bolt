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

"""Regression results are independent literals, not another optimized query."""

from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq
import pybolt

from boltml import DataFrame
from boltml.executor.ray import RayExecutor
from boltml.function import vectorFunction
from boltml.function.aggregation import Count, Sum
from boltml.optimizer.substrait_physical_pipeline import (
    SubstraitPhysicalOptimizerPipeline,
)
from boltml.optimizer.substrait_rules.partial_agg_before_exchange import (
    PartialAggBeforeExchange,
)
from boltml.optimizer.substrait_rules.add_exchanges import AddExchanges
from boltml.reader import LocalTableReader


class OptimizerCorrectnessTests(unittest.TestCase):
    def testMergedEmptyLimitsPreserveSchema(self):
        data = pa.table({"id": [0, 1, 2, 3], "label": ["a", "b", "c", "d"]})
        for offset in (2, 3):
            with self.subTest(offset=offset):
                frame = (
                    DataFrame(data)
                    .select(count=2, offset=1)
                    .select(["label", "id"])
                    .select(count=1, offset=offset)
                )
                table = frame.toArrow(pa.Table)
                self.assertEqual(table.to_pylist(), [])
                self.assertEqual(table.schema, data.select(["label", "id"]).schema)

    def testAggregateAfterMergedEmptyLimit(self):
        frame = DataFrame({"id": [0, 1, 2, 3]})
        frame.select(count=2).select(count=1, offset=3)
        frame.groupBy().aggregate(rows=Count(), values=Count("id"), total=Sum("id"))
        self.assertEqual(
            frame.toArrow(pa.Table).to_pylist(),
            [{"rows": 0, "values": 0, "total": None}],
        )

    def testDefaultRayCountStarIncludesNullRows(self):
        executor = RayExecutor()
        frame = DataFrame(
            pa.table({"grp": [1, 1, 2], "v": [None, 3, None]}), executor=executor
        )
        frame.groupBy("grp").aggregate(rows=Count(), values=Count("v"))
        self.assertCountEqual(
            frame.toArrow(pa.Table).to_pylist(),
            [
                {"grp": 1, "rows": 2, "values": 1},
                {"grp": 2, "rows": 1, "values": 0},
            ],
        )
        self.assertGreaterEqual(len(executor.lastExecutionSummary.stages), 2)

    def parquetFrame(self, folder, name, data, types):
        path = Path(folder) / f"{name}.parquet"
        pq.write_table(pa.table(data), path)
        return LocalTableReader(
            str(path), pybolt.RowType(list(data), types), pybolt.FileFormat.PARQUET
        ).read()

    def testJoinAggregationPreservesBothSidesMultiplicity(self):
        for computed in (False, True):
            with self.subTest(computed=computed):
                left = DataFrame({"k": [1, 1], "w": [2, 2]})
                right = DataFrame({"k": [1, 1, 1], "q": [5, 10, 15]})
                joined = left.join(right, keys={"k"})
                if computed:
                    joined.transform(w=lambda df: df["w"] * 2, q=lambda df: df["q"] * 2)
                result = joined.groupBy("k").aggregate(l=Sum("w"), r=Sum("q"))
                factor = 2 if computed else 1
                self.assertEqual(
                    result.toArrow(pa.Table).to_pylist(),
                    [{"k": 1, "l": 12 * factor, "r": 60 * factor}],
                )

    def testOneSidedSumStillCountsOppositeRows(self):
        left = DataFrame({"k": [1, 1], "w": [2, 3]})
        right = DataFrame({"k": [1, 1, 1], "q": [5, 10, 15]})
        result = left.join(right, keys={"k"}).groupBy("k").aggregate(total=Sum("w"))
        self.assertEqual(result.toArrow(pa.Table).to_pylist(), [{"k": 1, "total": 15}])

    def testJoinReorderPreservesSameNamedNonkeyColumns(self):
        with tempfile.TemporaryDirectory() as folder:
            types = [pybolt.BigintType(), pybolt.VarcharType()]
            left = self.parquetFrame(
                folder, "orders", {"k": [1, 2], "name": ["order1", "order2"]}, types
            )
            right = self.parquetFrame(
                folder, "customers", {"k": [1, 2], "name": ["alice", "bob"]}, types
            )
            result = left.join(right, keys={"k"})
            self.assertCountEqual(
                result.toArrow(pa.Table).to_pylist(),
                [
                    {"k": 1, "l_name": "order1", "r_name": "alice"},
                    {"k": 2, "l_name": "order2", "r_name": "bob"},
                ],
            )

    def testSemiJoinRewriteRetainsDeduplication(self):
        with tempfile.TemporaryDirectory() as folder:
            left = self.parquetFrame(
                folder,
                "orders",
                {"k": [1, 2, 3], "priority": ["HIGH", "HIGH", "LOW"]},
                [pybolt.BigintType(), pybolt.VarcharType()],
            )
            right = self.parquetFrame(
                folder,
                "items",
                {"k": [1, 1, 2, 3], "qty": [1, 2, 1, 1]},
                [pybolt.BigintType(), pybolt.BigintType()],
            )
            result = (
                left.join(right, keys={"k"})
                .groupBy("priority")
                .aggregate(n=Sum("qty"))
                .select(["priority"])
            )
            self.assertCountEqual(
                result.toArrow(pa.Table).to_pylist(),
                [{"priority": "HIGH"}, {"priority": "LOW"}],
            )

    def testFilterDoesNotEvaluateComputedUdfTwice(self):
        calls = []

        @vectorFunction(pybolt.BigintType())
        def optimizerCountingIdentity(values):
            calls.append(len(values))
            return pybolt.fromList(list(values))

        frame = DataFrame({"x": [1, 2, 3]})
        frame.transform(y=lambda df: df["x"].map(optimizerCountingIdentity))
        frame.filter(frame["y"] > 0).select(["y"])
        self.assertEqual(
            frame.toArrow(pa.Table).to_pylist(), [{"y": 1}, {"y": 2}, {"y": 3}]
        )
        self.assertEqual(sum(calls), 3)

    def testRealSumKeepsSinglePhaseWithCorrectResults(self):
        executor = RayExecutor(
            physical=SubstraitPhysicalOptimizerPipeline(defaultPartitionCount=2)
        )
        frame = DataFrame(
            pa.table(
                {"grp": [1, 1, 2], "v": pa.array([1.25, 2.5, 4.0], type=pa.float32())}
            ),
            executor=executor,
        )
        frame.groupBy("grp").aggregate(total=Sum("v"))
        before = AddExchanges().apply(frame._planBuilder_.toSubstraitPlan())
        after = PartialAggBeforeExchange().apply(before)
        self.assertEqual(before, after)
        self.assertCountEqual(
            frame.toArrow(pa.Table).to_pylist(),
            [{"grp": 1, "total": 3.75}, {"grp": 2, "total": 4.0}],
        )
        self.assertGreaterEqual(len(executor.lastExecutionSummary.stages), 2)


if __name__ == "__main__":
    unittest.main()
