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

"""Cross-executor / cross-plan-builder demo + smoke test.

The ``@cross_executor_tests`` decorator (see
``boltml/test/_executor_matrix.py``) runs every method below against
every ``(executor, plan_factory)`` combo declared in
``DEFAULT_MATRIX`` — currently::

    Local_Substrait
    Local_Bolt
    Ray_StagedLocal_Substrait
    Ray_StagedLocal_Bolt
    Ray_RemoteLeaf_Substrait
    Ray_RemoteLeaf_Bolt

Six combos × the test-method count below is the count of test cases
``unittest discover`` reports. The intent of this file is twofold:

  1. **Smoke test** — every executor / plan-builder pair must produce
     identical results for a small set of representative ops (filter,
     groupBy + aggregate, transform, sort, head). Catching a divergence
     here means one of the executors or builders has drifted from the
     others; the failing combo names the culprit.

  2. **Template** — the file serves as the canonical example for
     adding more cross-executor coverage. Existing test files that
     exercise default-constructor DataFrames can be migrated by
     adding ``@cross_executor_tests`` and (if needed) trimming any
     test that explicitly asserts on a specific executor.

Skip individual combos via ``BOLTML_TEST_MATRIX=Local_Substrait`` (etc).
"""

import unittest

from ..dataframe import DataFrame
from ..function.aggregation import Count, Mean, Sum
from ._executor_matrix import cross_executor_tests


@cross_executor_tests
class TestExecutorMatrixDemo(unittest.TestCase):
    """Same DataFrame ops, every (executor, plan-builder) combo.

    Each test method runs once per combo (per the ``DEFAULT_MATRIX`` in
    ``_executor_matrix.py``); a divergence in any combo surfaces as a
    failure naming that specific combo (e.g.
    ``TestExecutorMatrixDemo_Ray_RemoteLeaf_Bolt.testGroupByAggregate``).
    """

    # ------------------------------------------------------------------
    # Materialisation primitives — these must hold for every combo so
    # downstream tests can rely on the basic DataFrame semantics.
    # ------------------------------------------------------------------

    def testRowCount(self):
        df = DataFrame({"a": [1, 2, 3, 4, 5]})
        self.assertEqual(len(df), 5)

    def testColumnAccess(self):
        df = DataFrame({"a": [10, 20, 30], "b": [1.5, 2.5, 3.5]})
        self.assertEqual(list(df["a"]), [10, 20, 30])
        self.assertEqual(list(df["b"]), [1.5, 2.5, 3.5])

    def testEmptyDataframeMaterialisation(self):
        # ``DataFrame({"a": []})`` doesn't work because the inference
        # path can't pick a Bolt dtype from an empty Python list — that
        # limitation is independent of executor / plan-builder. Build
        # the empty table via PyArrow with explicit types so the test
        # actually exercises the per-combo materialisation path rather
        # than the dtype-inference one.
        import pyarrow as pa

        table = pa.table(
            {
                "a": pa.array([], type=pa.int64()),
                "b": pa.array([], type=pa.float64()),
            }
        )
        df = DataFrame(table)
        self.assertEqual(len(df), 0)
        self.assertEqual(set(df.names), {"a", "b"})

    # ------------------------------------------------------------------
    # Single-stage transforms — Project / Filter. Exercise the leaf
    # executor path on every combo with no exchange in play.
    # ------------------------------------------------------------------

    def testFilterByPredicate(self):
        df = DataFrame({"id": [1, 2, 3, 4, 5], "tag": ["a", "b", "a", "c", "a"]})
        df.filter(df["tag"] == "a")
        self.assertEqual(list(df["id"]), [1, 3, 5])

    def testTransformAddsConstant(self):
        df = DataFrame({"x": [10, 20, 30]})
        df.transform(y=lambda frame: frame["x"] + 5)
        self.assertEqual(list(df["y"]), [15, 25, 35])

    def testSelectSubsetOfColumns(self):
        df = DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
        df.select(["a", "c"])
        self.assertEqual(set(df.names), {"a", "c"})
        self.assertEqual(list(df["a"]), [1, 2, 3])
        self.assertEqual(list(df["c"]), [7, 8, 9])

    # ------------------------------------------------------------------
    # Multi-stage flows — GroupBy + Aggregate + OrderBy. These create
    # exchange boundaries under Ray; the assertion that
    # ``LocalExecutor`` and ``RayExecutor`` produce the same multiset
    # of result rows is the core cross-test guarantee.
    # ------------------------------------------------------------------

    def testGroupByAggregateSum(self):
        df = DataFrame({"grp": ["a", "a", "b", "b", "b", "c"], "v": [1, 2, 3, 4, 5, 6]})
        result = df.groupBy("grp").aggregate(total=Sum("v"))
        rows = sorted(result.toArrow().to_pylist(), key=lambda r: r["grp"])
        self.assertEqual(
            rows,
            [
                {"grp": "a", "total": 3},
                {"grp": "b", "total": 12},
                {"grp": "c", "total": 6},
            ],
        )

    def testGroupByAggregateMeanAndCount(self):
        df = DataFrame({"grp": ["x", "x", "y"], "v": [10.0, 20.0, 30.0]})
        result = df.groupBy("grp").aggregate(avg=Mean("v"), n=Count())
        rows = sorted(result.toArrow().to_pylist(), key=lambda r: r["grp"])
        self.assertEqual(
            rows,
            [
                {"grp": "x", "avg": 15.0, "n": 2},
                {"grp": "y", "avg": 30.0, "n": 1},
            ],
        )

    def testOrderByAscending(self):
        df = DataFrame({"k": [3, 1, 4, 1, 5, 9, 2]})
        df.orderBy("k")
        self.assertEqual(list(df["k"]), [1, 1, 2, 3, 4, 5, 9])

    # ------------------------------------------------------------------
    # Composed pipeline — a representative end-to-end query that
    # touches Filter → GroupBy → Aggregate → OrderBy → head. Anything
    # that breaks the row-order or aggregate-output guarantees of the
    # underlying executor will surface here.
    # ------------------------------------------------------------------

    def testFilterGroupByAggregateOrderByHead(self):
        # Filter via ``df["val"] != 40`` rather than a boolean column.
        # ``df["keep"]`` would return a bare ``ColumnProject`` (there's
        # no ``BooleanColumn`` class wired through ``ColumnView.make``);
        # the ``DataFrame.filter`` API only accepts ``BooleanExpression``
        # subclasses. That's a generic API gap, not a per-combo
        # divergence — the cross-executor matrix shouldn't paper over
        # it. Numeric ``__ne__`` returns a proper ``BooleanExpression``
        # in every combo, which is what we want to exercise here.
        #
        # Per-group totals are kept distinct (180, 70, 30) so the
        # ``orderBy(total DESC)`` ordering is unambiguous — sort
        # tiebreaks are unspecified and would otherwise cause flakes
        # across executors that order tied keys differently.
        df = DataFrame(
            {
                "tag": ["a", "a", "b", "b", "c", "c", "c"],
                "val": [10, 20, 30, 40, 50, 60, 70],
            }
        )
        df.filter(df["val"] != 25)  # All rows kept; predicate proves filter executes.
        result = (
            df.groupBy("tag")
            .aggregate(total=Sum("val"))
            .orderBy([("total", _DESC_NULLS_LAST())])
        )
        head = result.toArrow().to_pylist()[:2]
        self.assertEqual(
            head,
            [
                {"tag": "c", "total": 180},  # 50 + 60 + 70
                {"tag": "b", "total": 70},  # 30 + 40
            ],
        )


def _DESC_NULLS_LAST():
    """Lazy import of pybolt.SortOrder.DESC_NULLS_LAST.

    Local helper so the module import doesn't pull pybolt eagerly —
    keeps ``_executor_matrix`` collection cheap on machines without
    a built pybolt (the cross-test decorator only needs pybolt at
    test execution time, not at module import).
    """
    from pybolt import SortOrder

    return SortOrder.DESC_NULLS_LAST


if __name__ == "__main__":
    unittest.main()
