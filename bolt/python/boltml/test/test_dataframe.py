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

import copy
import os
import shutil
import unittest

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pybolt


from ..dataframe import DataFrame
from ..expression.case import Switch, If
from ..function import arrowAdapter, dataframeFunction
from ..function.aggregation import All
from ..function.aggregation import Any
from ..function.aggregation import Count
from ..function.aggregation import Max
from ..function.aggregation import Mean
from ..function.aggregation import Min
from ..function.aggregation import Sum
from ..reader.local import LocalTableReader
from ..tpch import Nation
from ..writer.local import LocalTableWriter

from .utils import WithDataFrame, Runtime

from pybolt.test.utils import StringGenerator, IntGenerator, FloatGenerator


def lazy(df: DataFrame):
    df.select(df.names)


class TestDataFrame(unittest.TestCase):
    @WithDataFrame.data(c0=[1], runtimes=Runtime.minimal())
    def testEqual(self, df):
        self.assertEqual(df, DataFrame(df._data_))

        rhs = df.copy()
        rhs[0, "c0"] = 2
        self.assertNotEqual(df, DataFrame(rhs._data_))

    @WithDataFrame.data(c0=[1], runtimes=Runtime.minimal())
    def testEqualAgainstNonDataFrameReturnsFalse(self, df):
        # ``__eq__`` must return ``False`` (not raise) when ``rhs`` isn't
        # a DataFrame. CaseExpression's dataframe-consistency check walks
        # cases with ``caseExpr.dataframe != dataframe`` and the literal
        # ``otherwise(0.0)`` arm carries ``dataframe=None`` — without
        # this isinstance guard ``len(rhs.names)`` would crash on
        # ``None``. Comparing against arbitrary non-DataFrame types is
        # also legal Python and must not raise.
        self.assertFalse(df == None)  # noqa: E711
        self.assertTrue(df != None)  # noqa: E711
        self.assertFalse(df == "not a dataframe")
        self.assertFalse(df == 42)
        self.assertFalse(df == [1])
        self.assertFalse(df == {"c0": [1]})

    @WithDataFrame.data(c0=[1], runtimes=Runtime.minimal())
    def testNames(self, df):
        self.assertEqual(df.names, ["c0"])

    @WithDataFrame.data(c0=[1])
    def testNamesLazy(self, df):
        lazy(df)
        self.assertEqual(df.names, ["c0"])

    @WithDataFrame.data(c0=[1], runtimes=Runtime.minimal())
    def testLen(self, df):
        self.assertEqual(len(df), 1)

    @WithDataFrame.data(c0=[1])
    def testLenLazy(self, df):
        lazy(df)
        self.assertEqual(len(df), 1)

    @WithDataFrame.data(c0=[1], runtimes=Runtime.minimal())
    def testGetitemValue(self, df):
        self.assertEqual(df[0, "c0"], 1)

    @WithDataFrame.data(c0=[1, 2, 3], runtimes=Runtime.minimal())
    def testGetitemColumnsAndSlice(self, df):
        self.assertEqual(df[1:-1, :][0, 0], 2)

    @WithDataFrame.data(c0=[1], runtimes=Runtime.minimal())
    def testSetitemValue(self, df):
        df[0, "c0"] = 2
        self.assertEqual(df[0, "c0"], 2)

    @WithDataFrame.data(c0=[1, 3], runtimes=Runtime.minimal())
    def testColumnToConstantValue(self, df):
        df["c0"] = 2
        self.assertEqual(len(df), 2)
        self.assertTrue(all(v == 2 for v in df["c0"]))

        df["c0"] = True
        self.assertTrue(all(v for v in df["c0"]))

    @WithDataFrame.data(c0=[1, 1], c1=[2, 2])
    def testColumnToOtherColumn(self, df):
        df["c1"] = df["c0"]
        df["new_col"] = df["c0"]
        self.assertEqual(len(df), 2)
        self.assertTrue(df["new_col"].equals(df["c1"]))

    @WithDataFrame.data(c0=[1], runtimes=Runtime.minimal())
    def testAppend(self, df):
        dfdf = df.copy().append(df)
        self.assertEqual(df, dfdf[: len(df), :])
        self.assertEqual(df, dfdf[len(df) :, :])

    @WithDataFrame.data(c0=[1, 1])
    def testSelect(self, df):
        result = df.copy().select(["c0"], offset=1, count=1)
        expected = df[1:, "c0"]
        self.assertEqual(result, expected)

    @WithDataFrame.random(c0=IntGenerator())
    def testTransform(self, df):
        col = df["c0"].copy()
        df.transform(c2=df["c0"] * 2, n2=df["c0"] * 2)
        self.assertTrue(all((lhs * 2 == rhs for lhs, rhs in zip(col, df["c2"]))))
        self.assertTrue(all((lhs * 2 == rhs for lhs, rhs in zip(col, df["n2"]))))

    @WithDataFrame.data(c0=[2, 4, 6])
    def testSwitch(self, df):
        expected = {"c0": [0, 2, 8]}
        expr = (
            Switch(df)
            .case(lambda df: df["c0"] < 4)
            .then(0)
            .case(lambda df: df["c0"] > 4)
            .then(8)
            .default(2)
        )

        df.transform(c0=expr)
        self.assertTrue(all((lhs == rhs for lhs, rhs in zip(expected["c0"], df["c0"]))))

    @WithDataFrame.data(c0=[2, 4, 6])
    def testIf(self, df):
        expected = {"c0": [0, 2, 2]}
        expr = If(df["c0"] < 4).then(0).otherwise(2)
        df.transform(c0=expr)
        self.assertTrue(all((lhs == rhs for lhs, rhs in zip(expected["c0"], df["c0"]))))

    @WithDataFrame.data(c0=[1, 5, 9])
    def testIfWithLiteralDefaultPicksCaseDataframe(self, df):
        # ``CaseExpression`` walks ``case.dataframe != dataframe`` for
        # consistency. The ``otherwise(0)`` arm wraps a literal, which
        # carries ``dataframe=None``. The fix at ``case.py:79`` picks
        # the first non-None ``dataframe`` from the cases when the
        # default is a literal. Without it, ``CaseExpression.__init__``
        # would pick ``None`` from the literal default and then complain
        # the case (bound to ``df``) is inconsistent.
        expr = If(df["c0"] > 4).then(df["c0"]).otherwise(0)
        df.transform(out=expr)
        self.assertEqual(list(df["out"]), [0, 5, 9])

    @WithDataFrame.data(c0=[1.0, 5.0, 9.0])
    def testIfWithLiteralFloatDefaultPicksCaseDataframe(self, df):
        # Same shape as above but with float columns + float default —
        # locks in coverage that the literal-default fix is type-agnostic
        # (the original regression surfaced with both int and float
        # literal default arms when CaseExpression walked the cases).
        expr = If(df["c0"] > 4.0).then(df["c0"]).otherwise(0.0)
        df.transform(out=expr)
        self.assertEqual(list(df["out"]), [0.0, 5.0, 9.0])

    @WithDataFrame.random(
        c0=IntGenerator(),
        c1=StringGenerator(),
        c2=IntGenerator(),
        c3=FloatGenerator(),
        length=8,
    )
    def testOrderBy(self, df):
        for col in df.names:
            df.orderBy(col)
            orderedCol = list(iter(df[col]))
            self.assertEqual(orderedCol, sorted(orderedCol))

    @WithDataFrame.random(c0=IntGenerator(), c2=IntGenerator(), length=8)
    def testShuffle(self, df):
        result = df.copy().shuffle(0).orderBy("c2")
        expected = df.copy().orderBy("c2")
        self.assertEqual(result, expected)

    @WithDataFrame.data(c0=[1, 2, 3], c1=["aa", "aa", "bb"])
    def testFilter(self, df):
        result = df.filter((df["c0"] > 1) & (df["c1"] == "aa"))
        expected = DataFrame({"c0": [2], "c1": ["aa"]})
        self.assertEqual(result, expected, msg=f"\n{result}")

    @WithDataFrame.data(c0=[0, 1, 2], c1=["aa", "bb", "cc"])
    def testFilterGreaterOrEqual(self, df):
        result = df.filter(df["c0"] >= 1)
        expected = DataFrame({"c0": [1, 2], "c1": ["bb", "cc"]})
        self.assertEqual(result, expected, msg=f"\n{result}")

    @WithDataFrame.data(
        c0=[1, 1, 2, 2], c1=["aa", "aa", "bb", "bb"], c2=[True, False, False, False]
    )
    def testGroupByMean(self, df):
        result = df.copy().groupBy("c1").aggregate(c0=Mean("c0"))
        expected = DataFrame({"c0": [1.0, 2.0], "c1": ["aa", "bb"]})
        self.assertEqual(result, expected)

        with self.assertRaises(RuntimeError):
            df.copy().groupBy("c1").aggregate(c0=Mean("c1"))

    @WithDataFrame.data(c0=[1, 1, 2, 2], c1=["aa", "aa", "bb", "bb"])
    def testGroupBySum(self, df):
        df.groupBy("c1").aggregate(c0=Sum("c0"))
        expected = DataFrame({"c0": [2, 4], "c1": ["aa", "bb"]})
        self.assertEqual(df, expected)

    @WithDataFrame.data(c0=[1, 2, 2, 1], c1=["aa", "aa", "bb", "bb"])
    def testGroupByMin(self, df):
        df.groupBy("c1").aggregate(c0=Min("c0"))
        expected = DataFrame({"c0": [1, 1], "c1": ["aa", "bb"]})
        self.assertEqual(df, expected)

    @WithDataFrame.data(c0=[1, 2, 2, 1], c1=["aa", "aa", "bb", "bb"])
    def testGroupByMax(self, df):
        df.groupBy("c1").aggregate(c0=Max("c0"))
        expected = DataFrame({"c0": [2, 2], "c1": ["aa", "bb"]})
        self.assertEqual(df, expected)

    @WithDataFrame.data(c0=[2, 2, 2, 1], c1=["aa", "aa", "bb", "bb"])
    def testGroupByCount(self, df):
        df.groupBy("c1").aggregate(c0=Count())
        expected = DataFrame({"c0": [2, 2], "c1": ["aa", "bb"]})
        self.assertEqual(df, expected)

    @WithDataFrame.data(c0=[True, False, True, True], c1=["aa", "aa", "bb", "bb"])
    def testGroupByAll(self, df):
        df.groupBy("c1").aggregate(c0=All("c0"))
        expected = DataFrame({"c0": [False, True], "c1": ["aa", "bb"]})
        self.assertEqual(df, expected)

    @WithDataFrame.data(c0=[True, False, False, False], c1=["aa", "aa", "bb", "bb"])
    def testGroupByAny(self, df):
        df.groupBy("c1").aggregate(c0=Any("c0"))
        expected = DataFrame({"c0": [True, False], "c1": ["aa", "bb"]})
        self.assertEqual(df, expected)

    @WithDataFrame.data(c0=[1, 1, 2, 2])
    def testMultiAggregation(self, df):
        result = (
            df.copy()
            .groupBy("c0")
            .aggregate(
                mean=Mean("c0"),
                count=Count(),
            )
        )
        expected = DataFrame({"mean": [1.0, 2.0], "count": [2, 2]})
        self.assertEqual(result[["mean", "count"]], expected)

    @WithDataFrame.data(
        c0=[1, 2, 2, 2], c1=["a", "a", "b", "b"], c2=[True, False, False, False]
    )
    def testDirectAggregation(self, df):
        def assertEquals(result, expected: list):
            expected = DataFrame({"c": expected})["c"]
            self.assertTrue(result.equals(expected))

        assertEquals(df.copy().groupBy("c1").mean("c0"), [1.5, 2.0])
        assertEquals(df.copy().groupBy("c1").sum("c0"), [3, 4])
        assertEquals(df.copy().groupBy("c1").min("c0"), [1, 2])
        assertEquals(df.copy().groupBy("c1").max("c0"), [2, 2])
        assertEquals(df.copy().groupBy("c1").count(), [2, 2])
        assertEquals(df.copy().groupBy("c1").all("c2"), [False, False])
        assertEquals(df.copy().groupBy("c1").any("c2"), [True, False])

    @WithDataFrame.data(c0=[1, 2], c1=["a", "b"])
    def testRename(self, df):
        original_names = list(df.names)
        renaming = {k: f"{k}_test" for k in original_names}
        df.rename(**renaming)
        # Verify all columns were renamed
        for name in original_names:
            self.assertNotIn(name, df.names)
            self.assertIn(f"{name}_test", df.names)

    @WithDataFrame.data(c0=[1, 2], c_lhs=["lhs", "lhs"])
    def testJoin(self, lhs_df):
        rhs = {"c0": [1, 3], "c_rhs": ["rhs", "rhs"]}
        expected = {"c0": [1], "c_lhs": ["lhs"], "c_rhs": ["rhs"]}
        rhs_df = DataFrame(
            rhs, executor=lhs_df._executor_, planFactory=lhs_df._planFactory_
        )
        self.assertEqual(DataFrame(expected), lhs_df.join(rhs_df))

    @WithDataFrame.random(
        c0=IntGenerator(),
        c1=StringGenerator(),
        c2=IntGenerator(),
        c3=FloatGenerator(),
        length=8,
        runtimes=Runtime.minimal(),
    )
    def testArrow(self, df):
        for t in (pa.Table, pa.RecordBatch, pa.Array, pa.StructArray):
            arrowTable = df.toArrow(dtype=t)
            dataframe = df.fromArrow(arrowTable)
            self.assertEqual(df, dataframe)

    def testFromArrowZeroRowTablePreservesSchema(self):
        # ``Table.to_struct_array()`` / ``RecordBatch.to_struct_array()``
        # raise ``ArrowInvalid: cannot construct ChunkedArray from
        # empty vector and omitted type`` on zero-row inputs because
        # pyarrow can't infer the chunk type from an empty list of
        # chunks. ``DataFrame.fromArrow`` builds an explicit empty
        # StructArray from the schema so the column types survive.
        # This case fires routinely in distributed plans where hash
        # partitioning leaves some buckets empty.
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("tag", pa.string()),
                pa.field("value", pa.float64()),
            ]
        )
        emptyTable = pa.Table.from_pylist([], schema=schema)
        self.assertEqual(emptyTable.num_rows, 0)

        df = DataFrame(emptyTable)
        self.assertEqual(len(df), 0)
        self.assertEqual(df.names, ["id", "tag", "value"])
        self.assertEqual(
            [str(df.dtype.childAt(i)) for i in range(len(df.dtype))],
            ["BIGINT", "VARCHAR", "DOUBLE"],
        )

    def testFromArrowZeroRowRecordBatchPreservesSchema(self):
        # Same zero-row guard, but starting from a ``pa.RecordBatch``
        # rather than ``pa.Table``. Both code paths funnel through
        # ``DataFrame.fromArrow``'s ``to_struct_array`` branch.
        schema = pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.float32())])
        emptyBatch = pa.RecordBatch.from_pylist([], schema=schema)
        self.assertEqual(emptyBatch.num_rows, 0)

        df = DataFrame(emptyBatch)
        self.assertEqual(len(df), 0)
        self.assertEqual(df.names, ["a", "b"])
        self.assertEqual(
            [str(df.dtype.childAt(i)) for i in range(len(df.dtype))],
            ["INTEGER", "REAL"],
        )

    def testFromArrowZeroRowSingleColumnPreservesType(self):
        # Single-column zero-row table — minimal repro of the
        # to_struct_array crash. ``DataFrame.fromArrow`` must still
        # produce a DataFrame whose column dtype matches the schema.
        emptyTable = pa.Table.from_pylist(
            [], schema=pa.schema([pa.field("c0", pa.int64())])
        )

        df = DataFrame(emptyTable)
        self.assertEqual(len(df), 0)
        self.assertEqual(df.names, ["c0"])
        self.assertEqual(str(df.dtype.childAt(0)), "BIGINT")

    @WithDataFrame.factory(
        lambda pl, ex: DataFrame(pl.tpchGenerator(Nation), executor=ex, planFactory=pl)
    )
    def testTpch(self, df):
        self.assertGreater(len(df), 0)

    @WithDataFrame.random(
        c0=IntGenerator(),
        c1=StringGenerator(),
        c2=IntGenerator(),
        c3=FloatGenerator(),
        length=8,
    )
    def testWriteRead(self, df):
        tableDir = "testWriteRead"
        LocalTableWriter(tableDir).write(df.copy())
        tableFile = os.path.join(tableDir, os.listdir(tableDir)[0])
        readDf = LocalTableReader(
            f"file:{tableFile}",
            df.dtype,
            pybolt.FileFormat.PARQUET,
        ).read()
        self.assertEqual(df, readDf)
        shutil.rmtree(tableDir)

    @WithDataFrame.random(
        c0=IntGenerator(),
        c1=StringGenerator(),
        c2=IntGenerator(),
        c3=FloatGenerator(),
        length=8,
    )
    def testArrowRead(self, df):
        tableDir = "testArrowRead"
        LocalTableWriter(tableDir).write(df.copy())
        tableFile = os.path.join(tableDir, os.listdir(tableDir)[0])
        readDf = DataFrame(
            executor=df._executor_, planFactory=df._planFactory_
        ).fromArrow(pq.read_table(tableFile))
        self.assertEqual(df, readDf)
        shutil.rmtree(tableDir)

    @WithDataFrame.random(
        c0=IntGenerator(),
        c1=StringGenerator(),
        c2=IntGenerator(),
        c3=FloatGenerator(),
        length=8,
    )
    def testDeepcopy(self, df):
        cpy = copy.deepcopy(df)
        self.assertTrue(df._data_ is not cpy._data_)
        self.assertEqual(df, cpy)

    def testArrowAdapterStructArray(self):
        dtype = pybolt.RowType(
            ["c0", "c1", "c2"],
            [pybolt.IntegerType(), pybolt.VarcharType(), pybolt.BigintType()],
        )

        @dataframeFunction(dtype)
        @arrowAdapter
        def multiplyByStructArray(df, value):
            ncol = df.type.num_fields
            arrays = [df.field(i) for i in range(ncol)]
            arrays[2] = pc.multiply(arrays[2], value)
            fields = [df.type.field(i) for i in range(ncol)]
            arrowResult = pa.StructArray.from_arrays(arrays=arrays, fields=fields)
            return arrowResult

        @WithDataFrame.dtype(dtype, length=8)
        def run(self, df):
            result = df.copy().map(multiplyByStructArray, 2)
            expected = df.copy().transform(c2=lambda df: df["c2"] * 2)
            self.assertEqual(result, expected)

        run(self)

    def testArrowAdapterRecordBatch(self):
        dtype = pybolt.RowType(
            ["c0", "c1", "c2"],
            [pybolt.IntegerType(), pybolt.VarcharType(), pybolt.BigintType()],
        )

        @dataframeFunction(dtype)
        @arrowAdapter(dtype=pa.RecordBatch)
        def multiplyByRecordBatch(df, value):
            arrays = df.columns
            arrays[2] = pc.multiply(arrays[2], value)
            return pa.RecordBatch.from_arrays(arrays, schema=df.schema)

        @WithDataFrame.dtype(dtype, length=8)
        def run(self, df):
            result = df.copy().map(multiplyByRecordBatch, 2)
            expected = df.copy().transform(c2=lambda df: df["c2"] * 2)
            self.assertEqual(result, expected)

        run(self)

    def testMap(self):
        dtype = pybolt.RowType(
            ["c0", "c1", "c2", "c3"],
            [
                pybolt.IntegerType(),
                pybolt.VarcharType(),
                pybolt.BigintType(),
                pybolt.DoubleType(),
            ],
        )

        @dataframeFunction(dtype)
        def dataframeAddValue(df, col, value):
            df[col] += value
            return df

        @WithDataFrame.dtype(dtype, length=8)
        def run(self, df):
            # Test execution on materialized dataframe
            result = df.copy().map(dataframeAddValue, "c3", 2.0)
            expected = df.copy().transform(c3=lambda df: df["c3"] + 2.0)
            self.assertEqual(result, expected)

        run(self)
