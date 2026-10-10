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

from typing import Callable
import unittest

import pyarrow
import pyarrow.compute as pc
import pybolt
from pybolt import BigintType, DoubleType


from ..dataframe import DataFrame
from ..expression.base import Expression
from ..function.aggregation import Mean
from ..function.vector import functionRegister as vectorFunctions
from ..function.stateful import (
    StatefulDataFrameFunction,
    StatefulVectorFunction,
    StatefulScalarFunction,
    StatefulMapBatchFunction,
)
from ..function import (
    Aggregator,
    arrowAdapter,
    mapBatchFunction,
    scalarFunction,
    vectorFunction,
    dataframeFunction,
)

from .utils import WithDataFrame, Runtime

executors = Runtime.executors(Runtime.all())
data = {
    "c0": [1, None, 2, 3, 4],
    "c1": [1, 1, 2, 3, 2],
    "c2": [1, 1, 1, 1, 1],
    "c3": [1.0, 1.0, 1.0, 1.0, 1.0],
}


class TestPythonUDF(unittest.TestCase):
    @WithDataFrame.data(**data)
    def __testExpressions(
        self,
        df: DataFrame,
        lhs: Callable[[DataFrame], Expression],
        rhs: Callable[[DataFrame], Expression],
    ):
        lhs_result = df.copy().transform(result=lhs)
        rhs_result = df.copy().transform(result=rhs)
        self.assertTrue(lhs_result["result"].equals(rhs_result["result"]))

    def testVectorFunctionSingleArg(self):
        @vectorFunction(pybolt.BigintType(), executors)
        def multiplyByTwo(a):
            arr = pybolt.exportToArrow(a)
            result = pc.multiply(arr, 2)
            return pybolt.importFromArrow(result)

        self.__testExpressions(
            lambda df: df["c0"].map(multiplyByTwo), lambda df: df["c0"] * 2
        )

    def testVectorFunctionDefaultArg(self):
        @vectorFunction(pybolt.BigintType(), executors)
        def addAb(a, b=3):
            for i in range(len(a)):
                if a[i] is not None:
                    a[i] = a[i] + b
            return a

        self.__testExpressions(
            lambda df: df["c0"] + 3,
            lambda df: df["c0"].map(addAb),
        )
        self.__testExpressions(
            lambda df: df["c0"] + 10,
            lambda df: df["c0"].map(addAb, 10),
        )

    def testScalarFunction(self):
        @scalarFunction(pybolt.BigintType(), executors)
        def addAbScalar(a, b=3):
            return a + b if (a is not None and b is not None) else None

        self.__testExpressions(
            lambda df: df["c0"].map(addAbScalar),
            lambda df: df["c0"] + 3,
        )
        self.__testExpressions(
            lambda df: df["c0"].map(addAbScalar, 10),
            lambda df: df["c0"] + 10,
        )

    def testPyarrowFunctionDoubleArg(self):
        @vectorFunction(pybolt.BigintType(), executors)
        @arrowAdapter
        def addPyarrow(a, b):
            return pc.add(a, b)

        self.__testExpressions(
            lambda df: df["c0"] + df["c0"],
            lambda df: df["c0"].map(addPyarrow, df["c0"]),
        )

    def testAggregationFunction(self):
        class TestAvg(Aggregator):
            def __init__(self, initSum: int = 0, initCount: int = 0):
                self.__sum = initSum
                self.__count = initCount

            @staticmethod
            def initializer():
                return TestAvg(0, 0)

            @staticmethod
            def aggregate(col):
                sumVal = 0
                count = len(col)
                for val in (col[i] for i in range(count)):
                    if val is None:
                        return TestAvg(0, 0)
                    sumVal += val
                return TestAvg(sumVal, count)

            def accumulate(self, other):
                self.__sum += other.__sum
                self.__count += other.__count

            def reduce(self):
                return self.__sum / self.__count

            def outputType(self):
                return pybolt.DoubleType()

        avg = TestAvg()

        @WithDataFrame.data(**data)
        def run(self, df):
            # Second iteration attempts to register the function again and should
            # skip the registration because it has been done in the first iteration.
            # If it doesn't, it will error.
            for _ in range(2):
                result = df.copy().groupBy("c1").aggregate(result=avg(df["c0"]))
                expected = df.copy().groupBy("c1").aggregate(result=Mean("c0"))
                self.assertEqual(expected, result)

        run(self)

    def testPlanNodePythonFunction(self):
        @dataframeFunction(DataFrame(data).dtype)
        def addToFirstCol(df, value=1):
            col = df["c0"]
            for i in range(len(col)):
                if col[i] is not None:
                    col[i] = col[i] + value
            return df

        @WithDataFrame.data(**data)
        def run(self, df):
            self.assertEqual(
                df.copy().transform(c0=lambda df: df["c0"] + 1),
                df.copy().map(addToFirstCol),
            )
            self.assertEqual(
                df.copy().transform(c0=lambda df: df["c0"] + 4),
                df.copy().map(addToFirstCol, 4),
            )

        run(self)

    def testMapBatchFunctionSimple(self):
        @mapBatchFunction(pybolt.BigintType(), executors)
        def testBatch(rowVector):
            a = rowVector.childAt("c0")
            b = rowVector.childAt("c2")
            for i in range(len(a)):
                if a[i] is not None and b[i] is not None:
                    a[i] = a[i] + b[i]
            return a

        self.__testExpressions(
            lambda df: df["c0"] + df["c2"], lambda df: df["c0"].map(testBatch, df["c2"])
        )

    def testMapBatchFunctionRecordBatch(self):
        dtype = pybolt.RowType(["c3"], [DoubleType()])

        @mapBatchFunction(dtype, executors)
        @arrowAdapter(pyarrow.RecordBatch)
        def testBatchDouble(rb):
            tb = pyarrow.Table.from_struct_array(rb.to_struct_array())
            c3 = pc.add(tb.column(0), tb.column(0))
            rb = pyarrow.record_batch([c3.combine_chunks()], names=["c3"])
            return rb

        def extractFieldFromRow(dtype: pybolt.BoltType):
            def fn(col: pybolt.BaseVector, field: str) -> pybolt.BaseVector:
                return col.childAt(field[0])

            fn.__name__ = f"extractFieldFromRow_{dtype}"
            if fn not in vectorFunctions:
                return vectorFunction(dtype, executors)(fn)
            else:
                return fn

        self.__testExpressions(
            lambda df: df["c3"] + df["c3"],
            lambda df: df["c3"]
            .map(testBatchDouble)
            .map(extractFieldFromRow(DoubleType()), "c3"),
        )

        dtype = pybolt.RowType(["c1", "c2"], [pybolt.BigintType(), pybolt.BigintType()])

        @mapBatchFunction(dtype, executors)
        @arrowAdapter(pyarrow.RecordBatch)
        def testBatchMulti(rb):
            tb = pyarrow.Table.from_struct_array(rb.to_struct_array())
            c1 = pc.add(tb.column(0), tb.column(1)).combine_chunks()
            tb = tb.remove_column(0)
            tb = tb.add_column(0, "c1", c1)
            return pyarrow.RecordBatch.from_struct_array(
                tb.to_struct_array().combine_chunks()
            )

        @WithDataFrame.data(**data)
        def run(self, df):
            expected = df.copy().transform(c1=lambda df: df["c1"] + df["c2"])
            result = (
                df.copy()
                .transform(c1=lambda df: df["c1"].map(testBatchMulti, df["c2"]))
                .transform(c2=lambda df: df["c1"])
                .transform(
                    c2=lambda df: df["c2"].map(extractFieldFromRow(BigintType()), "c2"),
                    c1=lambda df: df["c1"].map(extractFieldFromRow(BigintType()), "c1"),
                )
            )
            self.assertEqual(expected, result)

        run(self)

    def testStatefulDataFrameFunction(self):
        class Multiply(StatefulDataFrameFunction):
            def __init__(self, val: int, executors):
                self.__val = val
                super().__init__(executors)

            def outputType(self):
                return pybolt.RowType(["c0"], [pybolt.BigintType()])

            def __call__(self, df, col: str):
                df[col] *= self.__val
                return df

        double = Multiply(2, executors)

        @WithDataFrame.data(c0=[1])
        def run(self, df):
            lhs = df.copy().map(double, "c0")
            rhs = df.copy().transform(c0=lambda df: df["c0"] * 2)
            self.assertEqual(lhs, rhs)

        run(self)

    def testStatefulVectorFunction(self):
        class Multiply(StatefulVectorFunction):
            def __init__(self, val: int, executors):
                self.__val = val
                super().__init__(executors)

            def outputType(self):
                return pybolt.BigintType()

            def __call__(self, col):
                for i in range(len(col)):
                    col[i] *= self.__val
                return col

        double = Multiply(2, executors)

        @WithDataFrame.data(c0=[1])
        def run(self, df):
            self.assertEqual(
                df.copy().transform(c0=lambda df: df["c0"].map(double)),
                df.copy().transform(c0=lambda df: df["c0"] * 2),
            )

        run(self)

    def testStatefulScalarFunction(self):
        class Multiply(StatefulScalarFunction):
            def __init__(self, val: int, executors):
                self.__val = val
                super().__init__(executors)

            def outputType(self):
                return pybolt.BigintType()

            def __call__(self, val):
                return val * self.__val

        double = Multiply(2, executors)

        @WithDataFrame.data(c0=[1])
        def run(self, df):
            self.assertEqual(
                df.copy().transform(c0=lambda df: df["c0"].map(double)),
                df.copy().transform(c0=lambda df: df["c0"] * 2),
            )

        run(self)

    def testStatefulMapBatchFunction(self):
        class Multiply(StatefulMapBatchFunction):
            def __init__(self, val: int, executors):
                self.__val = val
                super().__init__(executors)

            def outputType(self):
                return pybolt.BigintType()

            def __call__(self, rowVector):
                a = rowVector.childAt("c0")
                for i in range(len(a)):
                    if a[i] is not None:
                        a[i] = a[i] * self.__val
                return a

        double = Multiply(2, executors)

        @WithDataFrame.data(c0=[1, 2, 3])
        def run(self, df):
            self.assertEqual(
                df.copy().transform(c0=lambda df: df["c0"].map(double)),
                df.copy().transform(c0=lambda df: df["c0"] * 2),
            )

        run(self)

    def testStatefulFunctionUnregistered(self):
        class UnregisteredFunction(StatefulDataFrameFunction):
            def __init__(self):
                pass

            def outputType(self):
                return pybolt.RowType([], [])

            def __call__():
                pass

        with self.assertRaises(RuntimeError):
            UnregisteredFunction()
