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
import unittest

import pyarrow as pa
import pybolt
from pybolt.test.utils import StringGenerator, IntGenerator, FloatGenerator

from ..function import scalarFunction
from ..dataframe import DataFrame
from ..expression.case import If

from .utils import WithDataFrame, Runtime


class TestBoltColumn(unittest.TestCase):
    @WithDataFrame.data(c0=[1], runtimes=Runtime.minimal())
    def testName(self, df):
        self.assertEqual(df["c0"].name, "c0")

    @WithDataFrame.data(c0=[1], c1=[2.0], c3=["x"], runtimes=Runtime.minimal())
    def testType(self, df):
        for col in df:
            self.assertEqual(col.dtype, df.dtype.findChild(col.name))

    @WithDataFrame.data(c0=[1], runtimes=Runtime.minimal())
    def testLen(self, df):
        self.assertEqual(len(df["c0"]), 1)

    @WithDataFrame.data(c0=[1], runtimes=Runtime.minimal())
    def testGetitemScalar(self, df):
        self.assertEqual(df["c0"][0], 1)

    @WithDataFrame.data(c0=[1, 2, 3], runtimes=Runtime.minimal())
    def testGetitemSlice(self, df):
        for lhs, rhs in zip([2, 3], df["c0"][1:]):
            self.assertEqual(lhs, rhs)

    @WithDataFrame.data(c0=[1, 2, 3], runtimes=Runtime.minimal())
    def testSetitem(self, df):
        values = [v for v in df["c0"]]
        for i, v in enumerate(values):
            df["c0"][i] += 2
            self.assertEqual(df["c0"][i], v + 2)

    @WithDataFrame.random(
        c0=IntGenerator(),
        c1=FloatGenerator(),
        c3=StringGenerator(),
        runtimes=Runtime.minimal(),
    )
    def testEqColumn(self, df):
        for column in df:
            self.assertTrue(column.equals(column))
            self.assertTrue(column.equals(df.copy()[column.name]))

    @WithDataFrame.random(c0=IntGenerator(), runtimes=Runtime.minimal())
    def testDeepcopy(self, lhs):
        rhs = copy.deepcopy(lhs["c0"])
        self.assertTrue(lhs["c0"].equals(rhs))

    @WithDataFrame.data(flag=[True, None, False], items=[[1], None, []])
    def testCopyGenericColumnInitializesExpression(self, df):
        expected = {"flag": [True, None, False], "items": [[1], None, []]}
        for name in df.names:
            with self.subTest(column=name):
                original = df[name]
                copied = original.copy()
                self.assertIsNot(copied.dataframe, df)
                self.assertEqual(copied.dtype, original.dtype)
                copied.dataframe.transform(missing=copied.isNone())
                self.assertEqual(
                    list(copied.dataframe["missing"]), [False, True, False]
                )
                copied.map(lambda c: c.isNotNone())
                self.assertEqual(list(copied), [True, False, True])
                self.assertEqual(df.toArrow(pa.Table).to_pydict(), expected)

    @WithDataFrame.data(c0=[1], runtimes=Runtime.minimal())
    def testRename(self, df):
        names = list(df.names)
        for n in names:
            col = df[n]
            name = f"{n}_test"
            col.rename(name)
            self.assertEqual(col.name, name)
        self.assertTrue(
            all(
                n.endswith("_test") and n.replace("_test", "") in names
                for n in df.names
            )
        )

    @WithDataFrame.data(
        number=[1, None, 3], text=["hi", None, "world"], flag=[True, None, False]
    )
    def testRenameRefreshesColumnExpression(self, df):
        cases = (
            ("number", lambda c: c + 1, [2, None, 4]),
            ("text", lambda c: c.uppercase(), ["HI", None, "WORLD"]),
            ("flag", lambda c: c.isNone(), [False, True, False]),
        )
        for name, expression, expected in cases:
            with self.subTest(column=name):
                column = df[name]
                renamed = f"new_{name}"
                self.assertIs(column.rename(renamed), column)
                column.map(expression)
                self.assertEqual(column.name, renamed)
                self.assertNotIn(name, df.names)
                self.assertEqual(list(column), expected)
                self.assertEqual(list(df[renamed]), expected)

    def testRenamePreservesEarlierMappedExpressions(self):
        runtimes = Runtime.all()

        @scalarFunction(pybolt.BigintType(), Runtime.executors(runtimes))
        def increment(value):
            return None if value is None else value + 1

        for runtime in runtimes:
            for mapping in (increment, lambda c: c + 1, None):
                with self.subTest(runtime=str(runtime), mapping=mapping):
                    df = DataFrame(
                        {"a": [1, None, 3]},
                        executor=runtime.executor,
                        planFactory=runtime.planFactory,
                    )
                    column = df["a"]
                    column.map(column + 1 if mapping is None else mapping)
                    column.rename("b").map(lambda c: c + 1)
                    copied = df.copy()
                    self.assertEqual(
                        copied.toArrow(pa.Table).to_pydict(), {"b": [3, None, 5]}
                    )
                    self.assertEqual(
                        df.toArrow(pa.Table).to_pydict(), {"b": [3, None, 5]}
                    )

    @WithDataFrame.data(a=[1, None, 3])
    def testRenamePreservesEarlierProjectionExpressions(self, df):
        column = df["a"]
        df.transform(
            alias=column,
            derived=column + 1,
            divided=column / 2,
            matched=If(column > 1).then(column).otherwise(0),
            unmatched=If(column > 1).then(0).otherwise(column),
        )
        column.rename("b").map(lambda c: c + 2)
        expected = {
            "b": [3, None, 5],
            "alias": [1, None, 3],
            "derived": [2, None, 4],
            "divided": [0.5, None, 1.5],
            "matched": [0, 0, 3],
            "unmatched": [1, None, 0],
        }
        self.assertEqual(df.copy().toArrow(pa.Table).to_pydict(), expected)
        self.assertEqual(df.toArrow(pa.Table).to_pydict(), expected)

    def testMapUdf(self):
        runtimes = Runtime.all()
        executors = Runtime.executors(runtimes)

        @scalarFunction(pybolt.BigintType(), executors)
        def addValue(lhs, rhs):
            return lhs + rhs

        @WithDataFrame.data(c0=[1, 2], runtimes=runtimes)
        def run(self, df):
            res = df["c0"].copy().map(addValue, 1)
            for lhs, rhs in zip(df["c0"], res):
                self.assertEqual(lhs + 1, rhs)

        run(self)

    @WithDataFrame.data(c0=[1, 2])
    def testMapExprCallable(self, df):
        col = df["c0"]
        res = col.copy().map(lambda c: c + 1)
        for lhs, rhs in zip(col, res):
            self.assertEqual(lhs + 1, rhs)

        res = col.copy().map(lambda c, v: c + v, 1)
        for lhs, rhs in zip(col, res):
            self.assertEqual(lhs + 1, rhs)

    @WithDataFrame.data(c0=[None, 10])
    def testIsNone(self, df):
        result = df.copy()["c0"].map(lambda c: c.isNone())
        self.assertTrue(result.equals(DataFrame({"c0": [True, False]})["c0"]))

        result = df.copy()["c0"].map(lambda c: c.isNotNone())
        self.assertTrue(result.equals(DataFrame({"c0": [False, True]})["c0"]))

    @WithDataFrame.random(c0=FloatGenerator())
    def testArith(self, df):
        df["result"] = (df["c0"] + df["c0"] - df["c0"]) * 2.0 / 2.0
        self.assertTrue(df["c0"].cast(pybolt.DoubleType()).equals(df["result"]))

    @WithDataFrame.random(c0=FloatGenerator())
    def testIadd(self, df):
        col = df["c0"].copy()
        df["c0"] += 2.0
        for lhs, rhs in zip(df["c0"], col):
            self.assertEqual(lhs, rhs + 2.0)

    @WithDataFrame.random(c0=FloatGenerator())
    def testIaddSelf(self, df):
        col = df["c0"].copy()
        df["c0"] += df["c0"]
        for lhs, rhs in zip(df["c0"], col):
            self.assertEqual(lhs, rhs + rhs)

    @WithDataFrame.random(c0=FloatGenerator())
    def testImul(self, df):
        col = df["c0"].copy()
        df["c0"] *= 2.0
        for lhs, rhs in zip(df["c0"], col):
            self.assertEqual(lhs, rhs * 2.0)

    @WithDataFrame.random(c0=FloatGenerator())
    def testIdiv(self, df):
        col = df["c0"].copy()
        df["c0"] /= 2.0
        for lhs, rhs in zip(df["c0"], col):
            self.assertEqual(lhs, rhs / 2.0)

    @WithDataFrame.random(c0=StringGenerator())
    def testConcat(self, df):
        col = df["c0"].copy()
        df["c0"] = df["c0"].concat("_test")
        for lhs, rhs in zip(df["c0"], col):
            self.assertEqual(lhs, rhs + "_test")

    def testArithDataFrameMismatch(self):
        c0 = DataFrame({"c0": [1.0]})["c0"]
        c1 = DataFrame({"c0": [2.0]})["c0"]
        with self.assertRaises(RuntimeError):
            c2 = c0 + c1  # noqa: F841
        with self.assertRaises(RuntimeError):
            c2 = c0 - c1  # noqa: F841
        with self.assertRaises(RuntimeError):
            c2 = c0 * c1  # noqa: F841
        with self.assertRaises(RuntimeError):
            c2 = c0 / c1  # noqa: F841

    def testBooleanDataFrameMismatch(self):
        c0 = DataFrame({"c0": [1.0]})["c0"]
        c1 = DataFrame({"c0": [2.0]})["c0"]
        with self.assertRaises(RuntimeError):
            c2 = (c0 > 2.0) & (c1 < 2.0)  # noqa: F841
        with self.assertRaises(RuntimeError):
            c2 = (c0 > 2.0) | (c1 < 2.0)  # noqa: F841

    def testBooleanEvalError(self):
        c0 = DataFrame({"c0": [1.0]})["c0"]
        c1 = DataFrame({"c0": [2.0]})["c0"]
        with self.assertRaises(RuntimeError):
            c1 = (c0 > 2.0) and (c0 < 2.0)  # noqa: F841
        with self.assertRaises(RuntimeError):
            c1 = (c0 > 2.0) or (c0 < 2.0)  # noqa: F841

    @WithDataFrame.random(c0=StringGenerator())
    def testSearchRemove(self, df):
        col = df["c0"].copy()
        df["c0"] = df["c0"].concat("_test").searchReplace("_test")
        self.assertTrue(col.equals(df["c0"]))

    @WithDataFrame.random(c0=StringGenerator())
    def testReplaceString(self, df):
        col = df["c0"].copy()
        df["c0"] = df["c0"].concat("_test").searchReplace("_test", "_replaced")
        for lhs, rhs in zip(df["c0"], col):
            self.assertEqual(lhs, rhs + "_replaced")

    @WithDataFrame.random(c0=StringGenerator(characters="ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
    def testLowercase(self, df):
        col = df["c0"].copy()
        df["c0"] = df["c0"].lowercase()
        for lhs, rhs in zip(df["c0"], col):
            self.assertEqual(lhs, rhs.lower())

    @WithDataFrame.random(c0=StringGenerator(characters="abcdefghijklmnopqrstuvwxyz"))
    def testUppercase(self, df):
        col = df["c0"].copy()
        df["c0"] = df["c0"].uppercase()
        for lhs, rhs in zip(df["c0"], col):
            self.assertEqual(lhs, rhs.upper())

    @WithDataFrame.data(c0=["Hello", "World"])
    def testStringEqual(self, df):
        result = df["c0"].copy().map(lambda c: c == "Hello")
        self.assertTrue(result.equals(DataFrame({"c0": [True, False]})["c0"]))

        result = df["c0"].copy().map(lambda c: c != "Hello")
        self.assertTrue(result.equals(DataFrame({"c0": [False, True]})["c0"]))
