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

"""Public API regressions checked against independent expected values."""

import operator
import unittest

import cloudpickle
import pyarrow as pa
import pybolt

from boltml import DataFrame
from boltml.expression.function import FunctionExpression
from boltml.expression.literal import LiteralExpression
from boltml.function import Aggregator, scalarFunction
from boltml.function.aggregation import Count, Sum
from boltml.function.stateful import StatefulScalarFunction
from boltml.plan_builder.bolt import BoltPlanBuilderFactory
from boltml.substrait.extensions import ExtensionRegister, extensionIndex
from boltml.substrait.signature import substraitSignature


class ApiCorrectnessTests(unittest.TestCase):
    def rows(self, frame):
        return frame.toArrow(pa.Table).to_pylist()

    def testCountStarIncludesNullsAndColumnCountExcludesNulls(self):
        frame = DataFrame(pa.table({"v": pa.array([None, 7, None], type=pa.int64())}))
        frame.groupBy().aggregate(rows=Count(), values=Count("v"))
        self.assertEqual(self.rows(frame), [{"rows": 3, "values": 1}])

    def testCountsOnEmptyInputAreZero(self):
        frame = DataFrame(pa.table({"v": pa.array([], type=pa.int64())}))
        frame.groupBy().aggregate(rows=Count(), values=Count("v"))
        self.assertEqual(self.rows(frame), [{"rows": 0, "values": 0}])

    def testDictionaryConstantsBroadcastAndPreserveColumnOrder(self):
        frame = DataFrame({"label": "ok", "id": [1, 2], "payload": b"raw"})
        self.assertEqual(frame.names, ["label", "id", "payload"])
        self.assertEqual(
            self.rows(frame),
            [
                {"label": "ok", "id": 1, "payload": b"raw"},
                {"label": "ok", "id": 2, "payload": b"raw"},
            ],
        )

    def testDictionaryConstantBetweenVectorsPreservesColumnOrder(self):
        frame = DataFrame({"left": [1, 2], "label": "ok", "right": [3, 4]})
        self.assertEqual(frame.names, ["left", "label", "right"])
        self.assertEqual(frame[0, 1], "ok")
        table = frame.toArrow(pa.Table)
        self.assertEqual(table.schema.names, ["left", "label", "right"])
        self.assertEqual(
            table.to_pylist(),
            [
                {"left": 1, "label": "ok", "right": 3},
                {"left": 2, "label": "ok", "right": 4},
            ],
        )

    def testDictionaryOnlyConstantsMakesOneRow(self):
        frame = DataFrame({"label": "ok", "payload": b"raw", "n": 2})
        self.assertEqual(frame.names, ["label", "payload", "n"])
        self.assertEqual(self.rows(frame), [{"label": "ok", "payload": b"raw", "n": 2}])

    def testCapturedColumnTracksSelectionReordering(self):
        frame = DataFrame({"a": [1, 2], "b": [10, 20]})
        captured = frame["a"]
        frame.select(["b", "a"]).transform(c=captured * 2)
        self.assertEqual(
            self.rows(frame),
            [
                {"b": 10, "a": 1, "c": 2},
                {"b": 20, "a": 2, "c": 4},
            ],
        )

    def testCapturedRemovedColumnFailsClearly(self):
        frame = DataFrame({"a": [1], "b": [10]})
        captured = frame["a"]
        frame.select(["b"])
        with self.assertRaisesRegex(ValueError, "Field 'a'.*not found"):
            frame.transform(c=captured * 2).toArrow(pa.Table)

    def testFilterRejectsAnotherDataframesExpression(self):
        frame = DataFrame({"a": [1, 2]})
        other = DataFrame({"a": [9, 8]})
        with self.assertRaisesRegex(ValueError, "another dataframe"):
            frame.filter(other["a"] > 8)
        self.assertEqual(self.rows(frame), [{"a": 1}, {"a": 2}])

    def testRightAndFullJoinPreserveUnmatchedKeysAndRightInput(self):
        cases = (
            (
                pybolt.JoinType.kRight,
                [{"key": 2, "v": 20, "w": "b"}, {"key": 3, "v": None, "w": "c"}],
            ),
            (
                pybolt.JoinType.kFull,
                [
                    {"key": 1, "v": 10, "w": None},
                    {"key": 2, "v": 20, "w": "b"},
                    {"key": 3, "v": None, "w": "c"},
                ],
            ),
        )
        for joinType, expected in cases:
            with self.subTest(joinType=joinType):
                left = DataFrame({"key": [1, 2], "v": [10, 20]})
                right = DataFrame({"key": [2, 3], "w": ["b", "c"]})
                left.join(right, {"key"}, joinType)
                self.assertCountEqual(self.rows(left), expected)
                self.assertEqual(right.names, ["key", "w"])
                self.assertEqual(
                    self.rows(right), [{"key": 2, "w": "b"}, {"key": 3, "w": "c"}]
                )

    def testSelfJoinUsesIndependentInputPlans(self):
        frame = DataFrame({"key": [1, 1], "value": [10, 20]})
        frame.join(frame, {"key"})
        self.assertCountEqual(
            self.rows(frame),
            [
                {"key": 1, "l_value": 10, "r_value": 10},
                {"key": 1, "l_value": 10, "r_value": 20},
                {"key": 1, "l_value": 20, "r_value": 10},
                {"key": 1, "l_value": 20, "r_value": 20},
            ],
        )

    def testFloatColumnTimesIntegerLiteralPreservesFractions(self):
        frame = DataFrame({"price": [1.5, 2.5]})
        frame.transform(left=frame["price"] * 2, right=2 * frame["price"])
        self.assertEqual(
            self.rows(frame),
            [
                {"price": 1.5, "left": 3.0, "right": 3.0},
                {"price": 2.5, "left": 5.0, "right": 5.0},
            ],
        )
        self.assertEqual(frame["left"].dtype, pybolt.DoubleType())

    def testIntegerDivisionPreservesFractionsAndReflectedOperandOrder(self):
        frame = DataFrame(pa.table({"x": pa.array([2, -4, None], type=pa.int32())}))
        frame.transform(quotient=frame["x"] / 4, reciprocal=1 / frame["x"])
        self.assertEqual(
            self.rows(frame),
            [
                {"x": 2, "quotient": 0.5, "reciprocal": 0.5},
                {"x": -4, "quotient": -1.0, "reciprocal": -0.25},
                {"x": None, "quotient": None, "reciprocal": None},
            ],
        )
        self.assertEqual(frame["quotient"].dtype, pybolt.DoubleType())
        self.assertEqual(frame["reciprocal"].dtype, pybolt.DoubleType())

    def testDivisionDoesNotRoundIntegersThroughFloat32(self):
        frame = DataFrame(
            pa.table(
                {
                    "large": pa.array([16777217], type=pa.int64()),
                    "one": pa.array([1.0], type=pa.float32()),
                }
            )
        )
        frame.transform(
            quotient=frame["large"] / frame["one"], reflected=16777217 / frame["one"]
        )
        self.assertEqual(list(frame["quotient"]), [16777217.0])
        self.assertEqual(list(frame["reflected"]), [16777217.0])

    def testIntegerOperandsPromoteInBothOrders(self):
        frame = DataFrame(
            pa.table(
                {
                    "small": pa.array([30000], type=pa.int16()),
                    "wide": pa.array([100000], type=pa.int32()),
                }
            )
        )
        frame.transform(
            left=frame["small"] + frame["wide"], right=frame["wide"] + frame["small"]
        )
        self.assertEqual(
            self.rows(frame),
            [{"small": 30000, "wide": 100000, "left": 130000, "right": 130000}],
        )
        self.assertEqual(frame["left"].dtype, pybolt.IntegerType())
        self.assertEqual(frame["right"].dtype, pybolt.IntegerType())

    def testTransformAcceptsLiteralExpression(self):
        frame = DataFrame({"a": [1, 2]})
        frame.transform(c=LiteralExpression(5))
        self.assertEqual(frame.names, ["a", "c"])
        self.assertEqual(self.rows(frame), [{"a": 1, "c": 5}, {"a": 2, "c": 5}])

    def testTransformRejectsUnsupportedValue(self):
        with self.assertRaisesRegex(TypeError, "Unsupported transform expression"):
            DataFrame({"a": [1]}).transform(c=object())

    def testAppendingToEmptyFrameDoesNotAliasSource(self):
        source = DataFrame({"a": [1]})
        result = DataFrame().append(source).append(DataFrame({"a": [2]}))
        self.assertEqual(self.rows(result), [{"a": 1}, {"a": 2}])
        self.assertEqual(self.rows(source), [{"a": 1}])
        source.append(DataFrame({"a": [3]}))
        self.assertEqual(self.rows(result), [{"a": 1}, {"a": 2}])

    def testContainsTreatsWildcardAndEscapeCharactersLiterally(self):
        for needle, values, expected in (
            ("a_b", ["a_b", "acb", "a__b"], ["a_b"]),
            ("%", ["10%", "100", "%"], ["10%", "%"]),
            ("\\", ["a\\b", "ab", "\\"], ["a\\b", "\\"]),
        ):
            with self.subTest(needle=needle):
                frame = DataFrame({"s": values})
                frame.filter(frame["s"].contains(needle))
                self.assertEqual(list(frame["s"]), expected)

    def testStringColumnsCompareByValue(self):
        for compare, expected in (
            (operator.eq, [0]),
            (operator.ne, [1, 2]),
            (operator.lt, [1]),
            (operator.le, [0, 1]),
            (operator.gt, [2]),
            (operator.ge, [0, 2]),
        ):
            with self.subTest(compare=compare.__name__):
                frame = DataFrame(
                    {"id": [0, 1, 2], "lhs": ["a", "a", "c"], "rhs": ["a", "b", "b"]}
                )
                frame.filter(compare(frame["lhs"], frame["rhs"]))
                self.assertEqual(list(frame["id"]), expected)

    def testStringOrdering(self):
        frame = DataFrame({"s": ["b", "aa", "a"]}).orderBy("s")
        self.assertEqual(list(frame["s"]), ["a", "aa", "b"])

    def testIntegerSumReturnsBigint(self):
        frame = DataFrame(pa.table({"v": pa.array([1, 2, None], type=pa.int32())}))
        frame.groupBy().aggregate(total=Sum("v"))
        self.assertEqual(self.rows(frame), [{"total": 3}])
        self.assertEqual(frame["total"].dtype, pybolt.BigintType())

    def testScalarUdfHonorsDeclaredTypesAndSurvivesCloudpickle(self):
        @scalarFunction(pybolt.IntegerType())
        def apiIntegerResult(value):
            return value + 1 if value is not None else None

        @scalarFunction(pybolt.RealType())
        def apiRealResult(value):
            return value * 0.5 if value is not None else None

        @scalarFunction(pybolt.IntegerType())
        def apiNullIntegerResult(value):
            return None

        cases = (
            (apiIntegerResult, pybolt.IntegerType(), [3, None, 5]),
            (apiRealResult, pybolt.RealType(), [1.0, None, 2.0]),
            (apiNullIntegerResult, pybolt.IntegerType(), [None, None, None]),
        )
        for fn, dtype, expected in cases:
            with self.subTest(function=fn.__name__):
                frame = DataFrame({"x": [2, None, 4]})
                frame.transform(result=frame["x"].map(fn))
                self.assertEqual(list(frame["result"]), expected)
                self.assertEqual(frame["result"].dtype, dtype)
                restored = cloudpickle.loads(cloudpickle.dumps(fn))
                output = restored(pybolt.fromList([2, None, 4]))
                self.assertEqual(list(output), expected)
                self.assertEqual(output.dtype(), dtype)

    def testStatefulScalarInstancesRemainIndependent(self):
        class ApiOffset(StatefulScalarFunction):
            def __init__(self, offset):
                self.offset = offset
                super().__init__()

            def outputType(self):
                return pybolt.BigintType()

            def __call__(self, value):
                return value + self.offset

        first, second = ApiOffset(10), ApiOffset(100)
        # ColumnView.map assigns in place, so each instance gets its own
        # input frame. Both registrations coexist before either execution.
        left = DataFrame({"x": [1, 2]})
        right = DataFrame({"x": [1, 2]})
        left["x"].map(first)
        right["x"].map(second)
        self.assertEqual(self.rows(left), [{"x": 11}, {"x": 12}])
        self.assertEqual(self.rows(right), [{"x": 101}, {"x": 102}])

    def testDifferentSameNamedAggregatorClassesDoNotShareRegistration(self):
        def makeAggregator(value):
            class SameName(Aggregator):
                @staticmethod
                def initializer():
                    return SameName()

                @staticmethod
                def aggregate(column):
                    return SameName()

                def accumulate(self, other):
                    pass

                def reduce(self):
                    return value

                def outputType(self):
                    return pybolt.BigintType()

            return SameName()

        for aggregate, expected in ((makeAggregator(7), 7), (makeAggregator(9), 9)):
            frame = DataFrame({"x": [1, 2]})
            frame.groupBy().aggregate(result=aggregate(frame["x"]))
            self.assertEqual(self.rows(frame), [{"result": expected}])

    def testExtensionAnchorsDistinguishEqualTextWithDifferentTypes(self):
        register = ExtensionRegister()
        narrow = FunctionExpression(
            "plus",
            pybolt.IntegerType(),
            [
                LiteralExpression(1, pybolt.IntegerType()),
                LiteralExpression(2, pybolt.IntegerType()),
            ],
        )
        wide = FunctionExpression(
            "plus",
            pybolt.BigintType(),
            [
                LiteralExpression(1, pybolt.BigintType()),
                LiteralExpression(2, pybolt.BigintType()),
            ],
        )
        self.assertEqual(narrow.expr(), wide.expr())
        self.assertNotEqual(
            extensionIndex(register[narrow]), extensionIndex(register[wide])
        )
        self.assertEqual(register[narrow].extension_function.name, "plus:i32_i32")
        self.assertEqual(register[wide].extension_function.name, "plus:i64_i64")

    def testDateAndTimestampSignaturesRetainTheirSemanticTypes(self):
        self.assertEqual(substraitSignature(pybolt.DateType()), "date")
        self.assertEqual(substraitSignature(pybolt.TimestampType()), "ts")

    def testSelectTailOffsetAndZeroCount(self):
        cases = (
            ({"offset": -2}, [3, 4]),
            ({"offset": -2, "count": 1}, [3]),
            ({"offset": -20}, [0, 1, 2, 3, 4]),
            ({"offset": 2}, [2, 3, 4]),
            ({"offset": 20}, []),
            ({"count": 0}, []),
            ({"offset": 2, "count": 0}, []),
        )
        for options, expected in cases:
            with self.subTest(options=options):
                frame = DataFrame({"x": [0, 1, 2, 3, 4]}).select(**options)
                self.assertEqual(list(frame["x"]), expected)

    def testSelectRejectsNegativeCount(self):
        with self.assertRaisesRegex(ValueError, "count must be non-negative"):
            DataFrame({"x": [1, 2]}).select(count=-1)

    def testNativeEmptySelectionsPreserveSchemaAndAggregation(self):
        primitive = pa.table({"id": [1, 2], "label": ["a", "b"]})
        nested = pa.table({"id": [1, 2], "payload": [{"value": 3}, {"value": 4}]})
        for data in (primitive, nested):
            for native in (False, True):
                for options in ({"count": 0}, {"count": 0, "offset": 1}, {"offset": 3}):
                    with self.subTest(
                        schema=data.schema, native=native, options=options
                    ):
                        kwargs = (
                            {"planFactory": BoltPlanBuilderFactory()} if native else {}
                        )
                        frame = DataFrame(data, **kwargs).select(**options)
                        # Copy replays the logical limit, including an empty range.
                        copied = frame.copy()
                        table = copied.toArrow(pa.Table)
                        self.assertEqual(table.to_pylist(), [])
                        self.assertEqual(table.schema, data.schema)
                        frame.groupBy().aggregate(rows=Count(), total=Sum("id"))
                        self.assertEqual(self.rows(frame), [{"rows": 0, "total": None}])


if __name__ == "__main__":
    unittest.main()
