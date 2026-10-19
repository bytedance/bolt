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

from decimal import Decimal
import pickle
from pathlib import Path
import subprocess
import sys
import textwrap
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pybolt as pb


class _StringAccumulator:
    empty = ""

    def __init__(self, value):
        self.value = value

    @classmethod
    def initializer(cls):
        return cls(cls.empty)

    @classmethod
    def aggregate(cls, column):
        return cls(cls.empty.join(value for value in column if value is not None))

    def accumulate(self, other):
        self.value += other.value

    def reduce(self):
        return self.value


class _BinaryAccumulator(_StringAccumulator):
    empty = b""


class TestVectorConversions(unittest.TestCase):
    def testHugeintVectorsPreserveSigned128BitValues(self):
        values = [None, -(2**127), -(2**64) - 7, -1, 0, 2**64 + 7, 2**127 - 1]
        dtype = pb.HugeintType()
        vector = pb.fromList(values, dtype)
        self.assertEqual(list(vector), values)
        self.assertEqual(list(vector[1:]), values[1:])
        indices = [6, 0, 1, 5, 3]
        dictionary = pb.dictionaryVector(vector, indices)
        self.assertEqual(list(dictionary), [values[i] for i in indices])
        for value in values:
            with self.subTest(value=value):
                constant = pb.constantVector(value, 2, dtype)
                self.assertEqual(list(constant), [value, value])
                vector[0] = value
                self.assertEqual(vector[0], value)
                self.assertEqual(vector.dtype(), dtype)
        for index in (-1, len(vector)):
            with self.assertRaises(IndexError):
                vector[index]

    def testHugeintInputRangeAndCategoriesAreChecked(self):
        dtype = pb.HugeintType()
        vector = pb.fromList([None, 2**80], dtype)
        invalid = [
            (-(2**127) - 1, OverflowError),
            (2**127, OverflowError),
            ((2**127 - 1) * 10, OverflowError),
            (True, TypeError),
            (1.5, TypeError),
            ("123", TypeError),
        ]
        for value, error in invalid:
            with self.subTest(value=value):
                with self.assertRaises(error):
                    pb.fromList([value], dtype)
                with self.assertRaises(error):
                    pb.constantVector(value, 2, dtype)
                for index in (0, 1):
                    with self.assertRaises(error):
                        vector[index] = value
                    self.assertEqual(list(vector), [None, 2**80])

    def testLongDecimalsPreserveUnscaledValuesAndArrowType(self):
        for precision, scale in ((25, 4), (38, 18)):
            dtype = pb.LongDecimalType(precision, scale)
            limit = 10**precision - 1
            values = [None, -limit, -(2**64) - 7, 0, 2**64 + 7, limit]
            decimals = [
                None
                if value is None
                else Decimal((int(value < 0), tuple(map(int, str(abs(value)))), -scale))
                for value in values
            ]
            source = pa.array(decimals, pa.decimal128(precision, scale))
            for vector in (pb.fromList(values, dtype), pb.importFromArrow(source)):
                with self.subTest(dtype=dtype, encoding=type(vector).__name__):
                    dictionary = pb.dictionaryVector(vector, [5, 0, 1, 4])
                    self.assertEqual(list(vector), values)
                    self.assertEqual(
                        list(dictionary), [values[i] for i in [5, 0, 1, 4]]
                    )
                    self.assertEqual(pb.exportToArrow(vector), source)
                    self.assertEqual(
                        pb.exportToArrow(dictionary).to_pylist(),
                        [decimals[i] for i in [5, 0, 1, 4]],
                    )
                    # Arrow imports may reference read-only buffers. Own the
                    # storage before testing mutation, as for other types.
                    mutable = vector.copy()
                    for value, decimal in zip(values, decimals):
                        constant = pb.constantVector(value, 2, dtype)
                        self.assertEqual(list(constant), [value, value])
                        self.assertEqual(
                            pb.exportToArrow(constant),
                            pa.array([decimal, decimal], source.type),
                        )
                        mutable[0] = value
                        self.assertEqual(mutable[0], value)
                        self.assertEqual(mutable.dtype(), dtype)

    def testLongDecimalPrecisionIsCheckedBeforeMutation(self):
        dtype = pb.LongDecimalType(25, 4)
        vector = pb.fromList([None, 10**25 - 1], dtype)
        for value in (-(10**25), 10**25, 2**127 - 1):
            with self.subTest(value=value):
                with self.assertRaisesRegex(OverflowError, "DECIMAL precision"):
                    pb.fromList([value], dtype)
                with self.assertRaisesRegex(OverflowError, "DECIMAL precision"):
                    pb.constantVector(value, 2, dtype)
                for index in (0, 1):
                    with self.assertRaisesRegex(OverflowError, "DECIMAL precision"):
                        vector[index] = value
                    self.assertEqual(list(vector), [None, 10**25 - 1])

    def testNestedLongDecimalsPreserveUnscaledValues(self):
        decimal = pb.LongDecimalType(25, 4)
        value = 10**24 + 7
        for values, dtype in (
            ([[value, None, -value], []], pb.ArrayType(decimal)),
            (
                [{"x": value, "y": None}, {"x": -value}],
                pb.MapType(pb.VarcharType(), decimal),
            ),
        ):
            with self.subTest(dtype=dtype):
                vector = pb.fromList(values, dtype)
                self.assertEqual(list(vector), values)
                vector[1] = values[0]
                self.assertEqual(vector[1], values[0])

    def testNarrowNumericAssignmentPreservesTypeAndNulls(self):
        for dtype, value in (
            (pb.TinyintType(), 7),
            (pb.SmallintType(), 1000),
            (pb.IntegerType(), 100000),
            (pb.RealType(), 1.25),
        ):
            with self.subTest(dtype=dtype):
                vector = pb.fromList([None], dtype)
                vector[0] = value
                self.assertEqual(vector[0], value)
                self.assertEqual(vector.dtype(), dtype)
                vector[0] = None
                self.assertIsNone(vector[0])
                vector[0] = -value
                self.assertEqual(vector[0], -value)
                self.assertEqual(vector.dtype(), dtype)

    def testNarrowIntegerAssignmentChecksBounds(self):
        for dtype, bits in (
            (pb.TinyintType(), 8),
            (pb.SmallintType(), 16),
            (pb.IntegerType(), 32),
        ):
            minimum, maximum = -(2 ** (bits - 1)), 2 ** (bits - 1) - 1
            vector = pb.fromList([None, 7], dtype)
            for value in (minimum, maximum):
                with self.subTest(dtype=dtype, value=value):
                    vector[1] = value
                    self.assertEqual(vector[1], value)
            for value in (minimum - 1, maximum + 1):
                for index in (0, 1):
                    with self.subTest(dtype=dtype, value=value, index=index):
                        with self.assertRaises(RuntimeError):
                            vector[index] = value
                        self.assertIsNone(vector[0])
                        self.assertEqual(vector[1], maximum)

    def testScalarAssignmentRejectsIncompatibleCategories(self):
        for dtype, initial, incompatible in (
            (pb.TinyintType(), 1, (True, 1.5, "1", [1], {"x": 1})),
            (pb.SmallintType(), 1, (True, 1.5)),
            (pb.IntegerType(), 1, (True, 1.5)),
            (pb.RealType(), 1.0, (True, "1.0", [1.0], {"x": 1.0})),
            (pb.BooleanType(), True, (1, 1.0)),
        ):
            vector = pb.fromList([initial], dtype)
            for value in incompatible:
                with self.subTest(dtype=dtype, value=value):
                    with self.assertRaises(TypeError):
                        vector[0] = value
                    self.assertEqual(vector[0], initial)
                    self.assertEqual(vector.dtype(), dtype)

    def testStringAndBinaryConstants(self):
        for value in ("text constant" * 3, b"binary\x00constant" * 3):
            with self.subTest(value=value):
                vector = pb.constantVector(value, 2)
                self.assertEqual([vector[0], vector[1]], [value, value])
                self.assertEqual(pb.exportToArrow(vector).to_pylist(), [value, value])

    def testInferArrayTypesAcrossEmptyRows(self):
        for values in (
            [[1], []],
            [[], [1]],
            [[None], [1], []],
            [[], [None], [1]],
            [[[], [None]], [[1]]],
            [{"a": []}, {"b": [1]}],
        ):
            with self.subTest(values=values):
                vector = pb.fromList(values)
                self.assertEqual([vector[i] for i in range(len(vector))], values)

    def testInferenceRejectsUnknownOrMixedElementTypes(self):
        for values in ([[], []], [[None], []], [{"a": []}]):
            with self.subTest(values=values), self.assertRaises(ValueError):
                pb.fromList(values)
        for values in ([[], 1], [[1], ["a"]], [{}, []]):
            with self.subTest(values=values), self.assertRaises(TypeError):
                pb.fromList(values)

    def testNestedBinaryAndAssignment(self):
        for values, dtype in (
            ([[b"\x00a"], [], None], pb.ArrayType(pb.VarbinaryType())),
            (
                [{"a": b"\x00a"}, {}, None],
                pb.MapType(pb.VarcharType(), pb.VarbinaryType()),
            ),
        ):
            with self.subTest(dtype=dtype):
                vector = pb.fromList(values, dtype)
                self.assertEqual([vector[i] for i in range(len(vector))], values)
                vector[1] = values[0]
                self.assertEqual(vector[1], values[0])
        vector = pb.fromList([b"before"])
        vector[0] = b"after\x00"
        self.assertEqual(vector[0], b"after\x00")

    def testConstantAndDictionaryArrowExportSupportsParquet(self):
        vector = pb.rowVector(
            ["constant", "dictionary"],
            [
                pb.constantVector(7, 3),
                pb.dictionaryVector(pb.fromList([10, 20]), [0, 1, 0]),
            ],
        )
        table = pa.Table.from_struct_array(pb.exportToArrow(vector))
        self.assertEqual(table.schema.field("constant").type, pa.int64())
        self.assertEqual(table.schema.field("dictionary").type, pa.int64())
        output = pa.BufferOutputStream()
        pq.write_table(table, output)
        restored = pq.read_table(pa.BufferReader(output.getvalue()))
        self.assertEqual(restored.to_pydict(), table.to_pydict())

    def testEquivalentNumpyDtypes(self):
        source = np.array([[1.0, 2.0]], dtype=np.float32)
        for array in (
            pickle.loads(pickle.dumps(source)),
            source.astype(np.dtype(np.float32, metadata={"unit": "feature"})),
        ):
            with self.subTest(dtype=array.dtype):
                vector = pb.fromNumpy(array, pb.RealType(), [2])
                np.testing.assert_array_equal(vector.to_numpy([2]), source)

    def testBooleanNumpyBridgeRejectsUnsupportedLayout(self):
        with self.assertRaises(TypeError):
            pb.fromNumpy(np.array([[True, False]]), pb.BooleanType(), [2])
        vector = pb.fromList([[True, False]], pb.ArrayType(pb.BooleanType()))
        with self.assertRaises(TypeError):
            vector.to_numpy([2])

    def testTensorNullElementsRequireExplicitFill(self):
        vector = pb.fromList([[1, None]], pb.ArrayType(pb.BigintType()))
        with self.assertRaisesRegex(ValueError, "null tensor elements"):
            vector.to_numpy([2])

    def testTensorShapeMatchesElements(self):
        vector = pb.fromList([[1, 2]], pb.ArrayType(pb.BigintType()))
        with self.assertRaises(ValueError):
            vector.to_numpy([3])

    def testEmptyTensorRoundTrip(self):
        source = np.empty((0, 2), dtype=np.int32)
        vector = pb.fromNumpy(source, pb.IntegerType(), [2])
        self.assertEqual(vector.to_numpy([2]).shape, (0, 2))
        vector = pb.fromList([], pb.ArrayType(pb.IntegerType()))
        self.assertEqual(vector.to_numpy([2]).shape, (0, 2))


class TestExecutionBindings(unittest.TestCase):
    def execute(self, builder):
        return pb.BoltTaskExecutor().execute(builder.planFragment(), self.id())

    def assertScriptExitsCleanly(self, script):
        # Use the same package as this test, including an editable install.
        packageRoot = str(Path(pb.__file__).parent.parent)
        script = f"import sys; sys.path.insert(0, {packageRoot!r})\n" + textwrap.dedent(
            script
        )
        result = subprocess.run(
            [sys.executable, "-I", "-B", "-c", script],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("Memory leak", result.stderr)

    def testModuleGlobalsExitCleanlyWithRegisteredUdf(self):
        self.assertScriptExitsCleanly(
            """
            import pybolt
            from boltml import DataFrame
            from boltml.function import scalarFunction

            frame = DataFrame({"x": [1, None, 3]})

            @scalarFunction(pybolt.BigintType())
            def addOne(value):
                return None if value is None else value + 1

            frame.transform(result=lambda df: df["x"].map(addOne))
            assert list(frame["result"]) == [2, None, 4]
            """
        )

    def testModuleGlobalsExitCleanlyWithUnusedDataframeFunction(self):
        self.assertScriptExitsCleanly(
            """
            from boltml import DataFrame
            from boltml.function import dataframeFunction

            frame = DataFrame({"x": [1, 2]})

            @dataframeFunction(frame.dtype)
            def identityFrame(source):
                return source

            assert list(frame["x"]) == [1, 2]
            """
        )

    def testModuleGlobalsExitCleanlyWithAggregator(self):
        self.assertScriptExitsCleanly(
            """
            import pybolt
            from boltml import DataFrame
            from boltml.function.aggregation import Aggregator

            class Total(Aggregator):
                def __init__(self, value=0):
                    self.value = value

                @staticmethod
                def initializer():
                    return Total()

                @staticmethod
                def aggregate(values):
                    return Total(sum(value for value in values if value is not None))

                def accumulate(self, other):
                    self.value += other.value

                def reduce(self):
                    return self.value

                def outputType(self):
                    return pybolt.BigintType()

            frame = DataFrame({"key": [1, 1], "x": [2, 3]})
            result = frame.groupBy("key").aggregate(total=Total()(frame["x"]))
            assert list(result["total"]) == [5]
            """
        )

    def testEmptyHashKeysHaveDefinedResults(self):
        values = pb.rowVector(["x"], [pb.fromList([1, 2, 3])])
        self.assertEqual(pb.computePartitionIndices(values, [], 4), [0, 0, 0])
        self.assertEqual(pb.computeRowHashes(values, []), [0, 0, 0])
        self.assertEqual(pb.computePartitionIndices(values, [0], 1), [0, 0, 0])

    def testPlanBuilderFromNodeCanInferArrayLiteral(self):
        values = pb.rowVector(["x"], [pb.fromList([1])])
        node = pb.PlanBuilder().values([values]).planNode()
        result = self.execute(pb.PlanBuilder(node).project(["array[1, 2] as items"]))
        self.assertEqual(result.childAt("items")[0], [1, 2])

    def testUnsupportedFileSchemeIsRejected(self):
        values = pb.rowVector(["x"], [pb.fromList([1])])
        builder = (
            pb.PlanBuilder()
            .values([values])
            .tableWrite(
                outputDirectoryPath="unsupported://bucket/table",
                fileFormat=pb.FileFormat.PARQUET,
            )
        )
        with self.assertRaisesRegex(RuntimeError, "FileSink is not registered"):
            self.execute(builder)

    def testStringAndBinaryAggregators(self):
        for accumulator, dtype, values, expected in (
            (
                _StringAccumulator,
                pb.VarcharType(),
                ["hello" * 4, " world" * 4],
                "hello" * 4 + " world" * 4,
            ),
            (
                _BinaryAccumulator,
                pb.VarbinaryType(),
                [b"a\x00" * 20, b"b"],
                b"a\x00" * 20 + b"b",
            ),
        ):
            name = "binding_" + accumulator.__name__.lower()
            with self.subTest(dtype=dtype):
                pb.registerPythonAggregator(accumulator, name, dtype, 1)
                inputs = pb.rowVector(["x"], [pb.fromList(values, dtype)])
                result = self.execute(
                    pb.PlanBuilder()
                    .values([inputs])
                    .singleAggregation([], [f"{name}(x)"])
                )
                self.assertEqual(result.childAt(result.dtype().names()[0])[0], expected)

    def testDefaultDialectUsesPrestoFunctions(self):
        values = pb.rowVector(["text"], [pb.fromList(["a.b"], pb.VarcharType())])
        result = self.execute(
            pb.PlanBuilder().values([values]).project(["split(text, '.') as parts"])
        )
        self.assertEqual(result.childAt("parts")[0], ["a", "b"])

    def testDefaultDialectChecksIntegerOverflow(self):
        values = pb.rowVector(["x"], [pb.fromList([2**62])])
        result = self.execute(
            pb.PlanBuilder().values([values]).project(["try(x * 4) as checked"])
        )
        self.assertIsNone(result.childAt("checked")[0])


if __name__ == "__main__":
    unittest.main()
