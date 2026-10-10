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

import gc
import unittest
import weakref

import numpy as np
import pyarrow as pa
import pybolt as pb


class TestBoltVector(unittest.TestCase):
    def testInheritance(self):
        v1 = pb.fromList([1, 2, 3])
        v2 = pb.fromList(["hello", "world"])
        v3 = pb.constantVector(1000, 10)
        v4 = pb.dictionaryVector(pb.fromList([1, 2, 3]), [0, 0, 1])

        self.assertTrue(isinstance(v1, pb.BaseVector))
        self.assertTrue(isinstance(v2, pb.BaseVector))
        self.assertTrue(isinstance(v3, pb.BaseVector))
        self.assertTrue(isinstance(v4, pb.BaseVector))

        self.assertTrue(isinstance(v1, pb.SimpleVector_BIGINT))
        self.assertTrue(isinstance(v2, pb.SimpleVector_VARBINARY))
        self.assertTrue(isinstance(v3, pb.SimpleVector_BIGINT))
        self.assertTrue(isinstance(v4, pb.SimpleVector_BIGINT))

        self.assertTrue(isinstance(v1, pb.FlatVector_BIGINT))
        self.assertTrue(isinstance(v2, pb.FlatVector_VARBINARY))
        self.assertTrue(isinstance(v3, pb.ConstantVector_BIGINT))
        self.assertTrue(isinstance(v4, pb.DictionaryVector_BIGINT))

        self.assertFalse(isinstance(v1, pb.ConstantVector_BIGINT))
        self.assertFalse(isinstance(v2, pb.ConstantVector_VARBINARY))
        self.assertFalse(isinstance(v3, pb.FlatVector_BIGINT))
        self.assertFalse(isinstance(v4, pb.ConstantVector_BIGINT))

    def testFromList(self):
        self.assertTrue(isinstance(pb.fromList([1, 2, 3]), pb.BaseVector))
        self.assertTrue(isinstance(pb.fromList([1, None, None]), pb.BaseVector))
        self.assertTrue(isinstance(pb.fromList(["hello", "world"]), pb.BaseVector))
        with self.assertRaises(TypeError):
            pb.fromList(["hello", 3.14])
        with self.assertRaises(ValueError):
            pb.fromList([None, None, None])
        with self.assertRaises(ValueError):
            pb.fromList([])

    def testFromListWithType(self):
        listA = [0, 1, 3]
        a = pb.fromList(listA, pb.BooleanType())
        self.assertEqual(a.typeKind().name, "BOOLEAN")
        for i in range(len(a)):
            self.assertTrue(isinstance(a[i], bool))
            self.assertEqual(a[i], bool(listA[i]))
        self.assertTrue(
            isinstance(
                pb.fromList([None, None, None], pb.VarcharType()),
                pb.BaseVector,
            )
        )
        emptyVector = pb.fromList([], pb.IntegerType())
        self.assertTrue(isinstance(emptyVector, pb.BaseVector))
        with self.assertRaises(IndexError):
            a = emptyVector[0]
        with self.assertRaises(RuntimeError):
            a = pb.fromList(
                [0, 1, 3], pb.VarcharType()
            )  # Conversion not possible from int to varchar
        listB = [0.2, 1.2, 3.23]
        b = pb.fromList(listB, pb.RealType())
        for i in range(len(listB)):
            self.assertNotAlmostEqual(listB[i], b[i], places=17)

        # dtype as a keyword argument
        integerVector = pb.fromList([1, 3, 11], dtype=pb.IntegerType())
        self.assertTrue(isinstance(integerVector, pb.BaseVector))
        self.assertEqual(integerVector.typeKind().name, "INTEGER")

    def testFromListWithArrayType(self):
        dtype = pb.ArrayType(pb.BigintType())
        v = pb.fromList([[1, 2, 3], None, [], [4]], dtype)
        self.assertEqual(v.typeKind().name, "ARRAY")

        array = pb.exportToArrow(v)
        self.assertEqual(array.type, pa.list_(pa.int64()))
        self.assertEqual(array.to_pylist(), [[1, 2, 3], None, [], [4]])
        self.assertEqual(v[0], [1, 2, 3])
        self.assertEqual(v[1], None)
        self.assertEqual(v[2], [])
        self.assertEqual(v[3], [4])

    def testFromListWithMapType(self):
        dtype = pb.MapType(pb.VarcharType(), pb.BigintType())
        v = pb.fromList([{"a": 1, "b": 2}, None, {}, {"x": None}], dtype)
        self.assertEqual(v.typeKind().name, "MAP")

        array = pb.exportToArrow(v)
        self.assertEqual(array.type, pa.map_(pa.string(), pa.int64()))
        self.assertEqual(
            array.to_pylist(),
            [[("a", 1), ("b", 2)], None, [], [("x", None)]],
        )
        self.assertEqual(v[0], {"a": 1, "b": 2})
        self.assertEqual(v[1], None)
        self.assertEqual(v[2], {})
        self.assertEqual(v[3], {"x": None})

    def testFromListInferMapTypeValidation(self):
        v = pb.fromList([{"a": 1}, {"b": None}, {"c": 3}])
        self.assertEqual(v.typeKind().name, "MAP")
        array = pb.exportToArrow(v)
        self.assertEqual(array.type, pa.map_(pa.string(), pa.int64()))
        self.assertEqual(
            array.to_pylist(),
            [[("a", 1)], [("b", None)], [("c", 3)]],
        )

        with self.assertRaises(TypeError):
            pb.fromList([{"a": 1}, {1: 2}])

        with self.assertRaises(TypeError):
            pb.fromList([{None: 1}])

        with self.assertRaises(TypeError):
            pb.fromList([{"a": 1}, {"b": "x"}])

        with self.assertRaises(ValueError):
            pb.fromList([{"a": None}, {"b": None}])

        with self.assertRaises(ValueError):
            pb.fromList([{}])

    def testVarbinaryBytesRoundtrip(self):
        data0 = b"\x00\x01"
        data1 = b"\xff\x10"
        v = pb.fromList([data0, data1])
        self.assertEqual(v.typeKind().name, "VARBINARY")
        self.assertEqual(v[0], data0)
        self.assertEqual(v[1], data1)

    def testDictionaryEncoding(self):
        baseIndices = [0, 0, 1, 0, 2]
        vec = pb.dictionaryVector(pb.fromList([1, 2, 3]), baseIndices)
        self.assertTrue(isinstance(vec, pb.DictionaryVector_BIGINT))
        expectedValues = [1, 1, 2, 1, 3]
        self.assertEqual(len(vec), len(expectedValues))
        for i in range(len(vec)):
            self.assertEqual(vec[i], expectedValues[i])

        indices = vec.indices()
        self.assertTrue(isinstance(indices, pb.DictionaryIndices))
        self.assertEqual(len(indices), len(baseIndices))
        for i in range(len(indices)):
            self.assertEqual(indices[i], baseIndices[i])

        baseVector = pb.fromList([1, 2, 3, 4, 5, 6, 7])
        baseIndices = [6, 6]
        vec = pb.dictionaryVector(baseVector, baseIndices)
        expectedValues = [7, 7]
        for i in range(len(vec)):
            self.assertEqual(vec[i], expectedValues[i])

        with self.assertRaises(TypeError):
            pb.dictionaryVector(pb.fromList([1, 2, 3]), ["a", 0, 0, "b"])
        with self.assertRaises(IndexError):
            pb.dictionaryVector(pb.fromList([1, 2, 3]), [1, 2, 1000000])
            pb.dictionaryVector(pb.fromList([1, 2, 3]), [0, -1, -2])

    def testArrayVector(self):
        v1 = pb.fromList([[1, 2, 3], [1, 2, 3]])
        self.assertTrue(isinstance(v1, pb.ArrayVector))
        self.assertTrue(isinstance(v1.elements(), pb.FlatVector_BIGINT))
        self.assertEqual(len(v1), 2)
        expectedFlat = [1, 2, 3, 1, 2, 3]
        self.assertEqual(len(expectedFlat), len(v1.elements()))
        for i in range(len(expectedFlat)):
            self.assertEqual(expectedFlat[i], v1.elements()[i])

        v2 = pb.fromList([[1], [1, 2, None]])
        self.assertTrue(isinstance(v2, pb.ArrayVector))
        self.assertTrue(isinstance(v2.elements(), pb.FlatVector_BIGINT))
        self.assertEqual(len(v2), 2)
        expectedFlat = [1, 1, 2, None]
        self.assertEqual(len(v2.elements()), len(expectedFlat))
        for i in range(len(expectedFlat)):
            self.assertEqual(expectedFlat[i], v2.elements()[i])

        doubleNested = pb.fromList([[[1, 2], [3, None]], [[1], [2]]])
        self.assertTrue(isinstance(doubleNested, pb.ArrayVector))
        self.assertTrue(isinstance(doubleNested.elements(), pb.ArrayVector))
        self.assertEqual(len(doubleNested), 2)
        elements = doubleNested.elements().elements()
        self.assertTrue(isinstance(elements, pb.FlatVector_BIGINT))
        self.assertEqual(len(elements), 6)
        expectedFirstElements = [1, 2, 3, None, 1, 2]
        self.assertEqual(len(elements), len(expectedFirstElements))
        for i in range(len(expectedFirstElements)):
            self.assertEqual(expectedFirstElements[i], elements[i])

        with self.assertRaises(TypeError):
            pb.fromList([[[1, 2], [3, 4]], [[1.1], [2.3]]])

        with self.assertRaises(ValueError):
            pb.fromList([[None], [None, None, None]])

        with self.assertRaises(TypeError):
            pb.fromList([[[1, 2], [3, 4]], [["hello"], ["world"]]])

    def testToString(self):
        self.assertEqual(
            str(pb.fromList([1, 2, 3])),
            "0: 1\n1: 2\n2: 3",
        )
        self.assertEqual(
            str(pb.fromList([1, None, 3])),
            "0: 1\n1: null\n2: 3",
        )

    def testGetItem(self):
        ints = pb.fromList([1, 2, None, None, 3])
        self.assertEqual(ints[0], 1)
        self.assertEqual(ints[1], 2)
        self.assertEqual(ints[2], None)
        self.assertEqual(ints[3], None)
        self.assertEqual(ints[4], 3)

        strs = pb.fromList(["hello", "world", None])
        self.assertEqual(strs[0], "hello")
        self.assertEqual(strs[1], "world")
        self.assertEqual(strs[2], None)
        self.assertNotEqual(strs[0], "world")
        self.assertNotEqual(strs[2], "world")

        with self.assertRaises(IndexError):
            ints[5]
        with self.assertRaises(IndexError):
            ints[-1]
        with self.assertRaises(IndexError):
            strs[1000]
        with self.assertRaises(IndexError):
            strs[-1000]

        arrays = pb.fromList([[1, 2], None, []], pb.ArrayType(pb.BigintType()))
        self.assertEqual(arrays[0], [1, 2])
        self.assertEqual(arrays[1], None)
        self.assertEqual(arrays[2], [])
        with self.assertRaises(IndexError):
            arrays[3]
        with self.assertRaises(IndexError):
            arrays[-1]
        arrays_first_none = pb.fromList([None, [1], []], pb.ArrayType(pb.BigintType()))
        self.assertEqual(arrays_first_none[0], None)
        self.assertEqual(arrays_first_none[1], [1])
        self.assertEqual(arrays_first_none[2], [])

        maps = pb.fromList(
            [{"a": 1}, None, {"b": None}],
            pb.MapType(pb.VarcharType(), pb.BigintType()),
        )
        self.assertEqual(maps[0], {"a": 1})
        self.assertEqual(maps[1], None)
        self.assertEqual(maps[2], {"b": None})
        with self.assertRaises(IndexError):
            maps[3]
        with self.assertRaises(IndexError):
            maps[-1]
        maps_first_value_none = pb.fromList(
            [{"a": None}, {"b": 2}],
            pb.MapType(pb.VarcharType(), pb.BigintType()),
        )
        self.assertEqual(maps_first_value_none[0], {"a": None})
        self.assertEqual(maps_first_value_none[1], {"b": 2})

    def testSetItem(self):
        ints = pb.fromList([1, 2, None, None, 3])
        self.assertEqual(ints[2], None)
        ints[2] = 10
        self.assertEqual(ints[2], 10)
        ints[4] = None
        self.assertEqual(ints[4], None)

        strs = pb.fromList(["googly", "doogly"])
        self.assertEqual(strs[1], "doogly")
        strs[1] = "moogly"
        self.assertEqual(strs[1], "moogly")
        strs[0] = None
        self.assertEqual(strs[0], None)

        with self.assertRaises(IndexError):
            ints[5] = 10
        with self.assertRaises(IndexError):
            ints[-1] = 10
        with self.assertRaises(IndexError):
            strs[1000] = "hi"
        with self.assertRaises(IndexError):
            strs[-1000] = "bye"
        with self.assertRaises(TypeError):
            ints[3] = "ni hao"
        with self.assertRaises(TypeError):
            strs[0] = 2

        arrays = pb.fromList([[1, 2], [3]], pb.ArrayType(pb.BigintType()))
        arrays[1] = [4, 5]
        self.assertEqual(arrays[1], [4, 5])
        arrays[0] = None
        self.assertEqual(arrays[0], None)
        with self.assertRaises(RuntimeError):
            arrays[1] = ["x"]
        with self.assertRaises(IndexError):
            arrays[3] = [1]
        with self.assertRaises(IndexError):
            arrays[-1] = [1]

        maps = pb.fromList(
            [{"a": 1}, {"b": 2}],
            pb.MapType(pb.VarcharType(), pb.BigintType()),
        )
        maps[0] = {"c": 3}
        self.assertEqual(maps[0], {"c": 3})
        maps[1] = None
        self.assertEqual(maps[1], None)
        with self.assertRaises(RuntimeError):
            maps[0] = {"d": "x"}
        with self.assertRaises(IndexError):
            maps[3] = {"e": 1}
        with self.assertRaises(IndexError):
            maps[-1] = {"e": 1}

    def testArrayMapEquality(self):
        arrays_a = pb.fromList(
            [[1, 2], None, [3]],
            pb.ArrayType(pb.BigintType()),
        )
        arrays_b = pb.fromList(
            [[1, 2], None, [3]],
            pb.ArrayType(pb.BigintType()),
        )
        self.assertEqual(arrays_a, arrays_b)

        maps_a = pb.fromList(
            [{"a": 1}, None, {"b": None}],
            pb.MapType(pb.VarcharType(), pb.BigintType()),
        )
        maps_b = pb.fromList(
            [{"a": 1}, None, {"b": None}],
            pb.MapType(pb.VarcharType(), pb.BigintType()),
        )
        self.assertEqual(maps_a, maps_b)

    def testLength(self):
        ints = pb.fromList([1, 2, None])
        self.assertEqual(len(ints), 3)
        self.assertEqual(ints.size(), 3)

        strs = pb.fromList(["hi", "bye"])
        self.assertEqual(len(strs), 2)
        self.assertEqual(strs.size(), 2)

    def testNumericLimits(self):
        biggerThanInt32 = pb.fromList([1 << 33])
        self.assertEqual(biggerThanInt32[0], 1 << 33)
        with self.assertRaises(RuntimeError):
            pb.fromList([1 << 63])

    def test_type(self):
        ints = pb.fromList([1, 2, None])
        self.assertEqual(ints.dtype(), pb.BigintType())
        self.assertEqual(ints.typeKind(), pb.TypeKind.BIGINT)

        strs = pb.fromList(["a", "b", None])
        self.assertEqual(strs.dtype(), pb.VarcharType())

    def testMisc(self):
        ints = pb.fromList([3, 4, 3, None])

        self.assertTrue(ints.mayHaveNulls())

        self.assertFalse(ints.isLazy())

        self.assertTrue(ints.isNullAt(3))
        self.assertFalse(ints.isNullAt(0))
        with self.assertRaises(IndexError):
            ints.isNullAt(10)
        with self.assertRaises(IndexError):
            ints.isNullAt(-10)

        self.assertEqual(ints.hashValueAt(0), ints.hashValueAt(2))
        self.assertNotEqual(ints.hashValueAt(0), ints.hashValueAt(1))
        with self.assertRaises(IndexError):
            ints.hashValueAt(10)
        with self.assertRaises(IndexError):
            ints.hashValueAt(-10)

    def testAppend(self):
        ints1 = pb.fromList([4, 2, 0, None])
        ints2 = pb.fromList([1, 3, 3, 7])
        self.assertEqual(len(ints1), 4)
        ints1.append(ints2)
        self.assertEqual(len(ints1), 8)
        self.assertEqual(ints1[5], 3)
        self.assertEqual(ints1[7], 7)

        strs1 = pb.fromList(["bork", "cork"])
        strs2 = pb.fromList(["pork", "stork"])
        self.assertEqual(len(strs1), 2)
        strs1.append(strs2)
        self.assertEqual(len(strs1), 4)
        self.assertEqual(strs1[3], "stork")

        with self.assertRaises(TypeError):
            ints2.append(strs2)

    def testSlice(self):
        a = pb.fromList(list(range(0, 10)))

        b = a.slice(2, 6)
        self.assertEqual(len(b), 4)
        for i in range(4):
            self.assertEqual(b[i], i + 2)

        with self.assertRaises(NotImplementedError):
            a.slice(2, 6, 2)

        d = a[3:6]
        self.assertEqual(len(d), 3)
        for i in range(3):
            self.assertEqual(d[i], i + 3)

        with self.assertRaises(NotImplementedError):
            a[3:8:3]

    def testExportToArrow(self):
        testCases = [
            ([1, 2, 3], pa.int64()),
            ([1.1, 2.2, 3.3], pa.float64()),
            (["ab", "bc", "ca"], pa.string()),
        ]
        for data, expectedType in testCases:
            with self.subTest(data=data):
                vector = pb.fromList(data)
                array = pb.exportToArrow(vector)

                self.assertEqual(array.type, expectedType)
                self.assertEqual(len(array), len(data))
                self.assertListEqual(array.tolist(), data)

    def testImportFromArrow(self):
        testCases = [
            ([11, 26, 31], pa.int64(), pb.IntegerType()),
            ([0.1, 2.5, 3.9], pa.float64(), pb.DoubleType()),
            (["az", "by", "cx"], pa.string(), pb.VarcharType()),
        ]
        for data, dtype, expectedType in testCases:
            with self.subTest(data=data):
                array = pa.array(data, type=dtype)
                boltVector = pb.importFromArrow(array)

                self.assertEqual(boltVector.size(), len(data))
                self.assertTrue(boltVector.dtype(), expectedType)
                for i in range(0, len(data)):
                    self.assertEqual(boltVector[i], data[i])

    def testRoundtripConversion(self):
        testCases = [
            ([41, 92, 13], pb.IntegerType()),
            ([17.19, 22.25, 13.3], pb.DoubleType()),
            (["aa1", "bb2", "cc3"], pb.VarcharType()),
        ]
        for data, expectedType in testCases:
            with self.subTest(data=data):
                vector = pb.fromList(data)
                array = pb.exportToArrow(vector)

                boltVector = pb.importFromArrow(array)
                self.assertEqual(boltVector.size(), len(data))
                self.assertTrue(boltVector.dtype(), expectedType)
                for i in range(0, len(data)):
                    self.assertEqual(boltVector[i], data[i])

    def testRowVectorBasic(self):
        vals = [
            pb.fromList([1, 2, 3]),
            pb.fromList([4.0, 5.0, 6.0]),
            pb.fromList(["a", "b", "c"]),
        ]

        colNames = ["x", "y", "z"]
        rw = pb.rowVector(colNames, vals)
        rwStr = str(rw)
        expectedStr = "0: {1, 4, a}\n1: {2, 5, b}\n2: {3, 6, c}"
        assert expectedStr == rwStr

    def testRowVectorWithNulls(self):
        vals = [
            pb.fromList([1, 2, 3, 1, 2]),
            pb.fromList([4, 5, 6, 4, 5]),
            pb.fromList([7, 8, 9, 7, 8]),
            pb.fromList([10, 11, 12, 10, 11]),
        ]

        colNames = ["a", "b", "c", "d"]
        rw = pb.rowVector(colNames, vals, {0: True, 2: True})
        rwStr = str(rw)
        expectedStr = (
            "0: null\n1: {2, 5, 8, 11}\n2: null\n3: {1, 4, 7, 10}\n4: {2, 5, 8, 11}"
        )
        assert expectedStr == rwStr

    def testRowVectorComparison(self):
        u = [
            pb.fromList([1, 2, 3]),
            pb.fromList([7, 4, 9]),
            pb.fromList([10, 11, 12]),
        ]

        v = [
            pb.fromList([1, 2, 3]),
            pb.fromList([7, 8, 9]),
            pb.fromList([10, 11, 12]),
        ]

        w = [
            pb.fromList([1, 2, 3]),
            pb.fromList([7, 8, 9]),
        ]

        uNames = ["a", "b", "c"]
        wNames = ["x", "y"]
        uRw = pb.rowVector(uNames, u)
        vRw = pb.rowVector(uNames, v)
        wRw = pb.rowVector(wNames, w)
        yRw = pb.rowVector(uNames, u)
        x1Rw = pb.rowVector(uNames, u, {0: True, 2: True})
        x2Rw = pb.rowVector(uNames, u, {0: True, 2: True})

        assert uRw != wRw  # num of children doesn't match
        assert uRw != vRw  # data doesn't match
        assert uRw == yRw  # data match
        assert x1Rw == x2Rw  # with null
        assert x1Rw != uRw  # with and without null


class TestTensorNumpyBridge(unittest.TestCase):
    """Zero-copy bridge between numpy ndarrays and ARRAY<element_type>-typed
    ArrayVectors with dense uniform rows. Boltml does not have a
    parameterized tensor logical type; the per-row shape is the
    producer/consumer contract, passed explicitly as the `shape` arg to
    both `pybolt.fromNumpy(arr, element_type, shape)` and
    `Series.to_numpy(shape)`."""

    def testRoundTripDtypeMatrix(self):
        cases = [
            (pb.TinyintType(), np.int8),
            (pb.SmallintType(), np.int16),
            (pb.IntegerType(), np.int32),
            (pb.BigintType(), np.int64),
            (pb.RealType(), np.float32),
            (pb.DoubleType(), np.float64),
        ]
        for elem, dtype in cases:
            with self.subTest(dtype=dtype):
                arr = np.arange(4 * 2 * 3, dtype=dtype).reshape(4, 2, 3)
                vec = pb.fromNumpy(arr, elem, [2, 3])
                self.assertIsInstance(vec, pb.ArrayVector)
                self.assertEqual(len(vec), 4)
                back = vec.to_numpy(shape=[2, 3])
                self.assertEqual(back.shape, (4, 2, 3))
                self.assertEqual(back.dtype, dtype)
                np.testing.assert_array_equal(back, arr)

    def testFromNumpyAliasesProducerMemory(self):
        # If we mutate the source numpy array, the values seen via the
        # vector's to_numpy() must reflect that — proving no copy.
        arr = np.zeros((3, 4), dtype=np.float32)
        vec = pb.fromNumpy(arr, pb.RealType(), [4])
        arr[1, 2] = 7.5
        back = vec.to_numpy(shape=[4])
        self.assertEqual(back[1, 2], 7.5)

    def testToNumpyAliasesVeloxMemory(self):
        # Inverse direction: the np.ndarray returned by to_numpy must
        # alias the underlying BufferPtr's memory. We mutate via the
        # numpy view and re-export to confirm.
        arr = np.array([[1, 2], [3, 4]], dtype=np.int32)
        vec = pb.fromNumpy(arr, pb.IntegerType(), [2])
        view = vec.to_numpy(shape=[2])
        view[0, 0] = 99
        view2 = vec.to_numpy(shape=[2])
        self.assertEqual(view2[0, 0], 99)
        self.assertEqual(arr[0, 0], 99)

    def testVectorOutlivesNumpy(self):
        # The BufferView holds a +1 ref on the numpy array; even after
        # the user drops their reference and runs GC, the data must
        # remain accessible through the vector.
        arr = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype=np.float32)
        ref = weakref.ref(arr)
        vec = pb.fromNumpy(arr, pb.RealType(), [3])
        del arr
        gc.collect()
        self.assertIsNotNone(ref())
        back = vec.to_numpy(shape=[3])
        np.testing.assert_array_equal(
            back, np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype=np.float32)
        )
        del back
        del vec
        gc.collect()
        self.assertIsNone(ref())

    def testNumpyOutlivesVector(self):
        # Symmetric: dropping the vector must leave the user's numpy
        # array fully usable (no aliasing crash, no data corruption).
        arr = np.array([[1.5, 2.5], [3.5, 4.5]], dtype=np.float64)
        vec = pb.fromNumpy(arr, pb.DoubleType(), [2])
        del vec
        gc.collect()
        np.testing.assert_array_equal(
            arr, np.array([[1.5, 2.5], [3.5, 4.5]], dtype=np.float64)
        )
        arr[0, 0] = -1.0
        self.assertEqual(arr[0, 0], -1.0)

    def testToNumpyOutlivesVector(self):
        # The np.ndarray returned by to_numpy holds a strong ref on the
        # underlying BufferPtr via its `base` capsule. Dropping the
        # vector must not invalidate the array.
        arr = np.array([[7.0, 8.0], [9.0, 10.0]], dtype=np.float32)
        vec = pb.fromNumpy(arr, pb.RealType(), [2])
        view = vec.to_numpy(shape=[2])
        del vec
        del arr
        gc.collect()
        np.testing.assert_array_equal(
            view,
            np.array([[7.0, 8.0], [9.0, 10.0]], dtype=np.float32),
        )

    def testFromNumpyShapeMismatch(self):
        bad = np.zeros((4, 3, 100, 224), dtype=np.float32)
        with self.assertRaises(ValueError):
            pb.fromNumpy(bad, pb.RealType(), [3, 224, 224])

    def testFromNumpyNdimMismatch(self):
        bad = np.zeros((4, 3, 224), dtype=np.float32)  # missing trailing axis
        with self.assertRaises(ValueError):
            pb.fromNumpy(bad, pb.RealType(), [3, 224, 224])

    def testFromNumpyDtypeMismatch(self):
        bad = np.zeros((3, 4), dtype=np.float64)  # double, not real
        with self.assertRaises(TypeError):
            pb.fromNumpy(bad, pb.RealType(), [4])

    def testFromNumpyNonContiguous(self):
        # Slice with a non-unit step to force non-C-contiguous.
        big = np.zeros((4, 6), dtype=np.int32)
        bad = big[:, ::2]  # shape (4, 3) but not C-contiguous
        self.assertFalse(bad.flags["C_CONTIGUOUS"])
        with self.assertRaises(ValueError):
            pb.fromNumpy(bad, pb.IntegerType(), [3])

    def testFromNumpyEmptyShape(self):
        with self.assertRaises(ValueError):
            pb.fromNumpy(np.zeros((3,), dtype=np.int32), pb.IntegerType(), [])

    def testFromNumpyNonPositiveShape(self):
        with self.assertRaises(ValueError):
            pb.fromNumpy(np.zeros((3, 4), dtype=np.int32), pb.IntegerType(), [0])
        with self.assertRaises(ValueError):
            pb.fromNumpy(np.zeros((3, 4), dtype=np.int32), pb.IntegerType(), [-1])

    def testToNumpyShapeRequired(self):
        plain = pb.fromList([[1, 2, 3], [4, 5, 6]], pb.ArrayType(pb.BigintType()))
        # No shape -> error (it is positional+required now).
        with self.assertRaises(TypeError):
            plain.to_numpy()
        # With shape -> zero-copy view of the elements buffer.
        arr = plain.to_numpy(shape=[3])
        self.assertEqual(arr.shape, (2, 3))
        self.assertEqual(arr.dtype, np.int64)
        np.testing.assert_array_equal(
            arr, np.array([[1, 2, 3], [4, 5, 6]], dtype=np.int64)
        )

    def testToNumpyReshapeViaShape(self):
        # Same elements buffer can be reshaped via different shape args
        # (provided product(shape) matches the per-row listSize).
        src = np.arange(12, dtype=np.int32).reshape(2, 2, 3)
        vec = pb.fromNumpy(src, pb.IntegerType(), [2, 3])
        # Original shape.
        np.testing.assert_array_equal(vec.to_numpy(shape=[2, 3]), src)
        # Flatten the per-row shape.
        flat = vec.to_numpy(shape=[6])
        np.testing.assert_array_equal(flat.reshape(2, 2, 3), src)
        # 3x2 reshape — also valid since 3*2 == 6 == per-row listSize.
        swap = vec.to_numpy(shape=[3, 2])
        np.testing.assert_array_equal(swap.reshape(2, 2, 3), src)
