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

import unittest

import numpy as np

import pybolt
from boltml.dataframe import DataFrame
from boltml.executor import LocalExecutor
from boltml.function import tensorAdapter, tensorFunction, vectorFunction
from boltml.function.vector import functionRegister


class TestTensorFunction(unittest.TestCase):
    def testRegistersAsArrayTypeAndStampsContract(self):
        @tensorFunction(pybolt.RealType(), [3, 224, 224])
        def _tensorfn_producer_contract(_):
            pass

        self.assertEqual(
            _tensorfn_producer_contract.tensor_element_type, pybolt.RealType()
        )
        self.assertEqual(_tensorfn_producer_contract.tensor_shape, [3, 224, 224])
        info = functionRegister[_tensorfn_producer_contract]
        self.assertEqual(info.outputType, pybolt.ArrayType(pybolt.RealType()))

    def testShapeValidationIsEager(self):
        with self.assertRaises(ValueError):

            @tensorFunction(pybolt.RealType(), [])
            def _(_):
                pass

        with self.assertRaises(ValueError):

            @tensorFunction(pybolt.RealType(), [3, 0, 224])
            def _(_):
                pass

        with self.assertRaises(ValueError):

            @tensorFunction(pybolt.RealType(), [3, -1, 224])
            def _(_):
                pass


class TestTensorAdapterOutput(unittest.TestCase):
    def testProducerReturnsNdarrayWrappedToArrayVector(self):
        @tensorFunction(pybolt.IntegerType(), [2, 3])
        @tensorAdapter(outputDtype=np.ndarray)
        def _tensorfn_producer_output(_unused_input):
            return np.arange(4 * 2 * 3, dtype=np.int32).reshape(4, 2, 3)

        dummy = pybolt.fromList([1, 2, 3, 4], pybolt.BigintType())
        out = _tensorfn_producer_output(dummy)
        self.assertIsInstance(out, pybolt.ArrayVector)
        self.assertEqual(len(out), 4)

        back = out.to_numpy(shape=[2, 3])
        np.testing.assert_array_equal(
            back, np.arange(24, dtype=np.int32).reshape(4, 2, 3)
        )

    def testNonNdarrayReturnRaises(self):
        @tensorFunction(pybolt.RealType(), [4])
        @tensorAdapter(outputDtype=np.ndarray)
        def _tensorfn_bad_output(_unused):
            return "not an ndarray"

        with self.assertRaises(TypeError):
            _tensorfn_bad_output(pybolt.fromList([0], pybolt.BigintType()))

    def testRequiresPairedTensorFunctionWhenNoExplicitShape(self):
        @vectorFunction(pybolt.ArrayType(pybolt.RealType()))
        @tensorAdapter(outputDtype=np.ndarray)
        def _tensorfn_producer_noshape(_unused):
            return np.zeros((2, 3), dtype=np.float32)

        with self.assertRaises(RuntimeError):
            _tensorfn_producer_noshape(pybolt.fromList([0, 0], pybolt.BigintType()))


class TestTensorAdapterInput(unittest.TestCase):
    def testConsumerReceivesNdarray(self):
        captured = {}

        @vectorFunction(pybolt.VarcharType())
        @tensorAdapter(inputDtype=np.ndarray, shape=[2, 3])
        def _tensorfn_consumer_explicitshape(tensors):
            captured["arg"] = tensors
            n = len(tensors)
            return pybolt.fromList([f"row{i}" for i in range(n)], pybolt.VarcharType())

        src = np.arange(4 * 2 * 3, dtype=np.int32).reshape(4, 2, 3)
        vec = pybolt.fromNumpy(src, pybolt.IntegerType(), [2, 3])

        out = _tensorfn_consumer_explicitshape(vec)
        self.assertIsInstance(captured["arg"], np.ndarray)
        self.assertEqual(captured["arg"].shape, (4, 2, 3))
        np.testing.assert_array_equal(captured["arg"], src)
        # Function's own return path is unchanged (no outputDtype).
        self.assertEqual(len(out), 4)

    def testInputAdapterInheritsShapeFromOuterTensorFunction(self):
        captured = {}

        @tensorFunction(pybolt.IntegerType(), [2, 3])
        @tensorAdapter(inputDtype=np.ndarray, outputDtype=np.ndarray)
        def _tensorfn_passthrough(tensors):
            captured["arg"] = tensors
            return tensors  # pass through unchanged

        src = np.arange(4 * 2 * 3, dtype=np.int32).reshape(4, 2, 3)
        vec = pybolt.fromNumpy(src, pybolt.IntegerType(), [2, 3])

        out = _tensorfn_passthrough(vec)
        self.assertEqual(captured["arg"].shape, (4, 2, 3))
        self.assertEqual(len(out), 4)
        np.testing.assert_array_equal(out.to_numpy(shape=[2, 3]), src)


class TestTensorAdapterValidation(unittest.TestCase):
    def testAtLeastOneOfInputOrOutputRequired(self):
        with self.assertRaises(ValueError):
            tensorAdapter()

    def testUnsupportedDtypeRejected(self):
        with self.assertRaises(ValueError):
            tensorAdapter(outputDtype=list)
        with self.assertRaises(ValueError):
            tensorAdapter(inputDtype=str)

    def testExplicitShapeValidatedEagerly(self):
        with self.assertRaises(ValueError):
            tensorAdapter(inputDtype=np.ndarray, shape=[])
        with self.assertRaises(ValueError):
            tensorAdapter(inputDtype=np.ndarray, shape=[2, 0, 3])


class TestColumnMapTensorFunction(unittest.TestCase):
    def testColumnMapProducesTensorColumn(self):
        @tensorFunction(pybolt.IntegerType(), [2, 3])
        @tensorAdapter(outputDtype=np.ndarray)
        def _tensorfn_expand_idx_to_tensor(idxs):
            n = len(idxs)
            out = np.empty((n, 2, 3), dtype=np.int32)
            for i in range(n):
                base = int(idxs[i]) * 100
                out[i] = np.arange(base, base + 6, dtype=np.int32).reshape(2, 3)
            return out

        df = DataFrame(
            {"id": pybolt.fromList([0, 1, 2, 3], pybolt.BigintType())},
            executor=LocalExecutor(),
        )
        df["tensor"] = df["id"].map(_tensorfn_expand_idx_to_tensor)

        self.assertEqual(
            df.dtype.findChild("tensor"),
            pybolt.ArrayType(pybolt.IntegerType()),
        )

        tensor_col = df["tensor"].data
        self.assertIsInstance(tensor_col, pybolt.ArrayVector)
        self.assertEqual(len(tensor_col), 4)

        actual = tensor_col.to_numpy(shape=_tensorfn_expand_idx_to_tensor.tensor_shape)
        expected = np.stack(
            [
                np.arange(i * 100, i * 100 + 6, dtype=np.int32).reshape(2, 3)
                for i in range(4)
            ]
        )
        self.assertEqual(actual.shape, (4, 2, 3))
        self.assertEqual(actual.dtype, np.int32)
        np.testing.assert_array_equal(actual, expected)


if __name__ == "__main__":
    unittest.main()
