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

"""Integration checks using public imports from the installed BoltML package."""

import base64
import io
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pybolt
from PIL import Image

from boltml import DataFrame
from boltml.function import (
    Aggregator,
    arrowAdapter,
    dataframeFunction,
    mapBatchFunction,
    scalarFunction,
    tensorAdapter,
    tensorFunction,
    vectorFunction,
)
from boltml.function.aggregation import Count, Mean, Sum
from boltml.function.image import downloadImages


class DataFrameExecutionTests(unittest.TestCase):
    def testFilterProjectAndGroupBy(self):
        frame = DataFrame({"key": ["a", "b", "a"], "value": [1, 2, 3]})
        frame.filter(frame["value"] > 1).transform(doubled=lambda df: df["value"] * 2)
        result = frame.groupBy("key").aggregate(
            total=Sum("doubled"), mean=Mean("value")
        )
        self.assertEqual(
            result.orderBy("key").toArrow(pa.Table).to_pylist(),
            [
                {"key": "a", "total": 6, "mean": 3.0},
                {"key": "b", "total": 4, "mean": 2.0},
            ],
        )

    def testLeftJoinPreservesUnmatchedRows(self):
        left = DataFrame({"id": [1, 2], "value": [10, 20]})
        right = DataFrame({"id": [2, 3], "value": [200, 300]})
        result = left.join(right, keys={"id"}, joinType=pybolt.JoinType.kLeft).orderBy(
            "id"
        )
        self.assertEqual(
            result.toArrow(pa.Table).to_pylist(),
            [
                {"id": 1, "l_value": 10, "r_value": None},
                {"id": 2, "l_value": 20, "r_value": 200},
            ],
        )

    def testTypedEmptyArrowInputPreservesSchema(self):
        schema = pa.schema([("id", pa.int64()), ("payload", pa.binary())])
        source = pa.Table.from_batches([], schema=schema)
        result = DataFrame(source).toArrow(pa.Table)
        self.assertEqual(result.schema, schema)
        self.assertEqual(result.num_rows, 0)


class UserFunctionTests(unittest.TestCase):
    def testScalarFunctionWithNulls(self):
        @scalarFunction(pybolt.BigintType())
        def integrationPlusOne(value):
            return None if value is None else value + 1

        frame = DataFrame({"value": [1, None, 3]})
        frame.transform(result=lambda df: df["value"].map(integrationPlusOne))
        self.assertEqual(list(frame["result"]), [2, None, 4])

    def testArrowVectorFunction(self):
        @vectorFunction(pybolt.BigintType())
        @arrowAdapter
        def integrationArrowAdd(left, right):
            return pc.add(left, right)

        frame = DataFrame({"x": [1, None, 3], "y": [10, 20, 30]})
        frame.transform(result=lambda df: df["x"].map(integrationArrowAdd, df["y"]))
        self.assertEqual(list(frame["result"]), [11, None, 33])

    def testMapBatchFunction(self):
        @mapBatchFunction(pybolt.BigintType())
        def integrationBatchAdd(batch):
            x, y = batch.childAt("x"), batch.childAt("y")
            return pybolt.fromList([x[i] + y[i] for i in range(len(x))])

        frame = DataFrame({"x": [1, 2], "y": [10, 20]})
        frame.transform(result=lambda df: df["x"].map(integrationBatchAdd, df["y"]))
        self.assertEqual(list(frame["result"]), [11, 22])

    def testDataframeFunctionExecutesAndReceivesArguments(self):
        schema = pybolt.RowType(["value"], [pybolt.BigintType()])

        @dataframeFunction(schema)
        def integrationShiftFrame(frame, amount=1):
            return frame.transform(value=lambda df: df["value"] + amount)

        result = DataFrame({"value": [1, 2, 3]}).map(integrationShiftFrame, amount=4)
        self.assertEqual(list(result["value"]), [5, 6, 7])

    def testPythonProductAggregation(self):
        class IntegrationProduct(Aggregator):
            def __init__(self, value=1):
                self.value = value

            @staticmethod
            def initializer():
                return IntegrationProduct()

            @staticmethod
            def aggregate(values):
                product = 1
                for value in values:
                    if value is not None:
                        product *= value
                return IntegrationProduct(product)

            def accumulate(self, other):
                self.value *= other.value

            def reduce(self):
                return self.value

            def outputType(self):
                return pybolt.BigintType()

        frame = DataFrame({"key": ["a", "a", "b", "a"], "value": [2, 3, 5, None]})
        result = frame.groupBy("key").aggregate(
            product=IntegrationProduct()(frame["value"])
        )
        self.assertEqual(
            result.orderBy("key").toArrow(pa.Table).to_pylist(),
            [{"key": "a", "product": 6}, {"key": "b", "product": 5}],
        )

    def testTensorProducerAndNumpyConsumer(self):
        @tensorFunction(pybolt.IntegerType(), [2, 2])
        @tensorAdapter(outputDtype=np.ndarray)
        def integrationProduceTensors(ids):
            return np.arange(len(ids) * 4, dtype=np.int32).reshape(len(ids), 2, 2)

        @tensorFunction(pybolt.IntegerType(), [2, 2])
        @tensorAdapter(inputDtype=np.ndarray, outputDtype=np.ndarray)
        def integrationIncrementTensors(tensors):
            return tensors + 1

        frame = DataFrame({"id": [0, 1]})
        frame.transform(tensor=lambda df: df["id"].map(integrationProduceTensors))
        frame.transform(result=lambda df: df["tensor"].map(integrationIncrementTensors))
        self.assertEqual(frame["result"].dtype, pybolt.ArrayType(pybolt.IntegerType()))
        actual = frame["result"].data.to_numpy(shape=[2, 2])
        np.testing.assert_array_equal(
            actual, np.arange(8, dtype=np.int32).reshape(2, 2, 2) + 1
        )

    def testImageBytesThroughPillowUdf(self):
        encoded = io.BytesIO()
        Image.new("RGB", (3, 2), color=(255, 0, 0)).save(encoded, format="PNG")

        @scalarFunction(pybolt.VarbinaryType())
        def integrationResizeImage(payload):
            if payload is None:
                return None
            with Image.open(io.BytesIO(payload)) as image:
                output = io.BytesIO()
                image.resize((1, 1)).save(output, format="PNG")
                return output.getvalue()

        frame = DataFrame({"image": [encoded.getvalue(), None]})
        frame.transform(resized=lambda df: df["image"].map(integrationResizeImage))
        self.assertEqual(frame["resized"].dtype, pybolt.VarbinaryType())
        values = list(frame["resized"])
        self.assertIsNone(values[1])
        with Image.open(io.BytesIO(values[0])) as image:
            self.assertEqual(image.size, (1, 1))
            self.assertEqual(image.getpixel((0, 0)), (255, 0, 0))

    def testDownloadImagesWithoutExternalNetwork(self):
        encoded = io.BytesIO()
        Image.new("RGB", (1, 1), color=(0, 255, 0)).save(encoded, format="PNG")
        payload = encoded.getvalue()
        url = "data:image/png;base64," + base64.b64encode(payload).decode("ascii")
        frame = DataFrame({"url": [url, "data:text/plain;base64,aGVsbG8=", None]})
        frame.transform(image=lambda df: df["url"].map(downloadImages))
        self.assertEqual(frame["image"].dtype, pybolt.VarbinaryType())
        self.assertEqual(list(frame["image"]), [payload, None, None])


class KnownRegressionTests(unittest.TestCase):
    """Assert intended behavior until these known regressions are fixed."""

    # Count() currently binds the first column instead of emitting COUNT(*).
    @unittest.expectedFailure
    def testCountStarIncludesNullRows(self):
        frame = DataFrame({"value": [1, None, 3]})
        result = frame.groupBy().aggregate(rows=Count(), nonnull=Count("value"))
        self.assertEqual(
            result.toArrow(pa.Table).to_pylist(), [{"rows": 3, "nonnull": 2}]
        )

    # Dictionary string constants are currently split into individual characters.
    @unittest.expectedFailure
    def testStringConstantBroadcasts(self):
        frame = DataFrame({"id": [1, 2], "label": "ok"})
        self.assertEqual(
            frame.toArrow(pa.Table).to_pylist(),
            [{"id": 1, "label": "ok"}, {"id": 2, "label": "ok"}],
        )

    # Substrait retains an empty null bitmap that NumPy export currently rejects.
    @unittest.expectedFailure
    def testInitialNumpyTensorRemainsConvertible(self):
        @tensorFunction(pybolt.IntegerType(), [2, 2])
        @tensorAdapter(inputDtype=np.ndarray, outputDtype=np.ndarray)
        def integrationInitialTensorIncrement(tensors):
            return tensors + 1

        source = np.arange(8, dtype=np.int32).reshape(2, 2, 2)
        vector = pybolt.fromNumpy(source, pybolt.IntegerType(), [2, 2])
        frame = DataFrame({"tensor": vector})
        frame.transform(
            result=lambda df: df["tensor"].map(integrationInitialTensorIncrement)
        )
        np.testing.assert_array_equal(
            frame["result"].data.to_numpy(shape=[2, 2]), source + 1
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
