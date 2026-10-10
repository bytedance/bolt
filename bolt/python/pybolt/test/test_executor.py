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
from pybolt import (
    BoltTaskExecutor,
    PlanBuilder,
    BigintType,
    fromList,
    registerPythonFunction,
    rowVector,
)

from .utils import ALL_BOLT_TYPES_ROW_TYPE, ColumnGenerator


class TestFunctionRegistration(unittest.TestCase):
    def setUp(self):
        self.executor = BoltTaskExecutor()

    def testExecute(self):
        vec = ColumnGenerator.from_dtype(ALL_BOLT_TYPES_ROW_TYPE)(8)
        pf = PlanBuilder().values([vec]).planFragment()
        result = self.executor.execute(pf, "TestExecutor")
        self.assertEqual(vec, result)

    def runQuery(self, planBuilder, outputColumn):
        """Helper to execute a plan and extract a result column."""
        planFragment = planBuilder.planFragment()
        result = self.executor.execute(planFragment, self.id())
        return result.childAt(outputColumn)

    def assertVectorEqual(self, vector, expectedValues):
        """Helper to assert that a vector contains the expected values."""
        self.assertEqual(len(vector), len(expectedValues))
        for i, expected in enumerate(expectedValues):
            self.assertEqual(vector[i], expected, f"At index {i}")

    def testVectorFunctionRegistration(self):
        # 1. Define a python vector function
        def pythonInc(v):
            newValues = [None if val is None else val + 1 for val in v]
            return fromList(newValues, BigintType())

        # 2. Register the function
        registerPythonFunction(pythonInc, "python_inc", BigintType(), 1)

        # 3. Create input data
        inputVec = fromList([1, 2, None, 4], BigintType())
        inputRow = rowVector(["c0"], [inputVec])

        # 4. Build and run plan
        planBuilder = (
            PlanBuilder().values([inputRow]).project(["python_inc(c0) as c0_inc"])
        )
        resultVec = self.runQuery(planBuilder, "c0_inc")

        # 5. Verify
        self.assertVectorEqual(resultVec, [2, 3, None, 5])

    def testMapbatchFunctionRegistration(self):
        # 1. Define a python batch function
        def pythonSumBatch(rowVec):
            # Access children by names inferred from the rowType
            names = rowVec.dtype().names()
            c0 = rowVec.childAt(names[0])
            c1 = rowVec.childAt(names[1])

            newValues = []
            for i in range(len(rowVec)):
                v0, v1 = c0[i], c1[i]
                newValues.append(None if v0 is None or v1 is None else v0 + v1)
            return fromList(newValues, BigintType())

        # 2. Register with mapBatch=True
        registerPythonFunction(
            pythonSumBatch, "python_sum_batch", BigintType(), 2, mapBatch=True
        )

        # 3. Create input data
        c0 = fromList([1, 2, 3, None], BigintType())
        c1 = fromList([10, 20, None, 40], BigintType())
        inputRow = rowVector(["c0", "c1"], [c0, c1])

        # 4. Build and run plan
        planBuilder = (
            PlanBuilder()
            .values([inputRow])
            .project(["python_sum_batch(c0, c1) as batch_sum"])
        )
        resultVec = self.runQuery(planBuilder, "batch_sum")

        # 5. Verify
        self.assertVectorEqual(resultVec, [11, 22, None, None])


if __name__ == "__main__":
    unittest.main()
