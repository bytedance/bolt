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

"""Project merging must preserve the number of Python UDF evaluations."""

import unittest

import pyarrow as pa
import pybolt

from boltml import DataFrame
from boltml.function import vectorFunction


class MergeProjectsCorrectnessTests(unittest.TestCase):
    def testRepeatedReferenceWithinExpressionEvaluatesUdfOnce(self):
        calls = []

        @vectorFunction(pybolt.BigintType())
        def mergeProjectsRepeatedInsideExpression(values):
            calls.append(len(values))
            return pybolt.fromList([len(calls)] * len(values))

        frame = DataFrame({"a": [10, 20, 30]})
        frame.transform(x=lambda df: df["a"].map(mergeProjectsRepeatedInsideExpression))
        frame.transform(z=lambda df: df["x"] + df["x"]).select(["z"])

        self.assertEqual(
            frame.toArrow(pa.Table).to_pylist(), [{"z": 2}, {"z": 2}, {"z": 2}]
        )
        self.assertEqual(calls, [3])

    def testRepeatedReferenceAcrossExpressionsEvaluatesUdfOnce(self):
        calls = []

        @vectorFunction(pybolt.BigintType())
        def mergeProjectsRepeatedAcrossExpressions(values):
            calls.append(len(values))
            return pybolt.fromList([len(calls)] * len(values))

        frame = DataFrame({"a": [10, 20, 30]})
        frame.transform(
            x=lambda df: df["a"].map(mergeProjectsRepeatedAcrossExpressions)
        )
        frame.transform(first=lambda df: df["x"] + 10, second=lambda df: df["x"] + 20)
        frame.select(["first", "second"])

        self.assertEqual(
            frame.toArrow(pa.Table).to_pylist(), [{"first": 11, "second": 21}] * 3
        )
        self.assertEqual(calls, [3])


if __name__ == "__main__":
    unittest.main()
