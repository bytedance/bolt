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

"""Tests for the WithDataFrame testing utility.

This module contains tests demonstrating the usage of the WithDataFrame
decorator and verifying its functionality.
"""

import pybolt
from pybolt.test.utils import StringGenerator

from .. import DataFrame

from .utils import Runtime, WithDataFrame

import unittest


class WithDataFrameTest(unittest.TestCase):
    """Test cases for the WithDataFrame decorator.

    These tests verify that the decorator correctly injects DataFrames
    with the expected properties for both explicit data and random data
    generation modes.
    """

    @WithDataFrame.data(c0=[1, 0])
    def test_data_decorator_creates_dataframe(self, df):
        """Test that @WithDataFrame.data creates a valid DataFrame.

        Verifies that the decorator injects a DataFrame with the correct
        structure and content when using explicit data.
        """
        self.assertTrue(isinstance(df, DataFrame))
        self.assertEqual(df, DataFrame({"c0": [1, 0]}))

    @WithDataFrame.random(c0=StringGenerator(), length=2)
    def test_random_decorator_creates_dataframe(self, df):
        """Test that @WithDataFrame.random creates a valid DataFrame.

        Verifies that the decorator injects a DataFrame with randomly
        generated data matching the specified column types and length.
        """
        self.assertTrue(isinstance(df, DataFrame))
        self.assertEqual(df.names, ["c0"])
        self.assertEqual(len(df), 2)
        self.assertEqual(df.types[0], pybolt.VarcharType())

    @WithDataFrame.dtype(
        pybolt.RowType(["c0", "c1"], [pybolt.IntegerType(), pybolt.VarcharType()]),
        length=3,
    )
    def test_dtype_decorator_creates_dataframe(self, df):
        """Test that @WithDataFrame.dtype creates a DataFrame of the RowType."""
        self.assertTrue(isinstance(df, DataFrame))
        self.assertEqual(df.names, ["c0", "c1"])
        self.assertEqual(len(df), 3)
        self.assertEqual(df.types, [pybolt.IntegerType(), pybolt.VarcharType()])

    @WithDataFrame.factory(
        lambda plan_builder, executor: DataFrame(
            {"c0": [42], "c1": ["factory"]},
            executor=executor,
            planFactory=plan_builder,
        ),
        runtimes=Runtime.minimal(),
    )
    def test_factory_decorator_creates_dataframe(self, df):
        """Test that @WithDataFrame.factory uses the provided factory."""
        self.assertTrue(isinstance(df, DataFrame))
        self.assertEqual(df, DataFrame({"c0": [42], "c1": ["factory"]}))


if __name__ == "__main__":
    unittest.main()
