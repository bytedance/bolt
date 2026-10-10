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

import pybolt as pb


class TestBoltTypes(unittest.TestCase):
    def testTypes(self):
        # Ensure we support all the basic types
        self.assertTrue(isinstance(pb.BooleanType(), pb.BoltType))
        self.assertTrue(isinstance(pb.IntegerType(), pb.BoltType))
        self.assertTrue(isinstance(pb.BigintType(), pb.BoltType))
        self.assertTrue(isinstance(pb.SmallintType(), pb.BoltType))
        self.assertTrue(isinstance(pb.TinyintType(), pb.BoltType))
        self.assertTrue(isinstance(pb.RealType(), pb.BoltType))
        self.assertTrue(isinstance(pb.DoubleType(), pb.BoltType))
        self.assertTrue(isinstance(pb.TimestampType(), pb.BoltType))
        self.assertTrue(isinstance(pb.VarcharType(), pb.BoltType))
        self.assertTrue(isinstance(pb.VarbinaryType(), pb.BoltType))

        self.assertEqual(pb.LongDecimalType(19, 9).kind(), pb.TypeKind.HUGEINT)
        self.assertEqual(pb.LongDecimalType(19, 9).name(), "DECIMAL")
        self.assertEqual(str(pb.LongDecimalType(19, 9)), "DECIMAL(19, 9)")
        self.assertTrue(pb.LongDecimalType(19, 9).isDecimal())
        self.assertEqual(pb.ShortDecimalType(12, 9).kind(), pb.TypeKind.BIGINT)
        self.assertEqual(pb.ShortDecimalType(12, 9).name(), "DECIMAL")
        self.assertEqual(str(pb.ShortDecimalType(12, 9)), "DECIMAL(12, 9)")
        self.assertTrue(pb.ShortDecimalType(12, 9).isDecimal())
        self.assertEqual(pb.UnknownType().kind(), pb.TypeKind.UNKNOWN)

        # Complex types
        self.assertTrue(isinstance(pb.ArrayType(pb.BooleanType()), pb.BoltType))
        self.assertTrue(
            isinstance(pb.MapType(pb.VarcharType(), pb.VarbinaryType()), pb.BoltType)
        )
        self.assertTrue(isinstance(pb.RowType(["c0"], [pb.BooleanType()]), pb.BoltType))

    def testComplexTypes(self):
        arrayType = pb.ArrayType(pb.BigintType())
        self.assertEqual(arrayType.elementType(), pb.BigintType())

        mapType = pb.MapType(pb.VarcharType(), pb.VarbinaryType())
        self.assertEqual(mapType.keyType(), pb.VarcharType())
        self.assertEqual(mapType.valueType(), pb.VarbinaryType())

        rowType = pb.RowType(
            ["c0", "c1", "c2"],
            [pb.BooleanType(), pb.BigintType(), pb.VarcharType()],
        )
        self.assertEqual(rowType.size(), 3)
        self.assertEqual(rowType.childAt(0), pb.BooleanType())
        self.assertEqual(rowType.findChild("c1"), pb.BigintType())
        self.assertEqual(rowType.getChildIdx("c1"), 1)
        self.assertEqual(rowType.nameOf(1), "c1")
        self.assertEqual(rowType.names(), ["c0", "c1", "c2"])
