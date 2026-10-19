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

import unittest
from functools import wraps

from pybolt import (
    ArrayType,
    BaseVector,
    BigintType,
    BooleanType,
    BoltType,
    DoubleType,
    HugeintType,
    IntegerType,
    LongDecimalType,
    MapType,
    RealType,
    RowType,
    ShortDecimalType,
    SmallintType,
    TimestampType,
    Timestamp,
    TinyintType,
    VarbinaryType,
    VarcharType,
    fromList,
    rowVector,
)

from ..dataframe import DataFrame

from .utils import Runtime


def makeVector(dtype: BoltType) -> BaseVector:
    """
    Make a vector of one element with the target `dtype`.
    For types that should be castable from one to the other, the value in the
    vector should compare to the same value after the cast.
    """
    if any(isinstance(dtype, t) for t in (SmallintType, IntegerType, BigintType)):
        return fromList([1], dtype)
    if any(isinstance(dtype, t) for t in (RealType, DoubleType)):
        return fromList([1.0], dtype)
    if isinstance(dtype, VarcharType):
        return fromList(["boltml"], dtype)
    if isinstance(dtype, VarbinaryType):
        return fromList([b"boltml"], dtype)
    if any(isinstance(dtype, t) for t in (BooleanType, TinyintType)):
        return fromList([True], dtype)
    if isinstance(dtype, TimestampType):
        return fromList([Timestamp(0, 0)], dtype)
    if isinstance(dtype, ArrayType):
        values = makeVector(dtype.elementType())
        return fromList([[values[i] for i in range(len(values))]], dtype)
    if isinstance(dtype, MapType):
        keys = makeVector(dtype.keyType())
        values = makeVector(dtype.valueType())
        return fromList([{keys[i]: values[i] for i in range(len(keys))}], dtype)
    if isinstance(dtype, RowType):
        names = dtype.names()
        vecs = [makeVector(dtype.childAt(i)) for i in range(len(dtype))]
        return rowVector(names, vecs)
    raise unittest.SkipTest(
        f"No constructor implemented for vector of elements {dtype}"
    )


def makeColumn(dtype: BoltType, name: str = "c0"):
    vec = makeVector(dtype)
    names = [name]
    return DataFrame(rowVector(names, [vec]))[name]


def parameters(lhsType: BoltType, rhsType: BoltType, validCast: bool = True):
    """
    Decorator for the tests of ColumnCastTest that tries to cast data of
    `lhsType` into data of `rhsType`.
    If `validCast` is True, then the test assumes that the cast is
    successful and it assesses that the result of the cast is correct.
    If `validCast` is False, then the test assumes that the cast should
    fail with a `TypeError`.
    """

    def decorator(fn):
        @wraps(fn)
        def wrapper(self):
            for runtime in Runtime.all():
                with self.subTest(str(runtime)):
                    lhs = DataFrame(
                        {"c0": makeVector(lhsType)},
                        executor=runtime.executor,
                        planFactory=runtime.planFactory,
                    )["c0"]
                    rhs = DataFrame(
                        {"c0": makeVector(rhsType)},
                        executor=runtime.executor,
                        planFactory=runtime.planFactory,
                    )["c0"]
                    if validCast:
                        self.assertTrue(lhs.cast(rhsType).equals(rhs))
                    else:
                        with self.assertRaises(TypeError):
                            lhs.cast(rhsType)

        return wrapper

    return decorator


class ColumnCastTest(unittest.TestCase):
    @parameters(TinyintType(), TinyintType(), validCast=True)
    def testTinyintType_TinyintType(self): ...

    @parameters(TinyintType(), SmallintType(), validCast=True)
    def testTinyintType_SmallintType(self): ...

    @parameters(TinyintType(), IntegerType(), validCast=True)
    def testTinyintType_IntegerType(self): ...

    @parameters(TinyintType(), BigintType(), validCast=True)
    def testTinyintType_BigintType(self): ...

    @parameters(TinyintType(), HugeintType(), validCast=True)
    def testTinyintType_HugeintType(self): ...

    @parameters(TinyintType(), BooleanType(), validCast=True)
    def testTinyintType_BooleanType(self): ...

    @parameters(TinyintType(), RealType(), validCast=True)
    def testTinyintType_RealType(self): ...

    @parameters(TinyintType(), DoubleType(), validCast=True)
    def testTinyintType_DoubleType(self): ...

    @parameters(TinyintType(), TimestampType(), validCast=False)
    def testTinyintType_TimestampType(self): ...

    @parameters(TinyintType(), VarcharType(), validCast=False)
    def testTinyintType_VarcharType(self): ...

    @parameters(TinyintType(), VarbinaryType(), validCast=False)
    def testTinyintType_VarbinaryType(self): ...

    @parameters(TinyintType(), ArrayType(IntegerType()), validCast=False)
    def testTinyintType_ArrayTypeIntegerType(self): ...

    @parameters(TinyintType(), ArrayType(BigintType()), validCast=False)
    def testTinyintType_ArrayTypeBigintType(self): ...

    @parameters(TinyintType(), ArrayType(VarcharType()), validCast=False)
    def testTinyintType_ArrayTypeVarcharType(self): ...

    @parameters(TinyintType(), ShortDecimalType(18, 2), validCast=False)
    def testTinyintType_ShortDecimalType182(self): ...

    @parameters(TinyintType(), LongDecimalType(19, 2), validCast=False)
    def testTinyintType_LongDecimalType192(self): ...

    @parameters(TinyintType(), MapType(VarcharType(), IntegerType()), validCast=False)
    def testTinyintType_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(TinyintType(), MapType(VarcharType(), BigintType()), validCast=False)
    def testTinyintType_MapTypeVarcharTypeBigintType(self): ...

    @parameters(SmallintType(), TinyintType(), validCast=True)
    def testSmallintType_TinyintType(self): ...

    @parameters(SmallintType(), SmallintType(), validCast=True)
    def testSmallintType_SmallintType(self): ...

    @parameters(SmallintType(), IntegerType(), validCast=True)
    def testSmallintType_IntegerType(self): ...

    @parameters(SmallintType(), BigintType(), validCast=True)
    def testSmallintType_BigintType(self): ...

    @parameters(SmallintType(), HugeintType(), validCast=True)
    def testSmallintType_HugeintType(self): ...

    @parameters(SmallintType(), BooleanType(), validCast=True)
    def testSmallintType_BooleanType(self): ...

    @parameters(SmallintType(), RealType(), validCast=True)
    def testSmallintType_RealType(self): ...

    @parameters(SmallintType(), DoubleType(), validCast=True)
    def testSmallintType_DoubleType(self): ...

    @parameters(SmallintType(), TimestampType(), validCast=False)
    def testSmallintType_TimestampType(self): ...

    @parameters(SmallintType(), VarcharType(), validCast=False)
    def testSmallintType_VarcharType(self): ...

    @parameters(SmallintType(), VarbinaryType(), validCast=False)
    def testSmallintType_VarbinaryType(self): ...

    @parameters(SmallintType(), ArrayType(IntegerType()), validCast=False)
    def testSmallintType_ArrayTypeIntegerType(self): ...

    @parameters(SmallintType(), ArrayType(BigintType()), validCast=False)
    def testSmallintType_ArrayTypeBigintType(self): ...

    @parameters(SmallintType(), ArrayType(VarcharType()), validCast=False)
    def testSmallintType_ArrayTypeVarcharType(self): ...

    @parameters(SmallintType(), ShortDecimalType(18, 2), validCast=False)
    def testSmallintType_ShortDecimalType182(self): ...

    @parameters(SmallintType(), LongDecimalType(19, 2), validCast=False)
    def testSmallintType_LongDecimalType192(self): ...

    @parameters(SmallintType(), MapType(VarcharType(), IntegerType()), validCast=False)
    def testSmallintType_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(SmallintType(), MapType(VarcharType(), BigintType()), validCast=False)
    def testSmallintType_MapTypeVarcharTypeBigintType(self): ...

    @parameters(IntegerType(), TinyintType(), validCast=True)
    def testIntegerType_TinyintType(self): ...

    @parameters(IntegerType(), SmallintType(), validCast=True)
    def testIntegerType_SmallintType(self): ...

    @parameters(IntegerType(), IntegerType(), validCast=True)
    def testIntegerType_IntegerType(self): ...

    @parameters(IntegerType(), BigintType(), validCast=True)
    def testIntegerType_BigintType(self): ...

    @parameters(IntegerType(), HugeintType(), validCast=True)
    def testIntegerType_HugeintType(self): ...

    @parameters(IntegerType(), BooleanType(), validCast=True)
    def testIntegerType_BooleanType(self): ...

    @parameters(IntegerType(), RealType(), validCast=True)
    def testIntegerType_RealType(self): ...

    @parameters(IntegerType(), DoubleType(), validCast=True)
    def testIntegerType_DoubleType(self): ...

    @parameters(IntegerType(), TimestampType(), validCast=False)
    def testIntegerType_TimestampType(self): ...

    @parameters(IntegerType(), VarcharType(), validCast=False)
    def testIntegerType_VarcharType(self): ...

    @parameters(IntegerType(), VarbinaryType(), validCast=False)
    def testIntegerType_VarbinaryType(self): ...

    @parameters(IntegerType(), ArrayType(IntegerType()), validCast=False)
    def testIntegerType_ArrayTypeIntegerType(self): ...

    @parameters(IntegerType(), ArrayType(BigintType()), validCast=False)
    def testIntegerType_ArrayTypeBigintType(self): ...

    @parameters(IntegerType(), ArrayType(VarcharType()), validCast=False)
    def testIntegerType_ArrayTypeVarcharType(self): ...

    @parameters(IntegerType(), ShortDecimalType(18, 2), validCast=False)
    def testIntegerType_ShortDecimalType182(self): ...

    @parameters(IntegerType(), LongDecimalType(19, 2), validCast=False)
    def testIntegerType_LongDecimalType192(self): ...

    @parameters(IntegerType(), MapType(VarcharType(), IntegerType()), validCast=False)
    def testIntegerType_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(IntegerType(), MapType(VarcharType(), BigintType()), validCast=False)
    def testIntegerType_MapTypeVarcharTypeBigintType(self): ...

    @parameters(BigintType(), TinyintType(), validCast=True)
    def testBigintType_TinyintType(self): ...

    @parameters(BigintType(), SmallintType(), validCast=True)
    def testBigintType_SmallintType(self): ...

    @parameters(BigintType(), IntegerType(), validCast=True)
    def testBigintType_IntegerType(self): ...

    @parameters(BigintType(), BigintType(), validCast=True)
    def testBigintType_BigintType(self): ...

    @parameters(BigintType(), HugeintType(), validCast=True)
    def testBigintType_HugeintType(self): ...

    @parameters(BigintType(), BooleanType(), validCast=True)
    def testBigintType_BooleanType(self): ...

    @parameters(BigintType(), RealType(), validCast=True)
    def testBigintType_RealType(self): ...

    @parameters(BigintType(), DoubleType(), validCast=True)
    def testBigintType_DoubleType(self): ...

    @parameters(BigintType(), TimestampType(), validCast=False)
    def testBigintType_TimestampType(self): ...

    @parameters(BigintType(), VarcharType(), validCast=False)
    def testBigintType_VarcharType(self): ...

    @parameters(BigintType(), VarbinaryType(), validCast=False)
    def testBigintType_VarbinaryType(self): ...

    @parameters(BigintType(), ArrayType(IntegerType()), validCast=False)
    def testBigintType_ArrayTypeIntegerType(self): ...

    @parameters(BigintType(), ArrayType(BigintType()), validCast=False)
    def testBigintType_ArrayTypeBigintType(self): ...

    @parameters(BigintType(), ArrayType(VarcharType()), validCast=False)
    def testBigintType_ArrayTypeVarcharType(self): ...

    @parameters(BigintType(), ShortDecimalType(18, 2), validCast=False)
    def testBigintType_ShortDecimalType182(self): ...

    @parameters(BigintType(), LongDecimalType(19, 2), validCast=False)
    def testBigintType_LongDecimalType192(self): ...

    @parameters(BigintType(), MapType(VarcharType(), IntegerType()), validCast=False)
    def testBigintType_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(BigintType(), MapType(VarcharType(), BigintType()), validCast=False)
    def testBigintType_MapTypeVarcharTypeBigintType(self): ...

    @parameters(HugeintType(), TinyintType(), validCast=True)
    def testHugeintType_TinyintType(self): ...

    @parameters(HugeintType(), SmallintType(), validCast=True)
    def testHugeintType_SmallintType(self): ...

    @parameters(HugeintType(), IntegerType(), validCast=True)
    def testHugeintType_IntegerType(self): ...

    @parameters(HugeintType(), BigintType(), validCast=True)
    def testHugeintType_BigintType(self): ...

    @parameters(HugeintType(), HugeintType(), validCast=True)
    def testHugeintType_HugeintType(self): ...

    @parameters(HugeintType(), BooleanType(), validCast=True)
    def testHugeintType_BooleanType(self): ...

    @parameters(HugeintType(), RealType(), validCast=True)
    def testHugeintType_RealType(self): ...

    @parameters(HugeintType(), DoubleType(), validCast=True)
    def testHugeintType_DoubleType(self): ...

    @parameters(HugeintType(), TimestampType(), validCast=False)
    def testHugeintType_TimestampType(self): ...

    @parameters(HugeintType(), VarcharType(), validCast=False)
    def testHugeintType_VarcharType(self): ...

    @parameters(HugeintType(), VarbinaryType(), validCast=False)
    def testHugeintType_VarbinaryType(self): ...

    @parameters(HugeintType(), ArrayType(IntegerType()), validCast=False)
    def testHugeintType_ArrayTypeIntegerType(self): ...

    @parameters(HugeintType(), ArrayType(BigintType()), validCast=False)
    def testHugeintType_ArrayTypeBigintType(self): ...

    @parameters(HugeintType(), ArrayType(VarcharType()), validCast=False)
    def testHugeintType_ArrayTypeVarcharType(self): ...

    @parameters(HugeintType(), ShortDecimalType(18, 2), validCast=False)
    def testHugeintType_ShortDecimalType182(self): ...

    @parameters(HugeintType(), LongDecimalType(19, 2), validCast=False)
    def testHugeintType_LongDecimalType192(self): ...

    @parameters(HugeintType(), MapType(VarcharType(), IntegerType()), validCast=False)
    def testHugeintType_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(HugeintType(), MapType(VarcharType(), BigintType()), validCast=False)
    def testHugeintType_MapTypeVarcharTypeBigintType(self): ...

    @parameters(BooleanType(), TinyintType(), validCast=True)
    def testBooleanType_TinyintType(self): ...

    @parameters(BooleanType(), SmallintType(), validCast=True)
    def testBooleanType_SmallintType(self): ...

    @parameters(BooleanType(), IntegerType(), validCast=True)
    def testBooleanType_IntegerType(self): ...

    @parameters(BooleanType(), BigintType(), validCast=True)
    def testBooleanType_BigintType(self): ...

    @parameters(BooleanType(), HugeintType(), validCast=True)
    def testBooleanType_HugeintType(self): ...

    @parameters(BooleanType(), BooleanType(), validCast=True)
    def testBooleanType_BooleanType(self): ...

    @parameters(BooleanType(), RealType(), validCast=True)
    def testBooleanType_RealType(self): ...

    @parameters(BooleanType(), DoubleType(), validCast=True)
    def testBooleanType_DoubleType(self): ...

    @parameters(BooleanType(), TimestampType(), validCast=False)
    def testBooleanType_TimestampType(self): ...

    @parameters(BooleanType(), VarcharType(), validCast=False)
    def testBooleanType_VarcharType(self): ...

    @parameters(BooleanType(), VarbinaryType(), validCast=False)
    def testBooleanType_VarbinaryType(self): ...

    @parameters(BooleanType(), ArrayType(IntegerType()), validCast=False)
    def testBooleanType_ArrayTypeIntegerType(self): ...

    @parameters(BooleanType(), ArrayType(BigintType()), validCast=False)
    def testBooleanType_ArrayTypeBigintType(self): ...

    @parameters(BooleanType(), ArrayType(VarcharType()), validCast=False)
    def testBooleanType_ArrayTypeVarcharType(self): ...

    @parameters(BooleanType(), ShortDecimalType(18, 2), validCast=False)
    def testBooleanType_ShortDecimalType182(self): ...

    @parameters(BooleanType(), LongDecimalType(19, 2), validCast=False)
    def testBooleanType_LongDecimalType192(self): ...

    @parameters(BooleanType(), MapType(VarcharType(), IntegerType()), validCast=False)
    def testBooleanType_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(BooleanType(), MapType(VarcharType(), BigintType()), validCast=False)
    def testBooleanType_MapTypeVarcharTypeBigintType(self): ...

    @parameters(RealType(), TinyintType(), validCast=False)
    def testRealType_TinyintType(self): ...

    @parameters(RealType(), SmallintType(), validCast=False)
    def testRealType_SmallintType(self): ...

    @parameters(RealType(), IntegerType(), validCast=False)
    def testRealType_IntegerType(self): ...

    @parameters(RealType(), BigintType(), validCast=False)
    def testRealType_BigintType(self): ...

    @parameters(RealType(), HugeintType(), validCast=False)
    def testRealType_HugeintType(self): ...

    @parameters(RealType(), BooleanType(), validCast=False)
    def testRealType_BooleanType(self): ...

    @parameters(RealType(), RealType(), validCast=True)
    def testRealType_RealType(self): ...

    @parameters(RealType(), DoubleType(), validCast=True)
    def testRealType_DoubleType(self): ...

    @parameters(RealType(), TimestampType(), validCast=False)
    def testRealType_TimestampType(self): ...

    @parameters(RealType(), VarcharType(), validCast=False)
    def testRealType_VarcharType(self): ...

    @parameters(RealType(), VarbinaryType(), validCast=False)
    def testRealType_VarbinaryType(self): ...

    @parameters(RealType(), ArrayType(IntegerType()), validCast=False)
    def testRealType_ArrayTypeIntegerType(self): ...

    @parameters(RealType(), ArrayType(BigintType()), validCast=False)
    def testRealType_ArrayTypeBigintType(self): ...

    @parameters(RealType(), ArrayType(VarcharType()), validCast=False)
    def testRealType_ArrayTypeVarcharType(self): ...

    @parameters(RealType(), ShortDecimalType(18, 2), validCast=False)
    def testRealType_ShortDecimalType182(self): ...

    @parameters(RealType(), LongDecimalType(19, 2), validCast=False)
    def testRealType_LongDecimalType192(self): ...

    @parameters(RealType(), MapType(VarcharType(), IntegerType()), validCast=False)
    def testRealType_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(RealType(), MapType(VarcharType(), BigintType()), validCast=False)
    def testRealType_MapTypeVarcharTypeBigintType(self): ...

    @parameters(DoubleType(), TinyintType(), validCast=False)
    def testDoubleType_TinyintType(self): ...

    @parameters(DoubleType(), SmallintType(), validCast=False)
    def testDoubleType_SmallintType(self): ...

    @parameters(DoubleType(), IntegerType(), validCast=False)
    def testDoubleType_IntegerType(self): ...

    @parameters(DoubleType(), BigintType(), validCast=False)
    def testDoubleType_BigintType(self): ...

    @parameters(DoubleType(), HugeintType(), validCast=False)
    def testDoubleType_HugeintType(self): ...

    @parameters(DoubleType(), BooleanType(), validCast=False)
    def testDoubleType_BooleanType(self): ...

    @parameters(DoubleType(), RealType(), validCast=True)
    def testDoubleType_RealType(self): ...

    @parameters(DoubleType(), DoubleType(), validCast=True)
    def testDoubleType_DoubleType(self): ...

    @parameters(DoubleType(), TimestampType(), validCast=False)
    def testDoubleType_TimestampType(self): ...

    @parameters(DoubleType(), VarcharType(), validCast=False)
    def testDoubleType_VarcharType(self): ...

    @parameters(DoubleType(), VarbinaryType(), validCast=False)
    def testDoubleType_VarbinaryType(self): ...

    @parameters(DoubleType(), ArrayType(IntegerType()), validCast=False)
    def testDoubleType_ArrayTypeIntegerType(self): ...

    @parameters(DoubleType(), ArrayType(BigintType()), validCast=False)
    def testDoubleType_ArrayTypeBigintType(self): ...

    @parameters(DoubleType(), ArrayType(VarcharType()), validCast=False)
    def testDoubleType_ArrayTypeVarcharType(self): ...

    @parameters(DoubleType(), ShortDecimalType(18, 2), validCast=False)
    def testDoubleType_ShortDecimalType182(self): ...

    @parameters(DoubleType(), LongDecimalType(19, 2), validCast=False)
    def testDoubleType_LongDecimalType192(self): ...

    @parameters(DoubleType(), MapType(VarcharType(), IntegerType()), validCast=False)
    def testDoubleType_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(DoubleType(), MapType(VarcharType(), BigintType()), validCast=False)
    def testDoubleType_MapTypeVarcharTypeBigintType(self): ...

    @parameters(TimestampType(), TinyintType(), validCast=False)
    def testTimestampType_TinyintType(self): ...

    @parameters(TimestampType(), SmallintType(), validCast=False)
    def testTimestampType_SmallintType(self): ...

    @parameters(TimestampType(), IntegerType(), validCast=False)
    def testTimestampType_IntegerType(self): ...

    @parameters(TimestampType(), BigintType(), validCast=False)
    def testTimestampType_BigintType(self): ...

    @parameters(TimestampType(), HugeintType(), validCast=False)
    def testTimestampType_HugeintType(self): ...

    @parameters(TimestampType(), BooleanType(), validCast=False)
    def testTimestampType_BooleanType(self): ...

    @parameters(TimestampType(), RealType(), validCast=False)
    def testTimestampType_RealType(self): ...

    @parameters(TimestampType(), DoubleType(), validCast=False)
    def testTimestampType_DoubleType(self): ...

    @parameters(TimestampType(), TimestampType(), validCast=False)
    def testTimestampType_TimestampType(self): ...

    @parameters(TimestampType(), VarcharType(), validCast=False)
    def testTimestampType_VarcharType(self): ...

    @parameters(TimestampType(), VarbinaryType(), validCast=False)
    def testTimestampType_VarbinaryType(self): ...

    @parameters(TimestampType(), ArrayType(IntegerType()), validCast=False)
    def testTimestampType_ArrayTypeIntegerType(self): ...

    @parameters(TimestampType(), ArrayType(BigintType()), validCast=False)
    def testTimestampType_ArrayTypeBigintType(self): ...

    @parameters(TimestampType(), ArrayType(VarcharType()), validCast=False)
    def testTimestampType_ArrayTypeVarcharType(self): ...

    @parameters(TimestampType(), ShortDecimalType(18, 2), validCast=False)
    def testTimestampType_ShortDecimalType182(self): ...

    @parameters(TimestampType(), LongDecimalType(19, 2), validCast=False)
    def testTimestampType_LongDecimalType192(self): ...

    @parameters(TimestampType(), MapType(VarcharType(), IntegerType()), validCast=False)
    def testTimestampType_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(TimestampType(), MapType(VarcharType(), BigintType()), validCast=False)
    def testTimestampType_MapTypeVarcharTypeBigintType(self): ...

    @parameters(VarcharType(), TinyintType(), validCast=False)
    def testVarcharType_TinyintType(self): ...

    @parameters(VarcharType(), SmallintType(), validCast=False)
    def testVarcharType_SmallintType(self): ...

    @parameters(VarcharType(), IntegerType(), validCast=False)
    def testVarcharType_IntegerType(self): ...

    @parameters(VarcharType(), BigintType(), validCast=False)
    def testVarcharType_BigintType(self): ...

    @parameters(VarcharType(), HugeintType(), validCast=False)
    def testVarcharType_HugeintType(self): ...

    @parameters(VarcharType(), BooleanType(), validCast=False)
    def testVarcharType_BooleanType(self): ...

    @parameters(VarcharType(), RealType(), validCast=False)
    def testVarcharType_RealType(self): ...

    @parameters(VarcharType(), DoubleType(), validCast=False)
    def testVarcharType_DoubleType(self): ...

    @parameters(VarcharType(), TimestampType(), validCast=False)
    def testVarcharType_TimestampType(self): ...

    @parameters(VarcharType(), VarcharType(), validCast=True)
    def testVarcharType_VarcharType(self): ...

    @parameters(VarcharType(), VarbinaryType(), validCast=True)
    def testVarcharType_VarbinaryType(self): ...

    @parameters(VarcharType(), ArrayType(IntegerType()), validCast=False)
    def testVarcharType_ArrayTypeIntegerType(self): ...

    @parameters(VarcharType(), ArrayType(BigintType()), validCast=False)
    def testVarcharType_ArrayTypeBigintType(self): ...

    @parameters(VarcharType(), ArrayType(VarcharType()), validCast=False)
    def testVarcharType_ArrayTypeVarcharType(self): ...

    @parameters(VarcharType(), ShortDecimalType(18, 2), validCast=False)
    def testVarcharType_ShortDecimalType182(self): ...

    @parameters(VarcharType(), LongDecimalType(19, 2), validCast=False)
    def testVarcharType_LongDecimalType192(self): ...

    @parameters(VarcharType(), MapType(VarcharType(), IntegerType()), validCast=False)
    def testVarcharType_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(VarcharType(), MapType(VarcharType(), BigintType()), validCast=False)
    def testVarcharType_MapTypeVarcharTypeBigintType(self): ...

    @parameters(VarbinaryType(), TinyintType(), validCast=False)
    def testVarbinaryType_TinyintType(self): ...

    @parameters(VarbinaryType(), SmallintType(), validCast=False)
    def testVarbinaryType_SmallintType(self): ...

    @parameters(VarbinaryType(), IntegerType(), validCast=False)
    def testVarbinaryType_IntegerType(self): ...

    @parameters(VarbinaryType(), BigintType(), validCast=False)
    def testVarbinaryType_BigintType(self): ...

    @parameters(VarbinaryType(), HugeintType(), validCast=False)
    def testVarbinaryType_HugeintType(self): ...

    @parameters(VarbinaryType(), BooleanType(), validCast=False)
    def testVarbinaryType_BooleanType(self): ...

    @parameters(VarbinaryType(), RealType(), validCast=False)
    def testVarbinaryType_RealType(self): ...

    @parameters(VarbinaryType(), DoubleType(), validCast=False)
    def testVarbinaryType_DoubleType(self): ...

    @parameters(VarbinaryType(), TimestampType(), validCast=False)
    def testVarbinaryType_TimestampType(self): ...

    @parameters(VarbinaryType(), VarcharType(), validCast=True)
    def testVarbinaryType_VarcharType(self): ...

    @parameters(VarbinaryType(), VarbinaryType(), validCast=True)
    def testVarbinaryType_VarbinaryType(self): ...

    @parameters(VarbinaryType(), ArrayType(IntegerType()), validCast=False)
    def testVarbinaryType_ArrayTypeIntegerType(self): ...

    @parameters(VarbinaryType(), ArrayType(BigintType()), validCast=False)
    def testVarbinaryType_ArrayTypeBigintType(self): ...

    @parameters(VarbinaryType(), ArrayType(VarcharType()), validCast=False)
    def testVarbinaryType_ArrayTypeVarcharType(self): ...

    @parameters(VarbinaryType(), ShortDecimalType(18, 2), validCast=False)
    def testVarbinaryType_ShortDecimalType182(self): ...

    @parameters(VarbinaryType(), LongDecimalType(19, 2), validCast=False)
    def testVarbinaryType_LongDecimalType192(self): ...

    @parameters(VarbinaryType(), MapType(VarcharType(), IntegerType()), validCast=False)
    def testVarbinaryType_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(VarbinaryType(), MapType(VarcharType(), BigintType()), validCast=False)
    def testVarbinaryType_MapTypeVarcharTypeBigintType(self): ...

    @parameters(ArrayType(IntegerType()), TinyintType(), validCast=False)
    def testArrayTypeIntegerType_TinyintType(self): ...

    @parameters(ArrayType(IntegerType()), SmallintType(), validCast=False)
    def testArrayTypeIntegerType_SmallintType(self): ...

    @parameters(ArrayType(IntegerType()), IntegerType(), validCast=False)
    def testArrayTypeIntegerType_IntegerType(self): ...

    @parameters(ArrayType(IntegerType()), BigintType(), validCast=False)
    def testArrayTypeIntegerType_BigintType(self): ...

    @parameters(ArrayType(IntegerType()), HugeintType(), validCast=False)
    def testArrayTypeIntegerType_HugeintType(self): ...

    @parameters(ArrayType(IntegerType()), BooleanType(), validCast=False)
    def testArrayTypeIntegerType_BooleanType(self): ...

    @parameters(ArrayType(IntegerType()), RealType(), validCast=False)
    def testArrayTypeIntegerType_RealType(self): ...

    @parameters(ArrayType(IntegerType()), DoubleType(), validCast=False)
    def testArrayTypeIntegerType_DoubleType(self): ...

    @parameters(ArrayType(IntegerType()), TimestampType(), validCast=False)
    def testArrayTypeIntegerType_TimestampType(self): ...

    @parameters(ArrayType(IntegerType()), VarcharType(), validCast=False)
    def testArrayTypeIntegerType_VarcharType(self): ...

    @parameters(ArrayType(IntegerType()), VarbinaryType(), validCast=False)
    def testArrayTypeIntegerType_VarbinaryType(self): ...

    @parameters(ArrayType(IntegerType()), ArrayType(IntegerType()), validCast=True)
    def testArrayTypeIntegerType_ArrayTypeIntegerType(self): ...

    @parameters(ArrayType(IntegerType()), ArrayType(BigintType()), validCast=True)
    def testArrayTypeIntegerType_ArrayTypeBigintType(self): ...

    @parameters(ArrayType(IntegerType()), ArrayType(VarcharType()), validCast=False)
    def testArrayTypeIntegerType_ArrayTypeVarcharType(self): ...

    @parameters(ArrayType(IntegerType()), ShortDecimalType(18, 2), validCast=False)
    def testArrayTypeIntegerType_ShortDecimalType182(self): ...

    @parameters(ArrayType(IntegerType()), LongDecimalType(19, 2), validCast=False)
    def testArrayTypeIntegerType_LongDecimalType192(self): ...

    @parameters(
        ArrayType(IntegerType()), MapType(VarcharType(), IntegerType()), validCast=False
    )
    def testArrayTypeIntegerType_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(
        ArrayType(IntegerType()), MapType(VarcharType(), BigintType()), validCast=False
    )
    def testArrayTypeIntegerType_MapTypeVarcharTypeBigintType(self): ...

    @parameters(ArrayType(BigintType()), TinyintType(), validCast=False)
    def testArrayTypeBigintType_TinyintType(self): ...

    @parameters(ArrayType(BigintType()), SmallintType(), validCast=False)
    def testArrayTypeBigintType_SmallintType(self): ...

    @parameters(ArrayType(BigintType()), IntegerType(), validCast=False)
    def testArrayTypeBigintType_IntegerType(self): ...

    @parameters(ArrayType(BigintType()), BigintType(), validCast=False)
    def testArrayTypeBigintType_BigintType(self): ...

    @parameters(ArrayType(BigintType()), HugeintType(), validCast=False)
    def testArrayTypeBigintType_HugeintType(self): ...

    @parameters(ArrayType(BigintType()), BooleanType(), validCast=False)
    def testArrayTypeBigintType_BooleanType(self): ...

    @parameters(ArrayType(BigintType()), RealType(), validCast=False)
    def testArrayTypeBigintType_RealType(self): ...

    @parameters(ArrayType(BigintType()), DoubleType(), validCast=False)
    def testArrayTypeBigintType_DoubleType(self): ...

    @parameters(ArrayType(BigintType()), TimestampType(), validCast=False)
    def testArrayTypeBigintType_TimestampType(self): ...

    @parameters(ArrayType(BigintType()), VarcharType(), validCast=False)
    def testArrayTypeBigintType_VarcharType(self): ...

    @parameters(ArrayType(BigintType()), VarbinaryType(), validCast=False)
    def testArrayTypeBigintType_VarbinaryType(self): ...

    @parameters(ArrayType(BigintType()), ArrayType(IntegerType()), validCast=True)
    def testArrayTypeBigintType_ArrayTypeIntegerType(self): ...

    @parameters(ArrayType(BigintType()), ArrayType(BigintType()), validCast=True)
    def testArrayTypeBigintType_ArrayTypeBigintType(self): ...

    @parameters(ArrayType(BigintType()), ArrayType(VarcharType()), validCast=False)
    def testArrayTypeBigintType_ArrayTypeVarcharType(self): ...

    @parameters(ArrayType(BigintType()), ShortDecimalType(18, 2), validCast=False)
    def testArrayTypeBigintType_ShortDecimalType182(self): ...

    @parameters(ArrayType(BigintType()), LongDecimalType(19, 2), validCast=False)
    def testArrayTypeBigintType_LongDecimalType192(self): ...

    @parameters(
        ArrayType(BigintType()), MapType(VarcharType(), IntegerType()), validCast=False
    )
    def testArrayTypeBigintType_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(
        ArrayType(BigintType()), MapType(VarcharType(), BigintType()), validCast=False
    )
    def testArrayTypeBigintType_MapTypeVarcharTypeBigintType(self): ...

    @parameters(ArrayType(VarcharType()), TinyintType(), validCast=False)
    def testArrayTypeVarcharType_TinyintType(self): ...

    @parameters(ArrayType(VarcharType()), SmallintType(), validCast=False)
    def testArrayTypeVarcharType_SmallintType(self): ...

    @parameters(ArrayType(VarcharType()), IntegerType(), validCast=False)
    def testArrayTypeVarcharType_IntegerType(self): ...

    @parameters(ArrayType(VarcharType()), BigintType(), validCast=False)
    def testArrayTypeVarcharType_BigintType(self): ...

    @parameters(ArrayType(VarcharType()), HugeintType(), validCast=False)
    def testArrayTypeVarcharType_HugeintType(self): ...

    @parameters(ArrayType(VarcharType()), BooleanType(), validCast=False)
    def testArrayTypeVarcharType_BooleanType(self): ...

    @parameters(ArrayType(VarcharType()), RealType(), validCast=False)
    def testArrayTypeVarcharType_RealType(self): ...

    @parameters(ArrayType(VarcharType()), DoubleType(), validCast=False)
    def testArrayTypeVarcharType_DoubleType(self): ...

    @parameters(ArrayType(VarcharType()), TimestampType(), validCast=False)
    def testArrayTypeVarcharType_TimestampType(self): ...

    @parameters(ArrayType(VarcharType()), VarcharType(), validCast=False)
    def testArrayTypeVarcharType_VarcharType(self): ...

    @parameters(ArrayType(VarcharType()), VarbinaryType(), validCast=False)
    def testArrayTypeVarcharType_VarbinaryType(self): ...

    @parameters(ArrayType(VarcharType()), ArrayType(IntegerType()), validCast=False)
    def testArrayTypeVarcharType_ArrayTypeIntegerType(self): ...

    @parameters(ArrayType(VarcharType()), ArrayType(BigintType()), validCast=False)
    def testArrayTypeVarcharType_ArrayTypeBigintType(self): ...

    @parameters(ArrayType(VarcharType()), ArrayType(VarcharType()), validCast=True)
    def testArrayTypeVarcharType_ArrayTypeVarcharType(self): ...

    @parameters(ArrayType(VarcharType()), ShortDecimalType(18, 2), validCast=False)
    def testArrayTypeVarcharType_ShortDecimalType182(self): ...

    @parameters(ArrayType(VarcharType()), LongDecimalType(19, 2), validCast=False)
    def testArrayTypeVarcharType_LongDecimalType192(self): ...

    @parameters(
        ArrayType(VarcharType()), MapType(VarcharType(), IntegerType()), validCast=False
    )
    def testArrayTypeVarcharType_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(
        ArrayType(VarcharType()), MapType(VarcharType(), BigintType()), validCast=False
    )
    def testArrayTypeVarcharType_MapTypeVarcharTypeBigintType(self): ...

    @parameters(ShortDecimalType(18, 2), TinyintType(), validCast=False)
    def testShortDecimalType182_TinyintType(self): ...

    @parameters(ShortDecimalType(18, 2), SmallintType(), validCast=False)
    def testShortDecimalType182_SmallintType(self): ...

    @parameters(ShortDecimalType(18, 2), IntegerType(), validCast=False)
    def testShortDecimalType182_IntegerType(self): ...

    @parameters(ShortDecimalType(18, 2), BigintType(), validCast=False)
    def testShortDecimalType182_BigintType(self): ...

    @parameters(ShortDecimalType(18, 2), HugeintType(), validCast=False)
    def testShortDecimalType182_HugeintType(self): ...

    @parameters(ShortDecimalType(18, 2), BooleanType(), validCast=False)
    def testShortDecimalType182_BooleanType(self): ...

    @parameters(ShortDecimalType(18, 2), RealType(), validCast=False)
    def testShortDecimalType182_RealType(self): ...

    @parameters(ShortDecimalType(18, 2), DoubleType(), validCast=False)
    def testShortDecimalType182_DoubleType(self): ...

    @parameters(ShortDecimalType(18, 2), TimestampType(), validCast=False)
    def testShortDecimalType182_TimestampType(self): ...

    @parameters(ShortDecimalType(18, 2), VarcharType(), validCast=False)
    def testShortDecimalType182_VarcharType(self): ...

    @parameters(ShortDecimalType(18, 2), VarbinaryType(), validCast=False)
    def testShortDecimalType182_VarbinaryType(self): ...

    @parameters(ShortDecimalType(18, 2), ArrayType(IntegerType()), validCast=False)
    def testShortDecimalType182_ArrayTypeIntegerType(self): ...

    @parameters(ShortDecimalType(18, 2), ArrayType(BigintType()), validCast=False)
    def testShortDecimalType182_ArrayTypeBigintType(self): ...

    @parameters(ShortDecimalType(18, 2), ArrayType(VarcharType()), validCast=False)
    def testShortDecimalType182_ArrayTypeVarcharType(self): ...

    @parameters(ShortDecimalType(18, 2), ShortDecimalType(18, 2), validCast=True)
    def testShortDecimalType182_ShortDecimalType182(self): ...

    @parameters(ShortDecimalType(18, 2), LongDecimalType(19, 2), validCast=True)
    def testShortDecimalType182_LongDecimalType192(self): ...

    @parameters(
        ShortDecimalType(18, 2), MapType(VarcharType(), IntegerType()), validCast=False
    )
    def testShortDecimalType182_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(
        ShortDecimalType(18, 2), MapType(VarcharType(), BigintType()), validCast=False
    )
    def testShortDecimalType182_MapTypeVarcharTypeBigintType(self): ...

    @parameters(LongDecimalType(19, 2), TinyintType(), validCast=False)
    def testLongDecimalType192_TinyintType(self): ...

    @parameters(LongDecimalType(19, 2), SmallintType(), validCast=False)
    def testLongDecimalType192_SmallintType(self): ...

    @parameters(LongDecimalType(19, 2), IntegerType(), validCast=False)
    def testLongDecimalType192_IntegerType(self): ...

    @parameters(LongDecimalType(19, 2), BigintType(), validCast=False)
    def testLongDecimalType192_BigintType(self): ...

    @parameters(LongDecimalType(19, 2), HugeintType(), validCast=False)
    def testLongDecimalType192_HugeintType(self): ...

    @parameters(LongDecimalType(19, 2), BooleanType(), validCast=False)
    def testLongDecimalType192_BooleanType(self): ...

    @parameters(LongDecimalType(19, 2), RealType(), validCast=False)
    def testLongDecimalType192_RealType(self): ...

    @parameters(LongDecimalType(19, 2), DoubleType(), validCast=False)
    def testLongDecimalType192_DoubleType(self): ...

    @parameters(LongDecimalType(19, 2), TimestampType(), validCast=False)
    def testLongDecimalType192_TimestampType(self): ...

    @parameters(LongDecimalType(19, 2), VarcharType(), validCast=False)
    def testLongDecimalType192_VarcharType(self): ...

    @parameters(LongDecimalType(19, 2), VarbinaryType(), validCast=False)
    def testLongDecimalType192_VarbinaryType(self): ...

    @parameters(LongDecimalType(19, 2), ArrayType(IntegerType()), validCast=False)
    def testLongDecimalType192_ArrayTypeIntegerType(self): ...

    @parameters(LongDecimalType(19, 2), ArrayType(BigintType()), validCast=False)
    def testLongDecimalType192_ArrayTypeBigintType(self): ...

    @parameters(LongDecimalType(19, 2), ArrayType(VarcharType()), validCast=False)
    def testLongDecimalType192_ArrayTypeVarcharType(self): ...

    @parameters(LongDecimalType(19, 2), ShortDecimalType(18, 2), validCast=True)
    def testLongDecimalType192_ShortDecimalType182(self): ...

    @parameters(LongDecimalType(19, 2), LongDecimalType(19, 2), validCast=True)
    def testLongDecimalType192_LongDecimalType192(self): ...

    @parameters(
        LongDecimalType(19, 2), MapType(VarcharType(), IntegerType()), validCast=False
    )
    def testLongDecimalType192_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(
        LongDecimalType(19, 2), MapType(VarcharType(), BigintType()), validCast=False
    )
    def testLongDecimalType192_MapTypeVarcharTypeBigintType(self): ...

    @parameters(MapType(VarcharType(), IntegerType()), TinyintType(), validCast=False)
    def testMapTypeVarcharTypeIntegerType_TinyintType(self): ...

    @parameters(MapType(VarcharType(), IntegerType()), SmallintType(), validCast=False)
    def testMapTypeVarcharTypeIntegerType_SmallintType(self): ...

    @parameters(MapType(VarcharType(), IntegerType()), IntegerType(), validCast=False)
    def testMapTypeVarcharTypeIntegerType_IntegerType(self): ...

    @parameters(MapType(VarcharType(), IntegerType()), BigintType(), validCast=False)
    def testMapTypeVarcharTypeIntegerType_BigintType(self): ...

    @parameters(MapType(VarcharType(), IntegerType()), HugeintType(), validCast=False)
    def testMapTypeVarcharTypeIntegerType_HugeintType(self): ...

    @parameters(MapType(VarcharType(), IntegerType()), BooleanType(), validCast=False)
    def testMapTypeVarcharTypeIntegerType_BooleanType(self): ...

    @parameters(MapType(VarcharType(), IntegerType()), RealType(), validCast=False)
    def testMapTypeVarcharTypeIntegerType_RealType(self): ...

    @parameters(MapType(VarcharType(), IntegerType()), DoubleType(), validCast=False)
    def testMapTypeVarcharTypeIntegerType_DoubleType(self): ...

    @parameters(MapType(VarcharType(), IntegerType()), TimestampType(), validCast=False)
    def testMapTypeVarcharTypeIntegerType_TimestampType(self): ...

    @parameters(MapType(VarcharType(), IntegerType()), VarcharType(), validCast=False)
    def testMapTypeVarcharTypeIntegerType_VarcharType(self): ...

    @parameters(MapType(VarcharType(), IntegerType()), VarbinaryType(), validCast=False)
    def testMapTypeVarcharTypeIntegerType_VarbinaryType(self): ...

    @parameters(
        MapType(VarcharType(), IntegerType()), ArrayType(IntegerType()), validCast=False
    )
    def testMapTypeVarcharTypeIntegerType_ArrayTypeIntegerType(self): ...

    @parameters(
        MapType(VarcharType(), IntegerType()), ArrayType(BigintType()), validCast=False
    )
    def testMapTypeVarcharTypeIntegerType_ArrayTypeBigintType(self): ...

    @parameters(
        MapType(VarcharType(), IntegerType()), ArrayType(VarcharType()), validCast=False
    )
    def testMapTypeVarcharTypeIntegerType_ArrayTypeVarcharType(self): ...

    @parameters(
        MapType(VarcharType(), IntegerType()), ShortDecimalType(18, 2), validCast=False
    )
    def testMapTypeVarcharTypeIntegerType_ShortDecimalType182(self): ...

    @parameters(
        MapType(VarcharType(), IntegerType()), LongDecimalType(19, 2), validCast=False
    )
    def testMapTypeVarcharTypeIntegerType_LongDecimalType192(self): ...

    @parameters(
        MapType(VarcharType(), IntegerType()),
        MapType(VarcharType(), IntegerType()),
        validCast=False,
    )
    def testMapTypeVarcharTypeIntegerType_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(
        MapType(VarcharType(), IntegerType()),
        MapType(VarcharType(), BigintType()),
        validCast=False,
    )
    def testMapTypeVarcharTypeIntegerType_MapTypeVarcharTypeBigintType(self): ...

    @parameters(MapType(VarcharType(), BigintType()), TinyintType(), validCast=False)
    def testMapTypeVarcharTypeBigintType_TinyintType(self): ...

    @parameters(MapType(VarcharType(), BigintType()), SmallintType(), validCast=False)
    def testMapTypeVarcharTypeBigintType_SmallintType(self): ...

    @parameters(MapType(VarcharType(), BigintType()), IntegerType(), validCast=False)
    def testMapTypeVarcharTypeBigintType_IntegerType(self): ...

    @parameters(MapType(VarcharType(), BigintType()), BigintType(), validCast=False)
    def testMapTypeVarcharTypeBigintType_BigintType(self): ...

    @parameters(MapType(VarcharType(), BigintType()), HugeintType(), validCast=False)
    def testMapTypeVarcharTypeBigintType_HugeintType(self): ...

    @parameters(MapType(VarcharType(), BigintType()), BooleanType(), validCast=False)
    def testMapTypeVarcharTypeBigintType_BooleanType(self): ...

    @parameters(MapType(VarcharType(), BigintType()), RealType(), validCast=False)
    def testMapTypeVarcharTypeBigintType_RealType(self): ...

    @parameters(MapType(VarcharType(), BigintType()), DoubleType(), validCast=False)
    def testMapTypeVarcharTypeBigintType_DoubleType(self): ...

    @parameters(MapType(VarcharType(), BigintType()), TimestampType(), validCast=False)
    def testMapTypeVarcharTypeBigintType_TimestampType(self): ...

    @parameters(MapType(VarcharType(), BigintType()), VarcharType(), validCast=False)
    def testMapTypeVarcharTypeBigintType_VarcharType(self): ...

    @parameters(MapType(VarcharType(), BigintType()), VarbinaryType(), validCast=False)
    def testMapTypeVarcharTypeBigintType_VarbinaryType(self): ...

    @parameters(
        MapType(VarcharType(), BigintType()), ArrayType(IntegerType()), validCast=False
    )
    def testMapTypeVarcharTypeBigintType_ArrayTypeIntegerType(self): ...

    @parameters(
        MapType(VarcharType(), BigintType()), ArrayType(BigintType()), validCast=False
    )
    def testMapTypeVarcharTypeBigintType_ArrayTypeBigintType(self): ...

    @parameters(
        MapType(VarcharType(), BigintType()), ArrayType(VarcharType()), validCast=False
    )
    def testMapTypeVarcharTypeBigintType_ArrayTypeVarcharType(self): ...

    @parameters(
        MapType(VarcharType(), BigintType()), ShortDecimalType(18, 2), validCast=False
    )
    def testMapTypeVarcharTypeBigintType_ShortDecimalType182(self): ...

    @parameters(
        MapType(VarcharType(), BigintType()), LongDecimalType(19, 2), validCast=False
    )
    def testMapTypeVarcharTypeBigintType_LongDecimalType192(self): ...

    @parameters(
        MapType(VarcharType(), BigintType()),
        MapType(VarcharType(), IntegerType()),
        validCast=False,
    )
    def testMapTypeVarcharTypeBigintType_MapTypeVarcharTypeIntegerType(self): ...

    @parameters(
        MapType(VarcharType(), BigintType()),
        MapType(VarcharType(), BigintType()),
        validCast=False,
    )
    def testMapTypeVarcharTypeBigintType_MapTypeVarcharTypeBigintType(self): ...
