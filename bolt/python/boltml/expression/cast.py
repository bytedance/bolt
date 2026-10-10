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


# Import all type classes
from pybolt import (
    ArrayType,
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
    TinyintType,
    UnknownType,
    VarbinaryType,
    VarcharType,
)

from .project import Expression


class SqlType:
    def __init__(self, dtype: BoltType):
        self.__dtype = dtype

    @property
    def dtype(self) -> BoltType:
        return self.__dtype

    def expr(self) -> str:
        if isinstance(self.__dtype, BooleanType):
            return "BOOLEAN"
        if isinstance(self.__dtype, TinyintType):
            return "TINYINT"
        if isinstance(self.__dtype, SmallintType):
            return "SMALLINT"
        if isinstance(self.__dtype, IntegerType):
            return "INTEGER"
        if isinstance(self.__dtype, BigintType):
            return "BIGINT"
        if isinstance(self.__dtype, RealType):
            return "REAL"
        if isinstance(self.__dtype, DoubleType):
            return "DOUBLE"
        if isinstance(self.__dtype, VarcharType):
            return "VARCHAR"
        if isinstance(self.__dtype, VarbinaryType):
            return "VARBINARY"
        if isinstance(self.__dtype, LongDecimalType) or isinstance(
            self.__dtype, ShortDecimalType
        ):
            return f"DECIMAL({self.__dtype.precision()}, {self.__dtype.scale()})"
        if isinstance(self.__dtype, TimestampType):
            return "TIMESTAMP"
        if isinstance(self.__dtype, ArrayType):
            elementType = SqlType(self.__dtype.elementType())
            return f"{elementType.expr()}[]"
        if isinstance(self.__dtype, MapType):
            keyType = SqlType(self.__dtype.keyType())
            valueType = SqlType(self.__dtype.valueType())
            return f"MAP<{keyType.expr()}, {valueType.expr()}>"
        if isinstance(self.__dtype, RowType):
            names = self.__dtype.names()
            types = [SqlType(self.__dtype.childAt(i)) for i in range(len(self.__dtype))]
            return (
                f"ROW({', '.join([n + ' ' + t.expr() for n, t in zip(names, types)])})"
            )
        raise TypeError(f"Unknown SQL type expression for {self.__dtype}.")


_integerTypes_ = [
    BigintType,
    BooleanType,
    DoubleType,
    HugeintType,
    IntegerType,
    RealType,
    SmallintType,
    TinyintType,
]

PRIMITIVE_CASTS = [
    (BigintType, _integerTypes_),
    (BooleanType, _integerTypes_),
    (DoubleType, [RealType, DoubleType]),
    (HugeintType, _integerTypes_),
    (IntegerType, _integerTypes_),
    (LongDecimalType, [LongDecimalType, ShortDecimalType]),
    (RealType, [RealType, DoubleType]),
    (ShortDecimalType, [LongDecimalType, ShortDecimalType]),
    (SmallintType, _integerTypes_),
    (TimestampType, []),
    (TinyintType, _integerTypes_),
    (UnknownType, []),
    (VarbinaryType, [VarbinaryType, VarcharType]),
    (VarcharType, [VarbinaryType, VarcharType]),
]

COMPLEX_TYPES = [ArrayType, MapType, RowType]


def checkCast(lhs: BoltType, rhs: BoltType):
    err = TypeError(f"Unsupported cast from {lhs} to {rhs}")
    primitiveCasts = next(
        (p[1] for p in PRIMITIVE_CASTS if isinstance(lhs, p[0])), None
    )
    if primitiveCasts is not None:
        if not any((isinstance(rhs, t) for t in primitiveCasts)):
            raise err
        return
    complexType = next(
        (complexType for complexType in COMPLEX_TYPES if isinstance(rhs, complexType)),
        None,
    )
    if complexType is None:
        raise err
    if not isinstance(lhs, complexType):
        raise err
    typePairs = []
    if isinstance(rhs, ArrayType):
        typePairs = [(lhs.elementType(), rhs.elementType())]
    elif isinstance(rhs, MapType):
        raise TypeError("MapType cast is not supported.")
    elif isinstance(rhs, RowType):
        if lhs.names() != rhs.names():
            raise err
        for i in range(len(lhs)):
            typePairs.append((lhs.childAt(i), rhs.childAt(i)))
    for src, dst in typePairs:
        checkCast(src, dst)


class CastExpression(Expression):
    def __init__(
        self,
        expr: Expression,
        targetType: BoltType,
    ):
        checkCast(expr.dtype, targetType)

        self.__input = expr
        newExpr = f"CAST({expr.expr()} AS {SqlType(targetType).expr()})"
        super().__init__(newExpr, targetType, expr.dataframe)

    def input(self) -> Expression:
        return self.__input
