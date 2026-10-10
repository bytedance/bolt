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

from inspect import isclass
from typing import Optional, Type

import pybolt

# Import all type classes
from pybolt import (
    ArrayType,
    BigintType,
    BooleanType,
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

_pyboltTypes_ = [
    ArrayType,
    BigintType,
    BooleanType,
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
]

_pyboltVectorTypes_ = [
    pybolt.ArrayVector,
    pybolt.BaseVector,
    pybolt.ConstantVector_BIGINT,
    pybolt.ConstantVector_BOOLEAN,
    pybolt.ConstantVector_DOUBLE,
    pybolt.ConstantVector_INTEGER,
    pybolt.ConstantVector_REAL,
    pybolt.ConstantVector_SMALLINT,
    pybolt.ConstantVector_TIMESTAMP,
    pybolt.ConstantVector_TINYINT,
    pybolt.ConstantVector_VARBINARY,
    pybolt.DictionaryVector_BIGINT,
    pybolt.DictionaryVector_BOOLEAN,
    pybolt.DictionaryVector_DOUBLE,
    pybolt.DictionaryVector_INTEGER,
    pybolt.DictionaryVector_REAL,
    pybolt.DictionaryVector_SMALLINT,
    pybolt.DictionaryVector_TIMESTAMP,
    pybolt.DictionaryVector_TINYINT,
    pybolt.DictionaryVector_VARBINARY,
    pybolt.FlatVector_BIGINT,
    pybolt.FlatVector_BOOLEAN,
    pybolt.FlatVector_DOUBLE,
    pybolt.FlatVector_INTEGER,
    pybolt.FlatVector_REAL,
    pybolt.FlatVector_SMALLINT,
    pybolt.FlatVector_TIMESTAMP,
    pybolt.FlatVector_TINYINT,
    pybolt.FlatVector_VARBINARY,
    pybolt.RowVector,
    pybolt.SimpleVector_BIGINT,
    pybolt.SimpleVector_BOOLEAN,
    pybolt.SimpleVector_DOUBLE,
    pybolt.SimpleVector_INTEGER,
    pybolt.SimpleVector_REAL,
    pybolt.SimpleVector_SMALLINT,
    pybolt.SimpleVector_TIMESTAMP,
    pybolt.SimpleVector_TINYINT,
    pybolt.SimpleVector_VARBINARY,
]


class _TensorType_(type):
    @classmethod
    def names(cls) -> list[str]:
        return cls._names_()

    @classmethod
    def dtype(cls) -> Type:
        return cls._dtype_()

    @staticmethod
    def _names_() -> list[str]:
        raise NotImplementedError(
            "Method _names_() needs to be implemented by child class"
        )

    @staticmethod
    def _dtype_() -> Type:
        raise NotImplementedError(
            "Method _dtype_() needs to be implemented by child class"
        )


def TensorType(dataType: Type, colNames: Optional[list[str]] = None):
    if colNames is not None:
        if len(set(colNames)) != len(colNames):
            raise ValueError(
                "TensorType colNames argument cannot contain duplicate names."
            )

        class TensorTypeImplWithNames(_TensorType_):
            @staticmethod
            def _names_() -> list[str]:
                if colNames is None:
                    return _TensorType_._names_()
                return colNames

            @staticmethod
            def _dtype_() -> Type:
                return dataType

        return TensorTypeImplWithNames
    else:

        class TensorTypeImpl(_TensorType_):
            @staticmethod
            def _dtype_():
                return dataType

        return TensorTypeImpl


def _isVectorType_(t) -> bool:
    if isclass(t):
        return any(issubclass(t, _t) for _t in _pyboltVectorTypes_)
    if type(t) is not type:
        return _isVectorType_(type(t))
    return any(isinstance(t, _t) for _t in _pyboltVectorTypes_)


def _isConstantType_(t) -> bool:
    if type(t) is type:
        dtype = t
    else:
        dtype = type(t)
    return dtype in [bool, int, float, str]


def _isArithmeticType_(t) -> bool:
    if type(t) is type:
        dtype = t
    else:
        dtype = type(t)
    if dtype in [int, float]:
        return True
    if any(
        (
            issubclass(dtype, t)
            for t in [
                RealType,
                DoubleType,
                IntegerType,
                BigintType,
                HugeintType,
                SmallintType,
                TinyintType,
            ]
        )
    ):
        return True
    return False


def _isStringType_(t) -> bool:
    if type(t) is type:
        dtype = t
    else:
        dtype = type(t)
    if dtype is str:
        return True
    if issubclass(dtype, VarcharType):
        return True
    return False


def _inferPyboltType_(t):
    if type(t) is type:
        dtype = t
        instance = None
    else:
        dtype = type(t)
        instance = t

    # Calling this function on a bolt type returns the same type object.
    if any(issubclass(dtype, pyboltType) for pyboltType in _pyboltTypes_):
        return instance

    if dtype is bool:
        return BooleanType()
    elif dtype is int:
        return BigintType()
    elif dtype is float:
        return DoubleType()
    elif dtype is str:
        return VarcharType()
    elif dtype is bytes:
        return VarbinaryType()
    elif dtype is list:
        return ArrayType()
    elif dtype is dict:
        if instance is None:
            raise TypeError(
                "Cannot infer bolt type from dictionary type. Argument needs"
                " to be dictionary instance."
            )
        names = list(instance.keys())
        err = next((n for n in names if type(n) is not str), None)
        if err is not None:
            raise TypeError(
                f"No known bolt type for dictionary with keys of type: {type(err)}"
            )
        values = [_inferPyboltType_(instance[k]) for k in names]
        return RowType(names, values)
