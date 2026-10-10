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

from typing import Union

from pybolt import (
    ArrayType,
    BigintType,
    BooleanType,
    BoltType,
    DoubleType,
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

from ..expression import FunctionExpression


def substraitSignature(arg: Union[BoltType, FunctionExpression]) -> str:
    if isinstance(arg, ArrayType):
        return f"array({substraitSignature(arg.elementType())})"
    if isinstance(arg, BigintType):
        return "i64"
    if isinstance(arg, BooleanType):
        return "bool"
    if isinstance(arg, DoubleType):
        return "fp64"
    if isinstance(arg, UnknownType):
        from .extensions import extensionRegister

        return f"u!{extensionRegister[arg].extension_type.name}"
    if isinstance(arg, IntegerType):
        return "i32"
    if isinstance(arg, (LongDecimalType, ShortDecimalType)):
        return f"decimal<{arg.precision()},{arg.scale()}>"
    if isinstance(arg, MapType):
        ksig = substraitSignature(arg.keyType())
        vsig = substraitSignature(arg.valueType())
        return f"map<{ksig},{vsig}>"
    if isinstance(arg, RealType):
        return "fp32"
    if isinstance(arg, RowType):
        sigs = [substraitSignature(arg.childAt(i)) for i in range(len(arg))]
        return f"struct<{','.join(sigs)}>"
    if isinstance(arg, SmallintType):
        return "i16"
    if isinstance(arg, TimestampType):
        return "pts"
    if isinstance(arg, TinyintType):
        return "i8"
    if isinstance(arg, VarbinaryType):
        return "vbin"
    if isinstance(arg, VarcharType):
        return "str"
    if isinstance(arg, FunctionExpression):
        args = "_".join([substraitSignature(expr.dtype) for expr in arg.arguments])
        if len(args) == 0:
            return arg.name
        return f"{arg.name}:{args}"
    raise TypeError(f"Unsupported conversion to substrait signature for type: {arg}")
