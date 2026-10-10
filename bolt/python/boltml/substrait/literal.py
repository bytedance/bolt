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

from typing import Any, Optional

from substrait.proto import type as stype
from substrait.proto import extensions
from substrait.proto import algebra

from pybolt import (
    ArrayType,
    BaseVector,
    BigintType,
    BoltType,
    BooleanType,
    DoubleType,
    Timestamp,
    IntegerType,
    LongDecimalType,
    MapType,
    RealType,
    RowType,
    RowVector,
    SmallintType,
    ShortDecimalType,
    TimestampType,
    TinyintType,
    VarbinaryType,
    VarcharType,
    fromList,
    rowVector,
)

from .types import substraitType, boltType

Type = stype.Type
Extension = extensions.SimpleExtensionDeclaration
Literal = algebra.Expression.Literal


def substraitVectorLiteral(
    vector: BaseVector, index: int, btype: Optional[BoltType] = None
) -> Literal:
    """Serialize one vector element without allocating replacement vectors."""
    btype = vector.dtype() if btype is None else btype
    if vector.isNullAt(index):
        return substraitLiteral(None, btype)
    if isinstance(btype, RowType):
        fields = [
            substraitVectorLiteral(vector.childAt(name), index, btype.childAt(i))
            for i, name in enumerate(btype.names())
        ]
        return Literal(struct=Literal.Struct(fields=fields))
    return substraitLiteral(vector[index], btype)


def substraitLiteral(val: Any, btype: BoltType) -> Literal:
    if val is None:
        return Literal(null=substraitType(btype))
    if isinstance(btype, ArrayType):
        if len(val) == 0:
            return Literal(empty_list=substraitType(btype).list)
        values = [substraitLiteral(v, btype.elementType()) for v in val]
        return Literal(list=Literal.List(values=values))
    if isinstance(btype, BigintType):
        return Literal(i64=val)
    if isinstance(btype, BooleanType):
        return Literal(boolean=val)
    if isinstance(btype, DoubleType):
        return Literal(fp64=val)
    if isinstance(btype, IntegerType):
        return Literal(i32=val)
    if isinstance(btype, (ShortDecimalType, LongDecimalType)):
        return Literal(
            decimal=Literal.Decimal(
                scale=btype.scale(),
                precision=btype.precision(),
                value=int(val).to_bytes(16, byteorder="little", signed=True),
            )
        )
    if isinstance(btype, MapType):
        if len(val) == 0:
            return Literal(empty_map=substraitType(btype).map)
        key_values = [
            Literal.Map.KeyValue(
                key=substraitLiteral(k, btype.keyType()),
                value=substraitLiteral(v, btype.valueType()),
            )
            for k, v in val.items()
        ]
        return Literal(map=Literal.Map(key_values=key_values))
    if isinstance(btype, RealType):
        return Literal(fp32=val)
    if isinstance(btype, RowType):
        if not isinstance(val, RowVector) or len(val) != 1:
            raise ValueError("Bolt RowType literal requires a RowVector with one row.")
        return substraitVectorLiteral(val, 0, btype)
    if isinstance(btype, SmallintType):
        return Literal(i16=val)
    if isinstance(btype, TimestampType):
        precision_timestamp = Literal.PrecisionTimestamp(
            precision=9, value=val.nanos() + 1_000_000_000 * val.seconds()
        )
        return Literal(precision_timestamp=precision_timestamp)
    if isinstance(btype, TinyintType):
        return Literal(i8=val)
    if isinstance(btype, VarbinaryType):
        return Literal(binary=val)
    if isinstance(btype, VarcharType):
        return Literal(string=val)
    raise ValueError(
        f"Unsupported conversion from bolt type {btype} to substrait literal."
    )


# ========================= Reverse Conversions =========================


def boltLiteral(
    lit: Literal,
    extensions: Optional[list[extensions.SimpleExtensionDeclaration]] = None,
) -> tuple[Any, BoltType]:
    """Return the value and pybolt type from a Substrait literal."""
    if extensions is None:
        extensions = []
    kind = lit.WhichOneof("literal_type")

    if kind == "null":
        return None, boltType(lit.null, extensions)
    if kind == "boolean":
        return lit.boolean, BooleanType()
    if kind == "i8":
        return lit.i8, TinyintType()
    if kind == "i16":
        return lit.i16, SmallintType()
    if kind == "i32":
        return lit.i32, IntegerType()
    if kind == "i64":
        return lit.i64, BigintType()
    if kind == "fp32":
        return lit.fp32, RealType()
    if kind == "fp64":
        return lit.fp64, DoubleType()
    if kind == "decimal":
        prec = lit.decimal.precision
        scale = lit.decimal.scale
        if prec < 19:
            return lit.decimal.value, ShortDecimalType(prec, scale)
        else:
            return lit.decimal.value, LongDecimalType(prec, scale)
    if kind == "string":
        return lit.string, VarcharType()
    if kind == "var_char":
        return lit.var_char.value, VarcharType()
    if kind == "binary":
        return lit.binary, VarbinaryType()
    if kind == "precision_timestamp":
        prec = lit.precision_timestamp.precision
        val = lit.precision_timestamp.value
        nanos = val if prec == 9 else val * (10 ** max(0, 9 - prec))
        sec = nanos // 1_000_000_000
        nsec = nanos % 1_000_000_000
        return Timestamp(int(sec), int(nsec)), TimestampType()
    if kind == "timestamp":
        micros = lit.timestamp
        sec = micros // 1_000_000
        nsec = (micros % 1_000_000) * 1_000
        return Timestamp(int(sec), int(nsec)), TimestampType()
    if kind == "empty_list":
        et = boltType(lit.empty_list.type, extensions)
        return [], ArrayType(et)
    if kind == "list":
        if not lit.list.values:
            return [], ArrayType(boltType(lit.empty_list.type, extensions))
        values = []
        _, elementType = boltLiteral(lit.list.values[0], extensions)
        for v in lit.list.values:
            values.append(boltLiteral(v, extensions)[0])
        return values, ArrayType(elementType)
    if kind == "empty_map":
        kt = boltType(lit.empty_map.key, extensions)
        vt = boltType(lit.empty_map.value, extensions)
        return {}, MapType(kt, vt)
    if kind == "map":
        if not lit.map.key_values:
            return {}, MapType(
                boltType(lit.empty_map.key, extensions),
                boltType(lit.empty_map.value, extensions),
            )
        out = {}
        ktype = boltLiteral(lit.map.key_values[0].key, extensions)[1]
        vtype = boltLiteral(lit.map.key_values[0].value, extensions)[1]
        for kv in lit.map.key_values:
            out[boltLiteral(kv.key, extensions)[0]] = boltLiteral(kv.value, extensions)[
                0
            ]
        return out, MapType(ktype, vtype)
    if kind == "struct":
        child_vals: list[Any] = []
        child_types: list[BoltType] = []
        for f in lit.struct.fields:
            v, t = boltLiteral(f, extensions)
            child_vals.append(v)
            child_types.append(t)
        names = [f"c{i}" for i in range(len(child_types))]
        cols = [
            fromList([v], t) if not isinstance(v, BaseVector) else v
            for v, t in zip(child_vals, child_types)
        ]
        return rowVector(names, cols), RowType(names, child_types)
    raise ValueError(f"unsupported substrait literal type: {kind}")
