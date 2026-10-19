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

from typing import Optional

from substrait.proto import type as stype
from substrait.proto import extensions
from substrait.proto import algebra

from pybolt import (
    ArrayType,
    BigintType,
    BoltType,
    BooleanType,
    DateType,
    DoubleType,
    FileFormat,
    IntegerType,
    JoinType,
    LongDecimalType,
    MapType,
    RealType,
    RowType,
    ShortDecimalType,
    SmallintType,
    SortOrder,
    TimestampType,
    TinyintType,
    UnknownType,
    VarbinaryType,
    VarcharType,
)

Type = stype.Type
Extension = extensions.SimpleExtensionDeclaration
File = algebra.ReadRel.LocalFiles.FileOrFiles
nullable = Type.Nullability.NULLABILITY_NULLABLE
required = Type.Nullability.NULLABILITY_REQUIRED


def _isDateType(btype: BoltType) -> bool:
    return isinstance(btype, DateType)


def substraitType(btype: BoltType, nullability=nullable) -> Type:
    if isinstance(btype, ArrayType):
        etype = substraitType(btype.elementType())
        return Type(list=Type.List(type=etype, nullability=nullability))
    if isinstance(btype, BigintType):
        return Type(i64=Type.I64(nullability=nullability))
    if isinstance(btype, BooleanType):
        return Type(bool=Type.Boolean(nullability=nullability))
    if _isDateType(btype):
        return Type(date=Type.Date(nullability=nullability))
    if isinstance(btype, DoubleType):
        return Type(fp64=Type.FP64(nullability=nullability))
    if isinstance(btype, UnknownType):
        from .extensions import extensionRegister, extensionIndex

        return Type(
            user_defined=Type.UserDefined(
                type_reference=extensionIndex(extensionRegister[btype]),
                nullability=nullability,
            )
        )
    if isinstance(btype, IntegerType):
        return Type(i32=Type.I32(nullability=nullability))
    if isinstance(btype, (LongDecimalType, ShortDecimalType)):
        return Type(
            decimal=Type.Decimal(
                scale=btype.scale(),
                precision=btype.precision(),
                nullability=nullability,
            )
        )
    if isinstance(btype, MapType):
        ktype = substraitType(btype.keyType(), required)
        vtype = substraitType(btype.valueType(), required)
        return Type(map=Type.Map(key=ktype, value=vtype, nullability=nullability))
    if isinstance(btype, RealType):
        return Type(fp32=Type.FP32(nullability=nullability))
    if isinstance(btype, RowType):
        types = [substraitType(btype.childAt(i)) for i in range(len(btype))]
        return Type(struct=Type.Struct(types=types))
    if isinstance(btype, SmallintType):
        return Type(i16=Type.I16(nullability=nullability))
    if isinstance(btype, TimestampType):
        return Type(
            precision_timestamp=Type.PrecisionTimestamp(
                precision=9, nullability=nullability
            )
        )
    if isinstance(btype, TinyintType):
        return Type(i8=Type.I8(nullability=nullability))
    if isinstance(btype, VarbinaryType):
        return Type(binary=Type.Binary(nullability=nullability))
    if isinstance(btype, VarcharType):
        return Type(string=Type.String(nullability=nullability))
    raise ValueError(f"Unsupported conversion from bolt type {btype} to substrait.")


def substraitFile(path: str, fformat: FileFormat) -> File:
    if fformat == FileFormat.PARQUET:
        return File(uri_file=path, parquet=File.ParquetReadOptions())
    if fformat == FileFormat.ORC:
        return File(uri_file=path, orc=File.OrcReadOptions())
    if fformat == FileFormat.DWRF:
        return File(uri_file=path, dwrf=File.DwrfReadOptions())
    if fformat == FileFormat.TEXT:
        return File(
            uri_file=path,
            text=File.DelimiterSeparatedTextReadOptions(
                field_delimiter=" ",
                quote='"',
                header_lines_to_skip=0,
            ),
        )
    raise RuntimeError(
        f"File format {fformat} conversion to substrait is not implemented."
    )


def substraitSortDirection(order: SortOrder) -> algebra.SortField.SortDirection:
    if order == SortOrder.ASC_NULLS_FIRST:
        return algebra.SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_FIRST
    if order == SortOrder.ASC_NULLS_LAST:
        return algebra.SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST
    if order == SortOrder.DESC_NULLS_FIRST:
        return algebra.SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_FIRST
    if order == SortOrder.DESC_NULLS_LAST:
        return algebra.SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_LAST
    raise ValueError(f"Invalid bolt sort order: {order}.")


def substraitJoinType(joinType: JoinType) -> algebra.HashJoinRel.JoinType:
    if joinType == JoinType.kLeft:
        return algebra.HashJoinRel.JoinType.JOIN_TYPE_LEFT
    if joinType == JoinType.kRight:
        return algebra.HashJoinRel.JoinType.JOIN_TYPE_RIGHT
    if joinType == JoinType.kInner:
        return algebra.HashJoinRel.JoinType.JOIN_TYPE_INNER
    if joinType == JoinType.kFull:
        return algebra.HashJoinRel.JoinType.JOIN_TYPE_OUTER
    if joinType == JoinType.kLeftSemiFilter:
        return algebra.HashJoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
    if joinType == JoinType.kRightSemiFilter:
        return algebra.HashJoinRel.JoinType.JOIN_TYPE_RIGHT_SEMI
    if joinType == JoinType.kLeftSemiProject:
        return algebra.HashJoinRel.JoinType.JOIN_TYPE_LEFT_MARK
    if joinType == JoinType.kRightSemiProject:
        return algebra.HashJoinRel.JoinType.JOIN_TYPE_RIGHT_MARK
    if joinType == JoinType.kAnti:
        return algebra.HashJoinRel.JoinType.JOIN_TYPE_LEFT_ANTI
    raise ValueError(f"Unsupported conversion from join type {joinType} to substrait.")


# ========================= Reverse Conversions =========================


def boltType(
    t: Type, extensions: Optional[list[extensions.SimpleExtensionDeclaration]] = None
) -> BoltType:
    """Convert Substrait Type to pybolt BoltType."""
    if extensions is None:
        extensions = []
    stype = t.WhichOneof("kind")
    if stype == "bool":
        return BooleanType()
    if stype == "i8":
        return TinyintType()
    if stype == "i16":
        return SmallintType()
    if stype == "i32":
        return IntegerType()
    if stype == "i64":
        return BigintType()
    if stype == "fp32":
        return RealType()
    if stype == "fp64":
        return DoubleType()
    if stype == "string":
        return VarcharType()
    if stype == "date":
        return DateType()
    if stype == "binary":
        return VarbinaryType()
    if stype == "varchar":
        return VarcharType()
    if stype == "decimal":
        if t.decimal.precision < 19:
            return ShortDecimalType(t.decimal.precision, t.decimal.scale)
        else:
            return LongDecimalType(t.decimal.precision, t.decimal.scale)
    if stype == "precision_timestamp" or stype == "timestamp":
        return TimestampType()
    if stype == "struct":
        types = [boltType(dtype, extensions) for dtype in t.struct.types]
        names = [f"c{i}" for i in range(len(types))]
        return RowType(names, types)
    if stype == "list":
        return ArrayType(boltType(t.list.type, extensions))
    if stype == "map":
        return MapType(
            boltType(t.map.key, extensions), boltType(t.map.value, extensions)
        )
    if stype == "user_defined":
        from .extensions import extensionIndex

        i = t.user_defined.type_reference
        ext = next(
            (
                e
                for e in extensions
                if e.WhichOneof("mapping_type") == "extension_type"
                and extensionIndex(e) == i
            ),
            None,
        )
        if ext is None:
            raise IndexError(f"No extension was found with type anchor: {i}")
        if ext.extension_type.name == "UNKNOWN":
            return UnknownType()
    raise ValueError(f"Unsupported conversion from substrait type: {t}")


def boltSortOrder(direction: algebra.SortField.SortDirection) -> SortOrder:
    if direction == algebra.SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_FIRST:
        return SortOrder.ASC_NULLS_FIRST
    if direction == algebra.SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST:
        return SortOrder.ASC_NULLS_LAST
    if direction == algebra.SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_FIRST:
        return SortOrder.DESC_NULLS_FIRST
    if direction == algebra.SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_LAST:
        return SortOrder.DESC_NULLS_LAST
    raise ValueError(f"Unsupported substrait sort direction: {direction}.")


def boltJoinType(jtype: algebra.HashJoinRel.JoinType) -> JoinType:
    if jtype == algebra.HashJoinRel.JoinType.JOIN_TYPE_LEFT:
        return JoinType.kLeft
    if jtype == algebra.HashJoinRel.JoinType.JOIN_TYPE_RIGHT:
        return JoinType.kRight
    if jtype == algebra.HashJoinRel.JoinType.JOIN_TYPE_INNER:
        return JoinType.kInner
    if jtype == algebra.HashJoinRel.JoinType.JOIN_TYPE_OUTER:
        return JoinType.kFull
    if jtype == algebra.HashJoinRel.JoinType.JOIN_TYPE_LEFT_SEMI:
        return JoinType.kLeftSemiFilter
    if jtype == algebra.HashJoinRel.JoinType.JOIN_TYPE_RIGHT_SEMI:
        return JoinType.kRightSemiFilter
    if jtype == algebra.HashJoinRel.JoinType.JOIN_TYPE_LEFT_MARK:
        return JoinType.kLeftSemiProject
    if jtype == algebra.HashJoinRel.JoinType.JOIN_TYPE_RIGHT_MARK:
        return JoinType.kRightSemiProject
    if jtype == algebra.HashJoinRel.JoinType.JOIN_TYPE_LEFT_ANTI:
        return JoinType.kAnti
    raise ValueError(f"Unsupported conversion from substrait join type: {jtype}.")


def boltFile(item: File) -> tuple[str, FileFormat]:
    """Extract (path, FileFormat) from ReadRel.LocalFiles.FileOrFiles."""
    # Determine path
    path = item.uri_path or item.uri_file or item.uri_folder or item.uri_path_glob
    # Determine format
    ff = item.WhichOneof("file_format")
    if ff == "parquet":
        return path, FileFormat.PARQUET
    if ff == "orc":
        return path, FileFormat.ORC
    if ff == "dwrf":
        return path, FileFormat.DWRF
    if ff == "text":
        return path, FileFormat.TEXT
    raise ValueError(f"Unsupported conversion from substrait file format type: {ff}.")
