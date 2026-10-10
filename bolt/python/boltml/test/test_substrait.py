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

import os
import tempfile
import unittest
from typing import Callable
from unittest.mock import patch

from substrait.proto import algebra, extensions

import pyarrow as pa
import pybolt
from pybolt import (
    ArrayType,
    BigintType,
    BooleanType,
    DoubleType,
    FileFormat,
    IntegerType,
    JoinType,
    LongDecimalType,
    MapType,
    RealType,
    RowType,
    rowVector,
    ShortDecimalType,
    SmallintType,
    Timestamp,
    TimestampType,
    TinyintType,
    UnknownType,
    VarbinaryType,
    VarcharType,
)
from pybolt.test.utils import IntGenerator, RowVectorGenerator, StringGenerator

from ..dataframe import DataFrame
from ..expression.boolean import BooleanExpression, BooleanOp
from ..expression.cast import CastExpression
from ..expression.function import FunctionExpression
from ..expression.literal import LiteralExpression
from ..expression.named import NamedExpression
from ..expression.project import ProjectExpression
from ..expression.string import StringProjectExpression
from ..function.aggregation import Mean
from ..function.dataframe import dataframeFunction
from ..plan_builder.substrait import SubstraitPlanBuilderFactory
from ..plan_builder.bolt import BoltPlanBuilderFactory
from ..reader import LocalTableReader
from ..substrait import rel
from ..substrait import literal as literalModule
from ..substrait.expression import substraitExpression
from ..substrait.extensions import extensionRegister
from ..substrait.literal import substraitLiteral, boltLiteral
from ..substrait.signature import substraitSignature
from ..substrait.types import (
    substraitType,
    substraitJoinType,
    substraitSortDirection,
    substraitFile,
    boltType,
    boltFile,
    boltJoinType,
    boltSortOrder,
)
from ..writer import LocalTableWriter


def makeDataframe(length: int = 4, planFactory=SubstraitPlanBuilderFactory()):
    generator = RowVectorGenerator(
        c0=IntGenerator(BigintType()).setMax(4).setMin(0),
        c1=IntGenerator(BigintType()).repeat(1),
        c2=IntGenerator(BigintType()).unique(),
        c3=StringGenerator(),
    )
    return DataFrame(generator(length), planFactory=planFactory)


ALL_BOLT_TYPES = (
    ArrayType(BigintType()),
    BigintType(),
    BooleanType(),
    DoubleType(),
    IntegerType(),
    LongDecimalType(19, 16),
    MapType(BigintType(), BigintType()),
    RealType(),
    RowType(["c0"], [BigintType()]),
    ShortDecimalType(16, 15),
    SmallintType(),
    TimestampType(),
    TinyintType(),
    UnknownType(),
    VarbinaryType(),
    VarcharType(),
)


class SubstraitTest(unittest.TestCase):
    def testTypeConversion(self):
        """Test that Bolt types can be converted to Substrait types and back."""
        for t in ALL_BOLT_TYPES:
            stype = substraitType(t)
            btype = boltType(stype, extensionRegister.getAll())
            self.assertEqual(btype, t)

    def testLiteralConversion(self):
        for val, t in (
            (pybolt.fromList([[1, 2]])[0], ArrayType(BigintType())),
            (1, BigintType()),
            (True, BooleanType()),
            (3.2, DoubleType()),
            (4, IntegerType()),
            (pybolt.fromList([{"key": 54}])[0], MapType(VarcharType(), BigintType())),
            (4.0, RealType()),
            (3, SmallintType()),
            (Timestamp(16, 354), TimestampType()),
            (6, TinyintType()),
            (b"sdfss", VarbinaryType()),
            ("hello", VarcharType()),
            (rowVector(["c"], [pybolt.fromList([1])]), RowType(["c"], [BigintType()])),
            (
                rowVector(["c"], [rowVector(["c"], [pybolt.fromList([1])])]),
                RowType(["c"], [RowType(["c"], [BigintType()])]),
            ),
        ):
            lit = substraitLiteral(val, t)
            result, resultType = boltLiteral(lit, extensionRegister.getAll())
            self.assertEqual(resultType, t)
            self.assertEqual(result, val)

    def testBinaryWireDoesNotNeedExtensions(self):
        binary = VarbinaryType()
        cases = (
            (b"encoded", binary),
            (None, binary),
            ([], ArrayType(binary)),
            ([b"first", None, b""], ArrayType(binary)),
            ({}, MapType(VarcharType(), binary)),
            (
                {"first": b"encoded", "missing": None},
                MapType(VarcharType(), binary),
            ),
        )
        for value, dtype in cases:
            with self.subTest(dtype=str(dtype), value=value):
                wireType = substraitType(dtype)
                wireLiteral = substraitLiteral(value, dtype)
                self.assertEqual(extensionRegister.find(wireType), [])
                self.assertEqual(extensionRegister.find(wireLiteral), [])
                self.assertEqual(boltType(wireType), dtype)
                decoded, decodedType = boltLiteral(wireLiteral)
                self.assertEqual(decodedType, dtype)
                self.assertEqual(decoded, value)
        self.assertEqual(substraitType(binary).WhichOneof("kind"), "binary")
        self.assertEqual(
            substraitLiteral(b"encoded", binary).WhichOneof("literal_type"), "binary"
        )
        self.assertEqual(substraitSignature(binary), "vbin")

    def testNestedBinaryTypeConversion(self):
        dtype = RowType(
            ["c0", "c1"],
            [
                ArrayType(VarbinaryType()),
                MapType(VarcharType(), ArrayType(VarbinaryType())),
            ],
        )
        wire = substraitType(dtype)
        self.assertEqual(boltType(wire), dtype)
        self.assertEqual(extensionRegister.find(wire), [])
        self.assertEqual(
            substraitSignature(dtype), "struct<array(vbin),map<str,array(vbin)>>"
        )

    def testUnknownTypeExtensionsAndTypedNulls(self):
        unknown = UnknownType()
        for value, dtype in (
            (None, unknown),
            ([], ArrayType(unknown)),
            ({}, MapType(VarcharType(), unknown)),
        ):
            with self.subTest(dtype=str(dtype)):
                wire = substraitLiteral(value, dtype)
                declarations = extensionRegister.find(wire)
                self.assertEqual(len(declarations), 1)
                self.assertEqual(declarations[0].extension_type.name, "UNKNOWN")
                self.assertEqual(boltLiteral(wire, declarations), (value, dtype))

        # Function and type anchors have independent namespaces in Substrait.
        declaration = extensionRegister[unknown]
        anchor = declaration.extension_type.type_anchor
        function = extensions.SimpleExtensionDeclaration(
            extension_function=extensions.SimpleExtensionDeclaration.ExtensionFunction(
                function_anchor=anchor, name="unrelated"
            )
        )
        self.assertEqual(
            boltType(substraitType(unknown), [function, declaration]), unknown
        )

    def testVirtualTableBinaryContainersDoNotRebuildVectors(self):
        arrays = pybolt.importFromArrow(
            pa.array([[b"first", None], [], None], type=pa.list_(pa.binary()))
        )
        maps = pybolt.importFromArrow(
            pa.array(
                [{"image": b"encoded"}, {}, None],
                type=pa.map_(pa.string(), pa.binary()),
            )
        )
        nested = rowVector(["images", "lookup"], [arrays, maps])
        data = rowVector(["nested"], [nested])
        oneRow = data.slice(0, 1)
        with patch.object(
            pybolt, "fromList", side_effect=AssertionError("vector rebuild")
        ):
            with patch.object(
                literalModule, "fromList", side_effect=AssertionError("vector rebuild")
            ):
                with patch.object(
                    literalModule,
                    "rowVector",
                    side_effect=AssertionError("vector rebuild"),
                ):
                    read = rel.virtualTable(data)
                    literal = substraitLiteral(oneRow, data.dtype())
        rows = read.virtual_table.values
        self.assertEqual(len(rows), 3)
        self.assertEqual(literal.struct, rows[0])
        fields = rows[0].fields[0].struct.fields
        self.assertEqual(fields[0].list.values[0].binary, b"first")
        self.assertEqual(fields[0].list.values[1].WhichOneof("literal_type"), "null")
        self.assertEqual(fields[1].map.key_values[0].value.binary, b"encoded")
        self.assertEqual(
            rows[1].fields[0].struct.fields[0].WhichOneof("literal_type"), "empty_list"
        )
        self.assertEqual(
            rows[1].fields[0].struct.fields[1].WhichOneof("literal_type"), "empty_map"
        )
        self.assertTrue(
            all(
                field.WhichOneof("literal_type") == "null"
                for field in rows[2].fields[0].struct.fields
            )
        )

    def testRowLiteralDistinguishesParentAndChildNulls(self):
        child = pybolt.fromList([b"hidden", None, b"visible"], VarbinaryType())
        nested = rowVector(["image"], [child], {0: True})
        data = rowVector(["nested"], [nested], {2: True})
        read = rel.virtualTable(data)
        fields = [row.fields[0] for row in read.virtual_table.values]
        self.assertEqual(fields[0].WhichOneof("literal_type"), "null")
        self.assertEqual(fields[0].null.WhichOneof("kind"), "struct")
        self.assertEqual(fields[1].WhichOneof("literal_type"), "struct")
        self.assertEqual(fields[1].struct.fields[0].WhichOneof("literal_type"), "null")
        self.assertEqual(fields[2].WhichOneof("literal_type"), "null")
        parentNull = substraitLiteral(nested.slice(0, 1), nested.dtype())
        self.assertEqual(parentNull.WhichOneof("literal_type"), "null")
        self.assertEqual(parentNull.null.WhichOneof("kind"), "struct")

    def testEmptyVirtualTableUsesTypedNullsWithoutNativeVectors(self):
        dtype = RowType(
            ["nested"],
            [RowType(["images"], [ArrayType(VarbinaryType())])],
        )
        data = rowVector(dtype)
        with patch.object(
            pybolt, "fromList", side_effect=AssertionError("vector rebuild")
        ):
            with patch.object(
                pybolt, "rowVector", side_effect=AssertionError("vector rebuild")
            ):
                empty = rel.virtualTable(data)
        self.assertEqual(empty.WhichOneof("rel_type"), "filter")
        self.assertFalse(empty.filter.condition.literal.boolean)
        read = empty.filter.input.read
        self.assertEqual(read.common.hint.stats.row_count, 0)
        self.assertEqual(list(read.base_schema.names), ["nested"])
        self.assertEqual(len(read.virtual_table.values), 1)
        self.assertEqual(
            read.virtual_table.values[0].fields[0].null,
            substraitType(dtype.childAt(0)),
        )

    def testFunctionSignature(self):
        df = makeDataframe()

        for fnExpr in (
            FunctionExpression(
                "add", BigintType(), [LiteralExpression(3), LiteralExpression(4)]
            ),
            FunctionExpression(
                "downloadImages",
                VarbinaryType(),
                [StringProjectExpression(df["c3"])],
            ),
        ):
            _ = substraitSignature(fnExpr)

    def testJoinTypeConversion(self):
        for jt in (
            JoinType.kLeft,
            JoinType.kRight,
            JoinType.kInner,
            JoinType.kFull,
            JoinType.kLeftSemiFilter,
            JoinType.kRightSemiFilter,
            JoinType.kLeftSemiProject,
            JoinType.kRightSemiProject,
            JoinType.kAnti,
        ):
            sj = substraitJoinType(jt)
            back = boltJoinType(sj)
            self.assertEqual(back, jt)

    def testSemiAndMarkJoinWireTypes(self):
        for bolt_type, wire_type in (
            (JoinType.kLeftSemiFilter, algebra.HashJoinRel.JOIN_TYPE_LEFT_SEMI),
            (JoinType.kRightSemiFilter, algebra.HashJoinRel.JOIN_TYPE_RIGHT_SEMI),
            (JoinType.kLeftSemiProject, algebra.HashJoinRel.JOIN_TYPE_LEFT_MARK),
            (JoinType.kRightSemiProject, algebra.HashJoinRel.JOIN_TYPE_RIGHT_MARK),
        ):
            self.assertEqual(substraitJoinType(bolt_type), wire_type)
            self.assertEqual(boltJoinType(wire_type), bolt_type)
        with self.assertRaises(ValueError):
            boltJoinType(algebra.HashJoinRel.JOIN_TYPE_RIGHT_ANTI)

    def testFileConversion(self):
        path = "/tmp/data.file"
        for fmt in (
            FileFormat.PARQUET,
            FileFormat.ORC,
            FileFormat.DWRF,
            FileFormat.TEXT,
        ):
            sf = substraitFile(path, fmt)
            back_path, back_fmt = boltFile(sf)
            self.assertEqual(back_path, path)
            self.assertEqual(back_fmt, fmt)

    def testSortDirectionConversion(self):
        for so in (
            pybolt.SortOrder.ASC_NULLS_FIRST,
            pybolt.SortOrder.ASC_NULLS_LAST,
            pybolt.SortOrder.DESC_NULLS_FIRST,
            pybolt.SortOrder.DESC_NULLS_LAST,
        ):
            sd = substraitSortDirection(so)
            back = boltSortOrder(sd)
            self.assertEqual(back, so)

        df = makeDataframe()
        for t in ALL_BOLT_TYPES:
            fnExpr = FunctionExpression("fn", BigintType(), [df["c0"]])
            _ = substraitSignature(fnExpr)

    def testExpressions(self):
        df = makeDataframe()
        for expr in (
            LiteralExpression(4),
            FunctionExpression(
                "downloadImages",
                VarbinaryType(),
                [StringProjectExpression(df["c3"])],
            ),
            ProjectExpression(df["c0"]),
            CastExpression(df["c0"], IntegerType()),
            BooleanExpression(
                ProjectExpression(df["c0"]),
                BooleanOp.LT,
                ProjectExpression(df["c1"]),
            ),
        ):
            rel = algebra.Rel(extension_leaf=algebra.ExtensionLeafRel())
            _ = substraitExpression(expr, rel)

    def testRel(self):
        df = makeDataframe()
        vtRel = rel.virtualTable(df._data_)
        _ = rel.localFilesRel(
            ["/usr/lib/file/parquet"], df.dtype, pybolt.FileFormat.PARQUET
        )
        _ = rel.projectRel(
            input=algebra.Rel(read=vtRel),
            expressions=[
                NamedExpression("c0", df["c0"] * 2),
            ],
        )
        _ = rel.aggregateRel(
            input=algebra.Rel(read=vtRel),
            keys=[df["c0"]],
            expressions=[
                NamedExpression(
                    "c2", FunctionExpression("avg", DoubleType(), [df["c2"]])
                ),
            ],
        )
        _ = rel.fetchRel(
            input=algebra.Rel(read=vtRel),
            count=10,
            offset=0,
        )
        _ = rel.sortRel(
            input=algebra.Rel(read=vtRel),
            order=[(df["c0"], pybolt.SortOrder.ASC_NULLS_LAST)],
        )
        _ = rel.filterRel(
            input=algebra.Rel(read=vtRel),
            condition=BooleanExpression(
                ProjectExpression(df["c0"]),
                BooleanOp.LT,
                ProjectExpression(df["c1"]),
            ),
        )
        names = df.dtype.names()
        _ = rel.joinRel(
            left=algebra.Rel(read=vtRel),
            right=algebra.Rel(read=vtRel),
            lhsKeys=[df["c0"]],
            rhsKeys=[df["c0"]],
            outputLayout=[*(f"l_{n}" for n in names), *(f"r_{n}" for n in names)],
            joinType=pybolt.JoinType.kInner,
        )

    def __testDataFrame(self, fn: Callable[[DataFrame], DataFrame]):
        df = makeDataframe(length=40)
        substraitDf = DataFrame(df._data_, planFactory=SubstraitPlanBuilderFactory())
        boltDf = DataFrame(df._data_, planFactory=BoltPlanBuilderFactory())

        substraitDf = fn(substraitDf)
        boltDf = fn(boltDf)
        self.assertEqual(substraitDf, boltDf)

    def testPlanBuilderJoin(self):
        rhs = makeDataframe(length=10)
        self.__testDataFrame(lambda lhs: lhs.join(rhs.copy()))

    def testPlanBuilderProject(self):
        self.__testDataFrame(lambda df: df.rename(c0="col0"))

    def testPlanBuilderFilter(self):
        self.__testDataFrame(lambda df: df.filter(df["c1"] == 1))

    def testPlanBuilderLimit(self):
        self.__testDataFrame(lambda df: df.select(count=50, offset=1))

    def testPlanBuilderShuffle(self):
        self.__testDataFrame(lambda df: df.shuffle(1))

    def testPlanBuilderAggregate(self):
        self.__testDataFrame(lambda df: df.groupBy("c0").aggregate(mean=Mean("c2")))

    def testPlanBuilderOrderBy(self):
        self.__testDataFrame(lambda df: df.orderBy("c2"))

    def testPlanBuilderPythonOperator(self):
        dtype = makeDataframe(1).dtype

        @dataframeFunction(dtype)
        def pythonOperator(df, col, mul):
            df[col] = df[col] * mul
            return df

        self.__testDataFrame(lambda df: df.map(pythonOperator, "c2", 2))

    def testPlanBuilderWriteRead(self):
        def roundTrip(df: DataFrame, fileFormat=FileFormat.DWRF):
            dtype = df.dtype
            with tempfile.TemporaryDirectory() as out_sub:
                planBuilderFactory = df._planFactory_
                LocalTableWriter(out_sub, fileFormat).write(df)
                df = None
                for path in [
                    os.path.join(root, f)
                    for root, _, files in os.walk(out_sub)
                    for f in files
                ]:
                    readDf = LocalTableReader(
                        path, dtype, fileFormat, planFactory=planBuilderFactory
                    ).read()
                    if df is None:
                        df = readDf
                    else:
                        df.append(readDf)
                _ = df.orderBy("c2")._data_
                return df

        self.__testDataFrame(roundTrip)
