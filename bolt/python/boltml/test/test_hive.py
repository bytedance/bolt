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

import copy
import unittest
from typing import Union

import pyarrow as pa
import pyarrow.compute as pc
import pybolt

from ..dataframe import DataFrame
from ..catalog import HiveCatalog, HiveClient, HiveOptions
from ..catalog.common import boltTypeToHiveType, hiveTypeToBoltType
from ..plan_builder.bolt import BoltPlanBuilderFactory
from .._generated.hive_metastore.ttypes import (
    AddPrimaryKeyRequest,
    Database,
    FieldSchema,
    Partition,
    SerDeInfo,
    SQLPrimaryKey,
    StorageDescriptor,
    Table,
)
from ..writer.local import LocalTableWriter

from .utils import WithDataFrame

data = {
    "c0": pybolt.fromList([False, True], pybolt.BooleanType()),
    "c2": pybolt.fromList([-1, 1], pybolt.IntegerType()),
    "c3": pybolt.fromList([-1 * 1 << 33, 1 << 33], pybolt.BigintType()),
    # pybolt does not implement conversion of HUGEINT type to variant.
    # "c4": pybolt.fromList([-1 * 1<<65, 1<<65], pybolt.HugeintType()),
    "c5": pybolt.fromList([-1, 1], pybolt.SmallintType()),
    "c6": pybolt.fromList([-1, 1], pybolt.TinyintType()),
    "c7": pybolt.fromList([-2.0, 2.0], pybolt.RealType()),
    "c8": pybolt.fromList([-2.456467567865868, 1.0], pybolt.DoubleType()),
    # The table read/write seams broken for Timestamp.
    # We handle the type conversion right for the table schema.
    # However, the value we read back from the file is different than the
    # value we wrote.
    "c9": pybolt.fromList(
        [pybolt.Timestamp(0, 0), pybolt.Timestamp(0, 0)], pybolt.TimestampType()
    ),
    "c10": pybolt.fromList(["val0", "val1"], pybolt.VarcharType()),
    "c11": pybolt.fromList(
        [s.encode("utf-8") for s in ("bin0", "bin1")],
        pybolt.VarbinaryType(),
    ),
    "c12": pybolt.fromList([[1, 2], [3, 4]], pybolt.ArrayType(pybolt.BigintType())),
    "c13": pybolt.fromList(
        [{"k0": "v0", "k1": "v1"}, {"k2": "v2"}],
        pybolt.MapType(pybolt.VarcharType(), pybolt.VarcharType()),
    ),
    # Bolt's parquet reader does not currently support materializing row
    # columns, so RowType is covered by the schema conversion test below.
    # "c14": pybolt.rowVector(
    #     ["f0", "f1"],
    #     [
    #         pybolt.fromList([0, 2], pybolt.BigintType()),
    #         pybolt.fromList([1, 3], pybolt.BigintType()),
    #     ],
    # ),
    # Pybolt constructor for variant of type ShortDecimal use the
    # DeepCopiedType as input, i.e BIGINT. It does not make sense from
    # a user perspective (i.e you would encode a python float as a
    # decimal value) but this is merely a test.
    "c15": pybolt.fromList([11, 232], pybolt.ShortDecimalType(16, 16)),
    # pybolt does not implement conversion of long decimal type to variant
    # because it is encoded as a HUGEINT type kind.
    # "c16": ([4343546567, 7678764532], pybolt.LongDecimalType(20, 19))
}


def rowTypeToHiveSchema(rowType: pybolt.RowType) -> list[FieldSchema]:
    return [
        FieldSchema(name, boltTypeToHiveType(boltType).lower())
        for name, boltType in (rowType[i] for i in range(len(rowType)))
    ]


class HiveTestCommon:
    def setUp(self):
        try:
            self.client = HiveClient()
        except Exception:
            raise unittest.SkipTest("Failed to initialize HiveClient for unittests.")
        self.dbName = f"boltml_{self.__class__.__name__.lower()}_db"
        self.db = Database(
            name=self.dbName,
            description="Database to test bolt connection to hive",
        )

        # Cleanup prior database if it was not properly removed.
        try:
            self.client.drop_database(self.dbName, True, True)
        except Exception:
            pass

        # Create database
        self.client.create_database(self.db)

    def tearDown(self):
        self.client.drop_database(self.dbName, True, True)

    @staticmethod
    def makeThriftKeys(schema: pybolt.RowType, keys: list[str]) -> list[FieldSchema]:
        thriftKeys = []
        for pk in keys:
            dtype = next(
                (schema.childAt(i) for i, n in enumerate(schema.names()) if n == pk),
                None,
            )
            if dtype is None:
                raise ValueError(
                    f"Could not find partition key '{pk}' in schema: '{schema}'"
                )
            thriftKeys.append(FieldSchema(pk, boltTypeToHiveType(dtype)))
        return thriftKeys

    def __registerTable(
        self,
        tableName: str,
        schema: pybolt.RowType,
        fileFormat: pybolt.FileFormat = pybolt.FileFormat.PARQUET,
        numBuckets: int = 0,
        partitionKeys: list[str] = [],
        primaryKeys: list[str] = [],
    ) -> Table:
        cols = rowTypeToHiveSchema(schema)
        hiveFileFormat = str(fileFormat).split(".")[-1].lower()
        thriftKeys = self.makeThriftKeys(schema, partitionKeys)

        db = self.client.get_database(self.dbName)
        locationUri = db.locationUri + "/" + tableName

        sd = StorageDescriptor(
            cols=cols,
            location=locationUri,
            inputFormat=hiveFileFormat,
            outputFormat=hiveFileFormat,
            compressed=False,
            numBuckets=numBuckets,
            serdeInfo=SerDeInfo(),
            storedAsSubDirectories=False,
        )

        table = Table(
            tableName=tableName,
            dbName=self.dbName,
            sd=sd,
            partitionKeys=thriftKeys,
            parameters={},
        )
        self.client.create_table(table)
        return table

    def __registerPrimaryKeys(self, tableName: str, primaryKeys: list[str]):
        thriftPks = [
            SQLPrimaryKey(self.dbName, tableName, column_name=pk, pk_name=f"{pk}_pk")
            for pk in primaryKeys
        ]
        self.client.add_primary_key(AddPrimaryKeyRequest(thriftPks))

    def __registerPartitions(
        self,
        table: Table,
        df: Union[DataFrame, pa.Table],
        partitionKeys: list[str] = [],
        path: str = "",
        partValues: list[str] = [],
    ):
        # Stop condition:
        if len(partitionKeys) == 0:
            sd = copy.deepcopy(table.sd)
            sd.location += path
            part = Partition(
                values=partValues,
                dbName=self.dbName,
                tableName=table.tableName,
                sd=sd,
            )
            self.client.add_partition(part)
            return

        if isinstance(df, DataFrame):
            df = df.toArrow(pa.Table)
        key = partitionKeys[0]

        # Collect unique values for the current partition key.
        col = next((c for (n, c) in zip(df.column_names, df.columns) if n == key), None)
        if col is None:
            raise ValueError(
                f"Invalid partition key '{key}'. Must be a "
                f"dataframe column: {df.column_names}"
            )
        for partValue in pc.unique(col).to_pylist():
            partName = f"{key}={partValue}"
            self.__registerPartitions(
                table,
                df,
                partitionKeys[1:],
                f"{path}/{partName}",
                partValues + [str(partValue)],
            )

    def registerTable(
        self,
        df: DataFrame,
        tableName: str,
        fileFormat: pybolt.FileFormat = pybolt.FileFormat.PARQUET,
        numBuckets: int = 0,
        partitionKeys: list[str] = [],
        primaryKeys: list[str] = [],
    ) -> Table:
        if numBuckets < 0:
            raise ValueError("numBuckets must be greater or equal to 0.")

        table = self.__registerTable(
            tableName,
            df.dtype,
            fileFormat,
            numBuckets,
            partitionKeys,
            primaryKeys,
        )

        if len(primaryKeys) > 0:
            self.__registerPrimaryKeys(tableName, primaryKeys)

        if len(partitionKeys) > 0:
            self.__registerPartitions(table, df, partitionKeys)

        return table


class HiveTest(unittest.TestCase, HiveTestCommon):
    def setUp(self):
        HiveTestCommon.setUp(self)

    def tearDown(self):
        HiveTestCommon.tearDown(self)

    def testHiveBoltTypeConversion(self):
        types = [
            pybolt.BooleanType(),
            pybolt.IntegerType(),
            pybolt.BigintType(),
            pybolt.HugeintType(),
            pybolt.SmallintType(),
            pybolt.TinyintType(),
            pybolt.RealType(),
            pybolt.DoubleType(),
            pybolt.TimestampType(),
            pybolt.VarcharType(),
            pybolt.VarbinaryType(),
            pybolt.UnknownType(),
            pybolt.ShortDecimalType(18, 2),
            pybolt.LongDecimalType(19, 2),
            pybolt.ArrayType(pybolt.BigintType()),
            pybolt.MapType(pybolt.VarcharType(), pybolt.VarcharType()),
            pybolt.RowType(
                ["f0", "f1"],
                [pybolt.BigintType(), pybolt.ArrayType(pybolt.VarcharType())],
            ),
        ]
        for dtype in types:
            with self.subTest(str(dtype)):
                self.assertEqual(hiveTypeToBoltType(boltTypeToHiveType(dtype)), dtype)

    def addTable(
        self,
        tableName: str,
        df: DataFrame,
        fileFormat: pybolt.FileFormat = pybolt.FileFormat.PARQUET,
        numBuckets: int = 0,
        partitionKeys: list[str] = [],
    ):
        table = self.registerTable(df, tableName, fileFormat, numBuckets, partitionKeys)
        writerDf = DataFrame(
            df._data_, executor=df._executor_, planFactory=BoltPlanBuilderFactory()
        )

        if len(partitionKeys) == 0 and numBuckets == 0:
            LocalTableWriter(table.sd.location, fileFormat).write(writerDf.copy())
        else:
            bucketBy = []
            if numBuckets > 0:
                bucketBy = [c for c in df.names if c not in partitionKeys]
            LocalTableWriter(
                table.sd.location,
                fileFormat,
                partitionKeys,
                (bucketBy, numBuckets),
            ).write(writerDf.copy())

    @WithDataFrame.data(**data)
    def testReadNoPartition(self, df):
        tableName = "table0"
        self.addTable(tableName, df)

        catalog = HiveCatalog(self.client)
        catalogDf = catalog.getTable(tableName, self.dbName).read(
            HiveOptions(), planFactory=df._planFactory_, executor=df._executor_
        )
        self.assertEqual(catalogDf, df)

    @WithDataFrame.data(**data)
    def testReadAllPartitions(self, df):
        tableName = "table1"
        self.addTable(tableName, df, partitionKeys=["c10"])

        catalog = HiveCatalog(self.client)
        catalogDf = catalog.getTable(tableName, self.dbName).read(
            HiveOptions(), planFactory=df._planFactory_, executor=df._executor_
        )

        # The order of the partitions returned by hive may not match the order
        # in which they were written by the test setup.
        lhs = df.orderBy(df.names)
        rhs = catalogDf.orderBy(catalogDf.names)
        self.assertEqual(lhs, rhs)

    @WithDataFrame.data(**data)
    def testReadPartitionsWithFilter(self, df):
        tableName = "table2"
        self.addTable(tableName, df, partitionKeys=["c10"])

        catalog = HiveCatalog(self.client)
        options = HiveOptions(partitionFilterExpr="c10='val0'")
        catalogDf = catalog.getTable(tableName, self.dbName).read(
            options, planFactory=df._planFactory_, executor=df._executor_
        )
        self.assertEqual(catalogDf, df.filter(df["c10"] == "val0"))

    @WithDataFrame.data(**data)
    def testReadEmptySplits(self, df):
        tableName = "table2"
        self.addTable(tableName, df, partitionKeys=["c10"])

        catalog = HiveCatalog(self.client)
        options = HiveOptions(partitionFilterExpr="c10='INVALID'")

        # The partition filter should result in an empty partition set.
        with self.assertRaises(ValueError):
            catalog.getTable(tableName, self.dbName).read(
                options, planFactory=df._planFactory_, executor=df._executor_
            )

    @WithDataFrame.data(**data)
    def testReadBucketed(self, df):
        tableName = "table0"
        self.addTable(tableName, df, numBuckets=len(df) + 1)

        catalog = HiveCatalog(self.client)
        catalogDf = catalog.getTable(tableName, self.dbName).read(
            HiveOptions(), planFactory=df._planFactory_, executor=df._executor_
        )
        self.assertEqual(catalogDf.orderBy(catalogDf.names), df.orderBy(df.names))

    @WithDataFrame.data(**data)
    def testReadBucketedAndPartitioned(self, df):
        tableName = "table0"
        self.addTable(tableName, df, partitionKeys=["c10"], numBuckets=len(df) + 1)

        catalog = HiveCatalog(self.client)
        catalogDf = catalog.getTable(tableName, self.dbName).read(
            HiveOptions(), planFactory=df._planFactory_, executor=df._executor_
        )
        self.assertEqual(catalogDf, df)
