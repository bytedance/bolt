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

"""Catalog regressions using local files and HMS metadata, without a service."""

import copy
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as parquet
import pybolt
import pypaimon

from .._generated.hive_metastore.ttypes import (
    FieldSchema,
    Partition,
    SerDeInfo,
    StorageDescriptor,
    Table,
)
from ..catalog.common import (
    boltTypeToHiveType,
    hiveFileFormatToBoltFileFormat,
    hiveTypeToBoltType,
)
from ..catalog.hive import HiveOptions, HiveTable
from ..catalog.paimon import (
    PaimonMergeEngine,
    PaimonOptions,
    PaimonSchema,
    PaimonSnapshot,
    PaimonTable,
    PaimonTimestamp,
    _SnapshotScanner,
    _restoreFileUri,
    makePaimonPredicate,
)
from ..expression import Expression, NumProjectExpression
from ..dataframe import DataFrame
from ..executor.ray import RayExecutor
from ..plan_builder.bolt import BoltPlanBuilder, BoltPlanBuilderFactory
from ..plan_builder.substrait import (
    SubstraitPlanBuilderFactory,
    plan_is_substrait_lowerable,
)


class CatalogTypeTest(unittest.TestCase):
    def testCommonCatalogTypes(self):
        for source, expected in {
            "date": "DATE",
            "CHAR(4)": "VARCHAR",
            "varchar(100)": "VARCHAR",
            "TIMESTAMP(6)": "TIMESTAMP",
            "BYTES": "VARBINARY",
            "VARBINARY(8)": "VARBINARY",
            "ARRAY<BIGINT NOT NULL> NOT NULL": "ARRAY<BIGINT>",
            "MAP<STRING NOT NULL, DECIMAL(12, 2) NOT NULL>": "MAP<VARCHAR,DECIMAL(12, 2)>",
        }.items():
            with self.subTest(source=source):
                self.assertEqual(str(hiveTypeToBoltType(source)), expected)
        self.assertEqual(boltTypeToHiveType(pybolt.DateType()), "date")

    def testNestedFieldNamesArePreserved(self):
        dtype = hiveTypeToBoltType(
            'STRUCT<userId: INT, `User, Name`: ARRAY<ROW<"Case:Sensitive": STRING>>>'
        )
        self.assertEqual(dtype.names(), ["userId", "User, Name"])
        self.assertEqual(dtype.childAt(1).elementType().names(), ["Case:Sensitive"])

    def testUnsupportedTimezoneDoesNotSilentlyLoseSemantics(self):
        with self.assertRaises(NotImplementedError):
            hiveTypeToBoltType("TIMESTAMP(6) WITH LOCAL TIME ZONE")

    def testStandardHiveFormats(self):
        for name, expected in {
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat": pybolt.FileFormat.PARQUET,
            "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat": pybolt.FileFormat.ORC,
            "parquet": pybolt.FileFormat.PARQUET,
        }.items():
            with self.subTest(name=name):
                self.assertEqual(hiveFileFormatToBoltFileFormat(name), expected)


class HiveMetadataTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.client = Mock()
        self.metadata = Table(
            tableName="events",
            dbName="default",
            sd=StorageDescriptor(
                cols=[FieldSchema("value", "bigint")],
                location=str(self.root),
                outputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            ),
            partitionKeys=[FieldSchema("day", "string")],
            parameters={},
        )

    def testPartitionColumnsAndNullValues(self):
        parquet.write_table(pa.table({"value": [7]}), self.root / "part.parquet")
        self.client.get_partitions.return_value = [
            Partition(values=["__HIVE_DEFAULT_PARTITION__"], sd=self.metadata.sd)
        ]
        table = HiveTable(self.client, self.metadata)
        self.assertEqual(table.getSchema().names(), ["value", "day"])
        splits = table.getReaderSplits(HiveOptions())
        self.assertEqual(len(splits), 1)
        self.assertEqual(splits[0].partMap, {"day": None})
        self.assertIsInstance(splits[0].toBoltSplit(), pybolt.Split)
        result = table.read(HiveOptions(), planFactory=BoltPlanBuilderFactory())
        self.assertEqual(result.toArrow().to_pylist(), [{"value": 7, "day": None}])

    def testDefaultFactoryReadsPartitionValuesAndRejectsRayLowering(self):
        parquet.write_table(pa.table({"value": [7]}), self.root / "part.parquet")
        self.client.get_partitions.return_value = [
            Partition(values=["2026-01-01"], sd=self.metadata.sd)
        ]
        table = HiveTable(self.client, self.metadata)
        dataframe = table.read(HiveOptions())
        self.assertFalse(
            plan_is_substrait_lowerable(dataframe._planBuilder_.logicalPlan())
        )
        with self.assertRaisesRegex(NotImplementedError, "Ray execution"):
            RayExecutor.eval(Mock(), dataframe._planBuilder_)
        self.assertEqual(
            dataframe.toArrow().to_pylist(), [{"value": 7, "day": "2026-01-01"}]
        )

        # A normal Substrait source can join a native-only catalog on its RHS.
        joined = DataFrame({"value": [7, 8]}).join(table.read(HiveOptions()), {"value"})
        self.assertEqual(
            joined.copy().toArrow().to_pylist(), [{"value": 7, "day": "2026-01-01"}]
        )

    def testEmptyPartitionedTableDoesNotReadUnregisteredFiles(self):
        (self.root / "orphan.parquet").touch()
        self.client.get_partitions.return_value = []
        table = HiveTable(self.client, self.metadata)
        self.assertEqual(table.getReaderSplits(HiveOptions()), [])
        dataframe = table.read(HiveOptions())
        self.assertEqual(dataframe.names, ["value", "day"])
        self.assertEqual(dataframe.toArrow().to_pylist(), [])

    def testHiddenFilesAndDirectoriesAreExcluded(self):
        self.metadata.partitionKeys = []
        for relative in (
            "data.parquet",
            "_SUCCESS",
            ".data.crc",
            "_temporary/part.parquet",
            ".staging/part.parquet",
            "bucket/part.parquet",
        ):
            path = self.root / relative
            path.parent.mkdir(exist_ok=True)
            path.touch()
        table = HiveTable(self.client, self.metadata)
        paths = {
            Path(split.filePath).relative_to(self.root).as_posix()
            for split in table.getReaderSplits(HiveOptions())
        }
        self.assertEqual(paths, {"data.parquet", "bucket/part.parquet"})
        self.client.get_partitions.assert_not_called()

    def testTextSerdeIsNotSilentlyReplacedByDefaultDelimiter(self):
        self.metadata.partitionKeys = []
        self.metadata.sd.outputFormat = (
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
        )
        self.metadata.sd.serdeInfo = SerDeInfo(parameters={"field.delim": ","})
        with self.assertRaisesRegex(
            NotImplementedError, "Unsupported hive file format"
        ):
            HiveTable(self.client, self.metadata).read(HiveOptions())


class PaimonMetadataTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "snapshot").mkdir()
        (self.root / "schema").mkdir()
        self.schema = {
            "id": 0,
            "fields": [{"id": 0, "name": "value", "type": "BIGINT"}],
            "primaryKeys": [],
            "partitionKeys": [],
            "options": {},
        }
        self.writeSchema(self.schema)

    def writeSchema(self, schema):
        (self.root / "schema" / f"schema-{schema['id']}").write_text(json.dumps(schema))

    def writeSnapshot(self, identifier, time, schemaId=0):
        (self.root / "snapshot" / f"snapshot-{identifier}").write_text(
            json.dumps({"id": identifier, "timeMillis": time, "schemaId": schemaId})
        )
        (self.root / "snapshot" / "LATEST").write_text(str(identifier))

    def testMissingFormatAndTableOptions(self):
        self.schema["primaryKeys"] = ["value"]
        self.schema["options"] = {
            "merge-engine": "aggregation",
            "fields.value.aggregate-function": "sum",
            "sequence.field": "value",
            "ignore-delete": True,
            "bucket": 1,
        }
        self.writeSchema(self.schema)
        self.writeSnapshot(1, 100)
        table = PaimonTable(str(self.root))
        params = table.getReaderParameters(PaimonOptions())
        self.assertEqual(
            params,
            self.schema["options"]
            | {
                "file.format": "parquet",
                "primary-key": "value",
                "ignore-delete": "true",
                "bucket": "1",
            },
        )
        overridden = table.getReaderParameters(
            PaimonOptions(mergeEngine=PaimonMergeEngine.DEDUPLICATE)
        )
        self.assertEqual(overridden["merge-engine"], "deduplicate")
        self.assertEqual(
            table.getReaderParameters(PaimonOptions())["merge-engine"], "aggregation"
        )

    def testUnsupportedMergeEngineIsRejected(self):
        self.schema["options"] = {"merge-engine": "first-row"}
        self.writeSchema(self.schema)
        self.writeSnapshot(1, 100)
        with self.assertRaisesRegex(NotImplementedError, "first-row"):
            PaimonTable(str(self.root)).getReaderParameters(PaimonOptions())

    def testExpiredSnapshotsAndMissingEarliestHint(self):
        self.writeSnapshot(5, 500)
        self.writeSnapshot(7, 700)
        for timestamp, expected in (
            (copy.deepcopy(PaimonTimestamp.EARLIEST), 5),
            (PaimonTimestamp.MILLIS(600), 5),
            (PaimonTimestamp.MILLIS(700), 7),
            (PaimonTimestamp.MILLIS(900), 7),
        ):
            with self.subTest(timestamp=timestamp):
                self.assertEqual(
                    PaimonSnapshot.fromTableUri(str(self.root), timestamp).id, expected
                )
        with self.assertRaisesRegex(ValueError, "No retained"):
            PaimonSnapshot.fromTableUri(str(self.root), PaimonTimestamp.MILLIS(100))

    def testLatestSchemaRefreshes(self):
        self.writeSnapshot(1, 100)
        table = PaimonTable(str(self.root))
        self.assertEqual(table.getSchema().names(), ["value"])
        newer = copy.deepcopy(self.schema)
        newer["id"] = 1
        newer["fields"].append({"id": 1, "name": "added", "type": "STRING"})
        self.writeSchema(newer)
        self.writeSnapshot(2, 200, 1)
        self.assertEqual(table.getSchema().names(), ["value", "added"])

    def testTimestampSelectsLastCommitInSameMillisecond(self):
        for identifier in (1, 2, 3):
            self.writeSnapshot(identifier, 100)
        snapshot = PaimonSnapshot.fromTableUri(
            str(self.root), PaimonTimestamp.MILLIS(100)
        )
        self.assertEqual(snapshot.id, 3)

    def testReadUsesRequestedSchemaAndOneSnapshot(self):
        self.writeSnapshot(1, 100)
        newer = copy.deepcopy(self.schema)
        newer["id"] = 1
        newer["fields"].append({"id": 1, "name": "added", "type": "STRING"})
        self.writeSchema(newer)
        self.writeSnapshot(2, 200, 1)
        table = PaimonTable(str(self.root))
        factory = Mock()
        with (
            patch.object(table, "_PaimonTable__readerSplits", return_value=[Mock()]),
            patch("boltml.catalog.common.DataFrame"),
        ):
            table.read(
                PaimonOptions(startTimestampMs=PaimonTimestamp.MILLIS(100)),
                planFactory=factory,
            )
        self.assertEqual(factory.fromCatalog.call_args.args[1].names(), ["value"])

    def testNotEqualPredicateUsesSdkApi(self):
        fields = PaimonSchema.fromJson(self.schema).fields
        expression = NumProjectExpression(
            Expression("value", pybolt.BigintType(), None)
        )
        predicate = makePaimonPredicate(fields, expression != 7)
        self.assertEqual(predicate.method, "notEqual")
        self.assertEqual(predicate.literals, [7])

    def testRemotePathsRestoreSchemeOnce(self):
        for table, source, expected in (
            (
                "s3://bucket/db/table",
                "bucket/db/table/data.parquet",
                "s3://bucket/db/table/data.parquet",
            ),
            (
                "s3://bucket/db/table",
                "s3://bucket/db/table/data.parquet",
                "s3://bucket/db/table/data.parquet",
            ),
            (
                "hdfs://host:9000/db/table",
                "host:9000/db/table/data.parquet",
                "hdfs://host:9000/db/table/data.parquet",
            ),
            (
                "hdfs://host:9000/db/table",
                "/db/table/data.parquet",
                "hdfs://host:9000/db/table/data.parquet",
            ),
            (
                "file:///tmp/table",
                "/tmp/table/data.parquet",
                "file:///tmp/table/data.parquet",
            ),
            ("/tmp/table", "/tmp/table/data.parquet", "/tmp/table/data.parquet"),
        ):
            with self.subTest(source=source):
                self.assertEqual(_restoreFileUri(table, source), expected)


class LocalPaimonReadTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.catalog = pypaimon.CatalogFactory().create(
            {"warehouse": self.directory.name}
        )
        self.catalog.create_database("test", False)
        schema = pypaimon.Schema.from_pyarrow_schema(pa.schema([("value", pa.int64())]))
        self.catalog.create_table("test.events", schema, False)
        self.path = str(Path(self.directory.name) / "test.db" / "events")

    def write(self, values):
        builder = self.catalog.get_table("test.events").new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow_batch(
                pa.record_batch([pa.array(values, pa.int64())], names=["value"])
            )
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()

    def values(self, splits):
        return sorted(
            value
            for split in splits
            for path in split.filePaths
            for value in parquet.read_table(path)["value"].to_pylist()
        )

    def testUnwrittenTableIsEmptyWithItsSchema(self):
        table = PaimonTable(self.path)
        self.assertEqual(table.getReaderSplits(PaimonOptions()), [])
        dataframe = table.read(PaimonOptions())
        self.assertEqual(dataframe.names, ["value"])
        self.assertEqual(dataframe.toArrow().to_pylist(), [])

    def testDefaultFormatAndLatestRefresh(self):
        table = PaimonTable(self.path)
        self.write([1])
        self.assertEqual(
            self.values(table.getReaderSplits(copy.deepcopy(PaimonOptions()))), [1]
        )
        self.write([2])
        self.assertEqual(self.values(table.getReaderSplits(PaimonOptions())), [1, 2])

    def testIncrementalEndIsInclusive(self):
        self.write([1])
        self.write([2])
        options = PaimonOptions(
            startTimestampMs=PaimonTimestamp.EARLIEST,
            endTimestampMs=PaimonTimestamp.LATEST,
        )
        self.assertEqual(
            self.values(PaimonTable(self.path).getReaderSplits(options)), [1, 2]
        )

    def testFullScanStaysOnResolvedSnapshot(self):
        self.write([1])
        original = _SnapshotScanner.plan_files

        def appendDuringPlanning(scanner):
            self.write([2])
            return original(scanner)

        with patch.object(_SnapshotScanner, "plan_files", appendDuringPlanning):
            splits = PaimonTable(self.path).getReaderSplits(PaimonOptions())
        self.assertEqual(self.values(splits), [1])


class NativeSourceFallbackTest(unittest.TestCase):
    def testComplexValuesRemainNativeAtBothSpillThresholds(self):
        arrays = [
            pa.array([{"UserID": 7}], pa.struct([("UserID", pa.int64())])),
            pa.array([None, [1, 2]], pa.list_(pa.int64())),
            pa.array([[Decimal("1.23")]], pa.list_(pa.decimal128(10, 2))),
            pa.array([None], pa.decimal128(10, 2)),
            pa.array([], pa.list_(pa.int64())),
            pa.array([[[("key", 1)]]], pa.list_(pa.map_(pa.string(), pa.int64()))),
            pa.array(
                [[("outer", [("inner", 2)])]],
                pa.map_(pa.string(), pa.map_(pa.string(), pa.int64())),
            ),
        ]
        for array in arrays:
            for threshold in ("0", "1000"):
                with (
                    self.subTest(dtype=str(array.type), threshold=threshold),
                    patch.dict(
                        "os.environ", {"BOLTML_VIRTUAL_TABLE_MAX_ROWS": threshold}
                    ),
                ):
                    data = pybolt.importFromArrow(
                        pa.StructArray.from_arrays([array], names=["payload"])
                    )
                    builder = SubstraitPlanBuilderFactory().fromData(data)
                    self.assertIsInstance(builder, BoltPlanBuilder)
                    self.assertFalse(plan_is_substrait_lowerable(builder.logicalPlan()))
                    dataframe = DataFrame(builder).copy().select(["payload"])
                    table = dataframe.toArrow(pa.Table)
                    self.assertEqual(
                        table.to_pylist(),
                        [{"payload": item} for item in array.to_pylist()],
                    )
                    self.assertEqual(table.schema, pa.schema([("payload", array.type)]))
                    with self.assertRaisesRegex(
                        NotImplementedError, "Ray execution is not supported"
                    ):
                        RayExecutor().eval(builder.copy())

    def testPrimitiveArraysStillUseSubstrait(self):
        data = pybolt.rowVector(
            ["tensor"],
            [pybolt.fromList([[1.0, 2.0]], pybolt.ArrayType(pybolt.DoubleType()))],
        )
        builder = SubstraitPlanBuilderFactory().fromData(data)
        self.assertTrue(plan_is_substrait_lowerable(builder.logicalPlan()))
        self.assertEqual(
            DataFrame(builder).toArrow().to_pylist(), [{"tensor": [1.0, 2.0]}]
        )

    def testNestedRowFileReadRetainsFieldNames(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "nested.parquet")
            arrow = pa.table(
                {
                    "payload": pa.array(
                        [{"UserID": 7}], pa.struct([("UserID", pa.int64())])
                    )
                }
            )
            parquet.write_table(arrow, path)
            schema = pybolt.RowType(
                ["payload"], [pybolt.RowType(["UserID"], [pybolt.BigintType()])]
            )
            builder = SubstraitPlanBuilderFactory().fromLocalFiles(
                [path], schema, pybolt.FileFormat.PARQUET
            )
            self.assertIsInstance(builder, BoltPlanBuilder)
            self.assertEqual(
                DataFrame(builder).toArrow().to_pylist(), [{"payload": {"UserID": 7}}]
            )


if __name__ == "__main__":
    unittest.main()
