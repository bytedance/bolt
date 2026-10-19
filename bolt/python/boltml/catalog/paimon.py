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

import json
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from functools import reduce
from operator import add
from typing import Any, Optional, Union
from urllib.parse import urlsplit, urlunsplit

import pypaimon
from pypaimon.schema.data_types import DataField
from pypaimon.read.scanner.full_starting_scanner import FullStartingScanner
from pypaimon.table.file_store_table import FileStoreTable

from pybolt import (
    FileFormat,
    FileSystem,
    RowType,
)
from pybolt import Split as BoltSplit

from .common import (
    CatalogTable,
    hiveFileFormatToBoltFileFormat,
    hiveTypeToBoltType,
    CatalogSplit,
)
from ..expression.boolean import BooleanExpression, BooleanOp


class PaimonMergeEngine(Enum):
    """
    Enumeration of Paimon merge engines.
    """

    AGGREGATION = "aggregation"
    DEDUPLICATE = "deduplicate"
    PARTIAL_UPDATE = "partial-update"
    # Bolt does not support first-row merge engine yet.
    # FIRST_ROW = "first-row"


class TimestampMeta:
    def __init__(self, name, value):
        self.__name = name
        self.__value = value

    def __hash__(self) -> int:
        return hash(self.value)

    def __repr__(self) -> str:
        if self.name == "MILLIS":
            return f"MILLIS({self.value})"
        return self.name

    def __eq__(self, other) -> bool:
        if not isinstance(other, type(self)):
            return False
        if other.name != self.name:
            return False
        return other.value == self.value

    @property
    def name(self) -> str:
        return self.__name

    @property
    def value(self) -> Union[str, int]:
        return self.__value


class PaimonTimestamp(TimestampMeta):
    """
    PaimonTimestamp type for PaimonOptions.
    """

    EARLIEST = TimestampMeta("EARLIEST", "EARLIEST")
    LATEST = TimestampMeta("LATEST", "LATEST")

    @classmethod
    def MILLIS(cls, ms: int):
        return TimestampMeta("MILLIS", ms)


@dataclass
class PaimonOptions:
    """
    User dataclass to provide options to read a paimon table.
    """

    # A predicate to filter partitions.
    # The predicate is built from a field, e.g.:
    # `(Field('c1') < 3) & (Field('c2') == 0)`
    partitionFilter: Optional[BooleanExpression] = None

    # The target size for splits.
    # The resulting splits may be smaller if there is less data to read than
    # this size.
    # The resulting splits may be larger if the last file size in the split is
    # large.
    targetSplitSize: int = 256 << 20  # 256 MiB

    # Timestamp for the snapshot from which to read split records.
    startTimestampMs: Optional[PaimonTimestamp] = field(
        default_factory=lambda: PaimonTimestamp.LATEST
    )

    # Inclusive maximum timestamp for the last split records. Without an end,
    # LATEST reads the full table; other starts read that snapshot's delta.
    endTimestampMs: Optional[PaimonTimestamp] = None

    # The merge engine needed to read files.
    mergeEngine: Optional[PaimonMergeEngine] = None


@dataclass
class PaimonSchema:
    """
    Base class resulting in reading a paimon schema from a schema file.
    """

    id: int
    fields: list[DataField]
    primaryKeys: list[str]
    partitionKeys: list[str]
    fileFormat: str
    options: dict[str, str] = field(default_factory=dict)

    @staticmethod
    def fromJson(jsonSchema: dict[str, Any]) -> "PaimonSchema":
        """
        Snapshots are encoded in json format.
        This function decodes the relevant fields for this module
        implementation.
        """
        return PaimonSchema(
            id=jsonSchema["id"],
            fields=[DataField.from_dict(f) for f in jsonSchema["fields"]],
            primaryKeys=jsonSchema["primaryKeys"],
            partitionKeys=jsonSchema["partitionKeys"],
            fileFormat=jsonSchema.get("options", {}).get("file.format", "parquet"),
            options=dict(jsonSchema.get("options", {})),
        )

    @staticmethod
    def fromTableUri(tableUri: str, schemaId: int) -> "PaimonSchema":
        schemaUri = f"{tableUri}/schema/schema-{schemaId}"
        fs = FileSystem.get(schemaUri)
        schemaContent = fs.read(schemaUri)
        jsonSchema = json.loads(schemaContent)
        return PaimonSchema.fromJson(jsonSchema)


@dataclass
class PaimonSnapshot:
    """
    Base class resulting from reading a paimon snapshot from a snapshot file.
    """

    id: int
    timeMs: int
    schemaId: int

    @staticmethod
    def fromJson(jsonSnapshot: dict[str, Any]) -> "PaimonSnapshot":
        """
        Snapshots are encoded in json format.
        This function decodes the relevant fields for this module
        implementation.
        """
        return PaimonSnapshot(
            id=jsonSnapshot["id"],
            timeMs=jsonSnapshot["timeMillis"],
            schemaId=jsonSnapshot["schemaId"],
        )

    @staticmethod
    def fromTableUri(
        uri: str,
        timestampMs: Optional[PaimonTimestamp] = PaimonTimestamp.LATEST,
    ) -> "PaimonSnapshot":
        """
        Reads a snapshot from the filesystem for a table path (`uri`).
        The resulting snapshot will be exactly the earliest or latest
        respectively if `timestampMs` is PaimonTimestamp.EARLIEST or PaimonTimestamp.LATEST or the
        closest snapshot that contains the `timestampMs`.
        """
        fs = FileSystem.get(uri)
        return PaimonSnapshot.__fromTableUri(uri, timestampMs, None, fs)

    @staticmethod
    def __fromTableUri(
        uri: str,
        timestampMs: Optional[PaimonTimestamp],
        snapshotId: Optional[int],
        fs: FileSystem,
    ) -> "PaimonSnapshot":
        if snapshotId is not None:
            snapshotFile = uri + f"/snapshot/snapshot-{snapshotId}"
            snapshotContent = fs.read(snapshotFile)
            jsonSnapshot = json.loads(snapshotContent)
            return PaimonSnapshot.fromJson(jsonSnapshot)
        if timestampMs in (PaimonTimestamp.EARLIEST, PaimonTimestamp.LATEST):
            snapshotFile = uri + f"/snapshot/{timestampMs.value}"
            if fs.exists(snapshotFile):
                snapshotId = int(fs.read(snapshotFile).strip())
                if fs.exists(uri + f"/snapshot/snapshot-{snapshotId}"):
                    return PaimonSnapshot.__fromTableUri(uri, None, snapshotId, fs)
        if timestampMs is None:
            raise ValueError(f"Invalid paimon timestamp: {timestampMs}.")

        # Expiration may remove snapshot 1 (and older hints). Search the actual
        # retained IDs, including tables whose writer does not create EARLIEST.
        snapshotIds = sorted(
            int(match[1])
            for entry in fs.list(uri + "/snapshot")
            if (match := re.fullmatch(r"snapshot-(\d+)", os.path.basename(entry)))
        )
        if not snapshotIds:
            raise ValueError(f"Paimon table '{uri}' has no retained snapshots")
        if timestampMs in (PaimonTimestamp.EARLIEST, PaimonTimestamp.LATEST):
            snapshotId = (
                snapshotIds[0]
                if timestampMs == PaimonTimestamp.EARLIEST
                else snapshotIds[-1]
            )
            return PaimonSnapshot.__fromTableUri(uri, None, snapshotId, fs)
        if isinstance(timestampMs.value, int):
            low = 0
            high = len(snapshotIds) - 1
            best: Optional[PaimonSnapshot] = None
            while low <= high:
                middle = low + (high - low) // 2
                snapshot = PaimonSnapshot.__fromTableUri(
                    uri, None, snapshotIds[middle], fs
                )
                commitTime = snapshot.timeMs
                if commitTime > timestampMs.value:
                    high = middle - 1
                else:
                    best = snapshot
                    low = middle + 1
            if best is None:
                raise ValueError(
                    f"No retained Paimon snapshot at or before {timestampMs.value}"
                )
            return best

        raise ValueError(f"Invalid paimon timestamp: {timestampMs}.")


@dataclass
class PaimonSplit(CatalogSplit):
    filePaths: list[str]
    fileFormat: FileFormat

    def toBoltSplit(self) -> BoltSplit:
        return BoltSplit.paimon(self.filePaths, self.fileFormat, {})


def makePaimonPredicate(
    fields: list[DataField], predicate: BooleanExpression
) -> pypaimon.common.predicate.Predicate:
    """
    Internal function to make a paimon predicate from our own predicate API.
    pypaimon requires to know the table fields when making the predicate which
    might not be known by the user when they craft the predicate but which is
    known by the PaimonTable when it is time to use it to list splits.
    """
    if not isinstance(predicate, BooleanExpression):
        raise TypeError(f"Invalid predicate type: {type(predicate)}")
    if predicate.op is BooleanOp.UDF:
        raise ValueError(
            f"Paimon predicate does not support UDF calls: {predicate.lhs}"
        )
    if predicate.op is BooleanOp.AND:
        return pypaimon.common.predicate_builder.PredicateBuilder.and_predicates(
            [
                makePaimonPredicate(fields, predicate.lhs),
                makePaimonPredicate(fields, predicate.rhs),
            ]
        )
    if predicate.op is BooleanOp.OR:
        return pypaimon.common.predicate_builder.PredicateBuilder.or_predicates(
            [
                makePaimonPredicate(fields, predicate.lhs),
                makePaimonPredicate(fields, predicate.rhs),
            ]
        )
    predicate_builder = pypaimon.common.predicate_builder.PredicateBuilder(fields)

    rhs = predicate.rhs
    if rhs.asLiteral() is None:
        raise ValueError(
            f"Unsupported value {rhs.expr()} for paimon boolean arithmetic "
            "comparison. Value must be a literal."
        )
    rhs = rhs.asLiteral().value
    if predicate.op is BooleanOp.EQ:
        return predicate_builder.equal(predicate.lhs.expr(), rhs)
    if predicate.op is BooleanOp.GE:
        return predicate_builder.greater_or_equal(predicate.lhs.expr(), rhs)
    if predicate.op is BooleanOp.GT:
        return predicate_builder.greater_than(predicate.lhs.expr(), rhs)
    if predicate.op is BooleanOp.LE:
        return predicate_builder.less_or_equal(predicate.lhs.expr(), rhs)
    if predicate.op is BooleanOp.LT:
        return predicate_builder.less_than(predicate.lhs.expr(), rhs)
    if predicate.op is BooleanOp.NE:
        return predicate_builder.not_equal(predicate.lhs.expr(), rhs)


class _SnapshotScanner(FullStartingScanner):
    """Pin the SDK's full scan to the snapshot used to resolve the schema."""

    def __init__(self, table, predicate, snapshotId):
        super().__init__(table, predicate, None)
        self.snapshotId = snapshotId

    def plan_files(self):
        snapshot = self.snapshot_manager.get_snapshot_by_id(self.snapshotId)
        if snapshot is None:
            raise ValueError(
                f"Paimon snapshot {self.snapshotId} is no longer available"
            )
        return self.read_manifest_entries(self.manifest_list_manager.read_all(snapshot))


def _restoreFileUri(tableUri: str, filePath: str) -> str:
    """Restore the scheme removed by pypaimon without repeating its authority."""
    table = urlsplit(tableUri)
    if not table.scheme or "://" in filePath:
        return filePath
    if table.netloc and filePath.lstrip("/").startswith(table.netloc + "/"):
        return table.scheme + "://" + filePath.lstrip("/")
    return urlunsplit((table.scheme, table.netloc, "/" + filePath.lstrip("/"), "", ""))


class PaimonTable(CatalogTable):
    """
    Class representing a paimon table on the filesystem.
    """

    def __init__(self, tableUri: str, options: PaimonOptions = PaimonOptions()):
        self.__uri: str = tableUri
        self.__options = options
        self.__schemas: dict[int, PaimonSchema] = {}

    def __paimonSnapshot(
        self,
        startTimestampMs: PaimonTimestamp,
    ) -> Optional[PaimonSnapshot]:
        """
        Get the snapshot of this table for a specific timestamp.
        """
        fs = FileSystem.get(self.__uri)
        snapshotDir = self.__uri + "/snapshot"
        if not fs.exists(snapshotDir):
            return None
        if not fs.exists(snapshotDir + "/LATEST") and not any(
            re.fullmatch(r"snapshot-\d+", os.path.basename(path))
            for path in fs.list(snapshotDir)
        ):
            return None
        return PaimonSnapshot.fromTableUri(self.__uri, startTimestampMs)

    def __paimonSchema(
        self,
        snapshot: Optional[PaimonSnapshot],
    ) -> PaimonSchema:
        """
        Get the schema of this table for a specific timestamp.
        """
        schemaId = (
            snapshot.schemaId
            if snapshot is not None
            else self.__paimonTable().table_schema.id
        )
        try:
            return self.__schemas[schemaId]
        except KeyError:
            schema = PaimonSchema.fromTableUri(self.__uri, schemaId)
            self.__schemas[schemaId] = schema
            return schema

    def __paimonTable(
        self, schemaId: Optional[int] = None
    ) -> pypaimon.table.table.Table:
        """
        Get the pypaimon table object corresponding to this table.
        """
        warehousePath = os.path.dirname(os.path.dirname(self.__uri))
        dbName = os.path.basename(os.path.dirname(self.__uri)).removesuffix(
            pypaimon.catalog.catalog.Catalog.DB_SUFFIX
        )
        tableName = self.getName()

        catalog = pypaimon.CatalogFactory().create(
            {pypaimon.common.config.CatalogOptions.WAREHOUSE: warehousePath}
        )
        table = catalog.get_table(f"{dbName}.{tableName}")
        if schemaId is not None and table.table_schema.id != schemaId:
            table = FileStoreTable(
                table.file_io,
                table.identifier,
                table.table_path,
                table.schema_manager.get_schema(schemaId),
                table.catalog_environment,
            )
        return table

    def getPath(self) -> str:
        return self.__uri

    def getName(self) -> str:
        return self.__uri.split("/")[-1]

    def getPartitionKeys(self) -> list[str]:
        return self.__paimonSchema(
            self.__paimonSnapshot(self.__options.startTimestampMs)
        ).partitionKeys

    def getFileFormat(self) -> FileFormat:
        return hiveFileFormatToBoltFileFormat(
            self.__paimonSchema(
                self.__paimonSnapshot(self.__options.startTimestampMs)
            ).fileFormat
        )

    def getSchema(self) -> RowType:
        return self.__rowType(
            self.__paimonSchema(self.__paimonSnapshot(self.__options.startTimestampMs))
        )

    @staticmethod
    def __rowType(schema: PaimonSchema) -> RowType:
        return RowType(
            [f.name for f in schema.fields],
            [hiveTypeToBoltType(str(f.type)) for f in schema.fields],
        )

    def _readState(self, options):
        if type(options) is not PaimonOptions:
            raise TypeError("Expected PaimonOptions for a PaimonTable read")
        snapshot = self.__paimonSnapshot(options.startTimestampMs)
        schema = self.__paimonSchema(snapshot)
        return (
            self.__rowType(schema),
            schema.partitionKeys,
            self.__readerParameters(options, schema),
            self.__readerSplits(options, snapshot, schema),
        )

    def getReaderParameters(self, options: PaimonOptions) -> dict:
        # Note that self.options and options are both PaimonOptions but may be
        # different.
        # When the table instance is created from a hive catalog, default
        # options may be sufficient to call the CatalogTable methods
        # but when it is time to read it (because they don't matter as long
        # as the schema remains the same through time), we want the real user
        # options that may impact the actual data in the table.
        if type(options) is not PaimonOptions:
            raise TypeError(
                f"Invalid option type '{type(options)}' for PaimonTable. "
                "Expected PaimonOptions."
            )
        schema = self.__paimonSchema(self.__paimonSnapshot(options.startTimestampMs))
        return self.__readerParameters(options, schema)

    @staticmethod
    def __readerParameters(options, schema):
        # Some Python writers persist numeric/bool option values. The native
        # connector accepts the same string-valued configuration as Java Paimon.
        parameters = {
            key: str(value).lower() if isinstance(value, bool) else str(value)
            for key, value in schema.options.items()
        }
        parameters["file.format"] = schema.fileFormat
        if len(schema.primaryKeys) > 0:
            parameters["primary-key"] = ",".join(schema.primaryKeys)
        if options.mergeEngine is not None:
            parameters["merge-engine"] = options.mergeEngine.value
        if parameters.get("merge-engine", "deduplicate") not in {
            engine.value for engine in PaimonMergeEngine
        }:
            raise NotImplementedError(
                f"Unsupported Paimon merge engine: {parameters['merge-engine']}"
            )
        return parameters

    def getReaderSplits(self, options: PaimonOptions) -> list[CatalogSplit]:
        # See note in getReaderParameters.
        if type(options) is not PaimonOptions:
            raise TypeError(
                f"Invalid option type '{type(options)}' for PaimonTable. "
                "Expected PaimonOptions."
            )

        snapshot = self.__paimonSnapshot(options.startTimestampMs)
        return self.__readerSplits(options, snapshot, self.__paimonSchema(snapshot))

    def __readerSplits(self, options, snapshot, schema):
        if snapshot is None:
            return []
        fileFormat = hiveFileFormatToBoltFileFormat(schema.fileFormat)
        table = self.__paimonTable(schema.id)
        table.options |= {
            pypaimon.common.core_options.CoreOptions.SOURCE_SPLIT_TARGET_SIZE: str(
                options.targetSplitSize
            )
        }
        partFilter = (
            makePaimonPredicate(schema.fields, options.partitionFilter)
            if options.partitionFilter is not None
            else None
        )
        if (
            options.endTimestampMs is None
            and options.startTimestampMs == PaimonTimestamp.LATEST
        ):
            scanner = _SnapshotScanner(table, partFilter, snapshot.id)
        else:
            start = snapshot.id - 1
            if options.endTimestampMs is None:
                end = start + 1
            else:
                endSnapshot = self.__paimonSnapshot(options.endTimestampMs)
                if endSnapshot.schemaId != schema.id:
                    raise NotImplementedError(
                        "Paimon incremental reads across schema changes are not supported"
                    )
                end = endSnapshot.id
            if end < snapshot.id:
                raise ValueError("Paimon end snapshot precedes the start snapshot")
            scanner = pypaimon.read.scanner.incremental_starting_scanner.IncrementalStartingScanner(
                table, partFilter, None, start, end
            )

        paimonSplits = scanner.scan().splits()

        if len(paimonSplits) == 0:
            return []
        if len(schema.primaryKeys) == 0:
            return [
                PaimonSplit([_restoreFileUri(self.__uri, f)], fileFormat)
                for f in reduce(add, [split.file_paths for split in paimonSplits])
            ]
        else:
            return [
                PaimonSplit(
                    [_restoreFileUri(self.__uri, f) for f in split.file_paths],
                    fileFormat,
                )
                for split in paimonSplits
            ]
