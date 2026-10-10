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
from dataclasses import dataclass, field
from enum import Enum
from functools import reduce
from operator import add
from typing import Any, Optional, Union
from urllib.parse import urlsplit, urlunsplit

import pypaimon
from pypaimon.schema.data_types import DataField

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

    # Maximum timestamp for the last split records.
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
            fileFormat=jsonSchema["options"]["file.format"],
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
        # This function is largely inspired from
        # pypaimon/snapshot/snapshot_manager.SnapshotManager.earlier_or_equal_time_millis.
        # It uses a recursive dichotic approach to find the closest `snapshotId`
        # before `timestampMs` with a special case handling EARLIEST and LATEST
        # timestamps.

        if snapshotId is not None:
            snapshotFile = uri + f"/snapshot/snapshot-{snapshotId}"
            snapshotContent = fs.read(snapshotFile)
            jsonSnapshot = json.loads(snapshotContent)
            return PaimonSnapshot.fromJson(jsonSnapshot)
        elif timestampMs in (PaimonTimestamp.EARLIEST, PaimonTimestamp.LATEST):
            snapshotFile = uri + f"/snapshot/{timestampMs.value}"
            # pypaimon doesn't always create snapshot/EARLIEST file.
            # sometimes it is snapshot/timestamp-1.
            if timestampMs is PaimonTimestamp.EARLIEST and not fs.exists(snapshotFile):
                return PaimonSnapshot.__fromTableUri(uri, None, 1, fs)
            snapshotId = int(fs.read(snapshotFile).strip())
            return PaimonSnapshot.__fromTableUri(uri, None, snapshotId, fs)
        if timestampMs is None:
            raise ValueError(f"Invalid paimon timestamp: {timestampMs}.")

        if isinstance(timestampMs.value, int):
            low = 1
            high = PaimonSnapshot.__fromTableUri(
                uri, PaimonTimestamp.LATEST, None, fs=fs
            ).id
            best: Optional[PaimonSnapshot] = None
            while low <= high:
                snapshotId = low + (high - low) // 2
                snapshot = PaimonSnapshot.__fromTableUri(uri, None, snapshotId, fs)
                commitTime = snapshot.timeMs
                if commitTime > timestampMs.value:
                    high = snapshotId - 1
                elif commitTime < timestampMs.value:
                    best = snapshot
                    low = snapshotId + 1
                else:
                    return snapshot
            return (
                best
                if best is not None
                else PaimonSnapshot.__fromTableUri(uri, None, 1, fs)
            )

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
        return predicate_builder.no_equal(predicate.lhs.expr(), rhs)


class PaimonTable(CatalogTable):
    """
    Class representing a paimon table on the filesystem.
    """

    def __init__(self, tableUri: str, options: PaimonOptions = PaimonOptions()):
        self.__uri: str = tableUri
        self.__options = options
        self.__snapshots: dict[PaimonTimestamp, PaimonSnapshot] = {}
        self.__schemas: dict[PaimonTimestamp, PaimonSchema] = {}

    def __paimonSnapshot(
        self,
        startTimestampMs: PaimonTimestamp,
    ) -> PaimonSnapshot:
        """
        Get the snapshot of this table for a specific timestamp.
        """
        try:
            return self.__snapshots[startTimestampMs]
        except KeyError:
            snapshot = PaimonSnapshot.fromTableUri(
                self.__uri,
                startTimestampMs,
            )
            self.__snapshots[startTimestampMs] = snapshot
            return snapshot

    def __paimonSchema(
        self,
        startTimestampMs: PaimonTimestamp,
    ) -> PaimonSchema:
        """
        Get the schema of this table for a specific timestamp.
        """
        try:
            return self.__schemas[startTimestampMs]
        except KeyError:
            schema = PaimonSchema.fromTableUri(
                self.__uri, self.__paimonSnapshot(startTimestampMs).schemaId
            )
            self.__schemas[startTimestampMs] = schema
            return schema

    def __paimonTable(self) -> pypaimon.table.table.Table:
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
        return catalog.get_table(f"{dbName}.{tableName}")

    def getPath(self) -> str:
        return self.__uri

    def getName(self) -> str:
        return self.__uri.split("/")[-1]

    def getPartitionKeys(self) -> list[str]:
        return self.__paimonSchema(self.__options.startTimestampMs).partitionKeys

    def getFileFormat(self) -> FileFormat:
        return hiveFileFormatToBoltFileFormat(
            self.__paimonSchema(self.__options.startTimestampMs).fileFormat
        )

    def getSchema(self) -> RowType:
        return RowType(
            [
                f.name
                for f in self.__paimonSchema(self.__options.startTimestampMs).fields
            ],
            [
                hiveTypeToBoltType(str(f.type))
                for f in self.__paimonSchema(self.__options.startTimestampMs).fields
            ],
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
        schema = self.__paimonSchema(options.startTimestampMs)
        parameters = {
            pypaimon.common.core_options.CoreOptions.FILE_FORMAT: schema.fileFormat,
        }
        if len(schema.primaryKeys) > 0:
            parameters["primary-key"] = ",".join(schema.primaryKeys)
        if options.mergeEngine is not None:
            parameters["merge-engine"] = options.mergeEngine.value
        return parameters

    def getReaderSplits(self, options: PaimonOptions) -> list[CatalogSplit]:
        # See note in getReaderParameters.
        if type(options) is not PaimonOptions:
            raise TypeError(
                f"Invalid option type '{type(options)}' for PaimonTable. "
                "Expected PaimonOptions."
            )

        schema = self.__paimonSchema(options.startTimestampMs)
        fileFormat = hiveFileFormatToBoltFileFormat(schema.fileFormat)
        table = self.__paimonTable()
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
            and options.startTimestampMs is PaimonTimestamp.LATEST
        ):
            scanner = pypaimon.read.scanner.full_starting_scanner.FullStartingScanner(
                table,
                partFilter,
                None,
            )
        else:
            start = self.__paimonSnapshot(options.startTimestampMs).id - 1
            if options.endTimestampMs is None:
                end = start + 1
            else:
                end = self.__paimonSnapshot(options.endTimestampMs).id - 1
            scanner = pypaimon.read.scanner.incremental_starting_scanner.IncrementalStartingScanner(
                table, partFilter, None, start, end
            )

        # The list of filenames returned by pypaimon does not contain the
        # file system path prefix such as: 'hdfs://localhost:9000'.
        # We need it for bolt paimon reader. We collect it from the
        # table uri.
        prefix = urlsplit(self.getPath())._replace(path="", query="", fragment="")
        prefix = urlunsplit(prefix)
        paimonSplits = scanner.scan().splits()

        if len(paimonSplits) == 0:
            return []
        if len(schema.primaryKeys) == 0:
            return [
                PaimonSplit([prefix + f], fileFormat)
                for f in reduce(add, [split.file_paths for split in paimonSplits])
            ]
        else:
            return [
                PaimonSplit([prefix + f for f in split.file_paths], fileFormat)
                for split in paimonSplits
            ]
