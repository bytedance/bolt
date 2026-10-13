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

from dataclasses import dataclass
import posixpath
from typing import Optional
from urllib.parse import urlsplit

from pybolt import FileFormat, RowType
from pybolt import Split as BoltSplit

from ..details import FileUtils
from .common import (
    CatalogTable,
    hiveFileFormatToBoltFileFormat,
    hiveTypeToBoltType,
    CatalogSplit,
)
from .._generated.hive_metastore.ThriftHiveMetastore import Client as ThriftClient
from .._generated.hive_metastore.ttypes import Table as ThriftTable
from .._generated.hive_metastore.ttypes import Partition


@dataclass
class HiveOptions:
    """
    Set of options to collect hive table splits.
    """

    partitionFilterExpr: Optional[str] = None
    maxPartitions: int = -1


@dataclass
class HiveSplit(CatalogSplit):
    """
    Class representing a split of a Hive table.
    A Hive split corresponds to a single file in a specific partition.
    """

    filePath: str
    fileFormat: FileFormat
    partMap: dict[str, Optional[str]]

    def toBoltSplit(self) -> BoltSplit:
        return BoltSplit.hive(self.filePath, self.fileFormat, self.partMap)


class HiveTable(CatalogTable):
    """
    Class representing a table in a hive catalog.
    """

    def __init__(self, client: ThriftClient, table: ThriftTable):
        self.__client = client
        self.__thriftTable = table

    def getPath(self) -> str:
        return self.__thriftTable.sd.location

    def getName(self) -> str:
        return self.__thriftTable.tableName

    def getPartitionKeys(self) -> list[str]:
        return [pk.name for pk in (self.__thriftTable.partitionKeys or [])]

    def getFileFormat(self) -> FileFormat:
        return hiveFileFormatToBoltFileFormat(self.__thriftTable.sd.outputFormat)

    def getSchema(self) -> RowType:
        fieldNames = []
        fieldTypes = []
        for field in (self.__thriftTable.sd.cols or []) + (
            self.__thriftTable.partitionKeys or []
        ):
            if field.name in fieldNames:
                continue
            fieldNames.append(field.name)
            fieldTypes.append(hiveTypeToBoltType(field.type))
        return RowType(fieldNames, fieldTypes)

    def getReaderParameters(self, options: HiveOptions) -> dict:
        if type(options) is not HiveOptions:
            raise TypeError(
                f"Invalid option type '{type(options)}' for HiveTable. "
                "Expected HiveOptions."
            )
        return self.__thriftTable.parameters or {}

    def getReaderSplits(self, options: HiveOptions) -> list[CatalogSplit]:
        if type(options) is not HiveOptions:
            raise TypeError(
                f"Invalid option type '{type(options)}' for HiveTable. "
                "Expected HiveOptions."
            )
        partitionKeys = self.getPartitionKeys()
        if not partitionKeys:
            partitions = [Partition(values=[], sd=self.__thriftTable.sd)]
        elif options.partitionFilterExpr is None:
            partitions = self.__client.get_partitions(
                self.__thriftTable.dbName,
                self.__thriftTable.tableName,
                options.maxPartitions,
            )
        else:
            partitions = self.__client.get_partitions_by_filter(
                db_name=self.__thriftTable.dbName,
                tbl_name=self.__thriftTable.tableName,
                filter=options.partitionFilterExpr,
                max_parts=options.maxPartitions,
            )

        splits: list[CatalogSplit] = []
        for p in partitions:
            if len(partitionKeys) != len(p.values):
                raise ValueError(
                    "Hive partition values do not match the partition keys"
                )
            partMap = {
                k: None if v == "__HIVE_DEFAULT_PARTITION__" else v
                for k, v in zip(partitionKeys, p.values)
            }
            fileFormat = hiveFileFormatToBoltFileFormat(
                p.sd.outputFormat or self.__thriftTable.sd.outputFormat
            )
            filenames = FileUtils.listFiles(p.sd.location)
            # TODO: support bucketed splits with multiple files.
            for f in filenames:
                relativePath = posixpath.relpath(
                    urlsplit(f).path, urlsplit(p.sd.location).path
                )
                if any(part.startswith((".", "_")) for part in relativePath.split("/")):
                    continue
                splits.append(HiveSplit(f, fileFormat, partMap))
        return splits
