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

from enum import Enum
from typing import Optional, Union

from .common import CatalogTable
from .hive import HiveOptions, HiveTable

# Keep Hive catalog imports usable when the Paimon client is unavailable.
try:
    from .paimon import PaimonOptions, PaimonTable
except ModuleNotFoundError:
    PaimonOptions = PaimonTable = None
from .._generated.hive_metastore.ThriftHiveMetastore import Client as ThriftClient
from .._generated.hive_metastore.ttypes import Table as ThriftTable


class TableKind(Enum):
    """
    The kind of table queried from a catalog
    """

    HIVE = 1
    PAIMON = 2

    @staticmethod
    def fromThriftTable(thriftTable: ThriftTable) -> "Self":  # noqa: F821
        """
        Infer table kind from a thrift table.
        """
        if (
            "PaimonOutputFormat" in thriftTable.sd.inputFormat
            or "PaimonOutputFormat" in thriftTable.sd.outputFormat
        ):
            return TableKind.PAIMON
        if "paimon_location" in thriftTable.parameters:
            return TableKind.PAIMON
        if (
            thriftTable.sd.serdeInfo.serializationLib is not None
            and "PaimonStorageHandler" in thriftTable.sd.serdeInfo.serializationLib
        ):
            return TableKind.PAIMON

        return TableKind.HIVE

    @property
    def defaultOptions(self) -> Union[HiveOptions, PaimonOptions]:
        if self == TableKind.PAIMON:
            return PaimonOptions()
        if self == TableKind.HIVE:
            return HiveOptions()
        raise NotImplementedError(f"No default options for table kind: {self}")


class HiveCatalog:
    """
    Hive table reader, reading from a catalog.
    """

    def __init__(self, client: ThriftClient):
        """
        Create a HiveCatalog client for a HiveClient.
        """
        self.__client = client

    def getTable(
        self,
        tableName: str,
        dbName: str,
        options: Optional[Union[HiveOptions, PaimonOptions]] = None,
    ) -> CatalogTable:
        thriftTable = self.__client.get_table(dbName, tableName)

        kind = TableKind.fromThriftTable(thriftTable)
        defaultOptions = kind.defaultOptions
        if options is None:
            options = defaultOptions
        elif not isinstance(options, defaultOptions.__class__):
            raise ValueError(
                f"Table kind ({kind}) and option type ({options.__class__}) mismatch."
            )
        if kind == TableKind.HIVE:
            return HiveTable(self.__client, thriftTable)
        elif kind == TableKind.PAIMON:
            return PaimonTable(thriftTable.sd.location, options)
        else:
            raise ValueError(f"Unsupported table kind value {kind}.")
