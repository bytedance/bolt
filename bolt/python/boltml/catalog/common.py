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

from abc import ABC, abstractmethod
import re
from typing import Union

import pybolt as pb
from pybolt import FileFormat, RowType, BoltType
from pybolt import Split as BoltSplit

from ..dataframe import DataFrame
from ..executor import Executor, LocalExecutor
from ..plan_builder.base import PlanBuilderFactory
from ..plan_builder.substrait import SubstraitPlanBuilderFactory


class CatalogSplit(ABC):
    @abstractmethod
    def toBoltSplit(self) -> BoltSplit: ...


class CatalogTable(ABC):
    """
    Base class for querying a catalog table.
    """

    @abstractmethod
    def getPath(self) -> str:
        """
        The table uri/path on the filesystem.
        """
        ...

    @abstractmethod
    def getName(self) -> str: ...

    @abstractmethod
    def getPartitionKeys(self) -> list[str]: ...

    @abstractmethod
    def getFileFormat(self) -> FileFormat: ...

    @abstractmethod
    def getSchema(self) -> RowType: ...

    @abstractmethod
    def getReaderParameters(
        self,
        options: Union["HiveOptions", "PaimonOptions"],  # noqa: F821
    ) -> dict[str, str]:
        """
        Get the key value dictionary to forward to bolt hive table read plan
        node.
        """
        ...

    @abstractmethod
    def getReaderSplits(
        self,
        options: Union["HiveOptions", "PaimonOptions"],  # noqa: F821
    ) -> list[CatalogSplit]:
        """
        Get a list of CatalogSplit for this table.

        Parameters:
        * options: A set of options specific to the child class, e.g
        `HiveOptions` for `HiveTable`.

        Returns:
        A list of CatalogSplit.
        """
        ...

    def read(
        self,
        options: Union["HiveOptions", "PaimonOptions"],  # noqa: F821
        planFactory: PlanBuilderFactory = SubstraitPlanBuilderFactory(),
        executor: Executor = LocalExecutor(),
    ) -> DataFrame:
        """
        Return a lazy dataframe ready to read this catalog table.

        """
        splits = self.getReaderSplits(options)
        if len(splits) == 0:
            raise ValueError(
                f"CatalogTable '{self.getName()}' read with options ({options}) "
                "yielded 0 splits. Perhaps options are too restrictive?"
            )

        planBuilder = planFactory.fromCatalog(
            self.getName(),
            self.getSchema(),
            self.getPartitionKeys(),
            self.getReaderParameters(options),
            splits,
        )
        return DataFrame(planBuilder, executor, planFactory=planFactory)


def hiveTypeToBoltType(hiveType: str) -> BoltType:
    """
    Convert string hive type from a hive thrift client into a bolt type.
    """

    # The input comes either from hive/thrift and is a string or from
    # paimon and is also a string.
    # If the string encoding or new encodings are added, we may
    # consider refactoring this function into a `Type` with various string
    # parsers.

    # The output is a bolt type forwarded to bolt reader.
    # Not only nullability is not encoded in bolt type system but the
    # reader doesn't use it. Hence, the choice to simply strip "not null"
    # away.
    def _splitTopLevel(value: str) -> list[str]:
        parts = []
        start = 0
        depth = 0
        for i, c in enumerate(value):
            if c in "(<":
                depth += 1
            elif c in ")>":
                depth -= 1
            elif c == "," and depth == 0:
                parts.append(value[start:i])
                start = i + 1
        parts.append(value[start:])
        return parts

    def _splitStructField(field: str) -> tuple[str, str]:
        depth = 0
        for i, c in enumerate(field):
            if c in "(<":
                depth += 1
            elif c in ")>":
                depth -= 1
            elif c == ":" and depth == 0:
                return field[:i].strip("`"), field[i + 1 :]
        raise NotImplementedError(f"Invalid hive struct field type: '{field}'.")

    hiveType = "".join(hiveType.lower().removesuffix(" not null").split())
    for t in (
        pb.BooleanType(),
        pb.IntegerType(),
        pb.BigintType(),
        pb.HugeintType(),
        pb.SmallintType(),
        pb.TinyintType(),
        pb.RealType(),
        pb.DoubleType(),
        pb.TimestampType(),
        pb.VarcharType(),
        pb.VarbinaryType(),
        pb.UnknownType(),
    ):
        boltTypeName = t.name().lower()
        if hiveType == boltTypeName:
            return t
    if hiveType == "int":
        return pb.IntegerType()
    if hiveType == "binary":
        return pb.VarbinaryType()
    if hiveType == "float":
        return pb.RealType()
    if hiveType == "string":
        return pb.VarcharType()
    if hiveType in ("void", "null"):
        return pb.UnknownType()
    match = re.fullmatch(r"decimal\((\d+),(\d+)\)", hiveType)
    if match:
        precision = int(match.group(1))
        scale = int(match.group(2))
        if precision <= 18:
            return pb.ShortDecimalType(precision, scale)
        return pb.LongDecimalType(precision, scale)
    if hiveType.startswith("array<") and hiveType.endswith(">"):
        return pb.ArrayType(hiveTypeToBoltType(hiveType[len("array<") : -1]))
    if hiveType.startswith("map<") and hiveType.endswith(">"):
        fields = _splitTopLevel(hiveType[len("map<") : -1])
        if len(fields) != 2:
            raise NotImplementedError(f"Invalid hive map type: '{hiveType}'.")
        return pb.MapType(hiveTypeToBoltType(fields[0]), hiveTypeToBoltType(fields[1]))
    for rowTypeName in ("struct", "row"):
        prefix = f"{rowTypeName}<"
        if hiveType.startswith(prefix) and hiveType.endswith(">"):
            fields = [
                _splitStructField(field)
                for field in _splitTopLevel(hiveType[len(prefix) : -1])
            ]
            return pb.RowType(
                [name for name, _ in fields],
                [hiveTypeToBoltType(dtype) for _, dtype in fields],
            )
    raise NotImplementedError(
        f"Conversion from hive type '{hiveType}' to bolt type is not supported."
    )


def boltTypeToHiveType(boltType: BoltType) -> str:
    """
    Convert a bolt type into a string hive type to pass to a hive thrift
    client.
    """
    if isinstance(boltType, (pb.ShortDecimalType, pb.LongDecimalType)):
        return f"decimal({boltType.precision()},{boltType.scale()})"
    if isinstance(boltType, pb.BooleanType):
        return "boolean"
    elif isinstance(boltType, pb.TinyintType):
        return "tinyint"
    elif isinstance(boltType, pb.SmallintType):
        return "smallint"
    elif isinstance(boltType, pb.IntegerType):
        return "int"
    elif isinstance(boltType, pb.BigintType):
        return "bigint"
    elif isinstance(boltType, pb.HugeintType):
        return "hugeint"
    elif isinstance(boltType, pb.RealType):
        return "float"
    elif isinstance(boltType, pb.DoubleType):
        return "double"
    elif isinstance(boltType, pb.TimestampType):
        return "timestamp"
    elif isinstance(boltType, pb.VarcharType):
        return "string"
    elif isinstance(boltType, pb.VarbinaryType):
        return "binary"
    elif isinstance(boltType, pb.UnknownType):
        return "void"
    elif isinstance(boltType, pb.ArrayType):
        return f"array<{boltTypeToHiveType(boltType.elementType())}>"
    elif isinstance(boltType, pb.MapType):
        return (
            f"map<{boltTypeToHiveType(boltType.keyType())},"
            f"{boltTypeToHiveType(boltType.valueType())}>"
        )
    elif isinstance(boltType, pb.RowType):
        fields = [
            f"{name}:{boltTypeToHiveType(boltType.childAt(i))}"
            for i, name in enumerate(boltType.names())
        ]
        return f"struct<{','.join(fields)}>"
    raise NotImplementedError(
        f"Conversion from bolt type '{boltType}' to hive type is not supported."
    )


def hiveFileFormatToBoltFileFormat(hiveFileFormat: str) -> FileFormat:
    """
    Convert a string file format obtained from a hive thrift client into a bolt
    file format.
    """
    hiveFileFormat = hiveFileFormat.upper()
    if hiveFileFormat == "JSON":
        return FileFormat.JSON
    if hiveFileFormat == "ORC":
        return FileFormat.ORC
    if hiveFileFormat == "PARQUET":
        return FileFormat.PARQUET
    if hiveFileFormat == "RC":
        return FileFormat.RC
    if hiveFileFormat == "RC_TEXT":
        return FileFormat.RC_TEXT
    if hiveFileFormat == "RC_BINARY":
        return FileFormat.RC_BINARY
    if hiveFileFormat == "TEXT":
        return FileFormat.TEXT
    raise NotImplementedError(
        f"Unsupported hive file format '{hiveFileFormat}' to bolt FileFormat."
    )
