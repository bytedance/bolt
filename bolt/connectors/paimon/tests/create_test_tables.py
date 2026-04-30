#!/usr/bin/env python3
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

from pypaimon import CatalogFactory
from pypaimon.catalog.catalog_exception import (
    TableNotExistException,
)
from typing import Any
import pyarrow as pa
from pypaimon import Schema
from pathlib import Path
import pandas as pd
from argparse import ArgumentParser
import os


def write_to_table(table: Any, data: pd.DataFrame):
    write_builder = table.new_batch_write_builder()
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()
    table_write.write_pandas(data)
    commit_messages = table_write.prepare_commit()
    table_commit.commit(commit_messages)
    table_write.close()
    table_commit.close()


def create_table(
    catalog, database: str, table_name: str, schema: pa.Schema
) -> tuple[bool, Any]:
    """
    Create a table in the catalog if it does not exist

    Returns:
        (bool, table): True if the table was created. False if it was not
        created because it already existed, plus the created table object.
    """
    try:
        table = catalog.get_table(f"{database}.{table_name}")
        return (False, table)
    except TableNotExistException:
        catalog.create_table(
            identifier=f"{database}.{table_name}",
            schema=schema,
            ignore_if_exists=False,
        )
        table = catalog.get_table(f"{database}.{table_name}")
        return (True, table)


def basic_table(catalog):
    pa_schema = pa.schema(
        [
            ("id", pa.int64()),
        ]
    )
    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=[],
        primary_keys=[],
        options={"bucket": "2"},
        comment="my test table",
    )
    data = {
        "id": [1, 2, 3],
    }
    dataframe = pd.DataFrame(data)
    (table_created, table) = create_table(
        catalog=catalog, database="test_db", table_name="basic", schema=schema
    )
    if not table_created:
        return
    write_to_table(table, dataframe)


def append_only_multiple_append(catalog):
    pa_schema = pa.schema(
        [
            ("id", pa.int64()),
        ]
    )
    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=[],
        primary_keys=[],
        options={"bucket": "2"},
        comment="my test table",
    )
    data_1 = {
        "id": [4, 5, 6],
    }
    data_2 = {
        "id": [7, 8, 9],
    }
    dataframe_1 = pd.DataFrame(data_1)
    dataframe_2 = pd.DataFrame(data_2)
    (table_created, table) = create_table(
        catalog=catalog,
        database="test_db",
        table_name="append_only_multiple_append",
        schema=schema,
    )
    if not table_created:
        return

    # write dataset 1
    write_to_table(table, dataframe_1)

    # write dataset 2
    write_to_table(table, dataframe_2)


def pk_no_overwrite(catalog):
    pa_schema = pa.schema(
        [
            ("id", pa.int64()),
        ]
    )
    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=[],
        primary_keys=["id"],
        options={"bucket": "2"},
        comment="my test table",
    )
    data_1 = {
        "id": [10, 11, 12],
    }
    data_2 = {
        "id": [13, 14, 15],
    }
    dataframe_1 = pd.DataFrame(data_1)
    dataframe_2 = pd.DataFrame(data_2)
    (table_created, table) = create_table(
        catalog=catalog, database="test_db", table_name="pk_no_overwrite", schema=schema
    )
    if not table_created:
        return

    # write dataset 1
    write_to_table(table, dataframe_1)

    # write dataset 2
    write_to_table(table, dataframe_2)


def pk_with_overwrite(catalog):
    pa_schema = pa.schema(
        [
            ("id", pa.int64()),
            ("value", pa.int64()),
        ]
    )
    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=[],
        primary_keys=["id"],
        options={"bucket": "2"},
        comment="my test table",
    )
    data_1 = {
        "id": [x for x in range(10)],
        "value": [2 * x for x in range(10)],
    }
    data_2 = {
        "id": [x for x in range(5, 15)],  # overlaps on [5, 9]
        "value": [3 * x for x in range(5, 15)],  # [15-42]
    }
    # resulting table should be
    # id:    0 1 2 3 4 5  6  7  8  9  10 11 12 13 14
    # value: 0 2 4 6 8 15 18 21 24 27 30 33 36 39 42
    dataframe_1 = pd.DataFrame(data_1)
    dataframe_2 = pd.DataFrame(data_2)
    (table_created, table) = create_table(
        catalog=catalog,
        database="test_db",
        table_name="pk_with_overwrite",
        schema=schema,
    )
    if not table_created:
        return

    # write dataset 1
    write_to_table(table, dataframe_1)

    # write dataset 2
    write_to_table(table, dataframe_2)


def data_evolution_table(catalog):
    pa_schema = pa.schema(
        [
            ("id", pa.int64()),
            ("value", pa.string()),
            ("length", pa.int32()),
        ]
    )
    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=[],
        primary_keys=[],
        options={
            "row-tracking.enabled": "true",
            "data-evolution.enabled": "true",
        },
        comment="data evolution test table",
    )
    data_1 = {
        "id": [1, 2, 3],
        "value": ["apple", "banana", "cherry"],
        "length": [None, None, None],
    }
    dataframe_1 = pd.DataFrame(data_1)
    (table_created, table) = create_table(
        catalog=catalog, database="test_db", table_name="data_evolution", schema=schema
    )
    if not table_created:
        return

    # write initial dataset
    write_to_table(table, dataframe_1)

    write_builder = table.new_batch_write_builder()
    batch_write = write_builder.new_write()
    table_update = write_builder.new_update().with_update_type(["length"])
    table_commit = write_builder.new_commit()
    data2 = pa.Table.from_pydict(
        {
            "_ROW_ID": [0, 1, 2],
            "length": [5, 6, 6],
        },
        schema=pa.schema(
            [
                ("_ROW_ID", pa.int64()),
                ("length", pa.int32()),
            ]
        ),
    )
    cmts = table_update.update_by_arrow_with_row_id(data2)
    table_commit.commit(cmts)
    table_commit.close()
    batch_write.close()


def partial_update_table(catalog):
    pa_schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("age", pa.int32()),
            ("salary", pa.float64()),
        ]
    )
    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=[],
        primary_keys=["id"],
        options={"merge-engine": "partial-update", "bucket": "1"},
        comment="partial update merge engine test table",
    )
    data_1 = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [30, 35, 40],
        "salary": [50000.0, 60000.0, 70000.0],
    }
    dataframe_1 = pd.DataFrame(data_1)
    (table_created, table) = create_table(
        catalog=catalog, database="test_db", table_name="partial_update", schema=schema
    )
    if not table_created:
        return

    write_to_table(table, dataframe_1)

    data_2 = {
        "id": [1, 3],
        "name": [None, None],
        "age": [None, None],
        "salary": [55000.0, 75000.0],
    }

    write_to_table(table, pd.DataFrame(data_2))

    # read table
    read_builder = table.new_read_builder()
    result = read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())
    print(result)


def aggregate_table(catalog):
    pa_schema = pa.schema(
        [
            ("id", pa.int64()),
            ("sales", pa.int64()),
            ("price", pa.float64()),
        ]
    )
    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=[],
        primary_keys=["id"],
        options={
            "merge-engine": "aggregation",
            "bucket": "1",
            "fields.price.aggregate-function": "max",
            "fields.sales.aggregate-function": "sum",
        },
        comment="aggregate merge engine test table",
    )
    data_1 = {
        "id": [1, 2, 3],
        "sales": [2, 3, 1],
        "price": [10.0, 20.0, 15.0],
    }
    dataframe_1 = pd.DataFrame(data_1)
    (table_created, table) = create_table(
        catalog=catalog, database="test_db", table_name="aggregate", schema=schema
    )
    if not table_created:
        return

    write_to_table(table, dataframe_1)

    data_2 = {
        "id": [1, 3],
        "sales": [1, 2],
        "price": [15.0, 25.0],
    }
    dataframe_2 = pd.DataFrame(data_2)
    write_to_table(table, dataframe_2)


def deduplicate_table(catalog):
    pa_schema = pa.schema(
        [
            ("id", pa.int64()),
            ("value", pa.string()),
            ("timestamp", pa.int64()),
        ]
    )
    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=[],
        primary_keys=["id"],
        options={
            "merge-engine": "deduplicate",
            "bucket": "2",
        },
        comment="deduplicate merge engine test table",
    )
    data_1 = {
        "id": [1, 2, 3],
        "value": ["v1", "v2", "v3"],
        "timestamp": [1000, 2000, 3000],
    }
    dataframe_1 = pd.DataFrame(data_1)
    (table_created, table) = create_table(
        catalog=catalog, database="test_db", table_name="deduplicate", schema=schema
    )
    if not table_created:
        return

    write_to_table(table, dataframe_1)

    data_2 = {
        "id": [1, 3],
        "value": ["v1", "v3_updated"],
        "timestamp": [2500, 3500],
    }
    dataframe_2 = pd.DataFrame(data_2)
    write_to_table(table, dataframe_2)

    # data 3
    data_3 = {
        "id": [2, 4],
        "value": ["v2_updated", "v4_updated"],
        "timestamp": [2500, 4500],
    }
    dataframe_3 = pd.DataFrame(data_3)
    write_to_table(table, dataframe_3)


def timestamp_precision_table(catalog):
    """Table with high-precision timestamps for testing kReadTimestampUnit."""
    pa_schema = pa.schema(
        [
            ("id", pa.int64()),
            ("ts", pa.timestamp("ns")),
            ("value", pa.int64()),
        ]
    )
    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=[],
        primary_keys=[],
        options={"bucket": "1"},
        comment="timestamp precision test table",
    )
    # Write timestamps with nanosecond precision so we can observe
    # truncation at milli/micro/nano levels.
    data = {
        "id": [1, 2, 3, 4, 5],
        "ts": [
            # Nanosecond-precision timestamps
            pd.Timestamp("2015-06-01 19:34:56.123456789"),
            pd.Timestamp("2023-04-21 09:09:34.567890123"),
            pd.Timestamp("2007-12-12 04:27:56.999999111"),
            pd.Timestamp("2000-01-01 00:00:00.000001000"),
            pd.Timestamp("1999-12-31 23:59:59.999999000"),
        ],
        "value": [10, 20, 30, 40, 50],
    }
    dataframe = pd.DataFrame(data)
    (table_created, table) = create_table(
        catalog=catalog,
        database="test_db",
        table_name="timestamp_precision",
        schema=schema,
    )
    if not table_created:
        return
    write_to_table(table, dataframe)


def main():
    parser = ArgumentParser()
    parser.add_argument("-b", "--base-path", default=None)
    args = parser.parse_args()
    if not args.base_path:
        args.base_path = str(Path(__file__).parent / "test_warehouse")
        # remove base path if already exists
        os.system(f"rm -rf {args.base_path}")
        print("deleted existing warehouse directory")

    base_path = args.base_path
    print(f"warehouse base path: {base_path}")
    catalog_options = {"warehouse": f"file://{base_path}"}
    catalog = CatalogFactory.create(catalog_options)

    catalog.create_database(
        name="test_db",
        ignore_if_exists=True,
    )

    tables = [
        basic_table,
        append_only_multiple_append,
        pk_no_overwrite,
        pk_with_overwrite,
        data_evolution_table,
        partial_update_table,
        aggregate_table,
        deduplicate_table,
        timestamp_precision_table,
    ]
    for create_table in tables:
        create_table(catalog)


if __name__ == "__main__":
    main()
