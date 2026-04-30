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

import argparse
import numpy as np
import pandas as pd
import pyarrow as pa
from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.catalog_exception import TableNotExistException
from pathlib import Path

# Constants
# Keep partitions and buckets constant so file count stays constant across SFs.
PARTITIONS = 10
BUCKETS = 4
# SF1 ≈ 1GB uncompressed data.
# Assuming roughly 100-200 bytes per row.
# 10^9 / 100 = 10,000,000 rows.
ROWS_PER_SCALE_UNIT = 10_000_000


def get_catalog(warehouse_path: Path):
    catalog_options = {"warehouse": f"file://{warehouse_path.absolute()}"}
    return CatalogFactory.create(catalog_options)


def create_database(catalog, database_name):
    try:
        catalog.create_database(
            name=database_name,
            ignore_if_exists=True,
        )
        print(f"Created database {database_name}.")
    except Exception as e:
        print(f"Failed to create database {database_name}: {e}")


def create_table_if_not_exists(catalog, database, table_name, schema):
    try:
        table = catalog.get_table(f"{database}.{table_name}")
        print(f"Table {database}.{table_name} already exists.")
        return False, table
    except TableNotExistException:
        catalog.create_table(
            identifier=f"{database}.{table_name}",
            schema=schema,
            ignore_if_exists=False,
        )
        print(f"Created table {database}.{table_name}.")
        return True, catalog.get_table(f"{database}.{table_name}")


def write_data(table, df):
    print("Writing dataframe with shape:", df.shape)
    print("Dtypes:\n", df.dtypes)
    print("Head:\n", df.head())

    write_builder = table.new_batch_write_builder()
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()

    table_write.write_pandas(df)

    commit_messages = table_write.prepare_commit()
    table_commit.commit(commit_messages)
    table_write.close()
    table_commit.close()


def generate_data_chunks(scale_factor, partitions):
    total_rows = int(ROWS_PER_SCALE_UNIT * scale_factor)
    rows_per_partition = total_rows // partitions

    print(f"Generating ~{total_rows} rows across {partitions} partitions...")

    # Process one partition at a time to save memory
    for pt in range(partitions):
        # Generate full partition data
        ids = np.arange(rows_per_partition) + (pt * rows_per_partition)
        # Use simple string generation
        # 'val_' + id
        # We can use numpy char ops
        # key_col: unique per row
        keys = np.char.add("key_", ids.astype(str))
        # val_col: random-ish, padded to achieve ~100 bytes per row total
        # Prefix length 60 + suffix
        vals = np.char.add(
            "val_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx_",
            (ids % 1000).astype(str),
        )
        nums = np.random.randint(0, 10000, size=rows_per_partition)
        # Use string for partition column as pypaimon/paimon often prefers string partitions
        pts = np.full(rows_per_partition, str(pt))

        df = pd.DataFrame(
            {"pt": pts, "id": ids, "key_col": keys, "val_col": vals, "num_col": nums}
        )

        yield df


def create_append_only(catalog, db, scale_factor):
    name = "append_only"
    pa_schema = pa.schema(
        [
            ("pt", pa.string()),
            ("id", pa.int64()),
            ("key_col", pa.string()),
            ("val_col", pa.string()),
            ("num_col", pa.int32()),
        ]
    )

    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=["pt"],
        primary_keys=[],  # No PK for append-only
        options={
            "bucket": str(BUCKETS),
            "bucket-key": "id",  # Bucket by ID
            "write-mode": "append-only",
        },
        comment="Append-only benchmark table",
    )

    created, table = create_table_if_not_exists(catalog, db, name, schema)
    if created:
        for df in generate_data_chunks(scale_factor, PARTITIONS):
            write_data(table, df)


def create_aggregate(catalog, db, scale_factor):
    name = "aggregate"
    pa_schema = pa.schema(
        [
            ("pt", pa.string()),
            ("id", pa.int64()),
            ("key_col", pa.string()),
            ("val_col", pa.string()),
            ("num_col", pa.int32()),
        ]
    )

    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=["pt"],
        primary_keys=["pt", "id"],
        options={
            "bucket": str(BUCKETS),
            "merge-engine": "aggregation",
            "fields.num_col.aggregate-function": "sum",
            "fields.val_col.aggregate-function": "last_non_null_value",
            "fields.key_col.aggregate-function": "last_non_null_value",
        },
        comment="Aggregate benchmark table",
    )

    created, table = create_table_if_not_exists(catalog, db, name, schema)
    if created:
        for df in generate_data_chunks(scale_factor, PARTITIONS):
            write_data(table, df)


def create_partial_update(catalog, db, scale_factor):
    name = "partial_update"
    pa_schema = pa.schema(
        [
            ("pt", pa.string()),
            ("id", pa.int64()),
            ("key_col", pa.string()),
            ("val_col", pa.string()),
            ("num_col", pa.int32()),
        ]
    )

    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=["pt"],
        primary_keys=["pt", "id"],
        options={"bucket": str(BUCKETS), "merge-engine": "partial-update"},
        comment="Partial-update benchmark table",
    )

    created, table = create_table_if_not_exists(catalog, db, name, schema)
    if created:
        for df in generate_data_chunks(scale_factor, PARTITIONS):
            write_data(table, df)


def create_deduplicate(catalog, db, scale_factor):
    name = "deduplicate"
    pa_schema = pa.schema(
        [
            ("pt", pa.string()),
            ("id", pa.int64()),
            ("key_col", pa.string()),
            ("val_col", pa.string()),
            ("num_col", pa.int32()),
        ]
    )

    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=["pt"],
        primary_keys=["pt", "id"],
        options={"bucket": str(BUCKETS), "merge-engine": "deduplicate"},
        comment="Deduplicate benchmark table",
    )

    created, table = create_table_if_not_exists(catalog, db, name, schema)
    if created:
        for df in generate_data_chunks(scale_factor, PARTITIONS):
            write_data(table, df)


def _sf_to_suffix(sf: float) -> str:
    # Produce compact, filesystem-friendly suffix for the scale factor.
    if float(int(sf)) == float(sf):
        return str(int(sf))
    # Replace dot with underscore for decimals, trim trailing zeros
    s = ("%g" % sf).replace(".", "_")
    return s


def main():
    parser = argparse.ArgumentParser(description="Create Paimon benchmark tables")
    parser.add_argument(
        "--scale-factor", type=float, default=1.0, help="Scale factor (1.0 ~= 1GB)"
    )
    parser.add_argument("--base-path", required=True, help="Warehouse base path")
    args = parser.parse_args()

    print(f"Warehouse path: {args.base_path}")
    print(f"Scale factor: {args.scale_factor}")

    path = Path(args.base_path)
    catalog = get_catalog(path)
    db_name = f"benchmark_db_sf{_sf_to_suffix(args.scale_factor)}"
    create_database(catalog, db_name)

    # Tables to create
    creators = [
        create_append_only,
        create_aggregate,
        create_partial_update,
        create_deduplicate,
    ]

    for creator in creators:
        creator(catalog, db_name, args.scale_factor)


if __name__ == "__main__":
    main()
