#!/usr/bin/env python3
#
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

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import os


def ensure_output_dir(dirname):
    os.makedirs(dirname, exist_ok=True)
    return dirname


def create_parquet():
    output_dir = ensure_output_dir("reader")
    num_rows = 400_000_000
    filename = os.path.join(output_dir, "a.parquet")

    pk_a = np.arange(1, num_rows + 1, dtype=np.int32)
    a = pk_a

    _SEQUENCE_NUMBER1 = np.arange(2, 2 * num_rows + 2, 2, dtype=np.int64)
    _VALUE_KIND = np.full(num_rows, 1, dtype=np.int8)

    table = pa.Table.from_arrays(
        [pk_a, a, _SEQUENCE_NUMBER1, _VALUE_KIND],
        names=["pk_a", "a", "_SEQUENCE_NUMBER", "_VALUE_KIND"],
    )

    pq.write_table(table, filename, row_group_size=1_000_000, compression="snappy")
    print(f"Created {filename} with {os.path.getsize(filename) / (1024 * 1024):.2f} MB")

    filename = os.path.join(output_dir, "b.parquet")

    pk_a = np.arange(num_rows / 2 + 1, 3 * num_rows / 2 + 1, dtype=np.int32)
    a = pk_a

    indices = np.arange(len(_SEQUENCE_NUMBER1))
    _SEQUENCE_NUMBER2 = np.where(
        indices % 2 == 0, _SEQUENCE_NUMBER1 + 1, _SEQUENCE_NUMBER1 - 1
    )

    table = pa.Table.from_arrays(
        [pk_a, a, _SEQUENCE_NUMBER2, _VALUE_KIND],
        names=["pk_a", "a", "_SEQUENCE_NUMBER", "_VALUE_KIND"],
    )

    pq.write_table(table, filename, row_group_size=1_000_000, compression="snappy")

    print(f"Created {filename} with {os.path.getsize(filename) / (1024 * 1024):.2f} MB")


def create_partial_update_parquet():
    """
    Generate parquet files for partial update engine benchmark.
    The partial update engine merges rows with the same primary key by
    taking the latest non-null value for each column based on sequence number.

    This creates two files with overlapping primary keys where:
    - File A: pk_a from 1 to num_rows, with values in columns a, b, c
    - File B: pk_a from num_rows/2+1 to num_rows (overlapping), with different values
    """
    output_dir = ensure_output_dir("partial-update")
    num_rows = 100_000_000
    filename = os.path.join(output_dir, "a.parquet")

    # File A: rows 1 to num_rows
    pk_a = np.arange(1, num_rows + 1, dtype=np.int32)
    a = pk_a.astype(np.float64)  # column a: double
    b = np.arange(10, num_rows + 10, dtype=np.int64)  # column b: bigint
    # column c: varchar (string)
    c = np.array([f"str_{i}" for i in range(1, num_rows + 1)], dtype=object)

    _SEQUENCE_NUMBER1 = np.arange(2, 2 * num_rows + 2, 2, dtype=np.int64)
    _VALUE_KIND = np.full(num_rows, 1, dtype=np.int8)

    table = pa.Table.from_arrays(
        [pk_a, a, b, c, _SEQUENCE_NUMBER1, _VALUE_KIND],
        names=["pk_a", "a", "b", "c", "_SEQUENCE_NUMBER", "_VALUE_KIND"],
    )

    pq.write_table(table, filename, row_group_size=1_000_000, compression="snappy")
    print(f"Created {filename} with {os.path.getsize(filename) / (1024 * 1024):.2f} MB")

    # File B: overlapping rows with different values
    # Overlap range: num_rows/2 + 1 to num_rows
    overlap_start = num_rows // 2 + 1
    overlap_end = num_rows
    num_overlap_rows = overlap_end - overlap_start + 1

    filename = os.path.join(output_dir, "b.parquet")

    pk_a = np.arange(overlap_start, overlap_end + 1, dtype=np.int32)
    # Different values for overlapping rows - these should be merged
    a = np.full(num_overlap_rows, 999.99, dtype=np.float64)  # column a: double
    b = np.arange(1000, 1000 + num_overlap_rows, dtype=np.int64)  # column b: bigint
    c = np.array(
        [f"updated_{i}" for i in range(overlap_start, overlap_end + 1)], dtype=object
    )

    # Higher sequence numbers so these values take precedence
    _SEQUENCE_NUMBER2 = np.arange(3, 2 * num_overlap_rows + 3, 2, dtype=np.int64)
    _VALUE_KIND = np.full(num_overlap_rows, 1, dtype=np.int8)

    table = pa.Table.from_arrays(
        [pk_a, a, b, c, _SEQUENCE_NUMBER2, _VALUE_KIND],
        names=["pk_a", "a", "b", "c", "_SEQUENCE_NUMBER", "_VALUE_KIND"],
    )

    pq.write_table(table, filename, row_group_size=1_000_000, compression="snappy")
    print(f"Created {filename} with {os.path.getsize(filename) / (1024 * 1024):.2f} MB")


if __name__ == "__main__":
    create_partial_update_parquet()
    create_parquet()
