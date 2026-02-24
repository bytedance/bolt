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


def create_parquet():
    num_rows = 400_000_000
    filename = "a.parquet"

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

    filename = "b.parquet"

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


if __name__ == "__main__":
    create_parquet()
