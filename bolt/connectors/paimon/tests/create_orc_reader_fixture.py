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

"""Write and independently verify the ORC negative timestamp reader fixture."""

import argparse

import pyarrow as pa
import pyarrow.orc as orc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output")
    args = parser.parse_args()
    print(f"Generating ORC timestamp fixture with PyArrow {pa.__version__}", flush=True)
    table = pa.table(
        {
            "ts": pa.array(
                [-1876544, -1000001, -1, None, 0, 1123456], type=pa.timestamp("us")
            )
        }
    )
    for path, schema in (
        (args.output, table.schema),
        (args.output + ".utc.orc", pa.schema([("ts", pa.timestamp("us", "UTC"))])),
    ):
        expected = table.cast(schema)
        orc.write_table(expected, path)
        actual = orc.read_table(path).cast(schema)
        if not actual.equals(expected):
            raise RuntimeError(
                f"PyArrow {pa.__version__} cannot round-trip {schema}: "
                f"expected {expected}, actual {actual}"
            )


if __name__ == "__main__":
    main()
