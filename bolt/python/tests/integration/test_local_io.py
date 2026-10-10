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

"""Local file integration checks using the installed BoltML package."""

from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.orc as orc
import pyarrow.parquet as pq
import pybolt

from boltml import DataFrame
from boltml.executor import LocalExecutor
from boltml.reader import LocalTableReader
from boltml.writer import LocalTableWriter


class RecordingLocalExecutor(LocalExecutor):
    """Capture the native writer result, including requested column statistics."""

    def eval(self, plan):
        self.lastResult = super().eval(plan)
        return self.lastResult


class LocalIoTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="boltml-io-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.data = {"id": [1, 2, 3], "name": ["alice", None, "carol"]}
        self.schema = pybolt.RowType(
            ["id", "name"], [pybolt.BigintType(), pybolt.VarcharType()]
        )

    def assertReadData(self, path, fileFormat):
        frame = LocalTableReader(path, self.schema, fileFormat).read()
        self.assertEqual(frame.toArrow(pa.Table).to_pydict(), self.data)

    def dataFiles(self, directory):
        files = sorted(path for path in directory.rglob("*") if path.is_file())
        self.assertTrue(files, f"No files written in {directory}")
        return files

    def testParquetReaderPathForms(self):
        path = self.directory / "source.parquet"
        pq.write_table(pa.table(self.data), path)
        for inputPath in (str(path), f"file:{path}", path.as_uri()):
            with self.subTest(path=inputPath):
                self.assertReadData(inputPath, pybolt.FileFormat.PARQUET)

    def testParquetWriterRoundTrip(self):
        source = DataFrame(self.data)
        for scheme in ("plain", "file"):
            with self.subTest(scheme=scheme):
                output = self.directory / scheme
                outputPath = str(output) if scheme == "plain" else output.as_uri()
                LocalTableWriter(outputPath).write(source.copy())
                files = self.dataFiles(output)
                self.assertEqual(len(files), 1)
                self.assertEqual(pq.ParquetFile(files[0]).read().to_pydict(), self.data)
                self.assertReadData(str(files[0]), pybolt.FileFormat.PARQUET)
        self.assertEqual(source.toArrow(pa.Table).to_pydict(), self.data)

    def testOrcReader(self):
        path = self.directory / "source.orc"
        orc.write_table(pa.table(self.data), path)
        self.assertReadData(str(path), pybolt.FileFormat.ORC)

    def testOrcWriterRoundTrip(self):
        output = self.directory / "orc"
        LocalTableWriter(str(output), fileFormat=pybolt.FileFormat.ORC).write(
            DataFrame(self.data)
        )
        files = self.dataFiles(output)
        self.assertEqual(len(files), 1)
        self.assertEqual(orc.ORCFile(files[0]).read().to_pydict(), self.data)
        self.assertReadData(f"file:{files[0]}", pybolt.FileFormat.ORC)

    def testPartitionByKeepsRowsInTheirDirectories(self):
        source = {"id": [1, 2, 3, 4], "ds": ["2026-10-08", "2026-10-09"] * 2}
        output = self.directory / "partitioned"
        LocalTableWriter(str(output), partitionBy=["ds"]).write(DataFrame(source))
        files = self.dataFiles(output)
        self.assertEqual(
            {path.parent.name for path in files}, {"ds=2026-10-08", "ds=2026-10-09"}
        )
        actual = []
        for path in files:
            data = pq.ParquetFile(path).read().to_pydict()
            self.assertEqual(list(data), ["id"])
            actual.extend((value, path.parent.name[3:]) for value in data["id"])
        self.assertEqual(sorted(actual), sorted(zip(source["id"], source["ds"])))

    def testBucketByCreatesDistinctFilesWithoutLosingRows(self):
        output = self.directory / "bucketed"
        LocalTableWriter(str(output), bucketBy=(["id"], 4)).write(
            DataFrame({"id": list(range(12))})
        )
        files = self.dataFiles(output)
        self.assertEqual(len(files), 4)
        actual = []
        for path in files:
            values = pq.ParquetFile(path).read().column("id").to_pylist()
            self.assertTrue(values)
            self.assertEqual(len({value % 4 for value in values}), 1)
            actual.extend(values)
        self.assertEqual(sorted(actual), list(range(12)))

    # The Substrait table writer currently drops requested column statistics.
    @unittest.expectedFailure
    def testWriterAggregationsProduceRequestedStatistics(self):
        executor = RecordingLocalExecutor()
        output = self.directory / "statistics"
        LocalTableWriter(str(output), aggregations=["max(id) AS max_id"]).write(
            DataFrame(self.data, executor=executor)
        )
        self.assertEqual(len(self.dataFiles(output)), 1)
        self.assertIn("max_id", executor.lastResult.dtype().names())
        self.assertIn(3, list(executor.lastResult.childAt("max_id")))


if __name__ == "__main__":
    unittest.main(verbosity=2)
