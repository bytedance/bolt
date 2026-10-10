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

"""Validate the installed wheel's contents and generated Hive bindings."""

from importlib import import_module
from importlib.machinery import EXTENSION_SUFFIXES
from importlib.metadata import distribution
import json
from pathlib import Path, PurePosixPath
import unittest


class DistributionTests(unittest.TestCase):
    def setUp(self):
        self.package = distribution("bolt-engine")
        self.assertIsNotNone(self.package.read_text("RECORD"))
        self.files = {str(path): path for path in self.package.files or ()}

    def testImportedModulesMatchInstalledRecord(self):
        directUrl = json.loads(self.package.read_text("direct_url.json") or "{}")
        self.assertFalse(directUrl.get("dir_info", {}).get("editable", False))
        native = [
            name
            for name in self.files
            if name.startswith("pybolt/pybolt.")
            and name.endswith(tuple(EXTENSION_SUFFIXES))
        ]
        self.assertEqual(len(native), 1, native)
        for name, expected in (
            ("pybolt", "pybolt/__init__.py"),
            ("boltml", "boltml/__init__.py"),
            ("pybolt.pybolt", native[0]),
        ):
            with self.subTest(module=name):
                self.assertIn(expected, self.files)
                actual = Path(import_module(name).__file__).resolve()
                installed = self.package.locate_file(self.files[expected]).resolve()
                self.assertEqual(actual, installed)
                self.assertTrue(
                    actual.is_relative_to(self.package.locate_file("").resolve())
                )

    def testInstalledFilesAndNotices(self):
        metadata = [name for name in self.files if name.endswith(".dist-info/METADATA")]
        self.assertEqual(len(metadata), 1, metadata)
        licenses = metadata[0].rsplit("/", 1)[0] + "/licenses"
        required = (
            "pybolt/__init__.py",
            "boltml/__init__.py",
            "boltml/_generated/hive_metastore/ThriftHiveMetastore.py",
            "boltml/_generated/hive_metastore/ttypes.py",
            "boltml/_generated/fb303/FacebookService.py",
            "boltml/third_party/hive/NOTICE.txt",
            f"{licenses}/LICENSE",
            f"{licenses}/NOTICE.txt",
            f"{licenses}/hive/NOTICE.txt",
        )
        for name in required:
            with self.subTest(file=name):
                self.assertIn(name, self.files)
                self.assertGreater(
                    self.package.locate_file(self.files[name]).stat().st_size, 0
                )
        # CI stages extra tests beside the package; only RECORD describes the
        # distribution. Installers may legitimately add bytecode to RECORD.
        for name in self.files:
            path = PurePosixPath(name)
            with self.subTest(file=name):
                self.assertFalse(
                    {"build", "_build", "packaging", "test", "tests"}.intersection(
                        path.parts
                    )
                )
                self.assertNotEqual(path.name, "CMakeLists.txt")
                self.assertNotIn(
                    path.suffix,
                    {".cpp", ".h", ".cmake", ".o", ".a", ".thrift", ".proto"},
                )

    def testGeneratedHiveBindingsRoundTrip(self):
        from boltml._generated.fb303 import FacebookService
        from boltml._generated.hive_metastore import ThriftHiveMetastore
        from boltml._generated.hive_metastore.ttypes import (
            FieldSchema,
            SerDeInfo,
            StorageDescriptor,
            Table,
        )
        from thrift.protocol import TBinaryProtocol
        from thrift.transport import TTransport

        output = TTransport.TMemoryBuffer()
        protocol = TBinaryProtocol.TBinaryProtocol(output)
        self.assertIsInstance(
            ThriftHiveMetastore.Client(protocol), FacebookService.Client
        )
        table = Table(
            tableName="测试_table",
            dbName="integration",
            parameters={"说明": "café 数据"},
            partitionKeys=[FieldSchema("day", "string", "分区")],
            sd=StorageDescriptor(
                cols=[FieldSchema("value", "string", "文本")],
                location="file:///tmp/boltml-integration",
                serdeInfo=SerDeInfo(name="serde", parameters={"encoding": "UTF-8"}),
                parameters={"nested": "保存"},
            ),
        )
        table.write(protocol)
        restored = Table()
        restored.read(
            TBinaryProtocol.TBinaryProtocol(TTransport.TMemoryBuffer(output.getvalue()))
        )
        self.assertEqual(restored, table)


if __name__ == "__main__":
    unittest.main(verbosity=2)
