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

import hashlib
import importlib.util
import io
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile


SCRIPT = Path(__file__).resolve().parents[1] / "setup_gluten_ci_environment.py"


class GlutenCiEnvironmentTest(unittest.TestCase):
    def setUp(self):
        self.assertTrue(SCRIPT.is_file(), "The Gluten environment setup is missing")
        spec = importlib.util.spec_from_file_location("gluten_environment", SCRIPT)
        self.setup = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.setup)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        zip_data = io.BytesIO()
        with zipfile.ZipFile(zip_data, "w") as archive:
            archive.writestr("test.py", "")
            archive.writestr("pyspark/version.py", "__version__: str = '3.5.5'\n")
        self.binary = {
            "RELEASE": b"Spark 3.5.5 built for Hadoop 3\n",
            "bin/spark-submit": b"#!/bin/sh\n",
            "jars/spark-core_2.12-3.5.5.jar": zip_data.getvalue(),
            "python/lib/pyspark.zip": zip_data.getvalue(),
            "python/lib/py4j-0.10.9.7-src.zip": zip_data.getvalue(),
        }
        self.source = {
            "sql/core/src/test/resources/sql-tests/inputs/example.sql": b"SELECT 1;\n",
            "sql/hive/src/test/resources/data/files/example.txt": b"example\n",
            "core/unused.txt": b"Do not extract unrelated source files\n",
        }

    def archive(self, name, files):
        data = io.BytesIO()
        with tarfile.open(fileobj=data, mode="w:gz") as archive:
            for path, content in files.items():
                member = tarfile.TarInfo(name.removesuffix(".tgz") + "/" + path)
                member.size = len(content)
                member.mode = 0o755 if path.startswith("bin/") else 0o644
                archive.addfile(member, io.BytesIO(content))
        return data.getvalue()

    def downloads(self, binary=None, bad_checksum=False):
        contents = {
            "spark-3.5.5-bin-hadoop3.tgz": self.archive(
                "spark-3.5.5-bin-hadoop3.tgz", self.binary if binary is None else binary
            ),
            "spark-3.5.5.tgz": self.archive("spark-3.5.5.tgz", self.source),
        }

        def download(url, destination):
            name = url.rsplit("/", 1)[1]
            self.assertTrue(url.startswith("https://archive.apache.org/dist/spark/"))
            if name.endswith(".sha512"):
                archive = name.removesuffix(".sha512")
                digest = (
                    "0" * 128
                    if bad_checksum
                    else hashlib.sha512(contents[archive]).hexdigest()
                )
                destination.write_text(f"{digest}  {archive}\n")
            else:
                destination.write_bytes(contents[name])

        return download

    def test_verified_archives_supply_binary_and_matching_source_sql(self):
        with patch.object(self.setup, "download", side_effect=self.downloads()):
            home = self.setup.provision_spark(self.root)
        self.assertEqual(home, self.root / "spark-3.5.5-bin-hadoop3")
        self.assertTrue(os.access(home / "bin/spark-submit", os.X_OK))
        self.assertTrue((home / "sql").is_symlink())
        self.assertEqual((home / "sql").resolve(), self.root / "spark-3.5.5/sql")
        self.assertEqual(
            (
                home / "sql/core/src/test/resources/sql-tests/inputs/example.sql"
            ).read_text(),
            "SELECT 1;\n",
        )
        self.assertFalse((self.root / "spark-3.5.5/core/unused.txt").exists())

    def test_checksum_failure_does_not_publish_a_spark_home(self):
        with patch.object(
            self.setup, "download", side_effect=self.downloads(bad_checksum=True)
        ):
            with self.assertRaisesRegex(ValueError, "SHA512"):
                self.setup.provision_spark(self.root)
        self.assertFalse((self.root / "spark-3.5.5-bin-hadoop3").exists())

    def test_pyspark_distribution_avoids_duplicate_binary_download(self):
        pyspark = self.root / "installed-pyspark"
        for name, data in self.binary.items():
            if name == "RELEASE":
                continue
            path = pyspark / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            if name.startswith("bin/"):
                path.chmod(0o755)
        original_sql = pyspark / "sql/__init__.py"
        original_sql.parent.mkdir()
        original_sql.write_text("# PySpark's Python API must remain intact\n")
        with patch.object(
            self.setup, "download", side_effect=self.downloads()
        ) as download:
            home = self.setup.provision_spark(self.root, pyspark_home=pyspark)
        self.assertTrue(original_sql.is_file())
        self.assertEqual((home / "jars").resolve(), pyspark / "jars")
        self.assertEqual((home / "sql").resolve(), self.root / "spark-3.5.5/sql")
        self.assertFalse((home / "RELEASE").exists())
        self.assertTrue(
            all("-bin-hadoop3" not in call.args[0] for call in download.call_args_list)
        )
        self.setup.validate_spark_home(home)

    def test_wrong_pyspark_zip_version_is_rejected(self):
        with patch.object(self.setup, "download", side_effect=self.downloads()):
            home = self.setup.provision_spark(self.root)
        (home / "RELEASE").unlink()
        with zipfile.ZipFile(home / "python/lib/pyspark.zip", "w") as archive:
            archive.writestr("pyspark/version.py", "__version__ = '4.0.0'\n")
        with self.assertRaisesRegex(ValueError, "Spark 3.5.5"):
            self.setup.validate_spark_home(home)

    def test_incomplete_binary_does_not_replace_a_working_installation(self):
        with patch.object(self.setup, "download", side_effect=self.downloads()):
            home = self.setup.provision_spark(self.root)
        binary = {
            path: data
            for path, data in self.binary.items()
            if path != "jars/spark-core_2.12-3.5.5.jar"
        }
        for archive in self.root.glob("*.tgz"):
            archive.unlink()
        with patch.object(
            self.setup, "download", side_effect=self.downloads(binary=binary)
        ):
            with self.assertRaisesRegex(ValueError, "Spark 3.5.5"):
                self.setup.provision_spark(self.root)
        self.assertIn("Spark 3.5.5", (home / "RELEASE").read_text())

    def test_missing_sql_inputs_are_rejected(self):
        self.source.pop("sql/core/src/test/resources/sql-tests/inputs/example.sql")
        with patch.object(self.setup, "download", side_effect=self.downloads()):
            with self.assertRaisesRegex(ValueError, "SQL"):
                self.setup.provision_spark(self.root)

    def test_corrupted_cached_archive_is_rejected(self):
        with patch.object(self.setup, "download", side_effect=self.downloads()):
            self.setup.provision_spark(self.root)
        (self.root / "spark-3.5.5-bin-hadoop3.tgz").write_bytes(b"corrupt")
        with patch.object(self.setup, "download", side_effect=self.downloads()):
            with self.assertRaisesRegex(ValueError, "SHA512"):
                self.setup.provision_spark(self.root)

    def test_environment_exports_binary_python_zips_and_worker_interpreter(self):
        with patch.object(self.setup, "download", side_effect=self.downloads()):
            home = self.setup.provision_spark(self.root)
        environment_file = self.root / "github-env"
        with patch.dict(
            os.environ,
            {"GITHUB_ENV": str(environment_file), "PYTHONPATH": "/existing/python"},
            clear=True,
        ):
            values = self.setup.export_environment(home)
        exported = dict(
            line.split("=", 1) for line in environment_file.read_text().splitlines()
        )
        self.assertEqual(exported, values)
        self.assertEqual(exported["BOLT_GLUTEN_SPARK_HOME"], str(home))
        self.assertEqual(exported["SPARK_HOME"], str(home))
        self.assertEqual(
            exported["PYTHONPATH"].split(os.pathsep),
            [
                str(home / "python/lib/py4j-0.10.9.7-src.zip"),
                str(home / "python/lib/pyspark.zip"),
                "/existing/python",
            ],
        )
        self.assertTrue(Path(exported["PYSPARK_PYTHON"]).is_file())

    def test_jre_without_headers_is_rejected_before_running_tools(self):
        java_home = self.root / "jre"
        (java_home / "bin").mkdir(parents=True)
        (java_home / "bin/java").write_text("")
        with patch.dict(os.environ, {"JAVA_HOME": str(java_home)}):
            with self.assertRaisesRegex(ValueError, "full JDK 17"):
                self.setup.validate_prerequisites()

    def test_wrong_java_version_is_rejected(self):
        java_home = self.root / "jdk"
        for name in ("bin/java", "bin/javac", "include/jni.h", "lib/libjawt.so"):
            path = java_home / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("")
        with (
            patch.dict(os.environ, {"JAVA_HOME": str(java_home)}),
            patch.object(
                self.setup.subprocess,
                "run",
                return_value=subprocess.CompletedProcess(
                    [], 0, stdout="", stderr='openjdk version "21.0.2"\n'
                ),
            ),
        ):
            with self.assertRaisesRegex(ValueError, "JDK 17"):
                self.setup.validate_prerequisites()


if __name__ == "__main__":
    unittest.main()
