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

"""Provision Gluten's Spark test runtime inside the existing Bolt CI image."""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import posixpath
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from urllib.request import urlopen
import zipfile


ROOT = Path(__file__).resolve().parents[2]
SPARK_VERSION = "3.5.5"
APACHE_SPARK = f"https://archive.apache.org/dist/spark/spark-{SPARK_VERSION}"
BINARY = f"spark-{SPARK_VERSION}-bin-hadoop3"
SOURCE = f"spark-{SPARK_VERSION}"
PYTHON_PACKAGES = {
    "setuptools": "77.0.3",
    "Cython": "3.3.0",
    # Spark 3.5 still uses NumPy aliases removed in NumPy 2.
    "numpy": "1.26.4",
    "pyspark": SPARK_VERSION,
    "pandas": "2.2.3",
    "pyarrow": "20.0.0",
}


def validate_prerequisites():
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise ValueError("Gluten CI requires Linux x86_64")
    if sys.version_info < (3, 10):
        raise ValueError("Gluten CI requires Python 3.10 or newer")
    home = Path(os.environ.get("JAVA_HOME", ""))
    if not os.environ.get("JAVA_HOME") or any(
        not (home / name).is_file()
        for name in ("bin/java", "bin/javac", "include/jni.h", "lib/libjawt.so")
    ):
        raise ValueError("Set JAVA_HOME to a full JDK 17 with JNI headers and AWT")
    java = subprocess.run(
        [str(home / "bin/java"), "-version"],
        capture_output=True,
        text=True,
        check=True,
    )
    if not re.search(r'version "17[.\"]', java.stdout + java.stderr):
        raise ValueError("Gluten CI requires JDK 17")
    subprocess.run([str(home / "bin/javac"), "-version"], check=True)


def install_python_packages():
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            *[f"{name}=={version}" for name, version in PYTHON_PACKAGES.items()],
        ],
        check=True,
    )
    # Check imports as well as metadata: a wheel with missing native runtime
    # dependencies is unusable even if pip installed the requested version.
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import importlib.metadata as metadata, json, sys; "
            "import setuptools, Cython, numpy, pyspark, pandas, pyarrow; "
            "from distutils.version import LooseVersion; "
            "expected = json.loads(sys.argv[1]); "
            "actual = {name: metadata.version(name) for name in expected}; "
            "assert actual == expected, (actual, expected)",
            json.dumps(PYTHON_PACKAGES),
        ],
        check=True,
    )


def download(url, destination):
    temporary = destination.with_name(destination.name + ".part")
    print(f"Downloading {url}", flush=True)
    try:
        with urlopen(url, timeout=120) as response, temporary.open("wb") as output:
            shutil.copyfileobj(response, output, length=1024 * 1024)
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


def checked_archive(directory, name):
    archive = directory / f"{name}.tgz"
    checksum = directory / f"{archive.name}.sha512"
    download(f"{APACHE_SPARK}/{checksum.name}", checksum)
    match = re.fullmatch(
        rf"([0-9a-fA-F]{{128}})\s+\*?{re.escape(archive.name)}\s*",
        checksum.read_text(),
    )
    if not match:
        raise ValueError(f"Invalid Apache SHA512 manifest for {archive.name}")
    if not archive.is_file():
        download(f"{APACHE_SPARK}/{archive.name}", archive)
    digest = hashlib.sha512()
    with archive.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != match[1].lower():
        raise ValueError(f"SHA512 verification failed for {archive.name}")
    return archive


def extract_archive(archive, directory, name, sql_only=False):
    with tarfile.open(archive, "r:gz") as contents:
        members = []
        for member in contents.getmembers():
            path = PurePosixPath(member.name)
            if (
                path.is_absolute()
                or ".." in path.parts
                or not path.parts
                or path.parts[0] != name
            ):
                raise ValueError(f"Unexpected Spark archive path: {member.name}")
            if sql_only and (len(path.parts) < 2 or path.parts[1] != "sql"):
                continue
            if member.issym() or member.islnk():
                base = str(path.parent) if member.issym() else ""
                target = PurePosixPath(
                    posixpath.normpath(posixpath.join(base, member.linkname))
                )
                if target.is_absolute() or not target.parts or target.parts[0] != name:
                    raise ValueError(f"Unexpected Spark archive link: {member.name}")
            elif not (member.isfile() or member.isdir()):
                raise ValueError(f"Unexpected Spark archive member: {member.name}")
            members.append(member)
        contents.extractall(directory, members=members)


def validate_spark_home(home):
    release = home / "RELEASE"
    if release.is_file():
        version_matches = bool(re.search(r"\bSpark 3\.5\.5\b", release.read_text()))
    else:
        try:
            with zipfile.ZipFile(home / "python/lib/pyspark.zip") as archive:
                version_matches = bool(
                    re.search(
                        r"__version__(?:\s*:\s*str)?\s*=\s*['\"]3\.5\.5['\"]",
                        archive.read("pyspark/version.py").decode(),
                    )
                )
        except (OSError, KeyError, zipfile.BadZipFile):
            version_matches = False
    if (
        not version_matches
        or not os.access(home / "bin/spark-submit", os.X_OK)
        or not (home / "jars/spark-core_2.12-3.5.5.jar").is_file()
        or not (home / "python/lib/pyspark.zip").is_file()
        or not list((home / "python/lib").glob("py4j-*-src.zip"))
    ):
        raise ValueError("Spark 3.5.5 binary distribution is incomplete")
    inputs = home / "sql/core/src/test/resources/sql-tests/inputs"
    if not inputs.is_dir() or not any(inputs.iterdir()):
        raise ValueError("Matching Spark SQL test resources are missing")


def provision_spark(directory, pyspark_home=None):
    directory = directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    binary = checked_archive(directory, BINARY) if pyspark_home is None else None
    source = checked_archive(directory, SOURCE)
    with tempfile.TemporaryDirectory(
        prefix="spark-extract-", dir=directory
    ) as temporary:
        staging = Path(temporary)
        if binary is not None:
            extract_archive(binary, staging, BINARY)
        else:
            # PyPI's full PySpark distribution already supplies these binaries.
            # Keep its Python sql/ package intact and attach test sources only
            # inside the separate Spark home used by Gluten's test harness.
            (staging / BINARY).mkdir()
            for name in (
                "bin",
                "sbin",
                "jars",
                "python",
                "data",
                "examples",
                "licenses",
            ):
                source_path = pyspark_home.resolve() / name
                if source_path.is_dir():
                    (staging / BINARY / name).symlink_to(source_path)
        extract_archive(source, staging, SOURCE, sql_only=True)
        (staging / BINARY / "sql").symlink_to(Path("..") / SOURCE / "sql")
        validate_spark_home(staging / BINARY)
        for name in (SOURCE, BINARY):
            destination = directory / name
            if destination.is_symlink():
                destination.unlink()
            elif destination.exists():
                shutil.rmtree(destination)
            (staging / name).replace(destination)
    return directory / BINARY


def export_environment(home):
    pythonpath = os.pathsep.join(
        str(path) for path in sorted((home / "python/lib").glob("*.zip"))
    )
    if os.environ.get("PYTHONPATH"):
        pythonpath += os.pathsep + os.environ["PYTHONPATH"]
    values = {
        "BOLT_GLUTEN_SPARK_HOME": str(home),
        "SPARK_HOME": str(home),
        "PYTHONPATH": pythonpath,
        "PYSPARK_PYTHON": sys.executable,
    }
    if output := os.environ.get("GITHUB_ENV"):
        with Path(output).open("a") as stream:
            for key, value in values.items():
                if "\n" in value or "\r" in value:
                    raise ValueError(f"Invalid multiline environment value: {key}")
                stream.write(f"{key}={value}\n")
    print(json.dumps(values, indent=2), flush=True)
    return values


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spark-dir", type=Path, default=ROOT / ".ci/spark35")
    args = parser.parse_args()
    try:
        validate_prerequisites()
        install_python_packages()
        import pyspark

        home = provision_spark(args.spark_dir, Path(pyspark.__file__).resolve().parent)
        export_environment(home)
    except (
        ValueError,
        OSError,
        subprocess.CalledProcessError,
        tarfile.TarError,
    ) as error:
        print(f"::error::{error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
