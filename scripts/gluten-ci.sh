#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

# Compare the package Gluten consumed with the package exported by this run.
check_bolt_package() {
  python3 - "$1" "$2" << 'PYTHON'
import json, sys
from pathlib import Path
reports = Path(sys.argv[1])
def identity(name):
    nodes = json.loads((reports / name).read_text())["graph"]["nodes"].values()
    packages = [n for n in nodes if n.get("name") == "bolt" and n.get("context") == "host"]
    if len(packages) != 1:
        raise SystemExit("Expected exactly one Bolt package")
    return {key: packages[0].get(key) for key in ("ref", "package_id", "prev")}
exported, consumed = identity("bolt-export.json"), identity("gluten-install.json")
if (not all(exported.values()) or exported != consumed or
        exported["ref"].partition("#")[0] != "bolt/ci-" + sys.argv[2]):
    raise SystemExit("Gluten did not consume this run's Bolt package")
(reports / "bolt-package.json").write_text(json.dumps(consumed) + "\n")
PYTHON
}

# Keep Maven/CTest's exit code; this additionally rejects empty or skipped-only runs.
check_reports() {
  python3 - "$1" << 'PYTHON'
from pathlib import Path
import sys
from xml.etree import ElementTree as ET
passed = failed = 0
for report in Path(sys.argv[1]).rglob("TEST-*.xml"):
    root = ET.parse(report).getroot()
    for case in root.iter("testcase"):
        if case.find("failure") is not None or case.find("error") is not None:
            failed += 1
        elif case.find("skipped") is None:
            passed += 1
if not passed or failed:
    raise SystemExit(f"Invalid test result: {passed} passed, {failed} failed")
print(f"Verified {passed} passing tests")
PYTHON
}

# Source this in each Spark step after setting Spark/Scala/JDK and cache paths.
spark_environment() {
  export SPARK_TESTING=true SPARK_ANSI_SQL_MODE=false SPARK_SCALA_VERSION="$SCALA_VERSION"
  SPARK_VERSION=$(xmllint --nonet --xpath \
    "string(//*[local-name()='profile'][*[local-name()='id']='spark-${SPARK_PROFILE}']/*[local-name()='properties']/*[local-name()='spark.version'])" \
    "$GLUTEN_ROOT/pom.xml")
  test -n "$SPARK_VERSION"
  export SPARK_VERSION
  local resource="spark${SPARK_PROFILE/./}"
  if [[ "$SPARK_PROFILE/$SCALA_VERSION" == 3.5/2.13 ]]; then resource+="-scala-2.13"; fi
  export SPARK_HOME="$GLUTEN_CACHE_ROOT/shims/$resource/spark_home"
  export PATH="$GLUTEN_ROOT/.venv/bin:$JAVA_HOME/bin:$PATH"
  export PYSPARK_PYTHON="$GLUTEN_ROOT/.venv/bin/python3"
  export LD_LIBRARY_PATH="$GLUTEN_ROOT/bolt-runtime:$GLUTEN_ROOT/cpp/build/package/bolt/linux/amd64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  local archives=("$SPARK_HOME"/python/lib/*.zip)
  if [[ -f "${archives[0]}" ]]; then
    local zipped
    zipped=$(
      IFS=:
      printf '%s' "${archives[*]}"
    )
    export PYTHONPATH="$zipped${PYTHONPATH:+:$PYTHONPATH}"
  fi
  export JAVA_TOOL_OPTIONS="-XX:ActiveProcessorCount=4 -XX:ErrorFile=$REPORTS/hs_err_pid%p.log"
  MVN=(env "HOME=$GLUTEN_ROOT/.home" setpriv --reuid=65534 --regid=65534 --clear-groups --no-new-privs "$GLUTEN_ROOT/build/mvn" -ntp -fae "-P$PROFILES"
    "-Dmaven.repo.local=$GLUTEN_MAVEN_REPO" -DskipTests=false -Dmaven.test.skip=false
    -Dmaven.test.failure.ignore=false -DfailIfNoTests=false)
  if [[ "$STAGE" != package ]]; then MVN+=("-Dmaven.compiler.release=$JDK_VERSION"); fi
}

# The upstream Makefile cannot pass all required Conan profiles/options yet.
# Use its Conan/CMake entry points with the same options as the local Bolt package.
build_native() (
  GLUTEN_ROOT=$(cd "${2:?Provide the Apache Gluten checkout as the second argument}" && pwd -P)
  BOLT_ROOT=$(cd "${1:?Usage: ci-bolt-build.sh BOLT_CHECKOUT GLUTEN_CHECKOUT [REPORTS_DIR]}" && pwd -P)
  export PATH="${HOME}/miniconda3/bin:${PATH}"
  export CC=${CC:-clang-22}
  export CXX=${CXX:-clang++-22}
  BUILD_THREADS=${CI_NUM_THREADS:-$(nproc)}
  MODE=${4:-full}
  if [[ "$MODE" != native && "$MODE" != full ]]; then
    echo "Expected native or full build mode" >&2
    exit 2
  fi
  REPORTS="${3:-$GLUTEN_ROOT/bolt-ci-reports/native}"
  mkdir -p "$REPORTS"
  REPORTS=$(cd "$REPORTS" && pwd -P)

  for tool in git make cmake ninja conan javac; do
    command -v "$tool" > /dev/null
  done
  if [[ -n "${CCACHE_DIR:-}" ]]; then
    command -v ccache > /dev/null
    ccache --show-config > "$REPORTS/ccache-config.txt"
  fi

  # Inspect the generated commands, since a persistent directory alone does not
  # enable ccache. Bolt's compiler launcher and Gluten's compile rule enable it.
  record_ccache_commands() {
    local build_dir="$1" report_name="$2"
    shift 2
    if [[ -n "${CCACHE_DIR:-}" ]]; then
      ninja -C "$build_dir" -t commands "$@" > "$REPORTS/$report_name"
      if ! grep -Eq '(^|[[:space:]])([^[:space:]]*/)?ccache[[:space:]].* -c ' "$REPORTS/$report_name"; then
        echo "No ccache compiler command found in $build_dir" >&2
        exit 1
      fi
    fi
  }

  # Use Bolt's CI toolchain profile for both Bolt and Gluten.
  python "$BOLT_ROOT/scripts/configure-conan-profile.py" \
    --compiler "$CC" --libcxx libstdc++11 --linker mold --lto none
  git -C "$BOLT_ROOT" diff --exit-code HEAD --
  BOLT_SHA=$(git -C "$BOLT_ROOT" rev-parse HEAD)
  export BOLT_BUILD_VERSION="ci-$BOLT_SHA"
  printf '%s\n' "$BOLT_SHA" > "$REPORTS/bolt-sha.txt"
  git -C "$GLUTEN_ROOT" rev-parse HEAD > "$REPORTS/gluten-sha.txt"
  git -C "$GLUTEN_ROOT" diff HEAD -- cpp > "$REPORTS/gluten-native.patch"

  BOLT_OPTIONS=(-o 'bolt/*:spark_compatible=True' -o 'bolt/*:enable_testutil=True'
    -o 'bolt/*:shared=False' -o 'bolt/*:enable_hdfs=True'
    -o 'bolt/*:enable_s3=True' -o 'bolt/*:enable_asan=False')
  HOST_PROFILE=(-pr:h default)
  BUILD_PROFILE=()
  profiles_dir="$(conan config home)/profiles"
  if [[ -f "$profiles_dir/host-profile" && -f "$profiles_dir/build-profile" ]]; then
    HOST_PROFILE=(-pr:h host-profile)
    BUILD_PROFILE=(-pr:b build-profile)
  fi
  if [[ -f "$BOLT_ROOT/scripts/conan/bolt.profile" ]]; then
    HOST_PROFILE+=(-pr:h "$BOLT_ROOT/scripts/conan/bolt.profile")
  fi
  printf -v HOST_PROFILE_ARGS '%q ' "${HOST_PROFILE[@]}"

  # Use the prepared CI toolchain and dependency remotes. Configure Bolt's
  # test-support dependencies, but build only the engine and test-support library.
  cd "$BOLT_ROOT"
  make release_spark_with_test BOLT_CONAN_CONFIGURE_ONLY=1 \
    "BUILD_VERSION=$BOLT_BUILD_VERSION" "CONAN_OVERRIDE=${BOLT_OPTIONS[*]}" \
    "CONAN_HOST_PROFILE_ARGS=$HOST_PROFILE_ARGS" \
    "CONAN_PROFILE_ARGS=$HOST_PROFILE_ARGS ${BUILD_PROFILE[*]}"
  # shellcheck source=/dev/null
  source "$BOLT_ROOT/_build/Release/generators/conanbuild.sh"
  record_ccache_commands "$BOLT_ROOT/_build/Release" bolt-build-commands.txt \
    bolt_engine bolt_testutils_static
  cmake --build --preset conan-release --target bolt_engine bolt_testutils_static \
    --parallel "$BUILD_THREADS"
  conan export-pkg . --name=bolt "--version=$BOLT_BUILD_VERSION" \
    "${HOST_PROFILE[@]}" "${BUILD_PROFILE[@]}" -s build_type=Release "${BOLT_OPTIONS[@]}" \
    -c 'bolt/*:tools.build:skip_test=False' --test-folder="" \
    --format=json > "$REPORTS/bolt-export.json"

  cd "$GLUTEN_ROOT/cpp"
  # PR native checks build all UT targets and their data, without benchmark binaries.
  BUILD_BENCHMARKS=True
  if [[ "$MODE" == native ]]; then
    BUILD_BENCHMARKS=False
  fi
  conan install . --name=gluten --version=ci-build \
    "${HOST_PROFILE[@]}" "${BUILD_PROFILE[@]}" -s build_type=Release "${BOLT_OPTIONS[@]}" \
    -o 'gluten/*:shared=True' -o 'gluten/*:build_tests=True' \
    -o 'gluten/*:build_examples=True' -o "gluten/*:build_benchmarks=$BUILD_BENCHMARKS" \
    -o 'gluten/*:enable_hdfs=True' -o 'gluten/*:enable_s3=True' \
    -o 'gluten/*:enable_asan=False' -c tools.build:skip_test=True \
    -c 'bolt/*:tools.build:skip_test=False' \
    -c 'arrow/*:tools.build:download_source=True' \
    --build=missing --build='~bolt/*' --lockfile= --format=json \
    > "$REPORTS/gluten-install.json"
  # shellcheck source=/dev/null
  source "$GLUTEN_ROOT/cpp/build/Release/generators/conanbuild.sh"
  {
    git --version
    "$CC" --version
    "$CXX" --version
    javac -version
    conan --version
    cmake --version
  } > "$REPORTS/tool-versions.txt" 2>&1
  # POST_BUILD discovery would execute test binaries even without ctest.
  cmake --preset conan-release -DCMAKE_GTEST_DISCOVER_TESTS_DISCOVERY_MODE=PRE_TEST
  record_ccache_commands "$GLUTEN_ROOT/cpp/build/Release" gluten-build-commands.txt
  cmake --build --preset conan-release --parallel "$BUILD_THREADS"
  if [[ -n "${CCACHE_DIR:-}" ]]; then
    ccache --show-stats > "$REPORTS/ccache-stats.txt"
  fi

  check_bolt_package "$REPORTS" "$BOLT_SHA"

  cd "$GLUTEN_ROOT"
  for library in libgluten.so libbolt_backend.so libglutenlibloader.so; do
    test -s "cpp/build/package/bolt/linux/amd64/$library"
  done

)
