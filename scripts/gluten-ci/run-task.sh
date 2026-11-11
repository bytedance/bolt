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

# The Python entry point validates task IDs, restores inputs and records results.
set -euo pipefail

kind=$1 stage=$2 spark=$3 jdk=$4 profiles=$5 selector=$6
# Connector UT dependencies are opt-in to the Bolt task, not shared backend profiles.
if [[ ",$profiles," == *,backends-bolt,* ]]; then
  for connector in delta hudi iceberg paimon; do
    if [[ ",$profiles," == *,"$connector",* ]]; then
      profiles+=",bolt-ut-$connector"
    fi
  done
fi
export GLUTEN_DIR=$7 BOLT_HOME=$8
bolt=$BOLT_HOME
export GLUTEN_CI_DRIVER_DIR=$9 GLUTEN_REPORTS_DIR=${10}
task_dir=${11} mode=${12}
helpers="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
native_dir="$GLUTEN_DIR/cpp/build/package/bolt/linux/amd64"

export PATH="${JAVA_HOME:?Set JAVA_HOME for this task}/bin:$PATH"
javac_version="$("$JAVA_HOME/bin/javac" -version 2>&1)"
if [[ $jdk == 8 ]]; then
  jdk=1.8
fi
if [[ "$javac_version" != "javac $jdk" && "$javac_version" != "javac $jdk."* ]]; then
  echo "Unexpected JDK version: $javac_version" >&2
  exit 1
fi

run_native() {
  export CC=clang-22 CXX=clang++-22 CI_NUM_THREADS=${CI_NUM_THREADS:-16}
  export PATH="${HOME}/miniconda3/bin:${PATH}"
  mkdir -p "${CONAN_HOME:?Set a job-private CONAN_HOME}" "$task_dir/runtime"
  bash "$bolt/scripts/install-bolt-deps.sh"
  if [[ -n "${BOLT_CI_ARROW_CACHE_SCRIPT:-}" ]]; then
    python3 "$BOLT_CI_ARROW_CACHE_SCRIPT" configure-local
  fi
  if [[ -n "${BOLT_CI_CONAN_REMOTE:-}" ]]; then
    conan remote add ci "$BOLT_CI_CONAN_REMOTE" --index 0
  fi
  bash "$helpers/ci-bolt-build.sh" "$bolt" "$GLUTEN_DIR" "$GLUTEN_REPORTS_DIR" "$mode"

  export LD_LIBRARY_PATH="$native_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  # shellcheck source=/dev/null
  source cpp/build/Release/generators/conanrun.sh
  local libs=("$native_dir"/*.so cpp/build/Release/bolt/udf/examples/libmyud{f,af}.so)
  local linkage="$GLUTEN_REPORTS_DIR/runtime-linkage.txt"
  ldd "${libs[@]}" > "$linkage"
  if grep '=> not found' "$linkage" >&2; then
    return 1
  fi
  local lib
  while IFS= read -r lib; do
    if [[ "$lib" == "${CONAN_HOME%/}/"* ]]; then
      cp -L "$lib" "$task_dir/runtime/"
    fi
  done < <(awk '$2 == "=>" {print $3}' "$linkage" | sort -u)
  if [[ "$mode" == full ]]; then
    ctest --test-dir cpp/build/Release --no-tests=error --output-on-failure \
      --output-junit "$GLUTEN_REPORTS_DIR/TEST-native.xml" -j "$CI_NUM_THREADS"
  fi
}

run_jvm() {
  local output="$GLUTEN_REPORTS_DIR/jvm" status=0 suite selected
  mkdir -p "$output"
  MVN+=("-Dbolt.native.loader.path=$native_dir/libglutenlibloader.so")
  ci_run "$output/discovery.log" ci_runtime "${MVN[@]}" dependency:build-classpath \
    -pl backends-bolt -DincludeScope=test "-Dmdep.outputFile=$output/test-classpath.txt"
  local classes="$GLUTEN_DIR/backends-bolt/target/scala-$SCALA_VERSION"
  ci_run "$output/discovery.log" "$JAVA_HOME/bin/java" --class-path \
    "$classes/test-classes:$classes/classes:$(cat "$output/test-classpath.txt")" \
    "$GLUTEN_CI_DRIVER_DIR/.github/scripts/DiscoverGlutenSuites.java" \
    "$output/discovered-suites.txt" "$classes/test-classes" "$classes/classes"

  # The loader suite calls System.load on the build output. A Spark JVM has
  # already loaded the packaged copy, which registers the same glog flags.
  local loader=org.apache.gluten.jni.BoltNativeLoaderSuite
  grep -Fxq "$loader" "$output/discovered-suites.txt"
  local ordinary=() delta=() isolated=("$loader")
  while IFS= read -r suite; do
    if [[ "$suite" == org.apache.spark.sql.delta.* ]]; then
      delta+=("$suite")
    elif [[ "$suite" != "$loader" ]]; then
      ordinary+=("$suite")
    fi
  done < "$output/discovered-suites.txt"
  (("${#ordinary[@]}" > 0))
  selected="$(
    IFS=,
    printf '%s' "${ordinary[*]}"
  )"
  CI_GROUP=backend CI_SUITES="$selected" ci_maven_test jvm backends-bolt \
    "${MVN[@]}" surefire:test scalatest:test "-DtagsToExclude=$GLUTEN_EXCLUDED_TAGS" \
    "-DargLine=-Xmx4g -Dspark.test.home=$SPARK_HOME" || status=$?
  if [[ ",$selector," == *,delta,* ]]; then
    (("${#delta[@]}" > 0))
    isolated+=("${delta[@]}")
  fi
  local suite_status
  for suite in "${isolated[@]}"; do
    suite_status=0
    CI_GROUP="$suite" CI_SUITES="$suite" ci_maven_test jvm backends-bolt \
      "${MVN[@]}" scalatest:test "-DtagsToExclude=$GLUTEN_EXCLUDED_TAGS" \
      "-DargLine=-Xmx4g -Dspark.test.home=$SPARK_HOME" || suite_status=$?
    if ((status == 0)); then status=$suite_status; fi
  done
  return "$status"
}

run_tpc() {
  ci_run "$GLUTEN_REPORTS_DIR/tpc-build.log" ci_runtime "${MVN[@]}" \
    -T "$GLUTEN_MAVEN_THREADS" install -DskipTests=true
  cd tools/gluten-it
  ci_run "$GLUTEN_REPORTS_DIR/tpc-build.log" ci_runtime "$GLUTEN_DIR/build/mvn" -ntp install \
    "-Dmaven.repo.local=$GLUTEN_MAVEN_REPO" "-Pspark-$SPARK_PROFILE" "-Pjava-$JDK_VERSION"
  local bundles=() jar
  for jar in "$GLUTEN_DIR"/package/target/gluten-package-*.jar; do
    case "$jar" in
      *-sources.jar | *-tests.jar | *-javadoc.jar | *-"$SPARK_PROFILE".jar) continue ;;
    esac
    bundles+=("$jar")
  done
  (("${#bundles[@]}" == 1))
  cmp "${bundles[0]}" "package/target/lib/${bundles[0]##*/}"

  local kind output status=0 comparison_status kinds
  IFS=, read -ra kinds <<< "$selector"
  for kind in "${kinds[@]}"; do
    output="$GLUTEN_REPORTS_DIR/tpc-$kind"
    mkdir -p "$output"
    : > "$output/run.log"
    comparison_status=0
    GLUTEN_IT_JVM_ARGS=-Xmx6G ci_run "$output/run.log" ci_runtime \
      bash sbin/gluten-it.sh queries-compare --local --preset=velox \
      --baseline-preset=vanilla "--benchmark-type=$kind" --error-on-memleak \
      --explain --off-heap-size=10g -s=1.0 --threads=16 --iterations=1 || comparison_status=$?
    if ((status == 0)); then status=$comparison_status; fi
  done
  return "$status"
}

prepare_spark() {
  # The fixed CI image's Python environment is read-only to the worker.
  virtualenv --system-site-packages "$task_dir/python"
  export PATH="$task_dir/python/bin:$PATH"
  export PYSPARK_PYTHON="$task_dir/python/bin/python3"
  local python_deps=("pyspark[sql]==$SPARK_VERSION")
  if [[ "$SPARK_PROFILE" == 3.* ]]; then
    python_deps=("pyspark==$SPARK_VERSION" numpy==1.26.4 pandas==2.2.3 pyarrow==20.0.0)
  fi
  local pip_cache="${PIP_CACHE_DIR:-$GLUTEN_CACHE_ROOT/pip}"
  mkdir -p "$pip_cache"
  local pip_install=(flock "$pip_cache/pyspark-$SPARK_VERSION.lock"
    python3 -m pip install --disable-pip-version-check --cache-dir "$pip_cache"
    --timeout 120 setuptools==77.0.3 Cython==3.3.0 "${python_deps[@]}")
  "${pip_install[@]}" || "${pip_install[@]}"
  command -v shasum
  # The driver is mounted read-only. Resource copies live in disposable scratch.
  ci_spark_resources
  local resources_home="$task_dir/.ci/spark-resources/shims/$SPARK_RESOURCE_MODULE/spark_home"
  ln -s "assembly/target/scala-$SCALA_VERSION/jars" "$resources_home/jars"
}

run_spark() {
  export SPARK_PROFILE="$spark" GLUTEN_MAVEN_THREADS="${GLUTEN_MAVEN_THREADS:-4}" IN_CI=1 LC_ALL=C.UTF-8
  export GLUTEN_CACHE_ROOT="${GLUTEN_CACHE_ROOT:-$task_dir/cache}"
  export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:+$JAVA_TOOL_OPTIONS }-XX:ActiveProcessorCount=$GLUTEN_MAVEN_THREADS"
  JAVA_TOOL_OPTIONS+=" -XX:ErrorFile=\"$GLUTEN_REPORTS_DIR/hs_err_pid%p.log\""
  export LD_LIBRARY_PATH="$GLUTEN_DIR/bolt-runtime${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  local GLUTEN_EXCLUDED_TAGS=org.apache.gluten.tags.UDFTest,org.apache.gluten.tags.EnhancedFeaturesTest,org.apache.gluten.tags.CudfTest,org.apache.gluten.tags.SkipTest
  # Select the Spark binary matching the Scala profile in the reviewed matrix.
  local scala="${profiles#*scala-}"
  export GLUTEN_CI_SCALA_VERSION="${scala%%,*}"
  # shellcheck source=scripts/gluten-ci/ci-bolt-spark-env.sh
  source "$helpers/ci-bolt-spark-env.sh"
  # The portable matrix supplies the profile set. The sourced helper's defaults
  # remain available to the existing GitHub Actions workflow.
  MVN=("$GLUTEN_DIR/build/mvn" -ntp -fae "-P$profiles"
    "-Dmaven.compiler.release=$JDK_VERSION" "-Dmaven.repo.local=$GLUTEN_MAVEN_REPO"
    -DskipTests=false -Dmaven.test.skip=false -Dmaven.test.failure.ignore=false -DfailIfNoTests=false)
  # ci_runtime adds BOLT_ROOT/.ci/gluten-runtime. Use scratch for this driver's
  # runtime and resource paths, and keep the restored libraries on LD_LIBRARY_PATH.
  export BOLT_ROOT="$task_dir"
  (cd "$native_dir" && sha256sum -- *.so) > "$GLUTEN_REPORTS_DIR/native.sha256"
  ci_run "$GLUTEN_REPORTS_DIR/jvm-build.log" ci_runtime "${MVN[@]}" \
    -T "$GLUTEN_MAVEN_THREADS" install -Pspark-ut -DskipTests=true -pl '!package'
  (cd "backends-bolt/target/scala-$SCALA_VERSION/classes/linux/amd64" \
    && sha256sum -c "$GLUTEN_REPORTS_DIR/native.sha256")
  prepare_spark 2>&1 | tee "$GLUTEN_REPORTS_DIR/spark-provision.log"
  # Provisioning changes PATH and the Spark/Python variables for the suite JVMs.
  export PATH="$task_dir/python/bin:$PATH" PYSPARK_PYTHON="$task_dir/python/bin/python3"
  export BOLT_GLUTEN_SPARK_HOME="$task_dir/.ci/spark-resources/shims/$SPARK_RESOURCE_MODULE/spark_home"
  ci_spark_environment

  local arg_line="-DargLine=-Xmx4g -Dspark.test.home=$SPARK_HOME"
  case "$stage" in
    jvm)
      run_jvm
      ;;
    udf)
      CI_SUITES="$selector" ci_maven_test "$stage" backends-bolt "${MVN[@]}" scalatest:test \
        -DtagsToInclude=org.apache.gluten.tags.UDFTest "$arg_line"
      ;;
    spark-1 | spark-2 | spark-3)
      local excluded="org.apache.spark.tags.ExtendedSQLTest,org.apache.spark.tags.SlowHiveTest,$GLUTEN_EXCLUDED_TAGS"
      ci_maven_test "$stage" reactor "${MVN[@]}" surefire:test scalatest:test -Pspark-ut \
        "-DwildcardSuites=$selector" "-DtagsToExclude=$excluded" "$arg_line"
      ;;
    extended | slow-hive)
      ci_maven_test "$stage" reactor "${MVN[@]}" surefire:test scalatest:test -Pspark-ut \
        "-DtagsToInclude=$selector" "$arg_line"
      ;;
    tpc)
      run_tpc
      ;;
  esac
}

cd "$GLUTEN_DIR"
case "$kind" in
  native)
    run_native 2>&1 | tee "$GLUTEN_REPORTS_DIR/native-build.log"
    ;;
  package | spark)
    mkdir -p "${GLUTEN_MAVEN_REPO:?Set a job-private GLUTEN_MAVEN_REPO}"
    if [[ "$kind" == package ]]; then
      "$GLUTEN_DIR/build/mvn" -ntp clean install "-P$profiles" \
        "-Dmaven.repo.local=$GLUTEN_MAVEN_REPO" -DskipTests 2>&1 | tee "$GLUTEN_REPORTS_DIR/package-build.log"
    else
      run_spark
    fi
    ;;
esac
