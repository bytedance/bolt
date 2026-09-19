#!/usr/bin/env bash
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

# Maven lifecycle baseline, following Gluten's velox_backend_x86.yml:
# clean test, serial reactor, Surefire + ScalaTest plugins. Use Bolt's profiles
# and the same tag exclusions / per-JVM limits as run.sh. All suites run in
# one invocation, including slow tests (upstream splits these across runners).
# Build native libraries / patched Arrow jars before invoking this script.
# Required: GLUTEN_HOME, SPARK_HOME, JAVA_HOME. Optional: MVN_PROFILES,
# TAGS_TO_EXCLUDE, JOB_JVM_OPTS, LOG_DIR, PARALLEL_LOG_DIR.
set -euo pipefail

: "${GLUTEN_HOME:?GLUTEN_HOME must point to the gluten source checkout}"
: "${SPARK_HOME:?SPARK_HOME must contain Spark binaries and SQL test resources}"
: "${JAVA_HOME:?JAVA_HOME must point to the test JDK}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${LOG_DIR:-$SCRIPT_DIR/logs/maven}"
PARALLEL_LOG_DIR="${PARALLEL_LOG_DIR:-$SCRIPT_DIR/logs}"
MVN_PROFILES="${MVN_PROFILES:--Pspark-3.5 -Pspark-ut -Pbackends-bolt -Pceleborn -Pjava-17}"
TAGS_TO_EXCLUDE="${TAGS_TO_EXCLUDE:-org.apache.gluten.tags.UDFTest,org.apache.gluten.tags.EnhancedFeaturesTest,org.apache.gluten.tags.SkipTest,org.apache.spark.tags.SkipTest}"
JOB_JVM_OPTS="${JOB_JVM_OPTS:--Xmx2g -XX:MaxMetaspaceSize=1g -XX:+UseG1GC -XX:ParallelGCThreads=2 -XX:ConcGCThreads=1 -XX:TieredStopAtLevel=1}"
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_TESTING=true

mkdir -p "$LOG_DIR"
LOG_DIR="$(cd "$LOG_DIR" && pwd)"
cd "$GLUTEN_HOME"
GLUTEN_HOME="$PWD"
read -r -a profiles <<< "$MVN_PROFILES"
# Continue after test failures so the remaining reactor modules are measured.
# The report checker below restores failure status, including suite aborts.
command=(./build/mvn -B -ntp clean test "${profiles[@]}"
  -Dexec.skip -Dmaven.test.failure.ignore=true
  -Dorg.slf4j.simpleLogger.showDateTime=true
  "-DargLine=$JOB_JVM_OPTS -Dspark.test.home=$SPARK_HOME"
  "-DtagsToExclude=$TAGS_TO_EXCLUDE" '-Dfilereports=W maven-scalatest.txt')
{
  printf 'Gluten commit: %s\n' "$(git rev-parse HEAD)"
  printf 'JAVA_HOME=%s\nSPARK_HOME=%s\n' "$JAVA_HOME" "$SPARK_HOME"
  printf '%q ' "${command[@]}"
  printf '\n'
} | tee "$LOG_DIR/_command.txt"
start=$(date +%s)
printf '%s\n' "$start" > "$LOG_DIR/_start_epoch.txt"
rc=0
"${command[@]}" > "$LOG_DIR/_maven.log" 2>&1 || rc=$?
elapsed=$(($(date +%s) - start))
printf '%s\n' "$rc" > "$LOG_DIR/_maven.rc"
printf '%s\n' "$elapsed" > "$LOG_DIR/_elapsed_seconds.txt"
echo "Maven clean test finished in ${elapsed}s (Maven exit $rc); see $LOG_DIR/_maven.log"
python3 "$SCRIPT_DIR/summarize_maven.py" --gluten-home "$GLUTEN_HOME" \
  --log-dir "$LOG_DIR" --parallel-log-dir "$PARALLEL_LOG_DIR"
