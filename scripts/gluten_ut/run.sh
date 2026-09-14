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

#
# Run the Gluten UT matrix against the Bolt backend.
#
#   1. mvn install -DskipTests        build jars + test-classes
#   2. classify                       per module: test classpath (mvn
#                                     dependency:build-classpath) + runnable
#                                     test classes (SuiteClassifier.java)
#   3. plan.py                        batch small suites / shard big ones into
#                                     JVM jobs of ~TARGET_SECS each
#   4. xargs -P JOBS                  one JVM per job (scalatest Runner or
#                                     JUnitCore, no mvn), bwrap-isolated
#                                     test-classes/, heaviest job first
#   5. summarize.py                   classify FAILED / ABORTED against
#                                     blacklist.txt (whole-line match)
#
# Required env: GLUTEN_HOME, SPARK_HOME, JAVA_HOME, bubblewrap binary on PATH.
# Optional env: JOBS (parallel JVMs, default min(nproc/2, RAM/4GB)),
#               TARGET_SECS, SHARD_MIN_SECS (job sizing), JOB_JVM_OPTS,
#               SHARD_INDEX / SHARD_COUNT (run 1/N of the jobs, for a CI matrix),
#               SKIP_INSTALL=1 (reuse the previous build; local iteration),
#               REFRESH_TIMINGS=1 (overwrite suite_times.txt / test_times.txt
#               with what this run measured).
#
# Logs + reports go to $SCRIPT_DIR/logs/. blacklist.txt, suite_times.txt and
# test_times.txt live next to this script; the timing files are only hints for
# packing/sharding, never a filter — unknown suites still run.
# Blacklist entry shape: `<FQCN>#<caseName>` or `<FQCN>#(aborted)`.
#
# Exit status: 0 if every failure is on the blacklist, else 1.

set -euo pipefail

###############################################################################
# Maven profiles. Override via env to switch Spark versions:
#   DEFAULT_SPARK_VERSION=3.5                  (default; the version that
#                                               gluten-parent's pom hard-
#                                               codes as the property defaults
#                                               for ${sparkshim.artifactId} /
#                                               ${spark.major.version} / etc.)
#   MVN_PROFILES='-Pspark-3.4 -Pspark-ut -Pbackends-bolt -Pceleborn -Pjava-17'
#
# When MVN_PROFILES targets a non-default spark version, the per-module mvn
# calls add `-am` so gluten-parent / gluten-substrait join the reactor and
# their property defaults get re-resolved via -P.
###############################################################################
DEFAULT_SPARK_VERSION="${DEFAULT_SPARK_VERSION:-3.5}"
MVN_PROFILES="${MVN_PROFILES:--Pspark-${DEFAULT_SPARK_VERSION} -Pspark-ut -Pbackends-bolt -Pceleborn -Pjava-17}"

MVN_AM=""
if [[ "$MVN_PROFILES" =~ -Pspark-(3\.[0-9]+) ]]; then
  [[ "${BASH_REMATCH[1]}" != "$DEFAULT_SPARK_VERSION" ]] && MVN_AM="-am"
fi

###############################################################################
# Config
###############################################################################
: "${GLUTEN_HOME:?GLUTEN_HOME must point to the gluten source checkout}"
: "${SPARK_HOME:?SPARK_HOME must point to an unpacked Spark source tree (for spark.test.home)}"
: "${JAVA_HOME:?JAVA_HOME must point to the JDK used to run the tests}"
[[ -d "$GLUTEN_HOME" ]] || {
  echo "GLUTEN_HOME=$GLUTEN_HOME is not a directory" >&2
  exit 1
}
[[ -d "$SPARK_HOME" ]] || {
  echo "SPARK_HOME=$SPARK_HOME is not a directory" >&2
  exit 1
}

# Spark's AbstractCommandBuilder.getScalaVersion() reads either of these dirs
# in source-build mode (only one allowed, otherwise "ambiguous Scala version").
# Without it, local-cluster Worker forks die with "Cannot find any build
# directories" before any Executor launches. The dir only has to exist — it
# stays empty. Idempotent so safe to repeat across runs.
mkdir -p "$SPARK_HOME/launcher/target/scala-2.12" 2> /dev/null || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Override via env to pick per-spark-version lists (e.g. blacklist-3.4.txt),
# or to share lists across multiple bolt checkouts.
BLACKLIST_FILE="${BLACKLIST_FILE:-$SCRIPT_DIR/blacklist.txt}"
SUITE_TIMES_FILE="${SUITE_TIMES_FILE:-$SCRIPT_DIR/suite_times.txt}"
TEST_TIMES_FILE="${TEST_TIMES_FILE:-$SCRIPT_DIR/test_times.txt}"
LOG_DIR="${LOG_DIR:-$SCRIPT_DIR/logs}"
MVN_BIN="${MVN_BIN:-mvn}"
# Job sizing: batches of small suites are filled up to TARGET_SECS; suites
# heavier than SHARD_MIN_SECS are split into shards of ~TARGET_SECS.
TARGET_SECS="${TARGET_SECS:-90}"
SHARD_MIN_SECS="${SHARD_MIN_SECS:-120}"
# scalatest tags to skip; same set as backends-bolt/pom.xml's `exclude-tests`
# profile plus gluten's own SkipTest.
TAGS_TO_EXCLUDE="${TAGS_TO_EXCLUDE:-org.apache.gluten.tags.UDFTest,org.apache.gluten.tags.EnhancedFeaturesTest,org.apache.gluten.tags.SkipTest,org.apache.spark.tags.SkipTest}"

# JVM sizing for one job. The poms leave the heap at the JVM default (1/4 of
# RAM), which is far too much once several JVMs run side by side: cap it, and
# limit the GC threads so N JVMs don't spawn N*cores of them. Stay on G1 —
# DynamicOffHeapSizingSuite expects the heap to shrink after an explicit GC,
# which ParallelGC doesn't do. Every gluten suite additionally reserves 1 GB
# of off-heap (native) memory. C1-only JIT (TieredStopAtLevel=1): a job JVM
# lives for a minute or two and mostly runs cold code, so C2 compilation was
# pure overhead (measured: -13% CPU, same wall time).
JOB_JVM_OPTS="${JOB_JVM_OPTS:--Xmx2g -XX:MaxMetaspaceSize=1g -XX:+UseG1GC -XX:ParallelGCThreads=2 -XX:ConcGCThreads=1 -XX:TieredStopAtLevel=1}"
JOB_MEM_GB="${JOB_MEM_GB:-4}" # heap + off-heap + metaspace budget per job
# Each job is a Spark local[2] driver plus Bolt native threads: about two
# cores per JVM saturates the CPU, and JOB_MEM_GB per JVM bounds the memory.
# Override via JOBS.
if [[ -z "${JOBS:-}" ]]; then
  cpu_jobs=$(($(grep -c ^processor /proc/cpuinfo 2> /dev/null || echo 4) / 2))
  mem_jobs=$(($(awk '/MemTotal/ {print $2}' /proc/meminfo 2> /dev/null || echo 16000000) / 1024 / 1024 / JOB_MEM_GB))
  JOBS=$((cpu_jobs < mem_jobs ? cpu_jobs : mem_jobs))
  ((JOBS < 1)) && JOBS=1
fi

mkdir -p "$LOG_DIR"
cd "$GLUTEN_HOME"
# step() prefixes each banner with "[<m:ss total> | prev <m:ss>]" so the
# wall-time of each phase is visible from the banner that opens the NEXT one.
SCRIPT_START=$(date +%s)
LAST_STEP=$SCRIPT_START
step() {
  local now total delta
  now=$(date +%s)
  total=$((now - SCRIPT_START))
  delta=$((now - LAST_STEP))
  printf '===== [%d:%02d total | prev %d:%02d] %s =====\n' \
    "$((total / 60))" "$((total % 60))" \
    "$((delta / 60))" "$((delta % 60))" "$*"
  LAST_STEP=$now
}
echo "GLUTEN_HOME=$GLUTEN_HOME  SPARK_HOME=$SPARK_HOME  JAVA_HOME=$JAVA_HOME  JOBS=$JOBS"

command -v bwrap > /dev/null 2>&1 || {
  echo "bwrap is required for per-job test-classes/ isolation. Install bubblewrap." >&2
  exit 1
}
command -v python3 > /dev/null 2>&1 || {
  echo "python3 is required (plan.py / summarize.py)." >&2
  exit 1
}
# Same as gluten's own UT jobs (velox_backend_x86.yml) and `make test_spark35`:
# Utils.isTesting must be true, e.g. HiveClientImpl.runSqlHive asserts on it.
export SPARK_TESTING=true

###############################################################################
# Step 1/5: install jars + test-classes
###############################################################################
step "Step 1/5: mvn clean install -DskipTests (-T $JOBS)"
if [[ "${SKIP_INSTALL:-0}" == 1 ]]; then
  # Local iteration only: reuse the jars / test-classes of a previous run.
  echo "SKIP_INSTALL=1: reusing existing build outputs"
else
  # clear stale targets
  find . -path '*/target/test-classes' -prune -exec rm -rf {} + 2> /dev/null
  find . -path '*/target/scala-*/test-classes' -prune -exec rm -rf {} + 2> /dev/null
  # shellcheck disable=SC2086
  "$MVN_BIN" clean install -T "$JOBS" $MVN_PROFILES \
    -DskipTests -Dexec.skip \
    > "$LOG_DIR/_install.log" 2>&1 || {
    echo "Install step failed; see $LOG_DIR/_install.log" >&2
    tail -40 "$LOG_DIR/_install.log" >&2
    exit 1
  }
fi

###############################################################################
# Step 2/5: per module, test classpath + runnable test classes
###############################################################################
step "Step 2/5: classify test classes"
CP_DIR="$LOG_DIR/classpath"
CLASSIFIED_DIR="$LOG_DIR/classified"
rm -rf "$CP_DIR" "$CLASSIFIED_DIR"
mkdir -p "$CP_DIR" "$CLASSIFIED_DIR"

# JVM flags the poms hand to every forked test JVM (--add-opens etc.).
JVM_ARGS=$("$MVN_BIN" -q -ntp help:evaluate -Dexpression=extraJavaTestArgs -DforceStdout 2> /dev/null | tr '\n' ' ' || true)
[[ "$JVM_ARGS" == *IgnoreUnrecognizedVMOptions* ]] || {
  echo "could not read extraJavaTestArgs from the gluten pom (got: '$JVM_ARGS')" >&2
  exit 1
}
export JVM_ARGS

# Modules = every directory holding a compiled test-classes/ (Scala or Java
# layout), skipping arrow's own tests under ep/_ep/.
MODULES=$(find . -type d \( -path '*/target/test-classes' -o -path '*/target/scala-*/test-classes' \) \
  \! -path '*/ep/_ep/*' \
  | sed -E 's|^\./(.+)/target/.*|\1|' | sort -u)

export MVN_BIN MVN_PROFILES MVN_AM CP_DIR CLASSIFIED_DIR SCRIPT_DIR GLUTEN_HOME SPARK_HOME

# module_dirs <module>: echo "<classes> <test-classes>" (absolute paths).
module_dirs() {
  local m="$GLUTEN_HOME/$1" classes="" test_classes=""
  for d in "$m/target/scala-"*/classes "$m/target/classes"; do
    [[ -d "$d" ]] && classes="$d" && break
  done
  for d in "$m/target/scala-"*/test-classes "$m/target/test-classes"; do
    [[ -d "$d" ]] && test_classes="$d" && break
  done
  echo "$classes $test_classes"
}
export -f module_dirs

classify_module() {
  local module="$1" tag="${1//\//_}" classes test_classes
  read -r classes test_classes < <(module_dirs "$module")
  # shellcheck disable=SC2086
  "$MVN_BIN" -q -ntp -pl "$module" $MVN_AM $MVN_PROFILES dependency:build-classpath \
    -Dmdep.includeScope=test -Dmdep.outputFile="$CP_DIR/$tag.txt" \
    > "$CP_DIR/$tag.log" 2>&1 || {
    echo "  ! dependency:build-classpath failed for $module; see $CP_DIR/$tag.log"
    return 0
  }
  local cp="$test_classes:$classes:$(cat "$CP_DIR/$tag.txt")"
  (
    cd "$GLUTEN_HOME/$module"
    "$JAVA_HOME/bin/java" -Xmx1g -Dlog4j.configurationFile=file:src/test/resources/log4j2.properties \
      -Dspark.test.home="$SPARK_HOME" -cp "$cp" \
      "$SCRIPT_DIR/SuiteClassifier.java" "$module" "$test_classes" \
      > "$CLASSIFIED_DIR/$tag.tsv" 2> "$CLASSIFIED_DIR/$tag.log"
  ) || echo "  ! SuiteClassifier failed for $module; see $CLASSIFIED_DIR/$tag.log"
  printf '  %-28s scalatest=%-4s junit=%s\n' "$module" \
    "$(grep -c '^scalatest' "$CLASSIFIED_DIR/$tag.tsv" || true)" \
    "$(grep -c '^junit' "$CLASSIFIED_DIR/$tag.tsv" || true)"
}
export -f classify_module
echo "$MODULES" | xargs -P "$JOBS" -I{} bash -c 'classify_module "{}"'
cat "$CLASSIFIED_DIR"/*.tsv > "$LOG_DIR/_classified.tsv"
echo "Discovered $(grep -cE '^(scalatest|junit)' "$LOG_DIR/_classified.tsv") test classes in $(echo "$MODULES" | wc -l) modules."
[[ -f "$BLACKLIST_FILE" ]] && echo "Blacklist: $(wc -l < "$BLACKLIST_FILE" | tr -d ' ') entries."

###############################################################################
# Step 3/5: plan JVM jobs
###############################################################################
step "Step 3/5: plan jobs (target ${TARGET_SECS}s, shard suites >= ${SHARD_MIN_SECS}s)"
JOBS_DIR="$LOG_DIR/jobs"
REPORTS_ROOT="$LOG_DIR/reports"
PLAN="$LOG_DIR/_plan.tsv"
rm -rf "$JOBS_DIR" "$REPORTS_ROOT"
mkdir -p "$JOBS_DIR" "$REPORTS_ROOT"
# SHARD_INDEX / SHARD_COUNT split the jobs across several runners (CI matrix).
python3 "$SCRIPT_DIR/plan.py" --classified "$LOG_DIR/_classified.tsv" \
  --suite-times "$SUITE_TIMES_FILE" --test-times "$TEST_TIMES_FILE" \
  --target "$TARGET_SECS" --shard-min "$SHARD_MIN_SECS" \
  --shard-index "${SHARD_INDEX:-0}" --shard-count "${SHARD_COUNT:-1}" \
  --jobs-dir "$JOBS_DIR" --plan "$PLAN"
NUM_JOBS=$(wc -l < "$PLAN" | tr -d ' ')

###############################################################################
# Step 4/5: dispatch
###############################################################################
step "Step 4/5: run $NUM_JOBS jobs with $JOBS parallel JVMs"
export JOBS_DIR REPORTS_ROOT TAGS_TO_EXCLUDE JOB_JVM_OPTS

# Mountpoints for the per-job private cwd state (see run_job); created here,
# once per module, so concurrent jobs don't race on creating them.
while IFS= read -r module; do
  [[ -z "$module" ]] && continue
  mkdir -p "$GLUTEN_HOME/$module/spark-warehouse"
done < <(cut -f2 "$PLAN" | sort -u)

run_job() {
  local job="$1" module="$2" kind="$3" weight="$4" members="$5" tests_file="$6"
  local tag="${module//\//_}" log="$JOBS_DIR/$job.log" rep="$REPORTS_ROOT/$job"
  local classes test_classes
  read -r classes test_classes < <(module_dirs "$module")
  local cp="$test_classes:$classes:$(cat "$CP_DIR/$tag.txt")"
  mkdir -p "$rep"
  local t0
  t0=$(date +%s)
  # Per-job isolation via bwrap: a private copy of test-classes/ under /tmp,
  # with the conflicting `unit-tests-working-home/` (used as Spark warehouse +
  # metastore by GlutenSQLTestsTrait.prepareWorkDir) carved out as a fresh dir
  # per job. --ro-bind $SPARK_HOME re-exposes it, otherwise --tmpfs /tmp may
  # hide it.
  local sandbox="/tmp/gluten-ut-sandbox/$job/test-classes"
  rm -rf "/tmp/gluten-ut-sandbox/$job"
  mkdir -p "$sandbox"
  cp -a "$test_classes/." "$sandbox/" 2> /dev/null
  rm -rf "$sandbox/unit-tests-working-home" 2> /dev/null
  mkdir "$sandbox/unit-tests-working-home"
  # Suites also write cwd-relative state into the module dir: the default
  # spark.sql.warehouse.dir (spark-warehouse/<suite>/...) and the embedded
  # Derby metastore (metastore_db/, derby.log). Two jobs of one module — in
  # particular two shards of the same suite — would clash there. So each job
  # gets a fresh private spark-warehouse/ bound over the module's (the
  # mountpoint was created before dispatch), and Derby is pointed at the job's
  # private /tmp via derby.system.home (Derby must create metastore_db itself,
  # binding an empty dir over it breaks it).
  local module_dir="$GLUTEN_HOME/$module" cwd_binds=()
  mkdir -p "/tmp/gluten-ut-sandbox/$job/spark-warehouse"
  cwd_binds+=(--bind "/tmp/gluten-ut-sandbox/$job/spark-warehouse" "$module_dir/spark-warehouse")
  cwd_binds+=(--dir /tmp/derby)

  local args=() s
  if [[ "$kind" == junit ]]; then
    args=(org.junit.runner.JUnitCore)
    IFS=',' read -ra s <<< "$members"
    args+=("${s[@]}")
  else
    args=(org.scalatest.tools.Runner -R "$classes $test_classes" -oW -u "$rep")
    IFS=',' read -ra s <<< "$TAGS_TO_EXCLUDE"
    for t in "${s[@]}"; do args+=(-l "$t"); done
    IFS=',' read -ra s <<< "$members"
    for t in "${s[@]}"; do args+=(-s "$t"); done
    if [[ "$tests_file" != "-" ]]; then
      while IFS= read -r t; do [[ -n "$t" ]] && args+=(-t "$t"); done < "$tests_file"
    fi
  fi
  local rc=0
  # shellcheck disable=SC2086
  bwrap \
    --dev-bind / / --tmpfs /tmp \
    --ro-bind "$SPARK_HOME" "$SPARK_HOME" \
    --bind "$sandbox" "$test_classes" \
    "${cwd_binds[@]}" \
    --chdir "$GLUTEN_HOME/$module" \
    "$JAVA_HOME/bin/java" $JVM_ARGS $JOB_JVM_OPTS \
    -Dlog4j.configurationFile=file:src/test/resources/log4j2.properties \
    -Dspark.test.home="$SPARK_HOME" -Dderby.system.home=/tmp/derby \
    -cp "$cp" "${args[@]}" > "$log" 2>&1 || rc=$?
  echo "$rc" > "$JOBS_DIR/$job.rc"
  rm -rf "/tmp/gluten-ut-sandbox/$job"
  local secs=$(($(date +%s) - t0))
  # CPU seconds (user+sys) of everything this shell waited for, i.e. the JVM.
  # `times` must run in this shell (a $(...) subshell has no children yet).
  local cpu
  times > "$JOBS_DIR/$job.times"
  cpu=$(tail -1 "$JOBS_DIR/$job.times" | awk '{ for (i = 1; i <= NF; i++) { split($i, a, /[ms]/); t += a[1] * 60 + a[2] } printf "%d", t }')
  local cases
  cases=$(sed -E 's/\x1b\[[0-9;]*m//g' "$log" \
    | grep -oE 'Total number of tests run: [0-9]+|^OK \([0-9]+ tests?\)|^Tests run: [0-9]+' | tail -1 \
    | grep -oE '[0-9]+' || true)
  # FD 3 = the parent's original stdout (terminal); see `exec 3>&1` below.
  printf '  done [%4ds wall %4ss cpu, est %4ss, %4s cases] %s\n' "$secs" "${cpu:-?}" "${weight%.*}" "${cases:-?}" "$job" >&3
}
export -f run_job

# Save the terminal stdout as FD 3 so run_job can print a one-line
# "done [...] <job>" to the user as soon as each job finishes, even though
# the dispatcher's own stdout is captured to _dispatch.log.
exec 3>&1
(
  xargs -P "$JOBS" -d '\n' -I{} bash -c 'IFS=$'"'"'\t'"'"' read -r -a f <<< "$1"; run_job "${f[@]}"' _ {} < "$PLAN"
) > "$LOG_DIR/_dispatch.log" 2>&1 &
DISPATCH_PID=$!

# Best-effort progress heartbeat.
while kill -0 $DISPATCH_PID 2> /dev/null; do
  sleep 10
  done_count=$(find "$JOBS_DIR" -name '*.rc' 2> /dev/null | wc -l)
  echo "  progress: $done_count / $NUM_JOBS jobs complete"
done
wait $DISPATCH_PID || true

###############################################################################
# Step 5/5: summarize
###############################################################################
step "Summary"
rc=0
python3 "$SCRIPT_DIR/summarize.py" --plan "$PLAN" --jobs-dir "$JOBS_DIR" \
  --reports-dir "$REPORTS_ROOT" --blacklist "$BLACKLIST_FILE" \
  --timings-dir "$LOG_DIR" --test-times-min "$SHARD_MIN_SECS" || rc=$?
if [[ "${REFRESH_TIMINGS:-0}" == 1 ]]; then
  cp "$LOG_DIR/_suite_times.txt" "$SUITE_TIMES_FILE"
  cp "$LOG_DIR/_test_times.txt" "$TEST_TIMES_FILE"
  echo "timing hints refreshed: $SUITE_TIMES_FILE, $TEST_TIMES_FILE"
else
  echo "measured timings in $LOG_DIR/_suite_times.txt and _test_times.txt (REFRESH_TIMINGS=1 to adopt them)"
fi
step "Done"
exit $rc
