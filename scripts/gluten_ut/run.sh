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

# Build JVM artifacts and run UT; native Bolt/Gluten libraries and patched Arrow
# jars must already exist (see .github/workflows/bolt_gluten_ut.yml).
# Requires Gluten with gluten.test.dir support (apache/gluten#13110), system Maven,
# a JDK with JShell, Scala 2.12 Spark binaries and source sql/ test resources,
# and Python packages NumPy 1.26.4, pandas 2.2.3 and PyArrow 15.0.2.
# Usage: GLUTEN_HOME=/path/to/gluten SPARK_HOME=/path/to/spark JAVA_HOME=/path/to/jdk17 \
#          JOBS=8 bash scripts/gluten_ut/run.sh
set -euo pipefail
: "${GLUTEN_HOME:?}" "${SPARK_HOME:?}" "${JAVA_HOME:?}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export GLUTEN_HOME="$(cd "$GLUTEN_HOME" && pwd)"
export SPARK_HOME="$(cd "$SPARK_HOME" && pwd)"
export JAVA_HOME PATH="$JAVA_HOME/bin:$PATH" SPARK_TESTING=true
for zip in "$SPARK_HOME"/python/lib/*.zip; do
  export PYTHONPATH="$zip${PYTHONPATH:+:$PYTHONPATH}"
done
# JOBS=1 runs the same suites serially. LOG_DIR holds plans, summaries, logs and XML.
export JOBS="${JOBS:-4}" LOG_DIR="${LOG_DIR:-$SCRIPT_DIR/logs}"
mkdir -p "$LOG_DIR"
export LOG_DIR="$(cd "$LOG_DIR" && pwd)"
[[ "$JOBS" =~ ^[1-9][0-9]*$ ]] || {
  echo 'JOBS must be positive' >&2
  exit 1
}
# RELEASE records Spark's full version; profiles use major.minor.
read -r _ spark_version _ < "$SPARK_HOME/RELEASE"
[[ "$spark_version" =~ ^([0-9]+\.[0-9]+)\. ]] || {
  echo "Cannot detect Spark version from $SPARK_HOME/RELEASE" >&2
  exit 1
}
# Gluten activates its Java profile from the JDK selected by JAVA_HOME.
profiles=("-Pspark-${BASH_REMATCH[1]}" -Pspark-ut -Pbackends-bolt -Pceleborn)

build_gluten() (
  cd "$GLUTEN_HOME"
  mkdir -p "$SPARK_HOME/launcher/target/scala-2.12"
  echo "Building Gluten JVM artifacts: ${profiles[*]} (log: $LOG_DIR/_install.log)"
  mvn -B -ntp clean install dependency:build-classpath \
    -T "$JOBS" "${profiles[@]}" -DskipTests -Dexec.skip \
    -Dmdep.includeScope=test -Dmdep.outputFile=target/ut-classpath.txt > "$LOG_DIR/_install.log" 2>&1 || {
    tail -50 "$LOG_DIR/_install.log" >&2
    exit 1
  }
  # Match Maven's test JVM flags when launching suites directly.
  mvn -B -ntp help:evaluate -Dexpression=extraJavaTestArgs \
    -Doutput="$LOG_DIR/_jvm_args.txt" > "$LOG_DIR/_jvm_args.log" 2>&1
)

discover_tests() (
  echo "Discovering test suites (plan: $LOG_DIR/_plan.tsv)"
  # Scan Maven outputs only after the build has finished.
  find "$GLUTEN_HOME" -type d -name test-classes \
    -path '*/target/*' ! -path '*/ep/*' -print0 > "$LOG_DIR/_test_dirs"
  : > "$LOG_DIR/_suites.tsv"
  while IFS= read -r -d '' tests; do
    # Some modules have an empty test directory and no test dependencies.
    if [[ -z $(find "$tests" -name '*.class' -print -quit) ]]; then
      continue
    fi
    module=${tests#"$GLUTEN_HOME"/}
    module=${module%%/target/*}
    classes="${tests%/*}/classes"
    dependencies=$(cat "$GLUTEN_HOME/$module/target/ut-classpath.txt")
    # Test resources come first, including log4j2.properties for automatic loading.
    cp="$tests:$classes:$dependencies"
    mkdir -p "$LOG_DIR/modules/$module"
    printf '%s\n' "$cp" > "$LOG_DIR/modules/$module/classpath.txt"

    # JShell compilation and its local JVM both need the test classpath.
    # ALL-DEFAULT includes java.sql, needed to discover Spark date suites.
    UT_MODULE="$module" UT_CLASSES="$tests" "$JAVA_HOME/bin/jshell" \
      --execution local --feedback silent --no-startup -J-Xmx1g \
      -J-Djava.util.prefs.userRoot="$LOG_DIR/prefs" \
      -J--add-modules=ALL-DEFAULT -J--class-path="$cp" --class-path "$cp" \
      - < "$SCRIPT_DIR/discover.jsh" >> "$LOG_DIR/_suites.tsv"
  done < "$LOG_DIR/_test_dirs"

  # Sort by module, framework and class before assigning stable job IDs.
  LC_ALL=C sort -t $'\t' -k2,2 -k1,1 -k3,3 \
    -o "$LOG_DIR/_suites.tsv" "$LOG_DIR/_suites.tsv"
  awk -F '\t' '
    { printf "%03d\t%s\t%s\t%s\n", NR-1, $2, $1, $3 }
    END {
      if (NR == 0) {
        print "No test suites found" > "/dev/stderr"
        exit 1
      }
    }
  ' "$LOG_DIR/_suites.tsv" > "$LOG_DIR/_plan.tsv"
)

run_job() (
  set -euo pipefail
  IFS=$'\t' read -r job module kind suites <<< "$1"
  cp=$(cat "$LOG_DIR/modules/$module/classpath.txt")
  read -r -a jvm_args <<< "$(tr '\n' ' ' < "$LOG_DIR/_jvm_args.txt")"
  # Each JVM owns its work/tmp/Derby/Gluten paths; clean only that private directory.
  work=$(mktemp -d "${TMPDIR:-/tmp}/gluten-$job-XXXXXX")
  trap 'rm -rf -- "$work"' EXIT
  mkdir -p "$work/tmp" "$work/derby" "$work/target" "$LOG_DIR/reports/$job"
  ln -s "$GLUTEN_HOME/$module/src" "$work/src"
  command=(
    "$JAVA_HOME/bin/java" "${jvm_args[@]}"
    -Xmx2g -XX:MaxMetaspaceSize=1g -XX:+UseG1GC -XX:ParallelGCThreads=2
    -XX:ConcGCThreads=1 -XX:TieredStopAtLevel=1 -XX:ReservedCodeCacheSize=256m
    "-Dspark.test.home=$SPARK_HOME" "-Dgluten.test.dir=$work/gluten"
    "-Djava.io.tmpdir=$work/tmp" "-Dderby.system.home=$work/derby" -cp "$cp"
  )
  if [[ "$kind" == junit ]]; then
    IFS=, read -r -a classes <<< "$suites"
    command+=(org.junit.runner.JUnitCore "${classes[@]}")
  else
    # Match the serial Maven baseline: exclude opt-in features and explicit skips.
    command+=(
      org.scalatest.tools.Runner -oW -u "$LOG_DIR/reports/$job"
      -l "org.apache.gluten.tags.UDFTest org.apache.gluten.tags.EnhancedFeaturesTest org.apache.gluten.tags.SkipTest org.apache.spark.tags.SkipTest"
      -s "$suites"
    )
  fi
  printf '%q ' "${command[@]}" > "$LOG_DIR/jobs/$job.command.sh"
  printf '\n' >> "$LOG_DIR/jobs/$job.command.sh"
  start=$SECONDS
  rc=0
  (cd "$work" && timeout --kill-after=30s 1800s "${command[@]}") > "$LOG_DIR/jobs/$job.log" 2>&1 || rc=$?
  printf '%s\n' "$rc" > "$LOG_DIR/jobs/$job.rc"
  # Report the JVM result here; summarize applies the blacklist.
  case "$rc" in
    0) result=PASS ;;
    124) result=TIMEOUT ;;
    *) result=FAIL ;;
  esac
  # Full class names remain in _plan.tsv; keep progress lines short.
  name=${suites##*.}
  if [[ "$kind" == junit ]]; then
    name="JUnit (${#classes[@]} classes)"
  fi
  printf '[%s] %s | %ss | module=%s | job=%s | exit=%s\n' \
    "$result" "$name" "$((SECONDS - start))" "$module" "$job" "$rc"
)

summarize() (
  # Require completed runs and exact failure names; blacklisted cases still execute.
  awk -F '\t' -v logs="$LOG_DIR" -v min_tests="${MIN_TESTS:-0}" '
    function fail(reason) {
      printf "%s (%s): %s; see %s\n", job, suite, reason, log_file
      status=1
    }
    FILENAME == ARGV[1] { allowed[$0]=1; next }
    {
      job=$1
      kind=$3
      suite=$4
      jobs++
      log_file=logs "/jobs/" job ".log"
      rc_file=logs "/jobs/" job ".rc"
      if ((getline rc < rc_file) != 1) rc="missing"
      close(rc_file)
      count=-1
      complete=found=reported=aborted=0
      while ((getline line < log_file) > 0) {
        columns=split(line, fields, " ")
        if (line ~ /^OK [(][0-9]+ tests?[)]$/) {
          count=substr(fields[2], 2)
          complete=1
        }
        if (line ~ /^Run completed in/) complete=1
        if (line ~ /^Total number of tests run: [0-9]+$/) count=fields[columns]
        # Both summaries put the failed-test / aborted-suite count in field 5.
        if (line ~ /^Tests: |^Suites: /) reported+=fields[5]
        if (index(line, "*** RUN ABORTED ***") == 1) aborted=1

        name=""
        # Greedy matching keeps FAILED markers that are part of the test name.
        if (match(line, /^[[:space:]]*- .* \*\*\* FAILED \*\*\*/)) {
          name=substr(line, 1, RLENGTH-length(" *** FAILED ***"))
          sub(/^[[:space:]]*- /, "", name)
          name=suite "#" name
        }
        if (fields[2] == "***" && fields[3] == "ABORTED" && fields[4] == "***")
          name=fields[1] "#(aborted)"
        if (name == "") continue
        found++
        if (name in allowed) known++
        else fail("unexpected failure: " name)
      }
      close(log_file)
      if (count >= 0) executed+=count
      if (rc == "124") fail("test JVM timed out")
      else if (rc != "0" && rc != "1") fail("missing result or abnormal JVM exit: " rc)
      if (!complete || count < 0 || aborted) fail("test run did not complete")
      if (kind == "junit" && rc != "0") fail("JUnit failed")
      if (kind == "scalatest" && (found != reported || (rc == "1" && !found)))
        fail("failure count mismatch: parsed " found ", reported " reported ", exit " rc)
    }
    END {
      if (!jobs || executed < min_tests) {
        printf "Too few tests: %d; expected at least %d (jobs: %d)\n", executed, min_tests, jobs
        status=1
      }
      printf "Jobs: %d, executed: %d, blacklisted failures/aborts: %d, status: %d\n", jobs, executed, known, status
      exit status
    }
  ' "$SCRIPT_DIR/blacklist.txt" "$LOG_DIR/_plan.tsv"
)

# Build, discover, run and summarize in order. Only test JVMs run in parallel.
build_gluten
rm -rf -- "$LOG_DIR/jobs" "$LOG_DIR/reports" "$LOG_DIR/modules"
mkdir -p "$LOG_DIR/jobs" "$LOG_DIR/reports" "$LOG_DIR/modules"
discover_tests
awk -F '\t' -v workers="$JOBS" '
  { classes += split($4, suites, ",") }
  END { printf "Running %d classes in %d jobs, %d JVMs\n", classes, NR, workers }
' "$LOG_DIR/_plan.tsv"
export -f run_job
status=0
if ! xargs -d '\n' -n1 -P "$JOBS" bash -c 'run_job "$1"' _ < "$LOG_DIR/_plan.tsv"; then
  echo "One or more workers failed; checking all job logs."
  status=1
fi
# Always show the summary, including when it returns a failing exit status.
summarize > "$LOG_DIR/_summary.txt" || status=1
cat "$LOG_DIR/_summary.txt"
exit "$status"
