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
# Run the gluten Spark UT matrix against the Bolt backend.
#
# JOBS=1  : single reactor-wide `mvn clean test` (returns mvn's exit code,
#           sees failures via per-module surefire-reports/).
# JOBS>=2 : parallel mode —
#   1. mvn install -DskipTests        build jars + test-classes
#   2. scan test-classes/             discover every test suite
#   3. xargs -P JOBS                  one mvn per suite, slow ones first,
#                                     bwrap-isolated target/surefire{,-reports}
#   4. summarize TEST-*.xml           split failures into expected/unexpected
#                                     by blacklist (suite- or case-level).
#
# Required env: GLUTEN_HOME, SPARK_HOME.
# Optional env: JOBS (parallelism, default nproc/3).
#
# Logs + reports go to $SCRIPT_DIR/logs/.
# blacklist.txt and slow_suites.txt live next to this script.
#
# Parallel-mode exit: 0 if every failing case is on the blacklist, else 1.

set -euo pipefail

###############################################################################
# Maven profiles — edit here to (de)activate optional sub-systems.
###############################################################################
MVN_PROFILES=(-Pspark-3.5 -Pspark-ut -Pbackends-bolt -Pceleborn -Pjava-17)

###############################################################################
# Config
###############################################################################
: "${GLUTEN_HOME:?GLUTEN_HOME must point to the gluten source checkout}"
: "${SPARK_HOME:?SPARK_HOME must point to the spark binary + test resources}"
[[ -d "$GLUTEN_HOME" ]] || {
  echo "GLUTEN_HOME=$GLUTEN_HOME is not a directory" >&2
  exit 1
}
[[ -d "$SPARK_HOME" ]] || {
  echo "SPARK_HOME=$SPARK_HOME is not a directory" >&2
  exit 1
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BLACKLIST_FILE="$SCRIPT_DIR/blacklist.txt"
SLOW_SUITES_FILE="$SCRIPT_DIR/slow_suites.txt"
LOG_DIR="$SCRIPT_DIR/logs"
MVN_BIN=mvn

# Empirically each suite needs ~3 active threads (mvn + surefire JVM + Spark
# internals). nproc/3 saturates CPU without thrashing. Override via JOBS.
if [[ -z "${JOBS:-}" ]]; then
  JOBS=$(($(nproc 2> /dev/null || echo 4) / 3))
  ((JOBS < 1)) && JOBS=1
fi

mkdir -p "$LOG_DIR"
cd "$GLUTEN_HOME"
step() { echo "===== $* ====="; }
echo "GLUTEN_HOME=$GLUTEN_HOME  SPARK_HOME=$SPARK_HOME  JOBS=$JOBS"

###############################################################################
# Sequential mode (JOBS=1): single reactor-wide mvn invocation.
###############################################################################
if ((JOBS == 1)); then
  step "Sequential mode: mvn clean test (reactor-wide)"
  exec "$MVN_BIN" clean test "${MVN_PROFILES[@]}" \
    -DfailIfNoTests=false -Dexec.skip -DDmaven.test.failure.ignore=true \
    -DargLine="-Dspark.test.home=$SPARK_HOME" > "$LOG_DIR"/sequential.log 2>&1og
  status=$?
  if [ $status -eq 0 ] && grep -q "\*\*\* FAILED \*\*\*" "$LOG_DIR"/sequential.log; then exit 1; fi; exit $status
fi

# Parallel mode requires bwrap for per-suite target/ isolation.
command -v bwrap > /dev/null 2>&1 || {
  echo "bwrap is required for parallel mode. Install bubblewrap or set JOBS=1." >&2
  exit 1
}

###############################################################################
# Step 1/3: install jars + test-classes
###############################################################################
step "Step 1/3: mvn clean install -DskipTests (-T $JOBS)"
"$MVN_BIN" clean install -T "$JOBS" "${MVN_PROFILES[@]}" \
  -DskipTests -Dexec.skip \
  > "$LOG_DIR/_install.log" 2>&1 || {
  echo "Install step failed; see $LOG_DIR/_install.log" >&2
  tail -40 "$LOG_DIR/_install.log" >&2
  exit 1
}

###############################################################################
# Step 2/3: discover suites
###############################################################################
step "Step 2/3: discover suites"
SUITE_MAP="$LOG_DIR/_suites.tsv" # tab-separated: <module>\t<fqcn>
: > "$SUITE_MAP"

# Test-class FQCNs we accept. Anything outside still skips for free since
# `mvn -Dtest=X` with -DfailIfNoTests=false is a no-op when X doesn't match.
NAME_REGEX='(Suite|Spec|Test|Validation|Statistics|Generator|Configuration|EncodingLong)'
mapfile -t TEST_CLASSES_DIRS < <(
  find . -mindepth 2 -maxdepth 6 -type d -name 'test-classes' 2> /dev/null \
    | sed 's|^\./||' | sort -u
)
for tc in "${TEST_CLASSES_DIRS[@]}"; do
  module="${tc%%/target/*}"
  while IFS= read -r class_file; do
    rel="${class_file#$tc/}"
    [[ "$rel" == *'$'* ]] && continue # skip inner / anon classes
    cls="${rel%.class}"
    cls="${cls//\//.}"
    [[ "$cls" == *DiscoverySuite* ]] && continue
    [[ "$cls" =~ $NAME_REGEX ]] || continue
    # Drop abstract base classes; they can't be instantiated as a suite.
    javap -p "$class_file" 2> /dev/null \
      | grep -qE '^(public[[:space:]]+)?abstract[[:space:]]+(class|interface)\b' \
      && continue
    printf '%s\t%s\n' "$module" "$cls" >> "$SUITE_MAP"
  done < <(find "$tc" -name '*.class' -type f)
done
# Same FQCN can compile into multiple modules' test-classes; dedup by FQCN.
sort -u -t$'\t' -k2,2 -o "$SUITE_MAP" "$SUITE_MAP"

NUM_RUN=$(wc -l < "$SUITE_MAP" | tr -d ' ')
echo "Discovered $NUM_RUN suites total."
if [[ -f "$BLACKLIST_FILE" ]]; then
  bl_total=$(grep -cvE '^\s*(#|$)' "$BLACKLIST_FILE" || true)
  bl_case=$(grep -vE '^\s*(#|$)' "$BLACKLIST_FILE" | grep -c '#' || true)
  echo "Blacklist: $((bl_total - bl_case)) suite-level + $bl_case case-level entries."
fi

###############################################################################
# Step 3/3: dispatch + summarize
###############################################################################
step "Step 3/3: run $NUM_RUN suites with $JOBS parallel jobs"

WORK_ROOT="$LOG_DIR/work"
REPORTS_ROOT="$LOG_DIR/reports"
rm -rf "$WORK_ROOT" "$REPORTS_ROOT"
mkdir -p "$WORK_ROOT" "$REPORTS_ROOT"

# Pre-create per-module bind mountpoints used by run_one_suite below.
while IFS= read -r module; do
  [[ -z "$module" ]] && continue
  rm -rf "$module/target/surefire-reports" 2> /dev/null || true
  mkdir -p "$module/target/surefire" "$module/target/surefire-reports"
done < <(cut -f1 "$SUITE_MAP" | sort -u)

export MVN_BIN GLUTEN_HOME SPARK_HOME LOG_DIR WORK_ROOT REPORTS_ROOT
export MVN_PROFILES_STR="${MVN_PROFILES[*]}"

run_one_suite() {
  local module="$1" suite="$2"
  local log="$LOG_DIR/${suite}.log"
  local sur="$WORK_ROOT/$suite/surefire"
  local rep="$REPORTS_ROOT/$suite"
  mkdir -p "$sur" "$rep"
  # Find the module's test-classes/ dir (Scala or Java layout).
  local tc=""
  for d in "$GLUTEN_HOME/$module/target/scala-2.12/test-classes" \
    "$GLUTEN_HOME/$module/target/test-classes"; do
    [[ -d "$d" ]] && {
      tc="$d"
      break
    }
  done
  # Per-suite isolation via bwrap:
  #   --bind          : private target/surefire (booter jar) + target/surefire-reports
  #   --tmp-overlay   : writable overlay on the module's test-classes/, so the
  #                     shared `unit-tests-working-home/` (used as Spark warehouse +
  #                     metastore by GlutenSQLTestsTrait.prepareWorkDir) is private
  #                     per suite. Writes go to an invisible tmpfs; the original
  #                     test-classes/ on disk is untouched.
  #   --ro-bind $SPARK_HOME : re-expose SPARK_HOME, otherwise --tmpfs /tmp would hide it.
  local overlay_args=()
  [[ -n "$tc" ]] && overlay_args=(--overlay-src "$tc" --tmp-overlay "$tc")
  # shellcheck disable=SC2086
  bwrap \
    --dev-bind / / --tmpfs /tmp \
    --ro-bind "$SPARK_HOME" "$SPARK_HOME" \
    --bind "$sur" "$GLUTEN_HOME/$module/target/surefire" \
    --bind "$rep" "$GLUTEN_HOME/$module/target/surefire-reports" \
    "${overlay_args[@]}" \
    --chdir "$GLUTEN_HOME" \
    "$MVN_BIN" surefire:test scalatest:test \
    -pl "$module" \
    $MVN_PROFILES_STR \
    -DfailIfNoTests=false -Dexec.skip -Dmaven.test.failure.ignore=true \
    -DargLine="-Dspark.test.home=$SPARK_HOME" \
    -Dtest="$suite" -DwildcardSuites="$suite" \
    > "$log" 2>&1 || true
  printf 'finished\t%s\n' "$suite"
}
export -f run_one_suite

# Slow-list priority: xargs preserves input order, so suites listed in
# slow_suites.txt grab the first JOBS workers and the long tail can't dangle.
DISPATCH_MAP="$LOG_DIR/_suites_dispatch_order.tsv"
if [[ -f "$SLOW_SUITES_FILE" ]]; then
  awk '
    NR==FNR { sub(/#.*/, "", $0); gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0)
              if ($0 != "") slow[$0]=1; next }
    { print ($2 in slow ? "0\t" : "1\t") $0 }
  ' "$SLOW_SUITES_FILE" "$SUITE_MAP" \
    | sort -s -k1,1 | cut -f2- > "$DISPATCH_MAP"
  num_slow=$(awk -v f="$SLOW_SUITES_FILE" '
    NR==FNR { sub(/#.*/, "", $0); gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0)
              if ($0 != "") slow[$0]=1; next }
    $2 in slow { n++ } END { print n+0 }
  ' "$SLOW_SUITES_FILE" "$SUITE_MAP")
  echo "Slow-suite priority queue: $num_slow suite(s) dispatched first."
else
  cp "$SUITE_MAP" "$DISPATCH_MAP"
fi

(
  tr '\t' ' ' < "$DISPATCH_MAP" | xargs -P "$JOBS" -L 1 \
    bash -c 'run_one_suite "$1" "$2"' _
) > "$LOG_DIR/_dispatch.log" 2>&1 &
DISPATCH_PID=$!

# Best-effort progress heartbeat.
while kill -0 $DISPATCH_PID 2> /dev/null; do
  sleep 10
  done_count=$(grep -c '^finished\b' "$LOG_DIR/_dispatch.log" 2> /dev/null || echo 0)
  echo "  progress: $done_count / $NUM_RUN suites complete"
done
wait $DISPATCH_PID || true

step "Summary"
# Walk each per-suite log, classify scalatest's `*** FAILED ***` /
# `*** ABORTED ***` markers as expected/unexpected against the blacklist.
# Blacklist syntax: `<FQCN>#<case>` for one case, `<FQCN>#*` for whole suite.
# Detailed failure messages live in $LOG_DIR/<suite>.log.

# Materialize the blacklist into a single grep-friendly file (one entry per line,
# blanks and comment lines stripped).
bl=$(mktemp)
trap 'rm -f "$bl"' EXIT
sed -E '/^[[:space:]]*(#|$)/d; s/^[[:space:]]+//; s/[[:space:]]+$//' \
  "$BLACKLIST_FILE" > "$bl"

expected=0
unexpected=0
unexpected_lines=""
for log in "$LOG_DIR"/*.log; do
  name="${log##*/}"
  case "$name" in _*) continue ;; esac # skip _install.log, _dispatch.log
  suite="${name%.log}"
  bl_hit=0
  grep -Fxq -- "$suite#*" "$bl" && bl_hit=1 # whole-suite blacklisted?

  # Strip ANSI codes, pull the test-case name out of each FAILED marker.
  cases=$(sed -E 's/\x1b\[[0-9;]*m//g' "$log" \
    | sed -nE 's/^- (.*) \*\*\* FAILED \*\*\*$/\1/p')
  if [[ -n "$cases" ]]; then
    while IFS= read -r c; do
      if ((bl_hit)) || grep -Fxq -- "$suite#$c" "$bl"; then
        expected=$((expected + 1))
      else
        unexpected=$((unexpected + 1))
        unexpected_lines+="  ! $suite#$c"$'\n'
      fi
    done <<< "$cases"
  elif sed -E 's/\x1b\[[0-9;]*m//g' "$log" | grep -q '\*\*\* ABORTED \*\*\*'; then
    if ((bl_hit)); then
      expected=$((expected + 1))
    else
      unexpected=$((unexpected + 1))
      unexpected_lines+="  ! $suite (aborted)"$'\n'
    fi
  fi
done

[[ -n "$unexpected_lines" ]] && printf '%s' "$unexpected_lines"
echo "expected failures:   $expected (on blacklist; not counted)"
echo "unexpected failures: $unexpected"
exit $((unexpected > 0 ? 1 : 0))
