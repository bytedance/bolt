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
#   2. scan test-classes/             discover every test suite
#   3. xargs -P JOBS                  one mvn per suite, slow ones first,
#                                     bwrap-isolated target/surefire{,-reports}
#   4. classify FAILED/ABORTED        against blacklist.txt (whole-file fixed-string match).
#
# Required env: GLUTEN_HOME, SPARK_HOME, bubblewrap binary on PATH.
# Optional env: JOBS (parallelism, default nproc/3).
#
# Logs + reports go to $SCRIPT_DIR/logs/.
# blacklist.txt / slow_suites.txt live next to this script. One entry per line,
# no comments, no blanks. Blacklist entry shape: `<FQCN>#<caseName>` for a
# specific failure, or `<FQCN>#(aborted)` for a whole-suite abort.
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
# When MVN_PROFILES targets a non-default spark version, run_one_suite adds
# `-am` so gluten-parent / gluten-substrait join the per-suite reactor and
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
SLOW_SUITES_FILE="${SLOW_SUITES_FILE:-$SCRIPT_DIR/slow_suites.txt}"
LOG_DIR="${LOG_DIR:-$SCRIPT_DIR/logs}"
MVN_BIN="${MVN_BIN:-mvn}"

# Empirically each suite needs ~3 active threads (mvn + surefire JVM + Spark
# internals). cpus/3 saturates CPU without thrashing. Override via JOBS.
if [[ -z "${JOBS:-}" ]]; then
  JOBS=$(($(grep -c ^processor /proc/cpuinfo 2> /dev/null || echo 4) / 3))
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
echo "GLUTEN_HOME=$GLUTEN_HOME  SPARK_HOME=$SPARK_HOME  JOBS=$JOBS"

command -v bwrap > /dev/null 2>&1 || {
  echo "bwrap is required for per-suite target/ isolation. Install bubblewrap." >&2
  exit 1
}

###############################################################################
# Step 1/3: install jars + test-classes
###############################################################################
step "Step 1/3: mvn clean install -DskipTests (-T $JOBS)"
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

###############################################################################
# Step 2/3: discover suites
###############################################################################
step "Step 2/3: discover suites"
SUITE_MAP="$LOG_DIR/_suites.tsv" # tab-separated: <module>\t<fqcn>

# Walk every .class under <module>/target/.../test-classes/ and emit
# `<module>\t<FQCN>` rows in $SUITE_MAP — one per runnable test suite.

# A class is concrete (runnable) iff javap's declaration line is NOT
# `abstract class` / `abstract interface` / plain `interface`.
is_concrete_class() {
  ! javap -p "$1" 2> /dev/null | head -3 \
    | grep -qE "^(public +)?abstract +(class|interface) "
}
export -f is_concrete_class

# Class names ending in one of these tokens are treated as test suites
# (matches naming conventions used across gluten + bolt test code).
SUITE_NAME_RE='(Suite|Spec|Test|Validation|Statistics|Generator|Configuration|EncodingLong)'

# Pipeline stages:
#   1. find    every <module>/target/[scala-X/]test-classes/*.class — skip
#              inner/anon classes (`$` in path), scalatest's leftover
#              DiscoverySuite stubs, and arrow's own Java tests under ep/_ep/.
#   2. xargs   drop abstract base classes via parallel javap.
#   3. sed     rewrite `./<module>/target/[scala-X/]test-classes/<path>.class`
#              into `<module><TAB><path>`.
#   4. awk     turn path slashes into FQCN dots and keep only suite-shaped names.
#   5. sort -u dedup by FQCN (same class can land in several modules).
find . -path '*/test-classes/*.class' \
  \! -path '*$*' \! -path '*DiscoverySuite*' \! -path '*/ep/_ep/*' \
  | xargs -P "$JOBS" -I{} bash -c 'is_concrete_class "{}" && echo "{}" || :' \
  | sed -nE 's|^\./(.+)/target/(scala-[^/]+/)?test-classes/(.+)\.class$|\1\t\3|p' \
  | awk -F'\t' -v OFS='\t' -v re="$SUITE_NAME_RE" \
    '{ gsub("/", ".", $2) } $2 ~ re' \
  | sort -u -t$'\t' -k2,2 > "$SUITE_MAP"

NUM_RUN=$(wc -l < "$SUITE_MAP" | tr -d ' ')
echo "Discovered $NUM_RUN suites total."
[[ -f "$BLACKLIST_FILE" ]] && echo "Blacklist: $(wc -l < "$BLACKLIST_FILE" | tr -d ' ') entries."

###############################################################################
# Step 3/3: dispatch + summarize
###############################################################################
step "Step 3/3: run $NUM_RUN suites with $JOBS parallel jobs"

WORK_ROOT="$LOG_DIR/work"
REPORTS_ROOT="$LOG_DIR/reports"
rm -rf "$WORK_ROOT" "$REPORTS_ROOT"
mkdir -p "$WORK_ROOT" "$REPORTS_ROOT"
# Drop stale per-suite logs from previous runs
find "$LOG_DIR" -maxdepth 1 -type f -name '*.log' \! -name '_*' -delete

# Pre-create per-module bind mountpoints used by run_one_suite below.
while IFS= read -r module; do
  [[ -z "$module" ]] && continue
  rm -rf "$module/target/surefire-reports" 2> /dev/null || true
  mkdir -p "$module/target/surefire" "$module/target/surefire-reports"
done < <(cut -f1 "$SUITE_MAP" | sort -u)

export MVN_BIN GLUTEN_HOME SPARK_HOME LOG_DIR WORK_ROOT REPORTS_ROOT
export MVN_PROFILES MVN_AM

run_one_suite() {
  local module="$1" suite="$2"
  local log="$LOG_DIR/${suite}.log"
  local sur="$WORK_ROOT/$suite/surefire"
  local rep="$REPORTS_ROOT/$suite"
  mkdir -p "$sur" "$rep"
  local t0=$(date +%s)
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
  #   --bind                : private target/surefire (booter jar) + target/surefire-reports
  #   --bind sandbox        : full copy of test-classes/ under /tmp, with the
  #                           conflicting `unit-tests-working-home/` (used as
  #                           Spark warehouse + metastore by GlutenSQLTestsTrait.
  #                           prepareWorkDir) carved out as a fresh dir per
  #                           suite.
  #   --ro-bind $SPARK_HOME : re-expose SPARK_HOME, otherwise --tmpfs /tmp may hide it.
  local bind_args=()
  local sandbox=""
  if [[ -n "$tc" ]]; then
    sandbox="/tmp/gluten-ut-sandbox/$suite/test-classes"
    rm -rf "/tmp/gluten-ut-sandbox/$suite"
    mkdir -p "$sandbox"
    cp -a "$tc/." "$sandbox/" 2> /dev/null
    rm -rf "$sandbox/unit-tests-working-home" 2> /dev/null
    mkdir "$sandbox/unit-tests-working-home"
    bind_args=(--bind "$sandbox" "$tc")
  fi
  local rc=0
  # shellcheck disable=SC2086
  bwrap \
    --dev-bind / / --tmpfs /tmp \
    --ro-bind "$SPARK_HOME" "$SPARK_HOME" \
    --bind "$sur" "$GLUTEN_HOME/$module/target/surefire" \
    --bind "$rep" "$GLUTEN_HOME/$module/target/surefire-reports" \
    "${bind_args[@]}" \
    --chdir "$GLUTEN_HOME" \
    "$MVN_BIN" surefire:test scalatest:test \
    -pl "$module" $MVN_AM \
    $MVN_PROFILES \
    -DfailIfNoTests=false -Dexec.skip -Dmaven.test.failure.ignore=true \
    -DargLine="-Dspark.test.home=$SPARK_HOME" \
    -Dtest="$suite" -DwildcardSuites="$suite" \
    -DtagsToExclude=org.apache.gluten.tags.UDFTest,org.apache.gluten.tags.EnhancedFeaturesTest,org.apache.gluten.tags.SkipTest \
    > "$log" 2>&1 || rc=$?
  [[ -n "$sandbox" ]] && rm -rf "/tmp/gluten-ut-sandbox/$suite"
  local secs=$(($(date +%s) - t0))
  local cases
  cases=$(sed -E 's/\x1b\[[0-9;]*m//g' "$log" \
    | grep -oE 'Total number of tests run: [0-9]+' | tail -1 \
    | grep -oE '[0-9]+' || true)
  # Trailing marker line for summary: distinguishes "mvn died before scalatest"
  # (rc != 0, no FAILED / ABORTED markers in the log) from "scalatest ran and
  # the suite passed" (rc == 0 because -Dmaven.test.failure.ignore=true; case
  # failures still show up as `*** FAILED ***` lines).
  printf '\nGLUTEN_UT_MVN_RC=%s\n' "$rc" >> "$log"
  # FD 3 = the parent's original stdout (terminal); see `exec 3>&1` below.
  printf '  done [%4ds, %4s cases] %s\n' "$secs" "${cases:-?}" "$suite" >&3
  printf 'finished\t%s\n' "$suite"
}
export -f run_one_suite

# Slow-list priority: xargs pulls from this file top-down so the suites
# named in slow_suites.txt grab the first JOBS workers and the long tail
# can't dangle. Both partitions keep SUITE_MAP's original order.
DISPATCH_MAP="$LOG_DIR/_suites_dispatch_order.tsv"
if [[ -f "$SLOW_SUITES_FILE" ]]; then
  awk 'NR==FNR{s[$0]=1;next}   $2 in s' "$SLOW_SUITES_FILE" "$SUITE_MAP" > "$DISPATCH_MAP"
  awk 'NR==FNR{s[$0]=1;next} !($2 in s)' "$SLOW_SUITES_FILE" "$SUITE_MAP" >> "$DISPATCH_MAP"
  echo "Slow-suite priority queue: $(wc -l < "$SLOW_SUITES_FILE") suite(s) dispatched first."
else
  cp "$SUITE_MAP" "$DISPATCH_MAP"
fi

# Save the terminal stdout as FD 3 so run_one_suite can print a one-line
# "done [...] <suite>" to the user as soon as each suite finishes, even
# though the dispatcher's own stdout is captured to _dispatch.log.
exec 3>&1
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
# Walk each per-suite log and emit one key per failure:
#   <FQCN>#<case>        — scalatest "*** FAILED ***" line
#   <FQCN>#(aborted)     — scalatest "*** ABORTED ***" line
# Each key is grep -Fxq'd against blacklist.txt; unmatched → unexpected.
declare -A fired
expected=0
unexpected=0
# Walk the suites that were actually dispatched this run (SUITE_MAP is the
# canonical list), not $LOG_DIR/*.log — that would also pick up stale per-
# suite logs left over from a previous run with a different profile / spark
# version.
while IFS=$'\t' read -r _module suite; do
  log="$LOG_DIR/$suite.log"
  [[ -f "$log" ]] || continue
  # mvn-failed (rc != 0 → mvn died before scalatest could run) bypasses the
  # blacklist: it always counts as unexpected, since blacklisting infra
  # failures would mask real regressions across PRs.
  rc=$(sed -nE 's/.*GLUTEN_UT_MVN_RC=([0-9]+).*/\1/p' "$log" | tail -1)
  if [[ -z "$rc" || "$rc" != "0" ]]; then
    unexpected=$((unexpected + 1))
    echo "  ! $suite#(mvn-failed)"
    continue
  fi
  clean=$(sed -E 's/\x1b\[[0-9;]*m//g' "$log")
  keys=$(echo "$clean" | sed -nE 's/^- (.*) \*\*\* FAILED \*\*\*$/'"$suite"'#\1/p')
  [[ "$clean" == *"*** ABORTED ***"* ]] && keys+=$'\n'"$suite#(aborted)"
  while IFS= read -r key; do
    [[ -z "$key" ]] && continue
    if grep -Fxq -- "$key" "$BLACKLIST_FILE"; then
      fired[$key]=1
      expected=$((expected + 1))
    else
      unexpected=$((unexpected + 1))
      echo "  ! $key"
    fi
  done <<< "$keys"
done < "$SUITE_MAP"

# Blacklist entries that didn't fire this run. If a case stays stale
# across multiple runs it's a candidate for removal from blacklist.txt.
stale=$(while IFS= read -r entry; do
  [[ -v fired[$entry] ]] || echo "  ? $entry"
done < "$BLACKLIST_FILE")
if [[ -n "$stale" ]]; then
  echo "stale blacklist entries (didn't fail this run; remove if consistently passing):"
  echo "$stale"
fi

echo "expected failures:   $expected (on blacklist; not counted)"
echo "unexpected failures: $unexpected"
exit $((unexpected > 0 ? 1 : 0))
