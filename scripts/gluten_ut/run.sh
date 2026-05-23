#!/usr/bin/env bash
#
# Run the full gluten Spark UT suite against the Bolt backend.
#
# Modes:
#   (default)          One reactor-wide `mvn clean test` invocation, single log.
#                      Fastest end-to-end but failures cluster in one log file.
#   --parallel [JOBS]  Pre-compile everything once, discover all test suites,
#                      then run each suite in its own mvn invocation in
#                      parallel. Per-suite stdout/stderr lands in $LOG_DIR/<fqcn>.log.
#                      JOBS defaults to $(nproc).
#
# Required environment variables:
#   GLUTEN_HOME : path to the gluten source checkout (the repo that ships
#                 backends-bolt/, gluten-ut/, etc.)
#   SPARK_HOME  : path to the spark source checkout used by suites that
#                 reference -Dspark.test.home (e.g. gluten-ut/spark35).
#
# Optional environment variables:
#   MVN              : maven binary to invoke (default: mvn)
#   LOG_DIR          : where per-suite logs land in parallel mode
#                      (default: $GLUTEN_HOME/gluten-ut-logs)
#   EXTRA_MVN_ARGS   : extra args appended to every mvn command line
#   BLACKLIST_FILE   : path to the suite blacklist (default: alongside this
#                      script as blacklist.txt). In parallel mode the listed
#                      suites are SKIPPED entirely — they neither run nor count
#                      against the exit status. Remove an entry as soon as the
#                      suite is fixed so it joins the green matrix again.
#
# Exit status:
#   Sequential mode: returns whatever the single mvn invocation returns
#                    (which is 0 with -Dmaven.test.failure.ignore=true even
#                    when individual tests fail; inspect surefire reports to
#                    decide pass/fail).
#   Parallel mode:   0 when every non-blacklisted suite is green; 1 otherwise.
#                    Prints a per-suite failure summary on stderr at the end.

set -euo pipefail

###############################################################################
# Args
###############################################################################
MODE="sequential"
PARALLEL_JOBS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --parallel)
      MODE="parallel"
      shift
      if [[ $# -gt 0 && "$1" =~ ^[0-9]+$ ]]; then
        PARALLEL_JOBS="$1"
        shift
      fi
      ;;
    --jobs|-j)
      PARALLEL_JOBS="$2"
      shift 2
      ;;
    -h|--help)
      sed -n '2,40p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown flag: $1" >&2
      exit 2
      ;;
  esac
done

if [[ "$MODE" == "parallel" && -z "$PARALLEL_JOBS" ]]; then
  PARALLEL_JOBS="$(nproc 2>/dev/null || echo 4)"
fi

###############################################################################
# Env validation + shared config
###############################################################################
: "${GLUTEN_HOME:?GLUTEN_HOME must point to the gluten source checkout}"
: "${SPARK_HOME:?SPARK_HOME must point to the spark source checkout (test resources)}"

[[ -d "$GLUTEN_HOME" ]] || { echo "GLUTEN_HOME=$GLUTEN_HOME is not a directory" >&2; exit 1; }
[[ -d "$SPARK_HOME"  ]] || { echo "SPARK_HOME=$SPARK_HOME is not a directory"   >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BLACKLIST_FILE="${BLACKLIST_FILE:-$SCRIPT_DIR/blacklist.txt}"
MVN_BIN="${MVN:-mvn}"

MVN_PROFILES=(-Pspark-3.5 -Pspark-ut -Pbackends-bolt -Piceberg -Pceleborn -Pjava-17)
COMMON_MVN_ARGS=(
  -DfailIfNoTests=false
  -Dexec.skip
  -Dmaven.test.failure.ignore=true
  -DargLine="-Dspark.test.home=$SPARK_HOME"
)

echo "GLUTEN_HOME=$GLUTEN_HOME"
echo "SPARK_HOME=$SPARK_HOME"
echo "MODE=$MODE${PARALLEL_JOBS:+ (jobs=$PARALLEL_JOBS)}"
if [[ -f "$BLACKLIST_FILE" ]]; then
  echo "BLACKLIST_FILE=$BLACKLIST_FILE"
fi

cd "$GLUTEN_HOME"

###############################################################################
# Sequential mode
###############################################################################
if [[ "$MODE" == "sequential" ]]; then
  exec "$MVN_BIN" clean test \
    "${MVN_PROFILES[@]}" \
    "${COMMON_MVN_ARGS[@]}" \
    ${EXTRA_MVN_ARGS:-}
fi

###############################################################################
# Parallel mode
###############################################################################
LOG_DIR="${LOG_DIR:-$GLUTEN_HOME/gluten-ut-logs}"
mkdir -p "$LOG_DIR"
# Clear old per-suite logs but keep the meta-step logs across runs if reused.
find "$LOG_DIR" -maxdepth 1 -name '*.log' -not -name '_*.log' -delete 2>/dev/null || true

step() { echo "===== $* ====="; }

step "Step 1/3: build & install all jars + test-classes (skipTests, -T $PARALLEL_JOBS)"
# `-DskipTests` keeps test-compile running but skips surefire/scalatest
# execution, so target/test-classes/ is populated for the discovery below.
# `-T $PARALLEL_JOBS` parallelizes the maven reactor across modules so this
# step doesn't dominate wall time.
"$MVN_BIN" clean install \
  -T "$PARALLEL_JOBS" \
  "${MVN_PROFILES[@]}" \
  -DskipTests \
  -Dexec.skip \
  ${EXTRA_MVN_ARGS:-} \
  >"$LOG_DIR/_install.log" 2>&1 || {
    echo "Install step failed; see $LOG_DIR/_install.log" >&2
    tail -40 "$LOG_DIR/_install.log" >&2
    exit 1
  }

step "Step 2/3: discover suites"
SUITE_MAP_ALL="$LOG_DIR/_suites_all.tsv"   # tab-separated: <module>\t<fqcn>
SUITE_MAP="$LOG_DIR/_suites.tsv"           # after blacklist filtering
SKIPPED_MAP="$LOG_DIR/_suites_skipped.tsv"
: >"$SUITE_MAP_ALL"
: >"$SUITE_MAP"
: >"$SKIPPED_MAP"

# Load blacklist into a hashable form.
declare -A BLACKLIST
if [[ -f "$BLACKLIST_FILE" ]]; then
  while IFS= read -r raw; do
    line="${raw%%#*}"
    line="${line#"${line%%[![:space:]]*}"}"   # ltrim
    line="${line%"${line##*[![:space:]]}"}"   # rtrim
    [[ -z "$line" ]] && continue
    BLACKLIST["$line"]=1
  done <"$BLACKLIST_FILE"
fi

# Auto-discover modules with compiled tests by scanning for test-classes/.
# We deliberately consider both maven-style (target/test-classes) and the
# scala-2.12 layout (target/scala-*/test-classes) that scalatest emits.
mapfile -t TEST_CLASSES_DIRS < <(
  {
    find . -mindepth 2 -maxdepth 6 -type d -name 'test-classes' 2>/dev/null
  } | sed 's|^\./||' | sort -u
)

# Class-name pattern broad enough to catch every TEST-*.xml the sequential
# `mvn test` produced: Suite/Spec/Test plus a handful of one-off names that
# don't follow the usual convention. Anything that slips through and is not
# actually a test class produces a no-op invocation, not a missed report.
NAME_REGEX='(Suite|Spec|Test|Validation|Statistics|Generator|Configuration|EncodingLong)'

for test_classes in "${TEST_CLASSES_DIRS[@]}"; do
  [[ -d "$test_classes" ]] || continue
  # Derive the module path from <module>/target/... or <module>/target/scala-*/...
  module="${test_classes%%/target/*}"
  while IFS= read -r class_file; do
    rel="${class_file#$test_classes/}"
    [[ "$rel" == *'$'* ]] && continue          # skip inner / anon classes
    cls="${rel%.class}"
    cls="${cls//\//.}"
    [[ "$cls" == *DiscoverySuite* ]] && continue  # scalatest's runtime artifacts
    [[ "$cls" =~ $NAME_REGEX ]] || continue
    # Drop abstract base classes — they cannot be instantiated as a runnable
    # suite. javap shows e.g. "public abstract class X extends Y".
    if javap -p "$class_file" 2>/dev/null \
         | grep -qE '^(public[[:space:]]+)?abstract[[:space:]]+(class|interface)\b'; then
      continue
    fi
    printf '%s\t%s\n' "$module" "$cls" >>"$SUITE_MAP_ALL"
    if [[ -n "${BLACKLIST[$cls]:-}" ]]; then
      printf '%s\t%s\n' "$module" "$cls" >>"$SKIPPED_MAP"
    else
      printf '%s\t%s\n' "$module" "$cls" >>"$SUITE_MAP"
    fi
  done < <(find "$test_classes" -name '*.class' -type f)
done

# Some FQCNs appear in more than one module's test-classes (e.g. shared
# gluten-ut copies). Dedup by FQCN so each suite gets dispatched exactly once;
# pick whichever module sorts first to make the choice reproducible.
sort -u -t$'\t' -k2,2 -o "$SUITE_MAP_ALL" "$SUITE_MAP_ALL"
sort -u -t$'\t' -k2,2 -o "$SUITE_MAP"     "$SUITE_MAP"
sort -u -t$'\t' -k2,2 -o "$SKIPPED_MAP"   "$SKIPPED_MAP"

NUM_ALL=$(wc -l <"$SUITE_MAP_ALL" | tr -d ' ')
NUM_RUN=$(wc -l <"$SUITE_MAP"     | tr -d ' ')
NUM_SKIP=$(wc -l <"$SKIPPED_MAP"  | tr -d ' ')
echo "Discovered $NUM_ALL suites total; will run $NUM_RUN, skip $NUM_SKIP (blacklisted)."
if (( NUM_SKIP > 0 )); then
  echo "Blacklisted (skipped):"
  cut -f2 "$SKIPPED_MAP" | sed 's/^/  - /'
fi

step "Step 3/3: run $NUM_RUN suites with $PARALLEL_JOBS parallel jobs"

# Each suite needs its own `target/surefire/` (booter jar tempDir) and
# `target/surefire-reports/` to avoid 48 concurrent mvn invocations on the
# same module clobbering each other ("The forked VM terminated without
# properly saying goodbye"). Neither surefire-maven-plugin nor
# scalatest-maven-plugin exposes a user-property override for those paths,
# so we bind-mount them per-suite with bubblewrap.
if ! command -v bwrap >/dev/null 2>&1; then
  echo "bwrap is required for parallel mode (per-suite target/ isolation)." >&2
  echo "Install bubblewrap (e.g. 'apt install bubblewrap') and re-run." >&2
  exit 1
fi

WORK_ROOT="$LOG_DIR/work"        # per-suite writable target/surefire/
REPORTS_ROOT="$LOG_DIR/reports"   # per-suite writable target/surefire-reports/
rm -rf "$WORK_ROOT" "$REPORTS_ROOT"
mkdir -p "$WORK_ROOT" "$REPORTS_ROOT"

# Wipe and pre-create the per-module surefire/ and surefire-reports/ paths so
# bwrap has stable mountpoints to bind onto.
while IFS= read -r module; do
  [[ -z "$module" ]] && continue
  rm -rf "$module/target/surefire-reports" 2>/dev/null || true
  mkdir -p "$module/target/surefire" "$module/target/surefire-reports"
done < <(cut -f1 "$SUITE_MAP_ALL" | sort -u)

export MVN_BIN GLUTEN_HOME SPARK_HOME LOG_DIR
export MVN_PROFILES_STR="${MVN_PROFILES[*]}"
export COMMON_MVN_ARGS_STR="${COMMON_MVN_ARGS[*]}"
export EXTRA_MVN_ARGS="${EXTRA_MVN_ARGS:-}"

export WORK_ROOT REPORTS_ROOT

run_one_suite() {
  local module="$1"
  local suite="$2"
  local log="$LOG_DIR/${suite}.log"
  local surefire_temp="$WORK_ROOT/$suite/surefire"
  local reports_dir="$REPORTS_ROOT/$suite"
  mkdir -p "$surefire_temp" "$reports_dir"
  # Bubblewrap bind-mounts give each worker its own private
  #   $module/target/surefire        -> $WORK_ROOT/$suite/surefire   (booter jars + tmp files)
  #   $module/target/surefire-reports -> $REPORTS_ROOT/$suite        (TEST-*.xml + *.txt)
  # Everything else on the filesystem (host paths, .m2 cache, source tree,
  # compiled classes) stays shared read-write so we don't have to copy
  # gigabytes per worker.
  # shellcheck disable=SC2086
  bwrap \
    --dev-bind / / \
    --tmpfs /tmp \
    --bind "$surefire_temp" "$GLUTEN_HOME/$module/target/surefire" \
    --bind "$reports_dir"   "$GLUTEN_HOME/$module/target/surefire-reports" \
    --chdir "$GLUTEN_HOME" \
    "$MVN_BIN" \
      surefire:test \
      scalatest:test \
      -pl "$module" \
      $MVN_PROFILES_STR \
      $COMMON_MVN_ARGS_STR \
      -Dtest="$suite" \
      -DwildcardSuites="$suite" \
      -DfailIfNoTests=false \
      $EXTRA_MVN_ARGS \
      >"$log" 2>&1 || true
  printf 'finished\t%s\n' "$suite"
}
export -f run_one_suite

# Feed module\tsuite tuples via xargs -L 1, parallelism = $PARALLEL_JOBS.
# Each line becomes two positional args ($1=module, $2=suite) to the wrapper.
(
  if (( NUM_RUN > 0 )); then
    tr '\t' ' ' <"$SUITE_MAP" \
      | xargs -P "$PARALLEL_JOBS" -L 1 \
        bash -c 'run_one_suite "$1" "$2"' _
  fi
) >"$LOG_DIR/_dispatch.log" 2>&1 &
DISPATCH_PID=$!

# Live progress (best-effort heartbeat).
while kill -0 $DISPATCH_PID 2>/dev/null; do
  sleep 10
  done_count=$(grep -c '^finished\b' "$LOG_DIR/_dispatch.log" 2>/dev/null || echo 0)
  echo "  progress: $done_count / $NUM_RUN suites complete"
done
wait $DISPATCH_PID || true

###############################################################################
# Summarize
###############################################################################
step "Summary"
python3 - "$GLUTEN_HOME" "$BLACKLIST_FILE" "$SUITE_MAP" "$SKIPPED_MAP" "$REPORTS_ROOT" <<'PY'
import glob
import os
import sys
import xml.etree.ElementTree as ET

home          = sys.argv[1]
blacklist_path = sys.argv[2]
suite_map      = sys.argv[3]
skipped_map    = sys.argv[4]
reports_root   = sys.argv[5]

def load_names(path):
    out = set()
    if not os.path.isfile(path):
        return out
    with open(path) as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            # support either bare FQCN or <module>\t<fqcn>
            out.add(line.split("\t")[-1])
    return out

blacklisted = load_names(blacklist_path)
ran          = load_names(suite_map)
skipped_set  = load_names(skipped_map)

results = {}  # fqcn -> (tests, errors, failures, skipped)
# Per-suite isolated reports first, then any leftover module-level reports.
candidate_globs = [
    os.path.join(reports_root, "*", "TEST-*.xml"),
    os.path.join(home, "**", "surefire-reports", "TEST-*.xml"),
]
seen = set()
for pat in candidate_globs:
    for path in glob.glob(pat, recursive=True):
        if path in seen:
            continue
        seen.add(path)
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError:
            continue
        name = root.get("name") or os.path.basename(path)
        tests = int(root.get("tests", 0))
        prev  = results.get(name)
        if prev is None or tests >= prev[0]:
            results[name] = (
                tests,
                int(root.get("errors", 0)),
                int(root.get("failures", 0)),
                int(root.get("skipped", 0)),
            )

# Only count suites we attempted to run.
counted = {name: r for name, r in results.items() if name in ran or not ran}

total_tests    = sum(r[0] for r in counted.values())
total_errors   = sum(r[1] for r in counted.values())
total_failures = sum(r[2] for r in counted.values())
total_skipped  = sum(r[3] for r in counted.values())
passed         = total_tests - total_errors - total_failures - total_skipped

failed_suites = [
    (name, *r) for name, r in counted.items() if r[1] + r[2] > 0
]
failed_suites.sort(key=lambda x: (-x[2] - x[3], x[0]))

print(f"Suites attempted: {len(counted)}")
print(f"Suites skipped (blacklisted): {len(skipped_set)}")
print(f"Total tests:    {total_tests}")
print(f"  passed:       {passed}")
print(f"  failures:     {total_failures}")
print(f"  errors:       {total_errors}")
print(f"  skipped/canceled: {total_skipped}")
print()
if failed_suites:
    fail_case_count = sum(s[2] + s[3] for s in failed_suites)
    print(f"FAILED: {len(failed_suites)} suite(s), {fail_case_count} failing case(s)")
    for name, tests, errors, failures, _ in failed_suites:
        print(f"  ! {name}: errors={errors} failures={failures} (of {tests})")
    sys.exit(1)

print("All attempted suites PASSED.")
sys.exit(0)
PY
