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

# Start a local Celeborn cluster, run shuffle e2e tests, then clean up.
#
# Usage: run_e2e.sh [--build-type Debug|Release]
# Build directory is resolved as _build/<build-type>.
#
# Environment variables (all optional):
#   BOLT_CELEBORN_GIT_REPO             - Celeborn git repo URL (default: https://github.com/apache/celeborn.git)
#   BOLT_CELEBORN_GIT_REF              - git ref to build (default: 81d89f3)
#   BOLT_CELEBORN_MASTER_HOST           - master bind host (default: 127.0.0.1)
#   BOLT_CELEBORN_MASTER_PORT           - master bind port (default: 19097)
#   BOLT_CELEBORN_NUM_WORKERS          - number of worker instances (default: $(nproc))
#   BOLT_CELEBORN_WORKER_BASE_PORT      - first worker rpc port (default: 19098)
#   BOLT_CELEBORN_TEST_PATTERNS        - comma-separated ctest -R patterns
#   BOLT_CELEBORN_CTEST_TIMEOUT_SECONDS - per-test timeout (default: 7200)

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CELEBORN_TEST_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
PROJECT_ROOT=$(cd "${SCRIPT_DIR}/../../../../../.." && pwd)

RUNTIME_DIR="/tmp/bolt-celeborn-runtime-${USER:-unknown}"
CELEBORN_HOME="${RUNTIME_DIR}/celeborn-bin"
CELEBORN_SOURCE_HOME="${RUNTIME_DIR}/celeborn-src"
CELEBORN_GIT_REPO=${BOLT_CELEBORN_GIT_REPO:-"https://github.com/apache/celeborn.git"}
CELEBORN_GIT_REF=${BOLT_CELEBORN_GIT_REF:-"81d89f3"}

MASTER_HOST=${BOLT_CELEBORN_MASTER_HOST:-"127.0.0.1"}
MASTER_PORT=${BOLT_CELEBORN_MASTER_PORT:-19097}
MASTER_ENDPOINT="${MASTER_HOST}:${MASTER_PORT}"

# Number of worker instances.  Each worker gets a unique set of ports
# (base + worker_index * 4) and its own storage directory.
NUM_WORKERS=${BOLT_CELEBORN_NUM_WORKERS:-$(nproc)}
WORKER_BASE_PORT=${BOLT_CELEBORN_WORKER_BASE_PORT:-19098}

CELEBORN_CONF_DIR="${CELEBORN_HOME}/conf"
CELEBORN_DATA_DIR="${RUNTIME_DIR}/worker-data"
CELEBORN_LOG_DIR="${RUNTIME_DIR}/logs"

STATE_DIR="${RUNTIME_DIR}/state"
CELEBORN_PID_DIR="${STATE_DIR}/pids"
LM_ENDPOINT_FILE="${STATE_DIR}/lifecycle_manager.endpoint"
LM_STOP_FILE="${STATE_DIR}/lifecycle_manager.stop"
LM_PID_FILE="${STATE_DIR}/lifecycle_manager.pid"
LM_APP_ID="bolt-shuffle-test-$$"

CTEST_TIMEOUT=${BOLT_CELEBORN_CTEST_TIMEOUT_SECONDS:-7200}
TEST_LOG_DIR="${RUNTIME_DIR}/test-logs"

mkdir -p "${RUNTIME_DIR}" "${STATE_DIR}" "${CELEBORN_LOG_DIR}" \
  "${CELEBORN_DATA_DIR}" "${CELEBORN_PID_DIR}" "${TEST_LOG_DIR}"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

usage() {
  echo "Usage: $0 [--build-type Debug|Release]"
}

BUILD_TYPE="Release"
while (($# > 0)); do
  case "$1" in
    --build-type)
      shift
      if (($# == 0)); then
        echo "--build-type requires a value" >&2
        usage
        exit 1
      fi
      BUILD_TYPE="$1"
      ;;
    --build-type=*) BUILD_TYPE="${1#*=}" ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "Unexpected argument: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done
BUILD_DIR="${PROJECT_ROOT}/_build/${BUILD_TYPE}"

if [[ "${BUILD_TYPE}" != "Debug" && "${BUILD_TYPE}" != "Release" ]]; then
  echo "Invalid --build-type: ${BUILD_TYPE}" >&2
  exit 1
fi
if [[ ! -d "${BUILD_DIR}" ]]; then
  echo "Build directory does not exist: ${BUILD_DIR}" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Java detection
# ---------------------------------------------------------------------------

if [[ -z "${JAVA_HOME:-}" ]] && command -v java > /dev/null 2>&1; then
  export JAVA_HOME
  JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")
fi
if ! command -v java > /dev/null 2>&1 || [[ -z "${JAVA_HOME:-}" ]]; then
  echo "Java not found. Install JDK 11+ and set JAVA_HOME." >&2
  exit 1
fi

# Detect Java major version for Celeborn build profile.
JAVA_MAJOR_VERSION="21"
if [[ "$(java -version 2>&1)" =~ version[[:space:]]\"([0-9]+) ]]; then
  JAVA_MAJOR_VERSION="${BASH_REMATCH[1]}"
fi

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

wait_for_port() {
  local host="$1" port="$2" timeout_sec="${3:-30}" waited=0
  while ((waited < timeout_sec)); do
    bash -c "</dev/tcp/${host}/${port}" 2> /dev/null && return 0
    sleep 1
    ((waited += 1))
  done
  return 1
}

# Kill Celeborn Master/Worker processes found via jps.
kill_celeborn_java() {
  command -v jps > /dev/null 2>&1 || return 0
  local pids=()
  while read -r pid cls _; do
    case "${cls}" in
      org.apache.celeborn.service.deploy.master.Master | \
        org.apache.celeborn.service.deploy.worker.Worker) pids+=("${pid}") ;;
    esac
  done < <(jps -l)
  ((${#pids[@]} == 0)) && return 0

  echo "Killing Celeborn processes: ${pids[*]}"
  kill "${pids[@]}" 2> /dev/null || true
  # Wait up to 10s for graceful exit, then force-kill.
  for _ in $(seq 1 10); do
    local alive=0
    for p in "${pids[@]}"; do kill -0 "$p" 2> /dev/null && alive=1 && break; done
    ((alive == 0)) && return 0
    sleep 1
  done
  for p in "${pids[@]}"; do kill -9 "$p" 2> /dev/null || true; done
}

# ---------------------------------------------------------------------------
# Celeborn provisioning: clone, build (with cache), and extract
# ---------------------------------------------------------------------------

provision_celeborn() {
  local ref_file="${CELEBORN_SOURCE_HOME}/.bolt_celeborn_archive_ref"

  # Clone if needed.
  if [[ ! -d "${CELEBORN_SOURCE_HOME}/.git" ]]; then
    echo "Cloning Celeborn from ${CELEBORN_GIT_REPO}"
    git clone "${CELEBORN_GIT_REPO}" "${CELEBORN_SOURCE_HOME}"
  fi
  git -C "${CELEBORN_SOURCE_HOME}" fetch --all --tags
  git -C "${CELEBORN_SOURCE_HOME}" checkout "${CELEBORN_GIT_REF}"

  local head_ref
  head_ref=$(git -C "${CELEBORN_SOURCE_HOME}" rev-parse --short=7 HEAD)

  # Rebuild only if ref changed or archive missing.
  local need_build=true
  if [[ -f "${ref_file}" && "$(< "${ref_file}")" == "${head_ref}" ]]; then
    for f in "${CELEBORN_SOURCE_HOME}"/apache-celeborn-*-bin.tgz; do
      [[ -f "$f" ]] && need_build=false && break
    done
  fi
  if ${need_build}; then
    rm -f "${CELEBORN_SOURCE_HOME}"/apache-celeborn-*-bin.tgz
    echo "Building Celeborn distribution (ref=${head_ref})"
    # shellcheck disable=SC2086
    (cd "${CELEBORN_SOURCE_HOME}" && ./build/make-distribution.sh \
      -DskipTests -Pspark-3.5 -Pjdk-${JAVA_MAJOR_VERSION})
    echo "${head_ref}" > "${ref_file}"
  fi

  # Extract archive.
  local archive=""
  for f in "${CELEBORN_SOURCE_HOME}"/apache-celeborn-*-bin.tgz; do
    [[ -f "$f" ]] && archive="$f" && break
  done
  if [[ -z "${archive}" ]]; then
    echo "Celeborn archive not found under ${CELEBORN_SOURCE_HOME}" >&2
    return 1
  fi
  echo "Extracting ${archive}"
  rm -rf "${CELEBORN_HOME}"
  mkdir -p "${CELEBORN_HOME}"
  tar -xzf "${archive}" -C "${CELEBORN_HOME}" --strip-components=1
}

# ---------------------------------------------------------------------------
# Celeborn service lifecycle
# ---------------------------------------------------------------------------

start_celeborn() {
  provision_celeborn
  kill_celeborn_java

  # Reset worker data.
  rm -rf "${CELEBORN_DATA_DIR}"
  mkdir -p "${CELEBORN_DATA_DIR}" "${CELEBORN_CONF_DIR}"

  # Write master env config.
  cat > "${CELEBORN_CONF_DIR}/celeborn-env.sh" << EOF
#!/usr/bin/env bash
export CELEBORN_LOG_DIR=${CELEBORN_LOG_DIR}
export CELEBORN_PID_DIR=${CELEBORN_PID_DIR}
EOF
  chmod +x "${CELEBORN_CONF_DIR}/celeborn-env.sh"

  # Write master config (also used by worker 0).
  local w0_rpc=$((WORKER_BASE_PORT))
  local w0_push=$((WORKER_BASE_PORT + 1))
  local w0_fetch=$((WORKER_BASE_PORT + 2))
  local w0_replicate=$((WORKER_BASE_PORT + 3))
  cat > "${CELEBORN_CONF_DIR}/celeborn-defaults.conf" << EOF
celeborn.master.host ${MASTER_HOST}
celeborn.master.port ${MASTER_PORT}
celeborn.master.endpoints ${MASTER_ENDPOINT}
celeborn.worker.rpc.port ${w0_rpc}
celeborn.worker.push.port ${w0_push}
celeborn.worker.fetch.port ${w0_fetch}
celeborn.worker.replicate.port ${w0_replicate}
celeborn.worker.storage.dirs ${CELEBORN_DATA_DIR}/w0
celeborn.client.push.buffer.max.size 256K
celeborn.data.io.numConnectionsPerPeer 8
EOF

  # Start master.
  "${CELEBORN_HOME}/sbin/start-master.sh"
  if ! wait_for_port "${MASTER_HOST}" "${MASTER_PORT}" 60; then
    echo "Celeborn master failed to start" >&2
    return 1
  fi
  echo "Master started (${MASTER_ENDPOINT})"

  # Start workers.  Each worker uses its own CELEBORN_CONF_DIR with unique
  # ports and storage directory so multiple instances can coexist on one host.
  for ((w = 0; w < NUM_WORKERS; w++)); do
    local rpc=$((WORKER_BASE_PORT + w * 4))
    local push=$((rpc + 1))
    local fetch=$((rpc + 2))
    local replicate=$((rpc + 3))
    local data_dir="${CELEBORN_DATA_DIR}/w${w}"
    local worker_conf_dir="${RUNTIME_DIR}/worker-conf-${w}"
    mkdir -p "${data_dir}" "${worker_conf_dir}"

    # Per-worker env (shared log/pid dirs).
    cat > "${worker_conf_dir}/celeborn-env.sh" << WEOF
#!/usr/bin/env bash
export CELEBORN_LOG_DIR=${CELEBORN_LOG_DIR}
export CELEBORN_PID_DIR=${CELEBORN_PID_DIR}
WEOF
    chmod +x "${worker_conf_dir}/celeborn-env.sh"

    # Per-worker config with unique ports (rpc/push/fetch/replicate/http)
    # and dedicated storage directory.
    local http_port=$((19200 + w))
    cat > "${worker_conf_dir}/celeborn-defaults.conf" << WEOF
celeborn.master.host ${MASTER_HOST}
celeborn.master.port ${MASTER_PORT}
celeborn.master.endpoints ${MASTER_ENDPOINT}
celeborn.worker.rpc.port ${rpc}
celeborn.worker.push.port ${push}
celeborn.worker.fetch.port ${fetch}
celeborn.worker.replicate.port ${replicate}
celeborn.worker.http.port ${http_port}
celeborn.worker.storage.dirs ${data_dir}
celeborn.client.push.buffer.max.size 256K
celeborn.data.io.numConnectionsPerPeer 8
WEOF

    CELEBORN_CONF_DIR="${worker_conf_dir}" \
      WORKER_INSTANCE=$((w + 1)) \
      "${CELEBORN_HOME}/sbin/start-worker.sh" "celeborn://${MASTER_ENDPOINT}"
    echo "Started worker ${w} (rpc=${rpc}, push=${push}, fetch=${fetch}, data=${data_dir})"
  done

  # Wait for all workers to report "Worker started." in their logs.
  local expected=${NUM_WORKERS}
  local waited=0
  while ((waited < 90)); do
    local started
    started=$(grep -l "Worker started\." "${CELEBORN_LOG_DIR}"/celeborn-*Worker*.out 2> /dev/null | wc -l)
    if ((started >= expected)); then
      echo "Celeborn cluster ready: ${NUM_WORKERS} worker(s)"
      return 0
    fi
    sleep 1
    ((waited += 1))
  done
  echo "Only $(grep -l "Worker started\." "${CELEBORN_LOG_DIR}"/celeborn-*Worker*.out 2> /dev/null | wc -l)/${expected} workers started within 90s" >&2
  return 1
}

stop_celeborn() {
  if [[ -d "${CELEBORN_HOME}" ]] && command -v java > /dev/null 2>&1; then
    "${CELEBORN_HOME}/sbin/stop-worker.sh" 2> /dev/null || true
    "${CELEBORN_HOME}/sbin/stop-master.sh" 2> /dev/null || true
  fi
  kill_celeborn_java
}

# ---------------------------------------------------------------------------
# LifecycleManager helper
# ---------------------------------------------------------------------------

start_lifecycle_manager() {
  # Skip if already running.
  if [[ -f "${LM_PID_FILE}" ]] && kill -0 "$(< "${LM_PID_FILE}")" 2> /dev/null; then
    echo "LifecycleManager already running (pid=$(< "${LM_PID_FILE}"))"
    return
  fi
  rm -f "${LM_STOP_FILE}" "${LM_ENDPOINT_FILE}" "${LM_PID_FILE}"

  # Find the Celeborn spark client jar from extracted Celeborn package.
  local jar_path=""
  for f in "${CELEBORN_HOME}/spark/celeborn-client-spark-"*.jar; do
    [[ -f "$f" ]] && jar_path="$f" && break
  done
  if [[ -z "${jar_path}" || ! -f "${jar_path}" ]]; then
    echo "LifecycleManager jar not found under ${CELEBORN_HOME}/spark." >&2
    return 1
  fi

  # Build classpath from all Celeborn jars.
  local classpath="${jar_path}"
  for dir in "${CELEBORN_HOME}"/{jars,master-jars,worker-jars,cli-jars}; do
    [[ -d "${dir}" ]] || continue
    for jar in "${dir}"/*.jar; do
      [[ -f "${jar}" && "${jar}" != "${jar_path}" ]] && classpath="${classpath}:${jar}"
    done
  done

  # Launch and wait for endpoint file.
  local helper_src="${CELEBORN_TEST_ROOT}/java/LifecycleManagerHelper.java"
  java -cp "${classpath}" "${helper_src}" "${MASTER_ENDPOINT}" "${LM_APP_ID}" \
    "${LM_ENDPOINT_FILE}" "${LM_STOP_FILE}" > "${CELEBORN_LOG_DIR}/lifecycle_manager.log" 2>&1 &
  echo $! > "${LM_PID_FILE}"

  local waited=0
  while ((waited < 30)); do
    [[ -s "${LM_ENDPOINT_FILE}" ]] && echo "LifecycleManager endpoint: $(< "${LM_ENDPOINT_FILE}")" && return 0
    sleep 1
    ((waited += 1))
  done
  echo "LifecycleManager failed to publish endpoint" >&2
  return 1
}

stop_lifecycle_manager() {
  [[ -f "${LM_PID_FILE}" ]] || return 0
  local pid
  pid=$(< "${LM_PID_FILE}")
  kill -0 "${pid}" 2> /dev/null || {
    rm -f "${LM_PID_FILE}" "${LM_ENDPOINT_FILE}" "${LM_STOP_FILE}"
    return 0
  }

  # Signal graceful shutdown via stop file, then force-kill if needed.
  touch "${LM_STOP_FILE}"
  for _ in $(seq 1 10); do
    kill -0 "${pid}" 2> /dev/null || break
    sleep 1
  done
  kill -0 "${pid}" 2> /dev/null && kill "${pid}" 2> /dev/null || true
  rm -f "${LM_PID_FILE}" "${LM_ENDPOINT_FILE}" "${LM_STOP_FILE}"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

cleanup() {
  stop_lifecycle_manager || true
  stop_celeborn || true
}
trap cleanup EXIT

start_celeborn
start_lifecycle_manager

# Resolve test patterns.
DEFAULT_TEST_PATTERNS=(
  "bolt_shuffle_spark_celeborn_e2e_test"
  "bolt_shuffle_spark_matrix_test"
  "bolt_shuffle_spark_large_partition_test"
  "bolt_shuffle_spark_memory_test"
)
if [[ -n "${BOLT_CELEBORN_TEST_PATTERNS:-}" ]]; then
  IFS=',' read -r -a TEST_PATTERNS <<< "${BOLT_CELEBORN_TEST_PATTERNS}"
else
  TEST_PATTERNS=("${DEFAULT_TEST_PATTERNS[@]}")
fi

# Launch each test pattern in parallel.
PIDS=()
PATTERN_NAMES=()
for pattern in "${TEST_PATTERNS[@]}"; do
  [[ -z "${pattern}" ]] && continue
  log_file="${TEST_LOG_DIR}/${pattern}.log"
  echo "Starting: ${pattern} (timeout=${CTEST_TIMEOUT}s)"
  BOLT_CELEBORN_E2E=1 \
    BOLT_SHUFFLE_TEST_REAL_CELEBORN=1 \
    BOLT_CELEBORN_LM_ENDPOINT_FILE="${LM_ENDPOINT_FILE}" \
    BOLT_CELEBORN_LM_APP_ID="${LM_APP_ID}" \
    ctest --test-dir "${BUILD_DIR}" --output-on-failure --timeout "${CTEST_TIMEOUT}" -R "${pattern}" \
    > "${log_file}" 2>&1 &
  PIDS+=($!)
  PATTERN_NAMES+=("${pattern}")
done

# Collect results.
FAILED=()
for i in "${!PIDS[@]}"; do
  if ! wait "${PIDS[$i]}"; then
    FAILED+=("${PATTERN_NAMES[$i]}")
    echo "FAILED: ${PATTERN_NAMES[$i]} (see ${TEST_LOG_DIR}/${PATTERN_NAMES[$i]}.log)"
  else
    echo "PASSED: ${PATTERN_NAMES[$i]}"
  fi
done

if ((${#FAILED[@]} > 0)); then
  echo "Failed test patterns: ${FAILED[*]}" >&2
  for p in "${FAILED[@]}"; do
    echo "--- ${p} ---"
    tail -20 "${TEST_LOG_DIR}/${p}.log"
    echo "---"
  done
  exit 1
fi
