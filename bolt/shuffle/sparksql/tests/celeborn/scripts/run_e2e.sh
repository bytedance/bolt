#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CELEBORN_TEST_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

RUNTIME_DIR=${BOLT_CELEBORN_RUNTIME_DIR:-"/tmp/bolt-celeborn-runtime-${USER:-unknown}"}
CELEBORN_HOME=${BOLT_CELEBORN_HOME:-"${RUNTIME_DIR}/celeborn-bin"}
CELEBORN_SOURCE_HOME=${BOLT_CELEBORN_SOURCE_HOME:-"${RUNTIME_DIR}/celeborn-src"}
CELEBORN_GIT_REPO=${BOLT_CELEBORN_GIT_REPO:-"https://github.com/apache/celeborn.git"}
CELEBORN_GIT_REF=${BOLT_CELEBORN_GIT_REF:-"81d89f3"}
CELEBORN_ARCHIVE_REF_FILE="${CELEBORN_SOURCE_HOME}/.bolt_celeborn_archive_ref"
if command -v java > /dev/null 2>&1; then
  JAVA_MAJOR_VERSION=$(java -version 2>&1)
else
  JAVA_MAJOR_VERSION=""
fi
if [[ "${JAVA_MAJOR_VERSION}" =~ version[[:space:]]\"([0-9]+) ]]; then
  JAVA_MAJOR_VERSION="${BASH_REMATCH[1]}"
else
  JAVA_MAJOR_VERSION="21"
fi
CELEBORN_BUILD_ARGS="-DskipTests -Pspark-3.5 -Pjdk-${JAVA_MAJOR_VERSION}"
LM_HELPER_JAR_PATH=""

MASTER_HOST=${BOLT_CELEBORN_MASTER_HOST:-"127.0.0.1"}
MASTER_PORT=${BOLT_CELEBORN_MASTER_PORT:-19097}
WORKER_RPC_PORT=${BOLT_CELEBORN_WORKER_RPC_PORT:-19098}
WORKER_PUSH_PORT=${BOLT_CELEBORN_WORKER_PUSH_PORT:-19099}
WORKER_FETCH_PORT=${BOLT_CELEBORN_WORKER_FETCH_PORT:-19100}
WORKER_REPLICATE_PORT=${BOLT_CELEBORN_WORKER_REPLICATE_PORT:-19101}

CELEBORN_CONF_DIR="${CELEBORN_HOME}/conf"
CELEBORN_DATA_DIR=${BOLT_CELEBORN_DATA_DIR:-"${RUNTIME_DIR}/worker-data"}
CELEBORN_LOG_DIR=${BOLT_CELEBORN_LOG_DIR:-"${RUNTIME_DIR}/logs"}

STATE_DIR="${RUNTIME_DIR}/state"
LM_ENDPOINT_FILE=${BOLT_CELEBORN_LM_ENDPOINT_FILE:-"${STATE_DIR}/lifecycle_manager.endpoint"}
LM_STOP_FILE=${BOLT_CELEBORN_LM_STOP_FILE:-"${STATE_DIR}/lifecycle_manager.stop"}
LM_PID_FILE=${BOLT_CELEBORN_LM_PID_FILE:-"${STATE_DIR}/lifecycle_manager.pid"}
LM_APP_ID=${BOLT_CELEBORN_LM_APP_ID:-"bolt-shuffle-test-${$}"}

mkdir -p "${RUNTIME_DIR}" "${STATE_DIR}" "${CELEBORN_LOG_DIR}" "${CELEBORN_DATA_DIR}"

clean_celeborn_runtime_state() {
  rm -rf "${CELEBORN_DATA_DIR}"
  mkdir -p "${CELEBORN_DATA_DIR}"
}

if [[ -z "${JAVA_HOME:-}" ]] && command -v java > /dev/null 2>&1; then
  JAVA_BIN=$(readlink -f "$(command -v java)")
  export JAVA_HOME
  JAVA_HOME=$(dirname "$(dirname "${JAVA_BIN}")")
fi

usage() {
  echo "Usage: $0 [--build-type Debug|Release] [build_dir]"
}

BUILD_TYPE="Debug"
BUILD_DIR=""
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
    --build-type=*)
      BUILD_TYPE="${1#*=}"
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      if [[ -z "${BUILD_DIR}" ]]; then
        BUILD_DIR="$1"
      else
        echo "Unexpected argument: $1" >&2
        usage
        exit 1
      fi
      ;;
  esac
  shift
done

if [[ "${BUILD_TYPE}" != "Debug" && "${BUILD_TYPE}" != "Release" ]]; then
  echo "Invalid --build-type: ${BUILD_TYPE}. Expected Debug or Release." >&2
  exit 1
fi

if [[ -z "${BUILD_DIR}" ]]; then
  BUILD_DIR="_build/${BUILD_TYPE}"
fi

function require_java() {
  if ! command -v java > /dev/null 2>&1; then
    echo "java command not found; install JDK 11+ in the development container" >&2
    return 1
  fi
  if [[ -z "${JAVA_HOME:-}" ]]; then
    echo "JAVA_HOME is empty; set JAVA_HOME before starting Celeborn" >&2
    return 1
  fi
}

function ensure_celeborn_binary() {
  ensure_celeborn_archive_from_github || return 1

  local archive_path=""
  for candidate in "${CELEBORN_SOURCE_HOME}"/apache-celeborn-*-bin.tgz; do
    if [[ -f "${candidate}" ]]; then
      archive_path="${candidate}"
      break
    fi
  done

  if [[ -z "${archive_path}" ]]; then
    echo "Celeborn archive not found under ${CELEBORN_SOURCE_HOME}." >&2
    return 1
  fi

  echo "Extracting ${archive_path}"
  rm -rf "${CELEBORN_HOME}"
  mkdir -p "${CELEBORN_HOME}"
  tar -xzf "${archive_path}" -C "${CELEBORN_HOME}" --strip-components=1
}

function ensure_celeborn_archive_from_github() {
  if [[ ! -d "${CELEBORN_SOURCE_HOME}/.git" ]]; then
    echo "Cloning Celeborn source from ${CELEBORN_GIT_REPO}"
    git clone "${CELEBORN_GIT_REPO}" "${CELEBORN_SOURCE_HOME}"
  fi

  echo "Checking out Celeborn ref ${CELEBORN_GIT_REF}"
  git -C "${CELEBORN_SOURCE_HOME}" fetch --all --tags
  git -C "${CELEBORN_SOURCE_HOME}" checkout "${CELEBORN_GIT_REF}"

  local current_ref
  current_ref=$(git -C "${CELEBORN_SOURCE_HOME}" rev-parse --short=7 HEAD)

  if [[ -f "${CELEBORN_ARCHIVE_REF_FILE}" ]]; then
    local built_ref
    built_ref=$(< "${CELEBORN_ARCHIVE_REF_FILE}")
    if [[ "${built_ref}" == "${current_ref}" ]]; then
      for candidate in "${CELEBORN_SOURCE_HOME}"/apache-celeborn-*-bin.tgz; do
        if [[ -f "${candidate}" ]]; then
          return 0
        fi
      done
    fi
  fi

  rm -f "${CELEBORN_SOURCE_HOME}"/apache-celeborn-*-bin.tgz

  echo "Building Celeborn distribution at ${CELEBORN_SOURCE_HOME}"
  # shellcheck disable=SC2086
  (cd "${CELEBORN_SOURCE_HOME}" && ./build/make-distribution.sh ${CELEBORN_BUILD_ARGS})
  echo "${current_ref}" > "${CELEBORN_ARCHIVE_REF_FILE}"
}

function ensure_lifecycle_manager_jar() {
  local helper_override=${BOLT_CELEBORN_LM_HELPER_JAR_PATH:-""}
  if [[ -n "${helper_override}" ]]; then
    if [[ ! -f "${helper_override}" ]]; then
      echo "Configured LM helper jar does not exist: ${helper_override}" >&2
      return 1
    fi
    LM_HELPER_JAR_PATH="${helper_override}"
    export LM_HELPER_JAR_PATH
    return
  fi

  for bundled in "${CELEBORN_HOME}/spark/celeborn-client-spark-"*.jar; do
    if [[ -f "${bundled}" ]]; then
      LM_HELPER_JAR_PATH="${bundled}"
      export LM_HELPER_JAR_PATH
      return
    fi
  done

  echo "LifecycleManager helper jar not found." >&2
  echo "Please provide BOLT_CELEBORN_LM_HELPER_JAR_PATH or ensure spark shaded client jar exists under ${CELEBORN_HOME}/spark." >&2
  return 1
}

function write_celeborn_conf() {
  mkdir -p "${CELEBORN_CONF_DIR}" "${CELEBORN_DATA_DIR}" "${CELEBORN_LOG_DIR}"

  cat > "${CELEBORN_CONF_DIR}/celeborn-env.sh" << EOF
#!/usr/bin/env bash
export CELEBORN_LOG_DIR=${CELEBORN_LOG_DIR}
EOF

  cat > "${CELEBORN_CONF_DIR}/celeborn-defaults.conf" << EOF
celeborn.master.host ${MASTER_HOST}
celeborn.master.port ${MASTER_PORT}
celeborn.master.endpoints ${MASTER_HOST}:${MASTER_PORT}
celeborn.worker.rpc.port ${WORKER_RPC_PORT}
celeborn.worker.push.port ${WORKER_PUSH_PORT}
celeborn.worker.fetch.port ${WORKER_FETCH_PORT}
celeborn.worker.replicate.port ${WORKER_REPLICATE_PORT}
celeborn.worker.storage.dirs ${CELEBORN_DATA_DIR}
celeborn.client.push.buffer.max.size 256K
celeborn.data.io.numConnectionsPerPeer 1
EOF

  chmod +x "${CELEBORN_CONF_DIR}/celeborn-env.sh"
}

function wait_for_port() {
  local host="$1"
  local port="$2"
  local timeout_sec="${3:-30}"
  local waited=0
  while ((waited < timeout_sec)); do
    if bash -c "</dev/tcp/${host}/${port}" > /dev/null 2>&1; then
      return 0
    fi
    sleep 1
    ((waited += 1))
  done
  return 1
}

stop_celeborn_java_processes() {
  if ! command -v jps > /dev/null 2>&1; then
    return
  fi

  local pids=()
  while read -r pid class_name _; do
    case "${class_name}" in
      org.apache.celeborn.service.deploy.master.Master | org.apache.celeborn.service.deploy.worker.Worker)
        pids+=("${pid}")
        ;;
    esac
  done < <(jps -l)

  if ((${#pids[@]} == 0)); then
    return
  fi

  echo "Stopping Celeborn Java processes: ${pids[*]}"
  kill "${pids[@]}" > /dev/null 2>&1 || true

  for _ in $(seq 1 10); do
    local running=0
    for pid in "${pids[@]}"; do
      if kill -0 "${pid}" > /dev/null 2>&1; then
        running=1
        break
      fi
    done
    if ((running == 0)); then
      return
    fi
    sleep 1
  done

  for pid in "${pids[@]}"; do
    if kill -0 "${pid}" > /dev/null 2>&1; then
      kill -9 "${pid}" > /dev/null 2>&1 || true
    fi
  done
}

function celeborn_master_endpoint() {
  echo "${MASTER_HOST}:${MASTER_PORT}"
}

stop_lifecycle_manager() {
  if [[ ! -f "${LM_PID_FILE}" ]]; then
    return
  fi

  local pid
  pid=$(cat "${LM_PID_FILE}")
  if ! kill -0 "${pid}" > /dev/null 2>&1; then
    rm -f "${LM_PID_FILE}" "${LM_ENDPOINT_FILE}" "${LM_STOP_FILE}"
    return
  fi

  touch "${LM_STOP_FILE}"
  for _ in $(seq 1 10); do
    if ! kill -0 "${pid}" > /dev/null 2>&1; then
      break
    fi
    sleep 1
  done

  if kill -0 "${pid}" > /dev/null 2>&1; then
    kill "${pid}" || true
  fi

  rm -f "${LM_PID_FILE}" "${LM_ENDPOINT_FILE}" "${LM_STOP_FILE}"
  echo "Stopped LifecycleManager helper"
}

stop_celeborn() {
  if [[ -d "${CELEBORN_HOME}" ]]; then
    if command -v java > /dev/null 2>&1 && [[ -n "${JAVA_HOME:-}" ]]; then
      "${CELEBORN_HOME}/sbin/stop-worker.sh" || true
      "${CELEBORN_HOME}/sbin/stop-master.sh" || true
    fi
  fi
  stop_celeborn_java_processes || true
  echo "Stopped Celeborn services"
}

start_celeborn() {
  require_java
  ensure_celeborn_binary
  stop_celeborn_java_processes
  clean_celeborn_runtime_state
  write_celeborn_conf

  echo "Starting Celeborn master"
  "${CELEBORN_HOME}/sbin/start-master.sh"

  if ! wait_for_port "${MASTER_HOST}" "${MASTER_PORT}" 60; then
    echo "Celeborn master failed to start on ${MASTER_HOST}:${MASTER_PORT}" >&2
    return 1
  fi

  echo "Starting Celeborn worker"
  "${CELEBORN_HOME}/sbin/start-worker.sh" "celeborn://${MASTER_HOST}:${MASTER_PORT}"

  sleep 3

  echo "Celeborn started"
  echo "master=$(celeborn_master_endpoint)"
  echo "worker_rpc_port=${WORKER_RPC_PORT}"
}

start_lifecycle_manager() {
  require_java
  ensure_celeborn_binary
  ensure_lifecycle_manager_jar

  if [[ -f "${LM_PID_FILE}" ]] && kill -0 "$(cat "${LM_PID_FILE}")" > /dev/null 2>&1; then
    echo "LifecycleManager helper already running with pid $(cat "${LM_PID_FILE}")"
    return
  fi

  rm -f "${LM_STOP_FILE}" "${LM_ENDPOINT_FILE}" "${LM_PID_FILE}"

  local helper_src="${CELEBORN_TEST_ROOT}/java/LifecycleManagerHelper.java"
  local classpath="${LM_HELPER_JAR_PATH}"
  for jar_dir in \
    "${CELEBORN_HOME}/jars" \
    "${CELEBORN_HOME}/master-jars" \
    "${CELEBORN_HOME}/worker-jars" \
    "${CELEBORN_HOME}/cli-jars"; do
    if [[ ! -d "${jar_dir}" ]]; then
      continue
    fi
    for jar in "${jar_dir}"/*.jar; do
      if [[ ! -f "${jar}" || "${jar}" == "${LM_HELPER_JAR_PATH}" ]]; then
        continue
      fi
      classpath="${classpath}:${jar}"
    done
  done

  echo "Starting LifecycleManager helper"
  java -cp "${classpath}" "${helper_src}" "$(celeborn_master_endpoint)" "${LM_APP_ID}" "${LM_ENDPOINT_FILE}" "${LM_STOP_FILE}" > "${CELEBORN_LOG_DIR}/lifecycle_manager.log" 2>&1 &
  echo $! > "${LM_PID_FILE}"

  for _ in $(seq 1 30); do
    if [[ -s "${LM_ENDPOINT_FILE}" ]]; then
      echo "LifecycleManager endpoint: $(cat "${LM_ENDPOINT_FILE}")"
      return
    fi
    sleep 1
  done

  echo "LifecycleManager helper failed to publish endpoint" >&2
  return 1
}

healthcheck_celeborn() {
  if ! wait_for_port "${MASTER_HOST}" "${MASTER_PORT}" 1; then
    echo "master port not ready: ${MASTER_HOST}:${MASTER_PORT}" >&2
    return 1
  fi

  local waited=0
  until ls "${CELEBORN_LOG_DIR}"/celeborn-*-org.apache.celeborn.service.deploy.worker.Worker-*.out > /dev/null 2>&1; do
    if ((waited >= 30)); then
      echo "worker log not found under ${CELEBORN_LOG_DIR}" >&2
      return 1
    fi
    sleep 1
    ((waited += 1))
  done

  waited=0
  until grep -q "Worker started\." "${CELEBORN_LOG_DIR}"/celeborn-*-org.apache.celeborn.service.deploy.worker.Worker-*.out; do
    if ((waited >= 60)); then
      echo "worker not fully started yet" >&2
      return 1
    fi
    sleep 1
    ((waited += 1))
  done

  echo "Celeborn healthcheck passed"
  echo "master=$(celeborn_master_endpoint)"
}

cleanup() {
  stop_lifecycle_manager || true
  stop_celeborn || true
}
trap cleanup EXIT

start_celeborn
healthcheck_celeborn
start_lifecycle_manager

if [[ ! -d "${BUILD_DIR}" ]]; then
  echo "Build directory does not exist: ${BUILD_DIR}" >&2
  exit 1
fi

TEST_PATTERNS=(
  "bolt_shuffle_spark_celeborn_e2e_test"
  "bolt_shuffle_spark_matrix_test"
  "bolt_shuffle_spark_large_partition_test"
  "bolt_shuffle_spark_memory_test"
)
CTEST_TIMEOUT_SECONDS=${BOLT_CELEBORN_CTEST_TIMEOUT_SECONDS:-7200}

FAILED_PATTERNS=()
for test_pattern in "${TEST_PATTERNS[@]}"; do
  echo "Running ctest pattern: ${test_pattern}"
  if ! BOLT_CELEBORN_E2E=1 \
    BOLT_SHUFFLE_TEST_REAL_CELEBORN=1 \
    BOLT_CELEBORN_LM_ENDPOINT_FILE="${LM_ENDPOINT_FILE}" \
    BOLT_CELEBORN_LM_APP_ID="${LM_APP_ID}" \
    ctest --test-dir "${BUILD_DIR}" --output-on-failure --timeout "${CTEST_TIMEOUT_SECONDS}" -R "${test_pattern}"; then
    FAILED_PATTERNS+=("${test_pattern}")
  fi
done

if ((${#FAILED_PATTERNS[@]} > 0)); then
  echo "Failing test patterns: ${FAILED_PATTERNS[*]}" >&2
  exit 1
fi
