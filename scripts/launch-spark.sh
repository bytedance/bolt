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
# One-click launcher for Spark + Gluten + Bolt.
#
# Builds Bolt (this repo), downloads Spark if needed, locates a pre-built
# Gluten bundle JAR at $GLUTEN_HOME, and starts a long-lived Spark Connect
# Server in standalone mode (one master + one worker JVM).
#
# Subcommands: start / stop / status / sql / shell

set -euo pipefail

# ----------------------------------------------------------------------------
# Paths & env.
# ----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null && pwd)"
BOLT_HOME="${BOLT_HOME:-$(cd "$SCRIPT_DIR/.." && pwd)}"

SPARK_VERSION="${SPARK_VERSION:-3.5.5}"
BOLT_BUILD_VERSION="${BOLT_BUILD_VERSION:-main}"
TMP_ROOT="${TMP_ROOT:-/tmp/spark-bolt}"
SPARK_HOME="${SPARK_HOME:-$TMP_ROOT/spark-${SPARK_VERSION}-bin-hadoop3}"
GLUTEN_HOME="${GLUTEN_HOME:-$TMP_ROOT/gluten}"
GLUTEN_FORK_URL="${GLUTEN_FORK_URL:-https://github.com/WangGuangxin/gluten.git}"
GLUTEN_FORK_BRANCH="${GLUTEN_FORK_BRANCH:-add_bolt_backend}"
BUILD=0

# Single-executor sizing. All overridable via env.
EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-2g}"
OFFHEAP_SIZE="${OFFHEAP_SIZE:-8g}"
DRIVER_MEMORY="${DRIVER_MEMORY:-2g}"
SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-256}"

# Ports. 21xxx range to avoid clashing with stock Spark defaults.
CONNECT_PORT="${CONNECT_PORT:-21002}"
SPARK_UI_PORT="${SPARK_UI_PORT:-21040}"
MASTER_RPC_PORT="${MASTER_RPC_PORT:-21077}"
MASTER_UI_PORT="${MASTER_UI_PORT:-21088}"
WORKER_UI_PORT="${WORKER_UI_PORT:-21089}"

EVENT_LOG_DIR="$TMP_ROOT/eventlogs"
HIVE_METASTORE_DIR="$TMP_ROOT/metastore_db"
HIVE_WAREHOUSE_DIR="$TMP_ROOT/warehouse"

PID_FILE="$TMP_ROOT/connect.pid"
MASTER_PID_FILE="$TMP_ROOT/master.pid"
WORKER_PID_FILE="$TMP_ROOT/worker.pid"
BOLT_MARKER="$BOLT_HOME/_build/.spark_exported"

PYSPARK_VENV="${PYSPARK_VENV:-$TMP_ROOT/pyenv-3.11}"
VENV_PYTHON="$PYSPARK_VENV/bin/python"

# Spark sbin scripts use $USER to construct PID-file names; export a sane
# default so we run cleanly in minimal containers / non-login shells.
export USER="${USER:-$(id -un)}"
export SPARK_PID_DIR="$TMP_ROOT/pids"
export SPARK_LOG_DIR="$TMP_ROOT/logs"

GLUTEN_JAR=""
GLUTEN_BACKEND=""

# ----------------------------------------------------------------------------
# Logging.
# ----------------------------------------------------------------------------
if [[ -t 1 ]]; then
  C_RESET=$'\033[0m'
  C_RED=$'\033[31m'
  C_YEL=$'\033[33m'
  C_BLU=$'\033[34m'
  C_GRN=$'\033[32m'
else
  C_RESET=""
  C_RED=""
  C_YEL=""
  C_BLU=""
  C_GRN=""
fi
log() { echo "${C_BLU}==>${C_RESET} $*"; }
warn() { echo "${C_YEL}WARN:${C_RESET} $*" >&2; }
die() {
  echo "${C_RED}ERROR:${C_RESET} $*" >&2
  exit 1
}
ok() { echo "${C_GRN}OK:${C_RESET} $*"; }

usage() {
  cat << EOF
Usage: $(basename "$0") <start|stop|status|sql|shell> [args]

Subcommands:
  start [--build]   Start standalone master + 1 worker + Connect Server.
                    Restarts if running. --build rebuilds Bolt + Gluten first.
  stop              Stop Connect Server, worker, master.
  status            Print running PIDs and URLs.
  sql               pyspark Connect SQL client. Forwards args to connect.py
                    (e.g. -e SQL, -f FILE, --vanilla).
  shell             pyspark Connect Python REPL.

Environment variables (defaults shown):
  EXECUTOR_CORES      $EXECUTOR_CORES        spark.executor.cores
  EXECUTOR_MEMORY     $EXECUTOR_MEMORY       spark.executor.memory (heap)
  OFFHEAP_SIZE        $OFFHEAP_SIZE          spark.memory.offHeap.size
  DRIVER_MEMORY       $DRIVER_MEMORY         spark.driver.memory
  SHUFFLE_PARTITIONS  $SHUFFLE_PARTITIONS    spark.sql.shuffle.partitions
  GLUTEN_HOME         $GLUTEN_HOME (auto-cloned + built if missing)
  GLUTEN_FORK_URL     $GLUTEN_FORK_URL
  GLUTEN_FORK_BRANCH  $GLUTEN_FORK_BRANCH
  SPARK_HOME          $SPARK_HOME (auto-downloaded if missing)
  TMP_ROOT            $TMP_ROOT
  Ports               CONNECT=$CONNECT_PORT UI=$SPARK_UI_PORT MASTER_RPC=$MASTER_RPC_PORT \\
                      MASTER_UI=$MASTER_UI_PORT WORKER_UI=$WORKER_UI_PORT
EOF
}

# ----------------------------------------------------------------------------
# Toolchain setup.
# ----------------------------------------------------------------------------
_is_jdk17() {
  [[ -x "$1/bin/java" && -x "$1/bin/javac" ]] || return 1
  "$1/bin/java" -version 2>&1 | head -n1 | grep -qE 'version "17[. ]'
}
detect_jdk17() {
  if [[ -n "${JAVA_HOME:-}" ]] && _is_jdk17 "$JAVA_HOME"; then
    export PATH="$JAVA_HOME/bin:$PATH"
    return
  fi
  local c
  for c in /usr/lib/jvm/{java-17-openjdk-amd64,java-17-openjdk,temurin-17-jdk-amd64,temurin-17-jdk} \
    /usr/lib/jvm/{java-17-*,temurin-17-*} /opt/java/openjdk-17*; do
    if _is_jdk17 "$c"; then
      export JAVA_HOME="$c"
      export PATH="$JAVA_HOME/bin:$PATH"
      log "Auto-detected JDK 17: $JAVA_HOME"
      return
    fi
  done
  warn "JDK 17 not found. Install OpenJDK 17 or set JAVA_HOME."
}

preflight() {
  detect_jdk17
  local missing=() cmd
  for cmd in java mvn cmake ninja curl unzip git; do
    command -v "$cmd" > /dev/null 2>&1 || missing+=("$cmd")
  done
  [[ ${#missing[@]} -eq 0 ]] || die "Missing tools: ${missing[*]}. Run scripts/setup-dev-env.sh first."
}

ensure_pyspark_venv() {
  command -v uv > /dev/null 2>&1 \
    || die "uv not found. Install: curl -LsSf https://astral.sh/uv/install.sh | sh"
  if [[ ! -x "$VENV_PYTHON" ]]; then
    log "Creating Python 3.11 venv at $PYSPARK_VENV"
    uv venv --python 3.11 "$PYSPARK_VENV"
  fi
  local installed
  installed="$("$VENV_PYTHON" -c 'import pyspark; print(pyspark.__version__)' 2> /dev/null || true)"
  if [[ "$installed" != "$SPARK_VERSION" ]]; then
    log "Installing pyspark==$SPARK_VERSION into venv"
    uv pip install --python "$VENV_PYTHON" --quiet \
      "pyspark==$SPARK_VERSION" grpcio grpcio-status googleapis-common-protos \
      packaging pandas pyarrow
  fi
}

ensure_spark() {
  [[ -x "$SPARK_HOME/bin/spark-shell" ]] && return
  if ! command -v aria2c > /dev/null 2>&1; then
    warn "aria2c not found; falling back to curl (the Spark archive download"
    warn "will be ~10x slower over archive.apache.org)."
    warn "Install: sudo apt install -y aria2  (or 'brew install aria2' on macOS)"
  fi
  mkdir -p "$TMP_ROOT"
  local tgz_name="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
  local tgz="$TMP_ROOT/$tgz_name" url
  for url in \
    "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${tgz_name}" \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${tgz_name}"; do
    log "Downloading $url"
    if command -v aria2c > /dev/null 2>&1; then
      aria2c -x 16 -s 16 --max-tries=5 --retry-wait=5 --continue=true \
        --allow-overwrite=true --console-log-level=warn \
        -d "$TMP_ROOT" -o "$tgz_name" "$url" && break
    else
      curl -fL --retry 5 --retry-delay 5 -C - -o "$tgz" "$url" && break
    fi
    rm -f "$tgz"
  done
  [[ -s "$tgz" ]] || die "All Spark download mirrors failed for $SPARK_VERSION"
  tar -xzf "$tgz" -C "$TMP_ROOT/" && rm -f "$tgz"
  [[ -x "$SPARK_HOME/bin/spark-shell" ]] \
    || die "Spark extraction did not produce $SPARK_HOME/bin/spark-shell"
}

# Clone Gluten fork if $GLUTEN_HOME isn't a checkout yet, build Bolt + Gluten
# as needed (on --build, or when their artifacts are missing), then locate
# the bundle JAR.
_find_gluten_jar() {
  # Prefer the Bolt-flavored bundle. Fall back to a Velox bundle if no Bolt
  # bundle exists in the checkout — that's how you point this launcher at a
  # pre-built Velox Gluten for backend comparison runs.
  local tag jar
  for tag in bolt velox; do
    jar=$(find "$GLUTEN_HOME" -type f \
      -path "*/target/gluten*${tag}*bundle*spark3.5*.jar" \
      ! -name '*-original.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' \
      -printf '%T@ %p\n' 2> /dev/null | sort -rn | head -n1 | cut -d' ' -f2-)
    if [[ -n "$jar" ]]; then
      echo "$jar"
      return
    fi
  done
}

ensure_gluten_jar() {
  if [[ ! -d "$GLUTEN_HOME/.git" ]]; then
    log "Cloning Gluten ($GLUTEN_FORK_BRANCH) into $GLUTEN_HOME"
    mkdir -p "$(dirname "$GLUTEN_HOME")"
    git clone --depth 1 -b "$GLUTEN_FORK_BRANCH" "$GLUTEN_FORK_URL" "$GLUTEN_HOME"
  fi

  if [[ $BUILD -eq 1 || ! -f "$BOLT_MARKER" ]]; then
    log "Building Bolt (make release_spark && make export_release)"
    (cd "$BOLT_HOME" && make release_spark BUILD_VERSION="$BOLT_BUILD_VERSION")
    (cd "$BOLT_HOME" && make export_release BUILD_VERSION="$BOLT_BUILD_VERSION")
    touch "$BOLT_MARKER"
  fi

  GLUTEN_JAR="$(_find_gluten_jar)"
  if [[ $BUILD -eq 1 || -z "$GLUTEN_JAR" ]]; then
    log "Building Gluten (make release && make arrow && make jar_spark35)"
    (cd "$GLUTEN_HOME" && make release)
    (cd "$GLUTEN_HOME" && make arrow)
    (cd "$GLUTEN_HOME" && make jar_spark35)
    GLUTEN_JAR="$(_find_gluten_jar)"
  fi

  [[ -n "$GLUTEN_JAR" ]] \
    || die "No Gluten bundle JAR under $GLUTEN_HOME. Run with --build to build it."
  # Detect which backend's native lib is shaded into the jar. Use bash
  # substring match (no pipe) so 'grep -q' SIGPIPE'ing echo under
  # set -o pipefail doesn't false-negative the check.
  local listing
  listing="$(unzip -l "$GLUTEN_JAR" 2> /dev/null || true)"
  if [[ "$listing" == *libbolt_backend.so* ]]; then
    GLUTEN_BACKEND=bolt
  elif [[ "$listing" == *libvelox.so* || "$listing" == *libgluten.so* ]]; then
    GLUTEN_BACKEND=velox
  else
    die "Gluten JAR $GLUTEN_JAR does not contain a known native backend lib (libbolt_backend.so / libvelox.so / libgluten.so)."
  fi
  ok "Gluten JAR: $GLUTEN_JAR (backend=$GLUTEN_BACKEND)"
}

prepare_env() {
  mkdir -p "$TMP_ROOT" "$SPARK_PID_DIR" "$SPARK_LOG_DIR" "$EVENT_LOG_DIR" \
    "$HIVE_METASTORE_DIR" "$HIVE_WAREHOUSE_DIR"
  preflight
  ensure_pyspark_venv
  ensure_spark
  ensure_gluten_jar
}

# ----------------------------------------------------------------------------
# Spark conf shared by master/worker/Connect server.
# ----------------------------------------------------------------------------
COMMON_CONF=()
build_common_conf() {
  # spark-connect jar from Ivy cache. In standalone mode it must be on the
  # executor's bootstrap classpath (--jars alone is too late — see comment
  # in stop history for context). Resolved by --packages on first run.
  local conn_jar="$HOME/.ivy2/cache/org.apache.spark/spark-connect_2.12/jars/spark-connect_2.12-$SPARK_VERSION.jar"
  COMMON_CONF=(
    --master "spark://localhost:$MASTER_RPC_PORT"
    --driver-memory "$DRIVER_MEMORY"
    --executor-memory "$EXECUTOR_MEMORY"
    --conf "spark.executor.cores=$EXECUTOR_CORES"
    --conf "spark.cores.max=$EXECUTOR_CORES"
    --conf "spark.sql.shuffle.partitions=$SHUFFLE_PARTITIONS"
    --conf "spark.sql.session.timeZone=UTC"
    --conf "spark.ui.enabled=true"
    --conf "spark.ui.port=$SPARK_UI_PORT"
    --conf "spark.eventLog.enabled=true"
    --conf "spark.eventLog.dir=file://$EVENT_LOG_DIR"
    --conf "spark.plugins=org.apache.gluten.GlutenPlugin"
    --conf "spark.memory.offHeap.enabled=true"
    --conf "spark.memory.offHeap.size=$OFFHEAP_SIZE"
    --conf "spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager"
    # Fail-fast on native crashes so SIGSEGV (exit 134) surfaces as the
    # original ExecutorLostFailure instead of MetadataFetchFailedException.
    --conf "spark.task.maxFailures=1"
    --conf "spark.stage.maxConsecutiveAttempts=1"
    # Pin user.timezone=UTC; Bolt's tz DB reads JVM user.timezone and chokes
    # on legacy aliases like Africa/Asmera.
    --conf "spark.driver.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true -Duser.timezone=UTC"
    --conf "spark.executor.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true -Duser.timezone=UTC"
    --conf "spark.driver.extraClassPath=$GLUTEN_JAR"
    --conf "spark.executor.extraClassPath=$GLUTEN_JAR:$conn_jar"
    # Persistent Derby Hive catalog. Tables + DDL survive restarts.
    --conf "spark.sql.catalogImplementation=hive"
    --conf "spark.sql.warehouse.dir=$HIVE_WAREHOUSE_DIR"
    --conf "javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$HIVE_METASTORE_DIR;create=true"
    --conf "datanucleus.schema.autoCreateAll=true"
  )
}

# ----------------------------------------------------------------------------
# PID helpers.
# ----------------------------------------------------------------------------
_pid_alive() { [[ -n "${1:-}" ]] && kill -0 "$1" 2> /dev/null; }
_read_pid() { [[ -f "$1" ]] && cat "$1" 2> /dev/null || true; }

_spark_pid() { # echo Spark-written PID file path for class suffix $1
  ls "$SPARK_PID_DIR"/spark-"$USER"-org.apache.spark.*"$1"*.pid 2> /dev/null | head -n1
}
_track_spark_pid() { # wait for Spark sbin to write PID, copy to $2
  local cls="$1" dst="$2" src="" i
  for i in $(seq 1 20); do
    src="$(_spark_pid "$cls")"
    [[ -n "$src" ]] && break
    sleep 0.5
  done
  [[ -n "$src" ]] || return 1
  cp "$src" "$dst"
}

# ----------------------------------------------------------------------------
# Standalone master + single worker lifecycle.
# ----------------------------------------------------------------------------
start_master() {
  local cur
  cur="$(_read_pid "$MASTER_PID_FILE")"
  _pid_alive "$cur" && {
    log "Master already running (PID $cur)"
    return 0
  }
  log "Starting standalone master (RPC :$MASTER_RPC_PORT, UI :$MASTER_UI_PORT)"
  SPARK_MASTER_HOST=localhost \
    SPARK_MASTER_PORT="$MASTER_RPC_PORT" \
    SPARK_MASTER_WEBUI_PORT="$MASTER_UI_PORT" \
    "$SPARK_HOME/sbin/start-master.sh" > /dev/null 2>&1 \
    || die "start-master.sh failed; check $SPARK_LOG_DIR"
  _track_spark_pid Master "$MASTER_PID_FILE" \
    || die "Master PID file not found in $SPARK_PID_DIR"
  ok "Master PID $(cat "$MASTER_PID_FILE")"
}

start_worker() {
  local cur
  cur="$(_read_pid "$WORKER_PID_FILE")"
  _pid_alive "$cur" && {
    log "Worker already running (PID $cur)"
    return 0
  }
  log "Starting worker ($EXECUTOR_CORES cores × $EXECUTOR_MEMORY heap + $OFFHEAP_SIZE offheap)"
  SPARK_WORKER_CORES="$EXECUTOR_CORES" \
    SPARK_WORKER_MEMORY="$EXECUTOR_MEMORY" \
    SPARK_WORKER_WEBUI_PORT="$WORKER_UI_PORT" \
    "$SPARK_HOME/sbin/start-worker.sh" \
    "spark://localhost:$MASTER_RPC_PORT" > /dev/null 2>&1 \
    || die "start-worker.sh failed; check $SPARK_LOG_DIR"
  _track_spark_pid Worker "$WORKER_PID_FILE" \
    || die "Worker PID file not found in $SPARK_PID_DIR"
  ok "Worker PID $(cat "$WORKER_PID_FILE")"
}

stop_worker() {
  local cur
  cur="$(_read_pid "$WORKER_PID_FILE")"
  if _pid_alive "$cur"; then
    "$SPARK_HOME/sbin/stop-worker.sh" > /dev/null 2>&1 || kill "$cur" 2> /dev/null || true
    ok "Worker stopped."
  fi
  rm -f "$WORKER_PID_FILE"
}
stop_master() {
  local cur
  cur="$(_read_pid "$MASTER_PID_FILE")"
  if _pid_alive "$cur"; then
    "$SPARK_HOME/sbin/stop-master.sh" > /dev/null 2>&1 || kill "$cur" 2> /dev/null || true
    ok "Master stopped."
  fi
  rm -f "$MASTER_PID_FILE"
}

# ----------------------------------------------------------------------------
# Connect Server lifecycle.
# ----------------------------------------------------------------------------
_probe_connect() { # wait up to 60s for pyspark `select 1` to succeed
  "$VENV_PYTHON" - << EOF
import time
from pyspark.sql import SparkSession
for _ in range(60):
    try:
        s = SparkSession.builder.remote("sc://localhost:$CONNECT_PORT").getOrCreate()
        s.sql("select 1").collect()
        import sys; sys.exit(0)
    except Exception:
        time.sleep(1)
import sys; sys.exit(1)
EOF
}

start_connect() {
  local cur
  cur="$(_read_pid "$PID_FILE")"
  if _pid_alive "$cur"; then
    log "Connect Server already running (PID $cur); restarting"
    stop_connect
  fi
  log "Starting Spark Connect Server on port $CONNECT_PORT"
  # spark-connect_2.12 jar must be on executor classpath in standalone; ship it
  # via --jars + --packages so workers receive it through Spark's file server.
  local conn_jar="$HOME/.ivy2/cache/org.apache.spark/spark-connect_2.12/jars/spark-connect_2.12-$SPARK_VERSION.jar"
  local jars_csv="$GLUTEN_JAR"
  [[ -f "$conn_jar" ]] && jars_csv="$GLUTEN_JAR,$conn_jar"
  "$SPARK_HOME/sbin/start-connect-server.sh" \
    --packages "org.apache.spark:spark-connect_2.12:$SPARK_VERSION" \
    "${COMMON_CONF[@]}" \
    --jars "$jars_csv" \
    --conf "spark.connect.grpc.binding.port=$CONNECT_PORT" \
    > /dev/null 2>&1 \
    || die "start-connect-server.sh failed; check $SPARK_LOG_DIR"
  _track_spark_pid SparkConnectServer "$PID_FILE" \
    || die "Connect Server PID file not found"
  log "Probing Connect Server (up to 60s)..."
  _probe_connect \
    || {
      tail -n 50 "$SPARK_LOG_DIR"/*SparkConnectServer*.out 2> /dev/null
      die "Probe failed."
    }
  ok "Connect Server PID $(cat "$PID_FILE")."
}

stop_connect() {
  local cur
  cur="$(_read_pid "$PID_FILE")"
  if _pid_alive "$cur"; then
    "$SPARK_HOME/sbin/stop-connect-server.sh" > /dev/null 2>&1 || kill "$cur" 2> /dev/null || true
    ok "Connect Server stopped."
  fi
  rm -f "$PID_FILE"
}

# ----------------------------------------------------------------------------
# Subcommands.
# ----------------------------------------------------------------------------
do_start() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --build)
        BUILD=1
        shift
        ;;
      *) die "unknown flag for start: $1 (only --build is accepted)" ;;
    esac
  done
  prepare_env
  build_common_conf
  start_master
  start_worker
  start_connect
  echo "    Connect:   sc://localhost:$CONNECT_PORT"
  echo "    Master UI: http://localhost:$MASTER_UI_PORT"
  echo "    Worker UI: http://localhost:$WORKER_UI_PORT"
  echo "    Spark UI:  http://localhost:$SPARK_UI_PORT"
  echo "    Logs:      $SPARK_LOG_DIR"
}

do_stop() {
  stop_connect
  stop_worker
  stop_master
}

do_status() {
  local p
  p="$(_read_pid "$PID_FILE")"
  if _pid_alive "$p"; then echo "connect : running pid=$p grpc=sc://localhost:$CONNECT_PORT ui=http://localhost:$SPARK_UI_PORT"; else echo "connect : not running"; fi
  p="$(_read_pid "$WORKER_PID_FILE")"
  if _pid_alive "$p"; then echo "worker  : running pid=$p ui=http://localhost:$WORKER_UI_PORT"; else echo "worker  : not running"; fi
  p="$(_read_pid "$MASTER_PID_FILE")"
  if _pid_alive "$p"; then echo "master  : running pid=$p rpc=spark://localhost:$MASTER_RPC_PORT ui=http://localhost:$MASTER_UI_PORT"; else echo "master  : not running"; fi
  local n=0
  [[ -d "$EVENT_LOG_DIR" ]] && n="$(ls -1 "$EVENT_LOG_DIR" 2> /dev/null | wc -l)"
  echo "events  : $EVENT_LOG_DIR ($n entries)"
  echo "logs    : $SPARK_LOG_DIR"
}

_ensure_running() {
  local cur
  cur="$(_read_pid "$PID_FILE")"
  if ! _pid_alive "$cur"; then
    log "Connect Server not running; starting it now"
    do_start
    cur="$(_read_pid "$PID_FILE")"
  else
    [[ -d "$SPARK_HOME" ]] || prepare_env
    [[ -x "$VENV_PYTHON" ]] || ensure_pyspark_venv
  fi
  echo "$cur"
}

do_sql() {
  local pid
  pid="$(_ensure_running)"
  # CONNECT_SERVER_PID enables connect.py's JVM-death watchdog.
  CONNECT_SERVER_PID="$pid" SPARK_LOG_DIR="$SPARK_LOG_DIR" \
    exec "$VENV_PYTHON" "$SCRIPT_DIR/launch-spark/connect.py" sql \
    --remote "sc://localhost:$CONNECT_PORT" "$@"
}

do_shell() {
  local pid
  pid="$(_ensure_running)"
  CONNECT_SERVER_PID="$pid" SPARK_LOG_DIR="$SPARK_LOG_DIR" \
    exec "$VENV_PYTHON" "$SCRIPT_DIR/launch-spark/connect.py" shell \
    --remote "sc://localhost:$CONNECT_PORT" "$@"
}

# ----------------------------------------------------------------------------
# Main.
# ----------------------------------------------------------------------------
case "${1:-}" in
  -h | --help | "")
    usage
    exit 0
    ;;
  start)
    shift
    do_start "$@"
    ;;
  stop)
    shift
    do_stop
    ;;
  status)
    shift
    do_status
    ;;
  sql)
    shift
    do_sql "$@"
    ;;
  shell)
    shift
    do_shell "$@"
    ;;
  *) die "unknown subcommand: $1 (try --help)" ;;
esac
