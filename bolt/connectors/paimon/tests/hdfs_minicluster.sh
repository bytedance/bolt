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

set -euo pipefail

usage() {
  cat << 'EOF'
Usage:
  hdfs_minicluster.sh start --name <container_name> --timeout-seconds <N>
  hdfs_minicluster.sh stop  --name <container_name>

This script manages an HDFS+YARN minicluster container for tests.
Supported container engines: docker, podman (auto-detected).

Start mode uses host networking only.
EOF
}

die() {
  echo "[hdfs_minicluster] ERROR: $*" >&2
  exit 1
}

detect_engine() {
  if command -v docker > /dev/null 2>&1; then
    if docker --version > /dev/null 2>&1; then
      echo docker
      return
    fi
  fi
  if command -v podman > /dev/null 2>&1; then
    if podman --version > /dev/null 2>&1; then
      echo podman
      return
    fi
  fi
  die "Neither docker nor podman is available in PATH"
}

IMAGE="docker.io/apache/hadoop:3.4.3"
NN_RPC_PORT=7878
NN_HTTP_PORT=7676

ensure_image_pulled() {
  local engine="$1"
  if "$engine" image inspect "$IMAGE" > /dev/null 2>&1; then
    return
  fi
  echo "[hdfs_minicluster] Pulling image $IMAGE..." >&2
  "$engine" pull "$IMAGE" > /dev/null
}

cleanup_container() {
  local engine="$1"
  local name="$2"
  "$engine" rm -f "$name" > /dev/null 2>&1 || true
}

wait_ready() {
  local engine="$1"
  local name="$2"
  local timeout_seconds="$3"

  local deadline=$(($(date +%s) + timeout_seconds))
  local check_cmd=("$engine" exec "$name" hdfs dfs -ls "hdfs://localhost:${NN_RPC_PORT}/")

  while [ "$(date +%s)" -lt "$deadline" ]; do
    if "${check_cmd[@]}" > /dev/null 2>&1; then
      echo "[hdfs_minicluster] Ready" >&2
      return
    fi
    sleep 0.25
  done

  echo "[hdfs_minicluster] Timed out waiting for readiness" >&2
  "$engine" logs "$name" >&2 || true
  return 1
}

cmd_start() {
  local name=""
  local timeout_seconds="60"

  while [ "$#" -gt 0 ]; do
    case "$1" in
      --name)
        name="$2"
        shift 2
        ;;
      --timeout-seconds)
        timeout_seconds="$2"
        shift 2
        ;;
      -h | --help)
        usage
        exit 0
        ;;
      *)
        die "Unknown argument for start: $1"
        ;;
    esac
  done

  [ -n "$name" ] || die "Missing --name"

  local engine
  engine="$(detect_engine)"

  ensure_image_pulled "$engine"
  cleanup_container "$engine" "$name"

  echo "[hdfs_minicluster] Starting container '$name' with engine '$engine'..." >&2
  "$engine" run --rm -d --name "$name" --network host \
    "$IMAGE" mapred minicluster \
    -format -nomr \
    -nnhttpport "$NN_HTTP_PORT" -nnport "$NN_RPC_PORT" \
    -D dfs.permissions=false > /dev/null

  if ! wait_ready "$engine" "$name" "$timeout_seconds"; then
    cleanup_container "$engine" "$name"
    die "Failed to start/validate HDFS minicluster"
  fi
}

cmd_stop() {
  local name=""
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --name)
        name="$2"
        shift 2
        ;;
      -h | --help)
        usage
        exit 0
        ;;
      *)
        die "Unknown argument for stop: $1"
        ;;
    esac
  done

  [ -n "$name" ] || die "Missing --name"

  local engine
  engine="$(detect_engine)"
  cleanup_container "$engine" "$name"
}

main() {
  if [ "$#" -lt 1 ]; then
    usage
    exit 2
  fi

  local subcmd="$1"
  shift
  case "$subcmd" in
    start) cmd_start "$@" ;;
    stop) cmd_stop "$@" ;;
    -h | --help | help)
      usage
      exit 0
      ;;
    *) die "Unknown subcommand: $subcmd" ;;
  esac
}

main "$@"
