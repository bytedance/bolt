#!/usr/bin/env bash
# Copyright (c) ByteDance Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/run_bm_row_container_benchmark_common.sh"

readonly DEFAULT_BINARY="${script_dir}/../../../../_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_complex_benchmark"
readonly DEFAULT_DATA_BYTES="134217728"
readonly DEFAULT_TIMEOUT_SECONDS="300"

binary="${DEFAULT_BINARY}"
data_bytes="${DEFAULT_DATA_BYTES}"
# The common runner expects this variable, but complex-operation cases do not
# implement a warm-up phase.
warmup_data_bytes="0"
output_dir="${script_dir}/../../../../log/bolt-bm-complex-$(date +%Y%m%d-%H%M%S)"
pre_sleep_seconds="0"
post_sleep_seconds="0"
timeout_seconds="${DEFAULT_TIMEOUT_SECONDS}"
include_regex=""
exclude_regex=""
drop_cache="0"
list_only="0"
extra_args=()

usage() {
  cat << 'EOF'
Usage:
  run_bm_row_container_complex_benchmark.sh [options] [-- extra benchmark args]

Options:
  --binary PATH              Complex operation benchmark binary to run.
  --output-dir DIR           Directory for stdout.txt and stderr.txt.
  --data-bytes BYTES         Logical bytes per case (default: 128 MiB).
  --timeout-seconds N        Per-case timeout (default: 300).
  --include-regex REGEX      Run only matching benchmark cases.
  --exclude-regex REGEX      Skip matching benchmark cases.
  --list-only                Print selected benchmark cases and exit.
  --help                     Print this message.

Extra Folly benchmark flags after "--" are passed to the binary.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary)
      require_value "$1" "${2:-}"
      binary="$2"
      shift 2
      ;;
    --output-dir)
      require_value "$1" "${2:-}"
      output_dir="$2"
      shift 2
      ;;
    --data-bytes)
      require_value "$1" "${2:-}"
      data_bytes="$2"
      shift 2
      ;;
    --timeout-seconds)
      require_value "$1" "${2:-}"
      timeout_seconds="$2"
      shift 2
      ;;
    --include-regex)
      require_value "$1" "${2:-}"
      include_regex="$2"
      shift 2
      ;;
    --exclude-regex)
      require_value "$1" "${2:-}"
      exclude_regex="$2"
      shift 2
      ;;
    --list-only)
      list_only="1"
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    --)
      shift
      extra_args=("$@")
      break
      ;;
    *)
      die "unknown option: $1"
      ;;
  esac
done

run_bm_row_container_cases
