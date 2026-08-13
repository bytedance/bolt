#!/usr/bin/env bash

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/run_bm_row_container_benchmark_common.sh"

readonly DEFAULT_BINARY="/data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_benchmark"
readonly DEFAULT_DATA_BYTES="26843545600"
readonly DEFAULT_WARMUP_DATA_BYTES="134217728"
readonly DEFAULT_PRE_SLEEP_SECONDS="10"
readonly DEFAULT_POST_SLEEP_SECONDS="10"
readonly DEFAULT_TIMEOUT_SECONDS="900"

binary="${DEFAULT_BINARY}"
data_bytes="${DEFAULT_DATA_BYTES}"
warmup_data_bytes="${DEFAULT_WARMUP_DATA_BYTES}"
output_dir="/data00/home/wangxinshuo.db/bolt/log/bolt-bm-row-container-pipeline-$(date +%Y%m%d-%H%M%S)"
pre_sleep_seconds="${DEFAULT_PRE_SLEEP_SECONDS}"
post_sleep_seconds="${DEFAULT_POST_SLEEP_SECONDS}"
timeout_seconds="${DEFAULT_TIMEOUT_SECONDS}"
include_regex=""
exclude_regex=""
drop_cache="1"
list_only="0"
extra_args=()

usage() {
  cat <<'EOF'
Usage:
  run_bm_row_container_pipeline_benchmark.sh [options] [-- extra benchmark args]

Options:
  --binary PATH              Benchmark binary to run.
                             Default: _build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_benchmark
  --output-dir DIR           Directory for stdout.txt and stderr.txt.
                             Default: /data00/home/wangxinshuo.db/bolt/log/bolt-bm-row-container-pipeline-YYYYmmdd-HHMMSS
  --data-bytes BYTES         Value for --bm_row_container_data_bytes.
                             Default: 26843545600
  --warmup-data-bytes BYTES  Value for --bm_row_container_warmup_data_bytes.
                             0 disables same-process per-case warm-up.
                             Default: 134217728
  --pre-sleep-seconds N      Sleep after dropping cache and before each case.
                             Default: 10
  --post-sleep-seconds N     Sleep after each case's final sync.
                             Default: 10
  --timeout-seconds N        Per-case timeout.
                             Default: 900
  --include-regex REGEX      Run only benchmark cases matching this bash regex.
  --exclude-regex REGEX      Skip benchmark cases matching this bash regex.
  --list-only                Print selected benchmark cases and exit.
  --no-drop-cache            Skip sudo drop_caches. Useful for smoke tests.
  --help                     Print this message.

Any arguments after "--" are passed to the benchmark binary.
String profile flags can be passed after "--":
  --bm_row_container_variable_max_string_length=64 affects variable_small only.
  --bm_row_container_large_string_length=1024 affects variable_large only.
Do not pass --bm_regex, --bm_row_container_data_bytes, or
--bm_row_container_warmup_data_bytes after "--"; use
--include-regex/--exclude-regex, --data-bytes, and --warmup-data-bytes instead.

Output:
  $output_dir/stdout.txt
  $output_dir/stderr.txt
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
    --warmup-data-bytes)
      require_value "$1" "${2:-}"
      warmup_data_bytes="$2"
      shift 2
      ;;
    --pre-sleep-seconds)
      require_value "$1" "${2:-}"
      pre_sleep_seconds="$2"
      shift 2
      ;;
    --post-sleep-seconds)
      require_value "$1" "${2:-}"
      post_sleep_seconds="$2"
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
    --no-drop-cache)
      drop_cache="0"
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
