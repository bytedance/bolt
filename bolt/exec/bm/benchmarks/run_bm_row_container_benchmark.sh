#!/usr/bin/env bash

set -uo pipefail

readonly DEFAULT_BINARY="/data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark"
readonly DEFAULT_DATA_BYTES="26843545600"
readonly DEFAULT_WARMUP_DATA_BYTES="134217728"
readonly DEFAULT_PRE_SLEEP_SECONDS="10"
readonly DEFAULT_POST_SLEEP_SECONDS="10"
readonly DEFAULT_TIMEOUT_SECONDS="900"

binary="${DEFAULT_BINARY}"
data_bytes="${DEFAULT_DATA_BYTES}"
warmup_data_bytes="${DEFAULT_WARMUP_DATA_BYTES}"
output_dir="/data00/home/wangxinshuo.db/bolt/log/bolt-bm-row-container-$(date +%Y%m%d-%H%M%S)"
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
  run_bm_row_container_benchmark.sh [options] [-- extra benchmark args]

Options:
  --binary PATH              Benchmark binary to run.
                             Default: _build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark
  --output-dir DIR           Directory for stdout.txt and stderr.txt.
                             Default: /data00/home/wangxinshuo.db/bolt/log/bolt-bm-row-container-YYYYmmdd-HHMMSS
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
Do not pass --bm_regex, --bm_row_container_data_bytes, or
--bm_row_container_warmup_data_bytes after "--"; use
--include-regex/--exclude-regex, --data-bytes, and --warmup-data-bytes instead.

Output:
  $output_dir/stdout.txt
  $output_dir/stderr.txt
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_value() {
  local option="$1"
  local value="${2:-}"
  [[ -n "${value}" ]] || die "${option} requires a value"
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

[[ -x "${binary}" ]] || die "benchmark binary is not executable: ${binary}"
[[ "${pre_sleep_seconds}" =~ ^[0-9]+$ ]] || die "--pre-sleep-seconds must be a non-negative integer"
[[ "${post_sleep_seconds}" =~ ^[0-9]+$ ]] || die "--post-sleep-seconds must be a non-negative integer"
[[ "${timeout_seconds}" =~ ^[1-9][0-9]*$ ]] || die "--timeout-seconds must be a positive integer"
[[ "${data_bytes}" =~ ^[1-9][0-9]*$ ]] || die "--data-bytes must be a positive integer"
[[ "${warmup_data_bytes}" =~ ^[0-9]+$ ]] || die "--warmup-data-bytes must be a non-negative integer"

for extra_arg in "${extra_args[@]}"; do
  case "${extra_arg}" in
    --bm_regex | --bm_regex=* | --bm_row_container_data_bytes | --bm_row_container_data_bytes=* | --bm_row_container_warmup_data_bytes | --bm_row_container_warmup_data_bytes=*)
      die "${extra_arg} is owned by this runner; use --include-regex/--exclude-regex, --data-bytes, or --warmup-data-bytes"
      ;;
  esac
done

case_to_bm_regex() {
  # This Folly benchmark build uses boost::regex_search for --bm_regex, but
  # anchored literal patterns with escaped parentheses do not match names like
  # readBm(bm_fixed). Replace the parentheses with single-character wildcards
  # and keep the rest literal-ish; current row-container case names are unique
  # under this pattern.
  sed -e 's/[][{}.^$*+?|\\]/\\&/g' -e 's/[()]/./g'
}

benchmark_case_name() {
  awk '
    /^[[:space:]]*$/ { next }
    /^=/ { next }
    /^-/ { next }
    /relative[[:space:]]+time\/iter[[:space:]]+iters\/s/ { next }
    {
      name = $1
      if (name ~ /^[[:alnum:]_]+\(.*\)$/) {
        print name
      }
    }
  '
}

enumerate_cases() {
  local probe_output
  if ! probe_output="$(
    "${binary}" \
      "${extra_args[@]}" \
      --bm_row_container_data_bytes=1024 \
      --bm_row_container_reusable_input_bytes=1024 \
      --bm_min_iters=1 \
      --bm_max_iters=1 \
      --bm_min_usec=0 \
      --bm_max_secs=1 \
      --bm_max_trials=1
  )"; then
    die "failed to enumerate benchmark cases from ${binary}"
  fi

  benchmark_case_name <<<"${probe_output}" | while IFS= read -r case_name; do
    if [[ -n "${include_regex}" && ! "${case_name}" =~ ${include_regex} ]]; then
      continue
    fi
    if [[ -n "${exclude_regex}" && "${case_name}" =~ ${exclude_regex} ]]; then
      continue
    fi
    printf '%s\n' "${case_name}"
  done
}

mapfile -t cases < <(enumerate_cases)

if [[ "${#cases[@]}" -eq 0 ]]; then
  die "no benchmark cases selected"
fi

if [[ "${list_only}" == "1" ]]; then
  printf '%s\n' "${cases[@]}"
  exit 0
fi

mkdir -p "${output_dir}" || die "failed to create output dir: ${output_dir}"
stdout_file="${output_dir}/stdout.txt"
stderr_file="${output_dir}/stderr.txt"
: >"${stdout_file}" || die "failed to write ${stdout_file}"
: >"${stderr_file}" || die "failed to write ${stderr_file}"

log_runner() {
  local message="$1"
  printf '%s\n' "${message}" >>"${stderr_file}"
}

shell_quote_command() {
  printf '%q ' "$@"
  printf '\n'
}

log_meminfo() {
  local label="$1"
  printf '%s %s\n' "----- ${label}" "$(date -Is) -----" >&2
  awk '
    /^(MemFree|MemAvailable|Buffers|Cached|SwapCached|Shmem|SReclaimable|Dirty|Writeback):/ {
      printf "  %-16s %12s %s\n", $1, $2, $3
    }
  ' /proc/meminfo >&2
}

drop_page_cache() {
  if [[ "${drop_cache}" != "1" ]]; then
    log_meminfo "meminfo before drop_caches skipped"
    printf '%s\n' "drop_caches skipped by --no-drop-cache" >&2
    return 0
  fi

  log_meminfo "meminfo before sync"
  sync
  log_meminfo "meminfo after sync before drop_caches"

  if ! echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null; then
    return 1
  fi

  log_meminfo "meminfo after drop_caches"
}

sudo_keepalive_pid=""
if [[ "${drop_cache}" == "1" ]]; then
  sudo -v || die "sudo validation failed"
  (
    while true; do
      sudo -n true
      sleep 60
    done
  ) &
  sudo_keepalive_pid="$!"
  trap '[[ -n "${sudo_keepalive_pid}" ]] && kill "${sudo_keepalive_pid}" 2>/dev/null || true' EXIT
fi

overall_status=0
total="${#cases[@]}"
index=0

for case_name in "${cases[@]}"; do
  index=$((index + 1))
  bm_regex="$(printf '%s' "${case_name}" | case_to_bm_regex)"
  cmd=(
    timeout "${timeout_seconds}s"
    "${binary}"
    "--bm_regex=${bm_regex}"
    "--bm_row_container_data_bytes=${data_bytes}"
  )
  if [[ "${warmup_data_bytes}" != "0" ]]; then
    cmd+=("--bm_row_container_warmup_data_bytes=${warmup_data_bytes}")
  fi
  cmd+=("${extra_args[@]}")

  log_runner "===== CASE ${index}/${total} ${case_name} START $(date -Is) ====="
  log_runner "command: $(shell_quote_command "${cmd[@]}")"

  if ! drop_page_cache >>"${stdout_file}" 2>>"${stderr_file}"; then
    log_runner "drop_caches failed before ${case_name}"
    overall_status=1
    continue
  fi

  sleep "${pre_sleep_seconds}"

  start_seconds="$(date +%s)"
  "${cmd[@]}" >>"${stdout_file}" 2>>"${stderr_file}"
  case_status="$?"
  end_seconds="$(date +%s)"
  elapsed_seconds=$((end_seconds - start_seconds))

  sync
  sleep "${post_sleep_seconds}"

  log_runner "===== CASE ${index}/${total} ${case_name} END exit=${case_status} elapsed=${elapsed_seconds}s $(date -Is) ====="

  if [[ "${case_status}" -ne 0 ]]; then
    overall_status=1
  fi
done

log_runner "===== ALL CASES END exit=${overall_status} output_dir=${output_dir} $(date -Is) ====="
printf 'stdout: %s\n' "${stdout_file}"
printf 'stderr: %s\n' "${stderr_file}"

exit "${overall_status}"
