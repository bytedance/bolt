die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_value() {
  local option="$1"
  local value="${2:-}"
  [[ -n "${value}" ]] || die "${option} requires a value"
}

validate_runner_inputs() {
  [[ -x "${binary}" ]] ||
    die "benchmark binary is not executable: ${binary}"
  [[ "${pre_sleep_seconds}" =~ ^[0-9]+$ ]] ||
    die "--pre-sleep-seconds must be a non-negative integer"
  [[ "${post_sleep_seconds}" =~ ^[0-9]+$ ]] ||
    die "--post-sleep-seconds must be a non-negative integer"
  [[ "${timeout_seconds}" =~ ^[1-9][0-9]*$ ]] ||
    die "--timeout-seconds must be a positive integer"
  [[ "${data_bytes}" =~ ^[1-9][0-9]*$ ]] ||
    die "--data-bytes must be a positive integer"
  [[ "${warmup_data_bytes}" =~ ^[0-9]+$ ]] ||
    die "--warmup-data-bytes must be a non-negative integer"
}

reject_runner_owned_extra_args() {
  for extra_arg in "${extra_args[@]}"; do
    case "${extra_arg}" in
      --bm_regex | --bm_regex=* | --bm_row_container_data_bytes | --bm_row_container_data_bytes=* | --bm_row_container_warmup_data_bytes | --bm_row_container_warmup_data_bytes=*)
        die "${extra_arg} is owned by this runner; use --include-regex/--exclude-regex, --data-bytes, or --warmup-data-bytes"
        ;;
    esac
  done
}

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

start_sudo_keepalive() {
  sudo_keepalive_pid=""
  if [[ "${drop_cache}" != "1" ]]; then
    return 0
  fi

  sudo -v || die "sudo validation failed"
  (
    while true; do
      sudo -n true
      sleep 60
    done
  ) &
  sudo_keepalive_pid="$!"
  trap '[[ -n "${sudo_keepalive_pid}" ]] && kill "${sudo_keepalive_pid}" 2>/dev/null || true' EXIT
}

run_bm_row_container_cases() {
  validate_runner_inputs
  reject_runner_owned_extra_args

  local cases
  mapfile -t cases < <(enumerate_cases)

  if [[ "${#cases[@]}" -eq 0 ]]; then
    die "no benchmark cases selected"
  fi

  if [[ "${list_only}" == "1" ]]; then
    printf '%s\n' "${cases[@]}"
    return 0
  fi

  mkdir -p "${output_dir}" || die "failed to create output dir: ${output_dir}"
  stdout_file="${output_dir}/stdout.txt"
  stderr_file="${output_dir}/stderr.txt"
  : >"${stdout_file}" || die "failed to write ${stdout_file}"
  : >"${stderr_file}" || die "failed to write ${stderr_file}"

  start_sudo_keepalive

  local overall_status=0
  local total="${#cases[@]}"
  local index=0

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

  return "${overall_status}"
}
