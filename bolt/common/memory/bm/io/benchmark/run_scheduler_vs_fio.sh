#!/usr/bin/env bash
set -euo pipefail

FIO_BIN="${FIO_BIN:-fio}"
SCHEDULER_BENCHMARK="${SCHEDULER_BENCHMARK:-_build/Release/bolt/common/memory/bm/io/benchmark/bolt_memory_bm_io_scheduler_benchmark}"
FILE="${FILE:-/tmp/bolt-bm-io-benchmark.dat}"
SIZE="${SIZE:-10g}"
SCENARIO="${SCENARIO:-all}"
RUNTIME="${RUNTIME:-90}"
DIRECT="${DIRECT:-0}"
OUTPUT_DIR="${OUTPUT_DIR:-/tmp/bolt-bm-io}"
ORDER="${ORDER:-scheduler_first}"
FIO_INVALIDATE="${FIO_INVALIDATE:-0}"
FIO_FORCE_ASYNC="${FIO_FORCE_ASYNC:-0}"
FIO_NORANDOMMAP="${FIO_NORANDOMMAP:-1}"
DROP_CACHES="${DROP_CACHES:-1}"
DROP_CACHES_DEBUG="${DROP_CACHES_DEBUG:-1}"

if ! command -v "${FIO_BIN}" >/dev/null 2>&1; then
  echo "fio not found: ${FIO_BIN}" >&2
  exit 1
fi

if [[ ! -x "${SCHEDULER_BENCHMARK}" ]]; then
  echo "scheduler benchmark not executable: ${SCHEDULER_BENCHMARK}" >&2
  exit 1
fi

mkdir -p "${OUTPUT_DIR}"

case "${SCENARIO}" in
  all)
    SCENARIOS=(bandwidth_read bandwidth_write iops_read iops_write)
    ;;
  bandwidth_read|bandwidth_write|iops_read|iops_write)
    SCENARIOS=("${SCENARIO}")
    ;;
  *)
    echo "Unsupported SCENARIO=${SCENARIO}" >&2
    exit 1
    ;;
esac

case "${ORDER}" in
  scheduler_first|fio_first)
    ;;
  *)
    echo "Unsupported ORDER=${ORDER}" >&2
    exit 1
    ;;
esac

scenario_rw() {
  case "$1" in
    bandwidth_read) echo "read" ;;
    bandwidth_write) echo "write" ;;
    iops_read) echo "randread" ;;
    iops_write) echo "randwrite" ;;
  esac
}

scenario_bs() {
  if [[ -n "${BS:-}" ]]; then
    echo "${BS}"
    return
  fi
  case "$1" in
    bandwidth_read|bandwidth_write) echo "1m" ;;
    iops_read|iops_write) echo "4k" ;;
  esac
}

scenario_iodepth() {
  if [[ -n "${IODEPTH:-}" ]]; then
    echo "${IODEPTH}"
    return
  fi
  case "$1" in
    bandwidth_read|bandwidth_write) echo "128" ;;
    iops_read|iops_write) echo "256" ;;
  esac
}

scenario_jobs() {
  if [[ -n "${NUMJOBS:-}" ]]; then
    echo "${NUMJOBS}"
    return
  fi
  echo "1"
}

drop_caches_if_enabled() {
  if [[ "${DROP_CACHES}" == "0" ]]; then
    return
  fi
  if [[ "${DROP_CACHES_DEBUG}" != "0" ]]; then
    echo "drop_caches before:"
    grep -E '^(Cached|Buffers|Dirty|Writeback):' /proc/meminfo
  fi
  sync
  sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
  if [[ "${DROP_CACHES_DEBUG}" != "0" ]]; then
    echo "drop_caches after:"
    grep -E '^(Cached|Buffers|Dirty|Writeback):' /proc/meminfo
  fi
}

bs_to_kb() {
  python3 - "$1" <<'PY'
import re
import sys

value = sys.argv[1].strip().lower()
match = re.fullmatch(r"(\d+)([km]?)", value)
if not match:
    raise SystemExit(f"unsupported block size: {value}")
num = int(match.group(1))
unit = match.group(2)
if unit == "m":
    num *= 1024
print(num)
PY
}

size_to_mb() {
  python3 - "$1" <<'PY'
import re
import sys

value = sys.argv[1].strip().lower()
match = re.fullmatch(r"(\d+)([kmg]?)", value)
if not match:
    raise SystemExit(f"unsupported size: {value}")
num = int(match.group(1))
unit = match.group(2)
if unit == "k":
    num //= 1024
elif unit == "g":
    num *= 1024
print(num)
PY
}

run_fio() {
  local scenario="$1"
  local rw="$2"
  local bs="$3"
  local iodepth="$4"
  local jobs="$5"
  local file_path="$6"
  local job_path="${OUTPUT_DIR}/fio-${scenario}-bs${bs}-qd${iodepth}-jobs${jobs}.fio"
  local json_path="${OUTPUT_DIR}/fio-${scenario}-bs${bs}-qd${iodepth}-jobs${jobs}.json"
  local force_async_config=""
  if [[ "${FIO_FORCE_ASYNC}" != "0" ]]; then
    force_async_config="force_async=${FIO_FORCE_ASYNC}"
  fi

  cat >"${job_path}" <<EOF
[global]
ioengine=io_uring
filename=${file_path}
size=${SIZE}
bs=${bs}
iodepth=${iodepth}
numjobs=${jobs}
runtime=${RUNTIME}
time_based=1
group_reporting=1
randrepeat=1
norandommap=${FIO_NORANDOMMAP}
invalidate=${FIO_INVALIDATE}
direct=${DIRECT}
${force_async_config}

[workload]
rw=${rw}
EOF

  if ! "${FIO_BIN}" --output-format=json --output="${json_path}" "${job_path}" >/dev/null; then
    echo "fio failed: scenario=${scenario}, job=${job_path}, output=${json_path}" >&2
    exit 1
  fi
  python3 - "${json_path}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    json.load(f)
PY
  echo "${json_path}"
}

run_scheduler() {
  local scenario="$1"
  local bs="$2"
  local iodepth="$3"
  local jobs="$4"
  local file_path="$5"
  local bs_kb
  bs_kb="$(bs_to_kb "${bs}")"
  local size_mb
  size_mb="$(size_to_mb "${SIZE}")"
  local json_path="${OUTPUT_DIR}/scheduler-${scenario}-bs${bs}-qd${iodepth}-jobs${jobs}.json"

  "${SCHEDULER_BENCHMARK}" \
    --bm_io_benchmark_path="${file_path}" \
    --bm_io_benchmark_file_size_mb="${size_mb}" \
    --bm_io_benchmark_block_size_kb="${bs_kb}" \
    --bm_io_benchmark_queue_depth="${iodepth}" \
    --bm_io_benchmark_jobs="${jobs}" \
    --bm_io_benchmark_runtime_sec="${RUNTIME}" \
    --bm_io_benchmark_scenario="${scenario}" \
    --bm_io_benchmark_output_json="${json_path}" >/dev/null

  echo "${json_path}"
}

summarize_pair() {
  python3 - "$@" <<'PY'
import json
import sys

scenario, fio_path, scheduler_path = sys.argv[1], sys.argv[2], sys.argv[3]
with open(fio_path, "r", encoding="utf-8") as f:
    fio = json.load(f)
with open(scheduler_path, "r", encoding="utf-8") as f:
    scheduler = json.load(f)

op = "read" if "read" in scenario else "write"
fio_job = fio["jobs"][0]
fio_stats = fio_job[op]
fio_clat = fio_stats.get("clat_ns", {}).get("percentile", {})

def fio_pct(name):
    value = fio_clat.get(name)
    return 0.0 if value is None else float(value) / 1000.0

rows = [
    {
        "scenario": scenario,
        "backend": "fio",
        "bs": scheduler["block_size"],
        "qd": scheduler["queue_depth"],
        "jobs": scheduler["jobs"],
        "iops": float(fio_stats.get("iops", 0.0)),
        "bw": float(fio_stats.get("bw_bytes", 0.0)) / 1024.0 / 1024.0,
        "p50": fio_pct("50.000000"),
        "p99": fio_pct("99.000000"),
        "errors": int(fio_job.get("error", 0)),
    },
    {
        "scenario": scenario,
        "backend": "scheduler",
        "bs": scheduler["block_size"],
        "qd": scheduler["queue_depth"],
        "jobs": scheduler["jobs"],
        "iops": float(scheduler["iops"]),
        "bw": float(scheduler["bw_mibps"]),
        "p50": float(scheduler["p50_us"]),
        "p99": float(scheduler["p99_us"]),
        "errors": int(scheduler["errors"]),
    },
]
for row in rows:
    print(
        f"{row['scenario']:<16} {row['backend']:<10} "
        f"bs={row['bs']:<8} qd={row['qd']:<4} jobs={row['jobs']:<3} "
        f"iops={row['iops']:<12.2f} bw_MiBps={row['bw']:<10.2f} "
        f"p50_us={row['p50']:<10.2f} p99_us={row['p99']:<10.2f} "
        f"errors={row['errors']}"
    )
if rows[0]["iops"] > 0 and rows[0]["bw"] > 0:
    iops_delta = (rows[1]["iops"] / rows[0]["iops"] - 1.0) * 100.0
    bw_delta = (rows[1]["bw"] / rows[0]["bw"] - 1.0) * 100.0
    print(f"{scenario:<16} overhead   iops={iops_delta:+.2f}% bw={bw_delta:+.2f}%")
print(f"{scenario:<16} scheduler_stats {scheduler['scheduler_stats']}")
PY
}

echo "scenario         backend    params"
for scenario in "${SCENARIOS[@]}"; do
  rw="$(scenario_rw "${scenario}")"
  bs="$(scenario_bs "${scenario}")"
  iodepth="$(scenario_iodepth "${scenario}")"
  jobs="$(scenario_jobs "${scenario}")"
  fio_file="${FILE}.${scenario}.fio"
  scheduler_file="${FILE}.${scenario}.scheduler"
  if [[ "${ORDER}" == "scheduler_first" ]]; then
    drop_caches_if_enabled
    scheduler_json="$(run_scheduler "${scenario}" "${bs}" "${iodepth}" "${jobs}" "${scheduler_file}")"
    drop_caches_if_enabled
    fio_json="$(run_fio "${scenario}" "${rw}" "${bs}" "${iodepth}" "${jobs}" "${fio_file}")"
  else
    drop_caches_if_enabled
    fio_json="$(run_fio "${scenario}" "${rw}" "${bs}" "${iodepth}" "${jobs}" "${fio_file}")"
    drop_caches_if_enabled
    scheduler_json="$(run_scheduler "${scenario}" "${bs}" "${iodepth}" "${jobs}" "${scheduler_file}")"
  fi
  summarize_pair "${scenario}" "${fio_json}" "${scheduler_json}"
done
