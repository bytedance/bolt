# DiskIoScheduler Versus fio io_uring Benchmark Plan

## Goals

The benchmarks under `bolt/common/memory/bm/io/benchmark` compare the
performance of:

1. fio using `io_uring` directly.
2. Bolt's `DiskIoScheduler` using the production scheduling path.

The benchmark focuses on two questions:

1. With the same file size, I/O pattern, block size, and queue depth, what is
   the end-to-end overhead of `DiskIoScheduler` relative to fio `io_uring`?
2. If there is a significant difference, is it more likely to come from
   request queuing, priority scheduling, adaptive depth, completion handling,
   future wakeups, or buffer preparation?

This measures **end-to-end scheduler overhead**, not raw `io_uring` system-call
overhead. fio is a mature user-space I/O benchmark, while `DiskIoScheduler`
includes BufferManager queues, scheduling, statistics, and future semantics.
Their internal behavior is therefore not completely equivalent.

## Recommended Structure

The benchmark is split into two components:

```text
run_scheduler_vs_fio.sh
bolt_memory_bm_io_scheduler_benchmark
```

Their responsibilities are:

1. `run_scheduler_vs_fio.sh` is the comparison entry point. It generates the
   fio job, invokes fio and the scheduler benchmark binary, parses both
   outputs, and prints a comparison table.
2. `bolt_memory_bm_io_scheduler_benchmark` runs only the `DiskIoScheduler`
   path. It neither invokes fio nor parses fio JSON.

This structure has several benefits:

1. fio remains an external baseline tool, and its job parameters are directly
   visible in the script.
2. The C++ benchmark does not depend on fio or need to execute external
   commands and parse JSON.
3. Comparison logic stays in the script, making it easier to add machine
   metadata collection, matrix runs, and result archival later.

## Invoking fio

The script invokes fio; the C++ benchmark does not.

Script path:

```bash
bolt/common/memory/bm/io/benchmark/run_scheduler_vs_fio.sh
```

By default, the script runs bandwidth and IOPS scenarios:

```bash
bolt/common/memory/bm/io/benchmark/run_scheduler_vs_fio.sh
```

Environment variables can select and configure a scenario:

```bash
SCENARIO=iops_read BS=4k IODEPTH=256 NUMJOBS=4 \
  bolt/common/memory/bm/io/benchmark/run_scheduler_vs_fio.sh
```

Common parameters:

```text
FIO_BIN=fio
SCHEDULER_BENCHMARK=_build/Release/bolt/common/memory/bm/io/benchmark/bolt_memory_bm_io_scheduler_benchmark
FILE=/tmp/bolt-bm-io-benchmark.dat
SIZE=4g
SCENARIO=bandwidth_read,bandwidth_write,iops_read,iops_write,all
BS=256k              # Defaults according to SCENARIO when unspecified
IODEPTH=128          # Defaults according to SCENARIO when unspecified
NUMJOBS=1
RUNTIME=30
DIRECT=0
FIO_INVALIDATE=0
FIO_FORCE_ASYNC=0      # Diagnostic option, not part of the default comparison
FIO_NORANDOMMAP=1
DROP_CACHES=0         # Set to 1 to clear the OS page cache before each run
DROP_CACHES_DEBUG=0   # Set to 1 to print /proc/meminfo before and after clearing
ORDER=scheduler_first
OUTPUT_DIR=/tmp/bolt-bm-io
```

The script produces:

1. The fio job file.
2. The fio JSON result.
3. The scheduler result.
4. A comparison table with IOPS, bandwidth, p50/p99 latency, and error counts.

`DIRECT` defaults to `0` because the current
`IoBuffer::allocateFromMalloc()` implementation does not guarantee the
alignment required by `O_DIRECT`. A separate `DIRECT=1` case can be added
after the scheduler path supports aligned buffers.

## Scenario Categories

IOPS and bandwidth tests are separate categories and should not be interpreted
as one matrix. Their primary metrics, block sizes, and queue depths differ.

### IOPS Scenarios

IOPS scenarios use small blocks and high queue depths. They focus on how
per-operation scheduling overhead, completion handling, and future wakeups
affect throughput.

Primary metrics:

1. IOPS
2. p50 and p99 latency
3. Scheduler IOPS relative to fio

| Scenario | fio `rw` | Scheduler Operation | Block Size | Queue Depth | Jobs |
| --- | --- | --- | --- | --- | --- |
| `iops_read` | `randread` | Read | 4 KiB, 16 KiB | 128, 256 | 1, 4 |
| `iops_write` | `randwrite` | Write | 4 KiB, 16 KiB | 128, 256 | 1, 4 |

### Bandwidth Scenarios

Bandwidth scenarios use large blocks and sequential I/O. They focus on the
large-block throughput limit for workloads such as BufferManager spill reads
and writes.

Primary metrics:

1. MiB/s
2. p50 and p99 latency
3. Scheduler bandwidth relative to fio

| Scenario | fio `rw` | Scheduler Operation | Block Size | Queue Depth | Jobs |
| --- | --- | --- | --- | --- | --- |
| `bandwidth_read` | `read` | Read | 256 KiB, 1 MiB, 4 MiB | 32, 128 | 1 |
| `bandwidth_write` | `write` | Write | 256 KiB, 1 MiB, 4 MiB | 32, 128 | 1 |

### Scenario Defaults

The script sets defaults according to `SCENARIO`:

| SCENARIO | fio `rw` | Default BS | Default IODEPTH | Default NUMJOBS | Primary Metric |
| --- | --- | --- | --- | --- | --- |
| `bandwidth_read` | `read` | 1 MiB | 128 | 1 | bandwidth |
| `bandwidth_write` | `write` | 1 MiB | 128 | 1 | bandwidth |
| `iops_read` | `randread` | 4 KiB | 256 | 1 | IOPS |
| `iops_write` | `randwrite` | 4 KiB | 256 | 1 | IOPS |

Explicit `BS`, `IODEPTH`, or `NUMJOBS` environment variables override these
defaults. The default order is `ORDER=scheduler_first`; set
`ORDER=fio_first` to run the reverse order as a control. fio and the scheduler
use separate data files derived from `FILE`, preventing the second buffered
run from directly inheriting the first run's page-cache state for the same
file.

The script sets `FIO_INVALIDATE=0` by default. fio normally defaults to
`invalidate=1`, which actively invalidates the page cache before starting a
job. The `DiskIoScheduler` path does not perform this action. Keeping fio's
default in buffered-I/O comparisons would test fio under cold-cache conditions
while the scheduler uses the normal buffered path, exaggerating the
difference due to fio's cache handling.

`FIO_FORCE_ASYNC=1` is a diagnostic option that passes `IOSQE_ASYNC` to fio and
can be used to study its effect on buffered `io_uring`. It is disabled by
default because the current `DiskIoScheduler` backend does not set
`IOSQE_ASYNC`; enabling it would change the comparison target.

The script sets `FIO_NORANDOMMAP=1` by default. fio's random map attempts to
avoid accessing the same block more than once per pass. The scheduler
currently cycles through a random-offset sequence and revisits data in the
same file during sufficiently long runs. Disabling fio's random map makes its
cache-hit model closer to the scheduler's for buffered random reads.

Set `DROP_CACHES=1` to compare cold page-cache behavior. Before every fio or
scheduler run, the script executes `sync` followed by
`sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'`. This affects the entire
machine's page cache and should only be used on a dedicated benchmark host.
Set `DROP_CACHES_DEBUG=1` as well to print `Cached`, `Buffers`, `Dirty`, and
`Writeback` before and after the reset.

The first version implements:

1. `bandwidth_read`
2. `bandwidth_write`
3. `iops_read`
4. `iops_write`
5. A single job
6. `direct=0`
7. `invalidate=0`
8. `norandommap=1`

Multi-job concurrency and the full matrix can be added later.

## C++ Benchmark Parameters

The C++ binary runs only the scheduler path and exposes these flags:

```text
--bm_io_benchmark_path=/tmp/bolt-bm-io-benchmark.dat
--bm_io_benchmark_file_size_mb=4096
--bm_io_benchmark_block_size_kb=256
--bm_io_benchmark_queue_depth=128
--bm_io_benchmark_jobs=1
--bm_io_benchmark_runtime_sec=30
--bm_io_benchmark_scenario=bandwidth_read|bandwidth_write|iops_read|iops_write
--bm_io_benchmark_output_json=/tmp/bolt-bm-io/scheduler.json
```

Random I/O uses a fixed seed. fio uses `randrepeat=1`, and the scheduler uses
the same seed to generate its offset sequence, making repeated runs
reproducible.

## Fairness Constraints

1. fio and the scheduler use the same scenario parameters and file size, but
   use different file paths.
2. Read-test files are created and preallocated before measurement; setup time
   is excluded.
3. Both paths perform a warmup that is excluded from measurement.
4. Both paths use the same block size, queue depth, runtime, and random-offset
   rules.
5. CPU affinity is optional and is not hard-coded into the benchmark.
6. Output explicitly records whether `direct=0` or `direct=1`.
7. Each scheduler write request transfers ownership of an `IoBuffer`. To avoid
   counting repeated allocation as scheduler overhead, the C++ benchmark
   preallocates a buffer pool before measurement.

## fio Job Template

The script generates a fio job like this:

```ini
[global]
ioengine=io_uring
filename=${path}
size=${file_size}
bs=${block_size}
iodepth=${queue_depth}
numjobs=${jobs}
runtime=${runtime_sec}
time_based=1
group_reporting=1
randrepeat=1
norandommap=1
invalidate=0
direct=0

[workload]
rw=${rw}
```

It runs fio as follows:

```text
fio --output-format=json --output=<json_path> <job_path>
```

At minimum, the script parses these JSON fields:

1. `read.iops` or `write.iops`
2. `read.bw_bytes` or `write.bw_bytes`
3. `read.clat_ns.percentile` or `write.clat_ns.percentile`
4. Error count

If fio is unavailable, the script fails immediately with an installation
hint. The C++ benchmark does not handle missing fio installations.

## Scheduler Path Design

The scheduler path performs these steps:

1. Prepare the same set of offsets and buffers.
2. Keep at most `queue_depth * jobs` requests in flight.
3. Submit requests through
   `diskIoScheduler().submit(std::move(request))`.
4. Submit a replacement whenever a future completes, until the runtime
   deadline is reached.
5. Track completed operations, completed bytes, submit latency, end-to-end
   latency, and error counts.
6. Stop submitting new requests at the deadline, then drain all previously
   submitted futures.

Write requests require a fresh `IoBuffer` because `IoRequest` takes ownership
of the buffer. Before measurement, the benchmark allocates a pool sized to the
maximum number of in-flight requests. Submission takes a buffer from the free
list, and completion recovers it from `IoResult`. Write buffers are filled
only during preallocation, so per-request allocation, deallocation, and
repeated filling are excluded from scheduler performance.

## Output Format

The script prints a compact table:

```text
scenario         backend      bs    qd   jobs   iops       bw_MiBps   p50_us  p99_us  errors
bandwidth_read   fio          1M    128  1      ...
bandwidth_read   scheduler    1M    128  1      ...
bandwidth_read   overhead     1M    128  1      iops=-x%   bw=-y%     p99=+z%
iops_read        fio          4K    256  1      ...
iops_read        scheduler    4K    256  1      ...
iops_read        overhead     4K    256  1      iops=-x%   bw=-y%     p99=+z%
```

The script also prints `diskIoScheduler().stats()` so slow cases can be
correlated with queue-depth, in-flight, dispatch, and completion statistics.

## File Layout

```text
bolt/common/memory/bm/io/benchmark/
  CMakeLists.txt
  SchedulerBenchmark.cpp
  run_scheduler_vs_fio.sh
  README.md
```

Recommended `CMakeLists.txt` contents:

```cmake
add_executable(
  bolt_memory_bm_io_scheduler_benchmark
  SchedulerBenchmark.cpp)

target_link_libraries(
  bolt_memory_bm_io_scheduler_benchmark
  PRIVATE bolt_memory_bm_io
          bolt_common_base
          ${FOLLY_BENCHMARK}
          glog::glog)
```

Add the following to `bolt/common/memory/bm/io/CMakeLists.txt`:

```cmake
if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmark)
endif()
```

## Risks

1. fio and `DiskIoScheduler` have different internal semantics, so the results
   can only be interpreted as end-to-end scheduler overhead.
2. `direct=1` requires aligned scheduler buffers. Without them, results are
   unreliable and the benchmark may fail immediately.
3. Small files are heavily affected by the page cache. Formal reports must
   record file size, machine memory, and the `direct` setting.
4. fio JSON parsing can initially be minimal, but missing fields must produce
   an explicit failure rather than silently reporting zero.
