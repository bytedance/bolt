# Latest Benchmark Result Analysis

This document summarizes the latest results in
`/data00/home/wangxinshuo.db/bolt/log.txt`.

## Test Conditions

This run used buffered I/O:

```text
direct=0
invalidate=0
norandommap=1
DROP_CACHES=1
DROP_CACHES_DEBUG=1
runtime=90s
fio and the scheduler used different data files
```

The benchmark dropped caches before every fio or scheduler run:

```bash
sync
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
```

## Page Cache Reset Results

The logs show that `drop_caches` took effect. A representative sample follows:

```text
drop_caches before:
Buffers:          242024 kB
Cached:         18046936 kB
Dirty:           2982216 kB
Writeback:        122244 kB

drop_caches after:
Buffers:            6060 kB
Cached:          2605476 kB
Dirty:               608 kB
Writeback:             0 kB
```

Dirty pages were also cleared before write scenarios:

```text
drop_caches before:
Cached:         13242368 kB
Dirty:          10029068 kB
Writeback:        237676 kB

drop_caches after:
Cached:          2604736 kB
Dirty:                84 kB
Writeback:             0 kB
```

Conclusion: the OS page cache and dirty/writeback state were reset to low
levels before each benchmark run.

## Performance Summary

| Scenario | Backend | Block | QD | IOPS | BW MiB/s | p50 us | p99 us | Errors |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| bandwidth_read | fio | 1 MiB | 128 | 2379.02 | 2379.02 | 20316.16 | 717225.98 | 0 |
| bandwidth_read | scheduler | 1 MiB | 128 | 5575.83 | 5575.83 | 24703.00 | 54286.00 | 0 |
| bandwidth_write | fio | 1 MiB | 128 | 2528.52 | 2528.52 | 33816.58 | 708837.38 | 0 |
| bandwidth_write | scheduler | 1 MiB | 128 | 2936.19 | 2936.19 | 34396.00 | 387351.00 | 0 |
| iops_read | fio | 4 KiB | 256 | 12384.48 | 48.38 | 25559.04 | 26607.62 | 0 |
| iops_read | scheduler | 4 KiB | 256 | 490409.00 | 1915.66 | 519.00 | 840.00 | 0 |
| iops_write | fio | 4 KiB | 256 | 377502.38 | 1474.62 | 667.65 | 864.26 | 0 |
| iops_write | scheduler | 4 KiB | 256 | 478280.00 | 1868.28 | 448.00 | 1066.00 | 0 |

Scheduler results relative to fio:

```text
bandwidth_read   +134.38%
bandwidth_write  +16.12%
iops_read        +3859.87%
iops_write       +26.70%
```

## Scheduler Timing Breakdown

### bandwidth_write

`bandwidth_write` was the primary scenario of interest in this run.

```text
average_device_latency_us=43574
max_latency_us=1733065
average_queue_wait_us=5.78598
max_queue_wait_us=266
average_backend_submit_us=0.0158232
max_backend_submit_us=225
average_backend_reap_us=0.0022088
max_backend_reap_us=224
average_worker_wait_us=166.412
max_worker_wait_us=1690606
average_future_fulfill_us=1.1083
max_future_fulfill_us=257
average_submit_batch_size=1.00045
average_completion_batch_size=1.00001
max_observed_inflight_requests=128
```

These measurements indicate the following:

1. `max_observed_inflight_requests=128`: the scheduler can saturate the fixed
   queue depth.
2. `average_queue_wait_us=5.78`: requests do not stall in the scheduler's
   pending queue.
3. `average_backend_submit_us=0.0158`: the `io_uring_submit` path is not the
   bottleneck.
4. `average_backend_reap_us=0.0022`: reaping CQEs is not the bottleneck.
5. `average_future_fulfill_us=1.1083`: fulfilling futures is not the
   bottleneck.
6. `average_device_latency_us=43574`: the long delay occurs after submission
   while waiting for kernel completion.
7. `max_worker_wait_us=1690606` is close to `max_latency_us=1733065`, showing
   that the worker mostly waits in epoll for completion events during the tail
   latency interval.

Therefore, the `bandwidth_write` bottleneck is not in the scheduler's
user-space submit, reap, or future path. It is in the kernel completion and
writeback path for buffered writes.

### bandwidth_read

```text
average_device_latency_us=4590.47
average_queue_wait_us=9498.81
average_backend_submit_us=177.849
average_backend_reap_us=1.60708
average_future_fulfill_us=21.8123
average_submit_batch_size=49.6144
average_completion_batch_size=49.6144
max_observed_inflight_requests=120
```

The read scenario has large completion batches, with both submit and
completion batch sizes around 49. `average_backend_submit_us=177.849` is
significantly higher than for writes. This suggests that some work in the
buffered-read scenario may complete or block inside `io_uring_submit()`, even
though overall throughput remains higher than fio.

### iops_write

```text
average_device_latency_us=235.113
max_latency_us=965614
average_queue_wait_us=191.435
average_backend_submit_us=0.0347318
average_backend_reap_us=0.602772
average_future_fulfill_us=1.6927
average_submit_batch_size=7.15592
average_completion_batch_size=7.04089
```

For 4 KiB writes, the scheduler's average submit, reap, and future overhead is
low, and throughput is higher than fio. `max_latency_us` and
`max_worker_wait_us` still show long-tail events, indicating that small writes
can also encounter occasional kernel-side waits, but with limited effect on
the average.

### iops_read

```text
average_device_latency_us=110.557
max_latency_us=595
average_queue_wait_us=193.131
average_backend_submit_us=1.02696
average_backend_reap_us=1.71235
average_future_fulfill_us=26.7312
average_submit_batch_size=94.5906
average_completion_batch_size=94.5906
```

The scheduler path is very fast for 4 KiB reads and uses large completion
batches. However, with `DROP_CACHES=1` and buffered reads, the faster scheduler
warms its own file sooner and repeatedly hits the page cache. The large
`iops_read` lead therefore cannot be attributed solely to lower scheduler
overhead.

## Conclusions

1. `drop_caches` was confirmed to work: `Cached`, `Dirty`, and `Writeback`
   dropped substantially after each reset.
2. All four scenarios completed with `errors=0`.
3. The scheduler was 16.12% faster than fio in `bandwidth_write`, although both
   showed significant p99 tail latency.
4. The timing breakdown shows that the `bandwidth_write` tail latency does not
   come from the scheduler's pending queue, backend submit, backend reap, or
   future fulfillment.
5. The `bandwidth_write` tail latency occurs primarily after submission while
   waiting for kernel completion. Together with the observed dirty-page and
   writeback changes, this points to buffered-write/writeback completion
   latency as the bottleneck.
6. `average_submit_batch_size=1` is the expected steady state when each
   completion is immediately replaced; it is not directly responsible for low
   throughput.
7. `iops_read` must be interpreted carefully under cold-cache buffered I/O:
   the scheduler reaches a warm-cache loop sooner, while fio spends a larger
   fraction of its run on cold reads.

## Follow-up Recommendations

1. To further validate the effect of writeback, compare `bandwidth_write` with
   `BS=256k`, `BS=512k`, and `BS=1m`, and observe whether p99 and
   `average_device_latency_us` increase with block size.
2. To compare the raw I/O stacks, add a `DIRECT=1` scenario after aligned
   buffers are supported.
3. To evaluate production buffered-I/O behavior, retain the current
   `DROP_CACHES=1` results as a cold-cache baseline and also collect
   `DROP_CACHES=0` warm-cache or steady-state results.
