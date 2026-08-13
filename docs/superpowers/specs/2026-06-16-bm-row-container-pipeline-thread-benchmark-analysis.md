# 2026-06-16 BM RowContainer 4-thread pipeline benchmark 运行结果分析

## 数据来源

- 运行日志目录：`/data00/home/wangxinshuo.db/bolt/log/bolt-bm-row-container-pipeline-thread-20260616-203529`
- benchmark stdout：`stdout.txt`
- runner stderr 与 4-thread pipeline metric：`stderr.txt`
- 本文只分析本次 4-thread pipeline 运行结果，不和历史运行结果对比。

## 运行概况

- 一共运行 12 个 case，全部 `exit=0`。
- 每个 case 单独进程运行，命令里统一使用：
  - `--bm_row_container_data_bytes=10737418240`，即每线程 10 GiB 逻辑数据量。
  - `--bm_row_container_warmup_data_bytes=134217728`，即每线程 128 MiB same-process warmup 数据量。
  - `timeout 900s`。
- 每个 case 内部使用 4 个 worker 线程，因此每个 measured case 的总逻辑数据量是 40 GiB。
- runner 在每个 case 前执行 `sync` 和 `drop_caches`，并打印 `/proc/meminfo`。从日志看，drop 后 `Cached` 基本稳定在约 2.86 GiB，`Dirty` 和 `Writeback` 很小，case 间 page cache 干扰被控制住。

## 指标口径

本 benchmark 有两个时间口径：

1. `sum_total_ms`：4 个线程各自 store、spill write、spill read、read 阶段耗时之和。这是本次正式比较口径。
2. `wall_ms` / benchmark stdout `time/iter`：4 线程 case 的进程内墙钟时间。当前实现中它还包含线程启动、每线程 `BenchmarkContext`、input batch、container 准备等开销，因此只作为并发运行参考。

BM 的 IO 层是进程级共享实例，因此 `process_io_*` 是主线程在 measured run 前后统一采集的进程级 delta，不是每线程 stats 相加。

## 总体结论

1. 按正式口径 `sum_total_ms`，BM RowContainer 在 6 个 dataset/compression 组合上全部快于 old RowContainer。
2. fixed 场景下 BM 仍然有明显收益，`sum_total_ms` 加速比为 1.47x 到 4.55x；fixed lz4 最好，fixed zstd 最弱。
3. variable 场景下 BM 也有收益，但明显弱于 fixed；`sum_total_ms` 加速比为 1.24x 到 2.04x。
4. 按参考口径 `wall_ms`，BM 在 5 个组合上更快，但 `variable raw` 反而更慢：old 为 62.47s，BM 为 75.98s。该场景 BM 的 `parallel_efficiency` 只有 1.527，明显低于 old 的 3.781。
5. raw 场景的进程级 IO queue wait 很高，特别是 BM fixed raw 和 BM variable raw，说明 4 线程并发下共享 IO scheduler/设备排队会显著影响 wall time。
6. variable raw 的正式工作量口径仍然是 BM 更快，但 wall time 更慢；如果端到端用户感知更接近 wall time，这个场景需要进一步看 IO 调度与每线程进度不均衡。

## 总耗时对比

这里使用 `sum_total_ms`，也就是 4 个线程阶段耗时之和。

| dataset | compression | old sum total | BM sum total | BM 加速比 | BM / old |
|---|---|---:|---:|---:|---:|
| fixed | raw | 461.27s | 266.91s | 1.73x | 57.9% |
| fixed | lz4 | 922.77s | 202.80s | 4.55x | 22.0% |
| fixed | zstd | 615.68s | 418.07s | 1.47x | 67.9% |
| variable | raw | 236.19s | 116.03s | 2.04x | 49.1% |
| variable | lz4 | 70.99s | 48.58s | 1.46x | 68.4% |
| variable | zstd | 79.44s | 64.23s | 1.24x | 80.9% |

## Wall time 参考

| dataset | compression | old wall | BM wall | BM 加速比 | BM / old | old efficiency | BM efficiency |
|---|---|---:|---:|---:|---:|---:|---:|
| fixed | raw | 119.27s | 112.20s | 1.06x | 94.1% | 3.867 | 2.379 |
| fixed | lz4 | 233.47s | 57.17s | 4.08x | 24.5% | 3.952 | 3.548 |
| fixed | zstd | 157.65s | 138.91s | 1.13x | 88.1% | 3.905 | 3.010 |
| variable | raw | 62.47s | 75.98s | 0.82x | 121.6% | 3.781 | 1.527 |
| variable | lz4 | 19.91s | 14.06s | 1.42x | 70.6% | 3.565 | 3.456 |
| variable | zstd | 21.95s | 17.77s | 1.24x | 80.9% | 3.619 | 3.615 |

`parallel_efficiency = sum_total_ms / wall_ms`。理想情况下 4 线程接近 4。old 在多数场景接近 3.6-3.95；BM 在 fixed raw、fixed zstd、variable raw 里明显低一些，说明这些场景的阶段耗时并没有很好地并行重叠，或者 wall 里有更多共享资源等待。

## Fixed 场景

### 阶段耗时

| compression | impl | store | spill write | spill read | read | sum total |
|---|---|---:|---:|---:|---:|---:|
| raw | old | 40.20s | 283.76s | 125.81s | 11.50s | 461.27s |
| raw | BM | 13.40s | 200.40s | 45.66s | 7.45s | 266.91s |
| lz4 | old | 40.16s | 289.32s | 581.69s | 11.59s | 922.77s |
| lz4 | BM | 13.40s | 107.75s | 74.15s | 7.49s | 202.80s |
| zstd | old | 40.05s | 358.17s | 205.90s | 11.56s | 615.68s |
| zstd | BM | 13.32s | 286.62s | 110.73s | 7.39s | 418.07s |

### 阶段加速比

| compression | store | spill write | spill read | read | sum total |
|---|---:|---:|---:|---:|---:|
| raw | 3.00x | 1.42x | 2.76x | 1.54x | 1.73x |
| lz4 | 3.00x | 2.69x | 7.84x | 1.55x | 4.55x |
| zstd | 3.01x | 1.25x | 1.86x | 1.56x | 1.47x |

### 物理 spill 大小

| compression | old spill bytes | BM physical spill bytes | BM / old |
|---|---:|---:|---:|
| raw | 42.00 GiB | 48.02 GiB | 114.3% |
| lz4 | 40.62 GiB | 41.37 GiB | 101.9% |
| zstd | 35.20 GiB | 34.71 GiB | 98.6% |

### 分析

fixed 下 BM 的 store 仍然非常稳定，约 3x 快于 old。与单线程 pipeline 类似，这是 batch append 对 fixed 类型的主要收益。

fixed lz4 是本次 4-thread 里 BM 表现最好的 fixed 场景，`sum_total_ms` 从 922.77s 降到 202.80s，约 4.55x。主要收益来自：

- spill write：289.32s -> 107.75s，约 2.69x。
- spill read：581.69s -> 74.15s，约 7.84x。

fixed raw 虽然 BM 的工作量口径快 1.73x，但 wall time 只快 1.06x。BM raw fixed 的进程级 IO completed bytes 为 96.03 GiB，平均 queue wait 达到 3.65s，平均 device latency 约 296.82ms，说明 raw fixed 下 4 个线程同时走 BM spill read/write 时，IO scheduler/设备排队对 wall time 影响很大。

fixed zstd 的收益有限，`sum_total_ms` 只快 1.47x。BM 的主要耗时仍在 spill write，占 BM 总工作量的 68.6%，这是 zstd 压缩成本主导的表现。

BM fixed 各阶段占比：

| compression | store | spill write | spill read | read |
|---|---:|---:|---:|---:|
| raw | 5.0% | 75.1% | 17.1% | 2.8% |
| lz4 | 6.6% | 53.1% | 36.6% | 3.7% |
| zstd | 3.2% | 68.6% | 26.5% | 1.8% |

## Variable 场景

### 阶段耗时

| compression | impl | store | spill write | spill read | read | sum total |
|---|---|---:|---:|---:|---:|---:|
| raw | old | 9.73s | 147.24s | 74.16s | 5.06s | 236.19s |
| raw | BM | 13.74s | 50.03s | 47.36s | 4.89s | 116.03s |
| lz4 | old | 9.67s | 20.37s | 35.87s | 5.09s | 70.99s |
| lz4 | BM | 13.61s | 10.61s | 19.76s | 4.61s | 48.58s |
| zstd | old | 9.67s | 27.21s | 37.46s | 5.09s | 79.44s |
| zstd | BM | 13.37s | 24.55s | 21.71s | 4.60s | 64.23s |

### 阶段加速比

| compression | store | spill write | spill read | read | sum total |
|---|---:|---:|---:|---:|---:|
| raw | 0.71x | 2.94x | 1.57x | 1.03x | 2.04x |
| lz4 | 0.71x | 1.92x | 1.82x | 1.10x | 1.46x |
| zstd | 0.72x | 1.11x | 1.73x | 1.11x | 1.24x |

### 物理 spill 大小

| compression | old spill bytes | BM physical spill bytes | BM / old |
|---|---:|---:|---:|
| raw | 40.65 GiB | 41.41 GiB | 101.9% |
| lz4 | 1.84 GiB | 1.78 GiB | 97.2% |
| zstd | 1.27 GiB | 1.26 GiB | 98.7% |

### 分析

variable 下 BM 的 store 明显慢于 old，三个压缩组合都是约 0.71x-0.72x。也就是说 BM variable 的端到端收益不是来自 store，而是来自 spill write/read。

variable raw 按 `sum_total_ms` 是 BM 更快：236.19s -> 116.03s，约 2.04x。主要来自：

- spill write：147.24s -> 50.03s，约 2.94x。
- spill read：74.16s -> 47.36s，约 1.57x。

但 variable raw 的 wall time 是 BM 更慢：62.47s -> 75.98s。这个场景的 BM `parallel_efficiency` 只有 1.527，而 old 是 3.781。BM process IO completed bytes 为 82.81 GiB，平均 queue wait 为 4.50s，平均 device latency 为 128.91ms。这个结果说明 BM variable raw 虽然总工作量更少，但并发执行时被共享 IO 排队或线程进度不均衡拖慢了 wall time。

variable lz4/zstd 下，BM 按 sum total 和 wall time 都更快。压缩后物理 IO 很小：

- lz4：BM physical spill bytes 约 1.78 GiB。
- zstd：BM physical spill bytes 约 1.26 GiB。

因此 compressed variable 场景里，IO 排队压力明显低于 raw，BM 的 wall time 也能体现收益。

BM variable 各阶段占比：

| compression | store | spill write | spill read | read |
|---|---:|---:|---:|---:|
| raw | 11.8% | 43.1% | 40.8% | 4.2% |
| lz4 | 28.0% | 21.8% | 40.7% | 9.5% |
| zstd | 20.8% | 38.2% | 33.8% | 7.2% |

## BM 进程级 IO 观察

| dataset | compression | completed bytes | avg queue wait | avg device latency | completed requests |
|---|---|---:|---:|---:|---:|
| fixed | raw | 96.03 GiB | 3651.29ms | 296.82ms | 24584 |
| fixed | lz4 | 82.74 GiB | 5007.93ms | 54.33ms | 24584 |
| fixed | zstd | 69.41 GiB | 3165.75ms | 45.27ms | 24584 |
| variable | raw | 82.81 GiB | 4504.43ms | 128.91ms | 21200 |
| variable | lz4 | 3.57 GiB | 201.27ms | 3.07ms | 21200 |
| variable | zstd | 2.51 GiB | 67.40ms | 2.23ms | 21200 |

raw 场景的 queue wait 明显高于压缩后的 variable 场景。这里的 queue wait 是进程级 IO scheduler 的累计统计按完成请求数求平均，不是单线程独占视角。它说明在 4 线程并发时，raw 场景会明显把共享 IO scheduler 压满。

fixed lz4 的平均 queue wait 也很高，但 wall time 仍然有 4.08x 收益，原因是 old fixed lz4 的 spill read 非常慢，BM 即使有较高 IO 排队仍然显著领先。

## 需要关注的点

1. 4-thread 场景下，BM 的 `sum_total_ms` 全部领先，说明按“所有线程工作时间之和”这个口径，BM 改造仍然有端到端收益。
2. `wall_ms` 不是所有场景都领先，尤其 variable raw。这个场景需要单独关注，因为实际用户感知往往更接近 wall time。
3. BM variable store 在 4-thread 下比 old 慢，虽然端到端被 spill 收益抵消，但后续如果减少 raw spill 或压缩后 IO 不再是瓶颈，variable store 可能重新变成限制项。
4. BM raw 场景 IO queue wait 很高，后续若优化 4-thread wall time，优先看 IO scheduler 并发深度、请求粒度、raw 读写路径和每线程进度不均衡。
5. variable lz4 在本次 4-thread 里是比较均衡的选择：sum total 快 1.46x，wall time 快 1.42x，物理 IO 从 40GiB 级别降到 1.78GiB。

## 附录
### stderr.txt
```text
===== CASE 1/12 pipelineOld4Threads(old4_raw_fixed) START 2026-06-16T20:35:29+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_thread_benchmark --bm_regex=pipelineOld4Threads.old4_raw_fixed. --bm_row_container_data_bytes=10737418240 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T20:35:29+08:00 -----
  MemFree:            252248976 kB
  MemAvailable:       250990824 kB
  Buffers:                82672 kB
  Cached:               3054928 kB
  SwapCached:                 0 kB
  Dirty:                    580 kB
  Writeback:                  0 kB
  Shmem:                2608460 kB
  SReclaimable:          169020 kB
----- meminfo after sync before drop_caches 2026-06-16T20:35:29+08:00 -----
  MemFree:            252247668 kB
  MemAvailable:       250989516 kB
  Buffers:                82688 kB
  Cached:               3054912 kB
  SwapCached:                 0 kB
  Dirty:                    176 kB
  Writeback:                124 kB
  Shmem:                2608460 kB
  SReclaimable:          169020 kB
----- meminfo after drop_caches 2026-06-16T20:35:29+08:00 -----
  MemFree:            252531760 kB
  MemAvailable:       251131680 kB
  Buffers:                 8356 kB
  Cached:               2861288 kB
  SwapCached:                 0 kB
  Dirty:                      0 kB
  Writeback:                176 kB
  Shmem:                2608464 kB
  SReclaimable:          153640 kB
[bm-row-container-metrics] pipelineOld4Threads dataset=fixed compression=raw iterations=1 threads=4 per_thread_logical_bytes=10737418240 total_logical_bytes=42949672960 rows_per_thread=536870912 total_rows=2147483648 sum_store_ms=40201.922 sum_spill_write_ms=283757.790 sum_spill_read_ms=125805.153 sum_read_ms=11503.410 sum_total_ms=461268.275 wall_ms=119269.606 parallel_efficiency=3.867 spill_bytes=45097500736 files=43016 batches=43016
===== CASE 1/12 pipelineOld4Threads(old4_raw_fixed) END exit=0 elapsed=121s 2026-06-16T20:37:50+08:00 =====
===== CASE 2/12 pipelineBm4Threads(bm4_raw_fixed) START 2026-06-16T20:37:50+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_thread_benchmark --bm_regex=pipelineBm4Threads.bm4_raw_fixed. --bm_row_container_data_bytes=10737418240 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T20:37:51+08:00 -----
  MemFree:            252260168 kB
  MemAvailable:       250982436 kB
  Buffers:                48948 kB
  Cached:               3032448 kB
  SwapCached:                 0 kB
  Dirty:                   1100 kB
  Writeback:                  0 kB
  Shmem:                2608856 kB
  SReclaimable:          186448 kB
----- meminfo after sync before drop_caches 2026-06-16T20:37:51+08:00 -----
  MemFree:            252259592 kB
  MemAvailable:       250981860 kB
  Buffers:                48968 kB
  Cached:               3032436 kB
  SwapCached:                 0 kB
  Dirty:                    624 kB
  Writeback:                  0 kB
  Shmem:                2608856 kB
  SReclaimable:          186448 kB
----- meminfo after drop_caches 2026-06-16T20:37:51+08:00 -----
  MemFree:            252526320 kB
  MemAvailable:       251128584 kB
  Buffers:                 9192 kB
  Cached:               2857672 kB
  SwapCached:                 0 kB
  Dirty:                    660 kB
  Writeback:                  0 kB
  Shmem:                2608832 kB
  SReclaimable:          161464 kB
[bm-row-container-metrics] pipelineBm4Threads dataset=fixed compression=raw iterations=1 threads=4 per_thread_logical_bytes=10737418240 total_logical_bytes=42949672960 rows_per_thread=536870912 total_rows=2147483648 sum_store_ms=13401.982 sum_spill_write_ms=200397.989 sum_spill_read_ms=45655.362 sum_read_ms=7451.305 sum_total_ms=266906.637 wall_ms=112201.647 parallel_efficiency=2.379 spill_write_count=12292 spill_write_bytes=51556384768 spill_physical_write_bytes=51556778112 spill_read_count=12292 spill_read_bytes=51556384768 spill_physical_read_bytes=51556778112 process_io_accepted=24584 process_io_completed=24584 process_io_completed_bytes=103113556224 process_io_successful=24584 process_io_failed=0 process_io_rejected=0 process_io_submit_batches=12172 process_io_completion_batches=12084 process_io_queue_wait_ms=89763388.577 process_io_avg_queue_wait_us=3651293.060 process_io_device_latency_ms=7297017.445 process_io_avg_device_latency_us=296819.779 process_io_end_to_end_latency_ms=97060418.297 process_io_avg_end_to_end_latency_us=3948113.338 process_io_backend_submit_ms=23290.053 process_io_backend_reap_ms=2.820 process_io_worker_wait_ms=48648.634 process_io_future_fulfill_ms=11.084
===== CASE 2/12 pipelineBm4Threads(bm4_raw_fixed) END exit=0 elapsed=114s 2026-06-16T20:40:05+08:00 =====
===== CASE 3/12 pipelineOld4Threads(old4_lz4_fixed) START 2026-06-16T20:40:05+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_thread_benchmark --bm_regex=pipelineOld4Threads.old4_lz4_fixed. --bm_row_container_data_bytes=10737418240 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T20:40:05+08:00 -----
  MemFree:            252286480 kB
  MemAvailable:       251002712 kB
  Buffers:                39664 kB
  Cached:               3043052 kB
  SwapCached:                 0 kB
  Dirty:                   1024 kB
  Writeback:                  0 kB
  Shmem:                2609244 kB
  SReclaimable:          173960 kB
----- meminfo after sync before drop_caches 2026-06-16T20:40:05+08:00 -----
  MemFree:            252284880 kB
  MemAvailable:       251001616 kB
  Buffers:                39680 kB
  Cached:               3043544 kB
  SwapCached:                 0 kB
  Dirty:                    520 kB
  Writeback:                  0 kB
  Shmem:                2609248 kB
  SReclaimable:          173960 kB
----- meminfo after drop_caches 2026-06-16T20:40:05+08:00 -----
  MemFree:            252513100 kB
  MemAvailable:       251117368 kB
  Buffers:                10100 kB
  Cached:               2864520 kB
  SwapCached:                 0 kB
  Dirty:                    476 kB
  Writeback:                 80 kB
  Shmem:                2609252 kB
  SReclaimable:          158140 kB
[bm-row-container-metrics] pipelineOld4Threads dataset=fixed compression=lz4 iterations=1 threads=4 per_thread_logical_bytes=10737418240 total_logical_bytes=42949672960 rows_per_thread=536870912 total_rows=2147483648 sum_store_ms=40163.326 sum_spill_write_ms=289321.629 sum_spill_read_ms=581692.823 sum_read_ms=11588.955 sum_total_ms=922766.733 wall_ms=233471.604 parallel_efficiency=3.952 spill_bytes=43612564580 files=43016 batches=43016
===== CASE 3/12 pipelineOld4Threads(old4_lz4_fixed) END exit=0 elapsed=236s 2026-06-16T20:44:21+08:00 =====
===== CASE 4/12 pipelineBm4Threads(bm4_lz4_fixed) START 2026-06-16T20:44:21+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_thread_benchmark --bm_regex=pipelineBm4Threads.bm4_lz4_fixed. --bm_row_container_data_bytes=10737418240 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T20:44:21+08:00 -----
  MemFree:            252240120 kB
  MemAvailable:       250974128 kB
  Buffers:                51424 kB
  Cached:               3050084 kB
  SwapCached:                 0 kB
  Dirty:                    556 kB
  Writeback:                  0 kB
  Shmem:                2608348 kB
  SReclaimable:          189320 kB
----- meminfo after sync before drop_caches 2026-06-16T20:44:21+08:00 -----
  MemFree:            252240120 kB
  MemAvailable:       250974128 kB
  Buffers:                51444 kB
  Cached:               3050064 kB
  SwapCached:                 0 kB
  Dirty:                    160 kB
  Writeback:                376 kB
  Shmem:                2608348 kB
  SReclaimable:          189320 kB
----- meminfo after drop_caches 2026-06-16T20:44:21+08:00 -----
  MemFree:            252514140 kB
  MemAvailable:       251122756 kB
  Buffers:                10312 kB
  Cached:               2868008 kB
  SwapCached:                 0 kB
  Dirty:                    184 kB
  Writeback:                  0 kB
  Shmem:                2608348 kB
  SReclaimable:          162820 kB
[bm-row-container-metrics] pipelineBm4Threads dataset=fixed compression=lz4 iterations=1 threads=4 per_thread_logical_bytes=10737418240 total_logical_bytes=42949672960 rows_per_thread=536870912 total_rows=2147483648 sum_store_ms=13396.112 sum_spill_write_ms=107753.016 sum_spill_read_ms=74153.488 sum_read_ms=7494.028 sum_total_ms=202796.644 wall_ms=57165.325 parallel_efficiency=3.548 spill_write_count=12292 spill_write_bytes=51556384768 spill_physical_write_bytes=44419552160 spill_read_count=12292 spill_read_bytes=51556384768 spill_physical_read_bytes=44419552160 process_io_accepted=24584 process_io_completed=24584 process_io_completed_bytes=88839104320 process_io_successful=24584 process_io_failed=0 process_io_rejected=0 process_io_submit_batches=12261 process_io_completion_batches=12359 process_io_queue_wait_ms=123114956.208 process_io_avg_queue_wait_us=5007930.207 process_io_device_latency_ms=1335527.624 process_io_avg_device_latency_us=54325.074 process_io_end_to_end_latency_ms=124450496.497 process_io_avg_end_to_end_latency_us=5062255.796 process_io_backend_submit_ms=21285.868 process_io_backend_reap_ms=2.523 process_io_worker_wait_ms=29678.654 process_io_future_fulfill_ms=7.939
===== CASE 4/12 pipelineBm4Threads(bm4_lz4_fixed) END exit=0 elapsed=59s 2026-06-16T20:45:40+08:00 =====
===== CASE 5/12 pipelineOld4Threads(old4_zstd_fixed) START 2026-06-16T20:45:40+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_thread_benchmark --bm_regex=pipelineOld4Threads.old4_zstd_fixed. --bm_row_container_data_bytes=10737418240 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T20:45:40+08:00 -----
  MemFree:            252299824 kB
  MemAvailable:       251006364 kB
  Buffers:                39892 kB
  Cached:               3036868 kB
  SwapCached:                 0 kB
  Dirty:                   1328 kB
  Writeback:                  0 kB
  Shmem:                2608668 kB
  SReclaimable:          175772 kB
----- meminfo after sync before drop_caches 2026-06-16T20:45:40+08:00 -----
  MemFree:            252299824 kB
  MemAvailable:       251006364 kB
  Buffers:                39920 kB
  Cached:               3036864 kB
  SwapCached:                 0 kB
  Dirty:                    424 kB
  Writeback:                  0 kB
  Shmem:                2608668 kB
  SReclaimable:          175772 kB
----- meminfo after drop_caches 2026-06-16T20:45:41+08:00 -----
  MemFree:            252522804 kB
  MemAvailable:       251119900 kB
  Buffers:                 9196 kB
  Cached:               2866628 kB
  SwapCached:                 0 kB
  Dirty:                    188 kB
  Writeback:                116 kB
  Shmem:                2608648 kB
  SReclaimable:          158356 kB
[bm-row-container-metrics] pipelineOld4Threads dataset=fixed compression=zstd iterations=1 threads=4 per_thread_logical_bytes=10737418240 total_logical_bytes=42949672960 rows_per_thread=536870912 total_rows=2147483648 sum_store_ms=40048.377 sum_spill_write_ms=358166.259 sum_spill_read_ms=205900.055 sum_read_ms=11563.673 sum_total_ms=615678.364 wall_ms=157646.371 parallel_efficiency=3.905 spill_bytes=37795322188 files=43016 batches=43016
===== CASE 5/12 pipelineOld4Threads(old4_zstd_fixed) END exit=0 elapsed=160s 2026-06-16T20:48:41+08:00 =====
===== CASE 6/12 pipelineBm4Threads(bm4_zstd_fixed) START 2026-06-16T20:48:41+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_thread_benchmark --bm_regex=pipelineBm4Threads.bm4_zstd_fixed. --bm_row_container_data_bytes=10737418240 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T20:48:41+08:00 -----
  MemFree:            252260732 kB
  MemAvailable:       250989240 kB
  Buffers:                49672 kB
  Cached:               3048508 kB
  SwapCached:                 0 kB
  Dirty:                   1056 kB
  Writeback:                  0 kB
  Shmem:                2608520 kB
  SReclaimable:          186996 kB
----- meminfo after sync before drop_caches 2026-06-16T20:48:41+08:00 -----
  MemFree:            252260164 kB
  MemAvailable:       250988672 kB
  Buffers:                49688 kB
  Cached:               3048512 kB
  SwapCached:                 0 kB
  Dirty:                    776 kB
  Writeback:                  0 kB
  Shmem:                2608524 kB
  SReclaimable:          186996 kB
----- meminfo after drop_caches 2026-06-16T20:48:41+08:00 -----
  MemFree:            252531820 kB
  MemAvailable:       251135680 kB
  Buffers:                 9224 kB
  Cached:               2867200 kB
  SwapCached:                 0 kB
  Dirty:                    396 kB
  Writeback:                100 kB
  Shmem:                2608524 kB
  SReclaimable:          160028 kB
[bm-row-container-metrics] pipelineBm4Threads dataset=fixed compression=zstd iterations=1 threads=4 per_thread_logical_bytes=10737418240 total_logical_bytes=42949672960 rows_per_thread=536870912 total_rows=2147483648 sum_store_ms=13323.518 sum_spill_write_ms=286619.954 sum_spill_read_ms=110733.380 sum_read_ms=7393.539 sum_total_ms=418070.390 wall_ms=138912.227 parallel_efficiency=3.010 spill_write_count=12292 spill_write_bytes=51556384768 spill_physical_write_bytes=37266313284 spill_read_count=12292 spill_read_bytes=51556384768 spill_physical_read_bytes=37266313284 process_io_accepted=24584 process_io_completed=24584 process_io_completed_bytes=74532626568 process_io_successful=24584 process_io_failed=0 process_io_rejected=0 process_io_submit_batches=12067 process_io_completion_batches=12105 process_io_queue_wait_ms=77826850.988 process_io_avg_queue_wait_us=3165752.155 process_io_device_latency_ms=1112952.476 process_io_avg_device_latency_us=45271.415 process_io_end_to_end_latency_ms=78939815.967 process_io_avg_end_to_end_latency_us=3211024.079 process_io_backend_submit_ms=16755.293 process_io_backend_reap_ms=0.539 process_io_worker_wait_ms=73293.288 process_io_future_fulfill_ms=5.418
===== CASE 6/12 pipelineBm4Threads(bm4_zstd_fixed) END exit=0 elapsed=142s 2026-06-16T20:51:23+08:00 =====
===== CASE 7/12 pipelineOld4Threads(old4_raw_variable) START 2026-06-16T20:51:23+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_thread_benchmark --bm_regex=pipelineOld4Threads.old4_raw_variable. --bm_row_container_data_bytes=10737418240 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T20:51:23+08:00 -----
  MemFree:            252315316 kB
  MemAvailable:       251020064 kB
  Buffers:                39612 kB
  Cached:               3026156 kB
  SwapCached:                 0 kB
  Dirty:                    888 kB
  Writeback:                  0 kB
  Shmem:                2608972 kB
  SReclaimable:          172332 kB
----- meminfo after sync before drop_caches 2026-06-16T20:51:23+08:00 -----
  MemFree:            252314756 kB
  MemAvailable:       251019504 kB
  Buffers:                39628 kB
  Cached:               3026140 kB
  SwapCached:                 0 kB
  Dirty:                    296 kB
  Writeback:                  0 kB
  Shmem:                2608972 kB
  SReclaimable:          172332 kB
----- meminfo after drop_caches 2026-06-16T20:51:23+08:00 -----
  MemFree:            252521440 kB
  MemAvailable:       251125072 kB
  Buffers:                 8860 kB
  Cached:               2868532 kB
  SwapCached:                 0 kB
  Dirty:                    280 kB
  Writeback:                  0 kB
  Shmem:                2608976 kB
  SReclaimable:          159036 kB
[bm-row-container-metrics] pipelineOld4Threads dataset=variable compression=raw iterations=1 threads=4 per_thread_logical_bytes=10737418240 total_logical_bytes=42949672960 rows_per_thread=10284884 total_rows=41139536 sum_store_ms=9725.535 sum_spill_write_ms=147242.599 sum_spill_read_ms=74164.112 sum_read_ms=5060.904 sum_total_ms=236193.150 wall_ms=62471.968 parallel_efficiency=3.781 spill_bytes=43649380816 files=41640 batches=41640
===== CASE 7/12 pipelineOld4Threads(old4_raw_variable) END exit=0 elapsed=64s 2026-06-16T20:52:47+08:00 =====
===== CASE 8/12 pipelineBm4Threads(bm4_raw_variable) START 2026-06-16T20:52:47+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_thread_benchmark --bm_regex=pipelineBm4Threads.bm4_raw_variable. --bm_row_container_data_bytes=10737418240 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T20:52:47+08:00 -----
  MemFree:            252265996 kB
  MemAvailable:       250987664 kB
  Buffers:                41956 kB
  Cached:               3037948 kB
  SwapCached:                 0 kB
  Dirty:                    384 kB
  Writeback:                  0 kB
  Shmem:                2608188 kB
  SReclaimable:          191276 kB
----- meminfo after sync before drop_caches 2026-06-16T20:52:47+08:00 -----
  MemFree:            252265996 kB
  MemAvailable:       250987664 kB
  Buffers:                41972 kB
  Cached:               3037936 kB
  SwapCached:                 0 kB
  Dirty:                    384 kB
  Writeback:                  0 kB
  Shmem:                2608188 kB
  SReclaimable:          191276 kB
----- meminfo after drop_caches 2026-06-16T20:52:47+08:00 -----
  MemFree:            252521272 kB
  MemAvailable:       251127548 kB
  Buffers:                 9840 kB
  Cached:               2867604 kB
  SwapCached:                 0 kB
  Dirty:                    296 kB
  Writeback:                120 kB
  Shmem:                2608188 kB
  SReclaimable:          162508 kB
[bm-row-container-metrics] pipelineBm4Threads dataset=variable compression=raw iterations=1 threads=4 per_thread_logical_bytes=10737418240 total_logical_bytes=42949672960 rows_per_thread=10284884 total_rows=41139536 sum_store_ms=13737.685 sum_spill_write_ms=50029.403 sum_spill_read_ms=47364.100 sum_read_ms=4894.671 sum_total_ms=116025.859 wall_ms=75984.292 parallel_efficiency=1.527 spill_write_count=10600 spill_write_bytes=44459622400 spill_physical_write_bytes=44459961600 spill_read_count=10600 spill_read_bytes=44459622400 spill_physical_read_bytes=44459961600 process_io_accepted=21200 process_io_completed=21200 process_io_completed_bytes=88919923200 process_io_successful=21200 process_io_failed=0 process_io_rejected=0 process_io_submit_batches=10150 process_io_completion_batches=10059 process_io_queue_wait_ms=95493874.578 process_io_avg_queue_wait_us=4504428.046 process_io_device_latency_ms=2732939.845 process_io_avg_device_latency_us=128912.257 process_io_end_to_end_latency_ms=98226824.946 process_io_avg_end_to_end_latency_us=4633340.799 process_io_backend_submit_ms=19858.329 process_io_backend_reap_ms=1.694 process_io_worker_wait_ms=15251.948 process_io_future_fulfill_ms=8.837
===== CASE 8/12 pipelineBm4Threads(bm4_raw_variable) END exit=0 elapsed=79s 2026-06-16T20:54:26+08:00 =====
===== CASE 9/12 pipelineOld4Threads(old4_lz4_variable) START 2026-06-16T20:54:26+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_thread_benchmark --bm_regex=pipelineOld4Threads.old4_lz4_variable. --bm_row_container_data_bytes=10737418240 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T20:54:26+08:00 -----
  MemFree:            252368820 kB
  MemAvailable:       251054656 kB
  Buffers:                33036 kB
  Cached:               2994552 kB
  SwapCached:                 0 kB
  Dirty:                    476 kB
  Writeback:                  0 kB
  Shmem:                2608444 kB
  SReclaimable:          172208 kB
----- meminfo after sync before drop_caches 2026-06-16T20:54:26+08:00 -----
  MemFree:            252368820 kB
  MemAvailable:       251054656 kB
  Buffers:                33052 kB
  Cached:               2994544 kB
  SwapCached:                 0 kB
  Dirty:                    120 kB
  Writeback:                376 kB
  Shmem:                2608444 kB
  SReclaimable:          172208 kB
----- meminfo after drop_caches 2026-06-16T20:54:26+08:00 -----
  MemFree:            252535596 kB
  MemAvailable:       251139668 kB
  Buffers:                 8816 kB
  Cached:               2869604 kB
  SwapCached:                 0 kB
  Dirty:                    120 kB
  Writeback:                376 kB
  Shmem:                2608444 kB
  SReclaimable:          157852 kB
[bm-row-container-metrics] pipelineOld4Threads dataset=variable compression=lz4 iterations=1 threads=4 per_thread_logical_bytes=10737418240 total_logical_bytes=42949672960 rows_per_thread=10284884 total_rows=41139536 sum_store_ms=9665.171 sum_spill_write_ms=20371.075 sum_spill_read_ms=35867.587 sum_read_ms=5087.850 sum_total_ms=70991.682 wall_ms=19913.173 parallel_efficiency=3.565 spill_bytes=1971377800 files=41640 batches=41640
===== CASE 9/12 pipelineOld4Threads(old4_lz4_variable) END exit=0 elapsed=21s 2026-06-16T20:55:07+08:00 =====
===== CASE 10/12 pipelineBm4Threads(bm4_lz4_variable) START 2026-06-16T20:55:07+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_thread_benchmark --bm_regex=pipelineBm4Threads.bm4_lz4_variable. --bm_row_container_data_bytes=10737418240 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T20:55:07+08:00 -----
  MemFree:            252364624 kB
  MemAvailable:       251054856 kB
  Buffers:                39432 kB
  Cached:               2988912 kB
  SwapCached:                 0 kB
  Dirty:                    524 kB
  Writeback:                  0 kB
  Shmem:                2608592 kB
  SReclaimable:          180380 kB
----- meminfo after sync before drop_caches 2026-06-16T20:55:07+08:00 -----
  MemFree:            252364624 kB
  MemAvailable:       251054856 kB
  Buffers:                39448 kB
  Cached:               2988904 kB
  SwapCached:                 0 kB
  Dirty:                    552 kB
  Writeback:                  0 kB
  Shmem:                2608592 kB
  SReclaimable:          180380 kB
----- meminfo after drop_caches 2026-06-16T20:55:07+08:00 -----
  MemFree:            252539288 kB
  MemAvailable:       251143724 kB
  Buffers:                 9548 kB
  Cached:               2869632 kB
  SwapCached:                 0 kB
  Dirty:                    556 kB
  Writeback:                  0 kB
  Shmem:                2608592 kB
  SReclaimable:          157964 kB
[bm-row-container-metrics] pipelineBm4Threads dataset=variable compression=lz4 iterations=1 threads=4 per_thread_logical_bytes=10737418240 total_logical_bytes=42949672960 rows_per_thread=10284884 total_rows=41139536 sum_store_ms=13611.274 sum_spill_write_ms=10607.279 sum_spill_read_ms=19756.463 sum_read_ms=4605.932 sum_total_ms=48580.948 wall_ms=14055.802 parallel_efficiency=3.456 spill_write_count=10600 spill_write_bytes=44459622400 spill_physical_write_bytes=1916264300 spill_read_count=10600 spill_read_bytes=44459622400 spill_physical_read_bytes=1916264300 process_io_accepted=21200 process_io_completed=21200 process_io_completed_bytes=3832528600 process_io_successful=21200 process_io_failed=0 process_io_rejected=0 process_io_submit_batches=10339 process_io_completion_batches=10507 process_io_queue_wait_ms=4266989.040 process_io_avg_queue_wait_us=201273.068 process_io_device_latency_ms=65093.480 process_io_avg_device_latency_us=3070.447 process_io_end_to_end_latency_ms=4332092.710 process_io_avg_end_to_end_latency_us=204343.996 process_io_backend_submit_ms=921.920 process_io_backend_reap_ms=0.474 process_io_worker_wait_ms=6338.002 process_io_future_fulfill_ms=4.571
===== CASE 10/12 pipelineBm4Threads(bm4_lz4_variable) END exit=0 elapsed=16s 2026-06-16T20:55:43+08:00 =====
===== CASE 11/12 pipelineOld4Threads(old4_zstd_variable) START 2026-06-16T20:55:43+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_thread_benchmark --bm_regex=pipelineOld4Threads.old4_zstd_variable. --bm_row_container_data_bytes=10737418240 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T20:55:43+08:00 -----
  MemFree:            252402304 kB
  MemAvailable:       251070416 kB
  Buffers:                28264 kB
  Cached:               2968020 kB
  SwapCached:                 0 kB
  Dirty:                   1036 kB
  Writeback:                  0 kB
  Shmem:                2608104 kB
  SReclaimable:          167756 kB
----- meminfo after sync before drop_caches 2026-06-16T20:55:43+08:00 -----
  MemFree:            252402304 kB
  MemAvailable:       251070416 kB
  Buffers:                28284 kB
  Cached:               2968020 kB
  SwapCached:                 0 kB
  Dirty:                    292 kB
  Writeback:                  0 kB
  Shmem:                2608104 kB
  SReclaimable:          167756 kB
----- meminfo after drop_caches 2026-06-16T20:55:43+08:00 -----
  MemFree:            252533976 kB
  MemAvailable:       251137772 kB
  Buffers:                 9176 kB
  Cached:               2869360 kB
  SwapCached:                 0 kB
  Dirty:                    304 kB
  Writeback:                  0 kB
  Shmem:                2608080 kB
  SReclaimable:          156872 kB
[bm-row-container-metrics] pipelineOld4Threads dataset=variable compression=zstd iterations=1 threads=4 per_thread_logical_bytes=10737418240 total_logical_bytes=42949672960 rows_per_thread=10284884 total_rows=41139536 sum_store_ms=9672.298 sum_spill_write_ms=27213.793 sum_spill_read_ms=37463.540 sum_read_ms=5091.184 sum_total_ms=79440.815 wall_ms=21948.133 parallel_efficiency=3.619 spill_bytes=1367439196 files=41640 batches=41640
===== CASE 11/12 pipelineOld4Threads(old4_zstd_variable) END exit=0 elapsed=23s 2026-06-16T20:56:26+08:00 =====
===== CASE 12/12 pipelineBm4Threads(bm4_zstd_variable) START 2026-06-16T20:56:26+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_thread_benchmark --bm_regex=pipelineBm4Threads.bm4_zstd_variable. --bm_row_container_data_bytes=10737418240 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T20:56:26+08:00 -----
  MemFree:            252301088 kB
  MemAvailable:       251007348 kB
  Buffers:                45864 kB
  Cached:               3026116 kB
  SwapCached:                 0 kB
  Dirty:                    428 kB
  Writeback:                  0 kB
  Shmem:                2608224 kB
  SReclaimable:          178912 kB
----- meminfo after sync before drop_caches 2026-06-16T20:56:26+08:00 -----
  MemFree:            252300520 kB
  MemAvailable:       251006784 kB
  Buffers:                45900 kB
  Cached:               3026100 kB
  SwapCached:                 0 kB
  Dirty:                    428 kB
  Writeback:                  0 kB
  Shmem:                2608224 kB
  SReclaimable:          178912 kB
----- meminfo after drop_caches 2026-06-16T20:56:26+08:00 -----
  MemFree:            252516764 kB
  MemAvailable:       251115832 kB
  Buffers:                 9420 kB
  Cached:               2870060 kB
  SwapCached:                 0 kB
  Dirty:                    472 kB
  Writeback:                  0 kB
  Shmem:                2608200 kB
  SReclaimable:          157544 kB
[bm-row-container-metrics] pipelineBm4Threads dataset=variable compression=zstd iterations=1 threads=4 per_thread_logical_bytes=10737418240 total_logical_bytes=42949672960 rows_per_thread=10284884 total_rows=41139536 sum_store_ms=13369.865 sum_spill_write_ms=24554.726 sum_spill_read_ms=21709.855 sum_read_ms=4596.617 sum_total_ms=64231.062 wall_ms=17765.961 parallel_efficiency=3.615 spill_write_count=10600 spill_write_bytes=44459622400 spill_physical_write_bytes=1349954778 spill_read_count=10600 spill_read_bytes=44459622400 spill_physical_read_bytes=1349954778 process_io_accepted=21200 process_io_completed=21200 process_io_completed_bytes=2699909556 process_io_successful=21200 process_io_failed=0 process_io_rejected=0 process_io_submit_batches=10191 process_io_completion_batches=10335 process_io_queue_wait_ms=1428898.744 process_io_avg_queue_wait_us=67400.884 process_io_device_latency_ms=47203.389 process_io_avg_device_latency_us=2226.575 process_io_end_to_end_latency_ms=1476112.681 process_io_avg_end_to_end_latency_us=69627.957 process_io_backend_submit_ms=704.737 process_io_backend_reap_ms=5.865 process_io_worker_wait_ms=9631.839 process_io_future_fulfill_ms=10.875
===== CASE 12/12 pipelineBm4Threads(bm4_zstd_variable) END exit=0 elapsed=19s 2026-06-16T20:57:05+08:00 =====
===== ALL CASES END exit=0 output_dir=/data00/home/wangxinshuo.db/bolt/log/bolt-bm-row-container-pipeline-thread-20260616-203529 2026-06-16T20:57:05+08:00 =====

```

### stdout.txt
```
============================================================================
[...]wContainerPipelineThreadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineOld4Threads(old4_raw_fixed)                        1.99min     8.38m
----------------------------------------------------------------------------
============================================================================
[...]wContainerPipelineThreadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineBm4Threads(bm4_raw_fixed)                          1.87min     8.91m
----------------------------------------------------------------------------
============================================================================
[...]wContainerPipelineThreadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineOld4Threads(old4_lz4_fixed)                        3.89min     4.28m
----------------------------------------------------------------------------
============================================================================
[...]wContainerPipelineThreadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineBm4Threads(bm4_lz4_fixed)                           57.17s    17.49m
----------------------------------------------------------------------------
============================================================================
[...]wContainerPipelineThreadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineOld4Threads(old4_zstd_fixed)                       2.63min     6.34m
----------------------------------------------------------------------------
============================================================================
[...]wContainerPipelineThreadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineBm4Threads(bm4_zstd_fixed)                         2.32min     7.20m
----------------------------------------------------------------------------
============================================================================
[...]wContainerPipelineThreadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineOld4Threads(old4_raw_variable)                     1.04min    16.01m
----------------------------------------------------------------------------
============================================================================
[...]wContainerPipelineThreadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineBm4Threads(bm4_raw_variable)                       1.27min    13.16m
----------------------------------------------------------------------------
============================================================================
[...]wContainerPipelineThreadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineOld4Threads(old4_lz4_variable)                      19.91s    50.22m
----------------------------------------------------------------------------
============================================================================
[...]wContainerPipelineThreadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineBm4Threads(bm4_lz4_variable)                        14.06s    71.14m
----------------------------------------------------------------------------
============================================================================
[...]wContainerPipelineThreadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineOld4Threads(old4_zstd_variable)                     21.95s    45.56m
----------------------------------------------------------------------------
============================================================================
[...]wContainerPipelineThreadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineBm4Threads(bm4_zstd_variable)                       17.77s    56.29m
----------------------------------------------------------------------------

```
