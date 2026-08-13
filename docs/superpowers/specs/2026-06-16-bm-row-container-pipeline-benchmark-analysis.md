# 2026-06-16 BM RowContainer pipeline benchmark 运行结果分析

## 数据来源

- 运行日志目录：`/data00/home/wangxinshuo.db/bolt/log/bolt-bm-row-container-pipeline-20260616-172604`
- benchmark stdout：`stdout.txt`
- runner stderr 与 pipeline metric：`stderr.txt`
- 本文只分析本次 pipeline 运行结果，不和历史运行结果对比。

## 运行概况

- 一共运行 12 个 case，全部 `exit=0`。
- 每个 case 单独进程运行，命令里统一使用：
  - `--bm_row_container_data_bytes=26843545600`，即 25 GiB 逻辑数据量。
  - `--bm_row_container_warmup_data_bytes=134217728`，即 128 MiB same-process warmup 数据量。
  - `timeout 900s`。
- runner 在每个 case 前执行 `sync` 和 `drop_caches`，并打印 `/proc/meminfo`。从日志看，drop 后 `Cached` 基本稳定在约 3.0 GiB，`Dirty` 和 `Writeback` 很小，case 间 page cache 干扰已经被明显控制。
- pipeline benchmark 覆盖的是 end-to-end 链路：store、spill write、spill read、read。BM 路径的 store 使用 batch append。

## 总体结论

1. 本次运行里，BM RowContainer 在所有 pipeline 组合上都快于 old RowContainer。
2. fixed raw 是 BM 优势最大的场景，总耗时从 219.36s 降到 40.55s，约 5.41x。
3. fixed zstd 是 BM 优势最小的 fixed 场景，总耗时从 364.73s 降到 243.21s，约 1.50x。主要原因是 zstd 压缩成本主导了 BM 的 spill write。
4. variable 场景里 BM 也全部领先，但 store/read 阶段优势不大，主要收益来自 spill write/read 链路。
5. 对 fixed 数据，压缩收益有限，lz4/zstd 主要增加压缩 CPU 成本；对 variable 数据，压缩效果非常明显，lz4/zstd 将物理写出从约 25.87 GiB 降到约 1.12 GiB/0.78 GiB。

## 总耗时

| dataset | compression | old total | BM total | BM 加速比 | BM / old |
|---|---|---:|---:|---:|---:|
| fixed | raw | 219.36s | 40.55s | 5.41x | 18.5% |
| fixed | lz4 | 232.67s | 92.29s | 2.52x | 39.7% |
| fixed | zstd | 364.73s | 243.21s | 1.50x | 66.7% |
| variable | raw | 74.85s | 30.46s | 2.46x | 40.7% |
| variable | lz4 | 39.58s | 24.07s | 1.64x | 60.8% |
| variable | zstd | 45.61s | 34.93s | 1.31x | 76.6% |

## Fixed 场景

### 阶段耗时

| compression | impl | store | spill write | spill read | read | total |
|---|---|---:|---:|---:|---:|---:|
| raw | old | 26.50s | 42.40s | 143.48s | 6.98s | 219.36s |
| raw | BM | 6.72s | 9.99s | 18.99s | 4.86s | 40.55s |
| lz4 | old | 26.35s | 95.81s | 103.50s | 7.00s | 232.67s |
| lz4 | BM | 6.73s | 62.24s | 18.48s | 4.85s | 92.29s |
| zstd | old | 26.56s | 217.19s | 113.96s | 7.02s | 364.73s |
| zstd | BM | 6.68s | 174.90s | 56.80s | 4.83s | 243.21s |

### 阶段加速比

| compression | store | spill write | spill read | read | total |
|---|---:|---:|---:|---:|---:|
| raw | 3.95x | 4.24x | 7.56x | 1.44x | 5.41x |
| lz4 | 3.92x | 1.54x | 5.60x | 1.44x | 2.52x |
| zstd | 3.98x | 1.24x | 2.01x | 1.45x | 1.50x |

### 物理 spill 大小

| compression | old spill bytes | BM physical spill bytes | BM / old |
|---|---:|---:|---:|
| raw | 26.25 GiB | 30.00 GiB | 114.3% |
| lz4 | 25.39 GiB | 25.86 GiB | 101.9% |
| zstd | 22.00 GiB | 21.69 GiB | 98.6% |

### 分析

fixed 场景下 BM 的 store 一直约 4x 快于 old，这是 batch append 在固定宽度数据上的主要收益。

fixed raw 里 BM 物理写出比 old 更多，但总耗时仍然只有 old 的 18.5%。这说明 raw fixed 的主要差距不是总写出字节数，而是 old path 在 spill read/rebuild 上成本非常重。old raw fixed 的 `spill_read_ms=143.48s` 是该 case 最大的单项耗时。

fixed 压缩场景下，BM 的瓶颈明显转向 spill write：

| compression | BM store 占比 | BM spill write 占比 | BM spill read 占比 | BM read 占比 |
|---|---:|---:|---:|---:|
| raw | 16.6% | 24.6% | 46.8% | 12.0% |
| lz4 | 7.3% | 67.4% | 20.0% | 5.3% |
| zstd | 2.7% | 71.9% | 23.4% | 2.0% |

fixed lz4/zstd 的物理写出下降不明显，但压缩 CPU 成本很高，因此总收益被压缩阶段限制。尤其 fixed zstd，BM 虽然仍快于 old，但总加速比只有 1.50x。

## Variable 场景

### 阶段耗时

| compression | impl | store | spill write | spill read | read | total |
|---|---|---:|---:|---:|---:|---:|
| raw | old | 5.60s | 14.89s | 51.36s | 3.00s | 74.85s |
| raw | BM | 5.08s | 8.95s | 13.65s | 2.78s | 30.46s |
| lz4 | old | 5.63s | 12.57s | 18.43s | 2.96s | 39.58s |
| lz4 | BM | 4.83s | 6.35s | 10.06s | 2.84s | 24.07s |
| zstd | old | 5.62s | 16.79s | 20.20s | 3.01s | 45.61s |
| zstd | BM | 5.34s | 14.70s | 12.10s | 2.79s | 34.93s |

### 阶段加速比

| compression | store | spill write | spill read | read | total |
|---|---:|---:|---:|---:|---:|
| raw | 1.10x | 1.66x | 3.76x | 1.08x | 2.46x |
| lz4 | 1.16x | 1.98x | 1.83x | 1.04x | 1.64x |
| zstd | 1.05x | 1.14x | 1.67x | 1.08x | 1.31x |

### 物理 spill 大小

| compression | old spill bytes | BM physical spill bytes | BM / old |
|---|---:|---:|---:|
| raw | 25.41 GiB | 25.87 GiB | 101.8% |
| lz4 | 1.15 GiB | 1.12 GiB | 97.2% |
| zstd | 0.80 GiB | 0.78 GiB | 98.6% |

### 分析

variable 场景里，BM 的 store/read 优势不大：

- raw store：5.60s -> 5.08s，约 1.10x。
- lz4 store：5.63s -> 4.83s，约 1.16x。
- zstd store：5.62s -> 5.34s，约 1.05x。
- read 基本只有 1.04x 到 1.08x。

因此 variable pipeline 的主要收益来自 spill 链路，尤其是 raw 的 spill read：

- raw spill read：51.36s -> 13.65s，约 3.76x。
- lz4 spill write：12.57s -> 6.35s，约 1.98x。
- lz4 spill read：18.43s -> 10.06s，约 1.83x。

variable 数据对压缩非常友好。BM raw 物理写出约 25.87 GiB，lz4 只有约 1.12 GiB，zstd 只有约 0.78 GiB。但 zstd 的额外压缩 CPU 成本较高，导致 variable zstd 总体只比 old 快 1.31x，也慢于 BM variable lz4。

BM variable 各阶段占比：

| compression | BM store 占比 | BM spill write 占比 | BM spill read 占比 | BM read 占比 |
|---|---:|---:|---:|---:|
| raw | 16.7% | 29.4% | 44.8% | 9.1% |
| lz4 | 20.1% | 26.4% | 41.8% | 11.8% |
| zstd | 15.3% | 42.1% | 34.6% | 8.0% |

raw/lz4 下，BM variable 的最大占比仍是 spill read；zstd 下，spill write 占比最高。

## BM 后续优化方向

1. raw 场景优先看 `spill_read`。fixed raw 和 variable raw 里，BM 的最大占比都是 spill read。
2. compressed 场景优先看 `spill_write`，尤其 fixed lz4/zstd。fixed 数据压缩收益不明显，但压缩 CPU 成本很高。
3. variable store 不是当前 end-to-end 的主要瓶颈。继续优化 variable store 对 pipeline 总耗时的收益有限。
4. fixed store 已经通过 batch append 获得明显收益；后续如果 Window 路径能稳定走 batch append，fixed pipeline 的写入阶段已经比较理想。
5. 对 variable 数据，lz4 当前比 zstd 更适合作为默认压缩候选：压缩后大小接近，但 zstd 总耗时更高。

## 附录
### stderr.txt

```text
===== CASE 1/12 pipelineOld(old_raw_fixed) START 2026-06-16T17:26:04+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_benchmark --bm_regex=pipelineOld.old_raw_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T17:26:04+08:00 -----
  MemFree:            249569236 kB
  MemAvailable:       251222668 kB
  Buffers:               194688 kB
  Cached:               5874364 kB
  SwapCached:                 0 kB
  Dirty:                    392 kB
  Writeback:                  0 kB
  Shmem:                2608864 kB
  SReclaimable:          260884 kB
----- meminfo after sync before drop_caches 2026-06-16T17:26:04+08:00 -----
  MemFree:            249568188 kB
  MemAvailable:       251221620 kB
  Buffers:               194704 kB
  Cached:               5874356 kB
  SwapCached:                 0 kB
  Dirty:                    264 kB
  Writeback:                192 kB
  Shmem:                2608864 kB
  SReclaimable:          260884 kB
----- meminfo after drop_caches 2026-06-16T17:26:05+08:00 -----
  MemFree:            252644528 kB
  MemAvailable:       251362156 kB
  Buffers:                 8520 kB
  Cached:               3075492 kB
  SwapCached:                 0 kB
  Dirty:                    268 kB
  Writeback:                192 kB
  Shmem:                2608844 kB
  SReclaimable:          174448 kB
[bm-row-container-metrics] pipelineOld dataset=fixed compression=raw iterations=1 logical_bytes=26843545600 rows=1342177280 store_ms=26504.416 spill_write_ms=42395.876 spill_read_ms=143481.413 read_ms=6981.178 total_ms=219362.883 spill_bytes=28185937944 files=26883 batches=26883
===== CASE 1/12 pipelineOld(old_raw_fixed) END exit=0 elapsed=223s 2026-06-16T17:30:08+08:00 =====
===== CASE 2/12 pipelineBm(bm_raw_fixed) START 2026-06-16T17:30:08+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_benchmark --bm_regex=pipelineBm.bm_raw_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T17:30:08+08:00 -----
  MemFree:            252470688 kB
  MemAvailable:       251295552 kB
  Buffers:                44800 kB
  Cached:               3238076 kB
  SwapCached:                 0 kB
  Dirty:                    500 kB
  Writeback:                  0 kB
  Shmem:                2608716 kB
  SReclaimable:          175840 kB
----- meminfo after sync before drop_caches 2026-06-16T17:30:08+08:00 -----
  MemFree:            252470696 kB
  MemAvailable:       251295560 kB
  Buffers:                44816 kB
  Cached:               3238060 kB
  SwapCached:                 0 kB
  Dirty:                      0 kB
  Writeback:                  0 kB
  Shmem:                2608716 kB
  SReclaimable:          175840 kB
----- meminfo after drop_caches 2026-06-16T17:30:08+08:00 -----
  MemFree:            252701788 kB
  MemAvailable:       251411260 kB
  Buffers:                 8904 kB
  Cached:               3075604 kB
  SwapCached:                 0 kB
  Dirty:                    112 kB
  Writeback:                  0 kB
  Shmem:                2608716 kB
  SReclaimable:          157532 kB
[bm-row-container-metrics] pipelineBm dataset=fixed compression=raw iterations=1 logical_bytes=26843545600 rows=1342177280 store_ms=6715.678 spill_write_ms=9992.180 spill_read_ms=18985.789 read_ms=4855.020 total_ms=40548.666 spill_write_count=7681 spill_write_bytes=32216449024 spill_physical_write_bytes=32216694816 spill_read_count=7681 spill_read_bytes=32216449024 spill_physical_read_bytes=32216694816
===== CASE 2/12 pipelineBm(bm_raw_fixed) END exit=0 elapsed=57s 2026-06-16T17:31:25+08:00 =====
===== CASE 3/12 pipelineOld(old_lz4_fixed) START 2026-06-16T17:31:25+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_benchmark --bm_regex=pipelineOld.old_lz4_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T17:31:25+08:00 -----
  MemFree:            252509788 kB
  MemAvailable:       251321228 kB
  Buffers:                36864 kB
  Cached:               3235636 kB
  SwapCached:                 0 kB
  Dirty:                    996 kB
  Writeback:                  0 kB
  Shmem:                2607880 kB
  SReclaimable:          167988 kB
----- meminfo after sync before drop_caches 2026-06-16T17:31:25+08:00 -----
  MemFree:            252510040 kB
  MemAvailable:       251321488 kB
  Buffers:                36888 kB
  Cached:               3235628 kB
  SwapCached:                 0 kB
  Dirty:                    456 kB
  Writeback:                  0 kB
  Shmem:                2607880 kB
  SReclaimable:          167988 kB
----- meminfo after drop_caches 2026-06-16T17:31:26+08:00 -----
  MemFree:            252712684 kB
  MemAvailable:       251422428 kB
  Buffers:                 8940 kB
  Cached:               3075096 kB
  SwapCached:                 0 kB
  Dirty:                    456 kB
  Writeback:                  0 kB
  Shmem:                2607880 kB
  SReclaimable:          157664 kB
[bm-row-container-metrics] pipelineOld dataset=fixed compression=lz4 iterations=1 logical_bytes=26843545600 rows=1342177280 store_ms=26348.902 spill_write_ms=95808.335 spill_read_ms=103504.815 read_ms=7004.035 total_ms=232666.087 spill_bytes=27257858112 files=26883 batches=26883
===== CASE 3/12 pipelineOld(old_lz4_fixed) END exit=0 elapsed=236s 2026-06-16T17:35:42+08:00 =====
===== CASE 4/12 pipelineBm(bm_lz4_fixed) START 2026-06-16T17:35:42+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_benchmark --bm_regex=pipelineBm.bm_lz4_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T17:35:42+08:00 -----
  MemFree:            252470604 kB
  MemAvailable:       251292164 kB
  Buffers:                37160 kB
  Cached:               3242884 kB
  SwapCached:                 0 kB
  Dirty:                    392 kB
  Writeback:                  0 kB
  Shmem:                2608928 kB
  SReclaimable:          175320 kB
----- meminfo after sync before drop_caches 2026-06-16T17:35:42+08:00 -----
  MemFree:            252470864 kB
  MemAvailable:       251292428 kB
  Buffers:                37176 kB
  Cached:               3242876 kB
  SwapCached:                 0 kB
  Dirty:                    220 kB
  Writeback:                228 kB
  Shmem:                2608928 kB
  SReclaimable:          175320 kB
----- meminfo after drop_caches 2026-06-16T17:35:42+08:00 -----
  MemFree:            252696788 kB
  MemAvailable:       251406388 kB
  Buffers:                 8632 kB
  Cached:               3075960 kB
  SwapCached:                 0 kB
  Dirty:                    268 kB
  Writeback:                  0 kB
  Shmem:                2608952 kB
  SReclaimable:          157976 kB
[bm-row-container-metrics] pipelineBm dataset=fixed compression=lz4 iterations=1 logical_bytes=26843545600 rows=1342177280 store_ms=6725.361 spill_write_ms=62236.066 spill_read_ms=18478.021 read_ms=4852.649 total_ms=92292.098 spill_write_count=7681 spill_write_bytes=32216449024 spill_physical_write_bytes=27762192235 spill_read_count=7681 spill_read_bytes=32216449024 spill_physical_read_bytes=27762192235
===== CASE 4/12 pipelineBm(bm_lz4_fixed) END exit=0 elapsed=125s 2026-06-16T17:38:07+08:00 =====
===== CASE 5/12 pipelineOld(old_zstd_fixed) START 2026-06-16T17:38:07+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_benchmark --bm_regex=pipelineOld.old_zstd_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T17:38:07+08:00 -----
  MemFree:            252560364 kB
  MemAvailable:       251337292 kB
  Buffers:                28296 kB
  Cached:               3182584 kB
  SwapCached:                 0 kB
  Dirty:                    188 kB
  Writeback:                144 kB
  Shmem:                2608568 kB
  SReclaimable:          165916 kB
----- meminfo after sync before drop_caches 2026-06-16T17:38:07+08:00 -----
  MemFree:            252559072 kB
  MemAvailable:       251336000 kB
  Buffers:                28312 kB
  Cached:               3182568 kB
  SwapCached:                 0 kB
  Dirty:                    188 kB
  Writeback:                144 kB
  Shmem:                2608568 kB
  SReclaimable:          165916 kB
----- meminfo after drop_caches 2026-06-16T17:38:07+08:00 -----
  MemFree:            252698324 kB
  MemAvailable:       251407444 kB
  Buffers:                 8748 kB
  Cached:               3075800 kB
  SwapCached:                 0 kB
  Dirty:                     72 kB
  Writeback:                184 kB
  Shmem:                2608592 kB
  SReclaimable:          157156 kB
[bm-row-container-metrics] pipelineOld dataset=fixed compression=zstd iterations=1 logical_bytes=26843545600 rows=1342177280 store_ms=26557.653 spill_write_ms=217193.680 spill_read_ms=113963.087 read_ms=7017.602 total_ms=364732.023 spill_bytes=23622070679 files=26883 batches=26883
===== CASE 5/12 pipelineOld(old_zstd_fixed) END exit=0 elapsed=369s 2026-06-16T17:44:36+08:00 =====
===== CASE 6/12 pipelineBm(bm_zstd_fixed) START 2026-06-16T17:44:36+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_benchmark --bm_regex=pipelineBm.bm_zstd_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T17:44:36+08:00 -----
  MemFree:            252454956 kB
  MemAvailable:       251303004 kB
  Buffers:                40184 kB
  Cached:               3265176 kB
  SwapCached:                 0 kB
  Dirty:                    424 kB
  Writeback:                  0 kB
  Shmem:                2608488 kB
  SReclaimable:          176792 kB
----- meminfo after sync before drop_caches 2026-06-16T17:44:36+08:00 -----
  MemFree:            252454412 kB
  MemAvailable:       251302460 kB
  Buffers:                40200 kB
  Cached:               3265176 kB
  SwapCached:                 0 kB
  Dirty:                    252 kB
  Writeback:                  0 kB
  Shmem:                2608488 kB
  SReclaimable:          176792 kB
----- meminfo after drop_caches 2026-06-16T17:44:36+08:00 -----
  MemFree:            252705184 kB
  MemAvailable:       251414320 kB
  Buffers:                 8392 kB
  Cached:               3075924 kB
  SwapCached:                 0 kB
  Dirty:                    272 kB
  Writeback:                  0 kB
  Shmem:                2608488 kB
  SReclaimable:          156888 kB
[bm-row-container-metrics] pipelineBm dataset=fixed compression=zstd iterations=1 logical_bytes=26843545600 rows=1342177280 store_ms=6679.351 spill_write_ms=174901.704 spill_read_ms=56799.705 read_ms=4828.058 total_ms=243208.818 spill_write_count=7681 spill_write_bytes=32216449024 spill_physical_write_bytes=23291447798 spill_read_count=7681 spill_read_bytes=32216449024 spill_physical_read_bytes=23291447798
===== CASE 6/12 pipelineBm(bm_zstd_fixed) END exit=0 elapsed=249s 2026-06-16T17:49:05+08:00 =====
===== CASE 7/12 pipelineOld(old_raw_variable) START 2026-06-16T17:49:05+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_benchmark --bm_regex=pipelineOld.old_raw_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T17:49:05+08:00 -----
  MemFree:            252541452 kB
  MemAvailable:       251333352 kB
  Buffers:                30652 kB
  Cached:               3209072 kB
  SwapCached:                 0 kB
  Dirty:                    352 kB
  Writeback:                  0 kB
  Shmem:                2608472 kB
  SReclaimable:          166920 kB
----- meminfo after sync before drop_caches 2026-06-16T17:49:05+08:00 -----
  MemFree:            252540892 kB
  MemAvailable:       251332792 kB
  Buffers:                30668 kB
  Cached:               3209056 kB
  SwapCached:                 0 kB
  Dirty:                      0 kB
  Writeback:                452 kB
  Shmem:                2608472 kB
  SReclaimable:          166920 kB
----- meminfo after drop_caches 2026-06-16T17:49:05+08:00 -----
  MemFree:            252710136 kB
  MemAvailable:       251419172 kB
  Buffers:                 8712 kB
  Cached:               3075600 kB
  SwapCached:                 0 kB
  Dirty:                    216 kB
  Writeback:                 92 kB
  Shmem:                2608476 kB
  SReclaimable:          156636 kB
[bm-row-container-metrics] pipelineOld dataset=variable compression=raw iterations=1 logical_bytes=26843545600 rows=25712209 store_ms=5600.574 spill_write_ms=14892.470 spill_read_ms=51355.155 read_ms=2998.412 total_ms=74846.610 spill_bytes=27280861949 files=26025 batches=26025
===== CASE 7/12 pipelineOld(old_raw_variable) END exit=0 elapsed=78s 2026-06-16T17:50:43+08:00 =====
===== CASE 8/12 pipelineBm(bm_raw_variable) START 2026-06-16T17:50:43+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_benchmark --bm_regex=pipelineBm.bm_raw_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T17:50:43+08:00 -----
  MemFree:            252515796 kB
  MemAvailable:       251307240 kB
  Buffers:                34264 kB
  Cached:               3196380 kB
  SwapCached:                 0 kB
  Dirty:                    512 kB
  Writeback:                  0 kB
  Shmem:                2608708 kB
  SReclaimable:          175268 kB
----- meminfo after sync before drop_caches 2026-06-16T17:50:43+08:00 -----
  MemFree:            252515796 kB
  MemAvailable:       251307240 kB
  Buffers:                34280 kB
  Cached:               3196376 kB
  SwapCached:                 0 kB
  Dirty:                    116 kB
  Writeback:                412 kB
  Shmem:                2608708 kB
  SReclaimable:          175268 kB
----- meminfo after drop_caches 2026-06-16T17:50:43+08:00 -----
  MemFree:            252691552 kB
  MemAvailable:       251401224 kB
  Buffers:                 9064 kB
  Cached:               3074932 kB
  SwapCached:                 0 kB
  Dirty:                    176 kB
  Writeback:                412 kB
  Shmem:                2608708 kB
  SReclaimable:          157380 kB
[bm-row-container-metrics] pipelineBm dataset=variable compression=raw iterations=1 logical_bytes=26843545600 rows=25712209 store_ms=5079.825 spill_write_ms=8947.800 spill_read_ms=13654.397 read_ms=2780.526 total_ms=30462.547 spill_write_count=6622 spill_write_bytes=27774681088 spill_physical_write_bytes=27774892992 spill_read_count=6622 spill_read_bytes=27774681088 spill_physical_read_bytes=27774892992
===== CASE 8/12 pipelineBm(bm_raw_variable) END exit=0 elapsed=39s 2026-06-16T17:51:43+08:00 =====
===== CASE 9/12 pipelineOld(old_lz4_variable) START 2026-06-16T17:51:43+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_benchmark --bm_regex=pipelineOld.old_lz4_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T17:51:43+08:00 -----
  MemFree:            252528428 kB
  MemAvailable:       251323028 kB
  Buffers:                28664 kB
  Cached:               3217384 kB
  SwapCached:                 0 kB
  Dirty:                    552 kB
  Writeback:                  0 kB
  Shmem:                2608828 kB
  SReclaimable:          166284 kB
----- meminfo after sync before drop_caches 2026-06-16T17:51:43+08:00 -----
  MemFree:            252528428 kB
  MemAvailable:       251323028 kB
  Buffers:                28684 kB
  Cached:               3217368 kB
  SwapCached:                 0 kB
  Dirty:                    468 kB
  Writeback:                 84 kB
  Shmem:                2608828 kB
  SReclaimable:          166284 kB
----- meminfo after drop_caches 2026-06-16T17:51:43+08:00 -----
  MemFree:            252704184 kB
  MemAvailable:       251412244 kB
  Buffers:                 8768 kB
  Cached:               3075924 kB
  SwapCached:                 0 kB
  Dirty:                    344 kB
  Writeback:                  0 kB
  Shmem:                2608828 kB
  SReclaimable:          155076 kB
[bm-row-container-metrics] pipelineOld dataset=variable compression=lz4 iterations=1 logical_bytes=26843545600 rows=25712209 store_ms=5625.697 spill_write_ms=12567.203 spill_read_ms=18430.782 read_ms=2955.030 total_ms=39578.712 spill_bytes=1232145318 files=26025 batches=26025
===== CASE 9/12 pipelineOld(old_lz4_variable) END exit=0 elapsed=43s 2026-06-16T17:52:46+08:00 =====
===== CASE 10/12 pipelineBm(bm_lz4_variable) START 2026-06-16T17:52:46+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_benchmark --bm_regex=pipelineBm.bm_lz4_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T17:52:46+08:00 -----
  MemFree:            252467256 kB
  MemAvailable:       251308588 kB
  Buffers:                37488 kB
  Cached:               3262376 kB
  SwapCached:                 0 kB
  Dirty:                    476 kB
  Writeback:                  0 kB
  Shmem:                2608972 kB
  SReclaimable:          175280 kB
----- meminfo after sync before drop_caches 2026-06-16T17:52:46+08:00 -----
  MemFree:            252467256 kB
  MemAvailable:       251308588 kB
  Buffers:                37504 kB
  Cached:               3262372 kB
  SwapCached:                 0 kB
  Dirty:                    448 kB
  Writeback:                  0 kB
  Shmem:                2608976 kB
  SReclaimable:          175280 kB
----- meminfo after drop_caches 2026-06-16T17:52:46+08:00 -----
  MemFree:            252702756 kB
  MemAvailable:       251411844 kB
  Buffers:                 9964 kB
  Cached:               3076144 kB
  SwapCached:                 0 kB
  Dirty:                    448 kB
  Writeback:                  0 kB
  Shmem:                2608976 kB
  SReclaimable:          155920 kB
[bm-row-container-metrics] pipelineBm dataset=variable compression=lz4 iterations=1 logical_bytes=26843545600 rows=25712209 store_ms=4829.467 spill_write_ms=6349.755 spill_read_ms=10056.147 read_ms=2838.721 total_ms=24074.090 spill_write_count=6622 spill_write_bytes=27774681088 spill_physical_write_bytes=1197661820 spill_read_count=6622 spill_read_bytes=27774681088 spill_physical_read_bytes=1197661820
===== CASE 10/12 pipelineBm(bm_lz4_variable) END exit=0 elapsed=27s 2026-06-16T17:53:33+08:00 =====
===== CASE 11/12 pipelineOld(old_zstd_variable) START 2026-06-16T17:53:33+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_benchmark --bm_regex=pipelineOld.old_zstd_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T17:53:33+08:00 -----
  MemFree:            252581712 kB
  MemAvailable:       251352516 kB
  Buffers:                26168 kB
  Cached:               3175900 kB
  SwapCached:                 0 kB
  Dirty:                   1032 kB
  Writeback:                  0 kB
  Shmem:                2609084 kB
  SReclaimable:          162992 kB
----- meminfo after sync before drop_caches 2026-06-16T17:53:33+08:00 -----
  MemFree:            252581712 kB
  MemAvailable:       251352516 kB
  Buffers:                26200 kB
  Cached:               3175884 kB
  SwapCached:                 0 kB
  Dirty:                    652 kB
  Writeback:                  0 kB
  Shmem:                2609088 kB
  SReclaimable:          162992 kB
----- meminfo after drop_caches 2026-06-16T17:53:33+08:00 -----
  MemFree:            252711692 kB
  MemAvailable:       251419520 kB
  Buffers:                 8980 kB
  Cached:               3076396 kB
  SwapCached:                 0 kB
  Dirty:                    660 kB
  Writeback:                  0 kB
  Shmem:                2609092 kB
  SReclaimable:          153748 kB
[bm-row-container-metrics] pipelineOld dataset=variable compression=zstd iterations=1 logical_bytes=26843545600 rows=25712209 store_ms=5619.654 spill_write_ms=16787.556 spill_read_ms=20195.734 read_ms=3005.527 total_ms=45608.472 spill_bytes=854646829 files=26025 batches=26025
===== CASE 11/12 pipelineOld(old_zstd_variable) END exit=0 elapsed=48s 2026-06-16T17:54:41+08:00 =====
===== CASE 12/12 pipelineBm(bm_zstd_variable) START 2026-06-16T17:54:41+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_benchmark --bm_regex=pipelineBm.bm_zstd_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T17:54:41+08:00 -----
  MemFree:            252570052 kB
  MemAvailable:       251349592 kB
  Buffers:                33480 kB
  Cached:               3179300 kB
  SwapCached:                 0 kB
  Dirty:                    452 kB
  Writeback:                  0 kB
  Shmem:                2608604 kB
  SReclaimable:          169208 kB
----- meminfo after sync before drop_caches 2026-06-16T17:54:41+08:00 -----
  MemFree:            252569492 kB
  MemAvailable:       251349040 kB
  Buffers:                33496 kB
  Cached:               3179296 kB
  SwapCached:                 0 kB
  Dirty:                     72 kB
  Writeback:                368 kB
  Shmem:                2608604 kB
  SReclaimable:          169208 kB
----- meminfo after drop_caches 2026-06-16T17:54:41+08:00 -----
  MemFree:            252715368 kB
  MemAvailable:       251422688 kB
  Buffers:                 8960 kB
  Cached:               3075888 kB
  SwapCached:                 0 kB
  Dirty:                      0 kB
  Writeback:                400 kB
  Shmem:                2608608 kB
  SReclaimable:          153240 kB
[bm-row-container-metrics] pipelineBm dataset=variable compression=zstd iterations=1 logical_bytes=26843545600 rows=25712209 store_ms=5339.745 spill_write_ms=14702.683 spill_read_ms=12096.401 read_ms=2794.086 total_ms=34932.916 spill_write_count=6622 spill_write_bytes=27774681088 spill_physical_write_bytes=842335189 spill_read_count=6622 spill_read_bytes=27774681088 spill_physical_read_bytes=842335189
===== CASE 12/12 pipelineBm(bm_zstd_variable) END exit=0 elapsed=38s 2026-06-16T17:55:39+08:00 =====
===== ALL CASES END exit=0 output_dir=/data00/home/wangxinshuo.db/bolt/log/bolt-bm-row-container-pipeline-20260616-172604 2026-06-16T17:55:39+08:00 =====
```

### stdout.txt
```
============================================================================
[...]s/BmRowContainerPipelineBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineOld(old_raw_fixed)                                 3.66min     4.56m
----------------------------------------------------------------------------
============================================================================
[...]s/BmRowContainerPipelineBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineBm(bm_raw_fixed)                                    40.55s    24.66m
----------------------------------------------------------------------------
============================================================================
[...]s/BmRowContainerPipelineBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineOld(old_lz4_fixed)                                 3.88min     4.30m
----------------------------------------------------------------------------
============================================================================
[...]s/BmRowContainerPipelineBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineBm(bm_lz4_fixed)                                   1.54min    10.84m
----------------------------------------------------------------------------
============================================================================
[...]s/BmRowContainerPipelineBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineOld(old_zstd_fixed)                                6.08min     2.74m
----------------------------------------------------------------------------
============================================================================
[...]s/BmRowContainerPipelineBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineBm(bm_zstd_fixed)                                  4.05min     4.11m
----------------------------------------------------------------------------
============================================================================
[...]s/BmRowContainerPipelineBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineOld(old_raw_variable)                              1.25min    13.35m
----------------------------------------------------------------------------
============================================================================
[...]s/BmRowContainerPipelineBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineBm(bm_raw_variable)                                 30.46s    32.83m
----------------------------------------------------------------------------
============================================================================
[...]s/BmRowContainerPipelineBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineOld(old_lz4_variable)                               39.63s    25.23m
----------------------------------------------------------------------------
============================================================================
[...]s/BmRowContainerPipelineBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineBm(bm_lz4_variable)                                 24.07s    41.54m
----------------------------------------------------------------------------
============================================================================
[...]s/BmRowContainerPipelineBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineOld(old_zstd_variable)                              45.66s    21.90m
----------------------------------------------------------------------------
============================================================================
[...]s/BmRowContainerPipelineBenchmark.cpp     relative  time/iter   iters/s
============================================================================
pipelineBm(bm_zstd_variable)                                34.93s    28.63m
----------------------------------------------------------------------------
```
