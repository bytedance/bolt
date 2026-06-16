# 2026-06-16 BM RowContainer benchmark 运行结果分析

## 数据来源

- 运行日志目录：`/data00/home/wangxinshuo.db/bolt/log/bolt-bm-row-container-20260616-152329`
- benchmark stdout：`stdout.txt`
- runner stderr 与诊断 metric：`stderr.txt`
- 本文只分析本次运行结果，不和历史运行结果对比。

## 运行概况

- 一共运行 38 个 case，全部 `exit=0`。
- 每个 case 单独进程运行，命令里统一使用：
  - `--bm_row_container_data_bytes=26843545600`，即 25 GiB 逻辑数据量。
  - `--bm_row_container_warmup_data_bytes=134217728`，即 128 MiB warmup 数据量。
  - `timeout 900s`。
- runner 在每个 case 前执行 cache 清理并打印 `/proc/meminfo`。从日志看，`MemAvailable` 基本稳定在约 250 GB，`Dirty` 和 `Writeback` 在 case 开始前很低，没有明显的脏页积压。
- 下文表格里的耗时使用 benchmark stdout 的 `time/iter`，不使用 runner 的 `elapsed`。runner `elapsed` 包含进程启动、warmup、cache 清理、数据准备等额外开销，适合观察是否超时或失败，不适合作为核心性能指标。

## 总体结论

1. 本次运行里，BM RowContainer 在 read、storeBatch、storeRow、spillRead、spillWrite 的所有同类组合上都快于 old RowContainer。
2. `storeBatch` 是目前 BM 写入路径里最强的结果：fixed 为 6.84s，old 为 23.11s；variable 为 4.74s，old 为 5.57s。
3. `storeRow` 仍明显慢于 `storeBatch`，但本次 BM 仍快于 old：fixed 为 22.77s vs 42.10s，variable 为 5.12s vs 6.16s。
4. BM 的 spill read fixed 场景收益很大，raw/lz4/zstd 分别约 7.8x、5.7x、2.0x；variable 场景 raw/lz4/zstd 分别约 3.6x、1.8x、1.6x。
5. BM 的 spill write raw fixed/variable 分别约 3.3x、1.3x 快于 old；压缩场景里压缩耗时已经成为主要成本，BM 仍然保持领先。
6. BM variable spill read 中，`StringView` rebase 开销约 0.78s，占比不高；主要时间仍在批量 pin/read 和压缩解压路径上。
7. BM variable spill write 中，`zeroUnusedHeapTail` 约 28ms，远小于整体 flush 时间，不是本次瓶颈。

## 基础读性能

| case | old | BM | BM 加速比 |
|---|---:|---:|---:|
| fixed | 7.06s | 4.54s | 1.56x |
| variable | 2.93s | 2.74s | 1.07x |

fixed 读场景下 BM 收益更明显。variable 读场景下两者接近，BM 略快。

## Batch store

| case | old | BM | BM 加速比 |
|---|---:|---:|---:|
| fixed | 23.11s | 6.84s | 3.38x |
| variable | 5.57s | 4.74s | 1.18x |

fixed batch store 是本次最明显的内存写入收益之一，说明新增 batch append 路径对 fixed 类型有效。variable batch store 也领先 old，但幅度小很多，说明变长字符串写入仍然受 heap 写入和字符串拷贝路径限制。

string copy path 的单独 benchmark：

| case | time/iter |
|---|---:|
| `stringCopyPathBmHeapSimd(bm_heap_simd_variable)` | 4.38s |
| `stringCopyPathBmHeapStd(bm_heap_std_variable)` | 5.77s |

SIMD copy 路径约为 std copy 的 1.32x，说明当前 variable batch store 的字符串写入已经吃到了 SIMD copy 收益。

## Spill read

### 结果表

| dataset | compression | old | BM | BM 加速比 |
|---|---|---:|---:|---:|
| fixed | raw | 2.45min | 18.83s | 7.81x |
| fixed | lz4 | 1.70min | 18.01s | 5.66x |
| fixed | zstd | 1.88min | 57.04s | 1.98x |
| variable | raw | 52.12s | 14.43s | 3.61x |
| variable | lz4 | 18.42s | 10.03s | 1.84x |
| variable | zstd | 20.12s | 12.23s | 1.65x |

### BM fixed spill read 诊断

| compression | `list_rows_ms` | `bulk_batch_pin_ms` | `bulk_append_ptrs_ms` | `bm_decompress_ms` | physical read |
|---|---:|---:|---:|---:|---:|
| raw | 18.56s | 14.51s | 4.02s | 0.00s | 32.22 GB |
| lz4 | 17.75s | 13.71s | 4.01s | 12.67s | 27.76 GB |
| zstd | 56.77s | 52.75s | 4.00s | 52.31s | 23.29 GB |

fixed 场景没有 string rebase，`bulk_append_ptrs_ms` 稳定在约 4s，基本是为 13.4 亿行构造指针结果的成本。raw/lz4 的总时间接近，lz4 少读了一部分物理字节，但引入了解压成本；zstd 物理字节更少，但解压成本显著增大，成为主要瓶颈。

### BM variable spill read 诊断

| compression | `list_rows_ms` | `bulk_batch_pin_ms` | `bulk_rebase_strings_ms` | `bulk_append_ptrs_ms` | `bm_decompress_ms` | physical read |
|---|---:|---:|---:|---:|---:|---:|
| raw | 14.42s | 13.56s | 0.78s | 0.08s | 0.00s | 27.77 GB |
| lz4 | 10.03s | 9.16s | 0.79s | 0.08s | 9.10s | 1.20 GB |
| zstd | 12.23s | 11.36s | 0.79s | 0.08s | 11.29s | 0.84 GB |

variable 场景里，BM reload 后需要 rebase 2571 万个 `StringView`，本次成本约 0.78-0.79s，不是主瓶颈。lz4/zstd 把物理读量压到 1.20 GB/0.84 GB，但解压时间基本决定了 `bulk_batch_pin_ms`，因此 lz4 在本次 variable spill read 中最优。

## Spill write

### 结果表

| dataset | compression | old | BM | BM 加速比 |
|---|---|---:|---:|---:|
| fixed | raw | 41.79s | 12.56s | 3.33x |
| fixed | lz4 | 1.59min | 1.03min | 1.54x |
| fixed | zstd | 3.65min | 2.91min | 1.25x |
| variable | raw | 14.71s | 11.16s | 1.32x |
| variable | lz4 | 12.57s | 6.40s | 1.96x |
| variable | zstd | 16.82s | 14.86s | 1.13x |

### BM fixed spill write 诊断

| compression | `store_setup_ms` | `flush_ms` | `bm_compress_ms` | physical write | blocks |
|---|---:|---:|---:|---:|---:|
| raw | 22.82s | 12.56s | 0.00s | 32.22 GB | 7681 |
| lz4 | 22.79s | 61.64s | 59.88s | 27.76 GB | 7681 |
| zstd | 22.81s | 174.50s | 171.63s | 23.29 GB | 7681 |

fixed spill write 的 `store_setup_ms` 约 22.8s，和 `storeRowBm(bm_fixed)` 的 22.77s 基本一致，说明 fixed spill write 的写入准备阶段主要就是 row store 成本。raw flush 主要是写 7681 个 row block；lz4/zstd flush 中压缩占绝对主导，分别约占 flush 的 97% 和 98%。

### BM variable spill write 诊断

| compression | `store_setup_ms` | `flush_ms` | `bm_compress_ms` | physical write | row blocks | heap blocks | `zeroUnusedHeapTail` |
|---|---:|---:|---:|---:|---:|---:|---:|
| raw | 5.36s | 11.16s | 0.00s | 27.77 GB | 246 | 6376 | 28.59ms |
| lz4 | 5.34s | 6.40s | 5.96s | 1.20 GB | 246 | 6376 | 27.98ms |
| zstd | 5.34s | 14.86s | 14.47s | 0.84 GB | 246 | 6376 | 27.97ms |

variable spill write 中，BM 数据由少量 row block 和大量 heap block 组成：246 个 row block，6376 个 heap block，总计 6622 个 block。`zeroUnusedHeapTail` 只有约 28ms，和整体 flush 相比可以忽略。压缩场景下，lz4/zstd 的 `bm_compress_ms` 分别约占 flush 的 93% 和 97%，说明压缩算法成本是主要瓶颈。

## Store row

| case | old | BM | BM 加速比 |
|---|---:|---:|---:|
| fixed | 42.10s | 22.77s | 1.85x |
| variable | 6.16s | 5.12s | 1.20x |

BM `storeRow` fixed 已经快于 old，但和 `storeBatchBm(bm_fixed)` 的 6.84s 相比仍有明显差距。这说明如果调用方能使用 batch append，fixed 写入仍然有很大收益；如果调用方只能走 `newRow + store`，当前路径还有优化空间。

BM `storeRow` 自带轻量诊断只在 16 MiB 小数据量上采样：

| dataset | rows | `append_only_ms` | `append_fixed_ms` | `append_full_ms` | `fixed_store_extra_ms` | `variable_store_extra_ms` |
|---|---:|---:|---:|---:|---:|---:|
| fixed | 838861 | 4.273 | 18.506 | 16.308 | 14.233 | 0.000 |
| variable | 16071 | 0.101 | 0.531 | 3.898 | 0.431 | 3.366 |

这个诊断主要用于看阶段比例，不应直接外推为 25 GiB 的绝对耗时。fixed 的 extra 主要在 fixed cell store；variable 的 extra 主要在字符串 store。

## 需要关注的点

1. `storeBatchBm` fixed 非常快，说明 batch append 是后续 Window 路径应优先利用的接口形态。
2. `storeRowBm` 虽然快于 old，但 fixed 仍比 batch 慢很多。如果 Window 仍大量使用 `newRow + store`，这条路径仍值得继续优化。
3. BM compressed spill write/read 的主要成本已经转向压缩/解压；继续优化 BM 元数据路径对这些 case 的收益会有限。
4. variable spill read 的 string rebase 成本存在但不大，当前不应作为首要优化对象。
5. variable spill write 的 heap tail zero 成本已经很小，本次结果不支持继续优先优化这部分。


## 附录
### stderr.txt
```
===== CASE 1/38 readOld(old_fixed) START 2026-06-16T15:23:29+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=readOld.old_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:23:29+08:00 -----
  MemFree:            251257028 kB
  MemAvailable:       249943628 kB
  Buffers:                33376 kB
  Cached:               2994112 kB
  SwapCached:                 0 kB
  Dirty:                    412 kB
  Writeback:                  0 kB
  Shmem:                2600004 kB
  SReclaimable:          160292 kB
----- meminfo after sync before drop_caches 2026-06-16T15:23:29+08:00 -----
  MemFree:            251255468 kB
  MemAvailable:       249942068 kB
  Buffers:                33404 kB
  Cached:               2994084 kB
  SwapCached:                 0 kB
  Dirty:                    412 kB
  Writeback:                  0 kB
  Shmem:                2600004 kB
  SReclaimable:          160292 kB
----- meminfo after drop_caches 2026-06-16T15:23:29+08:00 -----
  MemFree:            251453672 kB
  MemAvailable:       250041644 kB
  Buffers:                 8828 kB
  Cached:               2829152 kB
  SwapCached:                 0 kB
  Dirty:                    432 kB
  Writeback:                  0 kB
  Shmem:                2600004 kB
  SReclaimable:          152556 kB
===== CASE 1/38 readOld(old_fixed) END exit=0 elapsed=54s 2026-06-16T15:24:43+08:00 =====
===== CASE 2/38 readBm(bm_fixed) START 2026-06-16T15:24:43+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=readBm.bm_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:24:43+08:00 -----
  MemFree:            251272360 kB
  MemAvailable:       249953016 kB
  Buffers:                32424 kB
  Cached:               2983988 kB
  SwapCached:                 0 kB
  Dirty:                    812 kB
  Writeback:                  0 kB
  Shmem:                2600200 kB
  SReclaimable:          159772 kB
----- meminfo after sync before drop_caches 2026-06-16T15:24:43+08:00 -----
  MemFree:            251271800 kB
  MemAvailable:       249952460 kB
  Buffers:                32444 kB
  Cached:               2983988 kB
  SwapCached:                 0 kB
  Dirty:                    132 kB
  Writeback:                292 kB
  Shmem:                2600200 kB
  SReclaimable:          159772 kB
----- meminfo after drop_caches 2026-06-16T15:24:43+08:00 -----
  MemFree:            251448808 kB
  MemAvailable:       250042012 kB
  Buffers:                 8736 kB
  Cached:               2839460 kB
  SwapCached:                 0 kB
  Dirty:                    140 kB
  Writeback:                296 kB
  Shmem:                2600200 kB
  SReclaimable:          153100 kB
===== CASE 2/38 readBm(bm_fixed) END exit=0 elapsed=33s 2026-06-16T15:25:37+08:00 =====
===== CASE 3/38 readOld(old_variable) START 2026-06-16T15:25:37+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=readOld.old_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:25:37+08:00 -----
  MemFree:            251270600 kB
  MemAvailable:       249946776 kB
  Buffers:                25604 kB
  Cached:               2981308 kB
  SwapCached:                 0 kB
  Dirty:                    772 kB
  Writeback:                  0 kB
  Shmem:                2600328 kB
  SReclaimable:          160212 kB
----- meminfo after sync before drop_caches 2026-06-16T15:25:37+08:00 -----
  MemFree:            251270032 kB
  MemAvailable:       249946208 kB
  Buffers:                25620 kB
  Cached:               2981300 kB
  SwapCached:                 0 kB
  Dirty:                    320 kB
  Writeback:                  0 kB
  Shmem:                2600328 kB
  SReclaimable:          160212 kB
----- meminfo after drop_caches 2026-06-16T15:25:37+08:00 -----
  MemFree:            251435748 kB
  MemAvailable:       250028408 kB
  Buffers:                 7900 kB
  Cached:               2840764 kB
  SwapCached:                 0 kB
  Dirty:                    320 kB
  Writeback:                  0 kB
  Shmem:                2600328 kB
  SReclaimable:          151944 kB
===== CASE 3/38 readOld(old_variable) END exit=0 elapsed=11s 2026-06-16T15:26:08+08:00 =====
===== CASE 4/38 readBm(bm_variable) START 2026-06-16T15:26:08+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=readBm.bm_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:26:08+08:00 -----
  MemFree:            251285828 kB
  MemAvailable:       249951584 kB
  Buffers:                23828 kB
  Cached:               2963204 kB
  SwapCached:                 0 kB
  Dirty:                    548 kB
  Writeback:                  0 kB
  Shmem:                2600392 kB
  SReclaimable:          159452 kB
----- meminfo after sync before drop_caches 2026-06-16T15:26:08+08:00 -----
  MemFree:            251285828 kB
  MemAvailable:       249951584 kB
  Buffers:                23852 kB
  Cached:               2963192 kB
  SwapCached:                 0 kB
  Dirty:                    600 kB
  Writeback:                  0 kB
  Shmem:                2600392 kB
  SReclaimable:          159452 kB
----- meminfo after drop_caches 2026-06-16T15:26:08+08:00 -----
  MemFree:            251431232 kB
  MemAvailable:       250023908 kB
  Buffers:                 8332 kB
  Cached:               2840228 kB
  SwapCached:                 0 kB
  Dirty:                    616 kB
  Writeback:                  0 kB
  Shmem:                2600392 kB
  SReclaimable:          152276 kB
===== CASE 4/38 readBm(bm_variable) END exit=0 elapsed=9s 2026-06-16T15:26:37+08:00 =====
===== CASE 5/38 storeBatchOld(old_fixed) START 2026-06-16T15:26:37+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=storeBatchOld.old_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:26:37+08:00 -----
  MemFree:            251309336 kB
  MemAvailable:       249963404 kB
  Buffers:                25004 kB
  Cached:               2939068 kB
  SwapCached:                 0 kB
  Dirty:                    400 kB
  Writeback:                  0 kB
  Shmem:                2600492 kB
  SReclaimable:          159092 kB
----- meminfo after sync before drop_caches 2026-06-16T15:26:37+08:00 -----
  MemFree:            251309336 kB
  MemAvailable:       249963404 kB
  Buffers:                25032 kB
  Cached:               2939048 kB
  SwapCached:                 0 kB
  Dirty:                    416 kB
  Writeback:                  0 kB
  Shmem:                2600492 kB
  SReclaimable:          159092 kB
----- meminfo after drop_caches 2026-06-16T15:26:37+08:00 -----
  MemFree:            251431172 kB
  MemAvailable:       250024296 kB
  Buffers:                 8592 kB
  Cached:               2840772 kB
  SwapCached:                 0 kB
  Dirty:                    596 kB
  Writeback:                  0 kB
  Shmem:                2600492 kB
  SReclaimable:          151908 kB
===== CASE 5/38 storeBatchOld(old_fixed) END exit=0 elapsed=24s 2026-06-16T15:27:21+08:00 =====
===== CASE 6/38 storeBatchBm(bm_fixed) START 2026-06-16T15:27:21+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=storeBatchBm.bm_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:27:21+08:00 -----
  MemFree:            251318592 kB
  MemAvailable:       249972020 kB
  Buffers:                24092 kB
  Cached:               2937892 kB
  SwapCached:                 0 kB
  Dirty:                    916 kB
  Writeback:                  0 kB
  Shmem:                2599944 kB
  SReclaimable:          159372 kB
----- meminfo after sync before drop_caches 2026-06-16T15:27:21+08:00 -----
  MemFree:            251318600 kB
  MemAvailable:       249972032 kB
  Buffers:                24120 kB
  Cached:               2937884 kB
  SwapCached:                 0 kB
  Dirty:                     76 kB
  Writeback:                376 kB
  Shmem:                2599944 kB
  SReclaimable:          159372 kB
----- meminfo after drop_caches 2026-06-16T15:27:22+08:00 -----
  MemFree:            251439820 kB
  MemAvailable:       250033024 kB
  Buffers:                 8772 kB
  Cached:               2840460 kB
  SwapCached:                 0 kB
  Dirty:                    104 kB
  Writeback:                  0 kB
  Shmem:                2599948 kB
  SReclaimable:          151684 kB
===== CASE 6/38 storeBatchBm(bm_fixed) END exit=0 elapsed=8s 2026-06-16T15:27:50+08:00 =====
===== CASE 7/38 storeBatchOld(old_variable) START 2026-06-16T15:27:50+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=storeBatchOld.old_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:27:50+08:00 -----
  MemFree:            251326396 kB
  MemAvailable:       249979288 kB
  Buffers:                23776 kB
  Cached:               2938184 kB
  SwapCached:                 0 kB
  Dirty:                    576 kB
  Writeback:                  0 kB
  Shmem:                2600016 kB
  SReclaimable:          158856 kB
----- meminfo after sync before drop_caches 2026-06-16T15:27:50+08:00 -----
  MemFree:            251326396 kB
  MemAvailable:       249979288 kB
  Buffers:                23804 kB
  Cached:               2938156 kB
  SwapCached:                 0 kB
  Dirty:                     72 kB
  Writeback:                  0 kB
  Shmem:                2600016 kB
  SReclaimable:          158856 kB
----- meminfo after drop_caches 2026-06-16T15:27:50+08:00 -----
  MemFree:            251449036 kB
  MemAvailable:       250040484 kB
  Buffers:                 7576 kB
  Cached:               2840240 kB
  SwapCached:                 0 kB
  Dirty:                     72 kB
  Writeback:                  0 kB
  Shmem:                2600016 kB
  SReclaimable:          149608 kB
===== CASE 7/38 storeBatchOld(old_variable) END exit=0 elapsed=7s 2026-06-16T15:28:17+08:00 =====
===== CASE 8/38 storeBatchBm(bm_variable) START 2026-06-16T15:28:17+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=storeBatchBm.bm_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:28:17+08:00 -----
  MemFree:            251402048 kB
  MemAvailable:       250043288 kB
  Buffers:                23332 kB
  Cached:               2917220 kB
  SwapCached:                 0 kB
  Dirty:                    352 kB
  Writeback:                  0 kB
  Shmem:                2600048 kB
  SReclaimable:          156436 kB
----- meminfo after sync before drop_caches 2026-06-16T15:28:17+08:00 -----
  MemFree:            251401480 kB
  MemAvailable:       250042720 kB
  Buffers:                23352 kB
  Cached:               2917200 kB
  SwapCached:                 0 kB
  Dirty:                    352 kB
  Writeback:                  0 kB
  Shmem:                2600048 kB
  SReclaimable:          156436 kB
----- meminfo after drop_caches 2026-06-16T15:28:17+08:00 -----
  MemFree:            251501476 kB
  MemAvailable:       250092736 kB
  Buffers:                 8584 kB
  Cached:               2838960 kB
  SwapCached:                 0 kB
  Dirty:                    352 kB
  Writeback:                  0 kB
  Shmem:                2600048 kB
  SReclaimable:          149988 kB
===== CASE 8/38 storeBatchBm(bm_variable) END exit=0 elapsed=6s 2026-06-16T15:28:43+08:00 =====
===== CASE 9/38 stringCopyPathBmHeapSimd(bm_heap_simd_variable) START 2026-06-16T15:28:43+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=stringCopyPathBmHeapSimd.bm_heap_simd_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:28:43+08:00 -----
  MemFree:            251293508 kB
  MemAvailable:       249965912 kB
  Buffers:                25204 kB
  Cached:               2977556 kB
  SwapCached:                 0 kB
  Dirty:                    100 kB
  Writeback:                  0 kB
  Shmem:                2600132 kB
  SReclaimable:          156604 kB
----- meminfo after sync before drop_caches 2026-06-16T15:28:43+08:00 -----
  MemFree:            251294076 kB
  MemAvailable:       249966888 kB
  Buffers:                25228 kB
  Cached:               2978480 kB
  SwapCached:                 0 kB
  Dirty:                    124 kB
  Writeback:                  0 kB
  Shmem:                2600132 kB
  SReclaimable:          156604 kB
----- meminfo after drop_caches 2026-06-16T15:28:43+08:00 -----
  MemFree:            251457248 kB
  MemAvailable:       250048692 kB
  Buffers:                 8496 kB
  Cached:               2840664 kB
  SwapCached:                 0 kB
  Dirty:                      0 kB
  Writeback:                 68 kB
  Shmem:                2600112 kB
  SReclaimable:          148428 kB
===== CASE 9/38 stringCopyPathBmHeapSimd(bm_heap_simd_variable) END exit=0 elapsed=6s 2026-06-16T15:29:09+08:00 =====
===== CASE 10/38 stringCopyPathBmHeapStd(bm_heap_std_variable) START 2026-06-16T15:29:09+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=stringCopyPathBmHeapStd.bm_heap_std_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:29:09+08:00 -----
  MemFree:            251346704 kB
  MemAvailable:       249992576 kB
  Buffers:                23492 kB
  Cached:               2927008 kB
  SwapCached:                 0 kB
  Dirty:                    328 kB
  Writeback:                  4 kB
  Shmem:                2600172 kB
  SReclaimable:          155916 kB
----- meminfo after sync before drop_caches 2026-06-16T15:29:09+08:00 -----
  MemFree:            251346160 kB
  MemAvailable:       249992032 kB
  Buffers:                23540 kB
  Cached:               2926960 kB
  SwapCached:                 0 kB
  Dirty:                    328 kB
  Writeback:                  4 kB
  Shmem:                2600172 kB
  SReclaimable:          155916 kB
----- meminfo after drop_caches 2026-06-16T15:29:09+08:00 -----
  MemFree:            251455120 kB
  MemAvailable:       250046496 kB
  Buffers:                 8476 kB
  Cached:               2841164 kB
  SwapCached:                 0 kB
  Dirty:                     20 kB
  Writeback:                  0 kB
  Shmem:                2600172 kB
  SReclaimable:          148272 kB
===== CASE 10/38 stringCopyPathBmHeapStd(bm_heap_std_variable) END exit=0 elapsed=7s 2026-06-16T15:29:36+08:00 =====
===== CASE 11/38 spillReadOld(old_raw_fixed) START 2026-06-16T15:29:36+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillReadOld.old_raw_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:29:36+08:00 -----
  MemFree:            251326920 kB
  MemAvailable:       249984572 kB
  Buffers:                24564 kB
  Cached:               2949792 kB
  SwapCached:                 0 kB
  Dirty:                    868 kB
  Writeback:                  0 kB
  Shmem:                2600272 kB
  SReclaimable:          155728 kB
----- meminfo after sync before drop_caches 2026-06-16T15:29:36+08:00 -----
  MemFree:            251326928 kB
  MemAvailable:       249984588 kB
  Buffers:                24608 kB
  Cached:               2949768 kB
  SwapCached:                 0 kB
  Dirty:                    328 kB
  Writeback:                  0 kB
  Shmem:                2600272 kB
  SReclaimable:          155728 kB
----- meminfo after drop_caches 2026-06-16T15:29:36+08:00 -----
  MemFree:            251459320 kB
  MemAvailable:       250050748 kB
  Buffers:                 8552 kB
  Cached:               2841552 kB
  SwapCached:                 0 kB
  Dirty:                    332 kB
  Writeback:                  0 kB
  Shmem:                2600272 kB
  SReclaimable:          148040 kB
[bm-row-container-metrics] spillReadOld compression=raw dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 serialized_bytes=28185722880 batches=26883 create_reader_ms=12414.168 next_batch_ms=20831.231 copy_rows_ms=18838.371 list_rows_ms=7078.223
===== CASE 11/38 spillReadOld(old_raw_fixed) END exit=0 elapsed=235s 2026-06-16T15:33:51+08:00 =====
===== CASE 12/38 spillReadBm(bm_raw_fixed) START 2026-06-16T15:33:51+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillReadBm.bm_raw_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:33:51+08:00 -----
  MemFree:            251272192 kB
  MemAvailable:       249966336 kB
  Buffers:                37284 kB
  Cached:               3000000 kB
  SwapCached:                 0 kB
  Dirty:                    676 kB
  Writeback:                  0 kB
  Shmem:                2600768 kB
  SReclaimable:          166252 kB
----- meminfo after sync before drop_caches 2026-06-16T15:33:51+08:00 -----
  MemFree:            251271632 kB
  MemAvailable:       249965776 kB
  Buffers:                37300 kB
  Cached:               2999984 kB
  SwapCached:                 0 kB
  Dirty:                     96 kB
  Writeback:                300 kB
  Shmem:                2600768 kB
  SReclaimable:          166252 kB
----- meminfo after drop_caches 2026-06-16T15:33:52+08:00 -----
  MemFree:            251485300 kB
  MemAvailable:       250078824 kB
  Buffers:                 8988 kB
  Cached:               2842840 kB
  SwapCached:                 0 kB
  Dirty:                    116 kB
  Writeback:                300 kB
  Shmem:                2600768 kB
  SReclaimable:          150992 kB
[bm-row-container-metrics] spillReadBm compression=raw dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 row_ids=0 windows=0 result=pointers begin_ms=0.110 list_rows_ms=18560.904 window_load_ms=0.000 bulk_estimate_ms=0.032 bulk_reserve_ms=0.000 bulk_collect_blocks_ms=28.336 bulk_batch_pin_ms=14513.252 bulk_update_ptrs_ms=0.317 bulk_rebase_strings_ms=0.000 bulk_append_ptrs_ms=4018.683 bulk_append_row_ids_ms=0.000 bulk_estimated_bytes=32216449024 bulk_pinned_blocks=7681 bulk_pointer_rows=1342177280 bulk_row_id_rows=0 bulk_rebased_string_views=0 bm_batch_pins=1 bm_pin_reads=7681 bm_spill_read_count=7681 bm_spill_read_bytes=32216449024 bm_spill_physical_read_bytes=32216694816 bm_decompress_ms=0.000 io_accepted=7681 io_completed=7681 io_completed_bytes=32216694816 io_successful=7681 io_failed=0 io_rejected=0 io_submitted_high=7681 io_submitted_medium=0 io_submitted_low=0 io_completed_high=7681 io_completed_medium=0 io_completed_low=0 io_submit_batches=61 io_completion_batches=61 io_queue_wait_ms=56077657.085 io_avg_queue_wait_us=7300827.638 io_device_latency_ms=930026.525 io_avg_device_latency_us=121081.438 io_end_to_end_latency_ms=57007687.298 io_avg_end_to_end_latency_us=7421909.556 io_backend_submit_ms=14401.396 io_backend_reap_ms=0.249 io_worker_wait_ms=48.877 io_future_fulfill_ms=2.907
===== CASE 12/38 spillReadBm(bm_raw_fixed) END exit=0 elapsed=74s 2026-06-16T15:35:26+08:00 =====
===== CASE 13/38 spillReadOld(old_lz4_fixed) START 2026-06-16T15:35:26+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillReadOld.old_lz4_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:35:26+08:00 -----
  MemFree:            251281212 kB
  MemAvailable:       249954120 kB
  Buffers:                29204 kB
  Cached:               2971308 kB
  SwapCached:                 0 kB
  Dirty:                    396 kB
  Writeback:                  0 kB
  Shmem:                2600960 kB
  SReclaimable:          160684 kB
----- meminfo after sync before drop_caches 2026-06-16T15:35:26+08:00 -----
  MemFree:            251280652 kB
  MemAvailable:       249953560 kB
  Buffers:                29224 kB
  Cached:               2971288 kB
  SwapCached:                 0 kB
  Dirty:                    396 kB
  Writeback:                  0 kB
  Shmem:                2600960 kB
  SReclaimable:          160684 kB
----- meminfo after drop_caches 2026-06-16T15:35:26+08:00 -----
  MemFree:            251439956 kB
  MemAvailable:       250035192 kB
  Buffers:                 8440 kB
  Cached:               2845532 kB
  SwapCached:                 0 kB
  Dirty:                    148 kB
  Writeback:                  0 kB
  Shmem:                2600984 kB
  SReclaimable:          151928 kB
[bm-row-container-metrics] spillReadOld compression=lz4 dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 serialized_bytes=28185722880 batches=26883 create_reader_ms=13419.304 next_batch_ms=36155.908 copy_rows_ms=18813.442 list_rows_ms=7119.614
===== CASE 13/38 spillReadOld(old_lz4_fixed) END exit=0 elapsed=244s 2026-06-16T15:39:50+08:00 =====
===== CASE 14/38 spillReadBm(bm_lz4_fixed) START 2026-06-16T15:39:51+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillReadBm.bm_lz4_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:39:51+08:00 -----
  MemFree:            251231076 kB
  MemAvailable:       249924424 kB
  Buffers:                37396 kB
  Cached:               2996072 kB
  SwapCached:                 0 kB
  Dirty:                    600 kB
  Writeback:                  0 kB
  Shmem:                2599892 kB
  SReclaimable:          167612 kB
----- meminfo after sync before drop_caches 2026-06-16T15:39:51+08:00 -----
  MemFree:            251230524 kB
  MemAvailable:       249923904 kB
  Buffers:                37416 kB
  Cached:               2996060 kB
  SwapCached:                 0 kB
  Dirty:                    600 kB
  Writeback:                  0 kB
  Shmem:                2599892 kB
  SReclaimable:          167612 kB
----- meminfo after drop_caches 2026-06-16T15:39:51+08:00 -----
  MemFree:            251436068 kB
  MemAvailable:       250032004 kB
  Buffers:                 8776 kB
  Cached:               2847644 kB
  SwapCached:                 0 kB
  Dirty:                    656 kB
  Writeback:                  0 kB
  Shmem:                2599892 kB
  SReclaimable:          150300 kB
[bm-row-container-metrics] spillReadBm compression=lz4 dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 row_ids=0 windows=0 result=pointers begin_ms=0.124 list_rows_ms=17747.899 window_load_ms=0.000 bulk_estimate_ms=0.038 bulk_reserve_ms=0.000 bulk_collect_blocks_ms=26.993 bulk_batch_pin_ms=13711.545 bulk_update_ptrs_ms=0.299 bulk_rebase_strings_ms=0.000 bulk_append_ptrs_ms=4008.784 bulk_append_row_ids_ms=0.000 bulk_estimated_bytes=32216449024 bulk_pinned_blocks=7681 bulk_pointer_rows=1342177280 bulk_row_id_rows=0 bulk_rebased_string_views=0 bm_batch_pins=1 bm_pin_reads=7681 bm_spill_read_count=7681 bm_spill_read_bytes=32216449024 bm_spill_physical_read_bytes=27762192235 bm_decompress_ms=12672.043 io_accepted=7681 io_completed=7681 io_completed_bytes=27762192235 io_successful=7681 io_failed=0 io_rejected=0 io_submitted_high=7681 io_submitted_medium=0 io_submitted_low=0 io_completed_high=7681 io_completed_medium=0 io_completed_low=0 io_submit_batches=61 io_completion_batches=61 io_queue_wait_ms=51539608.107 io_avg_queue_wait_us=6710012.773 io_device_latency_ms=816350.177 io_avg_device_latency_us=106281.757 io_end_to_end_latency_ms=52355962.133 io_avg_end_to_end_latency_us=6816295.031 io_backend_submit_ms=13084.081 io_backend_reap_ms=0.277 io_worker_wait_ms=52.963 io_future_fulfill_ms=2.773
===== CASE 14/38 spillReadBm(bm_lz4_fixed) END exit=0 elapsed=138s 2026-06-16T15:42:29+08:00 =====
===== CASE 15/38 spillReadOld(old_zstd_fixed) START 2026-06-16T15:42:29+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillReadOld.old_zstd_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:42:29+08:00 -----
  MemFree:            251228916 kB
  MemAvailable:       249921692 kB
  Buffers:                30396 kB
  Cached:               3010564 kB
  SwapCached:                 0 kB
  Dirty:                    880 kB
  Writeback:                  0 kB
  Shmem:                2600304 kB
  SReclaimable:          159336 kB
----- meminfo after sync before drop_caches 2026-06-16T15:42:29+08:00 -----
  MemFree:            251229436 kB
  MemAvailable:       249922212 kB
  Buffers:                30416 kB
  Cached:               3010556 kB
  SwapCached:                 0 kB
  Dirty:                    440 kB
  Writeback:                  0 kB
  Shmem:                2600304 kB
  SReclaimable:          159336 kB
----- meminfo after drop_caches 2026-06-16T15:42:29+08:00 -----
  MemFree:            251424440 kB
  MemAvailable:       250021196 kB
  Buffers:                 8916 kB
  Cached:               2849244 kB
  SwapCached:                 0 kB
  Dirty:                    292 kB
  Writeback:                  0 kB
  Shmem:                2600304 kB
  SReclaimable:          150104 kB
[bm-row-container-metrics] spillReadOld compression=zstd dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 serialized_bytes=28185722880 batches=26883 create_reader_ms=11156.803 next_batch_ms=73310.546 copy_rows_ms=18542.968 list_rows_ms=7055.649
===== CASE 15/38 spillReadOld(old_zstd_fixed) END exit=0 elapsed=379s 2026-06-16T15:49:08+08:00 =====
===== CASE 16/38 spillReadBm(bm_zstd_fixed) START 2026-06-16T15:49:08+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillReadBm.bm_zstd_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:49:08+08:00 -----
  MemFree:            251203592 kB
  MemAvailable:       249909824 kB
  Buffers:                36844 kB
  Cached:               3022012 kB
  SwapCached:                 0 kB
  Dirty:                    644 kB
  Writeback:                  0 kB
  Shmem:                2600540 kB
  SReclaimable:          168564 kB
----- meminfo after sync before drop_caches 2026-06-16T15:49:08+08:00 -----
  MemFree:            251203016 kB
  MemAvailable:       249909248 kB
  Buffers:                36864 kB
  Cached:               3022012 kB
  SwapCached:                 0 kB
  Dirty:                    148 kB
  Writeback:                  4 kB
  Shmem:                2600540 kB
  SReclaimable:          168564 kB
----- meminfo after drop_caches 2026-06-16T15:49:09+08:00 -----
  MemFree:            251429504 kB
  MemAvailable:       250027200 kB
  Buffers:                 8536 kB
  Cached:               2850760 kB
  SwapCached:                 0 kB
  Dirty:                     20 kB
  Writeback:                 56 kB
  Shmem:                2600516 kB
  SReclaimable:          151572 kB
[bm-row-container-metrics] spillReadBm compression=zstd dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 row_ids=0 windows=0 result=pointers begin_ms=0.122 list_rows_ms=56772.068 window_load_ms=0.000 bulk_estimate_ms=0.036 bulk_reserve_ms=0.000 bulk_collect_blocks_ms=25.982 bulk_batch_pin_ms=52745.326 bulk_update_ptrs_ms=0.290 bulk_rebase_strings_ms=0.000 bulk_append_ptrs_ms=4000.206 bulk_append_row_ids_ms=0.000 bulk_estimated_bytes=32216449024 bulk_pinned_blocks=7681 bulk_pointer_rows=1342177280 bulk_row_id_rows=0 bulk_rebased_string_views=0 bm_batch_pins=1 bm_pin_reads=7681 bm_spill_read_count=7681 bm_spill_read_bytes=32216449024 bm_spill_physical_read_bytes=23291447798 bm_decompress_ms=52309.074 io_accepted=7681 io_completed=7681 io_completed_bytes=23291447798 io_successful=7681 io_failed=0 io_rejected=0 io_submitted_high=7681 io_submitted_medium=0 io_submitted_low=0 io_completed_high=7681 io_completed_medium=0 io_completed_low=0 io_submit_batches=61 io_completion_batches=61 io_queue_wait_ms=40879029.582 io_avg_queue_wait_us=5322097.329 io_device_latency_ms=691114.523 io_avg_device_latency_us=89977.154 io_end_to_end_latency_ms=41570147.981 io_avg_end_to_end_latency_us=5412074.988 io_backend_submit_ms=10560.681 io_backend_reap_ms=0.196 io_worker_wait_ms=46.402 io_future_fulfill_ms=2.649
===== CASE 16/38 spillReadBm(bm_zstd_fixed) END exit=0 elapsed=261s 2026-06-16T15:53:50+08:00 =====
===== CASE 17/38 spillReadOld(old_raw_variable) START 2026-06-16T15:53:50+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillReadOld.old_raw_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:53:50+08:00 -----
  MemFree:            251175272 kB
  MemAvailable:       249902072 kB
  Buffers:                33936 kB
  Cached:               3072116 kB
  SwapCached:                 0 kB
  Dirty:                    812 kB
  Writeback:                  0 kB
  Shmem:                2600152 kB
  SReclaimable:          162140 kB
----- meminfo after sync before drop_caches 2026-06-16T15:53:50+08:00 -----
  MemFree:            251175272 kB
  MemAvailable:       249902072 kB
  Buffers:                33952 kB
  Cached:               3072116 kB
  SwapCached:                 0 kB
  Dirty:                    328 kB
  Writeback:                  0 kB
  Shmem:                2600152 kB
  SReclaimable:          162140 kB
----- meminfo after drop_caches 2026-06-16T15:53:50+08:00 -----
  MemFree:            251433608 kB
  MemAvailable:       250032564 kB
  Buffers:                 9032 kB
  Cached:               2852108 kB
  SwapCached:                 0 kB
  Dirty:                    348 kB
  Writeback:                  0 kB
  Shmem:                2600152 kB
  SReclaimable:          151396 kB
[bm-row-container-metrics] spillReadOld compression=raw dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 serialized_bytes=27280653749 batches=26025 create_reader_ms=12363.593 next_batch_ms=9624.391 copy_rows_ms=7191.693 list_rows_ms=171.140
===== CASE 17/38 spillReadOld(old_raw_variable) END exit=0 elapsed=76s 2026-06-16T15:55:26+08:00 =====
===== CASE 18/38 spillReadBm(bm_raw_variable) START 2026-06-16T15:55:26+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillReadBm.bm_raw_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:55:26+08:00 -----
  MemFree:            251251932 kB
  MemAvailable:       249936072 kB
  Buffers:                34820 kB
  Cached:               2977892 kB
  SwapCached:                 0 kB
  Dirty:                    440 kB
  Writeback:                  0 kB
  Shmem:                2600324 kB
  SReclaimable:          170324 kB
----- meminfo after sync before drop_caches 2026-06-16T15:55:26+08:00 -----
  MemFree:            251251704 kB
  MemAvailable:       249935844 kB
  Buffers:                34840 kB
  Cached:               2977872 kB
  SwapCached:                 0 kB
  Dirty:                    440 kB
  Writeback:                  0 kB
  Shmem:                2600324 kB
  SReclaimable:          170324 kB
----- meminfo after drop_caches 2026-06-16T15:55:26+08:00 -----
  MemFree:            251431772 kB
  MemAvailable:       250031972 kB
  Buffers:                 9056 kB
  Cached:               2852688 kB
  SwapCached:                 0 kB
  Dirty:                    328 kB
  Writeback:                  0 kB
  Shmem:                2600324 kB
  SReclaimable:          152980 kB
[bm-row-container-metrics] spillReadBm compression=raw dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 row_ids=0 windows=0 result=pointers begin_ms=0.095 list_rows_ms=14421.409 window_load_ms=0.000 bulk_estimate_ms=0.011 bulk_reserve_ms=0.000 bulk_collect_blocks_ms=1.425 bulk_batch_pin_ms=13563.377 bulk_update_ptrs_ms=0.579 bulk_rebase_strings_ms=778.304 bulk_append_ptrs_ms=77.203 bulk_append_row_ids_ms=0.000 bulk_estimated_bytes=27774681088 bulk_pinned_blocks=6622 bulk_pointer_rows=25712209 bulk_row_id_rows=0 bulk_rebased_string_views=25712209 bm_batch_pins=1 bm_pin_reads=6622 bm_spill_read_count=6622 bm_spill_read_bytes=27774681088 bm_spill_physical_read_bytes=27774892992 bm_decompress_ms=0.000 io_accepted=6622 io_completed=6622 io_completed_bytes=27774892992 io_successful=6622 io_failed=0 io_rejected=0 io_submitted_high=6622 io_submitted_medium=0 io_submitted_low=0 io_completed_high=6622 io_completed_medium=0 io_completed_low=0 io_submit_batches=53 io_completion_batches=53 io_queue_wait_ms=45710447.263 io_avg_queue_wait_us=6902815.956 io_device_latency_ms=828126.130 io_avg_device_latency_us=125056.800 io_end_to_end_latency_ms=46538576.650 io_avg_end_to_end_latency_us=7027873.248 io_backend_submit_ms=13469.028 io_backend_reap_ms=0.253 io_worker_wait_ms=6.068 io_future_fulfill_ms=2.654
===== CASE 18/38 spillReadBm(bm_raw_variable) END exit=0 elapsed=40s 2026-06-16T15:56:26+08:00 =====
===== CASE 19/38 spillReadOld(old_lz4_variable) START 2026-06-16T15:56:26+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillReadOld.old_lz4_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:56:26+08:00 -----
  MemFree:            251279896 kB
  MemAvailable:       249953668 kB
  Buffers:                28132 kB
  Cached:               2972608 kB
  SwapCached:                 0 kB
  Dirty:                   1204 kB
  Writeback:                  0 kB
  Shmem:                2599816 kB
  SReclaimable:          161068 kB
----- meminfo after sync before drop_caches 2026-06-16T15:56:26+08:00 -----
  MemFree:            251279896 kB
  MemAvailable:       249953672 kB
  Buffers:                28152 kB
  Cached:               2972600 kB
  SwapCached:                 0 kB
  Dirty:                    260 kB
  Writeback:                  0 kB
  Shmem:                2599816 kB
  SReclaimable:          161068 kB
----- meminfo after drop_caches 2026-06-16T15:56:26+08:00 -----
  MemFree:            251431916 kB
  MemAvailable:       250031292 kB
  Buffers:                 9236 kB
  Cached:               2852000 kB
  SwapCached:                 0 kB
  Dirty:                    260 kB
  Writeback:                  0 kB
  Shmem:                2599816 kB
  SReclaimable:          151820 kB
[bm-row-container-metrics] spillReadOld compression=lz4 dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 serialized_bytes=27280653749 batches=26025 create_reader_ms=835.334 next_batch_ms=10086.612 copy_rows_ms=6895.043 list_rows_ms=171.720
===== CASE 19/38 spillReadOld(old_lz4_variable) END exit=0 elapsed=40s 2026-06-16T15:57:26+08:00 =====
===== CASE 20/38 spillReadBm(bm_lz4_variable) START 2026-06-16T15:57:26+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillReadBm.bm_lz4_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:57:26+08:00 -----
  MemFree:            251217516 kB
  MemAvailable:       249921008 kB
  Buffers:                41172 kB
  Cached:               3006916 kB
  SwapCached:                 0 kB
  Dirty:                    828 kB
  Writeback:                  0 kB
  Shmem:                2599952 kB
  SReclaimable:          173300 kB
----- meminfo after sync before drop_caches 2026-06-16T15:57:26+08:00 -----
  MemFree:            251217516 kB
  MemAvailable:       249921008 kB
  Buffers:                41192 kB
  Cached:               3006896 kB
  SwapCached:                 0 kB
  Dirty:                    324 kB
  Writeback:                  0 kB
  Shmem:                2599952 kB
  SReclaimable:          173300 kB
----- meminfo after drop_caches 2026-06-16T15:57:26+08:00 -----
  MemFree:            251427964 kB
  MemAvailable:       250027308 kB
  Buffers:                 8956 kB
  Cached:               2852792 kB
  SwapCached:                 0 kB
  Dirty:                    324 kB
  Writeback:                  0 kB
  Shmem:                2599952 kB
  SReclaimable:          151352 kB
[bm-row-container-metrics] spillReadBm compression=lz4 dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 row_ids=0 windows=0 result=pointers begin_ms=0.070 list_rows_ms=10026.555 window_load_ms=0.000 bulk_estimate_ms=0.010 bulk_reserve_ms=0.000 bulk_collect_blocks_ms=1.207 bulk_batch_pin_ms=9161.208 bulk_update_ptrs_ms=0.538 bulk_rebase_strings_ms=785.627 bulk_append_ptrs_ms=77.479 bulk_append_row_ids_ms=0.000 bulk_estimated_bytes=27774681088 bulk_pinned_blocks=6622 bulk_pointer_rows=25712209 bulk_row_id_rows=0 bulk_rebased_string_views=25712209 bm_batch_pins=1 bm_pin_reads=6622 bm_spill_read_count=6622 bm_spill_read_bytes=27774681088 bm_spill_physical_read_bytes=1197664460 bm_decompress_ms=9096.800 io_accepted=6622 io_completed=6622 io_completed_bytes=1197664460 io_successful=6622 io_failed=0 io_rejected=0 io_submitted_high=6622 io_submitted_medium=0 io_submitted_low=0 io_completed_high=6622 io_completed_medium=0 io_completed_low=0 io_submit_batches=53 io_completion_batches=53 io_queue_wait_ms=1996278.100 io_avg_queue_wait_us=301461.507 io_device_latency_ms=41315.502 io_avg_device_latency_us=6239.127 io_end_to_end_latency_ms=2037596.885 io_avg_end_to_end_latency_us=307701.130 io_backend_submit_ms=633.490 io_backend_reap_ms=0.158 io_worker_wait_ms=8.443 io_future_fulfill_ms=2.123
===== CASE 20/38 spillReadBm(bm_lz4_variable) END exit=0 elapsed=24s 2026-06-16T15:58:10+08:00 =====
===== CASE 21/38 spillReadOld(old_zstd_variable) START 2026-06-16T15:58:10+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillReadOld.old_zstd_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:58:10+08:00 -----
  MemFree:            251295632 kB
  MemAvailable:       249968584 kB
  Buffers:                32816 kB
  Cached:               2968372 kB
  SwapCached:                 0 kB
  Dirty:                    524 kB
  Writeback:                  0 kB
  Shmem:                2600048 kB
  SReclaimable:          159172 kB
----- meminfo after sync before drop_caches 2026-06-16T15:58:10+08:00 -----
  MemFree:            251295064 kB
  MemAvailable:       249968016 kB
  Buffers:                32844 kB
  Cached:               2968344 kB
  SwapCached:                 0 kB
  Dirty:                    524 kB
  Writeback:                  0 kB
  Shmem:                2600048 kB
  SReclaimable:          159172 kB
----- meminfo after drop_caches 2026-06-16T15:58:10+08:00 -----
  MemFree:            251447776 kB
  MemAvailable:       250046728 kB
  Buffers:                 8960 kB
  Cached:               2852432 kB
  SwapCached:                 0 kB
  Dirty:                    324 kB
  Writeback:                  0 kB
  Shmem:                2600048 kB
  SReclaimable:          150496 kB
[bm-row-container-metrics] spillReadOld compression=zstd dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 serialized_bytes=27280653749 batches=26025 create_reader_ms=657.658 next_batch_ms=12025.377 copy_rows_ms=6860.889 list_rows_ms=172.024
===== CASE 21/38 spillReadOld(old_zstd_variable) END exit=0 elapsed=46s 2026-06-16T15:59:16+08:00 =====
===== CASE 22/38 spillReadBm(bm_zstd_variable) START 2026-06-16T15:59:16+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillReadBm.bm_zstd_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T15:59:16+08:00 -----
  MemFree:            251252944 kB
  MemAvailable:       249943228 kB
  Buffers:                33628 kB
  Cached:               2993768 kB
  SwapCached:                 0 kB
  Dirty:                    420 kB
  Writeback:                  0 kB
  Shmem:                2600200 kB
  SReclaimable:          167808 kB
----- meminfo after sync before drop_caches 2026-06-16T15:59:16+08:00 -----
  MemFree:            251252384 kB
  MemAvailable:       249942668 kB
  Buffers:                33644 kB
  Cached:               2993752 kB
  SwapCached:                 0 kB
  Dirty:                      0 kB
  Writeback:                  0 kB
  Shmem:                2600200 kB
  SReclaimable:          167808 kB
----- meminfo after drop_caches 2026-06-16T15:59:16+08:00 -----
  MemFree:            251436388 kB
  MemAvailable:       250035152 kB
  Buffers:                 9024 kB
  Cached:               2852192 kB
  SwapCached:                 0 kB
  Dirty:                      0 kB
  Writeback:                 48 kB
  Shmem:                2600200 kB
  SReclaimable:          150440 kB
[bm-row-container-metrics] spillReadBm compression=zstd dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 row_ids=0 windows=0 result=pointers begin_ms=0.073 list_rows_ms=12227.789 window_load_ms=0.000 bulk_estimate_ms=0.010 bulk_reserve_ms=0.000 bulk_collect_blocks_ms=1.174 bulk_batch_pin_ms=11356.246 bulk_update_ptrs_ms=0.589 bulk_rebase_strings_ms=790.945 bulk_append_ptrs_ms=78.304 bulk_append_row_ids_ms=0.000 bulk_estimated_bytes=27774681088 bulk_pinned_blocks=6622 bulk_pointer_rows=25712209 bulk_row_id_rows=0 bulk_rebased_string_views=25712209 bm_batch_pins=1 bm_pin_reads=6622 bm_spill_read_count=6622 bm_spill_read_bytes=27774681088 bm_spill_physical_read_bytes=842320297 bm_decompress_ms=11289.834 io_accepted=6622 io_completed=6622 io_completed_bytes=842320297 io_successful=6622 io_failed=0 io_rejected=0 io_submitted_high=6622 io_submitted_medium=0 io_submitted_low=0 io_completed_high=6622 io_completed_medium=0 io_completed_low=0 io_submit_batches=53 io_completion_batches=53 io_queue_wait_ms=1448620.777 io_avg_queue_wait_us=218758.801 io_device_latency_ms=29701.092 io_avg_device_latency_us=4485.215 io_end_to_end_latency_ms=1478325.127 io_avg_end_to_end_latency_us=223244.507 io_backend_submit_ms=452.297 io_backend_reap_ms=0.171 io_worker_wait_ms=8.285 io_future_fulfill_ms=2.230
===== CASE 22/38 spillReadBm(bm_zstd_variable) END exit=0 elapsed=34s 2026-06-16T16:00:10+08:00 =====
===== CASE 23/38 spillWriteOld(old_raw_fixed) START 2026-06-16T16:00:10+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillWriteOld.old_raw_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T16:00:10+08:00 -----
  MemFree:            251257148 kB
  MemAvailable:       249944196 kB
  Buffers:                34372 kB
  Cached:               2996260 kB
  SwapCached:                 0 kB
  Dirty:                    488 kB
  Writeback:                  0 kB
  Shmem:                2600328 kB
  SReclaimable:          158196 kB
----- meminfo after sync before drop_caches 2026-06-16T16:00:10+08:00 -----
  MemFree:            251256580 kB
  MemAvailable:       249943628 kB
  Buffers:                34404 kB
  Cached:               2996228 kB
  SwapCached:                 0 kB
  Dirty:                    488 kB
  Writeback:                  0 kB
  Shmem:                2600328 kB
  SReclaimable:          158196 kB
----- meminfo after drop_caches 2026-06-16T16:00:11+08:00 -----
  MemFree:            251435208 kB
  MemAvailable:       250034240 kB
  Buffers:                 8992 kB
  Cached:               2853324 kB
  SwapCached:                 0 kB
  Dirty:                    312 kB
  Writeback:                  0 kB
  Shmem:                2600332 kB
  SReclaimable:          150496 kB
[bm-row-container-metrics] spillWriteOld compression=raw dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 store_setup_ms=42587.599 spill_ms=41790.813 spill_bytes=28185937944 files=26883
===== CASE 23/38 spillWriteOld(old_raw_fixed) END exit=0 elapsed=90s 2026-06-16T16:02:01+08:00 =====
===== CASE 24/38 spillWriteBm(bm_raw_fixed) START 2026-06-16T16:02:01+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillWriteBm.bm_raw_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T16:02:01+08:00 -----
  MemFree:            251217568 kB
  MemAvailable:       249915300 kB
  Buffers:                43032 kB
  Cached:               2999984 kB
  SwapCached:                 0 kB
  Dirty:                    716 kB
  Writeback:                  0 kB
  Shmem:                2600556 kB
  SReclaimable:          167512 kB
----- meminfo after sync before drop_caches 2026-06-16T16:02:01+08:00 -----
  MemFree:            251217600 kB
  MemAvailable:       249915332 kB
  Buffers:                43052 kB
  Cached:               2999980 kB
  SwapCached:                 0 kB
  Dirty:                    252 kB
  Writeback:                  0 kB
  Shmem:                2600556 kB
  SReclaimable:          167512 kB
----- meminfo after drop_caches 2026-06-16T16:02:01+08:00 -----
  MemFree:            251432928 kB
  MemAvailable:       250031760 kB
  Buffers:                 8796 kB
  Cached:               2853268 kB
  SwapCached:                 0 kB
  Dirty:                    264 kB
  Writeback:                  0 kB
  Shmem:                2600556 kB
  SReclaimable:          150660 kB
[bm-row-container-metrics] spillWriteBm compression=raw dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 store_setup_ms=22820.731 flush_ms=12559.141 bm_spill_write_count=7681 bm_spill_write_bytes=32216449024 bm_spill_physical_write_bytes=32216694816 bm_compress_ms=0.000 bm_compressed_blocks=0 flush_zero_heap_tail_ms=0.282 flush_collect_blocks_ms=1.081 flush_spill_blocks_ms=12530.193 flush_chunks=7681 flush_row_blocks=7681 flush_heap_blocks=0 flush_total_blocks=7681 flush_row_block_bytes=32216449024 flush_heap_block_bytes=0 flush_used_row_bytes=32212254720 flush_used_heap_bytes=0 flush_unused_heap_tail_bytes=0
===== CASE 24/38 spillWriteBm(bm_raw_fixed) END exit=0 elapsed=54s 2026-06-16T16:03:15+08:00 =====
===== CASE 25/38 spillWriteOld(old_lz4_fixed) START 2026-06-16T16:03:15+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillWriteOld.old_lz4_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T16:03:15+08:00 -----
  MemFree:            251250076 kB
  MemAvailable:       249940060 kB
  Buffers:                34724 kB
  Cached:               3000416 kB
  SwapCached:                 0 kB
  Dirty:                    472 kB
  Writeback:                  0 kB
  Shmem:                2600716 kB
  SReclaimable:          160064 kB
----- meminfo after sync before drop_caches 2026-06-16T16:03:15+08:00 -----
  MemFree:            251249500 kB
  MemAvailable:       249939484 kB
  Buffers:                34748 kB
  Cached:               3000392 kB
  SwapCached:                 0 kB
  Dirty:                    472 kB
  Writeback:                  0 kB
  Shmem:                2600716 kB
  SReclaimable:          160064 kB
----- meminfo after drop_caches 2026-06-16T16:03:16+08:00 -----
  MemFree:            251434220 kB
  MemAvailable:       250033396 kB
  Buffers:                 8888 kB
  Cached:               2853444 kB
  SwapCached:                 0 kB
  Dirty:                    472 kB
  Writeback:                  0 kB
  Shmem:                2600716 kB
  SReclaimable:          150800 kB
[bm-row-container-metrics] spillWriteOld compression=lz4 dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 store_setup_ms=42104.576 spill_ms=95124.619 spill_bytes=27257858112 files=26883
===== CASE 25/38 spillWriteOld(old_lz4_fixed) END exit=0 elapsed=158s 2026-06-16T16:06:14+08:00 =====
===== CASE 26/38 spillWriteBm(bm_lz4_fixed) START 2026-06-16T16:06:14+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillWriteBm.bm_lz4_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T16:06:14+08:00 -----
  MemFree:            251211132 kB
  MemAvailable:       249914484 kB
  Buffers:                35716 kB
  Cached:               3018716 kB
  SwapCached:                 0 kB
  Dirty:                    656 kB
  Writeback:                  0 kB
  Shmem:                2600516 kB
  SReclaimable:          167204 kB
----- meminfo after sync before drop_caches 2026-06-16T16:06:14+08:00 -----
  MemFree:            251211132 kB
  MemAvailable:       249914488 kB
  Buffers:                35748 kB
  Cached:               3018708 kB
  SwapCached:                 0 kB
  Dirty:                    168 kB
  Writeback:                  0 kB
  Shmem:                2600516 kB
  SReclaimable:          167204 kB
----- meminfo after drop_caches 2026-06-16T16:06:15+08:00 -----
  MemFree:            251432536 kB
  MemAvailable:       250031740 kB
  Buffers:                 8672 kB
  Cached:               2853772 kB
  SwapCached:                 0 kB
  Dirty:                    184 kB
  Writeback:                  0 kB
  Shmem:                2600516 kB
  SReclaimable:          150416 kB
[bm-row-container-metrics] spillWriteBm compression=lz4 dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 store_setup_ms=22788.735 flush_ms=61643.221 bm_spill_write_count=7681 bm_spill_write_bytes=32216449024 bm_spill_physical_write_bytes=27762192235 bm_compress_ms=59881.641 bm_compressed_blocks=7681 flush_zero_heap_tail_ms=0.280 flush_collect_blocks_ms=1.006 flush_spill_blocks_ms=61615.161 flush_chunks=7681 flush_row_blocks=7681 flush_heap_blocks=0 flush_total_blocks=7681 flush_row_block_bytes=32216449024 flush_heap_block_bytes=0 flush_used_row_bytes=32212254720 flush_used_heap_bytes=0 flush_unused_heap_tail_bytes=0
===== CASE 26/38 spillWriteBm(bm_lz4_fixed) END exit=0 elapsed=137s 2026-06-16T16:08:52+08:00 =====
===== CASE 27/38 spillWriteOld(old_zstd_fixed) START 2026-06-16T16:08:52+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillWriteOld.old_zstd_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T16:08:52+08:00 -----
  MemFree:            251259808 kB
  MemAvailable:       249947776 kB
  Buffers:                29152 kB
  Cached:               3000468 kB
  SwapCached:                 0 kB
  Dirty:                    332 kB
  Writeback:                  0 kB
  Shmem:                2599768 kB
  SReclaimable:          160504 kB
----- meminfo after sync before drop_caches 2026-06-16T16:08:52+08:00 -----
  MemFree:            251259248 kB
  MemAvailable:       249947216 kB
  Buffers:                29168 kB
  Cached:               3000452 kB
  SwapCached:                 0 kB
  Dirty:                    332 kB
  Writeback:                  0 kB
  Shmem:                2599768 kB
  SReclaimable:          160504 kB
----- meminfo after drop_caches 2026-06-16T16:08:52+08:00 -----
  MemFree:            251428440 kB
  MemAvailable:       250030136 kB
  Buffers:                 9936 kB
  Cached:               2856892 kB
  SwapCached:                 0 kB
  Dirty:                    332 kB
  Writeback:                  0 kB
  Shmem:                2599768 kB
  SReclaimable:          151252 kB
[bm-row-container-metrics] spillWriteOld compression=zstd dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 store_setup_ms=42195.306 spill_ms=218950.741 spill_bytes=23622070679 files=26883
===== CASE 27/38 spillWriteOld(old_zstd_fixed) END exit=0 elapsed=269s 2026-06-16T16:13:41+08:00 =====
===== CASE 28/38 spillWriteBm(bm_zstd_fixed) START 2026-06-16T16:13:41+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillWriteBm.bm_zstd_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T16:13:41+08:00 -----
  MemFree:            251171112 kB
  MemAvailable:       249908616 kB
  Buffers:                45520 kB
  Cached:               3074288 kB
  SwapCached:                 0 kB
  Dirty:                    252 kB
  Writeback:                  0 kB
  Shmem:                2600396 kB
  SReclaimable:          170048 kB
----- meminfo after sync before drop_caches 2026-06-16T16:13:41+08:00 -----
  MemFree:            251171112 kB
  MemAvailable:       249908616 kB
  Buffers:                45536 kB
  Cached:               3074280 kB
  SwapCached:                 0 kB
  Dirty:                    280 kB
  Writeback:                  0 kB
  Shmem:                2600396 kB
  SReclaimable:          170048 kB
----- meminfo after drop_caches 2026-06-16T16:13:41+08:00 -----
  MemFree:            251451140 kB
  MemAvailable:       250053504 kB
  Buffers:                 8660 kB
  Cached:               2858664 kB
  SwapCached:                 0 kB
  Dirty:                    300 kB
  Writeback:                  0 kB
  Shmem:                2600396 kB
  SReclaimable:          152272 kB
[bm-row-container-metrics] spillWriteBm compression=zstd dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 store_setup_ms=22811.678 flush_ms=174500.854 bm_spill_write_count=7681 bm_spill_write_bytes=32216449024 bm_spill_physical_write_bytes=23291447798 bm_compress_ms=171634.841 bm_compressed_blocks=7681 flush_zero_heap_tail_ms=0.286 flush_collect_blocks_ms=1.034 flush_spill_blocks_ms=174472.225 flush_chunks=7681 flush_row_blocks=7681 flush_heap_blocks=0 flush_total_blocks=7681 flush_row_block_bytes=32216449024 flush_heap_block_bytes=0 flush_used_row_bytes=32212254720 flush_used_heap_bytes=0 flush_unused_heap_tail_bytes=0
===== CASE 28/38 spillWriteBm(bm_zstd_fixed) END exit=0 elapsed=201s 2026-06-16T16:17:22+08:00 =====
===== CASE 29/38 spillWriteOld(old_raw_variable) START 2026-06-16T16:17:22+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillWriteOld.old_raw_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T16:17:22+08:00 -----
  MemFree:            251245296 kB
  MemAvailable:       249943784 kB
  Buffers:                36972 kB
  Cached:               3013884 kB
  SwapCached:                 0 kB
  Dirty:                    432 kB
  Writeback:                  0 kB
  Shmem:                2600200 kB
  SReclaimable:          160780 kB
----- meminfo after sync before drop_caches 2026-06-16T16:17:22+08:00 -----
  MemFree:            251245264 kB
  MemAvailable:       249943752 kB
  Buffers:                36988 kB
  Cached:               3013868 kB
  SwapCached:                 0 kB
  Dirty:                    108 kB
  Writeback:                296 kB
  Shmem:                2600200 kB
  SReclaimable:          160780 kB
----- meminfo after drop_caches 2026-06-16T16:17:23+08:00 -----
  MemFree:            251442768 kB
  MemAvailable:       250044904 kB
  Buffers:                 8868 kB
  Cached:               2858552 kB
  SwapCached:                 0 kB
  Dirty:                    124 kB
  Writeback:                  0 kB
  Shmem:                2600200 kB
  SReclaimable:          152036 kB
[bm-row-container-metrics] spillWriteOld compression=raw dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 store_setup_ms=6421.607 spill_ms=14712.357 spill_bytes=27280861949 files=26025
===== CASE 29/38 spillWriteOld(old_raw_variable) END exit=0 elapsed=26s 2026-06-16T16:18:09+08:00 =====
===== CASE 30/38 spillWriteBm(bm_raw_variable) START 2026-06-16T16:18:09+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillWriteBm.bm_raw_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T16:18:09+08:00 -----
  MemFree:            251274216 kB
  MemAvailable:       249955348 kB
  Buffers:                33812 kB
  Cached:               2974784 kB
  SwapCached:                 0 kB
  Dirty:                    460 kB
  Writeback:                  0 kB
  Shmem:                2600296 kB
  SReclaimable:          168392 kB
----- meminfo after sync before drop_caches 2026-06-16T16:18:09+08:00 -----
  MemFree:            251268672 kB
  MemAvailable:       249951316 kB
  Buffers:                33832 kB
  Cached:               2977788 kB
  SwapCached:                 0 kB
  Dirty:                    464 kB
  Writeback:                  0 kB
  Shmem:                2600296 kB
  SReclaimable:          168392 kB
----- meminfo after drop_caches 2026-06-16T16:18:09+08:00 -----
  MemFree:            251439192 kB
  MemAvailable:       250042328 kB
  Buffers:                 9352 kB
  Cached:               2858860 kB
  SwapCached:                 0 kB
  Dirty:                    540 kB
  Writeback:                  0 kB
  Shmem:                2600304 kB
  SReclaimable:          152812 kB
[bm-row-container-metrics] spillWriteBm compression=raw dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 store_setup_ms=5364.004 flush_ms=11163.635 bm_spill_write_count=6622 bm_spill_write_bytes=27774681088 bm_spill_physical_write_bytes=27774892992 bm_compress_ms=0.000 bm_compressed_blocks=0 flush_zero_heap_tail_ms=28.591 flush_collect_blocks_ms=0.841 flush_spill_blocks_ms=11132.837 flush_chunks=246 flush_row_blocks=246 flush_heap_blocks=6376 flush_total_blocks=6622 flush_row_block_bytes=1031798784 flush_heap_block_bytes=26742882304 flush_used_row_bytes=1028488360 flush_used_heap_bytes=26329302016 flush_unused_heap_tail_bytes=413580288
===== CASE 30/38 spillWriteBm(bm_raw_variable) END exit=0 elapsed=27s 2026-06-16T16:18:56+08:00 =====
===== CASE 31/38 spillWriteOld(old_lz4_variable) START 2026-06-16T16:18:56+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillWriteOld.old_lz4_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T16:18:56+08:00 -----
  MemFree:            251318456 kB
  MemAvailable:       249986144 kB
  Buffers:                26480 kB
  Cached:               2963120 kB
  SwapCached:                 0 kB
  Dirty:                    368 kB
  Writeback:                  0 kB
  Shmem:                2600396 kB
  SReclaimable:          160668 kB
----- meminfo after sync before drop_caches 2026-06-16T16:18:56+08:00 -----
  MemFree:            251318456 kB
  MemAvailable:       249986144 kB
  Buffers:                26496 kB
  Cached:               2963112 kB
  SwapCached:                 0 kB
  Dirty:                    412 kB
  Writeback:                  0 kB
  Shmem:                2600396 kB
  SReclaimable:          160668 kB
----- meminfo after drop_caches 2026-06-16T16:18:56+08:00 -----
  MemFree:            251450704 kB
  MemAvailable:       250052840 kB
  Buffers:                 8680 kB
  Cached:               2858620 kB
  SwapCached:                 0 kB
  Dirty:                    268 kB
  Writeback:                112 kB
  Shmem:                2600396 kB
  SReclaimable:          152380 kB
[bm-row-container-metrics] spillWriteOld compression=lz4 dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 store_setup_ms=6481.896 spill_ms=12569.360 spill_bytes=1232145318 files=26025
===== CASE 31/38 spillWriteOld(old_lz4_variable) END exit=0 elapsed=20s 2026-06-16T16:19:36+08:00 =====
===== CASE 32/38 spillWriteBm(bm_lz4_variable) START 2026-06-16T16:19:36+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillWriteBm.bm_lz4_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T16:19:36+08:00 -----
  MemFree:            251257432 kB
  MemAvailable:       249952812 kB
  Buffers:                40936 kB
  Cached:               2997392 kB
  SwapCached:                 0 kB
  Dirty:                    692 kB
  Writeback:                  0 kB
  Shmem:                2600488 kB
  SReclaimable:          167440 kB
----- meminfo after sync before drop_caches 2026-06-16T16:19:36+08:00 -----
  MemFree:            251257936 kB
  MemAvailable:       249953316 kB
  Buffers:                40968 kB
  Cached:               2997368 kB
  SwapCached:                 0 kB
  Dirty:                    728 kB
  Writeback:                  0 kB
  Shmem:                2600488 kB
  SReclaimable:          167440 kB
----- meminfo after drop_caches 2026-06-16T16:19:37+08:00 -----
  MemFree:            251443056 kB
  MemAvailable:       250045656 kB
  Buffers:                 8980 kB
  Cached:               2859528 kB
  SwapCached:                 0 kB
  Dirty:                    744 kB
  Writeback:                  0 kB
  Shmem:                2600492 kB
  SReclaimable:          152228 kB
[bm-row-container-metrics] spillWriteBm compression=lz4 dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 store_setup_ms=5336.606 flush_ms=6401.812 bm_spill_write_count=6622 bm_spill_write_bytes=27774681088 bm_spill_physical_write_bytes=1197664089 bm_compress_ms=5963.148 bm_compressed_blocks=6622 flush_zero_heap_tail_ms=27.975 flush_collect_blocks_ms=0.841 flush_spill_blocks_ms=6371.703 flush_chunks=246 flush_row_blocks=246 flush_heap_blocks=6376 flush_total_blocks=6622 flush_row_block_bytes=1031798784 flush_heap_block_bytes=26742882304 flush_used_row_bytes=1028488360 flush_used_heap_bytes=26329302016 flush_unused_heap_tail_bytes=413580288
===== CASE 32/38 spillWriteBm(bm_lz4_variable) END exit=0 elapsed=13s 2026-06-16T16:20:10+08:00 =====
===== CASE 33/38 spillWriteOld(old_zstd_variable) START 2026-06-16T16:20:10+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillWriteOld.old_zstd_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T16:20:10+08:00 -----
  MemFree:            251278912 kB
  MemAvailable:       249958672 kB
  Buffers:                32304 kB
  Cached:               2983064 kB
  SwapCached:                 0 kB
  Dirty:                    524 kB
  Writeback:                  0 kB
  Shmem:                2600548 kB
  SReclaimable:          159124 kB
----- meminfo after sync before drop_caches 2026-06-16T16:20:10+08:00 -----
  MemFree:            251278912 kB
  MemAvailable:       249958676 kB
  Buffers:                32332 kB
  Cached:               2983052 kB
  SwapCached:                 0 kB
  Dirty:                    108 kB
  Writeback:                444 kB
  Shmem:                2600548 kB
  SReclaimable:          159124 kB
----- meminfo after drop_caches 2026-06-16T16:20:10+08:00 -----
  MemFree:            251426312 kB
  MemAvailable:       250029104 kB
  Buffers:                 9288 kB
  Cached:               2867796 kB
  SwapCached:                 0 kB
  Dirty:                     24 kB
  Writeback:                  0 kB
  Shmem:                2608744 kB
  SReclaimable:          152228 kB
[bm-row-container-metrics] spillWriteOld compression=zstd dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 store_setup_ms=6487.182 spill_ms=16823.192 spill_bytes=854646829 files=26025
===== CASE 33/38 spillWriteOld(old_zstd_variable) END exit=0 elapsed=25s 2026-06-16T16:20:55+08:00 =====
===== CASE 34/38 spillWriteBm(bm_zstd_variable) START 2026-06-16T16:20:55+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=spillWriteBm.bm_zstd_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T16:20:55+08:00 -----
  MemFree:            251212308 kB
  MemAvailable:       249920244 kB
  Buffers:                41036 kB
  Cached:               3031112 kB
  SwapCached:                 0 kB
  Dirty:                    464 kB
  Writeback:                  0 kB
  Shmem:                2608852 kB
  SReclaimable:          167076 kB
----- meminfo after sync before drop_caches 2026-06-16T16:20:55+08:00 -----
  MemFree:            251212316 kB
  MemAvailable:       249920252 kB
  Buffers:                41052 kB
  Cached:               3031096 kB
  SwapCached:                 0 kB
  Dirty:                     56 kB
  Writeback:                396 kB
  Shmem:                2608852 kB
  SReclaimable:          167076 kB
----- meminfo after drop_caches 2026-06-16T16:20:55+08:00 -----
  MemFree:            251424224 kB
  MemAvailable:       250026340 kB
  Buffers:                 9052 kB
  Cached:               2868008 kB
  SwapCached:                 0 kB
  Dirty:                      0 kB
  Writeback:                136 kB
  Shmem:                2608856 kB
  SReclaimable:          151064 kB
[bm-row-container-metrics] spillWriteBm compression=zstd dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 store_setup_ms=5344.530 flush_ms=14862.430 bm_spill_write_count=6622 bm_spill_write_bytes=27774681088 bm_spill_physical_write_bytes=842472926 bm_compress_ms=14466.080 bm_compressed_blocks=6622 flush_zero_heap_tail_ms=27.965 flush_collect_blocks_ms=0.839 flush_spill_blocks_ms=14832.252 flush_chunks=246 flush_row_blocks=246 flush_heap_blocks=6376 flush_total_blocks=6622 flush_row_block_bytes=1031798784 flush_heap_block_bytes=26742882304 flush_used_row_bytes=1028488360 flush_used_heap_bytes=26329302016 flush_unused_heap_tail_bytes=413580288
===== CASE 34/38 spillWriteBm(bm_zstd_variable) END exit=0 elapsed=21s 2026-06-16T16:21:36+08:00 =====
===== CASE 35/38 storeRowOld(old_fixed) START 2026-06-16T16:21:36+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=storeRowOld.old_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T16:21:36+08:00 -----
  MemFree:            251233400 kB
  MemAvailable:       249929424 kB
  Buffers:                34060 kB
  Cached:               3022716 kB
  SwapCached:                 0 kB
  Dirty:                    800 kB
  Writeback:                  0 kB
  Shmem:                2608952 kB
  SReclaimable:          158752 kB
----- meminfo after sync before drop_caches 2026-06-16T16:21:36+08:00 -----
  MemFree:            251233400 kB
  MemAvailable:       249929428 kB
  Buffers:                34088 kB
  Cached:               3022696 kB
  SwapCached:                 0 kB
  Dirty:                    180 kB
  Writeback:                100 kB
  Shmem:                2608952 kB
  SReclaimable:          158752 kB
----- meminfo after drop_caches 2026-06-16T16:21:37+08:00 -----
  MemFree:            251420408 kB
  MemAvailable:       250024388 kB
  Buffers:                 9140 kB
  Cached:               2872352 kB
  SwapCached:                 0 kB
  Dirty:                     48 kB
  Writeback:                148 kB
  Shmem:                2608928 kB
  SReclaimable:          150516 kB
===== CASE 35/38 storeRowOld(old_fixed) END exit=0 elapsed=43s 2026-06-16T16:22:40+08:00 =====
===== CASE 36/38 storeRowBm(bm_fixed) START 2026-06-16T16:22:40+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=storeRowBm.bm_fixed. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T16:22:40+08:00 -----
  MemFree:            251257928 kB
  MemAvailable:       249945580 kB
  Buffers:                34836 kB
  Cached:               3004836 kB
  SwapCached:                 0 kB
  Dirty:                   1300 kB
  Writeback:                  0 kB
  Shmem:                2609092 kB
  SReclaimable:          159136 kB
----- meminfo after sync before drop_caches 2026-06-16T16:22:40+08:00 -----
  MemFree:            251257376 kB
  MemAvailable:       249945028 kB
  Buffers:                34852 kB
  Cached:               3004828 kB
  SwapCached:                 0 kB
  Dirty:                    372 kB
  Writeback:                  0 kB
  Shmem:                2609092 kB
  SReclaimable:          159136 kB
----- meminfo after drop_caches 2026-06-16T16:22:40+08:00 -----
  MemFree:            251426112 kB
  MemAvailable:       250030220 kB
  Buffers:                 9196 kB
  Cached:               2872516 kB
  SwapCached:                 0 kB
  Dirty:                     24 kB
  Writeback:                  0 kB
  Shmem:                2609100 kB
  SReclaimable:          150116 kB
[bm-row-container-metrics] storeRowBm dataset=fixed benchmark_iterations=1 diagnostic_iterations=1 benchmark_logical_bytes=26843545600 logical_bytes=16777216 rows=838861 append_only_ms=4.273 append_fixed_ms=18.506 append_full_ms=16.308 fixed_store_extra_ms=14.233 variable_store_extra_ms=0.000
===== CASE 36/38 storeRowBm(bm_fixed) END exit=0 elapsed=24s 2026-06-16T16:23:24+08:00 =====
===== CASE 37/38 storeRowOld(old_variable) START 2026-06-16T16:23:24+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=storeRowOld.old_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T16:23:24+08:00 -----
  MemFree:            251329412 kB
  MemAvailable:       250007988 kB
  Buffers:                31192 kB
  Cached:               2992176 kB
  SwapCached:                 0 kB
  Dirty:                    524 kB
  Writeback:                  0 kB
  Shmem:                2609152 kB
  SReclaimable:          157408 kB
----- meminfo after sync before drop_caches 2026-06-16T16:23:24+08:00 -----
  MemFree:            251329412 kB
  MemAvailable:       250007988 kB
  Buffers:                31216 kB
  Cached:               2992152 kB
  SwapCached:                 0 kB
  Dirty:                    116 kB
  Writeback:                392 kB
  Shmem:                2609152 kB
  SReclaimable:          157408 kB
----- meminfo after drop_caches 2026-06-16T16:23:24+08:00 -----
  MemFree:            251483304 kB
  MemAvailable:       250085312 kB
  Buffers:                 8828 kB
  Cached:               2868532 kB
  SwapCached:                 0 kB
  Dirty:                      0 kB
  Writeback:                484 kB
  Shmem:                2609128 kB
  SReclaimable:          150296 kB
===== CASE 37/38 storeRowOld(old_variable) END exit=0 elapsed=8s 2026-06-16T16:23:52+08:00 =====
===== CASE 38/38 storeRowBm(bm_variable) START 2026-06-16T16:23:52+08:00 =====
command: timeout 900s /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark --bm_regex=storeRowBm.bm_variable. --bm_row_container_data_bytes=26843545600 --bm_row_container_warmup_data_bytes=134217728 
----- meminfo before sync 2026-06-16T16:23:52+08:00 -----
  MemFree:            251297464 kB
  MemAvailable:       249971848 kB
  Buffers:                29868 kB
  Cached:               2985692 kB
  SwapCached:                 0 kB
  Dirty:                    440 kB
  Writeback:                  0 kB
  Shmem:                2609220 kB
  SReclaimable:          156964 kB
----- meminfo after sync before drop_caches 2026-06-16T16:23:52+08:00 -----
  MemFree:            251297464 kB
  MemAvailable:       249971848 kB
  Buffers:                29892 kB
  Cached:               2985668 kB
  SwapCached:                 0 kB
  Dirty:                    440 kB
  Writeback:                  0 kB
  Shmem:                2609220 kB
  SReclaimable:          156964 kB
----- meminfo after drop_caches 2026-06-16T16:23:52+08:00 -----
  MemFree:            251437708 kB
  MemAvailable:       250041688 kB
  Buffers:                 9164 kB
  Cached:               2872800 kB
  SwapCached:                 0 kB
  Dirty:                    304 kB
  Writeback:                  0 kB
  Shmem:                2609220 kB
  SReclaimable:          149796 kB
[bm-row-container-metrics] storeRowBm dataset=variable benchmark_iterations=1 diagnostic_iterations=1 benchmark_logical_bytes=26843545600 logical_bytes=16777216 rows=16071 append_only_ms=0.101 append_fixed_ms=0.531 append_full_ms=3.898 fixed_store_extra_ms=0.431 variable_store_extra_ms=3.366
===== CASE 38/38 storeRowBm(bm_variable) END exit=0 elapsed=6s 2026-06-16T16:24:18+08:00 =====
===== ALL CASES END exit=0 output_dir=/data00/home/wangxinshuo.db/bolt/log/bolt-bm-row-container-20260616-152329 2026-06-16T16:24:18+08:00 =====
```

### stdout.txt
```
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
readOld(old_fixed)                                           7.06s   141.69m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
readBm(bm_fixed)                                             4.54s   220.20m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
readOld(old_variable)                                        2.93s   340.73m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
readBm(bm_variable)                                          2.74s   364.40m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
storeBatchOld(old_fixed)                                    23.11s    43.28m
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
storeBatchBm(bm_fixed)                                       6.84s   146.27m
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
storeBatchOld(old_variable)                                  5.57s   179.56m
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
storeBatchBm(bm_variable)                                    4.74s   210.76m
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
stringCopyPathBmHeapSimd(bm_heap_simd_variable)              4.38s   228.34m
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
stringCopyPathBmHeapStd(bm_heap_std_variable)                5.77s   173.29m
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillReadOld(old_raw_fixed)                                2.45min     6.79m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillReadBm(bm_raw_fixed)                                   18.83s    53.12m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillReadOld(old_lz4_fixed)                                1.70min     9.78m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillReadBm(bm_lz4_fixed)                                   18.01s    55.52m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillReadOld(old_zstd_fixed)                               1.88min     8.84m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillReadBm(bm_zstd_fixed)                                  57.04s    17.53m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillReadOld(old_raw_variable)                              52.12s    19.19m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillReadBm(bm_raw_variable)                                14.43s    69.32m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillReadOld(old_lz4_variable)                              18.42s    54.29m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillReadBm(bm_lz4_variable)                                10.03s    99.67m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillReadOld(old_zstd_variable)                             20.12s    49.71m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillReadBm(bm_zstd_variable)                               12.23s    81.73m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillWriteOld(old_raw_fixed)                                41.79s    23.93m
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillWriteBm(bm_raw_fixed)                                  12.56s    79.62m
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillWriteOld(old_lz4_fixed)                               1.59min    10.51m
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillWriteBm(bm_lz4_fixed)                                 1.03min    16.22m
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillWriteOld(old_zstd_fixed)                              3.65min     4.57m
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillWriteBm(bm_zstd_fixed)                                2.91min     5.73m
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillWriteOld(old_raw_variable)                             14.71s    67.97m
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillWriteBm(bm_raw_variable)                               11.16s    89.58m
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillWriteOld(old_lz4_variable)                             12.57s    79.56m
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillWriteBm(bm_lz4_variable)                                6.40s   156.21m
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillWriteOld(old_zstd_variable)                            16.82s    59.44m
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillWriteBm(bm_zstd_variable)                              14.86s    67.28m
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
storeRowOld(old_fixed)                                      42.10s    23.75m
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
storeRowBm(bm_fixed)                                        22.77s    43.92m
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
storeRowOld(old_variable)                                    6.16s   162.45m
----------------------------------------------------------------------------
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerBatchStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
storeRowBm(bm_variable)                                      5.12s   195.21m
----------------------------------------------------------------------------
```