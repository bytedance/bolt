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
