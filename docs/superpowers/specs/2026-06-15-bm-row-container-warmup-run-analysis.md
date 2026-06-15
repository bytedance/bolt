# BM RowContainer Warm-up Runner Benchmark 分析

本文整理 `log/bolt-bm-row-container-20260615-141645` 下的最新 benchmark 结果，并和 2026-06-12 的逐 case cold runner 结果、2026-06-09 的单进程连跑结果做对比。

## 运行口径

日志文件：

- stdout: `log/bolt-bm-row-container-20260615-141645/stdout.txt`
- stderr: `log/bolt-bm-row-container-20260615-141645/stderr.txt`

runner 命令特征：

- benchmark binary: `_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark`
- logical input: `--bm_row_container_data_bytes=26843545600`
- same-process warm-up: `--bm_row_container_warmup_data_bytes=134217728`
- 每个 case 仍然独立启动 benchmark binary。
- 每个 case 前仍然执行 `sync + drop_caches`。
- 默认输出目录已经切到 `/data00/home/wangxinshuo.db/bolt/log/...`。

## 完整性检查

| 项目 | 数量 |
| --- | ---: |
| runner START | 32 |
| runner END exit=0 | 32 |
| warm-up 参数出现次数 | 32 |
| runner ERROR / Aborted / 非 0 exit | 0 |

runner 末尾状态：

```text
===== ALL CASES END exit=0 output_dir=/data00/home/wangxinshuo.db/bolt/log/bolt-bm-row-container-20260615-141645 2026-06-15T15:17:15+08:00 =====
```

因此本次主 row container benchmark 没有漏 case，也没有运行失败。

## 总体结果

### Store

| 数据集 | old time | BM time | BM 相对速度 |
| --- | ---: | ---: | ---: |
| fixed | 40.61s | 37.33s | 108.8% |
| variable | 6.20s | 10.40s | 59.6% |

fixed store 里 BM 小幅快于 old。variable store 仍然是本次最明显的问题：BM 比 old 慢，且与 2026-06-12 cold runner 的 10.44s 基本一致，128MiB warm-up 没有改善正式 store benchmark 的 25GiB variable 写入。

### Resident Read

| 数据集 | old time | BM time | BM 相对速度 |
| --- | ---: | ---: | ---: |
| fixed | 7.05s | 4.95s | 142.4% |
| variable | 3.06s | 2.94s | 104.1% |

resident read 基本稳定。BM fixed read 继续明显快于 old；variable read 两者接近。

### SpillWrite

| 数据集 | 压缩 | old time | BM time | BM 相对速度 |
| --- | --- | ---: | ---: | ---: |
| fixed | RAW | 42.38s | 8.81s | 481.0% |
| fixed | LZ4 | 1.58min | 1.04min | 151.9% |
| fixed | ZSTD | 3.64min | 3.05min | 119.3% |
| variable | RAW | 14.49s | 9.18s | 157.8% |
| variable | LZ4 | 12.56s | 6.25s | 201.0% |
| variable | ZSTD | 16.77s | 15.21s | 110.3% |

BM spill write 相比 2026-06-12 有小幅改善，但没有回到 2026-06-09 单进程连跑的热态水平。压缩路径仍然主要由压缩 CPU 主导。

### SpillRead

| 数据集 | 压缩 | old time | BM time | BM 相对速度 |
| --- | --- | ---: | ---: | ---: |
| fixed | RAW | 1.04min | 13.51s | 461.9% |
| fixed | LZ4 | 1.73min | 18.81s | 551.8% |
| fixed | ZSTD | 1.90min | 57.09s | 199.7% |
| variable | RAW | 52.96s | 7.29s | 726.5% |
| variable | LZ4 | 18.45s | 5.00s | 369.0% |
| variable | ZSTD | 20.14s | 6.58s | 306.1% |

warm-up 对 BM spill read 的收益最明显。尤其 variable RAW/LZ4/ZSTD 的 BM read 时间相比 2026-06-12 大约减半。

## 和 2026-06-12 Cold Runner 对比

2026-06-12 也是逐 case 独立进程 runner，但当时没有 same-process warm-up。下表只列变化较大的项：

| case | 2026-06-12 | 2026-06-15 | 变化 |
| --- | ---: | ---: | ---: |
| `spillReadBm(bm_raw_fixed)` | 19.15s | 13.51s | 快 29% |
| `spillReadBm(bm_raw_variable)` | 14.07s | 7.29s | 快 48% |
| `spillReadBm(bm_lz4_variable)` | 10.20s | 5.00s | 快 51% |
| `spillReadBm(bm_zstd_variable)` | 12.16s | 6.58s | 快 46% |
| `spillWriteBm(bm_raw_variable)` | 10.79s | 9.18s | 快 15% |
| `spillWriteBm(bm_lz4_variable)` | 7.61s | 6.25s | 快 18% |
| `spillWriteBm(bm_zstd_variable)` | 16.76s | 15.21s | 快 9% |
| `storeBm(bm_variable)` | 10.44s | 10.40s | 基本不变 |

结论：

- warm-up 明显改善 BM spill read。
- warm-up 小幅改善 BM spill write。
- warm-up 基本不改善 BM variable store。

## BM Spill Read 改善原因

BM spill read 的改善主要体现在 `bulk_batch_pin_ms` 下降：

| case | 2026-06-12 `bulk_batch_pin_ms` | 2026-06-15 `bulk_batch_pin_ms` |
| --- | ---: | ---: |
| fixed RAW | 14.86s | 9.22s |
| variable RAW | 13.19s | 6.28s |
| variable LZ4 | 9.32s | 3.98s |
| variable ZSTD | 11.28s | 5.56s |

这说明 128MiB same-process warm-up 对 BM 的进程内 IO 路径、io_uring worker、解压上下文等冷启动成本有明显帮助。runner 仍然逐 case 新进程，所以 warm-up 只能在单个 case 内生效，不能继承前一个 case 的进程内状态。

variable read 的具体 metric：

| 压缩 | BM time | physical read | decompress | BatchPin | rebase strings |
| --- | ---: | ---: | ---: | ---: | ---: |
| RAW | 7.29s | 27.77GB | 0 | 6.28s | 0.93s |
| LZ4 | 5.00s | 1.20GB | 3.94s | 3.98s | 0.93s |
| ZSTD | 6.58s | 844.13MB | 5.52s | 5.56s | 0.93s |

LZ4 仍然是 variable spill read 的最好点：物理读很小，解压成本低于 ZSTD。

## BM Spill Write 变化

BM variable spill write：

| 压缩 | BM time | flush | physical write | compress |
| --- | ---: | ---: | ---: | ---: |
| RAW | 9.18s | 9.18s | 27.77GB | 0 |
| LZ4 | 6.25s | 6.25s | 1.20GB | 5.90s |
| ZSTD | 15.21s | 15.21s | 844.10MB | 14.82s |

与 2026-06-12 相比有小幅改善，但改善幅度远小于 spill read。RAW variable 仍然没有回到 2026-06-09 单进程连跑的 3.56s 水平。

原因判断：

- 2026-06-09 是单 benchmark binary 连跑，前序 BM case 会把进程内全局 `DiskIoScheduler`、io_uring、worker 路径预热。
- 2026-06-15 虽然每个 case 内有 128MiB warm-up，但正式 case 仍然是新进程，且正式数据量是 25GiB。
- 128MiB warm-up 能覆盖初始化路径，但不足以消除 25GiB write 的冷触页和大量 payload 写入成本。

## BM Variable Store 仍然慢

`storeBm(bm_variable)`：

| 运行 | time |
| --- | ---: |
| 2026-06-09 单进程连跑 | 4.84s |
| 2026-06-12 cold runner | 10.44s |
| 2026-06-15 warm-up runner | 10.40s |

本次 stderr 的诊断 run：

```text
storeBm dataset=variable store_ms=7718.574
append_string_store_ms=7036.918
append_heap_alloc_ms=1485.053
append_string_copy_ms=1895.019
append_heap_record_ms=867.360
append_heap_allocations=6376
```

spill write setup 中的 variable append 更接近正式冷路径：

```text
store_setup_ms=15527.445
append_string_store_ms=14349.000
append_heap_alloc_ms=880.179
append_string_copy_ms=9805.557
append_heap_record_ms=881.453
```

这里可以看到两个结论：

1. `append_string_store_ms` 是 BM variable 写入的主要成本。
2. `append_heap_alloc_ms + append_heap_record_ms` 约 1.7s 左右，说明每行变长写重复执行 heap ensure/record 的确有优化空间，但它不是全部成本；大 payload copy 和 BM block 冷触页仍然是大头。

后续已经决定删除 `BmRowContainer::appendBatch()`，BM 写入接口回到对等的 `appendRow() + store()` row-at-a-time 口径。因此本节中的 `storeBm(bm_variable)` 结果只代表当时 `appendBatch()` 实现下的历史数据，不再作为后续 old/BM store 公平对比依据。后续 store 结果应以 `storeRowOld` 与 `storeRowBm` 为准。

## Block 数和 Chunk-local Heap 影响

本次 BM variable 仍然是：

```text
flush_chunks=246
flush_row_blocks=246
flush_heap_blocks=6376
flush_total_blocks=6622
flush_unused_heap_tail_bytes=413580288
```

这与 2026-06-12 一致，说明 chunk-local heap 造成的额外 98 个 heap block 仍然存在：

```text
6376 heap blocks - 6278 ideal global heap blocks = 98 heap blocks
98 * 4MiB = 411MiB
```

这会增加 variable spill read/write 的物理 block 数和尾部浪费，但只占总写出量约 1.5%，不能解释 BM variable store 或 RAW spill write 的主要耗时差异。

## 异常点

`spillReadOld(old_raw_fixed)` 在 stdout 上从 2026-06-12 的 2.47min 降到本次 1.04min，幅度很大。但两次 stderr phase metric 的主要阶段接近：

| 运行 | create reader | next batch | copy rows | list rows |
| --- | ---: | ---: | ---: | ---: |
| 2026-06-12 | 12.63s | 20.88s | 19.49s | 7.05s |
| 2026-06-15 | 12.49s | 20.79s | 19.05s | 7.08s |

因此这个 stdout 差异不能简单解释为 old read 代码本身加速。更可能是 Folly 统计、额外未覆盖阶段、或当时机器 IO 状态导致的口径差异。分析 BM 变化时不应把这个 old RAW fixed 异常作为主要依据。

## 结论

1. 2026-06-15 这次 runner 运行完整，32 个 case 全部成功。
2. same-process 128MiB warm-up 对 BM spill read 有明显收益，特别是 variable read。
3. warm-up 对 BM spill write 有小幅收益。
4. warm-up 对 BM variable store 基本没有收益，`storeBm(bm_variable)` 仍然约 10.4s。
5. 旧 `storeBm` benchmark 混入了 BM 独有的 `appendBatch()` 口径，不能继续作为 old/BM store 公平对比依据。
6. 后续 store benchmark 应只保留对等 row-at-a-time 接口：old 使用 `newRow() + store()`，BM 使用 `appendRow() + store()`。
