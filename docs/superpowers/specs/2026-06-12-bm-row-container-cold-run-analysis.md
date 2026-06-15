# BM RowContainer Cold Runner Benchmark 分析

本文整理 `log/bolt-bm-row-container-20260612-175724` 下的 benchmark 运行结果。该结果由 `run_bm_row_container_benchmark.sh` 逐 case 执行，每个 case 启动一次 benchmark binary，并在 case 前执行 `sync + drop_caches`。

## 运行口径

日志文件：

- stdout: `log/bolt-bm-row-container-20260612-175724/stdout.txt`
- stderr: `log/bolt-bm-row-container-20260612-175724/stderr.txt`

benchmark binary：

```text
_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark
```

输入数据量：

```text
--bm_row_container_data_bytes=26843545600
```

该 binary 包含四类 benchmark：

- resident read
- spill read
- spill write
- store

它不包含 pipeline benchmark。pipeline benchmark 已拆到另一个 binary：

```text
_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_benchmark
```

## 完整性检查

本次主 row container benchmark 没有缺 case：

| 项目 | 数量 |
| --- | ---: |
| runner START | 32 |
| runner END | 32 |
| stdout benchmark 结果行 | 32 |
| 非 0 exit / ERROR | 0 |

runner 末尾状态：

```text
===== ALL CASES END exit=0 output_dir=/tmp/bolt-bm-row-container-20260612-175724 =====
```

因此，如果发现结果里没有 pipeline，是因为当前 runner 默认只运行 `bolt_exec_bm_row_container_benchmark`，不是主 benchmark case 中途漏跑。

## 总体结果

### Store

| 数据集 | old time | BM time | BM 相对速度 |
| --- | ---: | ---: | ---: |
| fixed | 38.09s | 37.13s | 102.6% |
| variable | 6.24s | 10.44s | 59.8% |

fixed store 两者接近。variable store 本次 BM 明显慢于 old，这是和 2026-06-09 文档差异最大的点之一。

### Resident Read

| 数据集 | old time | BM time | BM 相对速度 |
| --- | ---: | ---: | ---: |
| fixed | 7.03s | 4.60s | 152.8% |
| variable | 2.94s | 2.77s | 106.1% |

resident read 结果稳定，BM fixed read 仍有明显优势，variable read 两者接近。

### SpillWrite

| 数据集 | 压缩 | old time | BM time | BM 相对速度 |
| --- | --- | ---: | ---: | ---: |
| fixed | RAW | 42.11s | 9.94s | 423.6% |
| fixed | LZ4 | 1.60min | 1.08min | 148.1% |
| fixed | ZSTD | 3.68min | 3.08min | 119.5% |
| variable | RAW | 14.26s | 10.79s | 132.2% |
| variable | LZ4 | 12.54s | 7.61s | 164.8% |
| variable | ZSTD | 16.72s | 16.76s | 99.8% |

BM fixed spill write 仍然明显优于 old。variable RAW 相比 2026-06-09 文档明显变慢，但仍快于 old；variable ZSTD 基本持平。

### SpillRead

| 数据集 | 压缩 | old time | BM time | BM 相对速度 |
| --- | --- | ---: | ---: | ---: |
| fixed | RAW | 2.47min | 19.15s | 767.6% |
| fixed | LZ4 | 1.70min | 18.68s | 546.0% |
| fixed | ZSTD | 1.92min | 56.82s | 202.7% |
| variable | RAW | 52.69s | 14.07s | 374.5% |
| variable | LZ4 | 18.49s | 10.20s | 181.3% |
| variable | ZSTD | 20.20s | 12.16s | 166.1% |

本次 cold runner 下 BM spill read 相比 old 非常强，尤其 fixed RAW/LZ4。这里和旧文档差异很大，主要需要结合运行口径和 IO 状态看，不能简单视作代码本身加速。

## SpillWrite Metrics

### fixed

| 压缩 | old spill | old bytes | BM flush | BM write bytes | BM physical bytes | BM compress |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| RAW | 42.11s | 28.19GB | 9.94s | 32.22GB | 32.22GB | 0 |
| LZ4 | 95.93s | 27.26GB | 64.94s | 32.22GB | 27.76GB | 59.85s |
| ZSTD | 220.88s | 23.62GB | 184.51s | 32.22GB | 23.29GB | 178.90s |

fixed write 的压缩路径仍然主要由压缩 CPU 主导。BM LZ4/ZSTD 的 `flush_ms` 与 `bm_compress_ms` 非常接近。

### variable

| 压缩 | old spill | old bytes | BM flush | BM write bytes | BM physical bytes | BM compress | BM blocks |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| RAW | 14.26s | 27.28GB | 10.79s | 27.77GB | 27.77GB | 0 | 6622 |
| LZ4 | 12.54s | 1.23GB | 7.61s | 27.77GB | 1.20GB | 5.92s | 6622 |
| ZSTD | 16.72s | 854.65MB | 16.76s | 27.77GB | 842.54MB | 15.07s | 6622 |

variable BM 当前会写 6622 个 block。2026-06-09 文档中 variable BM 是 6524 个 block、`27363639296` bytes；本次为 6622 个 block、`27774681088` bytes，多约 98 个 4MiB block。

这个差异不是逻辑数据格式或字符串长度变化导致的，输入仍然是 fixed 三列加 1KB string。额外 block 来自 heap block 复用边界：当前实现按 row chunk 拥有 heap blocks，heap block 不跨 chunk 继续复用，因此每个 chunk 的最后一个 heap block 会留下尾部碎片，并作为完整 4MiB block spill 出去。

精确推导：

```text
rows = 25,712,209
string_length = 1024B
heap_block_size = 4MiB
rows_per_heap_block = 4MiB / 1KiB = 4096

row_size = 40B
row_block_size = 4MiB
rows_per_chunk = floor(4MiB / 40B) = 104857
row_chunks = ceil(25,712,209 / 104857) = 246
```

如果 heap block 可以全局连续复用：

```text
heap_blocks = ceil(25,712,209 / 4096) = 6278
row_blocks = 246
total_blocks = 6278 + 246 = 6524
```

这正好对应 2026-06-09 文档中的 `bm_spill_write_count=6524`。

如果 heap block 按 chunk local 复用：

```text
full_chunks = 245
last_chunk_rows = 22244
heap_blocks_per_full_chunk = ceil(104857 / 4096) = 26
last_chunk_heap_blocks = ceil(22244 / 4096) = 6

heap_blocks = 245 * 26 + 6 = 6376
row_blocks = 246
total_blocks = 6376 + 246 = 6622
```

这正好对应本次日志中的 `bm_spill_write_count=6622`。因此 98 个额外 block 的来源是：

```text
6376 - 6278 = 98 heap blocks
98 * 4MiB = 411MiB
```

这个变化能解释一部分 RAW write 变慢，但不能单独解释从 3.56s 到 10.79s 的全部差异。多写 411MiB 只占总写出量约 1.5%，而耗时增加约 3 倍。

### RAW variable write 变慢复核

为了区分“代码路径本身变慢”和“运行口径导致热/冷状态不同”，又补跑了三个对照：

| 口径 | fixed raw | variable raw | 说明 |
| --- | ---: | ---: | --- |
| direct binary 连跑 fixed+variable | 10.56s | 3.37s | 同一进程，先跑 `bm_raw_fixed`，再跑 `bm_raw_variable` |
| direct binary 只跑 variable | - | 9.27s | 单进程，但没有前序 BM raw fixed 预热 |
| runner no-drop-cache，逐 case 新进程 | 10.82s | 9.49s | runner 逐 case 启动新进程，但不 drop page cache |

对应输出文件：

- `/tmp/bm-raw-write-direct-stdout.txt`
- `/tmp/bm-raw-write-direct-stderr.txt`
- `/tmp/bm-raw-write-direct-variable-only-stdout.txt`
- `/tmp/bm-raw-write-direct-variable-only-stderr.txt`
- `/tmp/bm-raw-write-runner-nodrop/stdout.txt`
- `/tmp/bm-raw-write-runner-nodrop/stderr.txt`

关键观察：

- `direct binary 连跑 fixed+variable` 时，`bm_raw_variable` 是 3.37s，接近 2026-06-09 的 3.56s。
- `direct binary 只跑 variable` 时，`bm_raw_variable` 是 9.27s，接近本次 runner 的 10.79s。
- `runner no-drop-cache` 下，`bm_raw_variable` 仍然是 9.49s，说明 `drop_caches` 不是主因。
- `bm_raw_fixed` 在 direct 连跑和 runner no-drop-cache 下都约 10.5-10.8s，说明当前冷进程下 RAW BM write 吞吐本身明显低于 2026-06-09 完整 binary 中的热态表现。

因此，`spillWriteBm(bm_raw_variable)` 从 3.56s 到 9-10s 的主要解释是：2026-06-09 的完整 binary 连跑让 BM 进程内全局 IO 路径处于热态，而 runner/variable-only 都是单 case 冷进程。BM 的 `DiskIoScheduler` 是进程内全局 static：

```cpp
static auto* scheduler = new DiskIoSchedulerImpl(DiskIoSchedulerConfig{});
```

完整 binary 连跑时，前序 BM spill/read/write case 会复用并预热这个 scheduler/io_uring/worker 路径；runner 每个 case 新进程，无法继承这个进程内状态。这个差异比多出的 98 个 heap blocks 更能解释 3 倍耗时变化。

## SpillRead Metrics

### BM fixed

| 压缩 | read time | physical read | decompress | BatchPin | append ptrs | pinned blocks |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| RAW | 19.15s | 32.22GB | 0 | 14.86s | 3.99s | 7681 |
| LZ4 | 18.68s | 27.76GB | 12.64s | 14.38s | 3.99s | 7681 |
| ZSTD | 56.82s | 23.29GB | 52.09s | 52.49s | 4.01s | 7681 |

fixed BM read 的 `bulk_append_ptrs_ms` 稳定在 4s 左右。RAW/LZ4 的 wall time 接近，ZSTD 明显受解压成本影响。

### BM variable

| 压缩 | read time | physical read | decompress | BatchPin | rebase strings | pinned blocks |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| RAW | 14.07s | 27.77GB | 0 | 13.19s | 0.79s | 6622 |
| LZ4 | 10.20s | 1.20GB | 9.25s | 9.32s | 0.79s | 6622 |
| ZSTD | 12.16s | 842.54MB | 11.21s | 11.28s | 0.80s | 6622 |

variable read 的 `rebaseStringViews()` 稳定在 0.8s 左右，不是主瓶颈。LZ4 仍然是 variable read 的较优折中，ZSTD 物理读更少但解压更贵。

### old spill read

| 数据集 | 压缩 | create reader | next batch | copy rows | list rows |
| --- | --- | ---: | ---: | ---: | ---: |
| fixed | RAW | 12.63s | 20.88s | 19.49s | 7.05s |
| fixed | LZ4 | 13.92s | 36.24s | 19.60s | 7.08s |
| fixed | ZSTD | 10.96s | 73.29s | 19.23s | 7.02s |
| variable | RAW | 13.35s | 9.61s | 7.18s | 0.17s |
| variable | LZ4 | 0.84s | 10.14s | 6.89s | 0.17s |
| variable | ZSTD | 0.66s | 12.10s | 6.86s | 0.17s |

old fixed read 的 `copy_rows_ms` 稳定在 19.2-19.6s，`list_rows_ms` 稳定在 7s 左右。old variable read 的主要成本在 `next_batch` 和 `copy_rows`。

## 与 2026-06-09 文档的关键差异

### 运行口径不同

2026-06-09 文档来自一次完整 benchmark binary 连跑：

```text
./_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark
```

本次来自 runner 逐 case 执行：

- 每个 case 单独启动进程。
- 每个 case 前 `sync + drop_caches`。
- 每个 case 之间 sleep。
- stdout 是 32 次独立 binary 输出拼接在一起。

因此，2026-06-09 结果和本次结果不是完全 apples-to-apples。尤其是 spill read/write 这类对 page cache、dirty writeback、IO 队列状态敏感的 benchmark，不能直接用数值变化断言代码回归。

### 明显变化点

| case | 2026-06-09 | 本次 cold runner | 变化 |
| --- | ---: | ---: | ---: |
| storeBm(bm_variable) | 4.84s | 10.44s | 变慢 2.16x |
| spillWriteBm(bm_raw_variable) | 3.56s | 10.79s | 变慢 3.03x |
| spillReadBm(bm_raw_variable) | 37.34s | 14.07s | 变快 2.65x |
| spillReadBm(bm_raw_fixed) | 1.31min | 19.15s | 变快 4.10x |

这些差异里既可能有代码变化，也明显混入了 runner 口径和系统 IO 状态变化。比如本次 BM spill read 的 IO backend submit 和 queue wait 形态与旧文档差别很大。

### variable BM block 数变化

2026-06-09 variable BM RAW write：

```text
bm_spill_write_count=6524
bm_spill_write_bytes=27363639296
```

本次 variable BM RAW write：

```text
bm_spill_write_count=6622
bm_spill_write_bytes=27774681088
```

多出的约 411MB 会直接影响 RAW write/read 的物理 IO。这个差异与 `SpillBlocks` 本身无关，也不是逻辑 row 格式或 1KB string 数据量变化导致的；它来自 chunk-local heap block 复用边界导致的尾部碎片。

## 结论

1. 本次 `log/bolt-bm-row-container-20260612-175724` 中主 row container benchmark 没漏跑，32 个 case 全部成功。
2. pipeline benchmark 没跑，因为它在独立 binary `bolt_exec_bm_row_container_pipeline_benchmark` 中。
3. 本次 cold runner 结果更适合观察“冷 cache、逐 case 隔离”下的表现；2026-06-09 文档更接近“完整 binary 连跑”的表现。
4. `storeBm(bm_variable)` 和 `spillWriteBm(bm_raw_variable)` 的变慢不能直接归因到 `StringView memcpy` 或 `BufferManager::SpillBlocks` 本身。metric 只能说明当前耗时落点，真正归因需要同 runner、同机器、同数据量的 commit A/B。
5. BM variable 当前 block 数从旧文档的 6524 增加到 6622，精确对应 chunk-local heap block 复用边界带来的 98 个额外 heap blocks。这会增加约 411MB RAW spill write/read 物理 IO，但它不是 `spillWriteBm(bm_raw_variable)` 变慢 3 倍的主因。
6. `spillWriteBm(bm_raw_variable)` 变慢 3 倍的主要原因是运行口径差异：完整 binary 连跑时 variable raw 继承前序 BM raw fixed 的进程内 IO 热态，单 case runner 或 variable-only direct run 则是冷进程，耗时稳定在 9-10s。

## 后续建议

1. 单独跑 pipeline benchmark：

```bash
sudo bash bolt/exec/bm/benchmarks/run_bm_row_container_benchmark.sh \
  --binary /data00/home/wangxinshuo.db/bolt/_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_pipeline_benchmark
```

2. 做同 runner A/B：

- 当前 HEAD 跑一次 cold runner。
- 2026-06-09 对应 commit 跑同一个 cold runner。
- 对比 `storeBm(bm_variable)`、`spillWriteBm(bm_raw_variable)`、`bm_spill_write_count`、`bm_spill_write_bytes`。

3. 继续定位 variable BM store 时，优先加 chunk/heap 碎片和 heap block 复用相关指标：

- row block 数
- heap block 数
- heap used bytes
- heap unused tail bytes
- 每个 chunk 的 heap block 数分布

这些指标比单纯看 `memcpy` 或 `SpillBlocks` 更能解释当前 variable 数据集的变化。
