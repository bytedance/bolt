# BmRowContainer Benchmark 性能分析

日期：2026-06-04

本文整理 `bolt/exec/bm/benchmarks/BmRowContainerBenchmark.cpp` 的最新一次 1GiB benchmark 结果，并结合 `log.txt` 中的细粒度 stats 分析 `RowContainer` 和 `BmRowContainer` 在写入、spill、读内存、读 spill 场景下的性能差异。

## 测试配置

运行命令：

```bash
_build/Release/bolt/exec/bm/benchmarks/bolt_bm_row_container_benchmark \
  --bm_row_container_print_stats=true \
  2>log.txt
```

当前 benchmark 的关键配置：

- 每个 dataset 的逻辑输入大小为 1GiB。
- RowContainer 使用 row-based spiller，`rowBasedSpillMode=COMPRESSION`。
- RowContainer spill 压缩为 `CompressionKind_ZSTD`。
- BmRowContainer 使用 BufferManager block spill，压缩为 `kZstdFrame`。
- `varchar_payload` 使用确定性伪随机 1024 字节字符串，不再使用单一字符 payload。
- 当前 ReadMemory / ReadSpill 读取的是第 0 列：
  - `fixed_int64`：`BIGINT`
  - `mixed_fixed`：`INTEGER`
  - `varchar_payload`：`BIGINT key`，不是 `VARCHAR payload`

## Benchmark 结果

| Dataset | 场景 | RowContainer | BmRowContainer | BM 相对表现 |
| --- | ---: | ---: | ---: | ---: |
| fixed_int64 | Write | 3.34s | 4.81s | 慢 1.44x |
| fixed_int64 | Spill | 5.59s | 3.11s | 快 1.80x |
| fixed_int64 | ReadMemory | 211.25ms | 1.77s | 慢 8.38x |
| fixed_int64 | ReadSpill | 2.29s | 13.23s | 慢 5.78x |
| mixed_fixed | Write | 3.02s | 3.85s | 慢 1.27x |
| mixed_fixed | Spill | 5.02s | 4.49s | 快 1.12x |
| mixed_fixed | ReadMemory | 124.17ms | 368.24ms | 慢 2.97x |
| mixed_fixed | ReadSpill | 1.81s | 5.94s | 慢 3.28x |
| varchar_payload | Write | 2.94s | 3.14s | 慢 1.07x |
| varchar_payload | Spill | 3.80s | 1.30s | 快 2.92x |
| varchar_payload | ReadMemory | 3.02ms | 8.49ms | 慢 2.81x |
| varchar_payload | ReadSpill | 1.23s | 105.47ms | 快 11.66x |

## Spill 写出分析

双方都启用 ZSTD 后，`BmRowContainerSpill` 在三个 dataset 上都优于或接近 RowContainer。

### fixed_int64

RowContainer stats：

```text
input_bytes=1,207,968,776
spilled_bytes=140,803,703
rows=134,217,728
total_us=5,586,112
fill_us=1,261,177
serialization_us=1,267,526
flush_us=2,944,687
write_us=70,180
```

BmRowContainer stats：

```text
spill_write_bytes=2,143,289,344
spill_physical_write_bytes=140,558,469
spill_write_count=511
compressed_blocks=511
compression_us=3,069,375
```

结论：

- BM 的物理写出大小和 RowContainer 非常接近，都是约 140MB。
- BM 总耗时约 3.11s，几乎全部是 ZSTD 压缩时间。
- RowContainer 总耗时约 5.59s，其中 row fill + serialization 约 2.53s，压缩 flush 约 2.94s。
- fixed_int64 下 BM 的优势来自省掉 row-based fill/serialization 的 CPU 成本。
- BM 需要压缩的逻辑字节更多，约 2.14GB；RowContainer row-based serialized input 约 1.21GB。这说明 BM block layout 比 RowContainer spill format 更膨胀，但仍然通过避免逐行序列化获得优势。

### mixed_fixed

RowContainer stats：

```text
input_bytes=1,124,880,970
spilled_bytes=281,965,150
rows=51,130,563
total_us=5,018,096
fill_us=584,792
serialization_us=476,125
flush_us=3,800,064
write_us=138,640
```

BmRowContainer stats：

```text
spill_write_bytes=1,224,736,768
spill_physical_write_bytes=272,369,411
spill_write_count=292
compressed_blocks=292
compression_us=4,476,864
```

结论：

- mixed_fixed 下 BM 仍略快，但优势较小，4.49s vs 5.02s。
- 两边物理写出大小接近，BM 约 272MB，RowContainer 约 282MB。
- BM 的压缩时间约 4.48s，基本等于总耗时。
- RowContainer 的 row fill/serialization 约 1.06s，flush 压缩约 3.80s。
- BM 节省了 row-based 逐行处理，但 ZSTD 压缩本身更慢，抵消了大部分收益。

### varchar_payload

RowContainer stats：

```text
input_bytes=984,894,351
spilled_bytes=813,153,472
rows=1,040,447
total_us=3,801,551
fill_us=6,314
serialization_us=96,987
flush_us=3,325,056
write_us=373,113
```

BmRowContainer stats：

```text
spill_write_bytes=985,661,440
spill_physical_write_bytes=796,763,220
spill_write_count=235
compressed_blocks=235
compression_us=1,274,699
```

结论：

- BM 在 varchar_payload spill 写出上优势明显，1.30s vs 3.80s。
- 两边逻辑写出大小接近，约 985MB；物理写出也接近，BM 约 797MB，RowContainer 约 813MB。
- RowContainer 的 ZSTD flush 花费 3.33s，BM 的 ZSTD 压缩只花 1.27s。
- 该场景行数少，RowContainer fill/serialization 不是主要问题；主要差异来自两条压缩路径和写出路径的效率。

## ReadMemory 分析

BM 在所有 ReadMemory 场景都慢于 RowContainer：

| Dataset | RowContainer | BmRowContainer | 差异 |
| --- | ---: | ---: | ---: |
| fixed_int64 | 211.25ms | 1.77s | 慢 8.38x |
| mixed_fixed | 124.17ms | 368.24ms | 慢 2.97x |
| varchar_payload | 3.02ms | 8.49ms | 慢 2.81x |

该场景没有 spill read，也没有解压。BM stats 中 `pin_count=0`，因为 blocks 仍在 `BmBlockState::pinnedHandle` 中。即使如此 BM 仍慢，说明基础读路径存在明显开销：

- `RowId` 需要转换成 block pointer + row offset。
- `extractColumn` 会构造 `std::vector<const char*> rowPtrs`。
- fixed-width 高行数场景会放大 per-row 指针解析和 rowPtrs 构造成本。

ReadMemory 的结果说明，BM 读路径问题不只来自 unspill，而是 `extractColumn` 基础设计还缺少 block-level fast path。

## ReadSpill 分析

### fixed_int64

RowContainer read spill：

```text
ReadSpill benchmark=2.29s
read_us=2,008,782
decompress_us=845,039
read_io_us=19,708
```

BmRowContainer read spill：

```text
ReadSpill benchmark=13.23s
pin_count=133,955,584
pin_in_memory=133,955,073
pin_read=511
spill_read_bytes=2,143,289,344
spill_physical_read_bytes=140,558,469
decompression_us=1,103,615
```

结论：

- BM 真正触发磁盘读的次数只有 511 次，和 spilled block 数一致。
- BM 解压耗时约 1.10s，不足以解释 13.23s 的总耗时。
- `pin_count` 接近行数，说明当前读路径对每行都进入 `pinRow` / `BufferManager::Pin` 路径。
- 主要瓶颈是行数级别的 `RowId -> row pointer` 转换、Pin 路径调用和 rowPtrs 构造。

### mixed_fixed

RowContainer read spill：

```text
ReadSpill benchmark=1.81s
read_us=1,701,964
decompress_us=1,206,676
read_io_us=46,127
```

BmRowContainer read spill：

```text
ReadSpill benchmark=5.94s
pin_count=51,030,504
pin_in_memory=51,030,212
pin_read=292
spill_read_bytes=1,224,736,768
spill_physical_read_bytes=272,369,416
decompression_us=1,570,438
```

结论：

- BM 只实际读回 292 个 blocks。
- 解压约 1.57s，但总耗时 5.94s。
- 和 fixed_int64 一样，剩余耗时主要来自每行 pin/lookup/extract 开销。

### varchar_payload

RowContainer read spill：

```text
ReadSpill benchmark=1.23s
read_us=1,225,063
decompress_us=1,053,468
read_io_us=112,838
```

BmRowContainer read spill：

```text
ReadSpill benchmark=105.47ms
pin_count=917,504
pin_read=7
spill_read_bytes=29,360,128
spill_physical_read_bytes=2,355,453
decompression_us=25,524
spilled_bytes remaining=956,301,312
```

结论：

- 当前 benchmark 只读第 0 列 `BIGINT key`，没有读取 `VARCHAR payload`。
- BM 只读回 7 个 row blocks，约 29MB logical bytes，没有读取大部分 heap payload blocks。
- 因此 BM 的 varchar_payload ReadSpill 很快，不能证明 BM 读取 VARCHAR payload 本身很快。
- 它只能说明：当只读取 key 列时，BM 可以避免读回大部分 payload blocks，这是 block-level layout 的一个潜在优势。

## 10GiB 实验结果

后续又执行了一组 10GiB 逻辑数据规模的实验：

```bash
_build/Release/bolt/exec/bm/benchmarks/bolt_bm_row_container_benchmark \
  --bm_row_container_data_gib=10 \
  2>log.txt
```

注意：当前 spill 写出仍是 buffered IO。row-based spill 在本地文件路径下最终走
`fwrite`，不是 direct IO；BM spill 也没有在 benchmark 计时外强制同步落盘。因此 spill
写出结果会受到 OS page cache、后台 writeback 和 benchmark 执行顺序影响。下面的结果适合比较
当前实现路径下的端到端耗时趋势，不应直接解释为稳定物理落盘吞吐。

### 10GiB Benchmark 结果

| Dataset | 场景 | RowContainer | BmRowContainer | BM 相对表现 |
| --- | ---: | ---: | ---: | ---: |
| fixed_int64 | Write | 36.55s | 47.62s | 慢 1.30x |
| fixed_int64 | Spill | 1.01min | 31.70s | 快 1.91x |
| fixed_int64 | ReadMemory | 2.13s | 18.33s | 慢 8.61x |
| fixed_int64 | ReadSpill | 22.89s | 2.25min | 慢 5.90x |
| mixed_fixed | Write | 32.04s | 40.07s | 慢 1.25x |
| mixed_fixed | Spill | 59.12s | 46.98s | 快 1.26x |
| mixed_fixed | ReadMemory | 1.31s | 3.60s | 慢 2.75x |
| mixed_fixed | ReadSpill | 18.17s | 59.09s | 慢 3.25x |
| varchar_payload | Write | 31.10s | 29.54s | 快 1.05x |
| varchar_payload | Spill | 1.30min | 11.90s | 快 6.55x |
| varchar_payload | ReadMemory | 33.48ms | 80.84ms | 慢 2.41x |
| varchar_payload | ReadSpill | 12.16s | 1.16s | 快 10.48x |

### 10GiB Stats 摘要

fixed_int64 spill：

```text
RowContainer:
input_bytes=12,079,687,696
spilled_bytes=1,407,583,699
rows=1,342,177,280
total_us=60,484,269
fill_us=12,285,029
serialization_us=12,529,696
flush_us=29,907,878
write_us=5,397,327

BmRowContainer:
spill_write_bytes=21,470,642,176
spill_physical_write_bytes=1,407,668,198
spill_write_count=5,119
compression_us=31,515,824
```

mixed_fixed spill：

```text
RowContainer:
input_bytes=11,248,809,692
spilled_bytes=2,915,848,555
rows=511,305,630
total_us=59,118,523
fill_us=3,081,090
serialization_us=4,689,857
flush_us=40,099,373
write_us=11,243,588

BmRowContainer:
spill_write_bytes=12,268,339,200
spill_physical_write_bytes=3,111,214,045
spill_write_count=2,925
compression_us=46,901,113
```

varchar_payload spill：

```text
RowContainer:
input_bytes=9,848,950,739
spilled_bytes=8,131,683,793
rows=10,404,475
total_us=77,958,254
fill_us=70,745
serialization_us=1,016,872
flush_us=34,764,844
write_us=42,102,472

BmRowContainer:
spill_write_bytes=9,919,528,960
spill_physical_write_bytes=7,992,256,878
spill_write_count=2,365
compression_us=11,800,078
```

fixed_int64 ReadSpill：

```text
RowContainer:
read_rows=1,342,177,280
read_us=19,954,175
decompress_us=8,312,544
read_io_us=200,014

BmRowContainer:
pin_count=1,341,915,136
pin_read=5,119
spill_read_bytes=21,470,642,176
spill_physical_read_bytes=1,407,668,198
decompression_us=11,128,623
```

mixed_fixed ReadSpill：

```text
RowContainer:
read_rows=511,305,630
read_us=17,049,636
decompress_us=12,173,108
read_io_us=398,701

BmRowContainer:
pin_count=511,178,850
pin_read=2,925
spill_read_bytes=12,268,339,200
spill_physical_read_bytes=3,111,211,705
decompression_us=15,133,363
```

varchar_payload ReadSpill：

```text
RowContainer:
read_rows=10,404,475
read_us=12,133,004
decompress_us=10,521,243
read_io_us=1,016,665

BmRowContainer:
pin_count=10,354,688
pin_read=79
spill_read_bytes=331,350,016
spill_physical_read_bytes=27,286,163
decompression_us=274,680
spilled_bytes remaining=9,588,178,944
```

### 10GiB 实验结论

10GiB 实验和 1GiB 实验的核心趋势一致：

1. `BmRowContainer` 的 spill 写出方向仍然成立。
   - fixed_int64：BM 快约 1.91x。
   - mixed_fixed：BM 快约 1.26x。
   - varchar_payload：BM 快约 6.55x，优势比 1GiB 实验更明显。

2. `BmRowContainer` 的 Write 在 fixed/mixed 仍慢于 RowContainer。
   - fixed_int64 慢约 1.30x。
   - mixed_fixed 慢约 1.25x。
   - varchar_payload 在这次实验中略快约 1.05x，可以先看作基本持平。

3. `BmRowContainer` 的 ReadMemory 仍然明显慢。
   - fixed_int64 慢约 8.61x。
   - mixed_fixed 慢约 2.75x。
   - varchar_payload 慢约 2.41x。

4. fixed/mixed 的 ReadSpill 短板没有变化。
   - fixed_int64 慢约 5.90x。
   - mixed_fixed 慢约 3.25x。
   - stats 显示 `pin_count` 仍是行数级别，而真正 `pin_read` 是 block 数级别。
   - 因此主要瓶颈仍然是每行 `RowId -> row pointer`、Pin 路径检查和 `rowPtrs` 构造，而不是磁盘读或解压本身。

5. varchar_payload 的 ReadSpill 仍然需要谨慎解释。
   - 当前读取的是第 0 列 `BIGINT key`，不是 `VARCHAR payload`。
   - BM 只读回 79 个 row blocks，仍有约 9.59GB spilled bytes 没有读回。
   - 这说明 BM 在只读 key 列时可以跳过 heap payload blocks，但不能证明读取 VARCHAR payload 本身很快。

## 2026-06-05 1GiB 最新实验

后续在 `BmPressureAwareBlockArena` 中引入 block-level pinned handle 缓存，并将
`BmRowContainer` 改为在 `MaybeReserve` 失败时由外围统一批量 spill 可回收 blocks。
修改后重新执行 1GiB benchmark：

```bash
_build/Release/bolt/exec/bm/benchmarks/bolt_bm_row_container_benchmark \
  --bm_row_container_data_gib=1 \
  2>log.txt
```

### 最新 1GiB Benchmark 结果

| Dataset | 场景 | RowContainer | BmRowContainer | BM 相对表现 |
| --- | ---: | ---: | ---: | ---: |
| fixed_int64 | Write | 3.38s | 5.93s | 慢 1.75x |
| fixed_int64 | Spill | 5.46s | 3.15s | 快 1.73x |
| fixed_int64 | ReadMemory | 209.62ms | 2.47s | 慢 11.78x |
| fixed_int64 | ReadSpill | 2.31s | 3.61s | 慢 1.56x |
| mixed_fixed | Write | 2.90s | 5.05s | 慢 1.74x |
| mixed_fixed | Spill | 4.98s | 4.49s | 快 1.11x |
| mixed_fixed | ReadMemory | 125.94ms | 573.46ms | 慢 4.55x |
| mixed_fixed | ReadSpill | 1.81s | 2.27s | 慢 1.25x |
| varchar_payload | Write | 2.95s | 3.21s | 慢 1.09x |
| varchar_payload | Spill | 3.79s | 1.30s | 快 2.92x |
| varchar_payload | ReadMemory | 3.08ms | 12.20ms | 慢 3.96x |
| varchar_payload | ReadSpill | 1.23s | 37.92ms | 快 32.44x |

### 最新 stats 摘要

Spill 写出：

```text
fixed_int64_BmRowContainerSpill:
spill_write_count=511
spill_write_bytes=2143289344
spill_physical_write_bytes=140558469
compression_us=3104675

mixed_fixed_BmRowContainerSpill:
spill_write_count=292
spill_write_bytes=1224736768
spill_physical_write_bytes=272369431
compression_us=4479040

varchar_payload_BmRowContainerSpill:
spill_write_count=235
spill_write_bytes=985661440
spill_physical_write_bytes=796762045
compression_us=1277817
```

ReadMemory：

```text
fixed_int64_BmRowContainerReadMemory:
allocated_blocks=512
pin_count=0
pinned_resident_bytes=2147483648

mixed_fixed_BmRowContainerReadMemory:
allocated_blocks=293
pin_count=0
pinned_resident_bytes=1228931072

varchar_payload_BmRowContainerReadMemory:
allocated_blocks=237
pin_count=0
pinned_resident_bytes=994050048
```

ReadSpill：

```text
fixed_int64_BmRowContainerReadSpill.after_read:
pin_count=511
pin_read=511
spill_read_bytes=2143289344
spill_physical_read_bytes=140558469
decompression_us=1135211

mixed_fixed_BmRowContainerReadSpill.after_read:
pin_count=292
pin_read=292
spill_read_bytes=1224736768
spill_physical_read_bytes=272369937
decompression_us=1600043

varchar_payload_BmRowContainerReadSpill.after_read:
pin_count=7
pin_read=7
spill_read_bytes=29360128
spill_physical_read_bytes=2354278
decompression_us=24524
spilled_bytes remaining=956301312
```

### 最新实验分析

1. ReadSpill 的主要瓶颈已经从“每行 pin”下降到“每 block pin”。
   - fixed_int64 的 `pin_count` 从历史行数级别下降到 511，等于 spilled row block 数。
   - mixed_fixed 的 `pin_count` 下降到 292。
   - 这说明 block state 持有 pinned handle 的设计有效，避免了每行反复进入
     `BufferManager::Pin` 路径。

2. fixed/mixed ReadSpill 大幅改善，但还没有完全追上 RowContainer。
   - fixed_int64：BM 从历史 13s 量级下降到 3.61s，但仍慢于 RowContainer 2.31s。
   - mixed_fixed：BM 下降到 2.27s，接近 RowContainer 1.81s。
   - 当前剩余差距更可能来自 `RowId -> block + offset`、per-row 边界检查、
     `rowPtrs` 构造和 extract 循环本身，而不再是 BM pin/read 反复调用。

3. ReadMemory 仍然是最明显短板。
   - stats 中 `pin_count=0`，说明所有 blocks 已经 pinned，没有 IO、Pin 或解压。
   - fixed_int64 仍慢 11.78x，mixed_fixed 慢 4.55x，varchar_payload 慢 3.96x。
   - 这说明内存读路径 CPU 开销仍然很高，尤其是高行数 fixed-width 场景。

4. Spill 写出方向继续成立。
   - fixed_int64 BM 快 1.73x。
   - mixed_fixed BM 只快 1.11x，主要因为 BM 的 ZSTD 压缩时间约 4.48s，几乎等于总耗时。
   - varchar_payload BM 快 2.92x。
   - 最新 stats 仍显示 BM spill 写出阶段主要被 `compression_us` 主导。

5. varchar_payload ReadSpill 继续需要谨慎解释。
   - 当前 benchmark 读取第 0 列 `BIGINT key`，不是 `VARCHAR payload`。
   - BM 只 read 7 个 row blocks，仍有约 956MB spilled bytes 没有读回。
   - 这个结果证明 BM 能跳过无关 heap payload blocks，但不能证明读取 `VARCHAR`
     payload 自身很快。

## 当前性能判断

### 成立的方向

1. BM block-level spill 写出方向成立。
   - 在双方都 ZSTD 的情况下，BM 在三个 dataset 上都快于或接近 RowContainer。
   - fixed_int64 和 varchar_payload 优势明显。

2. BM 可以避免读无关 payload blocks。
   - varchar_payload 读 key 列时，只读回少量 row blocks，没有读 heap payload blocks。

3. block-level pinned handle 缓存方向成立。
   - 最新 1GiB 实验中 fixed/mixed ReadSpill 的 `pin_count` 已经下降到 block 数级别。
   - ReadSpill 耗时从历史 fixed 13s、mixed 5-6s 量级分别下降到 3.61s 和 2.27s。

### 主要问题

1. BM ReadMemory 仍然明显慢。
   - 即使 blocks 已经 pinned，BM 仍慢于 RowContainer。
   - 最新 fixed_int64 慢 11.78x，mixed_fixed 慢 4.55x，varchar_payload 慢 3.96x。
   - 说明基础 `extractColumn` 路径仍需要优化。

2. BM fixed/mixed ReadSpill 仍有剩余差距。
   - `pin_count` 已经降到 block 数级别，问题不再是每行 Pin。
   - 剩余开销更可能来自每行 RowId 解析、row pointer 构造和 extract 循环。

3. varchar_payload 读路径 benchmark 还不完整。
   - 当前只测读取 key 列。
   - 尚未测读取 `VARCHAR` dependent payload 列。

## 建议下一步

优先级 1：优化 `BmRowContainer::extractColumn`

- 按连续 `rowBlockId` 分组。
- 每个 row block 只 resolve base pointer 一次。
- fixed-width 列直接从 `blockBase + rowOffset + columnOffset` 批量 extract。
- 避免每行调用 `pinRow`。
- 避免构造 `rowPtrs` 中间数组，至少在 fixed-width fast path 中移除。

预期收益：

- fixed_int64 / mixed_fixed 的 `ReadMemory` 明显下降。
- fixed_int64 / mixed_fixed 的 `ReadSpill` 继续下降。
- `pin_count` 已经降到 block 数级别，下一步目标是降低 per-row CPU 开销。

优先级 2：补充 VARCHAR payload 读取 benchmark

- 新增读取 dependent `VARCHAR` 列的 ReadMemory / ReadSpill。
- 对比读取 key 列和读取 payload 列的差异。
- 验证 BM heap blocks 的 read spill 性能。

优先级 3：继续拆分压缩策略

建议后续分别跑：

- ZSTD
- LZ4
- None

这样可以区分 block-level spill 自身优势和压缩算法成本。当前 ZSTD 下，写出阶段几乎都被压缩时间主导。

## 结论

在双方都启用 ZSTD 的情况下，`BmRowContainer` 的 spill 写出性能已经体现出价值；它通过 block-level spill 避免 RowContainer row-based fill/serialization，在 fixed_int64 和 varchar_payload 上优势明显。

引入 block-level pinned handle 缓存后，fixed/mixed 的 ReadSpill 已经明显改善，`pin_count`
从行数级别下降到 block 数级别。这说明之前最大的 ReadSpill 问题已经被定位并缓解。

当前最大短板转移到 ReadMemory 以及 ReadSpill 剩余的 per-row CPU 开销。要让
`BmRowContainer` 真正适合 StreamingWindowBuild 这类场景，下一步应优先实现
block-level batch read fast path：按 block 解析 base pointer，直接批量读取 fixed-width
列，减少每行 `RowId -> pointer`、边界检查和 `rowPtrs` 构造成本。
