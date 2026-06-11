# BM RowContainer Benchmark 对比分析

本文对比两次 25 GiB `bolt_exec_bm_row_container_benchmark` 结果：

- 基准版本：`2026-06-09-bm-row-container-benchmark-analysis.md` 中记录的结果。
- 最新版本：用户在 2026-06-11 运行并贴出的结果。

目标是判断最近改动后是否出现性能回退，并指出需要继续调查的路径。

## 结论

整体没有看到系统性回退。

- Store 路径明显提升，fixed/variable 的 BM 写入都比上一版快约 14%。
- SpillWrite 基本稳定，主要差异仍由压缩 CPU 成本决定。
- SpillRead 大多数压缩路径提升，尤其 fixed RAW 提升明显。
- 明确需要关注的回退有两个：
  - resident fixed read 从 4.64s 变为 5.05s，回退约 9%；
  - variable RAW spill read 从 37.34s 变为 50.46s，回退约 35%。
- 新增 pipeline 结果中，`bm_window_fixed` 非常慢，当前是最需要后续优化的路径。

最近的 `releaseAfterRead` 默认值修改和 `RowWriteContext` 拆头文件没有表现出系统性性能退化。

## 总体对比

### Store / Resident Read

| 场景 | 上一版 old | 上一版 BM | 最新 old | 最新 BM | BM 变化 | 判断 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Store fixed | 44.54s | 34.32s | 41.77s | 29.51s | +14.0% | BM 提升明显。 |
| Store variable | 9.59s | 4.84s | 6.24s | 4.18s | +13.6% | BM 提升，但 old 本次提升更大。 |
| Read fixed | 7.15s | 4.64s | 7.29s | 5.05s | -8.8% | 轻微回退，需要复跑确认。 |
| Read variable | 3.12s | 2.93s | 3.01s | 2.87s | +2.0% | 基本稳定。 |

Store fixed/variable 的 BM 绝对耗时都下降，说明写入路径没有回退。`storeBm(variable)` 相对 old 的优势从 198% 下降到 149%，主要原因是 old 本次从 9.59s 降到 6.24s，不能解读为 BM 回退。

resident fixed read 的回退比较独立：该路径不涉及 spill、pin、压缩或 RowId 解析，主要是 pointer scan + typed extract。建议先复跑确认是否稳定；如果稳定，再看 `extractColumnResident()` 的 fixed typed loop、layout 访问和编译 inline 情况。

### SpillRead

| 数据集 | 压缩 | 上一版 old | 上一版 BM | 最新 old | 最新 BM | BM 变化 | 判断 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| fixed | RAW | 1.97min | 1.31min | 55.03s | 39.40s | +49.8% | 大幅提升，可能包含 IO 状态差异。 |
| fixed | LZ4 | 1.81min | 1.41min | 1.72min | 1.33min | +5.3% | 小幅提升。 |
| fixed | ZSTD | 1.70min | 58.76s | 1.59min | 54.67s | +7.0% | 小幅提升。 |
| variable | RAW | 54.78s | 37.34s | 55.46s | 50.46s | -35.1% | 明显回退，需要复跑确认。 |
| variable | LZ4 | 14.77s | 4.33s | 11.29s | 3.63s | +16.2% | 提升。 |
| variable | ZSTD | 16.22s | 5.90s | 12.45s | 5.17s | +12.4% | 提升。 |

fixed spill read 三种压缩都变快，其中 RAW 从 78.46s 降到 39.40s，提升过大，更像本次 IO 状态更好或系统负载不同，而不一定是代码优化。

variable RAW 是唯一明显反向变化。它的物理读量只从约 27.36GB 增加到约 27.77GB，增幅约 1.5%，不足以解释 wall time 35% 回退。压缩路径 LZ4/ZSTD 都变快，因此不像是通用 read 逻辑退化，更可能是 RAW 大 IO 场景的 IO 波动或 BatchPin 等待波动。

### SpillWrite

| 数据集 | 压缩 | 上一版 old | 上一版 BM | 最新 old | 最新 BM | BM 变化 | 判断 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| fixed | RAW | 35.95s | 4.17s | 35.76s | 4.15s | +0.5% | 持平。 |
| fixed | LZ4 | 1.68min | 1.13min | 1.49min | 1.16min | -2.6% | 小幅波动。 |
| fixed | ZSTD | 3.63min | 3.00min | 3.57min | 3.12min | -4.0% | 小幅回退，仍由压缩 CPU 主导。 |
| variable | RAW | 13.92s | 3.56s | 13.97s | 3.56s | 0.0% | 持平。 |
| variable | LZ4 | 12.33s | 5.99s | 12.35s | 6.06s | -1.2% | 持平。 |
| variable | ZSTD | 16.94s | 15.05s | 16.61s | 14.87s | +1.2% | 持平。 |

SpillWrite 没有明显回退。fixed LZ4/ZSTD 的小幅变化主要落在压缩耗时上：

- fixed LZ4：BM `flush_ms` 约 67.82s -> 69.42s；
- fixed ZSTD：BM `flush_ms` 约 179.93s -> 187.34s；
- variable LZ4/ZSTD 基本稳定。

当前结论仍然不变：fixed write 中 RAW 最快，压缩路径主要被 CPU 成本主导；variable 写入中 LZ4 是比较均衡的选择。

## Pipeline 结果

上一版文档没有 pipeline 历史数据，因此这里只分析最新一次结果的结构，不判断历史回退。

| 场景 | old | BM loaded | BM window | 判断 |
| --- | ---: | ---: | ---: | --- |
| fixed | 6.41min | 4.62min | 1.52hr | loaded 明显快于 old；window fixed 异常慢。 |
| variable | 47.15s | 26.85s | 31.21s | loaded 最快；window 仍快于 old，但比 loaded 慢约 16%。 |

### fixed pipeline

old fixed：

| 阶段 | 耗时 |
| --- | ---: |
| store | 45.35s |
| spill write | 213.33s |
| spill read | 117.70s |
| extract | 7.11s |
| total | 383.50s |

BM loaded fixed：

| 阶段 | 耗时 |
| --- | ---: |
| store | 30.02s |
| spill write | 184.32s |
| spill read | 58.05s |
| extract | 4.96s |
| total | 277.35s |

BM loaded fixed 比 old 快约 38%。收益主要来自 store、spill read、extract 都更快，spill write 也略快。

BM window fixed：

| 指标 | 数值 |
| --- | ---: |
| total | 5484.23s |
| spill_read_ms | 5264.93s |
| windows | 20480 |
| bm_batch_pins | 28160 |
| bm_pin_reads | 7681 |
| bm_decompress_ms | 52.78s |

`bm_window_fixed` 的主要问题不是物理读或解压重复。`bm_pin_reads=7681` 与 loaded 路径一致，`bm_decompress_ms` 也只有约 52.8s。真正被放大的是 window 数量和 `BatchPin` 调度次数：`windows=20480`、`bm_batch_pins=28160`，导致 `spill_read_ms` 高达 5264.93s。

这个路径需要单独优化，否则 fixed 类型在大 working set + window read 模式下不可用。

### variable pipeline

old variable：

| 阶段 | 耗时 |
| --- | ---: |
| store | 9.47s |
| spill write | 16.60s |
| spill read | 17.36s |
| extract | 3.03s |
| total | 46.47s |

BM loaded variable：

| 阶段 | 耗时 |
| --- | ---: |
| store | 4.15s |
| spill write | 14.81s |
| spill read | 5.07s |
| extract | 2.82s |
| total | 26.85s |

BM window variable：

| 阶段 | 耗时 |
| --- | ---: |
| store | 4.13s |
| spill write | 14.79s |
| spill read | 9.58s |
| extract | 2.67s |
| total | 31.17s |

variable window 路径比 loaded 慢约 16%，但仍明显快于 old。它的 `windows=393`，远低于 fixed 的 20480，因此没有出现 fixed window 那种灾难性放大。

## 可能原因与优先级

### 1. fixed window read pipeline 是最高优先级

现象：

- `pipelineBm(bm_window_fixed)` 总耗时 1.52hr。
- `spill_read_ms=5264933ms`，占绝对主导。
- `bm_pin_reads=7681` 与 loaded 路径一致，说明没有重复读每个 block。
- `bm_batch_pins=28160`、`windows=20480` 明显过多。

初步判断：

窗口粒度太细，fixed 数据行数极大，导致大量窗口调度和 pin session 开销。即使物理读没有重复，控制面开销也被放大到不可接受。

后续方向：

- 对 fixed window read 增加窗口大小控制或自适应 batch rows。
- 在 `loadRows()` 中按 chunk 合并更大的连续 RowId 范围，减少 `BatchPin` 次数。
- 对已 pin chunk 做短期缓存，避免相邻窗口反复构造 pin 请求。
- benchmark 中增加 `window_rows`、`chunks_per_window`、`rows_per_chunk` 等指标。

### 2. resident fixed read 小回退需要复跑确认

现象：

- `readBm(fixed)` 从 4.64s 变为 5.05s，回退约 9%。
- 该路径不涉及 BM IO、spill、压缩或 RowId。

初步判断：

这可能是运行噪声，也可能是 extract loop 的编译布局、inline 或代码排列导致的小波动。由于变化不大，建议先复跑确认。

后续方向：

- 单独跑 `readBm(bm_fixed)` 多次，看方差。
- 如果稳定回退，再用 perf 看 `extractColumnTyped()`、`layout_.column()`、typed value copy 的热点。

### 3. variable RAW spill read 回退更像 IO 波动

现象：

- `spillReadBm(bm_raw_variable)` 从 37.34s 变为 50.46s。
- 物理读量只增加约 1.5%。
- LZ4/ZSTD variable read 都提升。

初步判断：

这不像通用读取逻辑退化，更像 RAW 大 IO 场景对设备状态、系统负载、IO queue wait 更敏感。

后续方向：

- 单独复跑 RAW variable spill read。
- 对比 `io_queue_wait_ms`、`io_device_latency_ms` 的波动。
- 如果稳定回退，再拆 `BatchPin` wall time 的 submit/wait/install 阶段。

## 仍然成立的结论

压缩算法选择结论没有变化：

| 场景 | 观察 |
| --- | --- |
| fixed write | RAW 最快；LZ4/ZSTD 主要被压缩 CPU 成本主导。 |
| fixed read | ZSTD 仍最快，少读物理数据抵消了解压成本。 |
| variable write | LZ4 仍是较均衡选择；RAW 写最快但物理写出太大；ZSTD 更小但 CPU 成本更高。 |
| variable read | LZ4 仍最好；ZSTD 更小但解压成本抵消收益。 |

BM RowContainer 的结构性优势也仍然成立：

- resident Store/Read 仍整体快于 old；
- SpillRead 避免 old read back 后重建 RowContainer；
- BM block 数明显少于 old files/batches；
- variable 场景下 BM 优势仍然明显。

## 建议

短期建议按以下顺序处理：

1. 优先优化 `bm_window_fixed` pipeline。这个路径当前耗时 1.52hr，是唯一不可接受的问题。
2. 单独复跑 resident fixed read，确认 9% 回退是否稳定。
3. 单独复跑 variable RAW spill read，确认是否只是 IO 波动。
4. 如果 fixed window 优化后仍慢，再给 `loadRows()` / `pinChunk()` 增加更细的 wall-time metrics。
5. 暂时不要因为 RAW variable 单次回退修改压缩策略；LZ4/ZSTD 路径没有表现出同类问题。
