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

## 原始数据
```
wangxinshuo.db@n37-127-061:/data00/home/wangxinshuo.db/bolt$ ./_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark 2>log.txt && cat log.txt 
============================================================================
[...]marks/BmRowContainerReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
readOld(old_fixed)                                           7.15s   139.83m
readBm(bm_fixed)                                154.08%      4.64s   215.45m
readOld(old_variable)                                        3.12s   320.27m
readBm(bm_variable)                             106.58%      2.93s   341.35m
----------------------------------------------------------------------------
============================================================================
[...]/BmRowContainerSpillReadBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillReadOld(old_raw_fixed)                                1.97min     8.45m
spillReadBm(bm_raw_fixed)                       150.75%    1.31min    12.75m
spillReadOld(old_lz4_fixed)                                1.81min     9.21m
spillReadBm(bm_lz4_fixed)                       128.53%    1.41min    11.84m
spillReadOld(old_zstd_fixed)                               1.70min     9.81m
spillReadBm(bm_zstd_fixed)                      173.42%     58.76s    17.02m
spillReadOld(old_raw_variable)                              54.78s    18.25m
spillReadBm(bm_raw_variable)                    146.69%     37.34s    26.78m
spillReadOld(old_lz4_variable)                              14.77s    67.72m
spillReadBm(bm_lz4_variable)                     340.7%      4.33s   230.72m
spillReadOld(old_zstd_variable)                             16.22s    61.65m
spillReadBm(bm_zstd_variable)                   275.11%      5.90s   169.61m
----------------------------------------------------------------------------
============================================================================
[...]BmRowContainerSpillWriteBenchmark.cpp     relative  time/iter   iters/s
============================================================================
spillWriteOld(old_raw_fixed)                                35.95s    27.82m
spillWriteBm(bm_raw_fixed)                      862.82%      4.17s   239.99m
spillWriteOld(old_lz4_fixed)                               1.68min     9.93m
spillWriteBm(bm_lz4_fixed)                       148.5%    1.13min    14.74m
spillWriteOld(old_zstd_fixed)                              3.63min     4.59m
spillWriteBm(bm_zstd_fixed)                     121.11%    3.00min     5.56m
spillWriteOld(old_raw_variable)                             13.92s    71.81m
spillWriteBm(bm_raw_variable)                    390.8%      3.56s   280.65m
spillWriteOld(old_lz4_variable)                             12.33s    81.10m
spillWriteBm(bm_lz4_variable)                   205.87%      5.99s   166.97m
spillWriteOld(old_zstd_variable)                            16.94s    59.04m
spillWriteBm(bm_zstd_variable)                  112.53%     15.05s    66.44m
----------------------------------------------------------------------------
============================================================================
[...]arks/BmRowContainerStoreBenchmark.cpp     relative  time/iter   iters/s
============================================================================
storeOld(old_fixed)                                         44.54s    22.45m
storeBm(bm_fixed)                                129.8%     34.32s    29.14m
storeOld(old_variable)                                       9.59s   104.27m
storeBm(bm_variable)                            198.27%      4.84s   206.73m
----------------------------------------------------------------------------
[bm-row-container-metrics] spillReadOld compression=raw dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 serialized_bytes=28185722880 batches=26883 create_reader_ms=10038.790 next_batch_ms=18642.340 copy_rows_ms=24593.229 list_rows_ms=4248.689
[bm-row-container-metrics] spillReadBm compression=raw dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 row_ids=0 windows=0 result=pointers begin_ms=0.002 try_load_all_ms=78459.992 window_load_ms=0.000 bulk_estimate_ms=0.030 bulk_reserve_ms=0.003 bulk_collect_blocks_ms=0.157 bulk_batch_pin_ms=73241.625 bulk_update_ptrs_ms=0.084 bulk_rebase_strings_ms=0.000 bulk_append_ptrs_ms=5217.832 bulk_append_row_ids_ms=0.000 bulk_estimated_bytes=32216449024 bulk_pinned_blocks=7681 bulk_pointer_rows=1342177280 bulk_row_id_rows=0 bulk_rebased_string_views=0 bm_batch_pins=1 bm_pin_reads=7681 bm_spill_read_count=7681 bm_spill_read_bytes=32216449024 bm_spill_physical_read_bytes=32216694816 bm_decompress_ms=0.000 io_accepted=7681 io_completed=7681 io_completed_bytes=32216694816 io_successful=7681 io_failed=0 io_rejected=0 io_submitted_high=7681 io_submitted_medium=0 io_submitted_low=0 io_completed_high=7681 io_completed_medium=0 io_completed_low=0 io_submit_batches=61 io_completion_batches=61 io_queue_wait_ms=22370088.564 io_avg_queue_wait_us=2912392.731 io_device_latency_ms=373133.896 io_avg_device_latency_us=48578.817 io_end_to_end_latency_ms=22743226.324 io_avg_end_to_end_latency_us=2960972.051 io_backend_submit_ms=5747.448 io_backend_reap_ms=0.251 io_worker_wait_ms=0.453 io_future_fulfill_ms=2.441
[bm-row-container-metrics] spillReadOld compression=lz4 dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 serialized_bytes=28185722880 batches=26883 create_reader_ms=4596.212 next_batch_ms=29025.582 copy_rows_ms=24735.374 list_rows_ms=4231.402
[bm-row-container-metrics] spillReadBm compression=lz4 dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 row_ids=0 windows=0 result=pointers begin_ms=0.002 try_load_all_ms=84440.725 window_load_ms=0.000 bulk_estimate_ms=0.031 bulk_reserve_ms=0.002 bulk_collect_blocks_ms=0.148 bulk_batch_pin_ms=79256.663 bulk_update_ptrs_ms=0.084 bulk_rebase_strings_ms=0.000 bulk_append_ptrs_ms=5183.504 bulk_append_row_ids_ms=0.000 bulk_estimated_bytes=32216449024 bulk_pinned_blocks=7681 bulk_pointer_rows=1342177280 bulk_row_id_rows=0 bulk_rebased_string_views=0 bm_batch_pins=1 bm_pin_reads=7681 bm_spill_read_count=7681 bm_spill_read_bytes=32216449024 bm_spill_physical_read_bytes=27766216315 bm_decompress_ms=11719.437 io_accepted=7681 io_completed=7681 io_completed_bytes=27766216315 io_successful=7681 io_failed=0 io_rejected=0 io_submitted_high=7681 io_submitted_medium=0 io_submitted_low=0 io_completed_high=7681 io_completed_medium=0 io_completed_low=0 io_submit_batches=61 io_completion_batches=61 io_queue_wait_ms=22306028.441 io_avg_queue_wait_us=2904052.655 io_device_latency_ms=416939.634 io_avg_device_latency_us=54281.947 io_end_to_end_latency_ms=22722971.931 io_avg_end_to_end_latency_us=2958335.104 io_backend_submit_ms=6062.435 io_backend_reap_ms=0.157 io_worker_wait_ms=0.436 io_future_fulfill_ms=2.458
[bm-row-container-metrics] spillReadOld compression=zstd dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 serialized_bytes=28185722880 batches=26883 create_reader_ms=4317.595 next_batch_ms=66013.446 copy_rows_ms=24474.802 list_rows_ms=4220.924
[bm-row-container-metrics] spillReadBm compression=zstd dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 row_ids=0 windows=0 result=pointers begin_ms=0.002 try_load_all_ms=58755.386 window_load_ms=0.000 bulk_estimate_ms=0.029 bulk_reserve_ms=0.003 bulk_collect_blocks_ms=0.139 bulk_batch_pin_ms=53438.675 bulk_update_ptrs_ms=0.089 bulk_rebase_strings_ms=0.000 bulk_append_ptrs_ms=5316.176 bulk_append_row_ids_ms=0.000 bulk_estimated_bytes=32216449024 bulk_pinned_blocks=7681 bulk_pointer_rows=1342177280 bulk_row_id_rows=0 bulk_rebased_string_views=0 bm_batch_pins=1 bm_pin_reads=7681 bm_spill_read_count=7681 bm_spill_read_bytes=32216449024 bm_spill_physical_read_bytes=23295136596 bm_decompress_ms=49895.495 io_accepted=7681 io_completed=7681 io_completed_bytes=23295136596 io_successful=7681 io_failed=0 io_rejected=0 io_submitted_high=7681 io_submitted_medium=0 io_submitted_low=0 io_completed_high=7681 io_completed_medium=0 io_completed_low=0 io_submit_batches=61 io_completion_batches=61 io_queue_wait_ms=16928907.854 io_avg_queue_wait_us=2203997.898 io_device_latency_ms=278267.206 io_avg_device_latency_us=36227.992 io_end_to_end_latency_ms=17207178.871 io_avg_end_to_end_latency_us=2240226.386 io_backend_submit_ms=4445.326 io_backend_reap_ms=0.149 io_worker_wait_ms=0.428 io_future_fulfill_ms=2.480
[bm-row-container-metrics] spillReadOld compression=raw dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 serialized_bytes=27280653749 batches=26025 create_reader_ms=4392.869 next_batch_ms=3501.384 copy_rows_ms=10751.509 list_rows_ms=120.590
[bm-row-container-metrics] spillReadBm compression=raw dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 row_ids=0 windows=0 result=pointers begin_ms=0.001 try_load_all_ms=37343.301 window_load_ms=0.000 bulk_estimate_ms=0.027 bulk_reserve_ms=0.002 bulk_collect_blocks_ms=0.066 bulk_batch_pin_ms=36686.594 bulk_update_ptrs_ms=0.432 bulk_rebase_strings_ms=609.378 bulk_append_ptrs_ms=46.518 bulk_append_row_ids_ms=0.000 bulk_estimated_bytes=27363639296 bulk_pinned_blocks=6524 bulk_pointer_rows=25712209 bulk_row_id_rows=0 bulk_rebased_string_views=25712209 bm_batch_pins=1 bm_pin_reads=6524 bm_spill_read_count=6524 bm_spill_read_bytes=27363639296 bm_spill_physical_read_bytes=27363848064 bm_decompress_ms=0.000 io_accepted=6524 io_completed=6524 io_completed_bytes=27363848064 io_successful=6524 io_failed=0 io_rejected=0 io_submitted_high=6524 io_submitted_medium=0 io_submitted_low=0 io_completed_high=6524 io_completed_medium=0 io_completed_low=0 io_submit_batches=52 io_completion_batches=52 io_queue_wait_ms=17206308.955 io_avg_queue_wait_us=2637386.412 io_device_latency_ms=322297.647 io_avg_device_latency_us=49401.847 io_end_to_end_latency_ms=17528609.845 io_avg_end_to_end_latency_us=2686788.756 io_backend_submit_ms=5154.090 io_backend_reap_ms=0.222 io_worker_wait_ms=0.200 io_future_fulfill_ms=2.146
[bm-row-container-metrics] spillReadOld compression=lz4 dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 serialized_bytes=27280653749 batches=26025 create_reader_ms=345.804 next_batch_ms=3408.502 copy_rows_ms=10522.341 list_rows_ms=119.313
[bm-row-container-metrics] spillReadBm compression=lz4 dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 row_ids=0 windows=0 result=pointers begin_ms=0.002 try_load_all_ms=4333.602 window_load_ms=0.000 bulk_estimate_ms=0.028 bulk_reserve_ms=0.002 bulk_collect_blocks_ms=0.078 bulk_batch_pin_ms=3667.592 bulk_update_ptrs_ms=0.366 bulk_rebase_strings_ms=618.298 bulk_append_ptrs_ms=46.951 bulk_append_row_ids_ms=0.000 bulk_estimated_bytes=27363639296 bulk_pinned_blocks=6524 bulk_pointer_rows=25712209 bulk_row_id_rows=0 bulk_rebased_string_views=25712209 bm_batch_pins=1 bm_pin_reads=6524 bm_spill_read_count=6524 bm_spill_read_bytes=27363639296 bm_spill_physical_read_bytes=1196187676 bm_decompress_ms=3199.945 io_accepted=6524 io_completed=6524 io_completed_bytes=1196187676 io_successful=6524 io_failed=0 io_rejected=0 io_submitted_high=6524 io_submitted_medium=0 io_submitted_low=0 io_completed_high=6524 io_completed_medium=0 io_completed_low=0 io_submit_batches=52 io_completion_batches=52 io_queue_wait_ms=1413216.026 io_avg_queue_wait_us=216618.030 io_device_latency_ms=16748.220 io_avg_device_latency_us=2567.170 io_end_to_end_latency_ms=1429967.569 io_avg_end_to_end_latency_us=219185.710 io_backend_submit_ms=261.808 io_backend_reap_ms=0.163 io_worker_wait_ms=0.250 io_future_fulfill_ms=2.140
[bm-row-container-metrics] spillReadOld compression=zstd dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 serialized_bytes=27280653749 batches=26025 create_reader_ms=279.349 next_batch_ms=4896.257 copy_rows_ms=10576.323 list_rows_ms=122.378
[bm-row-container-metrics] spillReadBm compression=zstd dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 row_ids=0 windows=0 result=pointers begin_ms=0.002 try_load_all_ms=5895.438 window_load_ms=0.000 bulk_estimate_ms=0.040 bulk_reserve_ms=0.003 bulk_collect_blocks_ms=0.072 bulk_batch_pin_ms=5226.815 bulk_update_ptrs_ms=0.400 bulk_rebase_strings_ms=621.181 bulk_append_ptrs_ms=46.623 bulk_append_row_ids_ms=0.000 bulk_estimated_bytes=27363639296 bulk_pinned_blocks=6524 bulk_pointer_rows=25712209 bulk_row_id_rows=0 bulk_rebased_string_views=25712209 bm_batch_pins=1 bm_pin_reads=6524 bm_spill_read_count=6524 bm_spill_read_bytes=27363639296 bm_spill_physical_read_bytes=844150284 bm_decompress_ms=4832.443 io_accepted=6524 io_completed=6524 io_completed_bytes=844150284 io_successful=6524 io_failed=0 io_rejected=0 io_submitted_high=6524 io_submitted_medium=0 io_submitted_low=0 io_completed_high=6524 io_completed_medium=0 io_completed_low=0 io_submit_batches=52 io_completion_batches=52 io_queue_wait_ms=1163500.882 io_avg_queue_wait_us=178341.643 io_device_latency_ms=13217.867 io_avg_device_latency_us=2026.037 io_end_to_end_latency_ms=1176721.974 io_avg_end_to_end_latency_us=180368.175 io_backend_submit_ms=205.188 io_backend_reap_ms=0.182 io_worker_wait_ms=0.284 io_future_fulfill_ms=2.071
[bm-row-container-metrics] spillWriteOld compression=raw dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 store_setup_ms=44711.653 spill_ms=35951.512 spill_bytes=28185937944 files=26883
[bm-row-container-metrics] spillWriteBm compression=raw dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 store_setup_ms=34374.598 flush_ms=4166.756 bm_spill_write_count=7681 bm_spill_write_bytes=32216449024 bm_spill_physical_write_bytes=32216694816 bm_compress_ms=0.000 bm_compressed_blocks=0
[bm-row-container-metrics] spillWriteOld compression=lz4 dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 store_setup_ms=44609.438 spill_ms=100715.506 spill_bytes=27257858112 files=26883
[bm-row-container-metrics] spillWriteBm compression=lz4 dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 store_setup_ms=34327.195 flush_ms=67821.113 bm_spill_write_count=7681 bm_spill_write_bytes=32216449024 bm_spill_physical_write_bytes=27764295018 bm_compress_ms=59662.186 bm_compressed_blocks=7681
[bm-row-container-metrics] spillWriteOld compression=zstd dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 store_setup_ms=44659.004 spill_ms=217908.701 spill_bytes=23622070679 files=26883
[bm-row-container-metrics] spillWriteBm compression=zstd dataset=fixed iterations=1 logical_bytes=26843545600 rows=1342177280 store_setup_ms=34402.994 flush_ms=179925.052 bm_spill_write_count=7681 bm_spill_write_bytes=32216449024 bm_spill_physical_write_bytes=23292101434 bm_compress_ms=178765.363 bm_compressed_blocks=7681
[bm-row-container-metrics] spillWriteOld compression=raw dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 store_setup_ms=9844.455 spill_ms=13924.699 spill_bytes=27280861949 files=26025
[bm-row-container-metrics] spillWriteBm compression=raw dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 store_setup_ms=5128.757 flush_ms=3563.126 bm_spill_write_count=6524 bm_spill_write_bytes=27363639296 bm_spill_physical_write_bytes=27363848064 bm_compress_ms=0.000 bm_compressed_blocks=0
[bm-row-container-metrics] spillWriteOld compression=lz4 dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 store_setup_ms=9793.454 spill_ms=12330.235 spill_bytes=1232145318 files=26025
[bm-row-container-metrics] spillWriteBm compression=lz4 dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 store_setup_ms=5072.203 flush_ms=5989.240 bm_spill_write_count=6524 bm_spill_write_bytes=27363639296 bm_spill_physical_write_bytes=1196013656 bm_compress_ms=5842.431 bm_compressed_blocks=6524
[bm-row-container-metrics] spillWriteOld compression=zstd dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 store_setup_ms=9984.876 spill_ms=16936.646 spill_bytes=854646829 files=26025
[bm-row-container-metrics] spillWriteBm compression=zstd dataset=variable iterations=1 logical_bytes=26843545600 rows=25712209 store_setup_ms=5401.663 flush_ms=15050.680 bm_spill_write_count=6524 bm_spill_write_bytes=27363639296 bm_spill_physical_write_bytes=844004971 bm_compress_ms=14876.023 bm_compressed_blocks=6524
```

## 建议

短期建议按以下顺序处理：

1. 优先优化 `bm_window_fixed` pipeline。这个路径当前耗时 1.52hr，是唯一不可接受的问题。
2. 单独复跑 resident fixed read，确认 9% 回退是否稳定。
3. 单独复跑 variable RAW spill read，确认是否只是 IO 波动。
4. 如果 fixed window 优化后仍慢，再给 `loadRows()` / `pinChunk()` 增加更细的 wall-time metrics。
5. 暂时不要因为 RAW variable 单次回退修改压缩策略；LZ4/ZSTD 路径没有表现出同类问题。
