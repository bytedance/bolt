# BM RowContainer Benchmark 分析

本文基于最新一次 `bolt_exec_bm_row_container_benchmark` 结果，分析 old `RowContainer` 与 `BmRowContainer` 在 Store、Read、SpillWrite、SpillRead 四类场景下的表现，并重点比较 RAW、LZ4、ZSTD 三种 spill 压缩配置的影响。

## 基准口径

本次运行的输入逻辑数据量为 25 GiB：

| 数据集 | 行数 | 逻辑形态 |
| --- | ---: | --- |
| fixed | 1,342,177,280 | `BIGINT + INTEGER + DOUBLE` |
| variable | 25,712,209 | fixed 三列 + `VARCHAR(1024)` |

计时范围：

| 场景 | 计时内容 |
| --- | --- |
| Store | 输入 `RowVector` 在计时外构造；计时区只包含写入 RowContainer。 |
| Read | RowContainer 和 row pointer 在计时外构造；计时区只包含 resident extract。 |
| SpillWrite | Folly benchmark 计时区只包含 spill/flush；stderr 的 `store_setup_ms` 是额外 metric，包含输入构造和写入 RowContainer。 |
| SpillRead | 源 RowContainer 和 spill 文件在计时外构造；计时区包含从 spill 读回并恢复可访问结构。 |

压缩配置：

| 名称 | old RowContainer | BM RowContainer |
| --- | --- | --- |
| RAW | `RowBasedSpillMode::RAW` | `CompressionKind::kNone` |
| LZ4 | `RowBasedSpillMode::COMPRESSION + CompressionKind_LZ4` | `CompressionKind::kLz4Block` |
| ZSTD | `RowBasedSpillMode::COMPRESSION + CompressionKind_ZSTD` | `CompressionKind::kZstdFrame` |

注意：

- `io_queue_wait_ms`、`io_device_latency_ms`、`io_end_to_end_latency_ms` 是所有 IO request 的累计时间，不是 wall time，不能直接与 `time/iter` 相加。
- old row-based spill benchmark 不拆分 spill run。因为 old `Spiller::SpillStatus::rowsWritten` 是 `int32_t`，benchmark 会在 old spill read/write 入口按 `logical_bytes / logical_row_bytes` 估算行数；如果超过 `INT32_MAX`，会在构造 RowContainer 之前直接退出。本次 fixed 25 GiB 未超过该限制。
- stderr metrics 只打印每个 benchmark/dataset/compression 第一次命中的调用；它适合看阶段拆分和量级，stdout 的 `time/iter` 是 Folly 最终统计口径。

## 总体结果

Resident Store/Read 不受压缩配置影响：

| 场景 | 数据集 | old time | BM time | BM 相对速度 | 结论 |
| --- | --- | ---: | ---: | ---: | --- |
| Store | fixed | 44.54s | 34.32s | 129.80% | BM batch typed store 优势明显。 |
| Store | variable | 9.59s | 4.84s | 198.27% | BM variable batch 写入优势非常明显。 |
| Read | fixed | 7.15s | 4.64s | 154.08% | BM resident fixed extract 明显更快。 |
| Read | variable | 3.12s | 2.93s | 106.58% | 两边接近，主要成本在 string view/result vector 处理。 |

SpillWrite 受压缩算法影响很大：

| 数据集 | 压缩 | old time | BM time | BM 相对速度 | 结论 |
| --- | --- | ---: | ---: | ---: | --- |
| fixed | RAW | 35.95s | 4.17s | 862.82% | BM RAW 写出极快，几乎只剩 block flush。 |
| fixed | LZ4 | 1.68min | 1.13min | 148.50% | LZ4 是 fixed write 的折中选择。 |
| fixed | ZSTD | 3.63min | 3.00min | 121.11% | ZSTD 压缩成本很高，只小幅领先 old。 |
| variable | RAW | 13.92s | 3.56s | 390.80% | BM RAW 快，但物理写出接近 27GiB。 |
| variable | LZ4 | 12.33s | 5.99s | 205.87% | LZ4 大幅压缩且 CPU 成本可控。 |
| variable | ZSTD | 16.94s | 15.05s | 112.53% | ZSTD 压缩更小，但 CPU 成本明显。 |

SpillRead 展示了 IO 与解压之间的权衡：

| 数据集 | 压缩 | old time | BM time | BM 相对速度 | 结论 |
| --- | --- | ---: | ---: | ---: | --- |
| fixed | RAW | 1.97min | 1.31min | 150.75% | RAW 无解压，但物理读最大。 |
| fixed | LZ4 | 1.81min | 1.41min | 128.53% | LZ4 少读一些数据，但没有赢过 RAW/ZSTD。 |
| fixed | ZSTD | 1.70min | 58.76s | 173.42% | ZSTD fixed read 最快，少读数据抵消了解压成本。 |
| variable | RAW | 54.78s | 37.34s | 146.69% | RAW 物理读太大，读回很慢。 |
| variable | LZ4 | 14.77s | 4.33s | 340.70% | LZ4 variable read 最佳。 |
| variable | ZSTD | 16.22s | 5.90s | 275.11% | ZSTD 更小但解压更贵，慢于 LZ4。 |

## Store

### fixed

| 实现 | time |
| --- | ---: |
| old | 44.54s |
| BM | 34.32s |

Store benchmark 的输入 batch 已经在计时外构造，因此这里主要测 row container 写入本身。fixed 数据集有 13.42 亿行，每行 3 个 fixed-width 列。

BM 比 old 快约 30%。fixed store 的 typed batch 路径稳定有效。old 仍然是逐行 `newRow()`、逐列 `store(decoded[column], row, target, column)`；BM 通过 `appendBatch(batch)` 减少了 per-row/per-column 调用和分支成本。

### variable

| 实现 | time |
| --- | ---: |
| old | 9.59s |
| BM | 4.84s |

variable 行数为 2571 万，每行有 1KB string。输入字符串在计时外生成，因此计时区主要是 string 写入 RowContainer 的成本。

BM 比 old 快约 2 倍，主要来自批量写入路径减少了 old `newRow() + store()` 的逐行调用开销。这个场景下 BM 的剩余瓶颈主要是把 string payload 写入 heap block 的内存 copy。

## Read

### fixed

| 实现 | time |
| --- | ---: |
| old | 7.15s |
| BM | 4.64s |

Read benchmark 测 resident extract：数据完全在内存中，输入是已保存的 row pointer，计时区对每个 batch、每个 column 调用 extract。

BM 比 old 快约 54%，说明 resident fixed extract 的 typed/non-null 快路径有效。这个场景没有 spill、pin、rebase 或 RowId 解析成本，瓶颈主要是顺序扫描 rows 并写出 result vector。

### variable

| 实现 | time |
| --- | ---: |
| old | 3.12s |
| BM | 2.93s |

variable read 两边接近，BM 快约 7%。该场景主要处理 fixed columns 和 string view/result vector 写入，不复制 25 GiB string payload。继续优化应先增加列级 metrics，确认 string column 与 fixed columns 的耗时比例。

## SpillWrite

### fixed

| 压缩 | old time | old bytes | BM time | BM physical bytes | BM compress |
| --- | ---: | ---: | ---: | ---: | ---: |
| RAW | 35.95s | 28.19GB | 4.17s | 32.22GB | 0 |
| LZ4 | 100.72s | 27.26GB | 67.82s | 27.76GB | 59.66s |
| ZSTD | 217.91s | 23.62GB | 179.93s | 23.29GB | 178.77s |

fixed write 的核心结论是：压缩 CPU 成本主导。

BM RAW 写出只有 4.17s，说明 BM block flush 本身很轻；一旦打开压缩，耗时主要来自压缩。LZ4 比 RAW 多花约 64s，只减少约 4.46GB 物理写出；ZSTD 再比 LZ4 少写约 4.47GB，但额外多花约 119s 压缩时间。

如果 fixed 场景偏 write-only 或 spill 后很少读，RAW 或 LZ4 更值得考虑；ZSTD 只有在读回频繁或 IO 成本特别高时才可能划算。

### variable

| 压缩 | old time | old bytes | BM time | BM physical bytes | BM compress |
| --- | ---: | ---: | ---: | ---: | ---: |
| RAW | 13.92s | 27.28GB | 3.56s | 27.36GB | 0 |
| LZ4 | 12.33s | 1.23GB | 5.99s | 1.20GB | 5.84s |
| ZSTD | 16.94s | 854.65MB | 15.05s | 844.00MB | 14.88s |

variable 数据虽然 string 值随机，但仍有明显可压缩性。BM LZ4 把 27.36GB raw payload 压到 1.20GB，只花 5.84s 压缩；ZSTD 进一步压到 844MB，但额外多花约 9s。

从写入角度看，BM LZ4 是更均衡的选择。RAW 写最快但物理写出太大；ZSTD 最省空间但 CPU 成本明显。

## SpillRead

### fixed

old fixed read：

| 压缩 | time | nextBatch | copyRows | listRows | serialized bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| RAW | 1.97min | 18.64s | 24.59s | 4.25s | 28.19GB |
| LZ4 | 1.81min | 29.03s | 24.74s | 4.23s | 28.19GB |
| ZSTD | 1.70min | 66.01s | 24.47s | 4.22s | 28.19GB |

old 的 `copy_rows_ms` 基本稳定在 24.5s 左右，这是读回后重建 RowContainer 的固定成本。压缩算法主要体现在 `nextBatch()`，RAW 最低，ZSTD 最高。

BM fixed read：

| 压缩 | time | physical read | decompress | BatchPin | append ptrs |
| --- | ---: | ---: | ---: | ---: | ---: |
| RAW | 78.46s | 32.22GB | 0 | 73.24s | 5.22s |
| LZ4 | 84.44s | 27.77GB | 11.72s | 79.26s | 5.18s |
| ZSTD | 58.76s | 23.30GB | 49.90s | 53.44s | 5.32s |

fixed read 里 ZSTD 最快。虽然 ZSTD 解压接近 50s，但它显著减少物理读和 IO 排队，最终 wall time 最低。LZ4 少读约 4.45GB，但解压和 IO 等待叠加后没有赢 RAW。

全量 pointer vector 构造稳定在 5.2s 左右。fixed 有 13.42 亿行，全量输出 `vector<char*>` 要写约 10GB 指针数组，这是合理成本。

### variable

old variable read：

| 压缩 | time | nextBatch | copyRows | listRows | serialized bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| RAW | 54.78s | 3.50s | 10.75s | 0.12s | 27.28GB |
| LZ4 | 14.77s | 3.41s | 10.52s | 0.12s | 27.28GB |
| ZSTD | 16.22s | 4.90s | 10.58s | 0.12s | 27.28GB |

old variable 的主要固定成本是 `copy_rows_ms`，约 10.5s。RAW 慢很多，主要不是 `nextBatch` 或 `copyRows` 单项解释完的，更多来自读回 raw 大文件的整体 IO 和 reader 创建成本。

BM variable read：

| 压缩 | time | physical read | decompress | rebase strings | BatchPin |
| --- | ---: | ---: | ---: | ---: | ---: |
| RAW | 37.34s | 27.36GB | 0 | 0.61s | 36.69s |
| LZ4 | 4.33s | 1.20GB | 3.20s | 0.62s | 3.67s |
| ZSTD | 5.90s | 844.15MB | 4.83s | 0.62s | 5.23s |

variable read 里 LZ4 最好。LZ4 已经把物理读降到 1.20GB，解压成本也较低；ZSTD 虽然继续减少到 844MB，但多出来的解压时间抵消了 IO 收益。RAW 无解压，但 27.36GB 物理读太大，显著慢于压缩路径。

BM variable 的 `rebaseStringViews()` 稳定在 0.61-0.62s，说明它不是当前主瓶颈。主要决策点是物理读量和解压 CPU 的平衡。

## 文件数与块数

old 的 files/batches 明显多于 BM blocks：

| 数据集 | old files/batches | BM blocks |
| --- | ---: | ---: |
| fixed | 26883 | 7681 |
| variable | 26025 | 6524 |

这会影响 old read 的 `create_reader_ms`、`next_batch_ms`，也会影响 old write 的 flush/write 次数。后续如果要做更严格的 apples-to-apples 对比，可以单独调 old `writeBufferSize` 或 file/batch 参数，看 old 的 files 数下降后 read/write 是否改善。

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


## 结论

当前 BM RowContainer 的结构性优势依然成立：

- resident Store/Read 快于 old；
- SpillRead 避免反序列化后重建 RowContainer；
- BM block 数少于 old files/batches；
- variable 场景下 BM 尤其明显。

压缩算法选择对 spill 性能影响很大：

| 场景 | 推荐观察 |
| --- | --- |
| fixed write | RAW 最快，LZ4 居中，ZSTD CPU 成本很高。 |
| fixed read | ZSTD 最快，少读物理数据抵消了解压成本。 |
| variable write | LZ4 是较好的折中；RAW 快但写出太大，ZSTD 更小但太贵。 |
| variable read | LZ4 最好，ZSTD 更小但解压成本抵消收益。 |

如果只能选一个通用默认算法，LZ4 比 ZSTD 更均衡，尤其 variable 场景优势明显。但 fixed read 上 ZSTD 仍然有优势，因此最终默认策略可能需要考虑算子读写比例：

- spill 后大概率读回：fixed 可以考虑 ZSTD，variable 更偏 LZ4；
- spill 后读回概率低或 write pressure 更重要：RAW/LZ4 更合适；
- 空间或 IO 带宽极端紧张：ZSTD 才更有吸引力。

短期后续工作：

1. 给 `BufferManager::BatchPin` 增加更细的 wall-time metrics，拆出 submit、wait、install、handle 构造。
2. 对 fixed 数据继续评估 block size 和 compression level，尤其是 ZSTD read 快但 write 太慢的问题。
3. 如果继续分析 old SpillWrite，补 old row-based spiller 的 serialization、flush、write 细分 metrics。
4. 对 resident read/store 只有在需要进一步压榨 CPU 性能时，再补列级 metrics 后优化 typed copy/extract 循环。
