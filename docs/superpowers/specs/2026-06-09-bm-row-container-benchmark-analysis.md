# BM RowContainer Benchmark 分析

本文基于 2026-06-09 的 `bolt_exec_bm_row_container_benchmark` 结果，分析 old `RowContainer` 与 `BmRowContainer` 在 Store、Read、SpillWrite、SpillRead 四类场景下的主要瓶颈。

## 基准口径

本次输入逻辑数据量为 10 GiB：

| 数据集 | 行数 | 逻辑形态 |
| --- | ---: | --- |
| fixed | 536,870,912 | `BIGINT + INTEGER + DOUBLE` |
| variable | 10,284,884 | fixed 三列 + `VARCHAR(1024)` |

四类 benchmark 的计时范围不同：

| 场景 | 计时内容 |
| --- | --- |
| Store | 输入 `RowVector` 在计时外构造；计时区只包含写入 RowContainer。 |
| Read | RowContainer 和 row pointer 在计时外构造；计时区只包含 resident extract。 |
| SpillWrite | Folly benchmark 计时区只包含 spill/flush；stderr 的 `store_setup_ms` 是额外 metric，包含输入构造和写入 RowContainer。 |
| SpillRead | 源 RowContainer 和 spill 文件在计时外构造；计时区包含从 spill 读回并恢复可访问结构。 |

注意：`io_queue_wait_ms`、`io_device_latency_ms`、`io_end_to_end_latency_ms` 是所有 IO request 的累计时间，不是 wall time。它们可以说明每个 request 的排队/设备延迟，但不能直接与 `time/iter` 相加。

## 总体结果

| 场景 | 数据集 | old time | BM time | BM 相对速度 | 初步结论 |
| --- | --- | ---: | ---: | ---: | --- |
| Store | fixed | 17.73s | 14.66s | 120.98% | BM batch typed store 更快，瓶颈仍是大量 fixed row 写入。 |
| Store | variable | 3.46s | 1.85s | 186.79% | BM variable store 优势明显，old 的逐行 store/string 路径成本更高。 |
| Read | fixed | 2.87s | 1.93s | 148.53% | BM resident extract 已明显优于 old，瓶颈是大规模 fixed 列扫描/写 result vector。 |
| Read | variable | 1.24s | 1.18s | 105.59% | 两边接近，瓶颈更偏向 string view/result vector 处理。 |
| SpillWrite | fixed | 83.62s | 72.13s | 115.94% | BM 写出瓶颈几乎完全是压缩。 |
| SpillWrite | variable | 6.62s | 6.05s | 109.46% | BM 小幅领先，瓶颈仍是压缩。 |
| SpillRead | fixed | 44.19s | 26.42s | 167.23% | BM 主要瓶颈是 `BatchPin` 中的读回和解压，其次是构造全量 pointer vector。 |
| SpillRead | variable | 5.85s | 2.31s | 252.63% | BM 避免重建 RowContainer，主要瓶颈是解压和 string view rebase。 |

## Store

### fixed

结果：

| 实现 | time |
| --- | ---: |
| old | 17.73s |
| BM | 14.66s |

Store benchmark 的输入 batch 已经在计时外构造，因此这里主要测 row container 写入本身。fixed 数据集有 5.37 亿行，每行 3 个 fixed-width 列，计时区内主要工作是：

- old：逐行 `newRow()`，逐列 `store(decoded[column], row, target, column)`。
- BM：`appendBatch(batch)`，走 batch 写入路径。

BM 比 old 快约 21%。这说明现在 fixed store 的 typed batch 路径已经产生收益，old 的逐行逐列 store 调用成本更高。BM 侧剩余瓶颈大概率是内存写带宽、row block 分配和 fixed column copy 循环本身，而不是类型 dispatch 或 nullable 分支。

下一步如果继续优化 Store fixed，需要增加 store 细分指标，例如 row block 分配次数、typed column copy 时间、pointer append 时间。当前 benchmark 没有这些内部拆分，不能进一步精确归因。

### variable

结果：

| 实现 | time |
| --- | ---: |
| old | 3.46s |
| BM | 1.85s |

variable 行数只有 1,028 万，但每行有 1KB string。输入字符串已经在计时外生成，因此计时区主要是 string 写入 RowContainer 的成本。

BM 比 old 快约 87%。主要原因应是 BM 的 `appendBatch()` 对 variable 数据采用批量写入，减少了 old `newRow() + store()` 的逐行调用开销。这个场景下 BM 的剩余瓶颈主要是把 string payload 写入 heap block 的内存 copy。

需要注意：这个 Store 场景与 SpillWrite 的 `store_setup_ms` 口径不同。SpillWrite 的 setup metric 包含输入构造，因此不能直接拿来解释 Store benchmark 的纯写入耗时。

## Read

### fixed

结果：

| 实现 | time |
| --- | ---: |
| old | 2.87s |
| BM | 1.93s |

Read benchmark 测的是 resident extract：数据完全在内存中，输入是已保存的 row pointer，计时区对每个 batch、每个 column 调用 extract。

fixed 数据集需要从 5.37 亿行里抽取 3 个 fixed-width 列。BM 比 old 快约 49%，说明目前 resident fixed extract 的 typed/non-null 快路径是有效的。这个场景没有 spill、pin、rebase 或 RowId 解析成本，瓶颈主要是顺序扫描 rows 并写出 result vector。

后续优化重点不是 RowId 或 BufferManager，而是 resident extract 的 CPU/memory bandwidth：

- fixed 列 extract 是否可以进一步减少 per-row 地址计算；
- result vector 写入是否有更宽的批量 copy 路径；
- row pointer 数组访问是否造成 cache miss。

### variable

结果：

| 实现 | time |
| --- | ---: |
| old | 1.24s |
| BM | 1.18s |

variable read 两边非常接近，BM 只快约 6%。该场景行数少很多，但多一列 string。extract 主要处理 fixed columns 和 string view/result vector 写入，不复制 10 GiB string payload。

BM 在 variable read 上没有 fixed 那么明显的优势，说明当前主要成本已经不是 old 的类型 dispatch，而是 string view 处理、result vector 写入和 pointer 访问。若要继续优化，应先给 resident extract 增加列级 metrics，确认 string column 与 fixed columns 分别耗时多少。

## SpillWrite

### fixed

结果：

| 实现 | spill time | setup metric | 写出信息 |
| --- | ---: | ---: | --- |
| old | 83.62s | 23.25s | `spill_bytes=9.45GB`, `files=10754` |
| BM | 72.13s | 19.29s | `physical_write=9.32GB`, `write_count=3073` |

BM 的详细指标：

| 指标 | 值 |
| --- | ---: |
| `flush_ms` | 72.13s |
| `bm_compress_ms` | 72.03s |
| `bm_spill_write_count` | 3073 |
| `bm_spill_write_bytes` | 12.89GB |
| `bm_spill_physical_write_bytes` | 9.32GB |

BM fixed spill write 的瓶颈非常明确：`flush_ms` 几乎全部被 `bm_compress_ms` 覆盖。RowContainer finalize、handle release、block 收集等逻辑不是主要成本。

old 的 stderr 没有压缩细分，但 spill time 为 83.62s，写出文件数为 10754，明显高于 BM 的 3073 个 block。old 侧主要成本应是 row-based serialization、压缩和更多 spill batch/file 管理成本的组合。

下一步如果优化 fixed SpillWrite，应优先看压缩和 IO pipeline，而不是 RowContainer 元数据：

- zstd level/strategy；
- block size 与压缩块数量；
- 压缩与写 IO 是否能 pipeline；
- 是否需要对 fixed 数据提供更适合压缩器的 layout。

### variable

结果：

| 实现 | spill time | setup metric | 写出信息 |
| --- | ---: | ---: | --- |
| old | 6.62s | 23.32s | `spill_bytes=341.8MB`, `files=10410` |
| BM | 6.05s | 22.47s | `physical_write=340.6MB`, `write_count=2610` |

BM 的详细指标：

| 指标 | 值 |
| --- | ---: |
| `flush_ms` | 6.05s |
| `bm_compress_ms` | 5.98s |
| `bm_spill_write_count` | 2610 |
| `bm_spill_write_bytes` | 10.95GB |
| `bm_spill_physical_write_bytes` | 340.6MB |

variable 的物理写出只有约 340MB，压缩后数据量远小于逻辑输入。BM 仍然是压缩占绝对主导，`flush_ms - compress_ms` 只有约 66ms。

因此 variable SpillWrite 的瓶颈同样不是 RowContainer flush 逻辑，而是压缩。BM 比 old 快约 9%，主要来自更少的写出块/文件和更直接的 block flush 路径。

## SpillRead

### fixed

结果：

| 实现 | time |
| --- | ---: |
| old | 44.19s |
| BM | 26.42s |

old 详细指标：

| 阶段 | 耗时 |
| --- | ---: |
| `create_reader_ms` | 3.66s |
| `next_batch_ms` | 28.56s |
| `copy_rows_ms` | 9.04s |
| `list_rows_ms` | 1.69s |
| `serialized_bytes` | 11.27GB |
| `batches/files` | 10754 |

old fixed 的最大成本是 `nextBatch()`，其次是把读回来的 serialized rows 重新 `copySerializedRow()` 到新 RowContainer。`list_rows_ms=1.69s` 是为了让 benchmark 对齐 BM，恢复后再构造全量 row pointer vector。这个成本与 BM 的 `bulk_append_ptrs_ms=1.70s` 基本等价。

BM 详细指标：

| 阶段 | 耗时 |
| --- | ---: |
| `try_load_all_ms` | 26.42s |
| `bulk_batch_pin_ms` | 24.73s |
| `bm_decompress_ms` | 21.25s |
| `bulk_append_ptrs_ms` | 1.70s |
| `bulk_update_ptrs_ms` | 0.04ms |
| `bulk_rebase_strings_ms` | 0 |
| `physical_read_bytes` | 9.32GB |
| `pin_reads` | 3073 |

BM fixed 的耗时结构可以拆成：

```text
BatchPin 非解压成本 ~= 24.73s - 21.25s = 3.48s
pointer vector 构造 ~= 1.70s
其他 RowContainer 逻辑 ~= 可忽略
```

这里 pointer vector 构造不是异常成本。fixed 有 5.37 亿行，全量输出 `vector<char*>` 本身要写约 4GB 指针数组；old 的 `list_rows_ms=1.69s` 与 BM 的 `bulk_append_ptrs_ms=1.70s` 对齐。

新增 IO 指标显示：

| IO 指标 | 值 |
| --- | ---: |
| `io_completed` | 3073 |
| `io_completed_bytes` | 9.32GB |
| `io_submit_batches` | 25 |
| `io_device_latency_ms` | 144,989ms |
| `io_queue_wait_ms` | 3,513,838ms |
| `io_avg_end_to_end_latency_us` | 1,190,637us |

这些 IO 时间是 request 级累计值，不能与 wall time 相加。它们说明 3073 个 4MB 级别 read request 在 scheduler 内部有明显排队和设备等待；因为并发执行，最终 wall time 体现为 `bulk_batch_pin_ms=24.73s`。

fixed SpillRead 的当前瓶颈排序：

1. zstd 解压：21.25s，占 BM wall time 的约 80%。
2. BatchPin 非解压读回路径：约 3.48s，包括 IO submit/wait、payload install、handle 构造等。
3. 全量 row pointer vector 构造：1.70s，属于与 old 对齐后的必要成本。

下一步优化建议：

- 先在 `BatchPin` 内部继续拆 metrics：submit reads、wait futures、install payload、make handles、unpin/bookkeeping。
- 对 fixed 数据评估压缩配置和 block size。当前 BM read/write 的 fixed 主成本都被 zstd 覆盖。
- pointer vector 构造可以暂时不作为重点，除非上层接口允许在某些路径不返回全量 pointer。

### variable

结果：

| 实现 | time |
| --- | ---: |
| old | 5.85s |
| BM | 2.31s |

old 详细指标：

| 阶段 | 耗时 |
| --- | ---: |
| `create_reader_ms` | 0.11s |
| `next_batch_ms` | 1.93s |
| `copy_rows_ms` | 3.63s |
| `list_rows_ms` | 0.05s |
| `serialized_bytes` | 10.91GB |
| `batches/files` | 10410 |

old variable 的主要瓶颈是 `copy_rows_ms=3.63s`。它需要把读回来的 serialized rows 重新插入新 RowContainer，variable string metadata/payload 处理成本较高。`next_batch_ms=1.93s` 是第二大成本。

BM 详细指标：

| 阶段 | 耗时 |
| --- | ---: |
| `try_load_all_ms` | 2.31s |
| `bulk_batch_pin_ms` | 2.04s |
| `bm_decompress_ms` | 1.90s |
| `bulk_rebase_strings_ms` | 0.25s |
| `bulk_append_ptrs_ms` | 0.02s |
| `physical_read_bytes` | 339.2MB |
| `pin_reads` | 2610 |

BM variable 的最大优势是读回后不重建 RowContainer，而是通过 BatchPin 把 blocks 读回，并对 string views 做 pointer rebase。相比 old 的 `copySerializedRow()`，这条路径避免了大量 row 级复制。

variable SpillRead 的当前瓶颈排序：

1. zstd 解压：1.90s，占 BM wall time 的约 82%。
2. string view rebase：0.25s，占约 11%。
3. BatchPin 非解压成本：约 0.15s。
4. pointer vector 构造：0.02s，行数少，不是瓶颈。

后续优化重点：

- 如果要优化 variable read，优先看解压，其次看 `rebaseStringViews()`。
- `rebaseStringViews()` 当前已经有单 heap fast path；如果实际运行中一个 part 多 heap 的比例较高，再考虑为 heap base 匹配增加索引结构。当前这次 benchmark 里 rebase 0.25s，不是第一优先级。

## 结论

当前 BM RowContainer 在四类场景下都不弱于 old，优势最大的场景是 variable Store 和 variable SpillRead。结合本次 metrics，主要瓶颈可以归纳为：

| 场景 | 当前主要瓶颈 |
| --- | --- |
| Store fixed | 大量 fixed row batch 写入，偏内存带宽/循环成本。 |
| Store variable | string payload 写入；BM 已明显优于 old。 |
| Read fixed | resident extract 的顺序扫描和 result vector 写入。 |
| Read variable | string view/result vector 处理，两边接近。 |
| SpillWrite fixed | zstd 压缩。 |
| SpillWrite variable | zstd 压缩。 |
| SpillRead fixed | zstd 解压，其次 BatchPin 非解压读回路径，再其次全量 pointer vector 构造。 |
| SpillRead variable | zstd 解压，其次 string view rebase。 |

短期最有价值的优化方向不是继续改 RowContainer 元数据结构，而是：

1. 给 `BufferManager::BatchPin` 增加更细的 wall-time metrics，拆出 submit、wait、install、handle 构造。
2. 针对 fixed spill read/write 评估压缩配置和 block size，因为 fixed 的读写主成本都被 zstd 覆盖。
3. 对 resident read/store 只有在需要进一步压榨 CPU 性能时，再补列级 metrics 后优化 typed copy/extract 循环。
