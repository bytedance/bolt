# BM RowContainer Benchmark 分析

本文基于最新一次 `bolt_exec_bm_row_container_benchmark` 结果，分析 old `RowContainer` 与 `BmRowContainer` 在 Store、Read、SpillWrite、SpillRead 四类场景下的主要瓶颈。

## 基准口径

本次运行的输入逻辑数据量为 25 GiB：

| 数据集 | 行数 | 逻辑形态 |
| --- | ---: | --- |
| fixed | 1,342,177,280 | `BIGINT + INTEGER + DOUBLE` |
| variable | 25,712,209 | fixed 三列 + `VARCHAR(1024)` |

四类 benchmark 的计时范围不同：

| 场景 | 计时内容 |
| --- | --- |
| Store | 输入 `RowVector` 在计时外构造；计时区只包含写入 RowContainer。 |
| Read | RowContainer 和 row pointer 在计时外构造；计时区只包含 resident extract。 |
| SpillWrite | Folly benchmark 计时区只包含 spill/flush；stderr 的 `store_setup_ms` 是额外 metric，包含输入构造和写入 RowContainer。 |
| SpillRead | 源 RowContainer 和 spill 文件在计时外构造；计时区包含从 spill 读回并恢复可访问结构。 |

注意：

- `io_queue_wait_ms`、`io_device_latency_ms`、`io_end_to_end_latency_ms` 是所有 IO request 的累计时间，不是 wall time。它们可以说明每个 request 的排队/设备延迟，但不能直接与 `time/iter` 相加。
- old row-based spill benchmark 不拆分 spill run。因为 old `Spiller::SpillStatus::rowsWritten` 是 `int32_t`，benchmark 会在 old spill read/write 入口按 `logical_bytes / logical_row_bytes` 估算行数；如果超过 `INT32_MAX`，会在构造 RowContainer 之前直接退出并打印错误，避免长时间运行后 coredump。本次 fixed 25 GiB 只有 13.42 亿行，未超过该限制。

## 总体结果

| 场景 | 数据集 | old time | BM time | BM 相对速度 | 初步结论 |
| --- | --- | ---: | ---: | ---: | --- |
| Store | fixed | 44.51s | 34.15s | 130.35% | BM batch typed store 优势明显，fixed 写入仍偏内存带宽和循环成本。 |
| Store | variable | 9.57s | 4.75s | 201.56% | BM variable store 优势非常明显，old 逐行 store/string 写入成本更高。 |
| Read | fixed | 7.11s | 4.58s | 155.28% | BM resident extract 明显优于 old，主要是 fixed 列顺序扫描和 result vector 写入。 |
| Read | variable | 3.04s | 2.84s | 107.22% | 两边接近，成本主要在 string view/result vector 处理。 |
| SpillWrite | fixed | 3.44min | 3.11min | 110.48% | 两边都主要被 zstd 压缩支配，BM 小幅领先。 |
| SpillWrite | variable | 16.72s | 14.91s | 112.16% | BM 小幅领先，压缩仍是主瓶颈。 |
| SpillRead | fixed | 1.82min | 58.70s | 186.06% | BM 避免重建 RowContainer，主要瓶颈是解压，其次是全量 pointer vector 构造。 |
| SpillRead | variable | 16.22s | 5.75s | 281.91% | BM 避免 `copySerializedRow()`，优势非常明显；瓶颈是解压和 string view rebase。 |

## Store

### fixed

| 实现 | time |
| --- | ---: |
| old | 44.51s |
| BM | 34.15s |

Store benchmark 的输入 batch 已经在计时外构造，因此这里主要测 row container 写入本身。fixed 数据集有 13.42 亿行，每行 3 个 fixed-width 列。

BM 比 old 快约 30%。这说明 fixed store 的 typed batch 路径已经产生稳定收益。old 仍然是逐行 `newRow()`、逐列 `store(decoded[column], row, target, column)`；BM 通过 `appendBatch(batch)` 减少了 per-row/per-column 调用和分支成本。

BM 侧剩余瓶颈大概率是内存写带宽、row block 分配和 fixed column copy 循环本身。若继续优化 Store fixed，需要增加 row block 分配次数、typed column copy 时间、pointer append 时间等细分指标。

### variable

| 实现 | time |
| --- | ---: |
| old | 9.57s |
| BM | 4.75s |

variable 行数为 2571 万，每行有 1KB string。输入字符串在计时外生成，因此计时区主要是 string 写入 RowContainer 的成本。

BM 比 old 快约 2.0 倍。主要原因是 BM 的 `appendBatch()` 对 variable 数据走批量写入路径，减少 old `newRow() + store()` 的逐行调用开销。这个场景下 BM 的剩余瓶颈主要是把 string payload 写入 heap block 的内存 copy。

需要注意：这个 Store 场景与 SpillWrite 的 `store_setup_ms` 口径不同。SpillWrite 的 setup metric 包含输入构造，因此不能直接拿来解释 Store benchmark 的纯写入耗时。

## Read

### fixed

| 实现 | time |
| --- | ---: |
| old | 7.11s |
| BM | 4.58s |

Read benchmark 测的是 resident extract：数据完全在内存中，输入是已保存的 row pointer，计时区对每个 batch、每个 column 调用 extract。

fixed 数据集需要从 13.42 亿行里抽取 3 个 fixed-width 列。BM 比 old 快约 55%，说明 resident fixed extract 的 typed/non-null 快路径有效。这个场景没有 spill、pin、rebase 或 RowId 解析成本，瓶颈主要是顺序扫描 rows 并写出 result vector。

后续优化重点不是 RowId 或 BufferManager，而是 resident extract 的 CPU/memory bandwidth：

- fixed 列 extract 是否可以进一步减少 per-row 地址计算；
- result vector 写入是否有更宽的批量 copy 路径；
- row pointer 数组访问是否造成 cache miss。

### variable

| 实现 | time |
| --- | ---: |
| old | 3.04s |
| BM | 2.84s |

variable read 两边非常接近，BM 快约 7%。该场景行数少很多，但多一列 string。extract 主要处理 fixed columns 和 string view/result vector 写入，不复制 25 GiB string payload。

BM 在 variable read 上没有 fixed 那么明显的优势，说明当前主要成本已经不是 old 的类型 dispatch，而是 string view 处理、result vector 写入和 pointer 访问。若要继续优化，应先给 resident extract 增加列级 metrics，确认 string column 与 fixed columns 分别耗时多少。

## SpillWrite

### fixed

| 实现 | spill time | setup metric | 写出信息 |
| --- | ---: | ---: | --- |
| old | 206.32s | 44.69s | `spill_bytes=23.62GB`, `files=26883` |
| BM | 186.75s | 34.35s | `physical_write=23.29GB`, `write_count=7681` |

old 当前指标只包含总 `spill_ms`、压缩后 `spill_bytes` 和 `files`。由于 old row-based spill 内部的 serialization、flush、write 时间没有在当前 benchmark metrics 中拆开，不能从这次输出里继续细分 old fixed write 的内部耗时。

BM 详细指标：

| 指标 | 值 |
| --- | ---: |
| `flush_ms` | 186.75s |
| `bm_compress_ms` | 183.51s |
| `bm_spill_write_count` | 7681 |
| `bm_spill_write_bytes` | 32.22GB |
| `bm_spill_physical_write_bytes` | 23.29GB |

fixed SpillWrite 的核心瓶颈是压缩。BM 的 `flush_ms` 几乎全部被 `bm_compress_ms` 覆盖。old 没有同等级的压缩耗时拆分，但它最终写出 23.62GB，和 BM 的 23.29GB 接近，因此两边都被压缩和写出物理数据主导。

BM 只快约 10%，原因是两边最终都要压缩并写出约 23GB 物理数据，结构差异被压缩成本覆盖。old 文件数为 26883，BM block 数为 7681，old 的 flush/write 调用更碎，但在 fixed write 中仍然不是压倒性差异。

### variable

| 实现 | spill time | setup metric | 写出信息 |
| --- | ---: | ---: | --- |
| old | 16.72s | 9.99s | `spill_bytes=854.65MB`, `files=26025` |
| BM | 14.91s | 5.21s | `physical_write=848.30MB`, `write_count=6524` |

old 当前指标只包含总 `spill_ms`、压缩后 `spill_bytes` 和 `files`，不能继续细分 serialization、flush、write 的各自耗时。

BM 详细指标：

| 指标 | 值 |
| --- | ---: |
| `flush_ms` | 14.91s |
| `bm_compress_ms` | 14.74s |
| `bm_spill_write_count` | 6524 |
| `bm_spill_write_bytes` | 27.36GB |
| `bm_spill_physical_write_bytes` | 848.30MB |

variable 的物理写出只有约 850MB，压缩后数据量远小于逻辑输入。BM 仍然是压缩占绝对主导，`flush_ms - compress_ms` 只有约 171ms。

BM 比 old 快约 12%。old 的文件数约为 BM block 数的 4 倍，额外 file/batch 管理和 row-based serialization 可能带来成本，但当前 old metrics 还不足以精确拆分。BM 的 setup metric 也显著更低，说明 variable store 路径在 spill write 准备阶段也有收益，但 Folly benchmark 的正式计时区只包含 spill/flush。

## SpillRead

### fixed

| 实现 | time |
| --- | ---: |
| old | 109.0s |
| BM | 58.70s |

old 详细指标：

| 阶段 | 耗时 |
| --- | ---: |
| `create_reader_ms` | 8.39s |
| `next_batch_ms` | 70.56s |
| `copy_rows_ms` | 23.23s |
| `list_rows_ms` | 4.22s |
| `serialized_bytes` | 28.19GB |
| `batches/files` | 26883 |

old fixed 的最大成本是 `nextBatch()`，其次是把读回来的 serialized rows 重新 `copySerializedRow()` 到新 RowContainer。`list_rows_ms=4.22s` 是为了让 benchmark 对齐 BM，恢复后再构造全量 row pointer vector。

BM 详细指标：

| 阶段 | 耗时 |
| --- | ---: |
| `try_load_all_ms` | 58.70s |
| `bulk_batch_pin_ms` | 52.84s |
| `bm_decompress_ms` | 50.35s |
| `bulk_append_ptrs_ms` | 5.86s |
| `bulk_update_ptrs_ms` | 0.08ms |
| `bulk_rebase_strings_ms` | 0 |
| `physical_read_bytes` | 23.29GB |
| `pin_reads` | 7681 |

BM fixed 的耗时结构可以拆成：

```text
BatchPin 非解压成本 ~= 52.84s - 50.35s = 2.49s
pointer vector 构造 ~= 5.86s
其他 RowContainer 逻辑 ~= 可忽略
```

这里 pointer vector 构造不是异常成本。fixed 有 13.42 亿行，全量输出 `vector<char*>` 本身要写约 10GB 指针数组；old 的 `list_rows_ms=4.22s` 与 BM 的 `bulk_append_ptrs_ms=5.86s` 是同一类成本。

新增 IO 指标显示：

| IO 指标 | 值 |
| --- | ---: |
| `io_completed` | 7681 |
| `io_completed_bytes` | 23.29GB |
| `io_submit_batches` | 61 |
| `io_device_latency_ms` | 269,855ms |
| `io_queue_wait_ms` | 16,385,165ms |
| `io_avg_end_to_end_latency_us` | 2,168,341us |

这些 IO 时间是 request 级累计值，不能与 wall time 相加。它们说明 7681 个 read request 在 scheduler 内部有明显排队和设备等待；因为并发执行，最终 wall time 体现为 `bulk_batch_pin_ms=52.84s`。

fixed SpillRead 的当前瓶颈排序：

1. zstd 解压：50.35s，占 BM wall time 的约 86%。
2. 全量 row pointer vector 构造：5.86s，数据量达到 10GB 指针写入。
3. BatchPin 非解压读回路径：约 2.49s，包括 IO submit/wait、payload install、handle 构造等。

BM 比 old 快约 1.86 倍，核心优势是避免 old 的 `copySerializedRow()` 重建 RowContainer，并且 block/file 数更少。

### variable

| 实现 | time |
| --- | ---: |
| old | 16.22s |
| BM | 5.75s |

old 详细指标：

| 阶段 | 耗时 |
| --- | ---: |
| `create_reader_ms` | 0.28s |
| `next_batch_ms` | 4.87s |
| `copy_rows_ms` | 10.63s |
| `list_rows_ms` | 0.12s |
| `serialized_bytes` | 27.28GB |
| `batches/files` | 26025 |

old variable 的主要瓶颈是 `copy_rows_ms=10.63s`。它需要把读回来的 serialized rows 重新插入新 RowContainer，variable string metadata/payload 处理成本较高。`next_batch_ms=4.87s` 是第二大成本。

BM 详细指标：

| 阶段 | 耗时 |
| --- | ---: |
| `try_load_all_ms` | 5.75s |
| `bulk_batch_pin_ms` | 5.07s |
| `bm_decompress_ms` | 4.70s |
| `bulk_rebase_strings_ms` | 0.62s |
| `bulk_append_ptrs_ms` | 0.06s |
| `physical_read_bytes` | 847.08MB |
| `pin_reads` | 6524 |

BM variable 的最大优势是读回后不重建 RowContainer，而是通过 BatchPin 把 blocks 读回，并对 string views 做 pointer rebase。相比 old 的 `copySerializedRow()`，这条路径避免了大量 row 级复制和 string 重写。

variable SpillRead 的当前瓶颈排序：

1. zstd 解压：4.70s，占 BM wall time 的约 82%。
2. string view rebase：0.62s，占约 11%。
3. BatchPin 非解压成本：约 0.37s。
4. pointer vector 构造：0.06s，行数较少，不是瓶颈。

## 文件数与块数

这次结果里 old 的 files/batches 明显多于 BM 的 blocks：

| 数据集 | old files/batches | BM blocks |
| --- | ---: | ---: |
| fixed | 26883 | 7681 |
| variable | 26025 | 6524 |

这会影响 old read 的 `create_reader_ms`、`next_batch_ms`，也会影响 old write 的 flush/write 次数。后续如果要做更严格的 apples-to-apples 对比，可以单独调 old `writeBufferSize` 或 file/batch 参数，看 old 的 files 数下降后 read/write 是否改善。

不过这不是 BM RowContainer 本身的问题。BM 当前的核心设计收益仍然来自：

- resident 路径的 batch typed store/extract；
- spill read 路径避免反序列化后重建 RowContainer；
- block 数更少，读回时通过 BatchPin 批量 pin/load。

## 结论

当前 BM RowContainer 在四类场景下都不弱于 old，优势最大的场景是 variable Store 和 variable SpillRead。结合本次 metrics，主要瓶颈可以归纳为：

| 场景 | 当前主要瓶颈 |
| --- | --- |
| Store fixed | 大量 fixed row batch 写入，偏内存带宽/循环成本。 |
| Store variable | string payload 写入；BM 已明显优于 old。 |
| Read fixed | resident extract 的顺序扫描和 result vector 写入。 |
| Read variable | string view/result vector 处理，两边接近。 |
| SpillWrite fixed | zstd 压缩；BM 和 old 都主要被压缩覆盖。 |
| SpillWrite variable | zstd 压缩；old 文件数更多，可能额外承担 row-based serialization 和更多 flush/write。 |
| SpillRead fixed | zstd 解压，其次全量 pointer vector 构造，再其次 BatchPin 非解压路径。 |
| SpillRead variable | zstd 解压，其次 string view rebase；BM 避免重建 RowContainer 是主要优势。 |

短期最有价值的优化方向：

1. 针对 fixed spill read/write 评估压缩配置和 block size，因为 fixed 的读写主成本都被 zstd 覆盖。
2. 给 `BufferManager::BatchPin` 增加更细的 wall-time metrics，拆出 submit、wait、install、handle 构造。
3. 如果继续优化 fixed SpillRead，关注全量 pointer vector 构造；25 GiB fixed 已经需要写约 10GB 指针数组。
4. 如果还需要继续分析 old SpillWrite，先补 old row-based spiller 的 serialization、flush、write 细分 metrics。
5. 对 resident read/store 只有在需要进一步压榨 CPU 性能时，再补列级 metrics 后优化 typed copy/extract 循环。
