# Bolt Native Lance Reader 与 Rust Lance Reader 性能对比

日期：2026-09-25

第 4 节保留优化前的跨实现基线，第 9 节记录各轮单线程 decode 优化结果和前后对比。

## 1. 结论摘要

1. **单线程全类型 decode 累计获得 7.61x 提升。** 同一 47 列 v2.2 文件、batch 4,096 下，Native 核心扫描中位数从 15.413 秒降到 2.025 秒，RSS 从 90.3 MiB 降到 67.3 MiB；提升来自范围解码、page plan 复用、SIMD codec kernel 和输出 buffer 所有权转移，不是扩大 batch 或增加 decoded/decompressed cache。
2. **单线程宽表仍快于 Rust。** 800 列真实 v2.0 数据全扫中，Native 核心扫描时间为 21.780 秒，Rust 为 26.852 秒，Native 快 18.9%；RSS 为 5.66 GiB 对 6.46 GiB，Native 低 12.4%。该数据不经过本轮 v2.1+ structural 优化，结果用于确认没有宽表内存回退。
3. **小数据与窄投影没有被牺牲。** 4,096 行、43 列小文件上 Native/Rust 核心时间为 9.94/10.48 ms，Native 快 5.2%；1M 行单列投影上一轮为 23.13/22.13 ms。
4. **Structural page 初始化已从 batch 生命周期中移出。** Nested 五列单线程为 0.664/0.486 秒。MiniBlock chunk table 和 repetition index 现在按 page 解析一次，并在 page 消费后立即释放；同步输入不驻留 payload。
5. **部分 nullable Int64 已达到 Rust 水平。** 0%、1%、50% null 的 Native/Rust 核心时间分别为 22.62/22.90、29.03/29.71、33.86/34.39 ms；100% null 继续走 constant fast path。
6. **row-addressed take API 已落地。** 在同一组地址和两列投影下，Native 1 点和 100 点核心时间分别为 0.829 ms 和 3.173 ms，Rust `ReadBatchParams::Indices` 为 2.712 ms 和 7.434 ms。Native 不再扫描 `row_id`，并保持调用方顺序、重复项、filter-first 和 batch 内存上界。

## 2. 测试对象与环境

| 项目 | 配置 |
| --- | --- |
| Bolt revision | `0e1693747385033c39376fe8ae2e69a8b59dc204`，另含本轮 scan-local page plan 修改 |
| Rust Lance revision | `f3dc9364c07d6e84706e1f07a9a0654b4ec480f1` |
| CPU | Intel Xeon Platinum 8260，2 socket，64 个在线 CPU，无 SMT |
| CPU 配额 | 两端固定 CPU `0-15`，均配置 16 个 decode/runtime/IO 线程 |
| 主测试 batch size | 4,096 行 |
| 重复次数 | 每个实现 7 轮，奇数轮 Native-Rust，偶数轮 Rust-Native |
| 统计检验 | 对每轮 Native/Rust wall time 的 log ratio 做 exact paired sign-flip test |
| 内存 | 每 1 ms 采样 `/proc/<pid>/status` 的 `VmHWM` |
| 数据缓存状态 | 每轮新建 reader/进程，不清 Linux page cache；结果主要反映 warm page-cache 下的 open、调度、解压、解码和 materialization |
| Native cache | 无 decoded/decompressed page cache |
| Rust cache | 主结果使用 256 MiB `LanceCache`；另有 `0` 容量对照 |

短用例同时报告两种时间：

- **核心 scan 时间**：由两端统一的 `scan_picoseconds_per_row` 和实际输入行数还原，排除 benchmark 进程和 runtime 初始化噪声。
- **wall time**：从进程创建到退出，适合观察完整宽表扫描和冷 reader open，但会明显放大毫秒级用例的进程启动差异。

预先指定的三个主假设为 800 列全扫、47 列全类型全扫和单点查询。三者 raw `p` 均为 `0.015625`，Holm 校正后均为 `0.046875`。类型分项和 batch-size 实验属于瓶颈诊断，不额外声称 family-wise 显著性。

## 3. 数据集设计

### 3.1 真实超宽表

`/tmp/data-0ee2-v20-zstd9-full.lance`

- Lance v2.0、Zstd level 9；
- 402,385 行、800 列；
- 文件大小 548,188,431 bytes；
- 用于 1/64/800 列投影和 800 列全表扫描；
- 这是当前本地开发的主验收数据集。

### 3.2 全类型与受控 null 数据集

`/tmp/bolt-lance-perf-all-types-1m/all_types_v2_2.lance`

- Lance v2.2、Zstd level 3；
- 1,048,576 行、47 个顶层字段；
- 文件大小 126,933,641 bytes；
- 覆盖 bool、全部有符号/无符号整数、float16/32/64、string/large string、binary/large binary/fixed binary、date/time/timestamp/duration、decimal128、Null、bfloat16、JSON、dictionary、List/LargeList/FixedSizeList、Map 和 Struct；
- 额外包含同值 Int64 的 0%、1%、50%、100% null 四列，用于隔离 validity/null materialization 成本；
- filter workload 使用 `filter_key in [0, 99]`，输出 10,500 行并校验 `row_id` checksum `5,460,519,750`。

### 3.3 Lance 官方 benchmark 数据调查

Lance 官方仓库没有可直接用于两端 file-reader 公平对比的单个“全类型大文件”；主要提供运行时生成器和面向 dataset/index 的数据集：

| 官方来源 | 负载 | 本报告处理方式 |
| --- | --- | --- |
| `rust/lance-file/benches/reader.rs` | 2M 行随机 Int32 全扫；500M 行、10% null、100,000 个间隔 5,120 行的随机 take；1/100 行每请求；memory/disk、cache/no-cache | 采用其 row gap、take 和 cache 对照思路；没有直接运行 500M 行版本，避免把超大临时生成成本混入本次 reader 对比 |
| `python/python/benchmarks/test_file.py` | 10M 行 UInt64，0%/50% null，nested struct，10 点 sample，覆盖 v2.0/v2.1/v2.2 | 将 0%/50% null 与 nested 设计合并进 47 列受控数据集 |
| `rust/lance-encoding/benches/decoder.rs` | 每种 primitive 128 MiB；FSL 维度 4/16/32/64/128；0%/50% null；dictionary string；packed struct | 用于确定类型分组和 null 密度；本报告测试完整 FileReader 路径，而非只测内存内 encoding kernel |
| `python/python/ci_benchmarks/datagen/basic.py` | 10M 行 UInt64/Int64/small string，并建立 BTree/Bitmap index | 适合后续 dataset/index 层测试，不适合当前单文件 FileReader 对比 |
| SIFT/GIST、Wikipedia、TPCH、HD-VILA、Image EDA | 向量检索、全文检索、分析查询、Blob/多模态 | 属于更高层 dataset/query benchmark，不应拿来归因 file decode 性能 |
| `test_data/` 与 exact-version fixtures | 小规模兼容性和回归数据 | 只用于类型能力验证，不用于性能结论 |

下一阶段建议把官方的 10M 单列、50% null、nested 和 100k take 生成器固化为可持久化 raw `.lance` 文件，并由同一个 manifest 驱动 Native/Rust 两端；不建议直接把 SIFT 或 TPCH 的总查询时间归因到 FileReader。

## 4. 优化前性能基线

### 4.1 全表扫描与宽表扩展性

| 用例 | Native 核心 scan | Rust 核心 scan | Native/Rust | Native wall | Rust wall | Native RSS | Rust RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 真实数据，1/800 列 | 27.7 ms | 25.5 ms | 1.08x | 0.388 s | 0.033 s | 50.7 MiB | 21.9 MiB |
| 真实数据，64/800 列 | 1.145 s | 0.724 s | 1.58x | 1.537 s | 0.768 s | 828.8 MiB | 1,194.6 MiB |
| 真实数据，800/800 列 | 7.429 s | 6.452 s | 1.15x | 8.155 s | 6.995 s | 6.62 GiB | 10.95 GiB |
| v2.2 全类型，47/47 列 | 9.200 s | 0.316 s | 29.1x | 9.574 s | 0.333 s | 198.1 MiB | 232.8 MiB |

观察：

- 800 列上 Native 的 RSS 优势来自 bounded scheduling 和较低的 batch readahead，但 6.62 GiB 仍然偏高；单个输出 batch 的 retained size 已达到约 527 MiB，Native pool peak 约 1.46 GiB，其余 RSS 主要来自并行临时 buffer、解压 workspace 和 allocator 保留。
- 宽表 Native/Rust CPU time 分别约 33.0/47.7 秒，对应平均 CPU 利用约 4.0/6.8 核。两端都未吃满 16 核，Native 的任务粒度和串行调度边界更明显。
- v2.0 宽表相对接近并不代表 v2.2 已完成优化。v2.2 structural page 路径完全不同，不能用 800 列结果替代全类型验收。

### 4.2 1% 过滤

| 用例 | Native 核心 scan | Rust 核心 scan | Native/Rust | Native RSS | Rust RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| 47 列，`filter_key` 选择约 1% | 3.039 s | 0.328 s | 9.27x | 169.1 MiB | 233.8 MiB |

Native 相比自己的全扫从 9.200 秒下降到 3.039 秒，证明 filter-first 和 late materialization 已经生效。Rust 当前 core decoder 不执行该谓词，benchmark 在所有列解码后用 Arrow filter，因此耗时与全扫接近。即使 Native 少 materialize 99% 的值，复杂列选中 run 仍会触发 page 级重复解码，所以尚未反超。

该结果不能解读为两端具有相同 filter pushdown 能力；它比较的是两端当前公开路径完成相同查询语义的成本。

### 4.3 点查询与稀疏 take

点查投影为 `row_id,string_value`。100 点用例的相邻点间隔 5,120 行，与 Lance 官方随机访问 benchmark 的 row gap 一致。

| 用例 | Native 当前路径 | Rust 当前路径 | Native 核心 scan | Rust 核心 scan | Native/Rust |
| --- | --- | --- | ---: | ---: | ---: |
| 已知 offset，1 行 | 扫描 `row_id`，命中后 late materialize | `ReadBatchParams::Indices` | 37.3 ms | 2.19 ms | 17.1x |
| 已知 offset，100 行 | `BigintValues` 扫描，按选中 run 解码 | `ReadBatchParams::Indices` | 265.3 ms | 6.87 ms | 38.6x |

优化前 Native 虽已有内部 `NativeLanceRowSelection`，但 `NativeLanceReader` 的公开扫描
入口没有等价于 Rust `Indices/Ranges` 的 row-domain API。100 个离散点会在
`FileColumnReader::read` 中拆成多个连续 run，逐 run 调用 `decodeRange`。因此该基线首先
反映 API 和调度能力缺口，其次才是 kernel 差距；第 9.7 节记录了新 take API 的结果。

### 4.4 各类型解码性能

以下均为 1,048,576 行、batch 4,096 的核心 scan 中位数，结果向量全部 materialize。

| 类型/类型组 | Native | Rust | Native/Rust |
| --- | ---: | ---: | ---: |
| Boolean | 10.5 ms | 3.83 ms | 2.74x |
| Int64，0% null，同值分布 | 24.4 ms | 8.80 ms | 2.77x |
| Float64 | 49.8 ms | 11.1 ms | 4.49x |
| 固定宽度数值组，13 列 | 551.7 ms | 45.2 ms | 12.2x |
| 时间/日期/Duration/Decimal，15 列 | 564.0 ms | 78.9 ms | 7.15x |
| String | 219.2 ms | 32.9 ms | 6.66x |
| String/Binary/扩展类型，7 列 | 652.0 ms | 123.4 ms | 5.28x |
| Dictionary<String> | 34.5 ms | 7.15 ms | 4.83x |
| List<Int32> | 632.3 ms | 18.4 ms | 34.4x |
| LargeList<String> | 1.297 s | 24.6 ms | 52.7x |
| FixedSizeList<Float32, 3> | 1.684 s | 9.29 ms | 181x |
| Map<Int32, String> | 2.043 s | 39.4 ms | 51.8x |
| Struct<Int64, String> | 5.833 s | 25.0 ms | 233x |
| 五种 nested 类型合计 | 6.876 s | 88.0 ms | 78.1x |

能力层面，Native 已覆盖上述类型及 v2.0-v2.3 的 Flat、Nullable、Bitpacked、Dictionary、FSST、MiniBlock、FullZip、Sparse、PackedStruct、VariablePackedStruct 和 Blob 等格式，并通过 123 个 reader tests。当前问题不是“不能解码”，而是 structural decode 的生命周期、批次边界和输出组装仍未达到 Rust 的实现效率。

明确不支持或要求上层参与的类型包括 Decimal256、负 scale decimal、不能无损映射到毫秒的 duration、未按整日对齐的 date64、缺少 resolver 的非 inline Blob，以及当前 Lance writer 本身不支持的 Union/RunEndEncoded/ListView 等。它们不能作为性能失败混入结果。

### 4.5 Null 处理

四列使用相同 Int64 公式，仅 validity 密度不同。

| Null 比例 | Native 核心 scan | 相对 Native 0% | Rust 核心 scan | 相对 Rust 0% | Native/Rust |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0% | 24.4 ms | 基线 | 8.80 ms | 基线 | 2.77x |
| 1% | 37.0 ms | +51.7% | 9.34 ms | +6.2% | 3.96x |
| 50% | 49.9 ms | +104.7% | 10.7 ms | +21.5% | 4.66x |
| 100% | 2.40 ms | -90.2% | 3.75 ms | -57.4% | 0.64x |

Native 的 all-null constant fast path有效；问题集中在部分 nullable 路径。当前实现多处使用 `std::vector<bool>`、逐 bit `push_back`、逐行 `setNull` 和逐层 validity 重建，少量 null 就使固定宽度 fast path退化。优化目标应是 packed bitmap 从 page 到 Bolt null buffer 的 word-level 传递，而不是为常见 null 比例增加分支特例。

### 4.6 Cache 对照

| Rust 配置 | 47 列全类型 wall | RSS |
| --- | ---: | ---: |
| `LanceCache=256 MiB` | 0.333 s | 232.8 MiB |
| `LanceCache=no_cache()` | 0.346 s | 227.0 MiB |

关闭 Rust cache 只使 wall time 增加约 3.9%、RSS 降低约 2.5%，无法解释 29 倍差距。Native 不应恢复 reader-scoped decoded/decompressed page cache。正确做法是 scan-local decoder state：状态只随单调扫描存在，page 消费完立即释放，不允许随机回看，也不允许跨 reader 复用解压结果。

## 5. 优化前根因分析

本节描述第 4 节基线对应的实现。第 9 节列出的范围解码和零拷贝改动已经消除其中
大部分重复工作，剩余问题仍按同一原则继续优化。

### 5.1 Structural page 在 batch 边界重复解码

`NativeLancePageSource::decodePhysicalColumn` 每个 batch 都新建 `NativeLanceStructuralPageReader`。当 `lanceStructuralPageSupportsRangeRead` 返回 false 时，它完整 decode 当前 page，再通过 `Vector::copy` 取出本 batch 的范围。Array、Map、Row、FixedSizeList、packed child、FullZip 和多数带 rep/def 的布局均明确落入 full-page 路径。

当前测试文件每个 writer batch/page 为 65,536 行，而 reader batch 为 4,096 行，因此同一 page 最多被完整解码约 16 次。诊断结果如下：

| 用例 | batch 4,096 | batch 65,536 | Native 提升 | Rust 变化 |
| --- | ---: | ---: | ---: | ---: |
| 47 列全类型 | 9.200 s | 1.221 s | 7.5x | 0.316 s -> 0.371 s |
| Struct | 5.833 s | 0.433 s | 13.5x | 25.0 ms -> 28.7 ms |

这直接证明重复 page decode 是第一瓶颈。增大 batch 同时把全类型 Native RSS 从 198 MiB 提高到 352 MiB，Rust 从 233 MiB 提高到 787 MiB，因此它只能作为诊断，不能作为生产优化。

### 5.2 并行粒度停留在顶层逻辑列

`NativeLanceRootColumnReader::read` 的 `ParallelFor` 只按顶层 file column 切任务。一个 Struct 即使包含多个独立 physical branch，仍由一个 worker 串行执行 `decodeNativeLanceStructuralColumn`。实测单 Struct 的 `perf stat` 为 6.21 秒 task-clock / 6.22 秒 elapsed，接近单核；全类型 Native 平均使用约 2.4 核，Rust 约 7.9 核。

复杂列工作量远大于普通 scalar，以“顶层列数”决定并发会产生严重长尾。任务图必须下沉到 physical branch/page chunk，同时由统一内存预算限制并发，而不是简单增大线程数。

### 5.3 多层临时对象、复制和逐行校验

当前 structural 路径存在以下通用成本：

- `decodeMiniBlock`/FullZip 将输入切成多个 `BufferPtr`，部分路径调用 `copyBuffer`；
- `makeLeafVector` 再分配 Bolt vector，并逐行转换 fixed、variable、dictionary index；
- `decodePhysicalColumn` 先构造 page 结果，再 `result->copy` 到 batch 输出；
- Struct/List/Map sibling merge 每个 batch 逐行比较 null、offset 和 size；
- variable 数据先构造逐值 view 集合，再逐行 `FlatVector<StringView>::set`。

这些步骤在重复 page decode 后被进一步放大。应先消除重复，再做 bulk kernel 和 ownership transfer，否则局部 SIMD 收益会被生命周期问题淹没。

### 5.4 稀疏 selection 按 run 重入 decoder

`FileColumnReader::read` 将 selection 展开为连续 run，并为每个 run 单独调用 `decodeRange`。对不支持 range read 的 structural page，同一 page 内多个离散 run 会重复初始化和完整 decode。100 点用例比单点增加约 7.1 倍 Native 核心时间，而 Rust 的 page-aware `Indices` 只增加约 3.1 倍。

### 5.5 宽表调度和内存并发仍需收敛

Native 800 列虽然比 Rust 少 39.5% RSS，但仍达到 6.62 GiB，并且 16 线程只得到约 4 核平均利用率。`readPlanMutex_`、scheduler result 提取、压缩 session map，以及以逻辑列为单位的大任务会限制扩展性。后续不能用无限 readahead 换吞吐，必须以 bytes 而不是 task count 控制在途工作。

## 6. 可直接提升性能的通用优化方案

### P0：实现单调、流式的 StructuralPageDecodeSession

新增 scan-local session，由 `NativeLancePageSource` 创建、由 `NativeLanceColumnReader` 的单调 cursor 驱动：

```text
prepare(page metadata, row selection)
  -> schedule compressed ranges
  -> decodeNext(output row budget)
       values cursor + rep cursor + def cursor + validity cursor
  -> publish batch slice
  -> advance / release page immediately
```

约束：

- key 只允许当前 `(physicalColumn, pageIndex)`，不是多 page LRU；
- 只保存 codec state、bounded compressed input、rep/def decoder state、offset tail 和当前输出块；
- 不保存完整 decoded/decompressed page；
- row request 必须单调，回退直接报错或新建独立 point-read session；
- FullZip、MiniBlock、Sparse、FixedSizeList 和 packed struct 共用同一 session 生命周期协议，各自提供 stateful kernel。

预期收益和验收：

- batch 4,096 的全类型核心时间从 9.20 秒降到不高于 1.5 秒；
- Struct 至少获得诊断实验已证明的 10 倍提升；
- batch 1,024/4,096/16,384 的吞吐差异控制在 20% 内；
- RSS 不得超过当前 batch 4,096 基线，且不能通过保留已解码 page 达标。

### 已完成 P0：给 Native Reader 增加 row-domain `Indices` API

`NativeLanceReader::createTakeReader()` 接受不可变、file-global 的 row ID 流。Coordinator
仅新增地址 cursor；地址批次通过现有 `NativeLanceRowSelection` 进入同一个
RootColumnReader/PageSource/PageReader，不新增第二套 reader 或 decoder。FileReader 在 open
阶段已有 page row index，因此 row IDs 直接映射到 page spans，不再扫描 `row_id`。

对于 sparse indices：

- 排序并保留恢复原顺序的 permutation；
- 按 physical page 分组；
- 每个 page 只初始化一次 decoder；
- selection 直接传给 page kernel，输出紧凑 vector；
- 多列共享同一个 immutable row selection，但不共享 decoded payload。

实现额外保证：`next()` 返回消费的地址数，过滤后输出可以更少；乱序和重复项通过
dictionary scatter 恢复；超出 `vector_size_t` 表示范围的地址跨度拆成多个 window；随机
take 不发布顺序 prefetch units，也不支持语义不明确的 Mutation。

验收目标：单点核心时间不高于 5 ms，100 个分散点不高于 15 ms，且读取字节随触及 page 数增长，而不是随全文件行数增长。

### P1：将并行任务从逻辑列下沉到 physical branch/page chunk

构建 `DecodeTaskGraph`：`Read -> Decompress -> DecodeLeaf -> ApplyRepDef -> Assemble`。Struct 的两个 child、Map 的 key/value、跨 page spans 可以并行，最终只在 ColumnReader 汇合。输入读取保持批量合并，但 scheduler-owned buffer 必须在 batch 边界释放；decoder 只保留当前 page 的流式状态，禁止跨 batch 缓存解压结果。

验收目标：复杂类型 16 线程平均 CPU 利用达到至少 6 核；800 列 RSS 不高于当前 6.62 GiB；线程数 1/4/16 的加速曲线单调且没有 oversubscription 回退。

### P1：直接写最终 Bolt buffer，消除 page-to-batch 二次复制

- Range 命中完整输出时转移 `BufferPtr` ownership 或返回 slice；
- fixed-width identity 类型在 little-endian 主机上使用 bulk copy，避免逐值 `readLittleEndian`；
- Variable/String 直接生成 Bolt `StringView`/values buffer，避免中间逐值容器；
- Dictionary 的字典值在当前 page session 内只组装一次，indices 直接写最终 vector；
- sibling null/offset 一致性在 page 初始化时验证一次，生产 batch merge 不再逐行重复验证。

验收目标：在 page 重复解码修复后，再将 batch 65,536 的全类型 Native/Rust 差距从 3.3 倍压到 1.5 倍以内。

### P1：统一 packed null bitmap 快路径

以 `BufferPtr + bitOffset + length` 表示 validity，不在 decode 内转换成 `std::vector<bool>`。全有效、全空、字节对齐和非对齐分别使用统一的 word copy/shift kernel；nested 每层只维护一个 validity cursor。输出 null count 在 word pass 中同时计算。

验收目标：Int64 1% null 相对 0% 的额外成本不超过 10%，50% null 不超过 25%；保持 100% null constant fast path。

### P1：selection 先按 page 聚合，再在 page 内向量化 compact

不要对每个连续 run 重入 `decodeRange`。ColumnReader 应把 batch-relative selection 转成 page-local indices/ranges，一次调度和初始化 page，再由 kernel 直接 compact 到目标 buffer。对于 variable/nested 类型，先批量生成 selected offsets，再只读取覆盖到的 payload ranges。

这同时改善 filter、mutation 和 point take，不依赖特定列名、过滤选择率或测试数据分布。

### P2：消除调度热路径上的全局串行区

在 `submitReadPlan()` 后冻结本批 read plan，让 worker 只读取 immutable page-owned handles。将 `readPlanMutex_` 从实际 decode/read 热路径移出；compressed session registry 按 physical column 分片或直接归 ColumnReader session 所有。先用 lock contention 和 executor queue 指标验证，再修改同步结构。

### P2：以字节预算驱动超宽表 wave scheduling

根据 page compressed bytes、预估输出 bytes 和 codec workspace，为列/physical branch 分批发射任务。目标不是降低并行度，而是在固定内存预算内始终保持 worker 有任务。动态 wave 应取代固定“16 batch readahead”或“一次提交全部 800 列”。

建议验收目标：800 列全扫达到 Native/Rust `<= 1.10x`，RSS 保持 Rust 的 60% 以下，并把 Native 平均 CPU 利用从约 4 核提升到至少 6 核。

## 7. 不采用的做法

- 不恢复 reader-scoped decoded/decompressed page cache；
- 不把 batch size 永久调到 65,536 来掩盖重复 decode；
- 不减少投影列、不跳过 vector materialization、不降低校验工作量来制造 benchmark 数字；
- 不用只含 primitive 的官方 benchmark 代替全类型和真实 800 列验收；
- 不把 dataset index、OS page cache 或跨查询结果缓存收益计入 FileReader decoder 优化；
- 不针对当前两个文件硬编码 page size、列数、null 密度或 codec。

## 8. 建议实施顺序与门禁

1. 先增加 page decode 次数、decoded rows、copied bytes、task queue wait、active bytes 五类 runtime counter，并把“每个 physical page 在顺序扫描中只初始化一次”写成测试。
2. 实现 StructuralPageDecodeSession，先覆盖 Struct、FixedSizeList、Map，再覆盖 scalar FullZip 和 Sparse。
3. 增加 Native `Ranges/Indices` 入口和 page-grouped selection，复跑 1/100/100,000 点 workload。
4. 下沉 physical branch 并行，按 batch/page 生命周期约束临时输入；验证 1/4/16 线程扩展性。
5. 做 null bitmap、variable 和 dictionary bulk materialization；每项必须用单类型数据隔离收益。
6. 每个优化分别跑 7 轮交替 paired benchmark；主门禁继续使用 800 列全扫、47 列全类型、单点查询三项并做 Holm 校正。

每一步必须同时满足正确性、吞吐和内存门禁。任何只在一个 batch size、一个列宽或一个 null 密度上生效的补丁都不应合入通用 reader 路径。

## 9. 单线程 Decode 优化结果

本轮没有增加 decoded/decompressed page cache，也没有扩大生产 batch。实现改动为：

- FullZip 的 item-only row domain 只解析和 materialize 请求行；固定宽度页按
  row stride 直接定位，变长页通过 on-disk offset index 定位；
- MiniBlock 使用 on-disk repetition index 将 List/Map 的 parent row range 映射到
  必需的连续 chunk，并补齐跨 chunk 的 preamble/trailer；
- FixedSizeList 的标量叶子按 MiniBlock chunk 做范围解码；混合 fixed/repeated
  domain 继续走保守路径；
- MiniBlock 子 buffer、RLE 子 buffer 和 variable value 改为共享 owner 的只读
  view，删除中间 payload 拷贝和逐值 `std::string` 所有权；
- FastLanes lane permutation 改为按输出位宽编译期选择的 O(1) 逆映射；
- 同宽 primitive、rep/def level 和常量值使用 bulk copy/fill，nullable item 的
  bitmap 按 64 位 word 构造。

### 9.1 优化前后

主对比使用同一 47 列文件、batch 4,096、单线程、七轮交替。这里的 baseline
是在本轮修改前由同一 worktree 二进制采集。

| 指标 | 优化前 Native | 优化后 Native | 变化 | Rust current |
| --- | ---: | ---: | ---: | ---: |
| 核心 scan 中位数 | 15.413 s | 4.197 s | **3.67x 加速** | 2.018 s |
| 端到端 wall 中位数 | 15.775 s | 4.561 s | **3.46x 加速** | 2.034 s |
| 峰值 RSS 中位数 | 90.3 MiB | 69.8 MiB | **降低 22.7%** | 206.5 MiB |
| optimized/baseline paired test | - | 0.275 | `p=0.015625` | - |

收益不是来自额外驻留内存：Native 同时减少约 20.5 MiB RSS。剩余 Native/Rust
核心时间差距约 2.08 倍，主要是每批 MiniBlock 解压、comprehensive
`decodeCompressive` dispatch 和输出 materialization。

### 9.2 普适性检查

| 场景 | Native 核心 scan | Rust 核心 scan | 结果 |
| --- | ---: | ---: | --- |
| 小数据，4,096 行、43 列、1 线程 | 13.56 ms | 10.74 ms | Native 慢 26.2%，没有复杂页级放大 |
| 窄投影，1M 行、1 列、1 线程 | 23.13 ms | 22.13 ms | Native 慢 4.5%，基本持平 |
| Nested 五列，1M 行、1 线程 | 0.939 s | 0.492 s | Native 慢 1.91x |
| 1% filter、47 列、1 线程 | 0.263 s | 2.071 s | Native 快 7.88x，late materialization 生效 |
| 800 列全扫、1 线程 | 21.293 s | 26.807 s | **Native 快 20.6%** |

800 列单线程 wall time 为 Native 21.925 秒、Rust 27.124 秒；RSS 分别为
5.69 GiB 和 6.19 GiB，Native 低 8.1%。因此本轮优化没有以牺牲宽表内存换取
小数据性能，也没有只优化某一种列宽。

受控 null 的最终单线程结果：

| Null 比例 | Native | Rust | Native/Rust |
| --- | ---: | ---: | ---: |
| 0% | 22.62 ms | 22.90 ms | 0.99x |
| 1% | 29.03 ms | 29.71 ms | 0.98x |
| 50% | 33.86 ms | 34.39 ms | 0.98x |

部分 nullable Int64 已达到 Rust 水平。100% null 继续使用 constant fast path。

### 9.3 后续单线程重点

当前剩余问题按收益排序：

1. MiniBlock chunk 大于 output batch 时仍可能跨 batch 重复 Zstd 解压；需要可暂停的
   compressed-input/value cursor，而不是保留解压后的 chunk。
2. `decodeCompressive` 的递归 dispatch、Zstd 初始化及 temporary buffer 生命周期仍占
   单线程 CPU 的主要部分，应将同一 page 的 codec plan 编译一次并复用不可变描述。
3. List/Map materialization 仍有 offsets/nulls 二次构造和 `ArrayVector::copyRanges`；应让
   page kernel 直接写 batch-owned offsets、sizes 和 null bitmap。
4. v2.0 单大 Zstd frame 的超宽稀疏 take 仍需要把每个被访问列流式推进到最远地址；
   后续应使用 writer 侧更细 page/reset-point 布局或 byte-budgeted decode 调度降低工作集，
   不能恢复 decoded/decompressed page cache。

### 9.4 Scan-local StructuralPagePlan 优化

本轮把 MiniBlock 的 chunk table 和 repetition index 从 batch-local decode 中提取为
`NativeLanceStructuralPagePlan`。PageSource 按 `(physical column, page index)` 持有当前扫描
涉及的不可变 plan，后续 batch 直接把 row range 映射为 chunk range；进入下一页或消费完
当前页后立即释放旧 plan。

内存和 I/O 约束如下：

- plan 只保存整数 chunk descriptor，不保存 compressed payload、decompressed buffer 或
  decoded vector；
- 同步 `BufferedInput` 仍按需读取 payload，不为优化扩大驻留范围；
- 异步 input 可在 metadata 阶段完成后，使用同一个 plan 提交精确 payload range；
- 单页且输出为 flat scalar/binary 时直接交付精确 range decode 结果；List、Map、Row 仍经
  batch assembler 归一化 offsets，避免 sibling branch 的 offset base 不一致。

单线程、batch 4,096、每端七轮交替的结果：

| 场景 | 上一轮 Native | 本轮 Native | Rust current | Native 变化 |
| --- | ---: | ---: | ---: | ---: |
| 47 列全类型 full scan | 4.197 s | 2.516 s | 2.023 s | **快 40.1%** |
| Nested 五列 full scan | 0.939 s | 0.787 s | 0.491 s | **快 16.2%** |
| 1% filter、47 列 | 0.263 s | 0.204 s | 2.068 s | **快 22.3%** |
| 4,096 行、43 列 | 13.56 ms | 11.63 ms | 10.61 ms | **快 14.2%** |
| 800 列 v2.0 full scan | 21.293 s | 21.780 s | 26.852 s | 路径未命中，作为无回退门禁 |

全类型 Native/Rust 核心时间比从 `2.08x` 降到 `1.24x`。全类型 Native 峰值 RSS
中位数从 69.8 MiB 降到 67.8 MiB；800 列 Native/Rust 峰值 RSS 为 5.66/6.42 GiB。
所有 Native/Rust 对比的 exact paired sign-flip `p = 0.015625`。800 列两轮 Native
之间约 4% 的差异伴随 Rust 运行波动，且 v2.0 代码路径未改，不归因为本轮优化收益。

本轮后剩余的首要 CPU 问题是同一 MiniBlock chunk 跨 batch 时仍重复执行 value/rep/def
解压。下一阶段应在 plan 之上加入可暂停 codec cursor；只允许保留 codec context、受预算
约束的 compressed input 和极小 tail，禁止保留完整 decoded/decompressed chunk。

### 9.5 SIMD decode 与输出 buffer 所有权转移

profile 显示 page plan 落地后，热点已从 metadata I/O 转移到 Zstd、ByteStreamSplit、
rep/def 拼接和 Bolt vector 物化。本轮继续做不增加驻留内存的通用优化：

- v2.1+ ByteStreamSplit 复用已有 Parquet/Arrow SIMD kernel，替换逐行逐字节转置；
- 与 Bolt 类型宽度、字节序完全一致的 primitive 复用 v2.0 raw-flat ownership API，直接将
  decoder 输出 buffer 交给 `FlatVector`；
- VARCHAR、VARBINARY 和 FixedSizeBinary 通过 `addStringBuffer` + `setNoCopy` 转移 value
  buffer 所有权；只有存在 out-of-line `StringView` 时才附加 owner；
- MiniBlock rep/def 直接 append 到最终 level vector，并使用 small-vector 保存常见的少量
  value buffer size，消除逐 chunk 临时容器；
- Struct/List/Map sibling 的 null bitmap、offset 和 size 一致性改为 word/buffer 批量校验，
  保留相同的完整校验语义。

单线程、batch 4,096、每端七轮交替的最新结果：

| 场景 | PagePlan 后 Native | 当前 Native | Rust current | 本轮 Native 变化 |
| --- | ---: | ---: | ---: | ---: |
| 47 列全类型 full scan | 2.516 s | 2.025 s | 2.011 s | **快 19.5%** |
| Nested 五列 full scan | 0.787 s | 0.664 s | 0.486 s | **快 15.6%** |
| 1% filter、47 列 | 0.204 s | 0.165 s | 2.028 s | **快 19.3%** |
| 4,096 行、43 列 | 11.63 ms | 9.94 ms | 10.48 ms | **快 14.6%** |

全类型 Native/Rust 核心 decode 差距已从上一轮的 24.4% 收窄到 0.7%，累计相对最初
15.413 秒基线提升 7.61 倍。Native 峰值 RSS 中位数为 67.3 MiB，低于上一轮的
67.8 MiB。800 列七轮中位数为 21.780 秒、5.66 GiB RSS；本轮修改只位于 structural
decoder 和 structural ColumnReader 合并路径，因此该 v2.0 数据集不命中新路径。

实测主数据集中 scalar MiniBlock 通常为 512 或 1,024 items，而 batch 为 4,096 行，
chunk 边界大多天然落在 batch 内。此时增加跨 batch codec cursor 不会减少主路径的 Zstd
frame 次数，反而会为每个活跃列增加状态。因此 cursor 只应在 plan 检测到 chunk 确实跨越
请求边界时启用，并继续遵守压缩输入预算；不能把它实现成通用 payload cache。

### 9.6 Reader 生命周期收敛与内存验收

在不修改 `BufferedInput` 的前提下，本轮进一步完成以下结构调整：

- filter prefetch 只读取第一个实际过滤列，projection 等 selection 生成后再规划；
- 每个 batch 完成后释放 scheduler 中未消费的输入，且在构造下一批输出前释放上一批；
- legacy Zstd session 按 physical column/page 索引，进入下一页时回收旧页；
- structural sibling 共享 canonical null、offset 和 size buffer，不缓存 decoded page；
- RootColumnReader 的 physical column、reader 和 stage 映射在构造时一次生成；
- structural 与 legacy 路径使用同一个 Zstd/LZ4 解压入口；
- 删除未接线的 memory-budget API 和只包装同步调用的多层状态对象。

16 线程、batch 4,096、每端七轮交替的结果：

| 场景 | 调整前 Native scan | 调整后 Native scan | 调整后 Native RSS | 调整后 pool peak |
| --- | ---: | ---: | ---: | ---: |
| 800 列 v2.0 full scan | 7.071 s | **6.722 s** | **6.23 GiB** | **1.13 GB** |
| 47 列全类型 full scan | - | **0.782 s** | **76.7 MiB** | **16.3 MiB** |
| 1% filter、47 列 | - | **0.166 s** | **52.0 MiB** | **2.50 MiB** |
| 单点 filter fallback | - | **22.4 ms** | **40.6 MiB** | **0.27 MiB** |

800 列调整前 RSS 中位数为 6.67 GiB、pool peak 为 1.59 GB；调整后分别下降
到 6.23 GiB 和 1.13 GB。scan 中位数同步改善 4.9%，没有用更大的 batch、readahead
或 decoded/decompressed cache 换取性能。对应结果文件为：

- `/tmp/lance-refactor-baseline-wide-800-7round.json`
- `/tmp/lance-refactor-final2-wide-800-7round.json`
- `/tmp/lance-refactor-final2-full-types-7round.json`
- `/tmp/lance-refactor-final-filter-7round.json`
- `/tmp/lance-refactor-final-point1-7round.json`

为排除跨时段波动，又将 take 前提交 `b0fc548753` 与当前提交用独立 Release 二进制做了
同机、七轮、奇偶换序 A/B。800 列 full scan 的 after/before 配对时间比中位数为
`1.0096x`，exact sign-flip `p=0.546875`；RSS 配对比中位数为 `1.0017x`。47 列 1%
filter 的配对时间比中位数为 `1.0047x`，`p=0.90625`；RSS 配对比中位数为
`0.9853x`。两项均没有可检测的性能或内存回退。

### 9.7 Row-addressed take 优化

相同 47 列全类型文件、`row_id,string_value` 投影、16 线程、每端七轮交替：

| 用例 | 旧 Native filter fallback | Native take | Rust take | Native/Rust | Native RSS | Native pool peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 点 | 22.415 ms | **0.829 ms** | 2.712 ms | **0.31x** | 39.2 MiB | 0.14 MiB |
| 100 点，间隔 5,120 行 | 119.124 ms | **3.173 ms** | 7.434 ms | **0.43x** | 40.1 MiB | 0.42 MiB |

Native 相对旧 fallback 分别提升 27.0x 和 37.5x；相对 Rust take 分别快 3.26x 和
2.32x。两项核心 scan log-ratio 的 exact paired sign-flip `p` 均为 `0.015625`，两端输出
行数与 `row_id` checksum 一致。窄点查的进程 wall time 仍由 Native 静态初始化主导，
因此 reader 路径比较使用 benchmark 核心 scan time，不把进程启动时间归因给 take
decoder。

主验收 800 列 v2.0 文件另取 100 个间隔 4,000 行的地址并投影全部列：Native/Rust
核心时间中位数为 **2.141/5.044 秒**，Native 快 2.36x；RSS 为 **4.62/0.445 GiB**，
Native pool peak 为 367.5 MiB。Native RSS 低于同文件全扫的 6.16 GiB，但该结果同时暴露
出 legacy 单大 Zstd frame 的限制：稀疏 selection 不能跳过 frame 前缀，800 个列 session
的 codec/allocator 工作集仍然较大。这是后续 page/reset-point 与 byte-budgeted decode
调度问题，不应以 page cache 规避。

对应结果文件为：

- `/tmp/lance-native-take-point1-7round-final-20260925.json`
- `/tmp/lance-native-take-point100-7round-final-20260925.json`
- `/tmp/lance-native-take-wide100-7round-20260925.json`
- `/tmp/lance-native-take-wide-fullscan-7round-20260925.json`
- `/tmp/lance-native-take-filter-7round-20260925.json`
- `/tmp/lance-take-ab-wide-fullscan-7round-20260925.json`
- `/tmp/lance-take-ab-filter-7round-20260925.json`

### 9.8 超宽 take 的 request-scoped codec 生命周期

v2.0 单大 Zstd frame 无法随机跳到解压后偏移。顺序 scan 必须跨 batch 保留单调 cursor，
否则每批都会从 frame 头重新解码；但 row-addressed take 的下一批地址没有单调保证，跨请求
保留 800 列 decoder 不仅不能提供稳定复用，还会让每个 `ZSTD_DCtx` 的 window workspace
长期驻留。新实现通过统一的 `NativeLanceDecoderStateRetention` 区分两种生命周期：

- `kScan` 保留原有跨 batch cursor，full scan 行为不变；
- `kRequest` 在一个逻辑列完成后释放其全部 physical page session；
- 释放粒度与有效解码并发度绑定，不增加 batch、readahead 或 decoded/decompressed cache；
- runtime stats 报告 active/peak reader 数、codec/input retained bytes 和 release 次数。

主验收数据 `/tmp/data-0ee2-v20-zstd9-full.lance`，800 列、100 个间隔 4,000 行的地址，
每端七轮奇偶交替：

| 线程 | 优化前 Native scan | 优化后 Native scan | 变化 | 优化前 RSS | 优化后 RSS | RSS 变化 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 10.501 s | **8.332 s** | **-20.7%** | 4.557 GiB | **0.087 GiB** | **-98.1%** |
| 16 | 2.127 s | **1.791 s** | **-15.8%** | 4.642 GiB | **0.353 GiB** | **-92.4%** |

两组核心 scan log-ratio exact sign-flip 均为 `p=0.015625`。最终 16T 运行中
`legacy_active_readers=0`，峰值 reader 数中位数为 34，峰值 codec/input 驻留约
157.5 MiB；1T 峰值 reader 数为 3，峰值驻留约 14.5 MiB。Native 16T 核心时间为
Rust 的约 0.38x，RSS 也低于 Rust 的 0.447 GiB。

800 列 full scan 仍使用 `kScan`。最终 1T 七轮中位数为 21.684 秒，对比原基线
21.916 秒没有回退；16T 跨时段结果为 7.200 秒。为排除环境波动，用同一源码分别构建
fixed/adaptive 二进制做同机七轮交替 A/B，差异为 `+1.85%`、`p=0.984375`，没有可检测
回退。

另外验证了两个并行方案但未保留：动态列任务抢占破坏文件列局部性，full-types smoke
从约 0.748 秒回退到 0.852 秒；physical-branch 并行使单 Struct 的 16T 中位数提升
22.9%，但严格同机 A/B 显示 1T 回退 3.03%（`p=0.015625`）。后者必须在不改变串行
函数代码布局的独立 decode-session/task graph 中实现后再合入。对 v2.0 长 frame 做
跨 batch 硬预算 LRU 也不成立：当列数超过预算时会在每个 batch 逐列淘汰，并从 frame
头重复解码，复杂度退化；通用解法需要 writer 侧 reset point/更细 page，不能在 reader
中用缓存或特判掩盖。

本轮新增结果：

- `/tmp/lance-final-session-wide-take-t1-7round.json`
- `/tmp/lance-final-session-wide-take-t16-7round.json`
- `/tmp/lance-final-wide-fullscan-t1-7round.json`
- `/tmp/lance-final-wide-fullscan-t16-7round.json`
- `/tmp/lance-wide-fullscan-adaptive-ab.json`
- `/tmp/lance-struct-branch-ab.json`
- `/tmp/lance-struct-branch-fastpath2-ab-t1.json`

## 10. 复现与验证

原始 JSON 位于 `/tmp/lance-perf-report-20260924/`。paired runner：

```bash
python3 bolt/dwio/lance/tests/run_native_rust_acceptance_benchmark.py \
  --dataset /tmp/data-0ee2-v20-zstd9-full.lance \
  --native-benchmark \
    _build/NativeWithFfi/bolt/dwio/lance/tests/bolt_dwio_native_lance_reader_benchmark \
  --rust-benchmark \
    /tmp/bolt-current-rust-lance-benchmark-comprehensive/target/release/bolt-current-rust-lance-benchmark \
  --output /tmp/lance-perf-report-20260924/wide_full_scan_800.json \
  --rounds 7 --batch-size 4096 --threads 16 --columns 800 \
  --scenario full_scan
```

本轮验证结果：

- Native Lance unit tests：129/129 通过；
- Native Lance TableScan tests：7/7 通过；
- 47 列数据生成、全量 materialization、过滤输出行数和 checksum 校验通过；
- take 的 1 行、100 行以及 800 列 100 行输出数量校验通过，前两者同时校验
  `row_id` checksum；take 与顺序读取还在 v2.0/v2.1/v2.2 的 scalar、semantic、
  dictionary、null 和 nested type matrix 上逐行相等；
- benchmark C++ target 和 Rust current-main harness 均使用 release build。
