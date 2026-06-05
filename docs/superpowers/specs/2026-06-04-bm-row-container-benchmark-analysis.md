# BmRowContainer Benchmark 性能分析

日期：2026-06-05

本文基于 `BmRowContainer::extractColumn` block-level fast path 实现后的
`bolt_bm_row_container_benchmark --bm_row_container_data_gib=1` 结果，以及
`log.txt` 中的 stats，分析当前 `RowContainer` 与 `BmRowContainer` 在写入、
spill 写出、内存读回、spill 读回四类场景下的性能差异，并整理下一步可行优化措施。

## Benchmark 口径

运行命令：

```bash
_build/Release/bolt/exec/bm/benchmarks/bolt_bm_row_container_benchmark \
  --bm_row_container_data_gib=1 \
  2>log.txt
```

关键口径：

- 每个 dataset 逻辑输入大小为 1GiB。
- RowContainer 使用 row-based spiller，spill 压缩为 `CompressionKind_ZSTD`。
- BmRowContainer 使用 BufferManager block spill，压缩为 `kZstdFrame`。
- `varchar_payload` 使用确定性伪随机 1024 字节字符串，不再是单一重复字符。
- ReadMemory / ReadSpill 均读取全部列：
  - `fixed_int64`：读 `BIGINT`
  - `mixed_fixed`：读 `INTEGER, BIGINT, DOUBLE, BOOLEAN`
  - `varchar_payload`：读 `BIGINT key, VARCHAR payload`
- BmRowContainer ReadMemory / ReadSpill 已使用 block-level extract fast path：
  - row block base pointer 按连续 block group resolve。
  - 不再构造 `rowPtrs` 中间数组。
  - 不再逐行调用 `pinRow`。
  - VARCHAR / VARBINARY 额外缓存最近 heap block base pointer。

## 最新结果

| Dataset | 场景 | RowContainer | BmRowContainer | BM 相对表现 |
| --- | ---: | ---: | ---: | ---: |
| fixed_int64 | Write | 3.32s | 6.07s | 慢 1.83x |
| fixed_int64 | Spill | 5.48s | 3.14s | 快 1.75x |
| fixed_int64 | ReadMemory | 208.95ms | 1.85s | 慢 8.85x |
| fixed_int64 | ReadSpill | 2.30s | 3.02s | 慢 1.31x |
| mixed_fixed | Write | 3.06s | 4.82s | 慢 1.58x |
| mixed_fixed | Spill | 4.89s | 4.49s | 快 1.09x |
| mixed_fixed | ReadMemory | 376.74ms | 1.05s | 慢 2.79x |
| mixed_fixed | ReadSpill | 1.81s | 2.75s | 慢 1.52x |
| varchar_payload | Write | 2.92s | 3.18s | 慢 1.09x |
| varchar_payload | Spill | 3.81s | 1.30s | 快 2.93x |
| varchar_payload | ReadMemory | 418.22ms | 451.46ms | 慢 1.08x |
| varchar_payload | ReadSpill | 1.23s | 1.69s | 慢 1.37x |

## Fast Path 前后对比

上一组全列读取结果中，BmRowContainer 读路径为：

| Dataset | 场景 | Fast Path 前 | Fast Path 后 | 变化 |
| --- | ---: | ---: | ---: | ---: |
| fixed_int64 | ReadMemory | 2.57s | 1.85s | 快 28.0% |
| fixed_int64 | ReadSpill | 3.73s | 3.02s | 快 19.0% |
| mixed_fixed | ReadMemory | 2.19s | 1.05s | 快 52.1% |
| mixed_fixed | ReadSpill | 3.90s | 2.75s | 快 29.5% |
| varchar_payload | ReadMemory | 469.35ms | 451.46ms | 快 3.8% |
| varchar_payload | ReadSpill | 1.71s | 1.69s | 快 1.2% |

结论：

- block-level extract fast path 已经生效，fixed/mixed 的内存读和 spill 读均有明显改善。
- fixed/mixed 的收益来自消除逐行 `pinRow`、`pinnedBlockDataAfterPressure` 判断和
  `rowPtrs` 构造。
- varchar_payload 收益很小，原因是行数只有约 104 万行，主要成本在字符串 payload
  拷贝、压缩/解压和 IO；减少 row block resolve 的收益有限。
- fast path 后 BM 读路径仍慢于 RowContainer，说明剩余瓶颈已经从 Pin/resolve 转移到
  per-row extract CPU 和 block layout 访问成本。

## Stats 观察

### Write 写入

最新结果：

| Dataset | RowContainer | BmRowContainer | BM 相对表现 |
| --- | ---: | ---: | ---: |
| fixed_int64 | 3.32s | 6.07s | 慢 1.83x |
| mixed_fixed | 3.06s | 4.82s | 慢 1.58x |
| varchar_payload | 2.92s | 3.18s | 慢 1.09x |

分析：

- fixed_int64 和 mixed_fixed 下，BmRowContainer 写入明显慢于 RowContainer。
  这说明当前 BmRowContainer 的写入路径仍有较重的 per-row/per-column CPU 开销。
- fixed_int64 是最差场景，BM 慢 1.83x。这个场景只有一个 BIGINT 列，理论上写入应接近
  顺序内存填充；现在慢很多，说明主要成本不是数据量复杂度，而是写入路径上的通用逻辑：
  - 每行 `newRow()` 都要检查/维护 row block。
  - 每列 `store()` 都会通过 `DecodedVector`、type dispatch、null bit 处理写入。
  - 即使没有变长列，也会走 BmRowContainer 的通用 row 初始化和 column store 流程。
  - BufferManager block 的 active data 访问和 pressure-aware arena 状态维护也比
    RowContainer 的 `AllocationPool` 直接 append 更重。
- mixed_fixed 慢 1.58x，列更多但行数比 fixed_int64 少，因此总劣势小于 fixed_int64。
  这说明写入成本不只是“列数越多越慢”，行数级的 `newRow/store` 固定开销非常关键。
- varchar_payload 只慢 1.09x，接近持平。原因是行数只有约 104 万，单行 payload 约
  1024 字节，主要成本转向字符串拷贝和 heap block append；BmRowContainer 的额外
  row-level 开销被大 payload 拷贝摊薄。

当前判断：

- BM write 的问题主要出现在高行数、小 row、fixed-width 场景。
- 这和 ReadMemory 的短板一致：只要每行数据很小，BmRowContainer 的 per-row 框架成本就会被放大。
- varchar_payload 写入接近 RowContainer，说明变长 payload append 设计本身没有暴露出明显劣势。

后续优化方向：

- 增加 batch store 入口，避免每行每列反复调用公开 `store()`。
- 对 fixed-width columns 做 typed batch store：
  - row allocation 仍逐行推进，但 column 写入在同一个 decoded vector 上 tight loop。
  - 直接写 row block raw memory，减少 `storeDispatch` 和 `DecodedVector::valueAt` 的重复成本。
- 对无 null fixed-width 数据增加 null-free store 路径：
  - 初始化 row null bits 后，不再每列每行反复写 `row[nullByte] &= ~nullMask`。
  - 有 null 的列再走 nullable fallback。
- 对 `newRow()` 做 block-local 批量分配：
  - 一次确认当前 block 可容纳多少 rows。
  - 在 batch store 中顺序初始化这些 rows，减少每行 `ensureWritableRowBlock()` 检查。

### Spill 写出

fixed_int64：

```text
RowContainer:
input_bytes=1207968776
spilled_bytes=140803703
rows=134217728
total_us=5476537
fill_us=1100685
serialization_us=1267782
flush_us=2998043
write_us=70513

BmRowContainer:
spill_write_count=511
spill_write_bytes=2143289344
spill_physical_write_bytes=140558469
compression_us=3084179
reclaimed_bytes=2143289344
```

mixed_fixed：

```text
RowContainer:
input_bytes=1124880970
spilled_bytes=281965150
rows=51130563
total_us=4892170
fill_us=421073
serialization_us=471963
flush_us=3856031
write_us=127917

BmRowContainer:
spill_write_count=292
spill_write_bytes=1224736768
spill_physical_write_bytes=272369430
compression_us=4475123
reclaimed_bytes=1224736768
```

varchar_payload：

```text
RowContainer:
input_bytes=984894351
spilled_bytes=813153472
rows=1040447
total_us=3813688
fill_us=9670
serialization_us=98365
flush_us=3315029
write_us=390091

BmRowContainer:
spill_write_count=235
spill_write_bytes=985661440
spill_physical_write_bytes=796762045
compression_us=1280125
reclaimed_bytes=985661440
```

分析：

- BM spill 写出在 fixed_int64 和 varchar_payload 上优势明显：
  - fixed_int64 快 1.75x。
  - varchar_payload 快 2.93x。
- mixed_fixed 只快 1.09x，主要因为 BM 的 `compression_us=4.48s`，几乎等于
  BmRowContainerSpill 总耗时 `4.49s`。
- BM spill 写出不是 IO bound：
  - fixed physical write 约 140MB，但总耗时 3.14s，主要在压缩。
  - mixed physical write 约 272MB，但 `compression_us=4.48s`。
  - varchar physical write 约 797MB，`compression_us=1.28s`，压缩比低但压缩 CPU
    更可控。
- RowContainer row-based spill 的 `flush_us` 很高，包含压缩、buffer flush、spiller
  写出调度等成本；`write_us` 本身不高，说明也不是裸写 IO 主导。

### ReadMemory

BM stats：

```text
fixed_int64:
allocated_blocks=512
pin_count=0
pinned_resident_bytes=2147483648

mixed_fixed:
allocated_blocks=293
pin_count=0
pinned_resident_bytes=1228931072

varchar_payload:
allocated_blocks=237
pin_count=0
pinned_resident_bytes=994050048
```

分析：

- ReadMemory 下 BM 没有 spill read、没有解压、没有 Pin：`pin_count=0`。
- fast path 后 fixed/mixed 已改善，但仍明显慢于 RowContainer：
  - fixed_int64 慢 8.85x。
  - mixed_fixed 慢 2.79x。
- 因为 IO 和 Pin 都不在路径上，剩余差距主要来自 CPU：
  - 每行通过 `RowId` 计算 `rowPtr`。
  - 每行读取 null byte。
  - 每行调用 `FlatVector::setNull`。
  - 每行调用 `FlatVector::set` 或 `setStringViewValue`。
  - BmRowContainer row block 存储的是 row-oriented layout，不利于全列连续读取。
- varchar_payload 只慢 1.08x，说明在低行数、大 payload 场景下，当前 fast path 已经接近
  RowContainer；继续优化 fixed-width 高行数场景优先级更高。

### ReadSpill

fixed_int64：

```text
RowContainer:
read_rows=134217728
read_us=2016258
decompress_us=853681
read_io_us=19393

BmRowContainer.after_read:
pin_count=511
pin_read=511
spill_read_bytes=2143289344
spill_physical_read_bytes=140558469
decompression_us=1333468
pinned_resident_bytes=2147483648
spilled_bytes=0
```

mixed_fixed：

```text
RowContainer:
read_rows=51130563
read_us=1697897
decompress_us=1211247
read_io_us=39702

BmRowContainer.after_read:
pin_count=292
pin_read=292
spill_read_bytes=1224736768
spill_physical_read_bytes=272369419
decompression_us=1596090
pinned_resident_bytes=1228931072
spilled_bytes=0
```

varchar_payload：

```text
RowContainer:
read_rows=1040447
read_us=1231016
decompress_us=1052057
read_io_us=117991

BmRowContainer.after_read:
pin_count=235
pin_read=235
spill_read_bytes=985661440
spill_physical_read_bytes=796762045
decompression_us=1178568
pinned_resident_bytes=994050048
spilled_bytes=0
```

分析：

- BM ReadSpill 的 Pin 已经是 block 数级别：
  - fixed_int64：511 次。
  - mixed_fixed：292 次。
  - varchar_payload：235 次。
- 这说明 fast path 和 arena pinned state 已经避免了行数级 Pin。
- BM ReadSpill 仍慢于 RowContainer，原因主要是两部分：
  - 解压略慢：fixed `1.33s vs 0.85s`，mixed `1.60s vs 1.21s`，
    varchar `1.18s vs 1.05s`。
  - 解压后 extract CPU 仍较重，尤其 fixed/mixed 高行数场景。
- fixed ReadSpill 相比 ReadMemory 只多约 `1.17s`，与 BM `decompression_us=1.33s`
  接近，说明 fixed 的 ReadSpill 已经基本由“解压 + ReadMemory extract”组成。
- varchar ReadSpill 物理读约 797MB，最终 1.69s，瓶颈更接近大 payload 解压和字符串复制。

## 当前结论

1. BM spill 写出方向是成立的。
   fixed/varchar 明显快于 RowContainer，mixed 接近持平。

2. block-level extract fast path 有效。
   fixed/mixed 的 ReadMemory 和 ReadSpill 均明显下降，证明之前的逐行 `pinRow` /
   `rowPtrs` 成本确实存在。

3. fast path 后，`pinRow` 已经不是主要瓶颈。
   stats 显示 ReadMemory `pin_count=0`，ReadSpill `pin_count` 为 block 数级别。

4. 当前最大短板仍是读路径 CPU。
   fixed_int64 ReadMemory 仍慢 8.85x，mixed_fixed ReadMemory 仍慢 2.79x；这不是 IO
   或 BufferManager Pin 问题，而是 row-oriented extract 的 per-row CPU 问题。

5. Write 路径同样暴露出 fixed-width 高行数短板。
   fixed_int64 Write 慢 1.83x，mixed_fixed Write 慢 1.58x；这和 ReadMemory 的问题一致，
   都指向高行数小 row 场景下 per-row 通用逻辑过重。

6. varchar_payload 的读写路径都已接近 RowContainer。
   ReadMemory 只慢 1.08x，ReadSpill 慢 1.37x；继续优化优先级低于 fixed/mixed。

## 可行优化措施

### 1. 固定宽度列读取使用 raw values + nulls 批量写入

当前 fast path 仍逐行调用：

```cpp
flatResult->setNull(output, isNull);
flatResult->set(output, value);
```

这会在高行数场景产生大量函数调用和分支。可以对 fixed-width primitive 增加更低层的写法：

- 通过 `flatResult->mutableRawValues()` 获取输出 values 指针。
- 通过 result null buffer 直接设置 null bit。
- 非 null 时直接写 `rawValues[output] = value`。

预期收益：

- 主要改善 fixed_int64 和 mixed_fixed ReadMemory。
- 对 ReadSpill 也有收益，因为 ReadSpill 解压后仍要走同一套 extract。

注意点：

- 必须保证 `FlatVector` values/nulls buffer 已 materialize。
- null bit 语义要和 `BaseVector::setNull` 完全一致。
- BOOLEAN 可能有特殊表示，需要单独确认 `FlatVector<bool>` 的 raw value API。
- 第一版可以先覆盖 BIGINT / INTEGER / DOUBLE / REAL / TINYINT / SMALLINT，BOOLEAN
  单独保留现有路径。

### 2. 固定宽度列按 block 内连续 rowOffset 做小批量 copy

当前 row 是 row-oriented layout，单列值不是完全连续的，但 append 顺序下同一个 block 内
`rowOffset` 通常是固定 stride：`fixedRowSize_`。可以在连续 block group 中识别：

```text
rows[i + 1].blockId == rows[i].blockId
rows[i + 1].rowOffset == rows[i].rowOffset + fixedRowSize_
```

如果连续，可以进入 stride loop：

```cpp
const char* valuePtr = blockBase + firstRowOffset + column.offset();
for each output:
  rawValues[out] = *reinterpret_cast<const T*>(valuePtr);
  valuePtr += fixedRowSize_;
```

预期收益：

- 减少每行 `rowBlockData + row.rowOffset + column.offset()` 的重复计算。
- 让编译器更容易优化 tight loop。
- 对 append-order 的 WindowBuild 主路径最有效。

注意点：

- null 仍需按 row stride 读取 null byte。
- rows 乱序时 fallback 到当前 fast path。
- 这个优化可以和措施 1 合并实现。

### 3. 增加 null-free 快路径

很多 benchmark dataset 没有 null。当前每行仍会做：

```cpp
isNullAt(rowPtr, column.nullByte(), column.nullMask())
flatResult->setNull(output, isNull)
```

可以在 store 阶段维护每列是否出现过 null：

```cpp
std::vector<bool> mayHaveNulls_;
```

当某列从未存过 null 时，extract 可以跳过 null 检查和 `setNull(false)`：

```cpp
if (!mayHaveNulls_[columnIndex]) {
  copy values only;
}
```

预期收益：

- 对 fixed_int64 非常直接。
- 对 mixed_fixed 也有收益，取决于 benchmark 数据是否包含 null。

注意点：

- 需要把 `BmRowColumn` 或 fast path 入参扩展出 column index，才能访问
  `mayHaveNulls_`。
- 一旦某列 store 过 null，该列后续永久走 nullable 路径即可，不需要恢复。
- result vector 可能复用带旧 nulls 的 buffer，null-free 路径必须确保输出区间 nulls
  被清理，或者证明 BaseVector 创建时 nulls 为空且 benchmark 不复用旧 result。更稳妥的
  做法是批量 clear output null bits，而不是逐行 `setNull(false)`。

### 4. ReadSpill 增加 block 级批量 Pin / 预取

当前 ReadSpill fast path 是遇到 block 才 pin：

```cpp
base = pinnedBlockDataAfterPressure(blockId)
```

对于顺序 rows，可以先扫描一小段 rows，收集即将访问的 block ids，调用 BufferManager
已有 batch/prefetch 能力，让 IO 和解压提前发生。

预期收益：

- 改善 ReadSpill，尤其 fixed/mixed 多 block 顺序读。
- 对 ReadMemory 无影响。

注意点：

- 不能把过多 blocks 一次性 pin 回内存，否则会破坏压力驱动策略。
- 需要与 `BmPressureAwareBlockArena` 的 make-reclaimable 机制配合，避免预取把可回收
  block 长时间 pin 住。
- 第一版可以只做小窗口，例如 8 或 16 个 block。

### 5. Spill 压缩策略按数据形态调优

当前 BM spill 写出在 mixed_fixed 下几乎被 `compression_us` 吃满：

```text
mixed_fixed_BmRowContainerSpill:
time = 4.49s
compression_us = 4.48s
```

可以增加 benchmark 对比：

- ZSTD 当前策略。
- LZ4。
- 不压缩。
- 不同 block size。

预期收益：

- 如果 mixed_fixed 使用 LZ4 或不压缩能显著降低 spill time，并且物理写 IO 可接受，
  可以考虑让 BM spill 压缩策略按场景配置。

注意点：

- 不能只看 Spill 写出，还要同时看 ReadSpill。
- fixed_int64 ZSTD 压缩比极高，切到 LZ4/None 可能让物理 IO 急剧上升。
- varchar_payload 压缩比低，但当前 BM 已经很快，需要谨慎评估收益。

### 6. 增加 fixed-width batch store 快路径

当前 Write 的 fixed/mixed 劣势说明写入端也需要 fast path。可以在 `store(RowVectorPtr)`
内部增加按列类型分发的 batch store，而不是完全依赖每行每列的公开 `store()`：

```cpp
rows = allocateRows(input->size());
for each column:
  storeColumnBatch(decoded[column], rows, column);
```

fixed-width batch store 的核心是：

- 一次 decode 一个输入 vector。
- 对 rows 做顺序 tight loop。
- 直接写 `blockBase + rowOffset + column.offset()`。
- null-free 列跳过 per-row null bit 更新。
- nullable 列才检查 `decoded.isNullAt(i)` 并设置 null bit。

预期收益：

- 主要改善 fixed_int64 Write 和 mixed_fixed Write。
- 对 WindowBuild 的批量 ingest 更贴近实际调用方式。

注意点：

- 公开 `newRow()` / `store()` 仍保留给逐行场景，不改变现有语义。
- batch store 需要处理 rows 跨 row block 的情况。
- VARCHAR batch store 可以后续再做，第一版先覆盖 fixed-width。

## 建议优先级

1. 优先做“固定宽度读取 raw values + nulls 批量写入”。
   这是当前 ReadMemory 最大差距的直接来源，风险相对可控。

2. 同时做“连续 rowOffset stride loop”。
   它和 raw values 写入天然可以放在同一个 fixed-width fast path 内。

3. 然后做 fixed-width batch store。
   Write fixed/mixed 的劣势同样明显，batch store 可以直接降低高行数小 row 的写入成本。

4. 再做 null-free 快路径。
   它同时服务 read 和 write；如果 benchmark 和实际 WindowBuild 数据常见无 null，
   收益会很直接。

5. ReadSpill 的 batch pin / prefetch 放在下一轮。
   当前 ReadSpill 已经降到 block 级 pin，继续优化需要更仔细处理内存压力和预取窗口。

6. Spill 压缩策略调优单独开 benchmark。
   它影响写 spill 和读 spill 两端，不建议和 extract CPU 优化混在一个改动里。
