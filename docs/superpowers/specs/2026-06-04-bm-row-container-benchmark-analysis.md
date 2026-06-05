# BmRowContainer Benchmark 性能分析

日期：2026-06-05

本文基于最新 `bolt_bm_row_container_benchmark --bm_row_container_data_gib=1`
结果和 `log.txt` 中的 stats，分析 `RowContainer` 与 `BmRowContainer`
在写入、spill 写出、内存读回、spill 读回四类场景下的差异，并给出
`BmRowContainer` 读路径 fast path 的完整优化方案。

## 当前 Benchmark 口径

运行命令：

```bash
_build/Release/bolt/exec/bm/benchmarks/bolt_bm_row_container_benchmark \
  --bm_row_container_data_gib=1 \
  2>log.txt
```

当前 benchmark 的关键口径：

- 每个 dataset 逻辑输入大小为 1GiB。
- RowContainer 使用 row-based spiller，`rowBasedSpillMode=COMPRESSION`。
- RowContainer spill 压缩为 `CompressionKind_ZSTD`。
- BmRowContainer 使用 BufferManager block spill，压缩为 `kZstdFrame`。
- `varchar_payload` 使用确定性伪随机 1024 字节字符串。
- ReadMemory / ReadSpill 已改为读取全部列：
  - `fixed_int64`：读 `BIGINT`
  - `mixed_fixed`：读 `INTEGER, BIGINT, DOUBLE, BOOLEAN`
  - `varchar_payload`：读 `BIGINT key, VARCHAR payload`

因此，最新 `varchar_payload` ReadSpill 不再是只读 key 的列裁剪场景，而是会读回
row blocks 和 heap payload blocks。

## 最新 Benchmark 结果

| Dataset | 场景 | RowContainer | BmRowContainer | BM 相对表现 |
| --- | ---: | ---: | ---: | ---: |
| fixed_int64 | Write | 3.37s | 5.82s | 慢 1.73x |
| fixed_int64 | Spill | 5.42s | 3.20s | 快 1.69x |
| fixed_int64 | ReadMemory | 209.52ms | 2.57s | 慢 12.27x |
| fixed_int64 | ReadSpill | 2.30s | 3.73s | 慢 1.62x |
| mixed_fixed | Write | 2.87s | 4.68s | 慢 1.63x |
| mixed_fixed | Spill | 4.81s | 4.50s | 快 1.07x |
| mixed_fixed | ReadMemory | 380.19ms | 2.19s | 慢 5.76x |
| mixed_fixed | ReadSpill | 1.82s | 3.90s | 慢 2.14x |
| varchar_payload | Write | 2.94s | 3.18s | 慢 1.08x |
| varchar_payload | Spill | 3.87s | 1.31s | 快 2.95x |
| varchar_payload | ReadMemory | 416.89ms | 469.35ms | 慢 1.13x |
| varchar_payload | ReadSpill | 1.23s | 1.71s | 慢 1.39x |

## Stats 关键观察

### Spill 写出

fixed_int64：

```text
RowContainer:
input_bytes=1207968776
spilled_bytes=140803703
rows=134217728
total_us=5416499
fill_us=1099767
serialization_us=1255142
flush_us=2949689
write_us=71949

BmRowContainer:
spill_write_count=511
spill_write_bytes=2143289344
spill_physical_write_bytes=140558469
compression_us=3153285
reclaimed_bytes=2143289344
```

mixed_fixed：

```text
RowContainer:
input_bytes=1124880970
spilled_bytes=281965150
rows=51130563
total_us=4812218
fill_us=423006
serialization_us=467506
flush_us=3775300
write_us=130644

BmRowContainer:
spill_write_count=292
spill_write_bytes=1224736768
spill_physical_write_bytes=272369430
compression_us=4490888
reclaimed_bytes=1224736768
```

varchar_payload：

```text
RowContainer:
input_bytes=984894351
spilled_bytes=813153472
rows=1040447
total_us=3871881
fill_us=8924
serialization_us=97407
flush_us=3381943
write_us=383092

BmRowContainer:
spill_write_count=235
spill_write_bytes=985661440
spill_physical_write_bytes=796762045
compression_us=1282525
reclaimed_bytes=985661440
```

结论：

- BM spill 写出方向成立。
- fixed_int64 下 BM 快 1.69x，主要来自省掉 row-based fill/serialization。
- mixed_fixed 下 BM 只快 1.07x，因为 BM 的 ZSTD `compression_us=4.49s`，几乎等于总耗时。
- varchar_payload 下 BM 快 2.95x，主要来自 BM block 压缩路径明显快于 RowContainer row-based flush。
- BM spill 写出阶段主要瓶颈是压缩 CPU，不是物理写 IO。

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

结论：

- ReadMemory 没有 spill read，没有解压，也没有 pin：`pin_count=0`。
- 读路径慢不是 BufferManager IO 或 Pin 成本导致的，而是 `BmRowContainer::extractColumn`
  的 CPU 开销。
- fixed_int64 慢 12.27x，mixed_fixed 慢 5.76x，都是高行数 fixed-width 场景。
- varchar_payload 全列读取后只慢 1.13x，说明行数较少时 per-row 开销没那么突出。

### ReadSpill

fixed_int64：

```text
RowContainer:
read_rows=134217728
read_us=1996858
decompress_us=829343
read_io_us=19210

BmRowContainer.after_read:
pin_count=511
pin_read=511
spill_read_bytes=2143289344
spill_physical_read_bytes=140558469
decompression_us=1150464
pinned_resident_bytes=2147483648
spilled_bytes=0
```

mixed_fixed：

```text
RowContainer:
read_rows=51130563
read_us=1702056
decompress_us=1215958
read_io_us=38231

BmRowContainer.after_read:
pin_count=292
pin_read=292
spill_read_bytes=1224736768
spill_physical_read_bytes=272369707
decompression_us=1620608
pinned_resident_bytes=1228931072
spilled_bytes=0
```

varchar_payload：

```text
RowContainer:
read_rows=1040447
read_us=1222064
decompress_us=1052770
read_io_us=109456

BmRowContainer.after_read:
pin_count=235
pin_read=235
spill_read_bytes=985661440
spill_physical_read_bytes=796762045
decompression_us=1177826
pinned_resident_bytes=994050048
spilled_bytes=0
```

结论：

- 全列读取后，varchar_payload 也读回全部 BM blocks：`pin_read=235`，
  `spill_read_bytes=985661440`，`spilled_bytes=0`。
- `pin_count` 已经是 block 数级别，不再是行数级别。
- 因此 fixed/mixed ReadSpill 的剩余慢点不是每行 `BufferManager::Pin`，而是每行
  `RowId -> block state -> base pointer -> row pointer`、null 检查、`rowPtrs`
  构造和 extract 循环本身。
- varchar_payload ReadSpill 现在慢 1.39x，之前只读 key 时的巨大优势已经消失；
  当前结果才代表全量 payload 读回性能。

## 目前暴露出的核心问题

### 1. ReadMemory 是最大短板

ReadMemory 没有 IO、没有解压、没有 pin，但 fixed_int64 仍慢 12.27x，mixed_fixed
慢 5.76x。这说明当前 `extractColumn` 的基础 CPU 路径不够好。

当前大致路径是：

```cpp
for row in rows:
  rowPtr = pinRow(row)
  rowPtrs.push_back(rowPtr)

extractDispatch(rowPtrs, column, result)
```

问题：

- `pinRow` 每行都会做 `blocks_.block(row.blockId)`。
- 每行都会进入 `pinnedBlockDataAfterPressure` / `tryPinnedData` 判断。
- 每行都有边界检查和 row pointer 构造。
- `extractColumn` 先构造 `std::vector<const char*> rowPtrs`，再二次遍历 rowPtrs。
- fixed-width 高行数场景会把这些 per-row 开销放大到秒级。

### 2. ReadSpill 已解决 Pin 次数问题，但仍有 per-row CPU 开销

引入 block-level pinned handle 后：

- fixed_int64 `pin_count=511`
- mixed_fixed `pin_count=292`
- varchar_payload `pin_count=235`

这说明每个 block 只 read/pin 一次，方向是对的。但 ReadSpill 仍慢于 RowContainer，
说明下一步必须优化 block 已经 pinned 之后的 extract CPU 路径。

### 3. Spill 写出主要受压缩影响

BM spill 写出总体有价值，但 mixed_fixed 优势很小。stats 显示 BM 的 spill 总时间基本
等于 `compression_us`，后续如果要继续优化 spill 写出，需要单独评估 ZSTD/LZ4/None
或压缩 block layout，而不是继续在 write path 上猜。

## 解决方案：Block-Level Extract Fast Path

可以一步实现完整版本，不必分阶段。目标是让 `BmRowContainer::extractColumn` 在支持的
类型上直接按 block 批量读取，绕过当前 rowPtrs + per-row pin 路径。

### 总体思路

当前路径是行粒度 resolve：

```cpp
for row in rows:
  rowPtr = pinRow(row)
  rowPtrs.push_back(rowPtr)
extractColumnTyped(rowPtrs, ...)
```

fast path 改为 block 粒度 resolve：

```cpp
while i < rows.size:
  blockId = rows[i].blockId
  blockBase = pinnedBlockDataAfterPressure(blockId)

  while i < rows.size && rows[i].blockId == blockId:
    row = blockBase + rows[i].rowOffset
    extract row[column] into result
    ++i
```

收益：

- 每个连续 block group 只 resolve 一次 base pointer。
- 不再构造 `rowPtrs` 中间数组。
- fixed-width 列直接从 `blockBase + rowOffset + column.offset()` 取值。
- null 也直接从 row 内 null byte 读取。
- ReadMemory 和 ReadSpill 共享收益。

### 新增接口

在 `BmRowContainer` 中新增私有入口。这里不额外引入能力判断 helper；直接在
`extractColumn` 中展开类型判断，加注释说明哪些类型暂时走 generic fallback。

```cpp
void extractColumnFast(
    TypeKind kind,
    folly::Range<const RowId*> rows,
    BmRowColumn column,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize);

template <TypeKind Kind>
void extractColumnFastTyped(
    folly::Range<const RowId*> rows,
    BmRowColumn column,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize);
```

`extractColumn(...)` 的结构调整为：

```cpp
void BmRowContainer::extractColumn(...) {
  validate inputs;

  switch (typeKinds_[column]) {
    // 复杂类型第一版仍走 generic 路径，避免在 fast path 中重新实现嵌套结构语义。
    case TypeKind::UNKNOWN:
    case TypeKind::OPAQUE:
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
    case TypeKind::VARIANT:
      break;
    default:
      extractColumnFast(
          typeKinds_[column],
          rows,
          rowColumns_[column],
          resultOffset,
          result,
          exactSize);
      return;
  }

  // fallback：保留当前 rowPtrs + extractDispatch 路径
}
```

进入 `extractColumnFast` 后，表示该类型必须由 fast path 完整处理；如果复杂类型意外进入
fast path，直接 `BOLT_NYI`，这是实现错误。

### Fixed-Width Fast Path

固定宽度 primitive 类型直接支持：

- BOOLEAN
- TINYINT / SMALLINT / INTEGER / BIGINT
- REAL / DOUBLE
- DATE / TIMESTAMP 等已有 `TypeTraits<Kind>::NativeType` 支持的 fixed-width 类型

逻辑：

```cpp
template <TypeKind Kind>
void extractColumnFastTyped(...) {
  if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
    extractStringColumnFast(...);
    return;
  }

  if constexpr (is unsupported complex type) {
    BOLT_NYI("BmRowContainer fast extract does not support type {}", ...);
  }

  using T = typename TypeTraits<Kind>::NativeType;
  auto* flat = result->asFlatVector<T>();
  BOLT_CHECK_NOT_NULL(flat);

  for each contiguous block group:
    base = pinnedBlockDataAfterPressure(blockId, ...);
    for each row in group:
      rowPtr = base + row.rowOffset;
      isNull = isNullAt(rowPtr, column.nullByte(), column.nullMask());
      flat->setNull(resultIndex, isNull);
      if (!isNull) {
        flat->set(resultIndex, *reinterpret_cast<const T*>(
            rowPtr + column.offset()));
      }
}
```

注意点：

- `resultOffset` 必须参与 result index 计算。
- `exactSize` 对 fixed-width 没有特殊含义，可以忽略。
- 仍然保留每行 `rowOffset + fixedRowSize_ <= block.usedBytes` 的 debug check；
  如果担心性能，可以只在 debug 或 `BOLT_DCHECK` 风格下保留。

### VARCHAR / VARBINARY Fast Path

全列读取后，varchar_payload 的 ReadMemory/ReadSpill 也进入真实 payload 读回，因此
VARCHAR 也应该纳入 fast path。

VARCHAR 的 row block 中存 `VarData`：

```cpp
struct VarData {
  uint32_t blockId;
  uint32_t offset;
  uint32_t size;
};
```

fast path：

```cpp
for each contiguous row block group:
  rowBase = pinnedBlockDataAfterPressure(rowBlockId, ...)

  uint32_t cachedHeapBlockId = invalid;
  const char* cachedHeapBase = nullptr;

  for row in group:
    rowPtr = rowBase + row.rowOffset
    if null:
      result.setNull(...)
      continue

    ref = *reinterpret_cast<const VarData*>(rowPtr + column.offset())
    if ref.size == 0:
      result.setStringViewValue(..., StringView("", 0), exactSize)
      continue

    if ref.blockId != cachedHeapBlockId:
      cachedHeapBase = pinnedBlockDataAfterPressure(ref.blockId, ...)
      cachedHeapBlockId = ref.blockId

    result.setStringViewValue(
        resultIndex,
        StringView(cachedHeapBase + ref.offset, ref.size),
        exactSize)
```

为什么需要 heap block cache：

- payload 写入通常是 append 顺序，同一批连续 rows 大概率落在同一个或相邻 heap block。
- cache 可以避免每个字符串都 resolve heap block。
- 即使 rows 跨 heap block，也只是每个 heap block resolve 一次或少数几次。

### 输入 rows 顺序假设

当前 benchmark 和 WindowBuild 的主路径通常是 append 顺序或接近 append 顺序，因此
`rows` 中连续 row 大概率同属一个 block。fast path 按“连续 block group”处理即可。

不建议第一版对 rows 做排序或重排：

- extract 必须保持输出顺序。
- 排序会引入额外索引映射和内存开销。
- 当前问题已能通过连续 block group 解决大部分高行数顺序读场景。

如果未来存在强随机访问模式，可以单独设计 block cache 或 prefetch 策略，不应混入第一版。

### Arena 侧接口要求

`BmPressureAwareBlockArena` 现有 `tryPinnedData/pinnedData` 能支撑 fast path，但建议新增
语义更明确的轻量接口：

```cpp
const char* resolveBlockData(uint32_t blockId);
```

或者直接在 `BmRowContainer` 中继续调用：

```cpp
pinnedBlockDataAfterPressure(blockId, failureMessage)
```

关键要求：

- fast path 每个 block group 调用一次，不要每行调用。
- `touch(lastAccess)` 只在 block 级 resolve 时发生，不要每行 touch。
- pin 失败仍沿用现有策略：外围批量 spill 可回收 blocks，再重试一次，失败就报错。

### Generic Fallback 策略

以下情况走当前 generic 路径：

- `extractColumn` switch 中明确排除的类型：ARRAY / MAP / ROW / OPAQUE / UNKNOWN / VARIANT。
- 后续新增但还没有明确 fast path 语义的类型。

未被 switch 排除的类型必须由 `extractColumnFast` 完整处理，包括 fixed-width primitive
和 VARCHAR / VARBINARY。

### 测试计划

需要补 `BmRowContainerTest`，不要只靠 benchmark：

1. fixed-width fast path：
   - 多个 row blocks。
   - 有 null。
   - `resultOffset != 0`。
   - 提取 BIGINT / INTEGER / DOUBLE / BOOLEAN。

2. VARCHAR fast path：
   - 多个 row blocks。
   - 多个 heap blocks。
   - null string、empty string、普通 string。
   - `exactSize=true/false` 至少覆盖一个。

3. spilled block readback：
   - 复用现有 spill skip 机制，io_uring 不可用时 skip。
   - 验证 spilled row blocks 和 heap blocks 读回后全列一致。

4. fallback：
   - 不支持复杂类型仍然 NYI 或走既有行为，不要 silent wrong result。

### Benchmark 预期

实现 fast path 后，预期变化：

- `fixed_int64_BmRowContainerReadMemory` 应显著下降，这是最主要收益点。
- `mixed_fixed_BmRowContainerReadMemory` 会下降，但全列读取有 4 列，仍会比 fixed 多扫描成本。
- `fixed_int64_BmRowContainerReadSpill` 和 `mixed_fixed_BmRowContainerReadSpill` 会继续下降，
  因为 pin/read 已是 block 级，剩余主要就是 per-row extract CPU。
- `varchar_payload_BmRowContainerReadMemory/ReadSpill` 可能小幅改善，收益取决于 heap block cache
  命中率和 `setStringViewValue` 成本。

### 风险和边界

- Fast path 会绕过当前通用 `extractColumnTyped(rowPtrs)`，必须保证 null 语义完全一致。
- VARCHAR 的 `StringView` 生命周期依赖 pinned heap block，fast path 中必须保证 heap block handle
  保持在 arena 中 pinned，不能用局部临时 handle。
- 如果 fast path 读取很多 spilled heap blocks，会把这些 blocks pin 回内存；这是当前全列读取语义下
  正确的行为，但需要继续依赖 BM 的内存压力机制和外围批量 spill 策略。
- 不要在 fast path 中做输出重排；输出顺序必须与输入 `rows` 顺序一致。

## 最终结论

最新全列读取 benchmark 修正了之前只读 key 的口径问题。现在可以确认：

1. BM spill 写出方向成立，fixed/varchar 有明显优势，mixed 接近持平。
2. BM ReadSpill 已经解决行数级 Pin 问题，`pin_count` 降到 block 数级别。
3. 当前最大瓶颈是 ReadMemory 和 ReadSpill 剩余的 per-row extract CPU 开销。
4. 下一步应实现 `BmRowContainer::extractColumn` 的 block-level fast path，一步覆盖
   fixed-width primitive 和 VARCHAR/VARBINARY，按 block resolve base pointer，避免每行
   `pinRow` 和 `rowPtrs` 构造。
