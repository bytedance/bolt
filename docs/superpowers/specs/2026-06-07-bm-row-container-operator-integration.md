# BmRowContainer 接入 StreamingWindowBuild 与 Window 计算重构

日期：2026-06-17

修订：2026-06-23。本文描述一阶段方案：`BmRowContainer` 承担 partition 输入行存储，Window 计算按 resident fast path 或 `ReadOnlyWindowReadSession` slow path 读取，aggregate 参数列通过 chunk materialize 控制 peak memory。

## 1. 目标

一阶段目标：

- 单个 SQL partition 的输入行进入 `BmRowContainer`，由 Bm/BufferManager 管理 resident 与 spill。
- 内存充足时使用 resident row pointer fast path，保留纯内存访问性能。
- 内存紧张时使用 `RowId + ReadOnlyWindowReadSession` slow path，按访问窗口加载 resident rows。
- Aggregate window 使用 chunk 输入模式，把 argVector peak 从 frame size 降到 batch size。
- Window 语义和现有实现保持一致，复杂 frame 使用 repeated-scan path。
- `IGNORE NULLS` 通过 `BmWindowPartition::extractNulls()` 提供等价语义。

## 2. 架构

新增 Bm 专用 build 与 partition：

```text
BmStreamingWindowBuild
  -> BmRowContainer
  -> PartitionDescriptor
  -> BmWindowPartition
```

Window 计算分两条路径：

```text
普通 WindowFunction:
  继续通过 WindowPartition 接口读取数据。
  BmWindowPartition 负责把 extract/compare/null/peer 请求映射到 Bm。

Aggregate WindowFunction:
  使用 BmAggregateWindowFunction。
  通过 BmWindowFrameReader 分块读取 frame 参数列。
  继续调用现有 Aggregate::addSingleGroupRawInput。
```

核心组件职责：

```text
BmStreamingWindowBuild
  在 Vector 层识别 SQL partition run。
  按 run 写入 BmRowContainer。
  在 partition boundary 处切换 Bm segment。

PartitionDescriptor
  记录一个 SQL partition 的 segment 列表和行数。

BmWindowPartition
  提供 WindowPartition 语义。
  选择 resident pointer fast path 或 RowId window-read slow path。

BmWindowFrameReader
  为 aggregate window 提供 chunk argVectors。

BmAggregateWindowFunction
  管理 accumulator。
  分块喂现有 Aggregate。
```

## 3. 写入流程

`BmStreamingWindowBuild` 在 input Vector 层先判断 SQL partition boundary，再写入 Bm。

```text
decode partition key columns
offset = 0

while offset < batchSize:
  find partition run [offset, boundary)
  write [offset, boundary) into BmRowContainer
  append active/flushed segment to current PartitionDescriptor

  if boundary < batchSize:
    close current partition
    start next partition
    offset = boundary
  else:
    record last written row pointer
    break
```

该流程让一个 Bm segment 只属于一个 SQL partition，读取阶段可以直接按 segment 列表加载。

## 4. Partition Boundary

输入已按 `{PARTITION BY keys + ORDER BY keys}` 排序，同一个 SQL partition 在 input 中是连续 run。

batch 内 boundary 查找：

- 比较当前 partition key 和 batch 最后一行 key。
- key 相等时，当前剩余 batch 归入同一个 partition run。
- key 变化时，在 batch 内查找第一个变化位置。
- 大 run 使用二分查找；短 run 使用线性扫描。

batch 内比较使用 decoded input Vector。

跨 batch 比较使用 copy-on-spill 设计：

- 写入 batch 末尾时记录最后写入 Bm 的 resident row pointer。
- 下一 batch 第一行通过 row-vs-vector comparator 和该 pointer 比较 `PARTITION BY` key。
- 热路径使用 row pointer 比较。
- 当最后一行所在 segment/chunk 进入 spill handoff，且该行仍用于下一次跨 batch 比较时，生成 last key snapshot。
- snapshot 用于完成下一次跨 batch 比较，比较完成后释放。

row-vs-vector comparator：

```text
left:  resident Bm row pointer 或 last key snapshot
right: input Vector row index
keys:  PARTITION BY columns
```

成本模型：

```text
常规 batch: pointer compare
spill handoff: copy last partition key once
```

## 5. Partition Descriptor

一个 SQL partition 对应一个 descriptor：

```cpp
struct PartitionDescriptor {
  std::vector<bm::SegmentId> segments;
  vector_size_t numRows{0};
  bool closed{false};
};
```

`BmStreamingWindowBuild` 在写入 run 时累加 `numRows`，在 segment flush 后记录 `SegmentId`。

## 6. BmWindowPartition 访问模式

`BmWindowPartition` 创建时选择访问模式：

```cpp
if (container.canBulkRead(segments)) {
  rowPtrs_ = container.beginBulkReadSegments(segments).loadRows();
  mode_ = kResidentPointers;
} else {
  readSession_ = container.beginReadOnlyWindowReadSegments(segments);
  rowIds_ = readSession_.listRowIds();
  mode_ = kRowIdWindowRead;
}
```

### kResidentPointers

内存充足时使用：

- 持有 `std::vector<char*> rowPtrs_`。
- `extractColumn` 直接调用 `extractColumnResident()`。
- `extractNulls` 直接读取 resident rows 的 null bit。
- `compare` 和 peer compare 直接使用 resident rows。
- partition 计算期间保持这些 segment resident。

### kRowIdWindowRead

内存紧张时使用：

- 持有 `std::vector<bm::RowId> rowIds_`。
- 按访问窗口调用 `ReadOnlyWindowReadSession::loadRows()`。
- 对加载出的 resident rows 执行 compare、extract、null bit 读取。
- batch 使用完成后调用 `evictLoadedChunks()` 释放当前 loaded chunks。

## 7. BmWindowPartition 接口

`BmWindowPartition` 提供现有 `WindowPartition` 所需语义：

- `numRows()`
- `extractColumn(column, offset, numRows, resultOffset, result)`
- `extractColumn(column, rowNumbers, resultOffset, result)`
- `extractNulls(...)`
- `computePeerBuffers(...)`
- `computeKRangeFrameBounds(...)`
- peer compare / sort key compare

slow path 的读取模板：

```text
logical row range / rowNumbers
  -> rowIds_
  -> readSession.loadRows(rowIds range)
  -> resident row pointers
  -> compare / extractColumnResident / read null bit
  -> evictLoadedChunks
```

## 8. 普通 WindowFunction

普通 WindowFunction 继续通过 `WindowPartition` 接口工作：

- `row_number`
- `rank`
- `dense_rank`
- `percent_rank`
- `cume_dist`
- `ntile`
- `lead`
- `lag`
- `first_value`
- `last_value`

`BmWindowPartition` 提供这些函数需要的能力：

- row count 和 row number 映射。
- peer boundary 计算。
- value column extract。
- offset/default column extract。
- `IGNORE NULLS` 所需 null bitmap。

## 9. Aggregate Window 分块执行

Bm aggregate window 使用新的 chunk 输入层：

```text
frame range
  -> BmWindowFrameReader.forEachArgBatch(...)
  -> materialize small argVectors
  -> Aggregate::addSingleGroupRawInput(...)
```

`BmAggregateWindowFunction` 伪流程：

```text
resetAggregateGroup()

for batch in frameReader.forEachArgBatch(frameStart, frameSize, argColumns):
  aggregate->addSingleGroupRawInput(
      rawSingleGroupRow,
      batchRows,
      batchArgVectors,
      false)

aggregate->extractValues(rawSingleGroupRow, result)
```

`Aggregate` API 继续使用现有接口：

- `initializeNewGroups`
- `addSingleGroupRawInput`
- `extractValues`
- `destroy`

## 10. Frame 执行策略

一阶段提供三类 aggregate 执行策略。

Whole-partition：

```text
UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING
  扫描 partition 一次。
  输出阶段复用同一个 aggregate result。
```

Forward-running：

```text
UNBOUNDED PRECEDING TO CURRENT ROW
  按 output 顺序推进 accumulator。
  每行只追加新增 rows。
```

Repeated-scan path：

```text
N PRECEDING TO N FOLLOWING
复杂 RANGE/GROUPS frame
CURRENT ROW TO UNBOUNDED FOLLOWING
其他 frameStart/frameEnd 变化的 frame

每个输出行:
  reset accumulator
  BmWindowFrameReader 分块读取当前 frame
  分块调用 addSingleGroupRawInput
  extract result
```

Repeated-scan path 和现有实现语义一致；Bm 路径使用小 argVectors 执行扫描。

## 11. IGNORE NULLS

`BmWindowPartition::extractNulls()` 提供 `IGNORE NULLS` 所需 null bitmap。

fast path：

```text
rowPtrs_ range
  -> read null byte/null mask
  -> fill null bitmap
```

slow path：

```text
rowIds_ range
  -> readSession.loadRows()
  -> read null byte/null mask
  -> fill null bitmap
  -> evictLoadedChunks
```

后续把 null bit 读取能力下沉到 Bm：

```cpp
BmRowContainer::extractNullsResident(
    const char* const* rows,
    int32_t numRows,
    int32_t column,
    const BufferPtr& result);
```

## 12. Bm API 使用

一阶段使用的 Bm 能力：

- `appendRow()` / `store()`
- `appendBatch(..., std::vector<char*>* rows)`
- `spillActiveSegment()` / `spillActivePartitionSegment()`
- `canBulkRead()`
- `beginBulkReadSegments()`
- `beginReadOnlyWindowReadSegments()`
- `ReadOnlyWindowReadSession::listRowIds()`
- `ReadOnlyWindowReadSession::loadRows()`
- `ReadOnlyWindowReadSession::evictLoadedChunks()`
- `compare(left, right, column)`
- `compareRows(left, right, flags)`
- `extractColumnResident(rows, numRows, column, result)`

后续推进的 Bm 能力：

- `extractNullsResident()`
- ordinal read API
- Bm-managed accumulator state

## 13. Peak Memory 模型

fast path peak：

```text
rowPtrs_ array
resident Bm blocks
small argVectors
accumulator state
output batch
null bitmap
```

slow path peak：

```text
rowIds_ array
current loaded chunks
small argVectors
accumulator state
output batch
null bitmap
```

关键控制点：

- 输入行 resident memory 交给 Bm/BufferManager。
- aggregate argVectors 由 `BmWindowFrameReader` batch size 控制。
- output memory 由 Window output batch 控制。
- slow path loaded chunks 由 `ReadOnlyWindowReadSession` 和 eviction 控制。

## 14. 一阶段交付

一阶段交付内容：

- Vector 层 partition boundary 判断。
- row-vs-vector partition key comparator。
- copy-on-spill last key snapshot。
- `BmStreamingWindowBuild` run 级写入。
- SQL partition boundary 切 Bm segment。
- `PartitionDescriptor` segment 列表维护。
- `BmWindowPartition` fast path。
- `BmWindowPartition` slow path。
- `extractColumn`、`extractNulls`、peer compare、frame compare 适配。
- `BmWindowFrameReader`。
- `BmAggregateWindowFunction` chunk 输入路径。
- `WindowBuildType::kBmStreamingWindowBuild` 测试注入。
- feature flag 和自动选择逻辑。
- benchmark 对比 old `StreamingWindowBuild`、Bm fast path、Bm slow path。

开启条件：

- 输入已按 `{PARTITION BY keys + ORDER BY keys}` 排序。
- BufferManager 可用。
- 输入类型位于 Bm 主路径支持范围内。
- feature flag 控制启用。

## 15. 后续推进

后续推进项：

1. `BmRowContainer::extractNullsResident()`。
2. `ReadOnlyWindowReadSession` ordinal API。
3. `CURRENT ROW TO UNBOUNDED FOLLOWING` reverse-running。
4. sliding frame 专用优化：retract、prefix accumulator、block accumulator。
5. accumulator state 放入 `BmRowContainer`，由 Bm 负责 spill。

## 16. 验证重点

需要重点验证：

- Vector partition key compare 和 old `RowContainer::compare()` 语义一致。
- row-vs-vector comparator 覆盖 null、fixed width、string、dictionary、constant vector。
- copy-on-spill last key snapshot 在跨 batch、跨 spill 时正确识别 partition boundary。
- 大量极小 partition 下 segment 数量和调度开销可接受。
- `WindowPartition` 适配层支持 Bm 构造和 old 构造共存。
- slow path peer/frame compare 的 load/evict 频率可接受。
- `BmAggregateWindowFunction` 覆盖 empty frame、null、constant argument、多参数 aggregate、output batch 跨边界。
- `IGNORE NULLS` 在 fast path 和 slow path 下结果一致。

## 17. 落地顺序

1. 实现 Vector partition key compare。
2. 实现 row-vs-vector partition key compare。
3. 实现 copy-on-spill last key snapshot。
4. 实现 `BmStreamingWindowBuild` run 级写入和 segment 切换。
5. 实现 `PartitionDescriptor` 管理。
6. 实现 `BmWindowPartition` fast path。
7. 实现 `BmWindowPartition` slow path。
8. 补齐 `extractColumn`、`extractNulls`、`computePeerBuffers`、frame compare。
9. 实现 `BmWindowFrameReader`。
10. 实现 `BmAggregateWindowFunction` chunk 输入路径。
11. 接入 `WindowBuildType::kBmStreamingWindowBuild`。
12. 接入 feature flag 和自动选择。
13. 增加 correctness tests、spill tests、benchmark。
