# BM RowContainer 使用说明

本文面向接入 `BmRowContainer` 的执行算子，只描述算子需要依赖的接口和生命周期。
内部的 segment/chunk/block 组织、StringView rebase 和 BufferManager pin 细节不作为算子
接入约束。

公共入口：

- `BmRowContainer.h`
- `BmRowContainerRead.h`
- `BmRowContainerTypes.h`

## 基本模型

`BmRowContainer` 是 row-based 临时数据容器。

- 写入阶段优先在内存中追加 row。
- 上层感知内存压力后调用 flush，把当前数据交给 BufferManager 管理。
- flush 返回 `SegmentId`。后续读回、释放、partition 管理都以 `SegmentId` 为单位。
- resident 阶段使用 `char*` row 指针；不能全量 resident 时使用 `RowId`，再交给
  `WindowReadSession` 批量转成 resident 指针。

## 创建

```cpp
using namespace bytedance::bolt::exec::bm;

BmRowContainer rows(
    {BIGINT(), VARCHAR()},
    {false, true},
    bufferManager,
    memory::bm::MemoryTag::kTesting);
```

`types` 和 `nullable` 必须一一对应。nullable 信息会参与 row layout 生成，非 nullable
列会走更短的快路径。

## 写入

批量输入优先使用 `appendBatch()`：

```cpp
std::vector<char*> rowPtrs = rows.appendBatch(input);
std::vector<char*> partitionRows = rows.appendBatch(input, partitionId);
```

天然逐行构造的算子使用 `appendRow()` + `store()`：

```cpp
auto context = rows.appendRow(partitionId);
rows.store(context, decodedKey, sourceIndex, keyColumn);
rows.store(context, decodedPayload, sourceIndex, payloadColumn);
char* row = context.row();
```

`RowWriteContext` 只描述当前 row 的写入位置，不要跨 container、跨 flush 或异步流程保存。

## Resident 指针访问

比较和列提取都要求输入 row 指针当前 resident。

```cpp
int32_t result = rows.compare(leftRow, rightRow, column, flags);
int32_t rowResult = rows.compareRows(leftRow, rightRow, keyFlags);

rows.extractColumnResident(
    rowPtrs.data(),
    rowPtrs.size(),
    column,
    outputVector);
```

flush 后旧指针不再有效。读回后需要重新通过 `listRows()`、`WindowReadSession` 或
`MergeReadSession` 获取指针。

## Flush

默认 partition：

```cpp
SegmentId segment = rows.flushActiveSegment();
```

多 partition：

```cpp
SegmentId segment = rows.flushActivePartitionSegment(partitionId);
```

同一个 partition 可以多次 flush，适合 Hash Build 这类分区写入场景：

```cpp
const auto& segments = rows.segmentsForPartition(partitionId);
```

## 全量读

如果 working set 预计可以全部 resident，先快速判断，再全量加载：

```cpp
std::vector<SegmentId> segments = ...;
folly::Range<const SegmentId*> range(segments.data(), segments.size());

if (rows.canLoadAllSegments(range)) {
  std::vector<char*> rowPtrs = rows.listRows(range);
  // rowPtrs 可直接用于 compare / extractColumnResident。
}
```

`canLoadAllSegments()` 只是快速判断。`listRows()` 会真正 reserve 和 pin；如果期间内存状态
变化，仍可能抛异常。

`listRows()` 返回的 `char*` 由 container 持有的 resident block 支撑。指针有效到对应
chunk/segment 被重新 spill、release，或 container 析构。

## Window read

如果不能全量加载，先列出 `RowId`，再按算子自己的访问窗口批量加载。

```cpp
std::vector<RowId> rowIds = rows.listRowIds(range);
auto session = rows.beginWindowReadSegments(range);

std::vector<RowId> needed = ...;
std::vector<char*> rowPtrs = session.loadRows(
    folly::Range<const RowId*>(needed.data(), needed.size()));
```

单行接口是显式慢路径：

```cpp
char* row = session.loadRow(rowId);
```

`WindowReadSession` 不管理内存释放。读阶段如果需要释放 resident 内存但未来还要继续使用这些
数据，调用 `spillLoadedSegments()`；如果数据已经完全消费，调用 release 接口。

## Reordered Segment 和 Merge Read

Sort / HashAgg 在内存中完成排序后，可以按排序后的 row 指针顺序物理写出一个可 merge 的
segment：

```cpp
SegmentId orderedSegment =
    rows.finalizeReorderedSegment(
        folly::Range<char* const*>(orderedRows.data(), orderedRows.size()));
```

多个有序 segment 使用 `MergeReadSession` 顺序读回：

```cpp
auto merge = rows.beginMergeReadSegments(range);

std::vector<char*> batch;
while (merge.next(batch, maxRows)) {
  rows.extractColumnResident(batch.data(), batch.size(), column, output);
}
```

`beginMergeReadSegments()` 只接受 `finalizeReorderedSegment()` 产生的有序 segment。普通
flush segment 不能直接进入 merge read。

merge read 默认是消费型读取：读完的 chunk 会在安全时机释放，避免后续内存压力下再次 spill
已经消费的数据。如果调用方需要重复读取，显式关闭读后释放：

```cpp
auto merge = rows.beginMergeReadSegments(range, false);
```

## 释放和重新 Spill

数据未来还可能再用，但当前需要释放 resident 内存：

```cpp
rows.spillLoadedSegments(range);
rows.spillAllLoadedBlocks();
```

数据已经不会再用：

```cpp
rows.releaseSegment(segment);
rows.releaseSegments(range);
```

## 常见接入方式

Sort / HashAgg：

1. `appendBatch()` 或 `appendRow()` 写入。
2. resident 阶段保留 row 指针并用 `compareRows()` 排序。
3. 排序后调用 `finalizeReorderedSegment()`。
4. 多个有序 segment 用 `beginMergeReadSegments()` 输出。

Hash Build：

1. 按 partition 写入：`appendBatch(input, partition)` 或 `appendRow(partition)`。
2. 每个 partition 可以多次 `flushActivePartitionSegment(partition)`。
3. probe 或后续处理某个 partition 时，读取 `segmentsForPartition(partition)`。
4. 能全量加载则 `listRows()`；不能则 `listRowIds()` + `WindowReadSession::loadRows()`。
5. partition 完成后释放对应 segments。

## 使用约束

- flush 后不要继续使用旧 row 指针。
- `RowId` 不建议由算子自行解析，应交回 `WindowReadSession`。
- `compare()`、`compareRows()`、`extractColumnResident()` 都要求 row 指针 resident。
- `RowWriteContext` 只用于当前 row 的逐列 store。
- 当前常规快路径覆盖 fixed-width 类型、`VARCHAR` 和 `VARBINARY`；复杂类型不要作为接入假设。
