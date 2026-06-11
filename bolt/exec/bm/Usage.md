# BM RowContainer 使用说明

本文面向需要接入 `BmRowContainer` 的执行算子。`BmRowContainer` 是一个 row-based
临时数据容器：写入阶段优先在内存中攒数据；当上层算子感知内存压力时，显式 flush 到
BufferManager 管理的 block；读回阶段优先尝试全量加载，失败后进入 window read。

公共入口主要在：

- `BmRowContainer.h`
- `BmRowContainerRead.h`
- `BmRowContainerTypes.h`

`BmRowLayout`、`BmSegmentCollection`、`BmRowBlockLoader`、`BmRowCopier` 是内部实现拆分，
算子侧通常不直接使用。

## 基本模型

`BmRowContainer` 内部按 `Segment` 组织数据：

- 写入时总是写到某个 active segment。
- `flushActiveSegment()` 或 `flushActivePartitionSegment(partition)` 会 finalize 当前
  active segment，并把它的 row block / heap block 交给 BufferManager 管理。
- flush 后返回 `SegmentId`。之后读回、释放都以 `SegmentId` 为边界。
- 每个 partition 可以产生多个 segment，适合 Hash Build 这类分区后多次 flush 的场景。

row 的访问形态有两种：

- `char*`：内存中已经 resident 的 row 指针，比较和 extract 的快路径都基于它。
- `RowId`：当 working set 无法一次性全部 pin 住时使用。上层拿到 `RowId` 后，应交回
  `WindowReadSession` 做 window read，不要自己把它变成随机 IO。

## 创建容器

创建时需要传入列类型、列 nullable 信息、BufferManager 和 MemoryTag。

```cpp
using namespace bytedance::bolt::exec::bm;

BmRowContainer rows(
    {BIGINT(), VARCHAR()},
    {false, true},
    bufferManager,
    memory::bm::MemoryTag::kTesting);
```

`types` 和 `nullable` 必须一一对应。nullable 信息会参与 row layout 生成，非 nullable
列会走更短的 null 快路径。

默认 row block 和 heap block 使用 `AllocateSize::kLarge`。一般算子不需要手动调整。

## 写入数据

### Batch 写入

如果输入是 `RowVectorPtr`，优先使用 `appendBatch()`。它会按列批量写入，并返回本批写入
的 row 指针。

```cpp
std::vector<char*> rowPtrs = rows.appendBatch(input);
```

带 partition 写入：

```cpp
std::vector<char*> rowPtrs = rows.appendBatch(input, partitionId);
```

返回的指针只在对应 block resident 时有效。发生 flush 后，上层如果保留了这些指针，应主动
清空；读回时重新通过 read session 获取指针或 `RowId`。

### 逐行写入

如果算子本身是逐行构造，使用 `appendRow()` 获取 `RowWriteContext`，再逐列调用
`store()`。

```cpp
SelectivityVector selected(input->size());
std::vector<DecodedVector> decoded(input->childrenSize());
for (auto column = 0; column < input->childrenSize(); ++column) {
  decoded[column].decode(*input->childAt(column), selected);
}

for (auto i = 0; i < input->size(); ++i) {
  auto context = rows.appendRow(partitionId);
  for (auto column = 0; column < input->childrenSize(); ++column) {
    rows.store(context, decoded[column], i, column);
  }
  char* row = context.row();
  // row 可以用于当前内存 resident 阶段的 compare / extract。
}
```

`RowWriteContext` 不是长期状态对象，只描述当前 row 的写入位置。不要跨 container 或跨
flush 复用它。

## 比较

比较接口完全基于 `char*` row 指针。

```cpp
int32_t r = rows.compare(leftRow, rightRow, column, flags);
```

多列比较：

```cpp
std::vector<CompareFlags> flags = {key0Flags, key1Flags};
int32_t r = rows.compareRows(leftRow, rightRow, flags);
```

典型使用：

- Sort 在内存中排序 row 指针。
- Hash Agg / Hash Build 在 resident 阶段使用指针访问 key 或 payload。
- Merge read 阶段通过 cursor 拿到当前 row 指针后再比较。

如果 segment 已经被 flush 且没有通过 read session pin 回内存，不要继续使用旧指针。

## 提取列

`extractColumnResident()` 只接受 resident row 指针。

```cpp
auto result = BaseVector::create(VARCHAR(), rowPtrs.size(), pool);
rows.extractColumnResident(
    rowPtrs.data(),
    rowPtrs.size(),
    column,
    result);
```

这个接口用于当前 rows 已经在内存中的场景，包括：

- 写入后尚未 flush。
- `listRows()` 全量加载后返回了指针。
- `WindowReadSession::loadRows()` 或 `loadRow()` 返回了指针。

如果只有 `RowId`，需要先通过 `WindowReadSession` 转成指针，再调用 extract。

## Flush

普通容器只有默认 partition 时，使用：

```cpp
SegmentId segment = rows.flushActiveSegment();
```

带 partition 的场景使用：

```cpp
SegmentId segment = rows.flushActivePartitionSegment(partitionId);
```

flush 的语义：

- finalize 当前 active segment。
- 把 segment 内 block 交给 BufferManager，必要时可被 spill。
- 返回稳定的 `SegmentId`。
- 下一次写入同一 partition 会创建新的 active segment。

Sort / Hash Agg 通常 flush 少量 segment；Hash Build 可以对每个 partition 多次 flush。

## 全量读

```cpp
std::vector<SegmentId> segments = ...;

if (rows.canLoadAllSegments({segments.data(), segments.size()})) {
  std::vector<char*> rowPtrs =
      rows.listRows({segments.data(), segments.size()});
  // rowPtrs 可直接用于 compare / extract。
}
```

`canLoadAllSegments()` 只是快速判断，不保证紧接着 `listRows()` 一定成功。`listRows()`
会真正 reserve / batch pin；如果内存不足或 IO 失败，会直接抛异常。

`listRows()` 成功后，所有相关 block 的 `BufferHandle` 由 `BmRowContainer` 持有，返回的
`char*` 只是指针视图。指针有效到对应 chunk/segment 被 `spillLoaded...()` 重新写出、
被 `release...()` 销毁，或 container 析构。

## Window read

当 working set 不能全量加载时，先列出 `RowId`，再按上层需要的访问窗口批量加载。

```cpp
std::vector<RowId> rowIds = rows.listRowIds({segments.data(), segments.size()});
auto session = rows.beginWindowReadSegments({segments.data(), segments.size()});

std::vector<RowId> needed = ...;
std::vector<char*> rowPtrs = session.loadRows({needed.data(), needed.size()});

rows.extractColumnResident(rowPtrs.data(), rowPtrs.size(), column, result);
```

单行慢路径：

```cpp
char* row = session.loadRow(rowId);
```

`WindowReadSession` 不持有内存；它只负责把 `RowId` 解析成需要加载的 chunk。真正的
`BufferHandle` 仍然写回 `BmRowContainer`。因此读阶段内存压力也统一由 container 处理：

```cpp
rows.spillLoadedSegments({segments.data(), segments.size()});
```

如果数据已经不会再用，调用 `releaseSegment()` / `releaseSegments()`，不要重新 spill。

## Reordered segment

`finalizeReorderedSegment()` 按传入的 row 指针顺序物理写入一个新的 finalized/flushed
segment。Sort / HashAgg 可以把这个返回的 `SegmentId` 当作一个有序 run 保存，但
RowContainer 本体只建模 segment。

当前实现会把传入 rows 完整复制到一个新的 segment 后再 flush。这个路径能保证后续
merge read 是顺序扫描，但会提高重排阶段的内存峰值；如果是在强内存压力下写出大 run，
后续应改成分段物化、分段 flush 的接口。

```cpp
std::vector<char*> orderedRows = ...;

SegmentId orderedSegment =
    rows.finalizeReorderedSegment({orderedRows.data(), orderedRows.size()});
```

读回多个物理有序 segment：

```cpp
std::vector<SegmentId> segments = ...;
auto mergeSession = rows.beginMergeReadSegments({segments.data(), segments.size()});

std::vector<char*> rowPtrs;
while (mergeSession.next(rowPtrs, maxRows)) {
  rows.extractColumnResident(rowPtrs.data(), rowPtrs.size(), column, result);
}
```

`beginMergeReadSegments()` 只接受 `orderedForMerge=true` 的 segment。普通
`flushActiveSegment()` 产生的 segment 不能直接进入 merge read；`finalizeReorderedSegment()`
产生的 segment 会标记为可 merge。

merge read 默认是消费型单向读取。`next()` 推进后，上一批已经读完的 chunk 会被释放，
避免已消费数据在后续内存压力下再次 spill。chunk metadata 会保留，后续再次读取这个
segment 会失败。

如果调用方需要重复读取同一个 segment，应显式关闭读后释放：

```cpp
auto mergeSession = rows.beginMergeReadSegments(
    {segments.data(), segments.size()},
    false);
```

## 释放数据

读阶段如果数据后面还可能再用，但需要释放当前内存，应重新 spill：

```cpp
rows.spillLoadedSegments({segments.data(), segments.size()});
rows.spillAllLoadedBlocks();
```

这些接口会把当前 resident 的 loaded blocks 写回 BufferManager storage，然后释放
`BufferHandle`，保留 segment/chunk metadata 和 `RowId` 可解析性。因为原 spill 文件读回
后会被销毁，所以不能把读阶段数据当作 clean block 直接 evict。

segment 不再需要时，应显式释放：

```cpp
rows.releaseSegment(segment);
```

批量释放：

```cpp
rows.releaseSegments({segments.data(), segments.size()});
```

read session 不会自动释放整个 segment，消费完成后应显式调用 release 接口。

## 常见接入方式

### Sort / Hash Agg

1. 写入阶段使用 `appendBatch()` 或 `appendRow()`。
2. 内存中比较时保留 row 指针，调用 `compareRows()`。
3. 需要生成有序输出时，传入排序后的 row 指针调用 `finalizeReorderedSegment()`。
4. 多个有序 segment 通过 `beginMergeReadSegments()` 和 `next()` 顺序读回。
5. merge read 默认会释放已消费 chunk；如果后续还要复读，应传 `releaseAfterRead=false`。

### Hash Build

1. 按 partition 写入：`appendBatch(input, partition)` 或 `appendRow(partition)`。
2. 每个 partition 可多次 `flushActivePartitionSegment(partition)`。
3. probe 或后续处理某个 partition 时，取 `segmentsForPartition(partition)`。
4. 先 `canLoadAllSegments()`。能全量加载则 `listRows()`，否则 `listRowIds()` 后按 join
   probe 需要的顺序调用 `WindowReadSession::loadRows()`。
5. partition 完成后释放它的 segments。

## 使用约束

- flush 后不要继续使用旧 row 指针。
- `RowId` 只用于 read session 的 window read，不建议由算子自行解析。
- `extractColumnResident()` 和 `compare()` 都要求输入 row 指针当前 resident。
- `RowWriteContext` 只用于当前 row 的逐列 store，不要保存到异步流程。
- 复杂类型目前不应作为常规接入假设；当前快路径主要覆盖 fixed-width 类型和 `VARCHAR`。
- `appendBatch()` 是批量输入的优先路径；逐行写入用于算子天然逐行构造的场景。
