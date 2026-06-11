# BM RowContainer 使用说明

本文面向需要接入 `BmRowContainer` 的执行算子。`BmRowContainer` 是一个 row-based
临时数据容器：写入阶段优先在内存中攒数据；当上层算子感知内存压力时，显式 flush 到
BufferManager 管理的 block；读回阶段优先尝试全量加载，失败后进入 window read。

公共入口主要在：

- `BmRowContainer.h`
- `BmRowContainerRead.h`
- `BmRowContainerTypes.h`

`BmRowLayout`、`BmRowStorage`、`BmRowBlockLoader`、`BmRowCopier` 是内部实现拆分，
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
  `BulkReadSession` 做 window read，不要自己把它变成随机 IO。

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
- `tryLoadAll()` 成功返回了指针。
- `loadRows()` 或 `loadRow()` window read 后返回了当前窗口内的指针。

如果只有 `RowId`，需要先通过 `BulkReadSession` 转成当前窗口内的指针，再调用 extract。

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

## Bulk read

读回一组 segment 时，先创建 `BulkReadSession`。

```cpp
std::vector<SegmentId> segments = ...;
ReadSessionOptions options;
auto session = rows.beginBulkReadSegments({segments.data(), segments.size()}, options);
```

然后调用 `tryLoadAll()`：

```cpp
std::vector<char*> rowPtrs;
std::vector<RowId> rowIds;
auto result = session.tryLoadAll(rowPtrs, rowIds);

if (result == LoadAllResult::kLoadedPointers) {
  // 全量 load 成功，rowPtrs 可直接用于 compare / extract。
} else {
  // 全量 load 失败，rowIds 描述所有 rows，后续走 window read。
}
```

`tryLoadAll()` 会先估算需要 pin 的字节数：

- 如果 `ReadSessionOptions::maxPinnedBytes` 非 0 且估算值超过限制，直接返回
  `kNeedWindowRead`。
- 否则调用 BufferManager 的 reserve / batch pin 尝试全量加载。
- 全量加载成功时返回 `rowPtrs`。
- 失败时清理已 pin 的 block，并返回 `rowIds`。

## Window read

当 `tryLoadAll()` 返回 `kNeedWindowRead` 时，上层可以按自己的访问顺序组织 `RowId`，
再批量交给 session 加载窗口。

```cpp
std::vector<RowId> needed = ...;
RowWindow window = session.loadRows({needed.data(), needed.size()});

std::vector<char*> rowPtrs;
rowPtrs.reserve(window.rows.size());
for (const auto& row : window.rows) {
  rowPtrs.push_back(row.ptr);
}

rows.extractColumnResident(rowPtrs.data(), rowPtrs.size(), column, result);
```

单行慢路径：

```cpp
char* row = session.loadRow(rowId);
```

`loadRows()` 会按 chunk 去 pin 数据，返回的指针只在下一次 `loadRows()` / `loadRow()` 或
session 析构前有效。上层应尽量批量提交同一访问窗口内的 `RowId`，避免退化成大量单行随机读。

## Reordered segment

`finalizeReorderedSegment()` 按传入的 row 指针顺序物理写入一个新的 finalized/flushed
segment。Sort / HashAgg 可以把这个返回的 `SegmentId` 当作一个有序 run 保存，但
RowContainer 本体只建模 segment。

```cpp
std::vector<char*> orderedRows = ...;

SegmentId orderedSegment =
    rows.finalizeReorderedSegment({orderedRows.data(), orderedRows.size()});
```

读回多个物理有序 segment：

```cpp
std::vector<SegmentId> segments = ...;
auto mergeSession = rows.beginMergeReadSegments({segments.data(), segments.size()});

auto cursor = mergeSession.cursor(segments[0]);
while (cursor.hasCurrent()) {
  const char* row = cursor.currentRow();
  cursor.advance();
}
```

比较两个 cursor 当前 row：

```cpp
int32_t r = mergeSession.compareCurrentRows(leftCursor, rightCursor, flags);
```

cursor 内部按 chunk 加载当前 row 所在窗口。`currentRow()` 返回的指针在 cursor 前进前有效。

如果调用方确认 merge read 是消费型单向读取，可以打开读后释放：

```cpp
auto mergeSession = rows.beginMergeReadSegments(
    {segments.data(), segments.size()},
    true);
```

`releaseAfterRead=true` 时，cursor 读完某个 chunk 后会释放该 chunk 的 BM block 引用，
避免已消费数据在后续内存压力下再次 spill。chunk metadata 会保留，后续再次读取这个 segment 会失败。

## 释放数据

segment 不再需要时，应显式释放：

```cpp
rows.releaseSegment(segment);
```

批量释放：

```cpp
rows.releaseSegments({segments.data(), segments.size()});
```

read session 不会自动释放 segment，消费完成后应显式调用 release 接口。

## 常见接入方式

### Sort / Hash Agg

1. 写入阶段使用 `appendBatch()` 或 `appendRow()`。
2. 内存中比较时保留 row 指针，调用 `compareRows()`。
3. 需要生成有序输出时，传入排序后的 row 指针调用 `finalizeReorderedSegment()`。
4. 多个有序 segment 通过 `beginMergeReadSegments()` 和 cursor 顺序读回。
5. 消费型 merge 可以使用 `releaseAfterRead=true`，否则输出完成后调用 `releaseSegment(segment)`。

### Hash Build

1. 按 partition 写入：`appendBatch(input, partition)` 或 `appendRow(partition)`。
2. 每个 partition 可多次 `flushActivePartitionSegment(partition)`。
3. probe 或后续处理某个 partition 时，取 `segmentsForPartition(partition)`。
4. 先 `tryLoadAll()`。成功则用指针访问；失败则按 join probe 需要的顺序调用 `loadRows()`。
5. partition 完成后释放它的 segments。

## 使用约束

- flush 后不要继续使用旧 row 指针。
- `RowId` 只用于 read session 的 window read，不建议由算子自行解析。
- `extractColumnResident()` 和 `compare()` 都要求输入 row 指针当前 resident。
- `RowWriteContext` 只用于当前 row 的逐列 store，不要保存到异步流程。
- 复杂类型目前不应作为常规接入假设；当前快路径主要覆盖 fixed-width 类型和 `VARCHAR`。
- `appendBatch()` 是批量输入的优先路径；逐行写入用于算子天然逐行构造的场景。
