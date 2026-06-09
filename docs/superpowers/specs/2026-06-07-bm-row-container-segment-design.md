# BmRowContainer Segment 化核心设计

日期：2026-06-07

本文定义新版 `BmRowContainer` 的核心语义和接口边界。目标是在保留现有 `RowContainer`
resident 快路径的同时，让 row-format 中间数据可以进入 `BufferManager` 管理，并在内存不足时通过
显式 flush、批量读回和窗口化读取继续工作。

Sort、Hash Agg、Hash Build / Hash Join 的接入流程见
`2026-06-07-bm-row-container-operator-integration.md`。本文是 RowContainer 类型、状态和 API 的
权威定义；算子集成文档只能使用本文定义的接口。

## 1. 设计目标

新版 RowContainer 必须同时满足两类需求：

- Store、Hash、Compare 的主路径仍然是纯内存指针访问。
- 当 working set 不能完整留在内存时，可以通过 BufferManager 以批量方式 flush、读回和窗口化读取。

具体约束：

- `RowId` 是长期身份，可以跨 flush、unpin、reload 保存。
- `char*` 是短期快路径，只在 resident 阶段或 read session 当前窗口内有效。
- 上层算子可以缓存 `char*`，但在 flush 对应 segment 后必须主动清空这些指针。
- `compare(const char*, const char*)` 永远不触发 IO，也不做 pin/load。
- `store()` 只写当前 resident active segment，不触发读回。
- BufferManager 读回和置换只发生在显式 read/merge 边界。
- Sort、Hash Agg、Hash Join 都不在 comparator、hash probe 命中点或单 row 访问处做随机 IO。

这不是透明按行缺页加载容器。任意 `RowId` 访问都不应该隐式触发 IO 或 BM load。IO 只允许出现在：

- `flushActiveSegment()` / `flushActivePartitionSegment()` 的全量下刷。
- `beginBulkReadSegments()` 创建 read session 时的全量尝试读回。
- `BulkReadSession` 的 window read。
- `MergeReadSession` 的 cursor 窗口推进。

## 2. 生命周期语义

Segment 的生命周期分三层概念：

```text
finalize:
  停止写入，冻结 row/chunk/part 元数据。

flush:
  finalize 后，把 segment 的 row/heap blocks 全量交给 BM/spill backing store 管理。

evict:
  BM 释放 resident memory copy。evict 是物理内存置换，不是 RowContainer 的写入生命周期边界。
```

`finalize` 和 `evict` 不能混用。`finalize` 只表示这个 segment 不再追加 row，元数据稳定；数据此时
仍然可以在内存中。`evict` 只表示 resident copy 被释放，后续需要通过 BM pin/load 重新获得地址。

建议的 segment 状态：

```cpp
enum class SegmentState {
  kActiveResident,     // 可继续写入，所有 cached ptr 对上层仍有效
  kFinalizedResident,  // 不可继续写入，元数据已冻结，数据仍 resident
  kFinalizedFlushed,   // 不可继续写入，已有 BM/backing store 表示，可读回
};
```

`kFinalizedFlushed` 不等于“当前不在内存”。它只表示数据已经有可读回的 backing store；BM 仍可在内存
充足时保留 resident copy。

## 3. Row 身份和指针

写入和 resident 快路径只向上层暴露 `char*`。`RowId` 不是写入阶段的返回值，也不作为
RowContainer 内部逐行保存的元数据；它只在 `BulkReadSession::tryLoadAll()` 进入 window read
fallback 时，根据 Segment / DataChunk / ChunkPart 元数据批量构造出来。

```cpp
using SegmentId = uint32_t;
using SortedRunId = uint32_t;
using BlockId = uint32_t;
using RowOffset = uint32_t;
using RowNumber = uint32_t;
using PartitionId = uint32_t;

struct RowId {
  SegmentId segmentId;
  RowNumber rowNumber;        // segment 内逻辑行号，用于定位 DataChunk/window
  BlockId rowBlockId;
  RowOffset rowOffset;        // rowBlock 内 byte offset
  BlockId primaryHeapBlockId; // hint；sentinel 表示没有 out-of-line value
};

```

`rowBlockId + rowOffset` 用于直接定位 row bytes。`rowNumber` 用于把 row 映射到 `DataChunk`，避免
window read 为了定位 chunk 反查所有 block 元数据。`primaryHeapBlockId` 是预取 hint，不是正确性的
唯一来源；一行可以有多个 variable-width 列，它们可能落在不同 heap block。真正的 heap pin/rebase
依赖 `ChunkPart` 元数据。

flush 后，上层必须清空对应 cached pointer：

```text
for each cached char* in flushed segment:
  ptr = nullptr
```

可以给 segment 增加 debug epoch，在 debug build 中检查 cached pointer 是否来自当前有效 epoch。
release build 不依赖 epoch 保证正确性。

## 4. 物理组织

RowContainer 使用三层元数据组织 BM blocks：

```text
Segment
  生命周期单位。一次连续写入形成一个 active segment，finalize 后停止写入。

DataChunk
  segment 内的逻辑扫描窗口。它记录一批连续 rowNumber 需要哪些 row blocks、heap blocks 和 parts。

ChunkPart
  chunk 内的物理连续切片。它是 pointer rebasing 的元数据单位。
```

### Segment

Segment 是一次连续写入的结果，也是非 partitioned flush 的最小逻辑单位。Hash Build 可以为每个
partition 维护独立 active segment，并让同一个 partition 拥有多个 finalized/flushed segments。

```cpp
using ChunkId = uint32_t;
using PartId = uint32_t;

struct SegmentMeta {
  SegmentId id;
  SegmentState state;
  std::optional<PartitionId> partitionId;
  std::vector<BlockId> rowBlocks;
  std::vector<BlockId> heapBlocks;
  std::vector<ChunkId> chunks;
  uint64_t numRows;
};
```

Segment 的物理行顺序就是写入顺序。Sort 和 Hash Agg 需要的有序 run 不是普通 Segment 的固有属性，
而是独立的 `SortedRun` 描述。

### DataChunk

DataChunk 是 read session 和 merge cursor 的窗口单位：

```cpp
struct DataChunkMeta {
  ChunkId id;
  SegmentId segmentId;
  RowNumber firstRowNumber;
  uint32_t rowCount;
  std::vector<PartId> parts;
  std::vector<BlockId> rowBlocks;
  std::vector<BlockId> heapBlocks;
};
```

一个 chunk 通常对应一个 vectorized batch 大小，例如 1024 或 4096 行。具体大小是实现参数，语义上
它表示“加载当前窗口所需 blocks”的单位。

`rowBlocks` 和 `heapBlocks` 是冗余索引，用于让窗口加载不必每次扫描所有 parts。

### ChunkPart

ChunkPart 描述 chunk 内一段物理连续 rows，并记录这些 rows 写入时关联的 heap block base。

```cpp
struct HeapBaseRef {
  BlockId heapBlockId;
  uintptr_t baseAddress; // 写入或上一次 rebase 后的 heap block base
  uint32_t capacity;
};

struct ChunkPartMeta {
  PartId id;
  ChunkId chunkId;
  BlockId rowBlockId;
  uint32_t rowBlockOffset;
  uint32_t rowCount;
  std::vector<HeapBaseRef> heapBases;
};
```

`HeapBaseRef` 的作用是支持 pointer rebasing。它记录 row 内 `StringView` pointer 当前对应的 heap
block base。当 heap block 重新 pin 到新地址时，窗口加载阶段可以批量修正 pointer。

## 5. Variable-width 和 pointer rebasing

row 内 variable-width 字段继续使用 `StringView` pointer 形态，保证 resident 快路径没有
`blockId + offset -> pointer` 的逐次转换成本。

存储规则：

```text
if value.size() <= heapBlockSize:
  放入普通 heap block；当前 heap block 空间不足则新开普通 heap block
else:
  分配 dedicated large heap block
  该 block capacity >= value.size()
  value 完整写入该 block，不跨 block
```

单个 out-of-line value 不跨 heap block。超过普通 heap block size 的 value 使用 dedicated large
heap block 完整存放，避免 compare/extract 需要 multipart reader。

写入 out-of-line value 时：

- 将 bytes 写入当前 pinned heap block。
- 在 row 内写 `StringView(pointer, size)`。
- 更新当前 `ChunkPartMeta::heapBases`。
- 如果该 row 还没有 `primaryHeapBlockId`，把该 heap block 记入 `RowId` hint。

当 heap block 地址变化时，在窗口加载边界执行 rebasing：

```text
pin part.rowBlockId
pin part.heapBases[*].heapBlockId
for each heap block in part.heapBases:
  if currentBase != recordedBase:
    scan rows in part
    for each variable-width column:
      if StringView is out-of-line and pointer belongs to recorded heap range:
        offset = oldPointer - recordedBase
        newPointer = currentBase + offset
        rewrite StringView pointer in row
    update recordedBase = currentBase
```

Pointer rebasing 只允许发生在：

- `beginBulkReadSegments()` 全量加载成功后。
- `BulkReadSession` 加载某个 chunk/window 时。
- `MergeReadSession` cursor 推进到新窗口时。

不要在 `compare()` 或 `extractString()` 的内层循环里临时修 pointer。

## 6. 写入、finalize 和 flush

非 partitioned 算子使用默认 active segment：

```cpp
char* newRow();
SegmentId flushActiveSegment();
```

Partitioned Hash Build 使用按 partition 独立的 active segment：

```cpp
char* newRow(PartitionId partition);
SegmentId flushActivePartitionSegment(PartitionId partition);
```

`store()` 只接受当前 resident row pointer：

```cpp
void store(
    const DecodedVector& decoded,
    vector_size_t sourceIndex,
    char* row,
    int32_t column);
```

Flush 语义：

```text
flushActiveSegment / flushActivePartitionSegment
  -> finalize 当前 active segment
  -> bulk 下刷该 segment 的所有 row/heap blocks
  -> segment state = kFinalizedFlushed
  -> 返回 SegmentId
  -> 容器为该 scope 创建新的 active segment
  -> 调用方清空旧 segment 的 cached pointers
```

Flush 是上层显式生命周期边界，不是内部 LRU。它是全量、大批量操作，不做逐 row 或逐 block 的随机
驱逐决策。

## 7. SortedRun

Sort 和 Hash Agg 需要 sorted run。新版设计只采用物理有序 run：调用方用 resident pointer 排序后，
RowContainer 按排序结果把 rows 拷贝到新的 materialized segment。这样 merge 阶段只需要顺序 cursor，
不需要向上层暴露 RowId 顺序数组。

```cpp
enum class SortedRunLayout {
  // 物理有序：materializedSegment 内的 row 顺序就是排序顺序。
  kMaterializedOrder,
};

struct SortedRunMeta {
  SortedRunId id;
  SortedRunLayout layout;
  SegmentId materializedSegment;
  uint64_t numRows;
};

struct SortedRunOptions {
  SortedRunLayout preferredLayout;
};

SortedRunId finalizeSortedRun(
    folly::Range<char* const*> sortedRows,
    const SortedRunOptions& options);
```

`finalizeSortedRun()` 的共同语义：

```text
1. 调用方已经用 resident ptr 排好 sortedRows。
2. RowContainer 按 `sortedRows` 顺序写出新的 materialized segment。
3. RowContainer 生成 `SortedRunMeta`。
4. 返回 SortedRunId，供 MergeReadSession 使用。
5. 调用方清空不再有效的 cached pointers。
```

- materialized segment 内 row 顺序就是 sorted run 顺序。
- MergeReadSession 每个 cursor 只顺序 pin 当前窗口并返回 `char*`。
- materialized segment 的物理行顺序就是排序顺序。
- materialized segment 进入 `kFinalizedFlushed`，供后续 merge 顺序扫描。
- source segment 在没有其他引用后可以释放；是否也下刷 source segment 取决于实现是否还需要它。

两种 layout 都由 `MergeReadSession` 屏蔽。算子只持有 `SortedRunId`，比较时只看
`cursor.currentRow()` 返回的有效 row pointer。

## 8. Resident fast path

resident 阶段提供接近现有 `RowContainer` 的接口：

```cpp
BmRowContainer(
    std::vector<TypePtr> types,
    std::vector<bool> nullable,
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    memory::bm::MemoryTag tag,
    ...);

void storeColumn(
    const DecodedVector& decoded,
    vector_size_t size,
    char* const* rows,
    int32_t column);

int32_t compare(
    const char* left,
    const char* right,
    int32_t column,
    CompareFlags flags = {});

int32_t compareRows(
    const char* left,
    const char* right,
    const std::vector<CompareFlags>& flags = {});

void extractColumnResident(
    char* const* rows,
    int32_t numRows,
    int32_t column,
    const VectorPtr& result);
```

`nullable` 是列级 layout 属性，由上层算子创建 container 时传入。非 nullable 列不分配 null bit，
store 时不接受 null，extract 时走无 null 检查的 typed 快路径；nullable 列才读取对应 null bit。
批量写入时，fixed-width 列应优先走 `storeColumn()`，让 type dispatch 和 nullable 判断发生在列级；
variable-width 列可以根据元数据维护成本选择列式写入或在 `newRow()` 后立即按行写入。

这些接口的前提是 rows 所属 segment resident，且 variable-width pointers 已经有效。它们不触发
pin、read、rebase 或 IO。`extractColumnResident()` 接收 `char* const*`，是为了让上层直接传
`std::vector<char*>` 的连续切片，避免 read 快路径为了 const 转换额外拷贝指针数组。

## 9. BulkReadSession

`BulkReadSession` 用于“我有一批 segments，要把 working set 准备好”的场景。创建 session 时先尝试
一次性加载全部目标 segments；如果失败，再进入 window read。

```cpp
enum class ReadMode {
  kFullyResident,
  kWindowRead,
};

struct ReadSessionOptions {
  uint64_t maxPinnedBytes{0}; // 0 表示由容器/BM 决定
  bool releaseWhenConsumed{false};
};

class BulkReadSession {
 public:
  ReadMode mode() const;

  LoadAllResult tryLoadAll(
      std::vector<char*>& rows,
      std::vector<RowId>& rowIds);

  RowWindow loadRows(folly::Range<const RowId*> rows);

  char* loadRow(const RowId& row);
};

BulkReadSession beginBulkReadSegments(
    folly::Range<const SegmentId*> segments,
    ReadSessionOptions options = {});
```

Session 创建流程：

1. 尝试 pin/load 目标 segments 的所有 row/heap blocks。
2. 如果成功，`mode() == kFullyResident`，`tryLoadAll()` 只填充 `rows`，这些 pointer 在 session
   生命周期内稳定。
3. 如果失败，`mode() == kWindowRead`，`tryLoadAll()` 只填充 `rowIds`。这些 RowId 是根据
   chunk/part 元数据构造出来的 durable fallback handle，不在写入阶段逐行保存。

`tryLoadAll()` 是互斥输出：

```text
kLoadedPointers:
  rows 非空，rowIds 为空

kNeedWindowRead:
  rows 为空，rowIds 非空
```

window read 下，上层可以通过两类显式慢路径重新获得 pointer：

```text
loadRows(rowIds)
  -> 批量加载 rowIds 所在 chunks
  -> 返回 RowView { RowId id; char* ptr }

loadRow(rowId)
  -> 显式单行慢路径
  -> 可以触发 row 所在 chunk 的 pin/load/rebase
```

`loadRow()` 是公开 API，但它不是 resident 快路径；调用方必须把它当作显式慢路径。它可能推进或替换
session 当前窗口，并让之前 `loadRows()` / `loadRow()` 返回的临时 pointer 失效。

Window read 流程：

```text
for each needed DataChunk in output order:
  pin chunk.rowBlocks
  pin chunk.heapBlocks
  rebase chunk.parts if needed
  extract rows belonging to this chunk into result positions
  release window pins that are no longer protected
```

如果输入 RowId 顺序和 segment 物理顺序不同，window read 仍必须按调用方给定顺序写 result。
实现可以内部按 chunk 分组读取，但需要保留输出 index 映射。

`releaseWhenConsumed` 只在上层显式设置时生效。容器不能自行猜测某个 segment 是否还会被后续阶段
使用。

## 10. MergeReadSession

`MergeReadSession` 用于 sorted run 多路归并。它不是全量读回所有 source segments，而是为每个
`SortedRun` 维护一个 cursor。

```cpp
class SegmentCursor {
 public:
  bool hasCurrent() const;
  const char* currentRow() const; // 当前 cursor window 内有效
  void advance();
};

class MergeReadSession {
 public:
  SegmentCursor cursor(SortedRunId run);

  int32_t compareCurrentRows(
      const SegmentCursor& left,
      const SegmentCursor& right,
      const std::vector<CompareFlags>& flags);

  void gatherCurrentRows(
      folly::Range<const SegmentCursor*> cursors,
      folly::Range<const IdentityProjection*> projections,
      RowVectorPtr& output);
};

MergeReadSession beginMergeReadSegments(
    folly::Range<const SortedRunId*> runs,
    ReadSessionOptions options = {});
```

Cursor 对上层只暴露 `currentRow()`：它始终表示当前 run 的下一条逻辑有序 row。内部按
`materializedSegment` 的 chunk/part 顺序扫描。

`compareCurrentRows()` 只比较窗口内已经有效的 row pointer，不触发单行 IO。

## 11. 释放和消费语义

不是所有读过的数据都需要再次 spill 回去。很多场景下，segment 被输出或 merge 消费后就不会再用，
这时应该显式释放。

```cpp
enum class ReleaseReason {
  kConsumed,   // 数据已经输出或合并完，不再需要
  kDiscarded,  // 算子提前结束或 clear
};

void releaseSegment(SegmentId segment, ReleaseReason reason);
void releaseSegments(folly::Range<const SegmentId*> segments, ReleaseReason reason);
```

释放语义：

```text
releaseSegment
  -> 要求没有 active read/merge session 持有该 segment
  -> 释放 row/heap block handles
  -> 删除 segment/chunk/part metadata
  -> 删除 RowId 到该 segment 的可访问性
```

`releaseSegment()` 和 flush 不同：flush 是把 resident 数据变成可读回的 finalized segment；
release 是销毁数据，之后对应 `RowId` 不再可用。

## 12. API 汇总

下面是接口层面的建议，实际实现可以拆分文件和类，但语义应保持一致。

```cpp
class BmRowContainer {
 public:
  BmRowContainer(
      std::vector<TypePtr> types,
      std::vector<bool> nullable,
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      memory::bm::MemoryTag tag,
      ...);

  char* newRow();
  char* newRow(PartitionId partition);

  void store(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      char* row,
      int32_t column);

  void storeColumn(
      const DecodedVector& decoded,
      vector_size_t size,
      char* const* rows,
      int32_t column);

  int32_t compare(
      const char* left,
      const char* right,
      int32_t column,
      CompareFlags flags = {});

  int32_t compareRows(
      const char* left,
      const char* right,
      const std::vector<CompareFlags>& flags = {});

  SegmentId flushActiveSegment();
  SegmentId flushActivePartitionSegment(PartitionId partition);

  SortedRunId finalizeSortedRun(
      folly::Range<char* const*> sortedRows,
      const SortedRunOptions& options);

  BulkReadSession beginBulkReadSegments(
      folly::Range<const SegmentId*> segments,
      ReadSessionOptions options = {});

  MergeReadSession beginMergeReadSegments(
      folly::Range<const SortedRunId*> runs,
      ReadSessionOptions options = {});

  void releaseSegment(SegmentId segment, ReleaseReason reason);
  void releaseSegments(
      folly::Range<const SegmentId*> segments,
      ReleaseReason reason);
};
```

## 13. 与现有 RowContainer 的差异

| 维度 | 现有 `RowContainer` | 新 `BmRowContainer` |
| --- | --- | --- |
| 长期 row identity | `char*` | window fallback 下构造的 `RowId` |
| 快路径 row access | `char*` | resident/session 内 `char*` |
| flush 后 pointer | 不适用 | 全部失效，上层清空 |
| variable-width slot | `StringView` | `StringView` |
| heap block 地址变化 | 普通内存不变化 | ChunkPart pointer rebasing |
| 超内存 bulk extract | 依赖算子 spiller/reader | `BulkReadSession` |
| sorted run merge | spill reader | `MergeReadSession` cursor |
| HashBuild 多次 flush | spiller partition | partition active segments |
| comparator IO | 不会 | 不允许 |
| 消费后释放 | clear/spiller 生命周期 | `releaseSegment(s)` |

## 14. 第一版边界

为了保证第一版可落地，建议明确以下边界：

- 只支持 `VARCHAR` / `VARBINARY` 的 pointer rebasing；复杂类型后续扩展。
- 单个 out-of-line value 不跨 heap block；大 value 使用 dedicated large block。
- Sort/HashAgg 的 sorted run 使用 `kMaterializedOrder`，按排序结果物理重排写出。
- HashJoin flushed partition 的单行访问只能通过 `BulkReadSession::loadRow()` 显式慢路径发生。
- HashJoin flushed partition 第一版必须走 partition restore：读回 build partition、重建局部
  resident hash table、再处理对应 probe partition。
- 如果某个 read window 连一个 row block 加必要 heap blocks 都无法 pin，直接报内存不足；
  不设计比 row block 更小的 IO 粒度。

## 15. 实现顺序建议

1. 实现 row/heap block 写入、resident pointer compare/extract，以及可由 chunk/part 元数据构造的
   `RowId`。
2. 实现 Segment、DataChunk、ChunkPart 元数据生成。
3. 实现 variable-width `StringView` 存储和 ChunkPart pointer rebasing。
4. 实现 `flushActiveSegment()`、`flushActivePartitionSegment()` 和 finalized segment 状态管理。
5. 实现 `BulkReadSession` 的全量加载尝试和 window read。
6. 实现 `SortedRunMeta`、`finalizeSortedRun()` 和 `MergeReadSession`，只支持
   `kMaterializedOrder`。
7. 实现 `releaseSegment(s)`，支持 read/merge session 结束后的显式销毁。
8. 实现 partitioned active segments，支持每个 partition 多次 flush。
9. 为 RowContainer 本体补齐单元测试和压力测试：resident 快路径、flush/read、window read、
   pointer rebasing、sorted run cursor、release lifecycle。
