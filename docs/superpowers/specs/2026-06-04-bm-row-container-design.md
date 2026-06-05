# BmRowContainer 容器设计

## 1. 目标和边界

新增一个独立的 `BmRowContainer`，用于验证 row-format 中间数据是否能更好地接入
task-level `BufferManager`。现有 `RowContainer` 不改，现有使用方不受影响。

这份文档只描述容器本身。Window 侧如何消费 `BmRowContainer`，见
`docs/superpowers/specs/2026-06-04-bm-row-container-window-integration.md`。

当前设计目标：

- fixed row 存在 BM row blocks；
- variable-width bytes 存在 BM heap blocks；
- 对外 row identity 从长期 `char*` 改为稳定 `RowId`；
- fixed row 内的变长字段保存 offset 引用，不保存 heap pointer；
- 使用 `BmPressureAwareBlockArena` 统一管理 BM blocks，row/heap 只是 `BmRowContainer` 对 block 的解释；
- 内存充足时 blocks 可以保持 pinned，读写路径复用已 pinned 的 block data；
- 需要新 block 或 pin block 且 `MaybeReserve` 失败时，由 arena 释放可回收 pins，再用
  `SpillBlocks` 下刷可回收 blocks；
- benchmark 对比容器层 write、spill、read memory、read spill 四类能力。

## 2. 为什么不能沿用 char*

现有 `RowContainer` 的操作模型建立在长期有效的 `char*` row 地址上：

- `RowContainer::newRow()` 返回 `char*`；
- 调用方会把这些指针保存在 row vector、partition range、sort rows 等结构中；
- `RowContainer::extractColumn()`、`extractNulls()`、`compare()` 等接口直接解引用这些指针；
- 变长值可能通过 view 指向容器内部管理的字符串内存。

`BufferManager` 的地址语义不同：

- `BlockHandle` 是稳定的 block 身份；
- `BufferHandle::Ptr()` 只在 block 被 pin 住期间有效；
- block unpin 并被 reclaim 后，同一个 block 再次 pin 回来时虚拟地址可能变化。

因此 `BmRowContainer` 不把 `char*` 暴露成稳定 row identity。对外只暴露稳定逻辑位置，
容器内部在访问时根据逻辑位置 pin 对应 block 并生成临时地址。

核心不变量：

```text
外部长期保存 RowId。
容器内部在访问期间把 RowId 转成临时 char*。
临时 char* 的生命周期必须被对应 BufferHandle 覆盖，且不能逃逸。
```

## 3. 共享接口契约

本节是 `BmRowContainer` 与 Window 集成文档共同使用的接口定义。两份文档必须保持一致；如果接口
需要调整，以本节为准，并同步更新 Window 集成文档。

`RowId` 是稳定 row identity：

```cpp
struct RowId {
  uint32_t blockId;
  uint32_t rowOffset;
};
```

`RowId` 通过 block id 和 block 内 offset 定位 fixed row。它可以安全保存在
`std::vector<RowId>` 中，并跨 unpin/reclaim 长期存在。

fixed row 内的变长字段保存 `VarData`：

```cpp
struct VarData {
  uint32_t blockId;
  uint32_t offset;
  uint32_t size;
};
```

核心公开操作都基于 `RowId`：

```cpp
class BmRowContainer {
 public:
  RowId newRow();

  void store(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      RowId row,
      int32_t column);

  void store(const RowVectorPtr& input);

  int32_t compare(
      RowId left,
      RowId right,
      int32_t column,
      CompareFlags flags = {});

  int32_t compareRows(
      RowId left,
      RowId right,
      const std::vector<CompareFlags>& flags = {});

  void extractColumn(
      folly::Range<const RowId*> rows,
      int32_t column,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize = false);

  void extractNulls(
      folly::Range<const RowId*> rows,
      int32_t column,
      const BufferPtr& result);

  int64_t numRows() const;
  int32_t fixedRowSize() const;
  uint64_t allocatedBytes() const;
  uint64_t usedBytes() const;
  std::optional<int64_t> estimateRowSize() const;

  RowColumn columnAt(int32_t index) const;
  const std::vector<RowColumn>& columns() const;
  const std::vector<TypePtr>& columnTypes() const;
  const std::vector<TypePtr>& keyTypes() const;

  void clear();
};
```

`BmRowContainer` 不暴露 `BufferHandle` 或稳定 `char*`。所有 `char*` 都是容器内部临时访问地址，
调用方只能长期保存 `RowId`。

## 4. 操作模型

`BmRowContainer` 的公开操作以 `RowId` 为输入输出，不向调用方返回可长期保存的 row 指针。

| 操作 | 语义 |
| --- | --- |
| `newRow()` | 在 active row block 中创建 fixed row，返回 `RowId` |
| `store(decoded, sourceIndex, RowId, column)` | 把指定列写入 `RowId` 对应 row |
| `store(RowVectorPtr)` | 批量写入输入 batch，返回 rows 对应的 `RowId` 列表 |
| `extractColumn(RowId range, column, ...)` | 按 `RowId` 列表提取指定列 |
| `extractNulls(RowId range, column, ...)` | 按 `RowId` 列表提取 null bits |
| `compare(RowId, RowId, column, flags)` | 比较两行指定列 |
| `compareRows(RowId, RowId, flags)` | 按 key columns 比较两行 |
| `clear()` | 清空 block handles、pins 和统计状态 |

所有内部访问都通过 `BmPressureAwareBlockArena::pinnedData()` 把 `RowId` 或 `VarData`
转成短生命周期 `char*`。这些地址只在 `BmRowContainer` 方法执行期间使用，不写入外部状态。

## 5. 存储布局

`BmRowContainer` 使用 row-format fixed layout：

- key types 在前；
- dependent types 在 key 后；
- 使用 `RowColumn` 描述字段 offset 和 null bit；
- null flags 放在 fixed row bytes 里；
- fixed-width 值 inline 存储。

物理存储使用一组统一的 BufferManager blocks：

```text
BmRowContainer
  BmPressureAwareBlockArena blocks_
    block 0: fixed rows
    block 1: variable-width bytes
    block 2: fixed rows
    block 3: variable-width bytes
    ...
```

`BmPressureAwareBlockArena` 不理解 row/heap，也不保存 block kind。它只管理 block 生命周期。
`BmRowContainer` 通过 `activeRowBlockId_`、`activeHeapBlockId_`、`RowId` 和 `VarData`
解释某个 block 的用途。

arena block 元数据：

```cpp
struct BmBlockState {
  std::shared_ptr<memory::bm::BlockHandle> block;
  std::optional<memory::bm::BufferHandle> pinnedHandle;
  char* data{nullptr};
  uint32_t capacity;
  uint32_t usedBytes;
  uint32_t liveRows;
  uint64_t lastAccess;
};
```

row block 和 heap block 都使用 `bytedance::bolt::memory::bm::AllocateSize::kLarge`，
也就是 `allocateSizeBytes(AllocateSize::kLarge)` 对应的 4MB block。`capacity` 记录每个
block 的实际容量，允许后续扩展出不同大小的 blocks。

`pinnedHandle` 表示 `BmRowContainer` 当前是否持有这个 block 的 pin。`data` 只在
`pinnedHandle.has_value()` 时有效，必须和 `pinnedHandle` 同步更新。不要在 `BmBlockState`
里单独维护一个 `bool pinned`，因为 pin 状态应该由 RAII handle 本身表达：
`pinnedHandle` 存在即容器持有 pin，`pinnedHandle.reset()` 即释放 pin 并清空 `data`。
`BlockMemory::pinCount` 是 BufferManager 的全局内部状态，`BmRowContainer` 不直接读取或修改它。

`lastAccess` 用于内存压力下选择优先释放哪些可回收 blocks。arena 用单调递增的
`accessClock_` 更新：

```cpp
block.lastAccess = ++accessClock_;
```

## 6. 变长字段

变长值在 `store()` 时直接转成 offset 引用。

不要在 fixed row 里保存 heap `char*` 指针，而是保存 `VarData`。`VARCHAR` 和 `VARBINARY`
的 fixed row 槽位保存 `VarData`，实际 bytes 写入 heap block。

写入流程：

```text
DecodedVector value
  -> 从当前 heap block 分配 bytes
  -> 把 bytes copy 到 pinned heap block
  -> 在 fixed row 中写入 VarData{blockId, offset, size}
```

读取、比较、提取流程：

```text
RowId
  -> arena.pinnedData(blockId)
  -> 从 fixed row 读 VarData
  -> arena.pinnedData(blockId)
  -> data = heapHandle.Ptr() + offset
```

fixed row 里没有 heap 地址，因此 block unpin/re-pin 后不需要修复 pointer。

## 7. Append 和 pin 生命周期

append 热路径需要持有当前可写 blocks 的 `BufferHandle`，避免每一行每一列都 pin/unpin。
读路径复用 `BmPressureAwareBlockArena` 中的 `pinnedHandle`，在内存充足时让读过的 blocks
保持 pinned；当 `MaybeReserve` 失败时，再按 `lastAccess` 释放可回收 blocks。

内存充足时：

- 当前 active row block 保持 pinned；
- 当前 active heap block 保持 pinned；
- 写满或读过的可回收 blocks 可以继续 pinned，直到内存压力触发 arena reclaim；
- 新 row 返回 `RowId`，不会把 `char*` 暴露给调用方。

当需要新 row/heap block 时：

```text
当前 active block 空间不足
  -> 需要分配新 block
  -> MaybeReserve(newBlockSize)
  -> 成功：分配新 block，并保持新 block pinned
  -> 失败：进入 spill 触发流程
```

可以增加 batch appender 作为 batch 写入的轻量状态：

```cpp
class BmRowAppender {
 public:
  RowId newRow();
  void store(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      RowId row,
      int32_t column);
};
```

容器层 benchmark 和后续 StreamingWindowBuild 每个 input batch 可以创建一个 appender。

## 8. BmPressureAwareBlockArena

`BmPressureAwareBlockArena` 是 `BmRowContainer` 内部通用 block 生命周期管理器。它统一持有
BufferManager blocks，并实现 pressure-aware 策略：

```text
内存充足:
  访问过的 blocks 保持 pinned，后续读写直接复用 data。

MaybeReserve 失败:
  按 lastAccess 释放 canReclaim(blockId) 允许释放的 pinned blocks。
  对已经 unpinned 且 canReclaim(blockId) 的 blocks 调用 SpillBlocks。
  再次 MaybeReserve，仍失败则报错。
```

arena 不理解 row/heap，也不判断 active block。`BmRowContainer` 通过 `canReclaimBlock(blockId)`
告诉 arena 哪些 block 当前可以被 unpin/spill：

```cpp
bool BmRowContainer::canReclaimBlock(uint32_t blockId) const {
  return blockId != activeRowBlockId_ &&
         blockId != activeHeapBlockId_;
}
```

### 8.1 Arena 接口

```cpp
class BmPressureAwareBlockArena {
 public:
  static constexpr uint32_t kInvalidBlockId =
      std::numeric_limits<uint32_t>::max();

  using CanReclaimFn = folly::FunctionRef<bool(uint32_t blockId)>;

  BmPressureAwareBlockArena(
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      memory::bm::MemoryTag tag);

  uint32_t allocateBlock(uint32_t capacity, CanReclaimFn canReclaim);
  char* pinnedData(uint32_t blockId, CanReclaimFn canReclaim);
  char* activeData(uint32_t blockId);

  BmBlockState& block(uint32_t blockId);
  const BmBlockState& block(uint32_t blockId) const;

  bool empty() const;
  uint32_t size() const;
  uint64_t allocatedBytes() const;
  uint64_t usedBytes() const;

  uint64_t makeBlocksReclaimable(
      uint64_t targetBytes,
      CanReclaimFn canReclaim);

  std::vector<std::shared_ptr<memory::bm::BlockHandle>>
  reclaimableBlocks(CanReclaimFn canReclaim) const;

  void spillReclaimableBlocks(
      uint64_t targetBytes,
      CanReclaimFn canReclaim);

  void clear();
};
```

### 8.2 allocateBlock

`allocateBlock(capacity, canReclaim)` 用于 append 需要新的 row/heap block：

```text
ensureMemoryForBlock(capacity, canReclaim)
  -> BufferManager::Allocate(capacity, tag)
  -> 保存 BlockHandle、BufferHandle、data、capacity、lastAccess
  -> 返回 blockId
```

新 block 初始保持 pinned。调用方负责解释它是 row block 还是 heap block，并更新
`activeRowBlockId_` 或 `activeHeapBlockId_`。

### 8.3 pinnedData 和 activeData

`pinnedData(blockId, canReclaim)` 用于访问任意 block：

```text
如果 block 已经 pinned:
  更新 lastAccess，返回 data。

如果 block 未 pinned:
  ensureMemoryForBlock(block.capacity, canReclaim)
  BufferManager::Pin(block.block)
  保存 BufferHandle 和 data
  更新 lastAccess
  返回 data。
```

`activeData(blockId)` 只用于当前 active row/heap block：

```text
要求 block 已经 pinned。
更新 lastAccess。
返回 data。
```

### 8.4 ensureMemoryForBlock

arena 在两个安全点准备 block 级内存：

- append 需要分配新的 row/heap block；
- 读写任意 block 时发现目标 block 当前没有 pinned handle，需要重新 pin。

两类路径都通过 arena 内部 helper：

```cpp
void BmPressureAwareBlockArena::ensureMemoryForBlock(
    uint32_t capacity,
    CanReclaimFn canReclaim,
    std::string_view failureMessage) {
  BOLT_CHECK_GT(capacity, 0);

  if (bufferManager_->MaybeReserve(capacity)) {
    return;
  }

  makeBlocksReclaimable(capacity, canReclaim);

  auto candidates = reclaimableBlocks(canReclaim);
  if (!candidates.empty()) {
    bufferManager_->SpillBlocks(candidates);
  }

  if (bufferManager_->MaybeReserve(capacity)) {
    return;
  }

  BOLT_FAIL("{}", failureMessage);
}
```

`MaybeReserve(capacity)` 在 append 分配新 block 时是精确需求；在 pin 已存在 block 时是保守需求，
表示“如果目标 block 已经被 spill，最坏需要读回一个 block 的内存”。如果目标 block 仍 resident，
后续 `Pin` 可能不消耗新的 payload 内存，`ReleaseUnusedReservation()` 会释放多余 reservation。

### 8.5 makeBlocksReclaimable

`makeBlocksReclaimable()` 负责销毁可回收 blocks 的 `BufferHandle`，让它们从 pinned 变成
unpinned，并清空对应 `data`：

```cpp
uint64_t BmPressureAwareBlockArena::makeBlocksReclaimable(
    uint64_t targetBytes,
    CanReclaimFn canReclaim);
```

语义：

```text
targetBytes > 0:
  按 lastAccess 从冷到热 unpin canReclaim(blockId) 为 true 的 blocks，
  累计 block size >= targetBytes 后停止。

targetBytes == 0:
  unpin 所有 canReclaim(blockId) 为 true 的 pinned blocks。
```

它只释放 pin，不直接释放内存，也不直接 spill。返回值是被 unpin 的 block size 累计值，不是
MemoryPool 已经 reclaimed 的真实字节数。

`BmRowContainer` 传入的 `canReclaimBlock()` 会排除当前 active row block 和 active heap block。
`BmRowContainer` 按单线程访问设计，pin/unpin 路径不处理并发读写 scope。

### 8.6 reclaimableBlocks 和 spillReclaimableBlocks

`reclaimableBlocks()` 只返回已经 unpinned、且 `canReclaim(blockId)` 为 true 的 blocks：

```cpp
std::vector<std::shared_ptr<memory::bm::BlockHandle>>
BmPressureAwareBlockArena::reclaimableBlocks(CanReclaimFn canReclaim) const;
```

`spillReclaimableBlocks()` 是组合 helper：

```cpp
void BmPressureAwareBlockArena::spillReclaimableBlocks(
    uint64_t targetBytes,
    CanReclaimFn canReclaim);
```

流程：

```text
makeBlocksReclaimable(targetBytes)
  -> reclaimableBlocks(canReclaim)
  -> BufferManager::SpillBlocks(blocks)
```

`spillReclaimableBlocks(0, canReclaim)` 表示 unpin 并 spill 所有可回收 blocks。benchmark
强制 readback 复用这个接口。

如果第二次 `MaybeReserve(blockSize)` 仍然失败，直接向上抛出内存不足错误。

这个策略的好处是：

- spill 发生在 block 边界，不会破坏半写入 row；
- 对固定行和变长数据统一适用；
- 上层仍然只持有 `RowId` 和 `VarData`，不会因为 block 被 spill 而失效；
- read path 命中 `BmBlockState::pinnedHandle` 时可以直接使用 `data`，避免重复 `Pin`；
- benchmark 复用同一机制，`spillAllBlocksForBenchmark()` 调用 arena 的
  `spillReclaimableBlocks(0, canReclaim)` 做强制 readback 测试。

## 9. 读写路径的 pinned block 访问

读写路径通过 `BmPressureAwareBlockArena` 访问 block。`BmRowContainer` 持有：

```cpp
BmPressureAwareBlockArena blocks_;
uint32_t activeRowBlockId_{BmPressureAwareBlockArena::kInvalidBlockId};
uint32_t activeHeapBlockId_{BmPressureAwareBlockArena::kInvalidBlockId};
```

`RowId::blockId` 和 `VarData::blockId` 都指向 `blocks_` 中的统一 block id。

row 访问：

```text
pinRow(RowId):
  blocks_.pinnedData(row.blockId, canReclaim) + row.rowOffset
```

变长访问：

```text
stringView(row, column):
  从 fixed row 读 VarData
  blocks_.pinnedData(ref.blockId, canReclaim) + ref.offset
```

写路径：

```text
newRow():
  如果 active row block 无空间，blocks_.allocateBlock(rowBlockSize_, canReclaim)
  使用 blocks_.activeData(activeRowBlockId_) 写 fixed row

appendVariableWidth():
  如果 active heap block 无空间，blocks_.allocateBlock(heapBlockSize_, canReclaim)
  使用 blocks_.activeData(activeHeapBlockId_) 写 bytes

store(decoded, sourceIndex, RowId, column):
  使用 blocks_.pinnedData(row.blockId, canReclaim) 写指定 row
```

这个设计的语义是 pressure-driven pinned arena：读过或写过的可回收 block 可以继续 pinned，
直到 `MaybeReserve` 失败时由 arena 按冷到热释放。


## 10. BufferManager 来源

使用 commit `7de0785683d8e8a4ddc59149929957edee7f8da9` 新增的 task-level BufferManager。

容器创建时需要传入：

- `std::shared_ptr<memory::bm::BufferManager>`；
- row block size；
- heap block size；
- `memory::bm::MemoryTag`，默认使用 `memory::bm::MemoryTag::kWindow`。

如果 BM 不可用，`BmRowContainer` 构造失败。

## 11. 内存统计和 RAII Unpin

容器暴露近似内存指标：

```cpp
uint64_t allocatedBytes() const;
uint64_t usedBytes() const;
std::optional<int64_t> estimateRowSize() const;
```

行数据的 resident 内存释放依赖 `BufferHandle` 的 RAII unpin：

- append/read 阶段内存充足时，row/heap blocks 可以保持 pinned；
- `MaybeReserve` 失败时，arena 通过 `makeBlocksReclaimable()` 释放一批可回收 block handles；
- handles 析构后对应 blocks 变成 unpinned，随后通过 `BufferManager::SpillBlocks()` 下刷；
- `RowId` 和 `VarData` 仍然保存逻辑位置，不依赖 resident 地址。

`BlockHandle` 仍由 `BmRowContainer` 持有，用于后续 pin/readback。

## 12. 测试计划

新增 `BmPressureAwareBlockArena` 单测：

- `allocateBlock()` 创建 block 后保持 pinned；
- `pinnedData()` 命中已 pinned block 时复用 `data`；
- `makeBlocksReclaimable(targetBytes)` 按 `lastAccess` 优先 unpin 冷 blocks；
- `makeBlocksReclaimable(0)` unpin 所有 `canReclaim(blockId)` 为 true 的 pinned blocks；
- `makeBlocksReclaimable()` 跳过 `canReclaim(blockId)` 为 false 的 blocks；
- `spillReclaimableBlocks(0)` unpin 并 spill 所有可回收 blocks，后续 `pinnedData()` 可读回；
- `MaybeReserve` 第二次失败时直接报错。

`BmRowContainer` 单测：

- fixed-width append/extract round trip；
- nullable columns；
- variable-width string append/extract round trip；
- variable-width 数据在 row block 和 heap block unpin/re-pin 后仍可读取；
- fixed-width column compare；
- variable-width column compare；
- 多 row blocks；
- 多 heap blocks；
- `RowId` 在 BM reclaim 和 re-pin 后仍有效；
- `RowId::blockId` 和 `VarData::blockId` 都通过统一 arena block id 正确定位；
- active row/heap block 被 `canReclaimBlock()` 保护，spill 后仍可继续 append；
- `spillAllBlocksForBenchmark()` 触发 arena spill 后 readback 数据仍正确。

回归测试：

- 现有 `RowContainerTest`；
- task-level BufferManager tests。

## 13. Benchmark 计划

benchmark 放在 `bolt/exec/bm/benchmarks`，只比较容器层能力。目标可执行文件：

```text
_build/Release/bolt/exec/bm/benchmarks/bolt_bm_row_container_benchmark
```

benchmark 对比 `RowContainer` 和 `BmRowContainer` 在相同输入数据下的四类场景：

```text
Write:
  只测写入容器的成本。

Spill:
  写入完成后触发 spill，测 spill 写出成本。

ReadMemory:
  数据保持 resident/pinned 策略下，测从容器读取列的成本。

ReadSpill:
  数据 spill 后再读取，测 readback + extract 成本。
```

Benchmark 数据集至少覆盖三类输入：

```text
fixed_int64:
  BIGINT

mixed_fixed:
  INTEGER, BIGINT, DOUBLE, BOOLEAN

varchar_payload:
  BIGINT key, VARCHAR payload，payload 使用随机字符
```

数据规模用参数控制。默认按 GiB 级数据量生成：

```text
--bm_row_container_data_gib=<N>
--bm_row_container_pool_capacity_gib=<N>
--bm_row_container_print_stats=<true|false>
```

benchmark 文件按场景拆分：

```text
BmRowContainerWriteBenchmark.cpp
BmRowContainerSpillBenchmark.cpp
BmRowContainerReadMemoryBenchmark.cpp
BmRowContainerReadSpillBenchmark.cpp
BmRowContainerBenchmarkUtil.cpp
```

BmRowContainer spill case 的流程：

```text
1. 创建 BufferManager，spill directory 使用 /tmp 下临时目录。
2. 创建 BmRowContainer，使用 MemoryTag::kWindow。
3. append/store 全部 rows。
4. 调用 `spillAllBlocksForBenchmark()` 释放并 spill arena 中所有可回收 blocks。
5. 执行 readback workload：
   - extractColumn 全量或按 batch 提取；
   - compare 相邻 rows。
6. 收集 BufferManagerStats 和 benchmark 耗时。
```

benchmark 使用受控接口主动触发 spill：

```cpp
void spillAllBlocksForBenchmark(); // 内部调用 blocks_.spillReclaimableBlocks(0, canReclaim)
```

每个 case 输出：

```text
caseName
rowCount
schema
stringSize
nullRatio
appendTimeMs
spillTimeMs
readbackTimeMs
extractTimeMs
compareTimeMs
allocatedBytes
usedBytes
bmSpilledBytes
bmStats.debugString()
```

Benchmark 注册在 `bolt/exec/bm/benchmarks/CMakeLists.txt`：

```cmake
add_executable(bolt_bm_row_container_benchmark BmRowContainerBenchmark.cpp)

target_link_libraries(
  bolt_bm_row_container_benchmark PRIVATE
    bolt_testutils
    ${FOLLY_BENCHMARK}
    GTest::gtest_main
)
```

## 14. 主要设计决策

1. `newRow()` 返回 `RowId`，不返回 `char*`。
2. fixed row 存在 BM row blocks。
3. variable-width bytes 存在 BM heap blocks。
4. fixed row 存 `VarData`，不存 heap pointer。
5. `BmPressureAwareBlockArena` 统一管理所有 BM blocks，不区分 row/heap kind。
6. `RowId::blockId` 和 `VarData::blockId` 都引用 arena 中的统一 block id。
7. `BmRowContainer` 通过 `activeRowBlockId_`、`activeHeapBlockId_` 和 `canReclaimBlock()` 定义
   哪些 blocks 不能被 unpin/spill。
8. 读写路径通过 `blocks_.pinnedData()` 把 `RowId` / `VarData` 转成容器内部临时 `char*`，不对外暴露。
9. row/heap blocks 统一使用 `AllocateSize::kLarge`。
10. `BmBlockState` 用 `std::optional<BufferHandle>` 表达 arena 是否持有 pin。
11. `BmBlockState::data` 只在 `pinnedHandle` 存在时有效，释放 pin 时必须清空。
12. 内存充足时 append/read blocks 可以保持 pinned。
13. 运行时在需要新 block 或 pin block 时先 `MaybeReserve`，失败后 arena 根据 `canReclaimBlock()`
    释放 pins 并 `SpillBlocks` 可回收 blocks。
14. `makeBlocksReclaimable(0)` 表示 unpin 所有可回收 pinned blocks；
    `spillReclaimableBlocks(0, canReclaim)` 表示 unpin 并 spill 所有可回收 blocks。
15. 第二次 `MaybeReserve` 失败直接报错。
16. benchmark 只做容器层 write/spill/read memory/read spill 对比。
17. 现有 `RowContainer` 对当前使用方保持不变。
