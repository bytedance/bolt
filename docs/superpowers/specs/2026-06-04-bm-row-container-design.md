# BmRowContainer 容器设计

## 1. 目标和边界

新增一个独立的 `BmRowContainer`，用于验证 row-format 中间数据是否能更好地接入
task-level `BufferManager`。现有 `RowContainer` 不改，现有使用方不受影响。

这份文档只描述容器本身。Window 侧如何消费 `BmRowContainer`，见
`docs/superpowers/specs/2026-06-04-bm-row-container-window-integration.md`。

第一版目标：

- fixed row 存在 BM row blocks；
- variable-width bytes 存在 BM heap blocks；
- 对外 row identity 从长期 `char*` 改为稳定 `BmRowRef`；
- fixed row 内的变长字段保存 offset 引用，不保存 heap pointer；
- 内存充足时 append blocks 可以保持 pinned；
- 需要新 block 且 `MaybeReserve` 失败时，释放冷 pins 并用 `SpillBlocks` 下刷；
- 第一版 benchmark 只比较容器层能力，不接入 Window operator。

第一版非目标：

- aggregate accumulator 存储；
- hash join `next` 链；
- probed flag；
- normalized key/JIT；
- `RowContainer::listRows(char**)` 兼容；
- row-based spiller 兼容；
- 多 segment combine 语义。

## 2. 为什么不能沿用 char*

`RowContainer` 的现有操作模型建立在长期有效的 `char*` row 地址上：

- `RowContainer::newRow()` 返回 `char*`；
- 调用方会把这些指针保存在 row vector、partition range、sort rows 等结构中；
- `RowContainer::extractColumn()`、`extractNulls()`、`compare()` 等接口直接解引用这些指针；
- `RowContainer` 把变长值存成 `StringView` 或 `std::string_view`，这些 view 可能指向
  `HashStringAllocator` 管理的内存。

`BufferManager` 的地址语义不同：

- `BlockHandle` 是稳定的 block 身份；
- `BufferHandle::Ptr()` 只在 block 被 pin 住期间有效；
- block unpin 并被 reclaim 后，同一个 block 再次 pin 回来时虚拟地址可能变化。

因此 `BmRowContainer` 不能把 `char*` 暴露成稳定 row identity。否则只有两种结果：

- 为了保证 `char*` 永远有效，所有 blocks 必须长期 pinned，BM 无法发挥 reclaim/spill 价值；
- 允许 block unpin/reload 后，旧 `char*` 会失效，后续解引用变成未定义行为。

核心不变量：

```text
外部长期保存 BmRowRef。
容器内部在访问期间把 BmRowRef 转成临时 char*。
临时 char* 的生命周期必须被对应 BufferHandle 覆盖，且不能逃逸。
```

## 3. 共享接口契约

本节是 `BmRowContainer` 与 Window 集成文档共同使用的接口定义。两份文档必须保持一致；如果接口
需要调整，以本节为准，并同步更新 Window 集成文档。

`BmRowRef` 是稳定 row identity：

```cpp
struct BmRowRef {
  uint32_t rowBlockId;
  uint32_t rowOffset;
};
```

`BmRowRef` 通过 row block 和 block 内 offset 定位 fixed row。它可以安全保存在
`std::vector<BmRowRef>` 中，也可以跨 unpin/reclaim 长期存在。

fixed row 内的变长字段保存 `BmVarRef`：

```cpp
struct BmVarRef {
  uint32_t heapBlockId;
  uint32_t heapOffset;
  uint32_t size;
};
```

核心公开操作都基于 `BmRowRef`：

```cpp
class BmRowContainer {
 public:
  BmRowRef newRow();

  void store(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      BmRowRef row,
      int32_t column);

  void store(const RowVectorPtr& input);

  int32_t compare(
      BmRowRef left,
      BmRowRef right,
      int32_t column,
      CompareFlags flags = {});

  int32_t compareRows(
      BmRowRef left,
      BmRowRef right,
      const std::vector<CompareFlags>& flags = {});

  void extractColumn(
      folly::Range<const BmRowRef*> rows,
      int32_t column,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize = false);

  void extractNulls(
      folly::Range<const BmRowRef*> rows,
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

`BmRowContainer` 不暴露 `PinnedRows`、`BufferHandle` 或稳定 `char*`。

## 4. RowContainer 接口取舍

第一版必须重做的接口：

| RowContainer 接口 | BmRowContainer 对应接口 | 第一版状态 |
| --- | --- | --- |
| `char* newRow()` | `BmRowRef newRow()` | 必须实现 |
| `store(decoded, index, char*, column)` | `store(decoded, index, BmRowRef, column)` | 必须实现 |
| `store(RowVectorPtr)` | `store(RowVectorPtr)` | 建议实现，便于 benchmark |
| `extractColumn(char**/rowNumbers, ...)` | `extractColumn(BmRowRef range, ...)` | 必须实现 |
| `extractNulls(char**, ...)` | `extractNulls(BmRowRef range, ...)` | 必须实现 |
| `compare(char*, char*, column, flags)` | `compare(BmRowRef, BmRowRef, column, flags)` | 必须实现 |
| `compareRows(char*, char*, flags)` | `compareRows(BmRowRef, BmRowRef, flags)` | 必须实现 |
| `eraseRows(char**)` | 暂无强制对应接口 | 第一版不实现 |
| `numRows()` | `numRows()` | 必须实现 |
| `fixedRowSize()` | `fixedRowSize()` | 必须实现 |
| `allocatedBytes()/usedBytes()` | `allocatedBytes()/usedBytes()` | 必须实现 |
| `estimateRowSize()` | `estimateRowSize()` | 必须实现 |
| `columnAt()/columns()/columnTypes()/keyTypes()` | 同名 accessor | 必须实现 |

第一版明确不实现的接口：

| RowContainer 接口 | 不实现原因 |
| --- | --- |
| `extractSerializedRows()` / `storeSerializedRow()` | 服务现有 spiller，不是第一版容器目标 |
| `copySerializedRow()` | 依赖现有 row serialization 格式 |
| `extractProbedFlags()` / `setProbedFlag()` | join 专用 |
| `hash()` | hash table 专用 |
| `listRows(char**)` / `listPartitionRows()` / `createRowPartitions()` | 强绑定长期 `char*` |
| `normalizedKey()` / `disableNormalizedKeys()` | JIT/sort/hash 优化，第一版不做 |
| `JITable()` / `codegenCompare()` / `codegenRowEqVectors()` | JIT 第一版不做 |
| `stringAllocator()` / `stringAllocatorShared()` | `BmRowContainer` 不使用 `HashStringAllocator` |
| `pool()` | BM 管理自己的 leaf pool，不向使用方暴露为 row 分配入口 |
| `prepareRead()` | 依赖 `HashStringAllocator` 链式数据 |

## 5. 存储布局

`BmRowContainer` 复用 `RowContainer` 的 row layout 思路：

- key types 在前；
- dependent types 在 key 后；
- 使用 `RowColumn` 描述字段 offset 和 null bit；
- null flags 放在 fixed row bytes 里；
- fixed-width 值 inline 存储。

物理存储从 `AllocationPool + HashStringAllocator` 改成 BufferManager blocks：

```text
BmRowContainer
  rowBlocks_
    block 0: fixed rows
    block 1: fixed rows
    ...

  heapBlocks_
    block 0: variable-width bytes
    block 1: variable-width bytes
    ...
```

block 元数据：

```cpp
struct BmBlockState {
  std::shared_ptr<memory::bm::BlockHandle> block;
  std::optional<memory::bm::BufferHandle> pinnedHandle;
  uint32_t usedBytes;
  uint32_t liveRows;
};
```

第一版 row block 和 heap block 都使用 `bytedance::bolt::memory::bm::AllocateSize::kLarge`，
也就是 `allocateSizeBytes(AllocateSize::kLarge)` 对应的 4MB block。`BmRowRef::rowOffset`
指向 fixed row 在 row block 内的起始位置。

`pinnedHandle` 表示 `BmRowContainer` 当前是否持有这个 block 的 pin。不要在 `BmBlockState`
里单独维护一个 `bool pinned`，因为 pin 状态应该由 RAII handle 本身表达：`pinnedHandle`
存在即容器持有 pin，`pinnedHandle.reset()` 即释放 pin。也不要为了这个需求修改 `BlockMemory`；
`BlockMemory::pinCount` 是 BufferManager 的全局内部状态，不适合作为 `BmRowContainer` 的局部
append/read 生命周期标记。

## 6. 变长字段

变长值在 `store()` 时直接转成 offset 引用。

不要在 fixed row 里保存 heap `char*` 指针，而是保存 `BmVarRef`。对于 `VARCHAR` 和
`VARBINARY`，`RowContainer` 里原来放 `StringView` 的 fixed row 位置，在 `BmRowContainer` 中改为
放 `BmVarRef`。对于序列化成 bytes 的 complex types，也同样放 `BmVarRef`。

写入流程：

```text
DecodedVector value
  -> 从当前 heap block 分配 bytes
  -> 把 bytes copy 到 pinned heap block
  -> 在 fixed row 中写入 BmVarRef{heapBlockId, heapOffset, size}
```

读取、比较、提取流程：

```text
BmRowRef
  -> pin row block
  -> 从 fixed row 读 BmVarRef
  -> pin heap block
  -> data = heapHandle.Ptr() + heapOffset
```

这样可以避开 DuckDB 的 `base_heap_ptr` 和 `RecomputeHeapPointers` 机制。fixed row 里没有旧 heap
地址，因此不存在需要修复的 stale pointer。

## 7. Append 和 pin 生命周期

append 热路径需要持有当前可写 blocks 的 `BufferHandle`，避免每一行每一列都 pin/unpin。

内存充足时：

- 当前 active row block 保持 pinned；
- 当前 active heap block 保持 pinned；
- 写满但仍希望留在内存中的历史 blocks 也可以继续 pinned；
- 新 row 返回 `BmRowRef`，不会把 `char*` 暴露给调用方。

当需要新 row/heap block 时：

```text
当前 active block 空间不足
  -> 需要分配新 block
  -> MaybeReserve(newBlockSize)
  -> 成功：分配新 block，并保持新 block pinned
  -> 失败：进入 spill 触发流程
```

建议增加 batch appender：

```cpp
class BmRowAppender {
 public:
  BmRowRef newRow();
  void store(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      BmRowRef row,
      int32_t column);
};
```

容器层 benchmark 和后续 StreamingWindowBuild 都可以每个 input batch 创建一个 appender。

## 8. Spill 触发策略

`BmRowContainer` 不在任意 row 访问过程中抢占式 spill，而是在需要分配新 block 的安全点触发。

推荐流程：

```cpp
bool BmRowContainer::ensureNewBlockReservation(size_t blockSize) {
  if (bm_->MaybeReserve(blockSize)) {
    return true;
  }

  releaseColdPins();

  auto candidates = collectSpillCandidates();
  if (!candidates.empty()) {
    bm_->SpillBlocks(candidates);
  }

  if (bm_->MaybeReserve(blockSize)) {
    return true;
  }

  BOLT_FAIL("BmRowContainer failed to reserve new block, size={}", blockSize);
}
```

`releaseColdPins()` 负责销毁冷 blocks 的 `BufferHandle`，让它们从 pinned 变成 unpinned。
`collectSpillCandidates()` 只能返回已经 unpinned、且当前没有被访问的 blocks：

- 已经写满或不再作为 append target 的 row blocks；
- 已经写满或不再作为 append target 的 heap blocks；
- 不能包含当前正在写入的 active row block；
- 不能包含当前正在写入的 active heap block；
- 不能包含当前 compare/extract scope 正在 pin 的 blocks。

如果第二次 `MaybeReserve(blockSize)` 仍然失败，直接向上抛出内存不足错误。第一版不做多轮 spill，
也不 fallback 到 `BufferManager::Reclaim(blockSize)`。

这个策略的好处是：

- spill 发生在 block 边界，不会破坏半写入 row；
- 对固定行和变长数据统一适用；
- 上层仍然只持有 `BmRowRef` 和 `BmVarRef`，不会因为 block 被 spill 而失效；
- benchmark 可以复用同一机制，也可以额外提供 `spillAllBlocksForBenchmark()` 做强制 readback 测试。

## 9. 读路径和 PinnedRows

`PinnedRows` 是 `BmRowContainer` 内部 RAII helper，不是 public API。

示例形态：

```cpp
class PinnedRows {
 public:
  const char* row(size_t index) const;

 private:
  std::vector<memory::bm::BufferHandle> rowHandles_;
  std::vector<const char*> rows_;
};
```

它的职责是把 `BmRowRef` 转成临时 `char*`，并保证这些 `char*` 的生命周期被对应
`BufferHandle` 覆盖。

它在 `BmRowContainer` 方法内部使用：

- `compare()` pin 左右 row block，并按被比较列需要 pin heap block；
- `extractColumn()` 为请求的 row range pin row blocks，并为变长值 pin heap blocks；
- `extractNulls()` 只需要 pin row blocks。

调用方不保存 `PinnedRows`，也不能从容器 API 获得长期有效的 `char*`。

## 10. BufferManager 来源

使用 commit `7de0785683d8e8a4ddc59149929957edee7f8da9` 新增的 task-level BufferManager。

容器创建时需要传入：

- `std::shared_ptr<memory::bm::BufferManager>`；
- row block size；
- heap block size；
- `memory::bm::MemoryTag`，第一版默认使用 `memory::bm::MemoryTag::kWindow`。

如果 BM 不可用，`BmRowContainer` 构造必须直接失败。不要在 `BmRowContainer` 内部 fallback 到
现有 `RowContainer`，也不要静默退回普通内存分配路径。

## 11. 内存统计和 RAII Unpin

容器暴露近似内存指标：

```cpp
uint64_t allocatedBytes() const;
uint64_t usedBytes() const;
std::optional<int64_t> estimateRowSize() const;
```

第一版不提供 `releaseRows()`。行数据的 resident 内存释放依赖 `BufferHandle` 的 RAII unpin：

- append 阶段内存充足时，当前可写 row/heap blocks 可以保持 pinned；
- `MaybeReserve` 失败时，容器释放一批冷 block 的 append/read handles；
- handles 析构后对应 blocks 变成 unpinned，随后通过 `BufferManager::SpillBlocks()` 下刷；
- `BmRowRef` 和 `BmVarRef` 仍然保存逻辑位置，不依赖 resident 地址。

`BlockHandle` 仍由 `BmRowContainer` 持有，用于后续 pin/readback。第一版不设计按 row 或 partition
主动 drop block handle 的接口。

## 12. 测试计划

`BmRowContainer` 单测：

- fixed-width append/extract round trip；
- nullable columns；
- variable-width string append/extract round trip；
- variable-width 数据在 row block 和 heap block unpin/re-pin 后仍可读取；
- fixed-width column compare；
- variable-width column compare；
- 多 row blocks；
- 多 heap blocks；
- `BmRowRef` 在 BM reclaim 和 re-pin 后仍有效；
- `MaybeReserve` 失败时先释放冷 pins，再触发 `SpillBlocks`；
- `MaybeReserve` 第二次失败时直接报错。

回归测试：

- 现有 `RowContainerTest`；
- task-level BufferManager tests。

## 13. Benchmark 计划

第一版 benchmark 只比较容器层能力，不接入 Window operator。

新增一个 exec benchmark 可执行文件，例如：

```text
bolt/exec/benchmarks/BmRowContainerBenchmark.cpp
target: bolt_bm_row_container_benchmark
```

这个 benchmark 对比现有 `RowContainer` 和新增 `BmRowContainer` 在相同输入数据下的
append、spill/readback、extract 和 compare 成本。

Benchmark 数据集至少覆盖三类输入：

```text
fixed_int64:
  BIGINT

mixed_fixed:
  INTEGER, BIGINT, DOUBLE, BOOLEAN

varchar_payload:
  BIGINT key, VARCHAR payload
```

`varchar_payload` 覆盖两种 string 分布：

- small string：大部分能 inline 或很短，例如 8 到 16 bytes；
- large string：明显走 heap，例如 128 bytes、1KB、4KB。

数据规模用参数控制：

```text
rows: 10K, 100K, 1M
null ratio: 0%, 10%
string size: 16B, 128B, 1KB, 4KB
```

推荐第一版 baseline：

```text
RowContainerNoSpill
BmRowContainerNoSpill
BmRowContainerSpillReadback
```

`RowContainerNoSpill` 衡量现有纯内存路径。
`BmRowContainerNoSpill` 衡量 BM block 化但不触发 spill 的额外成本。
`BmRowContainerSpillReadback` 衡量真正 unpin、spill、pin readback 的成本。

BM case 的 benchmark 流程：

```text
1. 创建 BufferManager，spill directory 使用 /tmp 下临时目录。
2. 创建 BmRowContainer，使用 MemoryTag::kWindow。
3. append/store 全部 rows。
4. 释放 append 阶段的冷 pinned handles。
5. 对 row blocks 和 heap blocks 调用 SpillBlocks。
6. 执行 readback workload：
   - extractColumn 全量或按 batch 提取；
   - compare 相邻 rows；
   - optional: sort-like compare loop。
7. 收集 BufferManagerStats 和 benchmark 耗时。
```

为了让 benchmark 能主动触发 spill，`BmRowContainer` 可以提供受控接口：

```cpp
void spillAllBlocksForBenchmark();
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

Benchmark 注册方式复用现有 `bolt/exec/benchmarks/CMakeLists.txt` 风格：

```cmake
add_executable(bolt_bm_row_container_benchmark BmRowContainerBenchmark.cpp)

target_link_libraries(
  bolt_bm_row_container_benchmark PRIVATE
    bolt_testutils
    ${FOLLY_BENCHMARK}
    GTest::gtest_main
)
```

第一版 benchmark 不包含：

- Window operator 端到端；
- SortWindowBenchmark 扩展；
- 多线程并发 pin/reclaim；
- JIT compare；
- RowContainer spiller 的完整公平对齐；
- 不同压缩算法矩阵。

## 14. 主要设计决策

1. `newRow()` 返回 `BmRowRef`，不返回 `char*`。
2. fixed row 存在 BM row blocks。
3. variable-width bytes 存在 BM heap blocks。
4. fixed row 存 `BmVarRef`，不存 heap pointer。
5. `PinnedRows` 是容器内部 RAII 状态，不对外暴露。
6. row/heap blocks 第一版统一使用 `AllocateSize::kLarge`。
7. `BmBlockState` 用 `std::optional<BufferHandle>` 表达容器是否持有 pin，不新增裸 bool，也不修改
   `BlockMemory`。
8. 内存充足时 append blocks 可以保持 pinned。
9. 运行时在需要新 block 时先 `MaybeReserve`，失败后释放冷 pins 并 `SpillBlocks` 冷 blocks。
10. 第二次 `MaybeReserve` 失败直接报错。
11. 第一版不提供 `releaseRows()`。
12. 第一版 benchmark 只做容器层，不做 Window 端到端。
13. 现有 `RowContainer` 对当前使用方保持不变。
