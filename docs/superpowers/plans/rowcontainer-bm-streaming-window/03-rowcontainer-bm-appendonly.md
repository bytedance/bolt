# 03. RowContainer BM-backed append-only 后端

## RowId 类型和编码

建议先放在 `RowContainer` 内：

```cpp
class RowContainer {
 public:
  using RowId = uint64_t;
};
```

第一版使用 32 + 32 编码：

```cpp
// 高 32 bit：RowContainer 内部 block index。
// 低 32 bit：该 block 内逻辑 row 指针相对 block base 的 byte offset。
RowId rowId = (uint64_t(blockIndex) << 32) | uint32_t(rowOffset);
```

`rowOffset` 指向逻辑 row 地址，也就是现有 `RowContainer::store()`、`compare()`、`extractColumn()` 使用的 `char* row`，不指向 normalized-key prefix 的起点。

建议 helper：

```cpp
static RowId makeRowId(uint32_t blockIndex, uint32_t rowOffset);
static uint32_t rowIdBlock(RowId rowId);
static uint32_t rowIdOffset(RowId rowId);
```

测试可见 helper：

```cpp
static RowId testingMakeRowId(uint32_t blockIndex, uint32_t rowOffset);
static uint32_t testingRowIdBlock(RowId rowId);
static uint32_t testingRowIdOffset(RowId rowId);
```

## RowContainerParam 扩展

在现有 `RowContainerParam` 上扩展 BM 相关字段：

```cpp
struct RowContainerParam {
  const std::vector<TypePtr>& keyTypes;
  std::vector<Accumulator>& accumulators;
  std::vector<TypePtr>& dependentTypes;
  bool nullableKeys;
  bool hasNext;
  bool isJoinBuild;
  bool hasProbedFlag;
  bool hasNormalizedKeys;
  bool useListRowIndex;

  bool appendOnly;
  memory::MemoryPool* pool;
  std::shared_ptr<HashStringAllocator> stringAllocator;

  std::shared_ptr<memory::bm::BufferManager> bufferManager;
  memory::bm::MemoryTag bmTag{memory::bm::MemoryTag::kWindow};
};
```

判定规则：

```cpp
const bool useBmAppendOnly = appendOnly && bufferManager != nullptr;
```

非 BM append-only 继续保留当前 `BufferPtr` 路径，方便单测和性能对比。

## BM block 元数据

新增内部结构：

```cpp
struct BmAppendBlock {
  std::shared_ptr<memory::bm::BlockHandle> block;
  memory::bm::BufferHandle appendPin;
  char* base{nullptr};
  uint32_t capacity{0};
  uint32_t used{0};
  uint32_t liveRows{0};
};
```

RowContainer 新增：

```cpp
std::shared_ptr<memory::bm::BufferManager> bufferManager_;
memory::bm::MemoryTag bmTag_{memory::bm::MemoryTag::kWindow};

std::vector<BmAppendBlock> bmRowsBlocks_;
std::vector<BmAppendBlock> bmStringBlocks_;
bool bmAppendOnly_{false};
```

BM RowContainer 第一阶段不新增 QueryConfig 控制 block size，也不从 `WindowBuild` 透传 block size。fixed row block 和 string block 默认都使用内部固定值：

```cpp
static constexpr size_t kBmAppendBlockBytes =
    memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kMedium);
```

也就是 1MiB。需要覆盖跨 block 的单测可以通过构造大量小行触发，或者后续增加测试专用 hook，不把这个参数暴露给线上查询配置。

`rowsBuffers_` 和 `stringBuffers_` 继续服务非 BM append-only。

## fixed row 分配

新增：

```cpp
RowId newRowId();
```

BM append-only 下，`newRowId()` 做三件事：

1. 确保当前 rows block 有空间。
2. 在当前 block 内 bump pointer。
3. 初始化 row 并返回 RowId。

伪代码：

```cpp
RowContainer::RowId RowContainer::newRowId() {
  BOLT_DCHECK(appendOnly_);
  BOLT_DCHECK(bmAppendOnly_);
  BOLT_DCHECK(mutable_);

  const auto bytes = fixedRowSize_ + normalizedKeySize_;
  auto& block = ensureBmRowsBlock(bytes);

  const auto aligned = bits::roundUp(block.used, alignment_);
  BOLT_CHECK_LE(aligned + bytes, block.capacity);

  const auto rowOffset = aligned + normalizedKeySize_;
  auto* row = block.base + rowOffset;
  block.used = aligned + bytes;
  ++block.liveRows;
  ++numRows_;

  if (normalizedKeySize_) {
    ++numRowsWithNormalizedKey_;
  }
  initializeRow(row, false);

  return makeRowId(bmRowsBlocks_.size() - 1, rowOffset);
}
```

## 变长数据分配

当前 append-only 的 `allocateAppendOnlyString()` 使用 `stringBuffers_`。BM 模式下可改为：

```cpp
char* RowContainer::allocateAppendOnlyString(int32_t size) {
  if (bmAppendOnly_) {
    return allocateBmAppendOnlyString(size);
  }
  ...
}
```

BM string block 也走 append-only bump pointer。返回 header 后的 payload 指针，保持当前 `HashStringAllocator::Header` 布局兼容。

注意：当前 append-only 仍在变长字段中写绝对地址。完整迁移 string block 到 BM 时，要确保 string payload 在访问期间也被 pin，或改成 offset 编码。

保守路线是第一阶段只迁 fixed row BM，string block 仍用当前 `stringBuffers_`；第二阶段再迁 string block。

## PinnedRows

新增 RAII 类型：

```cpp
class RowContainer {
 public:
  class PinnedRows {
   public:
    PinnedRows() = default;
    PinnedRows(PinnedRows&&) noexcept = default;
    PinnedRows& operator=(PinnedRows&&) noexcept = default;
    PinnedRows(const PinnedRows&) = delete;
    PinnedRows& operator=(const PinnedRows&) = delete;

    char* rowAt(vector_size_t index) const;
    char** data();
    vector_size_t size() const;
    folly::Range<char**> rows();

   private:
    friend class RowContainer;
    std::vector<memory::bm::BufferHandle> pins_;
    std::vector<char*> rows_;
  };

  PinnedRows pinRows(folly::Range<const RowId*> rowIds);
};
```

`pinRows()` 流程：

1. 从 RowId 中解析 block index。
2. 每个 unique block 只 pin 一次。
3. 用 pinned block base + row offset 生成 `char*`。
4. 把 pins 和 rows 放在同一个 `PinnedRows` 对象里。

## eraseRowIds

StreamingWindowBuild 第一阶段只需要 append-only 无复用删除。删除后更新 liveRows，释放 dead prefix blocks。

简化版本：

```cpp
void RowContainer::eraseRowIds(folly::Range<const RowId*> rowIds) {
  BOLT_CHECK(bmAppendOnly_);
  numRows_ -= rowIds.size();
  for (auto id : rowIds) {
    auto blockIndex = rowIdBlock(id);
    --bmRowsBlocks_[blockIndex].liveRows;
  }
  releaseDeadPrefixBlocks();
}
```

如果后续 append-only 也用于 aggregation，必须补上 aggregate destroy。
