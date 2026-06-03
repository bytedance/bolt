# 04. StreamingWindowBuild 和 WindowPartition 改造

## WindowBuild 接入 options

给 `WindowBuild` 增加可选参数：

```cpp
struct WindowRowContainerOptions {
  bool appendOnly{false};
  std::shared_ptr<memory::bm::BufferManager> bufferManager;
};
```

当 `options.appendOnly == true` 时，`WindowBuild.cpp` 用 `RowContainerParam` 构造：

```cpp
std::vector<Accumulator> accumulators;
auto keyTypes = slice(inputType_->children(), 0, numKeys);
auto dependentTypes = slice(inputType_->children(), numKeys, inputType_->size());
RowContainerParam param{
    keyTypes,
    accumulators,
    dependentTypes,
    true, // nullableKeys
    false, // hasNext
    false, // isJoinBuild
    false, // hasProbedFlag
    false, // hasNormalizedKeys
    false, // useListRowIndex
    true, // appendOnly
    pool,
    nullptr,
    options.bufferManager,
    memory::bm::MemoryTag::kWindow};
data_ = std::make_unique<RowContainer>(param);
```

否则保持原构造。

## 只在 StreamingWindowBuild 开启

`bolt/exec/Window.cpp` 的 `setStreamingWindowBuild()` 中，只有以下条件同时满足才启用：

- `needSort_ == false`
- query config 显式开启
- `driverCtx_->bufferManager()` 非空
- 有合法 BM spill path

SortWindowBuild、RowsStreamingWindowBuild、SpillableWindowBuild 第一阶段不接。

## StreamingWindowBuild 成员替换

把 `bolt/exec/StreamingWindowBuild.h` 中：

```cpp
std::vector<char*> sortedRows_;
std::vector<char*> inputRows_;
char* previousRow_ = nullptr;
```

替换为 BM 分支成员：

```cpp
std::vector<RowContainer::RowId> sortedRowIds_;
std::vector<RowContainer::RowId> inputRowIds_;
std::optional<RowContainer::RowId> previousRowId_;
```

推荐第一阶段：BM 模式使用 RowId 分支，默认模式保留旧 `char*` 分支，便于回滚。

## addInput 流程

BM append-only 开启后：

```cpp
auto rowId = data_->newRowId();
RowContainer::RowId ids[] = {rowId};
auto pinned = data_->pinRows(folly::Range<const RowContainer::RowId*>(ids, 1));
char* newRow = pinned.rowAt(0);

for (auto col = 0; col < input->childrenSize(); ++col) {
  data_->store(decodedInputVectors_[col], row, newRow, col);
}
```

比较 partition key 时 pin previous 和 current：

```cpp
if (previousRowId_.has_value()) {
  RowContainer::RowId ids[] = {*previousRowId_, rowId};
  auto pinned = data_->pinRows(folly::Range<const RowContainer::RowId*>(ids, 2));
  if (compareRowsWithKeys(pinned.rowAt(0), pinned.rowAt(1), partitionKeyInfo_)) {
    buildNextPartition();
  }
}
inputRowIds_.push_back(rowId);
previousRowId_ = rowId;
```

## buildNextPartition

BM 模式：

```cpp
partitionStartRows_.push_back(sortedRowIds_.size());
sortedRowIds_.insert(
    sortedRowIds_.end(), inputRowIds_.begin(), inputRowIds_.end());
inputRowIds_.clear();
```

默认模式保留旧 `sortedRows_`。

## nextPartition

BM 模式下构造 RowId-backed partition：

```cpp
auto begin = partitionStartRows_[currentPartition_];
auto end = partitionStartRows_[currentPartition_ + 1];
std::vector<RowContainer::RowId> ids(
    sortedRowIds_.begin() + begin, sortedRowIds_.begin() + end);

return std::make_shared<RowIdWindowPartition>(
    data_.get(), std::move(ids), inversedInputChannels_, sortKeyInfo_);
```

建议第一版复制 ids，使 partition 生命周期与 build 侧 vector 解耦。

## RowIdWindowPartition

建议新建类，避免大改现有 `WindowPartitionImpl<RowFormat::kRowContainer>`：

```cpp
class RowIdWindowPartition : public WindowPartition {
 public:
  RowIdWindowPartition(
      RowContainer* data,
      std::vector<RowContainer::RowId> rowIds,
      const std::vector<column_index_t>& inputMapping,
      const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo);

  vector_size_t numRows() const override;

  void extractColumn(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize = false) const override;

 private:
  std::vector<RowContainer::RowId> rowIds_;
};
```

## extractColumn lazy pin

连续 range：

```cpp
auto ids = folly::Range<const RowContainer::RowId*>(
    rowIds_.data() + partitionOffset, numRows);
auto pinned = data_->pinRows(ids);
data_->extractColumn(
    pinned.rows().data(),
    pinned.size(),
    inputMapping_[columnIndex],
    resultOffset,
    result,
    exactSize);
```

带 rowNumbers 的接口：

1. 根据 rowNumbers gather RowId。
2. pin gathered RowId。
3. 用 dense rows 调现有 `extractColumn()`。

## peer/frame 计算

第一版以“方法调用粒度” pin，不做整个 partition 长期 pin。

例如 `computePeerBuffers(start, end, ...)`：

- pin `[start, end)`。
- 如果需要和 `start - 1` 比较，则额外 pin `start - 1`。
- 方法返回前释放 pin。

后续优化点：

- 按 block 缓存 pin，减少同一方法内重复 pin。
- 对后续 block 调 `BufferManager::Prefetch()`。
- 针对大 partition 分段计算 peer/frame。
