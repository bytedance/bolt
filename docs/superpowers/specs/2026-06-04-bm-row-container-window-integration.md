# BmRowContainer 与 Window 集成设计

## 1. 目标和边界

这份文档描述 `BmRowContainer` 如何接入 Window，第一版只考虑 StreamingWindowBuild。

容器本身的 row layout、block 管理、pin 生命周期、spill 触发和容器层 benchmark，见
`docs/superpowers/specs/2026-06-04-bm-row-container-design.md`。本文件只描述 Window 侧如何和
容器对接。

第一版 Window 接入目标：

- StreamingWindowBuild 保存 `RowId`，不保存长期 `char*`；
- WindowPartition 消费 `RowId` range；
- partition 输出、peer 计算、frame 计算通过 `BmRowContainer` 的 compare/extract 接口访问数据；
- Window 代码不直接持有 `PinnedRows`、`BufferHandle` 或任何 row/heap `char*`；
- BM enabled 但 task-level BufferManager 缺失时直接报错，不做 failover。

第一版不覆盖：

- SortWindowBuild；
- RowsStreamingWindowBuild；
- SpillableWindowBuild；
- row-based spiller 的 serialized row 读取路径；
- JIT compare；
- Window operator 端到端 benchmark。

## 2. 和容器文档的接口契约

Window 集成使用的接口必须和 `BmRowContainer` 容器设计文档保持一致。这里复述同一份契约，避免
Window 侧引入另一套 row identity。

`RowId` 是 Window 侧唯一长期保存的 row identity：

```cpp
struct RowId {
  uint32_t rowBlockId;
  uint32_t rowOffset;
};
```

`VarData` 是容器 fixed row 内部的变长字段引用。Window 侧不直接读取或保存它，但接口契约中列出，
用于说明变长字段也不会通过 heap pointer 暴露给 Window：

```cpp
struct VarData {
  uint32_t heapBlockId;
  uint32_t heapOffset;
  uint32_t size;
};
```

Window 侧只通过下面这些 `BmRowContainer` 方法访问数据：

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

注意：接口里没有 `releaseRows()`。第一版 Window 不按 partition 主动释放容器 block。resident 内存
释放由容器内部 `BufferHandle` RAII unpin 和 `SpillBlocks` 负责。

## 3. 交接语义

第一份容器文档和本 Window 集成文档的交接点可以概括为：

```text
RowContainer 时代：
  RowContainer::newRow() -> char*
  Window 长期保存 char*
  Window 把 char* 传回 RowContainer/RowContainer static helpers 访问数据

BmRowContainer 时代：
  BmRowContainer::newRow() -> RowId
  Window 长期保存 RowId
  Window 把 RowId 传回 BmRowContainer 方法访问数据
```

Window 不能做的事情：

- 不能把 `RowId` 转成 `char*`；
- 不能保存 `BufferHandle`；
- 不能保存 `PinnedRows`；
- 不能直接读取 `VarData`；
- 不能假设 row/heap block 当前 resident；
- 不能在 BM 不可用时静默退回 `RowContainer`。

Window 可以做的事情：

- 保存和移动 `RowId`；
- 对 `std::vector<RowId>` 做 partition 范围管理；
- 调用 `BmRowContainer::compare()` 判断 partition 或 peer boundary；
- 调用 `BmRowContainer::extractColumn()` / `extractNulls()` 生成输出向量；
- 调用 `BmRowContainer::estimateRowSize()` / `usedBytes()` 做统计或估算。

## 4. StreamingWindowBuild 改动

新增 BM 专用 StreamingWindowBuild 路径，不直接改掉所有 `WindowBuild` 的存储模型。

建议改动：

- 保留现有 `RowContainer` 给当前非 BM window paths 使用；
- BM enabled 路径持有 `std::unique_ptr<BmRowContainer> bmData_`；
- BM 路径里的 row vectors 使用 `std::vector<RowId>`；
- `addInput()` 中使用 `BmRowAppender` 批量写入当前 input；
- partition boundary 判断使用 `bmData_->compare(...)`；
- 不设计按 prefix partition 主动释放的接口；
- resident 内存通过容器内部 `BufferHandle` RAII unpin 和 `SpillBlocks` 下刷。

当前 `StreamingWindowBuild::addInput()` 的核心变化：

```text
RowContainer path:
  char* newRow = data_->newRow()
  data_->store(decoded, sourceRow, newRow, col)
  inputRows_.push_back(newRow)

BmRowContainer path:
  RowId newRow = appender.newRow()
  appender.store(decoded, sourceRow, newRow, col)
  bmInputRows_.push_back(newRow)
```

如果 BM enabled 但 `DriverCtx::bufferManager()` 或等价 task-level BM 来源为空，构造 BM path 时直接
报错。不要在这个分支中创建普通 `RowContainer` 作为 fallback。

## 5. BmWindowPartition

增加一个 BM partition 实现，持有 `RowId` range，而不是 `char**`。

概念形态：

```cpp
class BmWindowPartition : public WindowPartition {
 public:
  BmWindowPartition(
      BmRowContainer* data,
      folly::Range<const RowId*> rows,
      ...);

  void extractColumn(...) const override;
  void extractNulls(...) const;
  std::pair<vector_size_t, vector_size_t> computePeerBuffers(...) const override;
};
```

当前 `WindowPartition` base class 和 `RowContainer*`、`folly::Range<char**>` 绑定较深。
实现阶段可以二选一：

- 新增 sibling partition base，BM path 使用新的 partition 类型；
- 小心泛化现有 `WindowPartition`，但不影响旧路径。

无论选择哪种，BM partition 都必须保持同一个边界：partition 保存 `RowId`，访问数据时调用
`BmRowContainer` 方法，不直接处理 pin/readback。

## 6. Window 访问路径映射

现有 RowContainer 路径到 BmRowContainer 路径的映射：

| Window 需求 | RowContainer 方式 | BmRowContainer 方式 |
| --- | --- | --- |
| 保存输入行 | `std::vector<char*>` | `std::vector<RowId>` |
| 新增行 | `data_->newRow()` 返回 `char*` | `appender.newRow()` 返回 `RowId` |
| 写入列 | `data_->store(decoded, row, char*, col)` | `appender.store(decoded, row, RowId, col)` |
| partition boundary | `data_->compare(char*, char*, key, flags)` | `bmData_->compare(RowId, RowId, key, flags)` |
| peer boundary | `data_->compare(char*, char*, key, flags)` | `bmData_->compare(RowId, RowId, key, flags)` |
| extract output column | `RowContainer::extractColumn(char**, ...)` | `bmData_->extractColumn(RowId range, ...)` |
| extract nulls | `RowContainer::extractNulls(char**, ...)` | `bmData_->extractNulls(RowId range, ...)` |
| direct null check | `RowContainer::isNullAt(char*, ...)` | `bmData_->extractNulls(...)` 或容器内封装方法 |

如果 Window 现有代码中有直接 `RowContainer::isNullAt(partition_[i], ...)` 的逻辑，BM path 不能照搬。
应改成容器方法封装，避免 Window 直接拿 row address。

## 7. Pinning 和 spill 归属

Window 代码不直接持有 `PinnedRows`。它只持有 `RowId`，并通过容器接口访问：

- `extractColumn()` 内部按请求范围 pin row/heap blocks；
- `extractNulls()` 内部只 pin row blocks；
- `compare()` 内部 pin 两个 row 所在 block 和必要 heap block；
- `computePeerBuffers()` 和 range frame 逻辑如果需要优化，也应通过容器方法支持批量访问。

Window 不负责判断 block 是否 pinned、spilled、prefetching 或 resident。`MaybeReserve` 失败后的
`releaseColdPins()` 和 `SpillBlocks()` 都属于 `BmRowContainer` 内部策略。

这样 Window 的数据结构不会因为 BM block 被 spill/reload 而失效。

## 8. Window 下的 block 释放

第一版 Window 接入不设计 `releaseRows()`。StreamingWindowBuild 不负责按 partition 主动释放
row/heap block handle。

释放 resident 内存依赖容器内部 `BufferHandle` 的 RAII unpin：

- 内存充足时，`BmRowContainer` 可以保持 append 产生的 blocks pinned；
- 当后续 append 需要新 block 且 `MaybeReserve` 失败时，容器释放冷 block handles；
- handles 析构后 blocks 变成 unpinned；
- 容器把这些 unpinned cold blocks 交给 `BufferManager::SpillBlocks()` 下刷；
- Window 侧保存的 `RowId` 不受影响，后续访问时由容器重新 pin/readback。

## 9. 测试计划

Window 接入后补充测试：

- BM enabled 的 StreamingWindowBuild basic partition output；
- BM enabled 但 task-level BufferManager 缺失时直接报错；
- partition boundary compare 跨 row block；
- variable-width sort key 的 peer boundary compare；
- extract output columns across spilled/readback row blocks；
- direct null/frame logic 不再依赖 `RowContainer::isNullAt(char*)`；
- 现有 `WindowTest` 和 `RowStreamingWindowTest` 回归。

## 10. Window Benchmark 后续计划

第一版 benchmark 不做 Window operator 端到端。原因是 Window benchmark 会混入很多非容器因素：

- partition boundary 判断；
- peer group 计算；
- window function 计算；
- output batch 构造；
- operator stats 采集；
- 是否走 SortWindowBuild、StreamingWindowBuild、RowsStreamingWindowBuild 或 SpillableWindowBuild。

等容器 benchmark 稳定、`BmRowContainer` 接入 StreamingWindowBuild 后，再基于现有
`SortWindowBenchmark.cpp` 扩展 BM mode，对比：

```text
RowContainer StreamingWindowBuild
vs
BmRowContainer StreamingWindowBuild
```

可复用指标：

- windowTotalTimeMs；
- windowSpillBytes / windowSpillRows；
- windowSpillTotalTime；
- windowAddInputTime；
- windowExtractColumnTime；
- windowOutputTime；
- windowLoadFromSpillTime；
- buildPartitionTime。

## 11. 主要设计决策

1. Window 保存 `RowId`，不保存 `char*`。
2. Window 通过 `BmRowContainer::compare/extractColumn/extractNulls` 访问数据。
3. Window 代码不直接持有 `PinnedRows`、`BufferHandle` 或 `VarData`。
4. 不设计 `releaseRows()`；resident 内存释放依赖容器内部 `BufferHandle` RAII unpin。
5. BM enabled 但 task-level BufferManager 缺失时直接报错，不做 failover。
6. 第一版只接 StreamingWindowBuild，不覆盖其他 WindowBuild。
7. 第一版不做 Window operator 端到端 benchmark。

