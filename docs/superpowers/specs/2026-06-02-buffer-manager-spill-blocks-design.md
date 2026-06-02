# BufferManager SpillBlocks 设计

日期：2026-06-02

## 目标

为 `BufferManager` 增加一个 best-effort 的 `SpillBlocks` 接口，让调用方可以主动指定一批已经 unpin 的 resident block，将它们 spill 到外部存储，从而尽早释放内存。

这个接口需要复用现有 BufferManager 的 spill 写盘路径，避免继续加重 `BufferManager.cpp`，也避免复制 `Reclaim()` 里的 submit/harvest 逻辑。

## 非目标

- `SpillBlocks` 不负责 unpin。
- 这个接口不接收 `BufferHandle`。
- 不保证调用方传入的每个 block 都一定被 spill。
- 写盘失败时不做 rollback。
- 本次不 rename 或移除 `ReclaimWriteWindow`。

## Public API

在 `bolt/common/memory/bm/BufferManager.h` 增加：

```cpp
void SpillBlocks(std::span<const std::shared_ptr<BlockHandle>> blocks);
```

接口语义：

- 这是一个 best-effort advisory API。
- 只处理当前满足条件的 block：`pinCount == 0`、`state == kInMemory`、并且 `payload.has_value()`。
- `nullptr` block 会跳过。
- 仍然 pinned 的 block 会跳过。
- `kSpilled`、`kPrefetching`、`kSpilling` 状态的 block 会跳过。
- 接口不会 unpin。调用方必须先销毁相关 `BufferHandle`。
- I/O 失败按现有 `Reclaim()` 的方式暴露，也就是使用现有 `BOLT_FAIL` 语义。
- 接口没有返回值。观测依赖现有 BufferManager stats 和日志。

## 文件布局

新增两个内部组件，让 `BufferManager.cpp` 只保留 API glue 和日志：

```text
bolt/common/memory/bm/
  SpillCandidateProvider.h
  SpillCandidateProvider.cpp
  SpillWriteDriver.h
  SpillWriteDriver.cpp
```

保留现有底层组件：

```text
bolt/common/memory/bm/ReclaimWriteWindow.h
bolt/common/memory/bm/ReclaimWriteWindow.cpp
```

`ReclaimWriteWindow` 继续作为底层 primitive，负责单个写窗口内的 pending write 管理和 block 状态转换。

## SpillCandidateProvider

`SpillCandidateProvider` 表示一个 pull-based 的候选 block 来源：

```cpp
using SpillCandidateProvider =
    std::function<std::shared_ptr<BlockMemory>()>;
```

为用户传入的 `BlockHandle` 增加一个 provider 工厂：

```cpp
SpillCandidateProvider MakeBlockHandleSpillCandidateProvider(
    std::span<const std::shared_ptr<BlockHandle>> blocks);
```

这个 provider 每次被调用时，向后扫描 `blocks`，返回下一个当前可 spill 的 `BlockMemory`：

```cpp
while (index < blocks.size()) {
  const auto& block = blocks[index++];
  if (!block || !block->memory_) {
    continue;
  }

  auto memory = block->memory_;
  if (memory->pinCount == 0 &&
      memory->state == BlockMemoryState::kInMemory &&
      memory->payload.has_value()) {
    return memory;
  }
}
return nullptr;
```

访问 `BlockHandle::memory_` 可以通过在 `BlockHandle.h` 增加一个聚焦的 friend 声明完成：

```cpp
friend SpillCandidateProvider MakeBlockHandleSpillCandidateProvider(
    std::span<const std::shared_ptr<BlockHandle>> blocks);
```

这和当前代码风格一致：BM 内部已经通过 friend 表达内部所有权边界。

## SpillWriteDriver

`SpillWriteDriver` 负责通用的 provider-driven spill 写盘流程：

```cpp
class SpillWriteDriver {
 public:
  using SubmitWrite =
      std::function<SpillWriteFuture(IoBuffer&, size_t, IoPriority)>;

  SpillWriteDriver(
      uint32_t maxInflight,
      IoPriority priority,
      SubmitWrite submitWrite,
      BufferManagerStatsCollector& accounting);

  uint64_t Spill(uint64_t targetBytes, SpillCandidateProvider nextCandidate);

 private:
  uint32_t maxInflight_;
  IoPriority priority_;
  SubmitWrite submitWrite_;
  BufferManagerStatsCollector& accounting_;
};
```

`SpillWriteDriver::Spill` 放置当前 `BufferManager::Reclaim` 中通用的 submit/harvest 循环：

- 创建 `ReclaimWriteWindow`。
- 当 write window 还有容量时，从 provider 拉取候选 block 并提交写盘。
- 当 `targetBytes > 0` 时，达到目标后停止继续提交；`targetBytes == 0` 表示尽量 spill provider 返回的所有候选。
- harvest pending writes。
- 写盘失败时使用和当前 `Reclaim()` 相同的 `BOLT_FAIL` 信息暴露错误。
- 通过 `BufferManagerStatsCollector` 记录 attempted block 和 reclaimed bytes。
- 返回实际 reclaimed bytes，供 `Reclaim()` 日志使用；`SpillBlocks()` 可以只用于日志，不暴露给调用方。

## BufferManager 集成

在 `BufferManager` 增加 private helper：

```cpp
SpillWriteDriver MakeSpillWriteDriver();
```

实现：

```cpp
SpillWriteDriver BufferManager::MakeSpillWriteDriver() {
  return SpillWriteDriver{
      config_.maxReclaimWriteInflight,
      config_.writePriority,
      [this](IoBuffer& payload, size_t rawSize, IoPriority priority) {
        return spillStore_->SubmitWriteBlock(payload, rawSize, priority);
      },
      *accounting_};
}
```

`Reclaim()` 保留当前对外行为和日志，但把通用 spill 循环交给 `SpillWriteDriver`：

```cpp
uint64_t BufferManager::Reclaim(uint64_t targetBytes) {
  accounting_->RecordReclaim();
  VLOG(1) << "BM Reclaim begin" << ...;

  auto driver = MakeSpillWriteDriver();
  const auto reclaimed = driver.Spill(targetBytes, [this]() {
    auto memory = evictionQueue_->PopEvictable();
    if (!memory) {
      VLOG(1) << "BM Reclaim no evictable block" << ...;
    }
    return memory;
  });

  VLOG(1) << "BM Reclaim end" << ...;
  return reclaimed;
}
```

新增 `SpillBlocks()`：

```cpp
void BufferManager::SpillBlocks(
    std::span<const std::shared_ptr<BlockHandle>> blocks) {
  VLOG(1) << "BM SpillBlocks begin"
          << " requested_blocks=" << blocks.size()
          << " bm=" << debugString();

  auto driver = MakeSpillWriteDriver();
  const auto reclaimed =
      driver.Spill(0, MakeBlockHandleSpillCandidateProvider(blocks));

  VLOG(1) << "BM SpillBlocks end"
          << " requested_blocks=" << blocks.size()
          << " reclaimed_bytes=" << reclaimed
          << " bm=" << debugString();
}
```

## 构建改动

更新 `bolt/common/memory/bm/CMakeLists.txt`，把新增源文件加入 `bolt_memory_bm`：

```cmake
add_library(
  bolt_memory_bm
  AllocateSize.cpp
  BlockHandle.cpp
  BlockStateMachine.cpp
  BufferHandle.cpp
  BufferManager.cpp
  BufferManagerReclaimer.cpp
  BufferManagerStats.cpp
  EvictionQueue.cpp
  MemoryTag.cpp
  ReclaimWriteWindow.cpp
  SpillCandidateProvider.cpp
  SpillWriteDriver.cpp
  SpillStore.cpp)
```

如果新增独立测试文件，也同步更新 `bolt/common/memory/bm/tests/CMakeLists.txt`。

## 测试计划

增加组件级测试和一组 BufferManager 集成测试。

### SpillCandidateProviderTest

覆盖：

- 跳过 `nullptr` block。
- 跳过 pinned resident block。
- 返回 unpinned resident block。
- 跳过 already spilled block。
- 跳过 `kPrefetching` 和 `kSpilling` block。
- 重复 block 不需要显式去重；第一次提交后状态会变成 `kSpilling` 或 `kSpilled`，后续重复项自然不再满足候选条件。

### SpillWriteDriverTest

覆盖：

- `targetBytes == 0` 时 spill provider 返回的所有候选。
- `targetBytes > 0` 时达到目标后停止继续提交。
- 遵守 `maxInflight`。
- 写盘失败按现有 `BOLT_FAIL` 语义暴露。
- 正确记录 attempted blocks 和 reclaimed bytes。

### BufferManagerTest

覆盖：

- `SpillBlocks` 跳过仍然 pinned 的 block。
- 调用方销毁 `BufferHandle` 后，`SpillBlocks({block})` 可以 spill 这个 block。
- 成功 spill 后，`reclaimableBytes()` 下降，`stats().spilledBytes` 增加。
- 对 already spilled block 调用 `SpillBlocks` 不出错。
- `SpillBlocks({block})` 后再 `Pin(block)`，可以正常从 spill store 读回。

## 风险和兼容性

这个设计保留现有 `Reclaim()` 语义：`ReclaimWriteWindow` 仍然是状态转换和 pending write 的底层 primitive，只是把 provider-driven 的循环抽到 `SpillWriteDriver`。

新增能力是用户可以主动请求 spill 指定 block。由于 `SpillBlocks` 会跳过 pinned 和非 resident block，它不会让仍然存活的 `BufferHandle` 指向已释放 payload。

主要实现风险是 refactor `Reclaim()` 时不小心改变 accounting 或 `targetBytes` 行为。实现时需要用现有 reclaim 相关测试做回归，并补充 `SpillWriteDriver` 的边界测试。
