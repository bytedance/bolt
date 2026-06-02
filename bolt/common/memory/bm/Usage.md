# BufferManager 使用说明

`BufferManager` 用于管理执行过程中的连续内存块。调用方通过 `Allocate()` 创建
block，通过 `Pin()` 获取可访问的 `BufferHandle`。当内存压力出现时，未被 pin 的
resident block 可以被 spill 到磁盘；后续再次 `Pin()` 时会自动读回。

## 核心概念

- `BlockHandle`：逻辑 block 句柄。可以长期保存，用于后续再次访问同一块数据。
- `BufferHandle`：一次 pin。它 RAII 持有 block payload，析构或 `Destroy()` 时自动 unpin。
- `BlockMemory`：BM 内部状态，记录 payload、spill segment、pin count 和状态机。
- `BufferManager`：拥有自己的 leaf `MemoryPool`，并在该 pool 上安装 reclaimer。

调用方通常只需要理解 `BlockHandle` 和 `BufferHandle`：

- 保存 `BlockHandle`，表示以后还要访问这块逻辑数据。
- 持有 `BufferHandle`，表示当前正在访问这块数据的内存 payload。
- 释放 `BufferHandle` 后，payload 指针不能再使用。

## 头文件

```cpp
#include "bolt/common/memory/bm/BufferManager.h"
```

常用命名空间：

```cpp
using namespace bytedance::bolt::memory::bm;
```

## 初始化

调用方先准备父 `MemoryPool`，再通过 `Create()` 创建 `BufferManager`。
`BufferManager` 必须由 `Create()` 创建，不能直接栈上构造。

```cpp
BufferManagerConfig config;
config.poolName = "sort-buffer-manager"; // 同一个父 pool 下必须唯一
config.spillStoreConfig.fileAllocatorConfig.directory = "/tmp/bolt-bm-sort";
config.spillStoreConfig.fileAllocatorConfig.bucket_sizes = {
    static_cast<int64_t>(allocateSizeBytes(AllocateSize::kSmall)),
    static_cast<int64_t>(allocateSizeBytes(AllocateSize::kMedium)),
    static_cast<int64_t>(allocateSizeBytes(AllocateSize::kLarge)),
};
config.spillStoreConfig.fileAllocatorConfig.file_size_limit_bytes =
    1024LL * 1024LL * 1024LL;
config.spillStoreConfig.fileAllocatorConfig.max_open_files_per_bucket = 64;

auto manager = BufferManager::Create(parentPool, std::move(config));
```

`fileAllocatorConfig` 使用 `bm/file` 模块，具体 offset 分配规则见
`bolt/common/memory/bm/file/Usage.md`。

BM 的读写 IO 走 `bm/io` 的 `DiskIoScheduler`。当前默认 backend 基于 io_uring；
如果运行环境不允许 io_uring，真实 spill/read 可能在 scheduler 初始化时抛异常。

## API 分层

### 基础 API

优先使用基础 API：

```cpp
BufferHandle Allocate(size_t size, MemoryTag tag);
BufferHandle Pin(const std::shared_ptr<BlockHandle>& block);
```

这两个接口语义最直接：

- `Allocate()` 创建一个新 block，并返回已经 pinned 的 `BufferHandle`。
- `Pin()` 访问一个已有 block，并返回一个 RAII 管理的 `BufferHandle`。

大多数调用方应该先用这两个接口把生命周期写清楚。

### 高级 API

下面这些接口属于高级 API：

```cpp
std::vector<BufferHandle>
BatchAllocate(size_t count, size_t size, MemoryTag tag);
std::vector<BufferHandle> BatchPin(
    std::span<const std::shared_ptr<BlockHandle>> blocks);
void Prefetch(std::span<const std::shared_ptr<BlockHandle>> blocks);
void SpillBlocks(std::span<const std::shared_ptr<BlockHandle>> blocks);
```

这些接口会影响批量 pin/unpin、异步读、主动 spill、统计记账和错误传播路径。不清楚
语义和后果时不要使用。先用 `Allocate()` 和 `Pin()` 实现正确逻辑，再根据性能或
内存压力需求引入高级 API。

## 基础用法

### Allocate

`Allocate()` 从 BM 的 leaf `MemoryPool` 分配连续内存，并返回已经 pin 住的
`BufferHandle`。

```cpp
BufferHandle handle =
    manager->Allocate(allocateSizeBytes(AllocateSize::kLarge),
                      MemoryTag::kSort);

std::shared_ptr<BlockHandle> block = handle.block();
char* data = handle.Ptr();

FillBlock(data, block->size());
```

如果之后还要访问这块逻辑数据，保存 `handle.block()`：

```cpp
runBlocks.push_back(handle.block());
```

如果不保存 `BlockHandle`，该 block 会随最后一个 `BufferHandle` 销毁而释放，通常
只适合临时 buffer 场景。

`MemoryTag` 用于可观测性和问题排查。优先传真实来源，例如 `kSort`、`kHashBuild`、
`kAggregation`，不要长期使用 `kUnknown`。

### Pin

`Pin()` 返回一个可访问的 `BufferHandle`：

```cpp
{
  BufferHandle handle = manager->Pin(block);
  Consume(handle.Ptr(), block->size());
} // handle 析构，自动 unpin
```

调用方不需要关心 block 当前在哪里：

- 如果 block 在内存中，`Pin()` 直接返回 resident payload。
- 如果 block 已经 spilled，`Pin()` 会读回 payload。
- 如果 block 正在 prefetching，`Pin()` 会等待已有 read future。

`BufferHandle` 是 move-only 类型。不要拷贝，也不要在 `BufferHandle` 析构后继续
使用 `Ptr()` 返回的指针。

如果需要提前释放 pin，可以显式调用：

```cpp
handle.Destroy();
```

多数场景直接依赖 RAII 即可。

## 高级 API

### BatchAllocate

`BatchAllocate()` 一次创建多个同 size、同 tag 的 pinned block：

```cpp
std::vector<BufferHandle> handles =
    manager->BatchAllocate(blockCount, blockBytes, MemoryTag::kSort);
```

语义：

- `BatchAllocate(0, 0, tag)` 返回空 vector。
- 非空 batch 的 `size` 必须大于 0。
- 返回结果的统计语义等价于连续调用 `Allocate()`。

除非明确需要批量创建同规格 block，否则优先使用 `Allocate()`。

### BatchPin

`BatchPin()` 批量访问多个 block：

```cpp
std::vector<std::shared_ptr<BlockHandle>> blocks = ...;
std::vector<BufferHandle> handles = manager->BatchPin(blocks);

for (auto& handle : handles) {
  Consume(handle.Ptr());
}
```

语义：

- 返回的每个 `BufferHandle` 都通过 RAII unpin。
- 如果任意 block 读回失败，异常会向上传播。
- 调用方需要理解批量读回和异常边界。

如果只是访问少量 block，优先使用 `Pin()`。

### Prefetch

`Prefetch()` 是异步读 hint：

```cpp
manager->Prefetch(blocks);

// 后续真正访问时仍然需要 Pin 或 BatchPin。
BufferHandle handle = manager->Pin(block);
```

语义：

- `Prefetch()` 不返回 `BufferHandle`。
- `Prefetch()` 不保证数据已经读回。
- 后续访问 block 时仍然必须调用 `Pin()` 或 `BatchPin()`。
- 提交 read 失败、非法 block、内存分配失败等错误会通过异常向上传播。

不清楚异步读和后续 `Pin()` 交互时，不要使用 `Prefetch()`。

### SpillBlocks

`SpillBlocks()` 是主动 spill 指定 block 的 best-effort hint：

```cpp
std::vector<std::shared_ptr<BlockHandle>> blocks = ...;
manager->SpillBlocks(blocks);
```

它只会尝试处理当前满足条件的 block：

- `BlockHandle` 非空。
- `pinCount == 0`，也就是没有任何 `BufferHandle` 正在持有 payload。
- block 仍是 resident 状态。
- block 当前有有效 payload。

下面这些情况会被跳过：

- block 仍然 pinned。
- block 已经 spilled。
- block 正在 prefetching 或 spilling。
- 空 `BlockHandle`。

`SpillBlocks()` 不会 unpin，也不会销毁任何 `BufferHandle`。调用方如果希望某个
block 可被主动 spill，必须先释放对应的 `BufferHandle`：

```cpp
std::shared_ptr<BlockHandle> block;
{
  BufferHandle handle = manager->Allocate(blockBytes, MemoryTag::kSort);
  block = handle.block();
  FillBlock(handle.Ptr());
} // handle 析构，block 才可能被 SpillBlocks 处理

std::array<std::shared_ptr<BlockHandle>, 1> blocks{block};
manager->SpillBlocks(blocks);
```

不清楚 pin count、resident/spilled 状态、以及写盘失败后果时，不要使用
`SpillBlocks()`。多数场景只需要释放 `BufferHandle`，让 BM 的 reclaimer 或显式
`Reclaim()` 决定何时 spill。

## 内存回收

### Reclaim

`BufferManager` 会在自己的 leaf pool 上安装 `BufferManagerReclaimer`。当父
`MemoryPool` 的仲裁机制需要释放内存时，会回调该 reclaimer，reclaimer 再调用 BM
把可驱逐 block spill 到磁盘。

也可以显式调用：

```cpp
uint64_t reclaimed = manager->Reclaim(256 * 1024 * 1024);
```

语义：

- `targetBytes == 0` 表示尽量 reclaim 所有当前可驱逐 resident block。
- 只有 `pinCount == 0` 且仍在内存中的 block 才可 reclaim。
- 仍被 `BufferHandle` 持有的 block 不会被 spill。

### MaybeReserve 模式

如果调用方希望在即将触达内存上限时主动形成 run，可以先用 `MaybeReserve()` 探测：

```cpp
if (!manager->MaybeReserve(nextBlockBytes) && !activeRun.empty()) {
  Sort(activeRun);
  activeRun.clear(); // 清理 BufferHandle，让 blocks 变成可 reclaim
  manager->ReleaseUnusedReservation();
}

auto handle = manager->Allocate(nextBlockBytes, MemoryTag::kSort);
auto block = handle.block();
```

注意：

- `MaybeReserve()` 只是 reservation 探测，不会替代 `Allocate()`。
- 如果 `MaybeReserve()` 成功但后续不分配，应调用 `ReleaseUnusedReservation()`。
- `activeRun.clear()` 或 `BufferHandle` 析构后，block 只是变成可驱逐；真正 spill
  仍由 reclaimer、显式 `Reclaim()` 或高级 API `SpillBlocks()` 触发。

## 常见模式：外排 sort

写 run 阶段保存 `BlockHandle`，并持有 `BufferHandle` 直到 run 完成：

```cpp
std::vector<std::shared_ptr<BlockHandle>> currentRunBlocks;
std::vector<BufferHandle> currentRunPins;

while (HasMoreInput()) {
  if (!manager->MaybeReserve(blockBytes) && !currentRunPins.empty()) {
    SortPinnedBlocks(currentRunPins);
    SaveRunMetadata(currentRunBlocks);

    currentRunPins.clear(); // unpin，使 run blocks 可被 BM spill
    currentRunBlocks.clear();
    manager->ReleaseUnusedReservation();
    continue;
  }

  auto handle = manager->Allocate(blockBytes, MemoryTag::kSort);
  auto block = handle.block();
  FillBlock(handle.Ptr());

  currentRunBlocks.push_back(std::move(block));
  currentRunPins.push_back(std::move(handle));
}
```

merge 阶段只保存 `BlockHandle`，访问时按需 `Pin()`：

```cpp
for (const auto& block : run.blocks) {
  BufferHandle handle = manager->Pin(block);
  Merge(handle.Ptr(), block->size());
}
```

完整 benchmark 示例见：

- `bolt/common/memory/bm/benchmark/BufferManagerSortBenchmark.cpp`
- `bolt/common/memory/bm/benchmark/BufferManagerParallelSortBenchmark.cpp`

## 生命周期约束

- `BufferManager` 生命周期必须长于它创建的 `BufferHandle`、`BlockHandle` 和
  `BlockMemory`。
- 如果 `BufferHandle` 析构时发现 owner `BufferManager` 已经销毁，会走 fatal
  诊断路径，而不是支持 late destruction。
- `BlockHandle` 可以保存到 operator/run metadata 中，但对应 BM 必须仍然存活。
- spill 文件 segment 由 BM 内部管理，调用方不要直接操作 segment。

推荐由 query/operator 级别的根对象持有 `std::shared_ptr<BufferManager>`，并保证
所有 handle/block metadata 在 BM 销毁前释放。

## 线程模型

当前 `BufferManager` 本身按单线程对象设计。不要让同一个 BM 实例被多个业务线程
并发调用 `Allocate()`、`Pin()`、`Reclaim()`、`Prefetch()` 或 `SpillBlocks()`。

多线程场景推荐：

- 每个 worker/task 持有自己的 `BufferManager`。
- 多个 BM 可以共享上层 `ExecutionMemoryPool` / 父 pool，由现有内存仲裁机制统一
  约束总内存。
- 如果某个上层 reclaimer 可能跨线程回调 BM，需要保证回调不会进入别的线程正在
  使用的 BM，或者在 BM 外层加同步。

## 可观测性

BM 提供三类观测接口：

```cpp
BufferManagerStats stats = manager->stats();
std::vector<BufferManagerTagStats> tagStats = manager->tagStats();
std::string debug = manager->debugString();
```

常用字段：

- `pinnedResidentBytes`：仍被 `BufferHandle` 持有的 resident 内存。
- `unpinnedResidentBytes`：可被 spill 的 resident 内存。
- `spilledBytes`：当前已 spill 到磁盘的逻辑字节数。
- `prefetchingBytes` / `spillingBytes`：过渡态字节数。
- `reclaimedBytes`：BM 通过 spill 释放的字节数。
- `spillWriteBytes` / `spillReadBytes`：spill 写入和读回的数据量。
- `fileAllocateFailures`、`fileFreeFailures`、`readIoFailures`、
  `writeIoFailures`：错误计数。
- `evictionQueueSize`、`evictionQueueStaleEntries`：驱逐队列状态。

调试时可以打开 `--v=1 --logtostderr=1` 查看 BM 的 VLOG，包括 `MaybeReserve`、
`Reclaim`、`SpillBlocks`、spill candidate、reclaimer 回调等路径。

## 错误处理

当前接口采用项目内异常风格：

- 内存分配失败会从 `MemoryPool` 路径抛出异常。
- 文件分配、IO、非法状态等会通过 `BOLT_FAIL` / `BOLT_CHECK` 异常或 fatal 路径暴露。
- 析构路径不会抛异常；严重生命周期/状态错误走 fatal 诊断。

调用方应在 operator/task 边界捕获异常，并按现有 query 失败路径处理。
