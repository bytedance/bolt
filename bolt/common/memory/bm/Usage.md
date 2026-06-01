# BufferManager 使用说明

`BufferManager` 为执行过程中的连续内存块提供统一管理能力。调用方通过
`Allocate()` 创建 block，通过 `Pin()` 获取可访问的 `BufferHandle`；当内存
压力出现时，未 pin 的 resident block 可以被 `BufferManager` spill 到磁盘，
后续再次 `Pin()` 时会自动读回。

核心语义：

- `BlockHandle` 表示一块逻辑 block，可长期保存，用于后续再次访问。
- `BufferHandle` 表示一次 pin，RAII 持有 block payload，析构时自动 unpin。
- `BlockMemory` 是内部状态，记录 payload、spill extent、pin count 和状态机。
- `BufferManager` 自己创建 leaf `MemoryPool`，并在该 pool 上安装 reclaimer。

## 头文件

```cpp
#include "bolt/common/memory/bm/BufferManager.h"
```

常用命名空间：

```cpp
using namespace bytedance::bolt::memory::bm;
```

## 初始化

调用方需要先准备一个父 `MemoryPool`，再通过 factory 创建 `BufferManager`。
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

`BufferManager` 的读写 IO 走 `bm/io` 里的 `DiskIoScheduler`。当前默认 backend
基于 io_uring，如果运行环境不允许 io_uring，真实 spill/read 可能在 scheduler
初始化时抛异常。

## 分配 Block

`Allocate()` 会从 BM 的 leaf `MemoryPool` 分配连续内存，并返回已经 pin 住的
`BufferHandle`。如果需要之后再次访问，可以从 handle 中取出并保存
`BlockHandle`。

```cpp
BufferHandle handle =
    manager->Allocate(allocateSizeBytes(AllocateSize::kLarge),
                      MemoryTag::kSort);
std::shared_ptr<BlockHandle> block = handle.block();

char* data = handle.Ptr();
// 写入数据...

// handle 析构后自动 unpin。block 仍然可以保存到 run metadata 里。
```

`MemoryTag` 用于可观测性和问题排查，优先传真实来源，例如 `kSort`、
`kHashBuild`、`kAggregation`，不要长期使用 `kUnknown`。

如果不保存 `handle.block()`，该 block 会随最后一个 `BufferHandle` 销毁而释放，
通常只适合临时 buffer 场景。

## 批量分配 Block

`BatchAllocate()` 用于一次创建多个同 size、同 tag 的 pinned block：

```cpp
std::vector<BufferHandle> handles =
    manager->BatchAllocate(blockCount, blockBytes, MemoryTag::kSort);

for (auto& handle : handles) {
  auto block = handle.block();
  FillBlock(handle.Ptr());
  currentRunBlocks.push_back(std::move(block));
  currentRunPins.push_back(std::move(handle));
}
```

`BatchAllocate(0, 0, tag)` 返回空 vector。非空 batch 的 size 必须大于 0。
返回结果的统计语义等价于连续调用 `Allocate()`。

## Pin 和 Unpin

`Pin()` 返回一个可访问的 `BufferHandle`：

```cpp
{
  BufferHandle handle = manager->Pin(block);
  Consume(handle.Ptr(), block->size());
} // 自动 unpin
```

调用方不需要关心 block 当前是否在内存中：

- 如果 block 是 `IN_MEMORY`，`Pin()` 直接返回。
- 如果 block 是 `SPILLED`，`Pin()` 会同步提交并等待 read，读回后释放旧 file extent。
- 如果 block 是 `PREFETCHING`，`Pin()` 会等待已有 read future 并安装 payload。

`BufferHandle` 是 move-only 类型。不要拷贝，不要在 `BufferHandle` 析构后继续
使用 `Ptr()` 返回的指针。

如果希望提前释放 pin，可以显式调用：

```cpp
handle.Destroy();
```

多数场景直接依赖 RAII 即可。

## BatchPin

`BatchPin()` 用于批量访问多个 block：

```cpp
std::vector<std::shared_ptr<BlockHandle>> blocks = ...;
std::vector<BufferHandle> handles = manager->BatchPin(blocks);

for (auto& handle : handles) {
  Consume(handle.Ptr());
}
```

当前语义是 all-or-throw：如果任意 block 读回失败，会通过异常向上传播。返回的
`BufferHandle` 同样通过 RAII unpin。

## Prefetch

`Prefetch()` 是异步 hint：

```cpp
manager->Prefetch(blocks);

// 后续真正访问时仍然需要 Pin 或 BatchPin。
BufferHandle handle = manager->Pin(block);
```

`Prefetch()` 不返回 handle，也不保证数据一定已经读回。它的失败语义是尽量温和：
内存分配失败、非法 block、IO 提交失败等会记录日志和统计，block 保持可由后续
`Pin()` 正常处理的状态。

## Reclaim

`BufferManager` 会在自己的 leaf pool 上安装 `BufferManagerReclaimer`。当父
`MemoryPool` 的仲裁机制需要释放内存时，会回调该 reclaimer，reclaimer 再调用
BM 把可驱逐 block spill 到磁盘。

也可以显式调用：

```cpp
uint64_t reclaimed = manager->Reclaim(256 * 1024 * 1024);
```

`targetBytes == 0` 表示尽量 reclaim 所有当前可驱逐 resident block。

只有 `pinCount == 0` 且仍在内存中的 block 才可 reclaim。仍被 `BufferHandle`
持有的 block 不会被 spill。

## maybeReserve 模式

如果调用方希望在即将触达内存上限时主动形成 run，可以先用
`MaybeReserve()` 探测：

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
  仍由 reclaimer 或显式 `Reclaim()` 触发。

## 生命周期约束

当前 BM 设计假设：

- `BufferManager` 生命周期必须长于它创建的 `BufferHandle`、`BlockHandle` 和
  `BlockMemory`。
- 如果 `BufferHandle` 析构时发现 owner `BufferManager` 已经销毁，会走 fatal
  诊断路径，而不是支持 late destruction。
- `BlockHandle` 可以保存到 operator/run metadata 中，但对应 BM 必须仍然存活。
- spill 文件 extent 的释放由 BM 内部管理，调用方不要直接操作 extent。

推荐由 query/operator 级别的根对象持有 `std::shared_ptr<BufferManager>`，并保证
所有 handle/block metadata 在 BM 销毁前释放。

## 线程模型

当前 `BufferManager` 本身按单线程对象设计。不要让同一个 BM 实例被多个业务线程
并发调用 `Allocate()`、`Pin()`、`Unpin()`、`Reclaim()` 或 `Prefetch()`。

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
- `reclaimedBytes`：BM 通过 reclaim spill 掉的字节数。
- `spillWriteBytes` / `spillReadBytes`：spill 写入和读回的数据量。
- `fileAllocateFailures`、`fileFreeFailures`、`readIoFailures`、
  `writeIoFailures`：错误计数。
- `evictionQueueSize`、`evictionQueueStaleEntries`：驱逐队列状态。

调试时可以打开 `--v=1 --logtostderr=1` 查看 BM 的 VLOG，包括 `MaybeReserve`、
`Reclaim`、spill candidate、reclaimer 回调等路径。

## 常见模式：外排 sort

简化流程：

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

后续 merge 阶段只保存 `BlockHandle`，访问时按需 `Pin()`：

```cpp
for (const auto& block : run.blocks) {
  BufferHandle handle = manager->Pin(block);
  Merge(handle.Ptr(), block->size());
}
```

完整 benchmark 示例见：

- `bolt/common/memory/bm/benchmark/BufferManagerSortBenchmark.cpp`
- `bolt/common/memory/bm/benchmark/BufferManagerParallelSortBenchmark.cpp`

## 错误处理

当前接口采用项目内异常风格：

- 内存分配失败会从 `MemoryPool` 路径抛出异常。
- 文件分配、IO、非法状态等会通过 `BOLT_FAIL` / `BOLT_CHECK` 异常或 fatal 路径暴露。
- 析构路径不会抛异常；严重生命周期/状态错误走 fatal 诊断。

调用方应在 operator/task 边界捕获异常，并按现有 query 失败路径处理。
