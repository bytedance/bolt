# BufferManager Usage Guide

`BufferManager` manages contiguous memory blocks used during execution. A
caller creates blocks with `Allocate()` and obtains accessible memory through a
`BufferHandle` returned by `Pin()`. Under memory pressure, unpinned resident
blocks can spill to disk and are loaded automatically by a later `Pin()`.

## Core Concepts

- `BlockHandle` is the durable logical handle for a block.
- `BufferHandle` represents one pin. It owns access to the payload through RAII
  and unpins the block when destroyed.
- `BlockMemory` is the internal state: payload, spill segment, pin count, and
  state-machine state.
- `BufferManager` owns a leaf `MemoryPool` and installs a reclaimer on it.

Keep a `BlockHandle` when the logical block will be accessed again. Keep a
`BufferHandle` only while its payload pointer is in use. A pointer returned by
`BufferHandle::Ptr()` is invalid after that handle is released.

## Header and Namespace

```cpp
#include "bolt/common/memory/bm/BufferManager.h"

using namespace bytedance::bolt::memory::bm;
```

## Initialization

Prepare a parent `MemoryPool`, then create the manager through `Create()`.
`BufferManager` must not be constructed directly on the stack.

```cpp
BufferManagerConfig config;
config.poolName = "sort-buffer-manager"; // Unique within the parent pool.
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

`fileAllocatorConfig` uses the `bm/file` module. See
`bolt/common/memory/bm/file/Usage.md` for its allocation rules.

BM reads and writes use the `bm/io` `DiskIoScheduler`. Its default backend uses
io_uring. If the runtime does not support or permit io_uring, scheduler
initialization can throw when a real spill or reload starts.

## API Levels

### Basic API

```cpp
BufferHandle Allocate(size_t size, MemoryTag tag);
BufferHandle Pin(const std::shared_ptr<BlockHandle>& block);
```

- `Allocate()` creates a block and returns it already pinned.
- `Pin()` accesses an existing block and returns an RAII-managed pin.

Start with these APIs so ownership and lifetime remain explicit.

### Advanced API

```cpp
std::vector<BufferHandle>
BatchAllocate(size_t count, size_t size, MemoryTag tag);
std::vector<BufferHandle> BatchPin(
    std::span<const std::shared_ptr<BlockHandle>> blocks);
void Prefetch(std::span<const std::shared_ptr<BlockHandle>> blocks);
void SpillBlocks(std::span<const std::shared_ptr<BlockHandle>> blocks);
```

These APIs affect batch pinning, asynchronous reads, proactive spills,
accounting, and error propagation. Add them only after the basic lifetime is
correct and a measured performance or memory-pressure need exists.

## Basic Operations

### Allocate

```cpp
BufferHandle handle = manager->Allocate(
    allocateSizeBytes(AllocateSize::kLarge), MemoryTag::kSort);

std::shared_ptr<BlockHandle> block = handle.block();
char* data = handle.Ptr();
FillBlock(data, block->size());
```

Save `handle.block()` if the logical block will be accessed later:

```cpp
runBlocks.push_back(handle.block());
```

Without a retained `BlockHandle`, the block is released with the last
`BufferHandle`, which is normally appropriate only for temporary buffers.

Use an accurate `MemoryTag`, such as `kSort`, `kHashBuild`, or `kAggregation`,
for diagnostics. Avoid using `kUnknown` as a permanent default.

### Pin

```cpp
{
  BufferHandle handle = manager->Pin(block);
  Consume(handle.Ptr(), block->size());
} // Automatically unpins the block.
```

The caller does not need to know the block's current location:

- resident blocks return their payload immediately;
- spilled blocks are loaded from disk;
- prefetching blocks wait for the existing read future.

`BufferHandle` is move-only. To release a pin early, leave its scope or assign
an empty handle:

```cpp
handle = BufferHandle{};
```

## Advanced Operations

### BatchAllocate

```cpp
std::vector<BufferHandle> handles =
    manager->BatchAllocate(blockCount, blockBytes, MemoryTag::kSort);
```

- `BatchAllocate(0, 0, tag)` returns an empty vector.
- A non-empty batch requires `size > 0`.
- Accounting is equivalent to repeated `Allocate()` calls.

### BatchPin

```cpp
std::vector<std::shared_ptr<BlockHandle>> blocks = ...;
std::vector<BufferHandle> handles = manager->BatchPin(blocks);

for (auto& handle : handles) {
  Consume(handle.Ptr());
}
```

Every returned handle unpins through RAII. A reload failure propagates as an
exception. Prefer `Pin()` for a small number of blocks.

### Prefetch

```cpp
manager->Prefetch(blocks);

// A later access still requires Pin() or BatchPin().
BufferHandle handle = manager->Pin(block);
```

`Prefetch()` is an asynchronous hint. It returns no handle and does not promise
that data is resident. Submission, allocation, and invalid-block failures
propagate through the existing exception path.

### SpillBlocks

```cpp
std::vector<std::shared_ptr<BlockHandle>> blocks = ...;
manager->SpillBlocks(blocks);
```

This is a best-effort proactive spill. A block is eligible only when it is
non-null, resident, has a valid payload, and has `pinCount == 0`. Pinned,
spilled, prefetching, spilling, and null blocks are skipped.

`SpillBlocks()` never releases a pin. Release the corresponding
`BufferHandle` first:

```cpp
std::shared_ptr<BlockHandle> block;
{
  BufferHandle handle = manager->Allocate(blockBytes, MemoryTag::kSort);
  block = handle.block();
  FillBlock(handle.Ptr());
} // The block is now eligible for spilling.

std::array<std::shared_ptr<BlockHandle>, 1> blocks{block};
manager->SpillBlocks(blocks);
```

Most callers should release handles and let the reclaimer or explicit
`Reclaim()` choose when to spill.

## Memory Reclamation

### Reclaim

The manager installs `BufferManagerReclaimer` on its leaf pool. Parent-pool
arbitration invokes the reclaimer, which spills eligible blocks.

```cpp
uint64_t reclaimed = manager->Reclaim(256 * 1024 * 1024);
```

- `targetBytes == 0` requests all currently reclaimable resident blocks.
- Only resident blocks with `pinCount == 0` are reclaimable.
- A block held by a `BufferHandle` is never spilled.

### MaybeReserve

Use `MaybeReserve()` to decide whether to close an active run before reaching a
memory limit:

```cpp
if (!manager->MaybeReserve(nextBlockBytes) && !activeRun.empty()) {
  Sort(activeRun);
  activeRun.clear(); // Releases pins and makes blocks reclaimable.
  manager->ReleaseUnusedReservation();
}

auto handle = manager->Allocate(nextBlockBytes, MemoryTag::kSort);
auto block = handle.block();
```

`MaybeReserve()` is only a reservation probe; it does not allocate. Call
`ReleaseUnusedReservation()` if a successful probe is not followed by an
allocation. Releasing a handle only makes a block eligible: the reclaimer,
`Reclaim()`, or `SpillBlocks()` performs the actual spill.

## External-Sort Pattern

Keep both block metadata and pins while writing a run:

```cpp
std::vector<std::shared_ptr<BlockHandle>> currentRunBlocks;
std::vector<BufferHandle> currentRunPins;

while (HasMoreInput()) {
  if (!manager->MaybeReserve(blockBytes) && !currentRunPins.empty()) {
    SortPinnedBlocks(currentRunPins);
    SaveRunMetadata(currentRunBlocks);

    currentRunPins.clear(); // Makes run blocks eligible for BM spill.
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

During merge, keep `BlockHandle`s and pin each block on demand:

```cpp
for (const auto& block : run.blocks) {
  BufferHandle handle = manager->Pin(block);
  Merge(handle.Ptr(), block->size());
}
```

Complete benchmark examples:

- `bolt/common/memory/bm/benchmark/BufferManagerSortBenchmark.cpp`
- `bolt/common/memory/bm/benchmark/BufferManagerParallelSortBenchmark.cpp`

## Lifetime Requirements

- `BufferManager` must outlive all `BufferHandle`, `BlockHandle`, and
  `BlockMemory` instances that it created.
- Destroying a `BufferHandle` after its owner manager has been destroyed takes
  the fatal diagnostic path; late destruction is unsupported.
- Operators and run metadata may retain `BlockHandle`s only while the manager
  remains alive.
- Spill segments are internal and must not be manipulated by callers.

A query- or operator-level root object should own a
`std::shared_ptr<BufferManager>` and release all handles and block metadata
before destroying it.

## Threading Model

`BufferManager` is currently a single-thread-owned object. Do not concurrently
call `Allocate()`, `Pin()`, `Reclaim()`, `Prefetch()`, or `SpillBlocks()` on the
same instance.

For multithreaded execution:

- give each worker or task its own manager;
- allow managers to share a parent execution pool so existing arbitration
  limits aggregate memory;
- ensure cross-thread reclaimer callbacks cannot enter a manager concurrently,
  or provide synchronization outside the manager.

## Observability

```cpp
BufferManagerStats stats = manager->stats();
std::vector<BufferManagerTagStats> tagStats = manager->tagStats();
std::string debug = manager->debugString();
```

Important fields include:

- `pinnedResidentBytes` and `unpinnedResidentBytes`;
- `spilledBytes`, `prefetchingBytes`, and `spillingBytes`;
- `reclaimedBytes`, `spillWriteBytes`, and `spillReadBytes`;
- `fileAllocateFailures`, `fileFreeFailures`, `readIoFailures`, and
  `writeIoFailures`;
- `evictionQueueSize` and `evictionQueueStaleEntries`.

These are broad operational metrics, not per-block or per-row trace metrics.
Sample them at operator lifecycle boundaries and export them through existing
runtime statistics. Do not add fine-grained instrumentation to BM hot paths.

Normal reserve, reclaim, proactive-spill, and reclaimer paths do not log. Use
`stats()`, `tagStats()`, `debugString()`, and I/O scheduler statistics for
diagnostics. Logs are reserved for failures and suspicious events.

In release builds, an internal accounting underflow emits a rate-limited
`[MEM][BM]` warning and saturates the value at zero. Debug builds also expose it
through `BOLT_DCHECK`.

## Error Handling

- Allocation failures propagate from `MemoryPool`.
- File, I/O, and invalid-state failures use the project's `BOLT_FAIL` and
  `BOLT_CHECK` exception paths, or a fatal path where required.
- Destructors do not throw; severe lifetime or state violations are fatal.

Catch exceptions at the operator or task boundary and use the existing query
failure path.
