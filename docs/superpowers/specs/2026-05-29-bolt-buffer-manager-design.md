# Bolt BufferManager Design

## Scope

This design adds a DuckDB-style BufferManager under
`bolt/common/memory/bm`. The first version manages execution-time temporary
blocks only. It does not manage persistent database pages, checkpoints, WAL, or
page-cache semantics.

The manager owns resident block memory through a dedicated child
`MemoryPool`, spills unpinned blocks through the existing BM file allocator, and
uses the BM disk IO scheduler for read and write requests.

Out of scope for the first version:

- `PinReadOnly` or const buffer handles.
- `canDestroy` / destroyable blocks.
- block resize / `ReAllocate`.
- independent BufferManager memory limits.
- queue purge or compaction.
- thread-safe public API.

## Public API

`BufferManager` is created through a factory so handles can keep a weak owner
reference for diagnostics:

```cpp
class BufferManager : public std::enable_shared_from_this<BufferManager> {
 public:
  static std::shared_ptr<BufferManager> Create(
      memory::MemoryPool& parent,
      BufferManagerConfig config);

  BufferHandle Allocate(
      size_t size,
      std::shared_ptr<BlockHandle>* block = nullptr);

  BufferHandle Pin(const std::shared_ptr<BlockHandle>& block);

  std::vector<BufferHandle> BatchPin(
      std::span<const std::shared_ptr<BlockHandle>> blocks);

  void Prefetch(std::span<const std::shared_ptr<BlockHandle>> blocks);

  uint64_t ReclaimForTest(uint64_t targetBytes);
};
```

`Allocate` follows DuckDB's model: it creates a block and returns an already
pinned mutable `BufferHandle`. If `block` is not null, the created
`BlockHandle` is written there for future `Pin`, `BatchPin`, or `Prefetch`
calls.

`Pin`, `BatchPin`, and `Allocate` return handles directly. Resource failures
such as memory allocation failure, memory arbitration failure, file allocation
failure, and IO failure use Bolt's existing exception path. This keeps memory
failure behavior aligned with `MemoryPool` and DuckDB.

`BatchPin` is all-or-throw from the caller's perspective. If every block is
pinned, it returns handles in input order. If any pin fails, the function throws
and any handles already created during the call are destroyed during stack
unwind.

`Prefetch` is fire-and-forget. It does not return a handle, does not pin the
block, and does not guarantee that the block remains resident until a later
`Pin`.

## Configuration

```cpp
struct BufferManagerConfig {
  std::string poolName{"buffer_manager"};
  FileBlockAllocatorConfig fileAllocatorConfig;
  IoPriority readPriority{IoPriority::High};
  IoPriority writePriority{IoPriority::Medium};
  IoPriority prefetchPriority{IoPriority::Low};
};
```

The manager creates a leaf child pool from `parent` and installs a
BufferManager memory reclaimer on that pool. The manager also creates and owns a
dedicated `FileBlockAllocator` with `CreateFileBlockAllocator`.

## Core Types

The type model mirrors DuckDB:

- `BlockHandle`: caller-visible logical block handle. It wraps a
  `std::shared_ptr<BlockMemory>`.
- `BlockMemory`: internal state and resource owner for a block.
- `BufferHandle`: move-only RAII pin guard. Destruction calls
  `BufferManager::Unpin`.

`BufferHandle` is not copyable. Move construction and move assignment transfer
the pin. `Destroy()` is idempotent. `Ptr()` returns the resident payload pointer
and checks that the handle is valid.

`BufferHandle` holds a `std::weak_ptr<BufferManager>` for diagnostics. On
destruction it locks the manager and calls `Unpin`; lock failure is a
`BOLT_CHECK` because handles must not outlive their manager.

## Resource Ownership

Resident payload memory is an `IoBuffer` allocated from the manager's leaf pool:

```cpp
IoBuffer::allocateFromPool(pool_.get(), size)
```

`BlockMemory` stores the resident payload as an `IoBuffer`, not as a raw
pointer. When a payload is moved into an IO request, scheduler ownership keeps
the memory alive until the `IoResult` is returned. If the `IoBuffer` is
destroyed, its deleter frees memory back to the leaf pool.

Spill locations are wrapped in an `OwnedFileExtent` RAII helper. The helper
stores a `FileExtent` and a `std::weak_ptr<FileBlockAllocator>`. Destruction
locks the allocator and calls `Free(extent)`. Lock failure or free failure is a
`BOLT_CHECK`, because extents must not outlive their manager-owned allocator and
each extent must be freed exactly once.

`BufferManager` is expected to outlive all `BlockHandle`, `BlockMemory`, and
`BufferHandle` objects it creates. Weak owner references exist to diagnose
violations, not to make late destruction supported.

## Block States

The first version uses three states:

```text
IN_MEMORY    payload is resident in the leaf MemoryPool
SPILLED      payload is not resident; a FileExtent holds the durable copy
PREFETCHING  async read has been submitted; the old FileExtent is still held
```

There is no dirty bit. Any in-memory unpinned block selected by reclaim is
written to a fresh file extent.

### State Transitions

`Allocate(size)`:

```text
new block -> IN_MEMORY(pin_count = 1)
```

`Unpin`:

```text
IN_MEMORY(pin_count > 1) -> IN_MEMORY(pin_count - 1)
IN_MEMORY(pin_count == 1) -> IN_MEMORY(pin_count = 0), enqueue for eviction
```

`Pin`:

```text
IN_MEMORY    -> pin_count++
SPILLED      -> read extent synchronously, install payload, free extent,
                pin_count = 1
PREFETCHING  -> wait for prefetch future, install payload, free extent,
                pin_count = 1
```

If prefetch IO fails and a later `Pin` observes that result, the block returns
to `SPILLED` and `Pin` retries with a synchronous read. A synchronous read
failure throws and leaves the block spilled.

`Prefetch`:

```text
SPILLED -> PREFETCHING(read future)
```

`Prefetch` skips blocks already `IN_MEMORY` or `PREFETCHING`.

`Reclaim`:

```text
IN_MEMORY(pin_count = 0) -> write fresh extent synchronously
                         -> SPILLED on success
```

Reclaim skips `SPILLED`, pinned `IN_MEMORY`, and unfinished `PREFETCHING`
blocks. Before selecting victims, reclaim performs non-blocking harvest of
ready prefetch futures. A harvested block becomes `IN_MEMORY(pin_count = 0)`
and can be reclaimed in the same pass.

## IO Integration

All read and write requests use `DiskIoScheduler`.

Readback from `SPILLED`:

1. Allocate an `IoBuffer` from the BM leaf pool.
2. Submit a read request using the block's file extent.
3. Wait for the future.
4. On success, move `result.buffer` into `BlockMemory::payload`.
5. Release the old `OwnedFileExtent`.
6. On failure, let `result.buffer` release memory and throw.

Spill from `IN_MEMORY`:

1. Allocate a fresh file extent for the block size.
2. Move `BlockMemory::payload` into a write request.
3. Submit and wait for the future.
4. On success, keep the fresh `OwnedFileExtent`; let `result.buffer` release
   memory back to the pool; set state to `SPILLED`.
5. On failure, move `result.buffer` back into `BlockMemory::payload`, free the
   fresh extent, keep state `IN_MEMORY`, and throw.

Prefetch:

1. Allocate an `IoBuffer` from the BM leaf pool.
2. Submit a read request.
3. Store the returned future in the block and set state to `PREFETCHING`.
4. Later `Pin` waits for the future, or reclaim/API entry points harvest it
   with a zero-timeout readiness check.

## File Allocator Integration

The manager owns one non-thread-safe `FileBlockAllocator` instance. This fits
the first-version single-threaded BM contract.

Each spill write allocates a fresh extent. Pin or successful prefetch releases
the old extent after the payload is installed in memory. This keeps the state
model simple: when a block is resident, it has no retained disk copy.

## MemoryPool And Reclaim Integration

`BufferManager::Create` creates a leaf child pool and installs a
`BufferManagerReclaimer` using `setReclaimer`.

The reclaimer holds a `std::weak_ptr<BufferManager>`. Normal use expects the
manager to be alive. If the weak pointer has expired, the reclaimer reports no
reclaimable memory.

`reclaimableBytes()` returns `unpinnedResidentBytes_`. Under the single-thread
contract this is effectively exact and avoids scanning the eviction queue.

`unpinnedResidentBytes_` updates:

- `Allocate`: no change because the new block is pinned.
- `Unpin` to zero in `IN_MEMORY`: add block size.
- `Pin` of an unpinned `IN_MEMORY` block: subtract block size.
- `Pin` from `SPILLED` or `PREFETCHING`: no change because the result is pinned.
- ready prefetch harvest to `IN_MEMORY(pin_count = 0)`: add block size.
- successful reclaim of an unpinned resident block: subtract block size.

`reclaim(targetBytes, maxWaitMs, stats)` drives the same logic as
`ReclaimForTest`. IO or file errors throw. The returned reclaimed bytes are the
actual resident bytes released by successful spills.

`maxWaitMs` is accepted for interface compatibility. The first version may
ignore it because spill writes are synchronous from the reclaimer's point of
view.

## Eviction Queue

The eviction queue follows DuckDB's append-only sequence-number model:

```cpp
struct EvictionEntry {
  std::weak_ptr<BlockMemory> block;
  uint64_t sequence;
};
```

Each `BlockMemory` has an `evictionSequence`.

- `Unpin` to zero increments the sequence and appends an entry.
- `Pin` increments the sequence, invalidating older queue entries.
- Reclaim pops from the front and skips expired weak pointers, sequence
  mismatches, pinned blocks, and non-`IN_MEMORY` blocks.

The first version does not implement DuckDB-style purge. Stale queue entries
are removed naturally when reclaim scans the queue.

## Threading Model

The first version is not a thread-safe BufferManager.

Callers must serialize all public calls on a manager instance, including
`Allocate`, `Pin`, `BatchPin`, `Prefetch`, `ReclaimForTest`, and handle
destruction. Production reclaim must enter through existing Bolt
operator/task reclaim coordination so that BM reclaim does not race with normal
BM API calls.

`Prefetch` submits asynchronous IO, but background IO completion does not
mutate BM state directly. State installation happens in `Pin` or in
non-blocking harvest performed by BM API/reclaim entry points.

The owned file allocator is also not thread-safe and is only used under the
same single-threaded BM contract.

## Error Handling

Memory allocation and memory arbitration failures use existing `MemoryPool`
exceptions.

File allocator errors are converted to Bolt exceptions with the file error code
and native errno where available.

IO scheduler errors are converted to Bolt exceptions with the IO error code and
native errno where available.

Reclaim write failure preserves the in-memory payload by moving the returned
`IoBuffer` back into the block before throwing.

Invalid internal states use `BOLT_CHECK`.

## Tests

Unit tests should cover:

- `Allocate` returns a pinned handle and writes the optional `BlockHandle`.
- `BufferHandle` is move-only and unpins exactly once.
- `Pin` returns existing resident payload without IO.
- `ReclaimForTest` spills unpinned blocks and releases MemoryPool bytes.
- `Pin` reads a spilled block back and releases the old file extent.
- `BatchPin` pins all blocks in order and unpins already-pinned handles on
  failure.
- `Prefetch` transitions `SPILLED` to `PREFETCHING`; `Pin` harvests the future.
- Reclaim harvests ready prefetches and can spill those blocks in the same
  pass.
- Reclaim skips pinned blocks.
- Eviction queue stale entries are skipped by sequence number.
- IO write failure during reclaim restores payload and leaves the block
  resident.
- File extent is freed exactly once across pin, reclaim, and destruction paths.
- `BufferManagerReclaimer::reclaimableBytes` follows `unpinnedResidentBytes_`.

Integration tests should verify interaction with a real `MemoryPool`, real
`FileBlockAllocator`, and `DiskIoScheduler` where the environment supports the
configured IO backend.
