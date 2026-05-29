# BufferManager Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the first Bolt BM BufferManager described in `bolt/common/memory/bm/docs/superpowers/specs/2026-05-29-bolt-buffer-manager-design.md`.

**Architecture:** Implement BufferManager directly under `bolt/common/memory/bm` and expose it through a top-level `bolt_memory_bm` library. The implementation follows DuckDB's `BlockHandle` / `BlockMemory` / `BufferHandle` model, stores resident payloads in MemoryPool-backed `IoBuffer`, stores spilled locations in `OwnedFileExtent`, and uses `DiskIoScheduler` for read/write IO. Reclaim is exposed through a `MemoryReclaimer` installed on a BM-owned child pool.

**Tech Stack:** C++20, existing Bolt `MemoryPool` / `MemoryReclaimer`, BM `FileBlockAllocator`, BM `DiskIoScheduler`, GTest.

---

## Files

- Modify: `bolt/common/memory/bm/CMakeLists.txt` to define `bolt_memory_bm` from the new BufferManager sources and link `bolt_memory_bm_file`, `bolt_memory_bm_io`, and `bolt_memory`.
- Modify: `bolt/common/memory/bm/file/FileBlockAllocator.h` and `.cpp` so `CreateFileBlockAllocator` returns `std::shared_ptr<FileBlockAllocator>`.
- Modify: file allocator tests that expect `unique_ptr` factory behavior.
- Create: `bolt/common/memory/bm/MemoryTag.h`.
- Create: `bolt/common/memory/bm/OwnedFileExtent.h/.cpp`.
- Create: `bolt/common/memory/bm/BufferHandle.h/.cpp`.
- Create: `bolt/common/memory/bm/BlockHandle.h/.cpp`.
- Create: `bolt/common/memory/bm/BufferManager.h/.cpp`.
- Create: `bolt/common/memory/bm/tests/CMakeLists.txt`.
- Create: `bolt/common/memory/bm/tests/BufferManagerTest.cpp`.

## Task 1: File Allocator Shared Ownership

**Files:**
- Modify: `bolt/common/memory/bm/file/FileBlockAllocator.h`
- Modify: `bolt/common/memory/bm/file/FileBlockAllocator.cpp`
- Modify: `bolt/common/memory/bm/file/tests/FileBlockAllocatorFactoryTest.cpp`

- [ ] **Step 1: Write/update failing factory test**

Add a test that documents shared ownership:

```cpp
TEST(FileBlockAllocatorFactoryTest, createReturnsSharedAllocator) {
  auto allocator = CreateFileBlockAllocator(testConfig());
  static_assert(std::is_same_v<decltype(allocator), std::shared_ptr<FileBlockAllocator>>);
  ASSERT_NE(nullptr, allocator);
  auto other = allocator;
  EXPECT_EQ(2, allocator.use_count());
}
```

- [ ] **Step 2: Run test and verify failure**

Run: `cmake --build --preset conan-$(cat _build/.build_type) --target bolt_memory_bm_file_test`

Expected: compile failure because the factory still returns `std::unique_ptr<FileBlockAllocator>`.

- [ ] **Step 3: Change factory signature and implementation**

Change header:

```cpp
std::shared_ptr<FileBlockAllocator> CreateFileBlockAllocator(
    FileBlockAllocatorConfig config);
```

Change implementation:

```cpp
std::shared_ptr<FileBlockAllocator> CreateFileBlockAllocator(
    FileBlockAllocatorConfig config) {
  return std::make_shared<FileBlockAllocatorImpl>(std::move(config));
}
```

- [ ] **Step 4: Build file tests**

Run: `cmake --build --preset conan-$(cat _build/.build_type) --target bolt_memory_bm_file_test`

Expected: build succeeds.

- [ ] **Step 5: Commit**

```bash
git add bolt/common/memory/bm/file/FileBlockAllocator.h \
        bolt/common/memory/bm/file/FileBlockAllocator.cpp \
        bolt/common/memory/bm/file/tests/FileBlockAllocatorFactoryTest.cpp
git commit -m "Update BM file allocator factory ownership"
```

## Task 2: BufferManager Skeleton

**Files:**
- Modify: `bolt/common/memory/bm/CMakeLists.txt`
- Create: `bolt/common/memory/bm/MemoryTag.h`
- Create: `bolt/common/memory/bm/tests/CMakeLists.txt`
- Create: `bolt/common/memory/bm/tests/BufferManagerTest.cpp`

- [ ] **Step 1: Write compile-only test for API shape**

Create a test that includes the future headers and references the API:

```cpp
TEST(BufferManagerApiTest, memoryTagHasStableNames) {
  EXPECT_EQ("Unknown", toString(MemoryTag::kUnknown));
  EXPECT_EQ("Testing", toString(MemoryTag::kTesting));
}
```

- [ ] **Step 2: Run build and verify failure**

Run: `cmake --build --preset conan-$(cat _build/.build_type) --target bolt_memory_bm_test`

Expected: target/header missing.

- [ ] **Step 3: Add CMake target and MemoryTag**

Create `MemoryTag.h`:

```cpp
enum class MemoryTag : uint8_t {
  kUnknown,
  kHashBuild,
  kAggregation,
  kSort,
  kWindow,
  kExchange,
  kTesting,
};

const char* toString(MemoryTag tag);
```

Implement `toString` inline with a switch.

- [ ] **Step 4: Build skeleton test**

Run: `cmake --build --preset conan-$(cat _build/.build_type) --target bolt_memory_bm_test`

Expected: build succeeds.

- [ ] **Step 5: Commit**

```bash
git add bolt/common/memory/bm/CMakeLists.txt bolt/common/memory/bm/MemoryTag.h bolt/common/memory/bm/tests
git commit -m "Add BM BufferManager skeleton"
```

## Task 3: RAII Types

**Files:**
- Create: `OwnedFileExtent.h/.cpp`
- Create: `BlockHandle.h/.cpp`
- Create: `BufferHandle.h/.cpp`
- Test: `BufferManagerTest.cpp`

- [ ] **Step 1: Write failing tests for move-only handles and MemoryTag storage**

Test that `BufferHandle` is move-only and `BlockHandle` exposes size/tag:

```cpp
static_assert(!std::is_copy_constructible_v<BufferHandle>);
static_assert(std::is_move_constructible_v<BufferHandle>);

TEST(BufferManagerHandleTest, blockHandleExposesSizeAndTag) {
  auto block = testingCreateBlockHandle(4096, MemoryTag::kTesting);
  EXPECT_EQ(4096, block->size());
  EXPECT_EQ(MemoryTag::kTesting, block->tag());
}
```

- [ ] **Step 2: Run test and verify failure**

Run: `cmake --build --preset conan-$(cat _build/.build_type) --target bolt_memory_bm_test`

Expected: missing types/helpers.

- [ ] **Step 3: Implement minimal RAII types**

Implement `BlockMemory`, `BlockHandle`, `BufferHandle`, and `OwnedFileExtent` enough to compile:

- `BlockMemory` stores `id`, `size`, `tag`, `state`, `pinCount`, `evictionSequence`, optional `IoBuffer`, optional `OwnedFileExtent`, optional prefetch future.
- `BlockHandle` wraps `std::shared_ptr<BlockMemory>`.
- `BufferHandle` stores `std::weak_ptr<BufferManager>` and `std::shared_ptr<BlockHandle>`, is move-only, `Destroy()` is idempotent.
- `OwnedFileExtent` holds `FileExtent` and `std::weak_ptr<FileBlockAllocator>`.

- [ ] **Step 4: Build handle tests**

Run: `cmake --build --preset conan-$(cat _build/.build_type) --target bolt_memory_bm_test`

Expected: build succeeds.

- [ ] **Step 5: Commit**

```bash
git add bolt/common/memory/bm/BlockHandle.h bolt/common/memory/bm/BlockHandle.cpp bolt/common/memory/bm/BufferHandle.h bolt/common/memory/bm/BufferHandle.cpp bolt/common/memory/bm/OwnedFileExtent.h bolt/common/memory/bm/OwnedFileExtent.cpp bolt/common/memory/bm/tests/BufferManagerTest.cpp
git commit -m "Add BM buffer handle primitives"
```

## Task 4: Allocate, Pin, and Unpin In Memory

**Files:**
- Modify: `BufferManager.h/.cpp`
- Test: `BufferManagerTest.cpp`

- [ ] **Step 1: Write failing Allocate/Pin tests**

Tests:

```cpp
TEST(BufferManagerTest, allocateReturnsPinnedWritablePayload) {
  auto bm = makeTestBufferManager();
  std::shared_ptr<BlockHandle> block;
  auto handle = bm->Allocate(4096, MemoryTag::kTesting, &block);
  ASSERT_NE(nullptr, block);
  ASSERT_NE(nullptr, handle.Ptr());
  std::memset(handle.Ptr(), 7, 4096);
  EXPECT_EQ(4096, block->size());
  EXPECT_EQ(MemoryTag::kTesting, block->tag());
}

TEST(BufferManagerTest, pinResidentBlockSeesWrittenBytes) {
  auto bm = makeTestBufferManager();
  std::shared_ptr<BlockHandle> block;
  {
    auto handle = bm->Allocate(4096, MemoryTag::kTesting, &block);
    handle.Ptr()[0] = 42;
  }
  auto repin = bm->Pin(block);
  EXPECT_EQ(42, repin.Ptr()[0]);
}
```

- [ ] **Step 2: Verify tests fail**

Run: `cmake --build --preset conan-$(cat _build/.build_type) --target bolt_memory_bm_test`

Expected: missing implementation.

- [ ] **Step 3: Implement Allocate, Pin resident, Unpin**

Implement:

- factory `Create`
- `Allocate` creates `BlockMemory`, allocates `IoBuffer::allocateFromPool`, pin count 1.
- `Pin` for `IN_MEMORY` increments pin count and returns `BufferHandle`.
- `Unpin` decrements, appends eviction entry when count reaches 0.
- `unpinnedResidentBytes_` update rules.

- [ ] **Step 4: Build tests**

Run: `cmake --build --preset conan-$(cat _build/.build_type) --target bolt_memory_bm_test`

Expected: build succeeds.

- [ ] **Step 5: Commit**

```bash
git add bolt/common/memory/bm/BlockHandle.h bolt/common/memory/bm/BlockHandle.cpp bolt/common/memory/bm/BufferHandle.h bolt/common/memory/bm/BufferHandle.cpp bolt/common/memory/bm/OwnedFileExtent.h bolt/common/memory/bm/OwnedFileExtent.cpp bolt/common/memory/bm/tests/BufferManagerTest.cpp
git commit -m "Implement BM allocate and resident pin"
```

## Task 5: Reclaim Spill and Pin Readback

**Files:**
- Modify: `BufferManager.cpp`
- Test: `BufferManagerTest.cpp`

- [ ] **Step 1: Write failing spill/readback tests**

Tests:

```cpp
TEST(BufferManagerTest, reclaimSpillsUnpinnedBlockAndPinReadsBack) {
  auto bm = makeTestBufferManager();
  std::shared_ptr<BlockHandle> block;
  {
    auto handle = bm->Allocate(4096, MemoryTag::kTesting, &block);
    handle.Ptr()[0] = 99;
  }
  EXPECT_GE(bm->ReclaimForTest(4096), 4096);
  auto handle = bm->Pin(block);
  EXPECT_EQ(99, handle.Ptr()[0]);
}
```

- [ ] **Step 2: Verify test fails**

Run: `cmake --build --preset conan-$(cat _build/.build_type) --target bolt_memory_bm_test`

Expected: reclaim/pin spilled not implemented.

- [ ] **Step 3: Implement spill/readback**

Implement:

- file extent allocation
- write request via scheduler
- `SPILLING` transition and failure restore
- readback from `SPILLED`
- fatal on old extent release failure after readback

- [ ] **Step 4: Build tests**

Run: `cmake --build --preset conan-$(cat _build/.build_type) --target bolt_memory_bm_test`

Expected: build succeeds.

- [ ] **Step 5: Commit**

```bash
git add bolt/common/memory/bm/BlockHandle.h bolt/common/memory/bm/BlockHandle.cpp bolt/common/memory/bm/BufferHandle.h bolt/common/memory/bm/BufferHandle.cpp bolt/common/memory/bm/OwnedFileExtent.h bolt/common/memory/bm/OwnedFileExtent.cpp bolt/common/memory/bm/tests/BufferManagerTest.cpp
git commit -m "Implement BM spill and readback"
```

## Task 6: BatchPin and Prefetch

**Files:**
- Modify: `BufferManager.cpp`
- Test: `BufferManagerTest.cpp`

- [ ] **Step 1: Write failing tests**

Tests cover:

- `BatchPin` returns handles in input order.
- `BatchPin` reads multiple spilled blocks with submitted futures.
- `Prefetch` transitions spilled block and later `Pin` harvests it.
- `Prefetch` allocation/submission failure does not throw and leaves block spilled.

- [ ] **Step 2: Verify tests fail**

Run: `cmake --build --preset conan-$(cat _build/.build_type) --target bolt_memory_bm_test`

Expected: batch/prefetch missing.

- [ ] **Step 3: Implement BatchPin and Prefetch**

Implement:

- collect spilled blocks
- submit all read futures before waiting
- reuse existing prefetch futures
- prefetch fire-and-forget with failure logging
- harvest ready prefetches at reclaim/API entry points

- [ ] **Step 4: Build tests**

Run: `cmake --build --preset conan-$(cat _build/.build_type) --target bolt_memory_bm_test`

Expected: build succeeds.

- [ ] **Step 5: Commit**

```bash
git add bolt/common/memory/bm/BlockHandle.h bolt/common/memory/bm/BlockHandle.cpp bolt/common/memory/bm/BufferHandle.h bolt/common/memory/bm/BufferHandle.cpp bolt/common/memory/bm/OwnedFileExtent.h bolt/common/memory/bm/OwnedFileExtent.cpp bolt/common/memory/bm/tests/BufferManagerTest.cpp
git commit -m "Implement BM batch pin and prefetch"
```

## Task 7: MemoryReclaimer Integration

**Files:**
- Modify: `BufferManager.h/.cpp`
- Test: `BufferManagerTest.cpp`

- [ ] **Step 1: Write failing reclaimer tests**

Tests:

```cpp
TEST(BufferManagerReclaimerTest, reportsUnpinnedResidentBytes) {
  auto bm = makeTestBufferManager();
  std::shared_ptr<BlockHandle> block;
  {
    auto handle = bm->Allocate(4096, MemoryTag::kTesting, &block);
  }
  uint64_t reclaimable = 0;
  ASSERT_TRUE(bm->pool()->reclaimer()->reclaimableBytes(*bm->pool(), reclaimable));
  EXPECT_EQ(4096, reclaimable);
}
```

- [ ] **Step 2: Verify test fails**

Run: `cmake --build --preset conan-$(cat _build/.build_type) --target bolt_memory_bm_test`

Expected: reclaimer missing.

- [ ] **Step 3: Implement BufferManagerReclaimer**

Implement:

- weak owner
- `reclaimableBytes`
- `reclaim`
- debug single-thread/reentrancy guard

- [ ] **Step 4: Build tests**

Run: `cmake --build --preset conan-$(cat _build/.build_type) --target bolt_memory_bm_test`

Expected: build succeeds.

- [ ] **Step 5: Commit**

```bash
git add bolt/common/memory/bm/BlockHandle.h bolt/common/memory/bm/BlockHandle.cpp bolt/common/memory/bm/BufferHandle.h bolt/common/memory/bm/BufferHandle.cpp bolt/common/memory/bm/OwnedFileExtent.h bolt/common/memory/bm/OwnedFileExtent.cpp bolt/common/memory/bm/tests/BufferManagerTest.cpp
git commit -m "Connect BM BufferManager to memory reclaim"
```

## Task 8: Final Verification

**Files:**
- All changed files.

- [ ] **Step 1: Build narrow targets**

Run:

```bash
cmake --build --preset conan-$(cat _build/.build_type) --target bolt_memory_bm_file_test
cmake --build --preset conan-$(cat _build/.build_type) --target bolt_memory_bm_test
```

Expected: both builds succeed.

- [ ] **Step 2: Run tests if binaries are available**

Run the produced test binaries from the current build tree. Expected: all new BM file and buffer manager tests pass.

- [ ] **Step 3: Commit any final fixes**

```bash
git add bolt/common/memory/bm/BlockHandle.h bolt/common/memory/bm/BlockHandle.cpp bolt/common/memory/bm/BufferHandle.h bolt/common/memory/bm/BufferHandle.cpp bolt/common/memory/bm/OwnedFileExtent.h bolt/common/memory/bm/OwnedFileExtent.cpp bolt/common/memory/bm/tests/BufferManagerTest.cpp
git commit -m "Finalize BM BufferManager implementation"
```
