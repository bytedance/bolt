# BM Spill Compression Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add BM spill compression using LZ4, ZSTD, and Snappy while keeping BufferManager unaware of compression metadata.

**Architecture:** Add a focused `bm/compress` library for codec APIs and spill record encoding. Move `FileBlockAllocator` ownership into `SpillStore`, expose block-level write/read methods, and let `BufferManager` only call `WriteBlock` and `SubmitReadBlock`.

**Tech Stack:** C++20, GTest, BM DiskIoScheduler, FileBlockAllocator, LZ4/ZSTD/Snappy native APIs.

---

## File Map

- Create `bolt/common/memory/bm/compress/CompressionConfig.h`: compression thresholds and algorithm config.
- Create `bolt/common/memory/bm/compress/CompressionCodec.h/.cpp`: public compression/decompression helpers and algorithm dispatch.
- Create `bolt/common/memory/bm/compress/SpillRecordHeader.h/.cpp`: fixed header encode/decode validation.
- Create `bolt/common/memory/bm/compress/CMakeLists.txt`: `bolt_memory_bm_compress` and optional tests.
- Create `bolt/common/memory/bm/compress/tests/CMakeLists.txt`.
- Create `bolt/common/memory/bm/compress/tests/CompressionCodecTest.cpp`.
- Modify `bolt/common/memory/bm/SpillStore.h/.cpp`: add `SpillStoreConfig`, `WriteBlock`, `SpillReadFuture`, `SubmitReadBlock`, and own `FileBlockAllocator`.
- Modify `bolt/common/memory/bm/BlockHandle.h`: change `prefetchFuture` type to `SpillReadFuture`.
- Modify `bolt/common/memory/bm/BufferManager.h/.cpp`: pass `SpillStoreConfig`, remove allocator member, call block-level spill APIs, update physical compression stats.
- Modify `bolt/common/memory/bm/BufferManagerObservability.h/.cpp`: add physical/compression stats fields.
- Modify `bolt/common/memory/bm/CMakeLists.txt`: add compress subdirectory and link target.
- Modify `bolt/common/memory/bm/tests/BufferManagerTest.cpp`: update config field path and add integration tests.

## Tasks

### Task 1: Compress Module Red Test

**Files:**
- Create: `bolt/common/memory/bm/compress/tests/CompressionCodecTest.cpp`
- Create: `bolt/common/memory/bm/compress/tests/CMakeLists.txt`
- Create: `bolt/common/memory/bm/compress/CMakeLists.txt`
- Modify: `bolt/common/memory/bm/CMakeLists.txt`

- [ ] **Step 1: Write tests for codec behavior**

Add tests for LZ4/ZSTD/Snappy round-trip, threshold fallback, `NONE`, and incompressible fallback.

- [ ] **Step 2: Run test target and verify it fails to build**

Run: `cmake --build --preset conan-release --target bolt_memory_bm_compress_test`

Expected: FAIL because compression headers/functions do not exist yet.

### Task 2: Compress Module Implementation

**Files:**
- Create: `bolt/common/memory/bm/compress/CompressionConfig.h`
- Create: `bolt/common/memory/bm/compress/CompressionCodec.h`
- Create: `bolt/common/memory/bm/compress/CompressionCodec.cpp`
- Create: `bolt/common/memory/bm/compress/SpillRecordHeader.h`
- Create: `bolt/common/memory/bm/compress/SpillRecordHeader.cpp`

- [ ] **Step 1: Implement config, codec dispatch, and header helpers**

Use native APIs: `LZ4_compress_default`, `ZSTD_compress`, `snappy::RawCompress`, and matching decompression APIs. Allocate returned buffers from `MemoryPool` using `IoBuffer::allocateFromPool`.

- [ ] **Step 2: Build and run compress tests**

Run: `cmake --build --preset conan-release --target bolt_memory_bm_compress_test`

Then run the resulting test binary from the release build tree.

Expected: PASS.

### Task 3: SpillStore and BufferManager Red Test

**Files:**
- Modify: `bolt/common/memory/bm/tests/BufferManagerTest.cpp`

- [ ] **Step 1: Update BM tests for new config and add compression integration cases**

Change test config to `config.spillStoreConfig.fileAllocatorConfig`. Add tests for default compression readback, high threshold readback, and LZ4/ZSTD/Snappy readback.

- [ ] **Step 2: Run BM test build and verify failure**

Run: `cmake --build --preset conan-release --target bolt_memory_bm_test`

Expected: FAIL because `SpillStoreConfig`, `compressionConfig`, and block-level APIs do not exist yet.

### Task 4: SpillStore and BM Implementation

**Files:**
- Modify: `bolt/common/memory/bm/SpillStore.h`
- Modify: `bolt/common/memory/bm/SpillStore.cpp`
- Modify: `bolt/common/memory/bm/BlockHandle.h`
- Modify: `bolt/common/memory/bm/BufferManager.h`
- Modify: `bolt/common/memory/bm/BufferManager.cpp`
- Modify: `bolt/common/memory/bm/BufferManagerObservability.h`
- Modify: `bolt/common/memory/bm/BufferManagerObservability.cpp`

- [ ] **Step 1: Add SpillStoreConfig and block-level write/read APIs**

`SpillStore` owns `FileBlockAllocator`, writes `[header][storedPayload]`, returns `SpillWriteResult`, and returns a `SpillReadFuture` whose `get()` decodes on the caller thread.

- [ ] **Step 2: Update BufferManager integration**

Remove `allocator_`, construct `SpillStore(config_.spillStoreConfig, pool_.get())`, call `WriteBlock` and `SubmitReadBlock`, and keep `BlockMemory` free of compression metadata.

- [ ] **Step 3: Add stats fields**

Track physical write/read bytes, compressed block count, compression time, and decompression time.

- [ ] **Step 4: Build and run BM tests**

Run: `cmake --build --preset conan-release --target bolt_memory_bm_test`

Then run the resulting BM test binary.

Expected: PASS or skip existing io_uring-unavailable cases.

### Task 5: Final Verification

**Files:**
- All files changed above.

- [ ] **Step 1: Build narrow targets**

Run:

```bash
cmake --build --preset conan-release --target bolt_memory_bm_compress_test
cmake --build --preset conan-release --target bolt_memory_bm_test
```

- [ ] **Step 2: Run tests**

Run both test binaries from the release build tree.

- [ ] **Step 3: Inspect git diff**

Run: `git diff --stat` and `git diff --check`.

Expected: no whitespace errors; unrelated existing `Makefile` and unrelated untracked spec remain untouched.
