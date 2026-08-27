/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <vector>

#include "bolt/common/memory/MemoryPool.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// L0 of the Cell shuffle writer: the only component that talks to the
/// engine MemoryPool for cell data.
///
/// "Chunk owns memory, Cell owns data": memory is requested from and returned
/// to the pool exclusively in fixed-size chunks (default 4MB, matching the
/// pool's reservation quantum), each carved into fixed-size cells. Cells are
/// handed out by id; what a cell holds is the concern of the directory layer
/// above.
///
/// Accounting is exact and O(1): allocatedBytes() is live chunks times chunk
/// size, which is precisely the pool-visible footprint; usedCells() counts
/// cells currently handed out.
///
/// Not thread-safe; a shuffle writer is single-threaded.
class ChunkAllocator {
 public:
  static constexpr uint32_t kInvalidCell = 0xFFFFFFFFu;

  /// Called before the allocator asks the pool for a new chunk. This is the
  /// caller's budget choke point: it may reserve memory, or spill and
  /// recycle() cells; allocation then retries the freelist before growing.
  using GrowCallback = std::function<void()>;

  /// Both sizes must be powers of two, 0 < cellBytes <= chunkBytes.
  ChunkAllocator(
      memory::MemoryPool* pool,
      uint32_t chunkBytes,
      uint32_t cellBytes);
  ~ChunkAllocator();

  ChunkAllocator(const ChunkAllocator&) = delete;
  ChunkAllocator& operator=(const ChunkAllocator&) = delete;

  /// Returns a cell id. Order of preference: freelist, never-yet-handed slot
  /// of a retained chunk, then a new chunk from the pool (invoking beforeGrow
  /// first and re-checking the freelist after it, since beforeGrow may spill
  /// and recycle).
  uint32_t allocCell(const GrowCallback& beforeGrow);

  /// O(1) id -> address. Valid until the owning chunk is released by
  /// shrink() or destruction.
  char* cellData(uint32_t cellId) const {
    return chunks_[cellId >> cellsPerChunkShift_].data<char>() +
        (static_cast<size_t>(cellId & cellsPerChunkMask_) << cellBytesShift_);
  }

  uint32_t cellBytes() const {
    return cellBytes_;
  }

  /// Upper bound (exclusive) of ids ever handed out; sizes side arrays
  /// indexed by cell id.
  uint32_t cellIdCapacity() const {
    return static_cast<uint32_t>(chunks_.size()) << cellsPerChunkShift_;
  }

  /// Returns a cell to the freelist for reuse. The chunk stays with the
  /// allocator until shrink().
  void recycle(uint32_t cellId);

  /// Logically frees every cell and invalidates all outstanding ids; chunks
  /// are retained for reuse. Used after a full drain when more input is
  /// expected.
  void resetAll();

  /// Returns fully idle chunks (no live cell) to the pool. Freelist entries
  /// pointing into released chunks are dropped. Returns bytes released.
  int64_t shrink();

  /// Live chunks times chunk size: exactly the pool-visible footprint.
  int64_t allocatedBytes() const {
    return static_cast<int64_t>(liveChunks_) * chunkBytes_;
  }

  int64_t usedCells() const {
    return usedCells_;
  }

  int64_t usedBytes() const {
    return usedCells_ * static_cast<int64_t>(cellBytes_);
  }

 private:
  static constexpr uint32_t kNoChunk = 0xFFFFFFFFu;

  uint32_t cellsPerChunk() const {
    return 1u << cellsPerChunkShift_;
  }

  /// Allocates a chunk from the pool into a hole slot or a new slot and makes
  /// it the current bump chunk.
  void growChunk();

  /// Points bumpChunk_ at a retained chunk that still has never-handed slots,
  /// or kNoChunk when none exists.
  void findBumpChunk();

  memory::MemoryPool* const pool_;
  const uint32_t chunkBytes_;
  const uint32_t cellBytes_;
  uint32_t cellBytesShift_;
  uint32_t cellsPerChunkShift_;
  uint32_t cellsPerChunkMask_;

  /// Chunk slots; an empty ContiguousAllocation is a hole left by shrink().
  std::vector<memory::ContiguousAllocation> chunks_;
  /// Next never-handed slot per chunk ("bump watermark").
  std::vector<uint32_t> chunkBump_;
  /// Live (handed, not recycled) cells per chunk.
  std::vector<uint32_t> chunkLiveCells_;
  /// Cells handed back by recycle(), LIFO.
  std::vector<uint32_t> freeList_;

  /// Chunk currently served by bump allocation, or kNoChunk.
  uint32_t bumpChunk_{kNoChunk};

  uint32_t liveChunks_{0};
  int64_t usedCells_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
