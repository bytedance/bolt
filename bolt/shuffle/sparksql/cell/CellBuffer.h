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

#include <folly/CPortability.h>
#include <folly/Likely.h>

#include "bolt/common/memory/MemoryPool.h"
#include "bolt/shuffle/sparksql/cell/CellFormat.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

// Buffers shuffle data by partition and stream, with chunk-backed cell
// allocation, data chains, and null tracking.

/// The Cell shuffle writer's chunk allocator: the only component that talks to
/// the engine MemoryPool for cell data.
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
  /// Stores beforeGrow for later allocations; construction does not call it.
  ChunkAllocator(
      memory::MemoryPool* pool,
      uint32_t chunkBytes,
      uint32_t cellBytes,
      GrowCallback beforeGrow = {});
  ~ChunkAllocator();

  ChunkAllocator(const ChunkAllocator&) = delete;
  ChunkAllocator& operator=(const ChunkAllocator&) = delete;

  /// Returns a cell id. Order of preference: freelist, never-yet-handed slot
  /// of a retained chunk, then a new chunk from the pool (invoking the stored
  /// callback first and re-checking the freelist after it, since it may spill
  /// and recycle).
  uint32_t allocCell();

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

  /// Sorts the freelist so reuse consumes cells chunk by chunk (lowest
  /// chunk first). Called after a full recycle: refilling then packs the
  /// leading chunks and leaves trailing chunks untouched, so shrink()
  /// can hand idle memory back without a spill - the property that makes
  /// declining a reclaim an honest, materially compliant answer.
  void packFreelist();

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
  const GrowCallback beforeGrow_;
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

/// The Cell shuffle writer's (partition, stream) -> cell-chain
/// directory. Cells express data ownership; all real memory lives in the
/// ChunkAllocator below.
///
/// A chain's cells are all full except the last. Chain order is append
/// order, which is the byte order of the stream. Each chain keeps its tail's
/// write position as a pointer: a linked cell's chunk is never freed or
/// moved, so the hot appends reach the cell without the cell id arithmetic.
class DataCells {
 public:
  DataCells(
      memory::MemoryPool* pool,
      ChunkAllocator* allocator,
      uint32_t numPartitions,
      uint32_t numStreams);
  ~DataCells();

  DataCells(const DataCells&) = delete;
  DataCells& operator=(const DataCells&) = delete;

  /// Appends bytes to the (pid, stream) chain, allocating cells as needed.
  ///
  /// Spill-safe by construction: every cell the append may need is allocated
  /// (held unlinked) before a single byte is copied, so a spill fired from
  /// the allocator's callback or the pool's arbitration during chunk growth
  /// sees only complete prior appends. The spill must release chains via
  /// releaseAll()/releasePartition() (recycling), never the allocator's
  /// resetAll(), so held ids stay valid.
  void append(uint32_t pid, uint32_t stream, const void* data, uint32_t bytes) {
    auto& info = infos_[chainIndex(pid, stream)];
    // Fast path: the bytes fit the tail cell - the overwhelmingly common
    // case. Nothing is allocated, so no spill can fire mid-append. An empty
    // chain has tailLeft 0, and bytes == 0 wraps past it: both go slow.
    if (FOLLY_LIKELY(bytes - 1 < info.tailLeft)) {
      ::memcpy(info.tail, data, bytes);
      info.tail += bytes;
      info.tailLeft -= bytes;
      totalBytes_ += bytes;
      return;
    }
    appendSlow(pid, stream, data, bytes);
  }

  /// Contiguous scratch inside the current tail cell, or nullptr when the
  /// tail cannot hold maxBytes. Allocates nothing, so no spill can fire
  /// between a reserve and its commit; the caller writes up to maxBytes and
  /// commits the actual size.
  char* tryReserve(uint32_t pid, uint32_t stream, uint32_t maxBytes) {
    const auto& info = infos_[chainIndex(pid, stream)];
    return maxBytes <= info.tailLeft && info.tail != nullptr ? info.tail
                                                             : nullptr;
  }

  void commit(uint32_t pid, uint32_t stream, uint32_t bytes) {
    auto& info = infos_[chainIndex(pid, stream)];
    info.tail += bytes;
    info.tailLeft -= bytes;
    totalBytes_ += bytes;
  }

  /// Recycles every chain's cells back to the allocator (the Run drain's
  /// release step). Unlike ChunkAllocator::resetAll, ids not owned by any
  /// chain survive.
  void releaseAll();

  /// Total bytes buffered for (pid, stream).
  uint64_t bytes(uint32_t pid, uint32_t stream) const {
    return chainBytes(infos_[chainIndex(pid, stream)]);
  }

  /// Visits the chain in byte order: fn(const char* data, uint32_t bytes).
  template <typename F>
  void scan(uint32_t pid, uint32_t stream, F&& fn) const {
    const auto& info = infos_[chainIndex(pid, stream)];
    if (info.numCells == 0) {
      return;
    }
    uint32_t id = info.firstCell;
    for (uint32_t i = 0; i + 1 < info.numCells; ++i) {
      fn(allocator_->cellData(id), allocator_->cellBytes());
      id = next_[id];
    }
    fn(allocator_->cellData(id), allocator_->cellBytes() - info.tailLeft);
  }

  /// Clears every chain. The caller resets or shrinks the allocator; ids
  /// held by this directory are invalid afterwards.
  void reset();

  /// Recycles the cells of one partition back to the allocator: a partial
  /// Run's release step.
  void releasePartition(uint32_t pid);

  uint64_t totalBytes() const {
    return totalBytes_;
  }

  uint32_t numPartitions() const {
    return numPartitions_;
  }

  uint32_t numStreams() const {
    return numStreams_;
  }

 private:
  /// One chain, in half a cache line: the append path reads tail and
  /// tailLeft only.
  struct alignas(32) ChainInfo {
    /// Write position in the last cell; nullptr while the chain is empty.
    char* tail{nullptr};
    /// Bytes left in the last cell; 0 while the chain is empty.
    uint32_t tailLeft{0};
    uint32_t firstCell{ChunkAllocator::kInvalidCell};
    uint32_t lastCell{ChunkAllocator::kInvalidCell};
    uint32_t numCells{0};
  };
  static_assert(sizeof(ChainInfo) == 32);

  uint64_t chainBytes(const ChainInfo& info) const {
    return static_cast<uint64_t>(info.numCells) * allocator_->cellBytes() -
        info.tailLeft;
  }

  void
  appendSlow(uint32_t pid, uint32_t stream, const void* data, uint32_t bytes);

  /// Stream-major: one column's chains are contiguous, matching the cache
  /// layer's per-column flush pattern.
  size_t chainIndex(uint32_t pid, uint32_t stream) const {
    return static_cast<size_t>(stream) * numPartitions_ + pid;
  }

  /// Links an already-allocated cell to the end of the chain.
  void linkCell(ChainInfo& info, uint32_t id);

  memory::MemoryPool* const pool_;
  ChunkAllocator* const allocator_;
  const uint32_t numPartitions_;
  const uint32_t numStreams_;

  /// numStreams * numPartitions chain infos; pool-backed.
  ChainInfo* infos_{nullptr};
  /// cellId -> next cell in its chain; grows with the allocator's id space.
  uint32_t* next_{nullptr};
  uint32_t nextCapacity_{0};
  uint64_t totalBytes_{0};
};

/// Null bitmaps per (partition, non-UNKNOWN wire column) for the current
/// checkpoint window, in the null body's bit order: bit 1 = non-null, bit 0
/// = null.
///
/// Storage is lazy at both levels. A partition holds nothing until its first
/// null: then it gets a table of one 16-byte slot per column. A slot holds
/// the column's counted all-null prefix and, only once a null follows a
/// non-null row, a bitmap of its own. Untouched (partition, column) pairs
/// summarize as NO_NULL, which realizes the "all rows non-null by default,
/// never touched" design; rows beyond a bitmap's capacity are implicitly
/// non-null.
///
/// A bitmap grows by doubling. Tables and small bitmaps are carved from
/// arena chunks of the pool; a small bitmap's outgrown block goes to a
/// freelist of its size class for the next slot to reach that size. Large
/// bitmaps come from the pool itself and go back to it as they grow, so the
/// blocks held but unused stay under 112 bytes per slot. Everything returns
/// to the pool when the window closes.
class NullCells {
 public:
  NullCells(
      memory::MemoryPool* pool,
      uint32_t numPartitions,
      uint32_t numColumns);
  ~NullCells();

  NullCells(const NullCells&) = delete;
  NullCells& operator=(const NullCells&) = delete;

  /// Marks rowInWindow (0-based since the checkpoint window opened) of
  /// column col in partition pid as null. Rows are visited in strictly
  /// increasing order per partition; capacity grows on demand.
  ///
  /// Forced inline: the split loops call it per null row, and the compiler
  /// otherwise keeps it out of line behind a PLT call; table opening and
  /// growth stay out of line.
  ///
  /// An all-null-so-far column is pure counting: while every row of the
  /// window has been null, only the slot's prefix advances and no bitmap
  /// exists. The first non-null row freezes the prefix (it simply never
  /// calls in), and later nulls fall through to the bitmap.
  FOLLY_ALWAYS_INLINE void
  setNull(uint32_t pid, uint32_t col, uint32_t rowInWindow) {
    Slot& slot = slotFor(pid, col);
    if (rowInWindow == slot.prefix) {
      ++slot.prefix;
      return;
    }
    if (FOLLY_UNLIKELY((rowInWindow >> 3) >= slot.capBytes)) {
      grow(slot, rowInWindow);
    }
    slot.bits[rowInWindow >> 3] &=
        static_cast<uint8_t>(~(1u << (rowInWindow & 7)));
  }

  /// Bulk form for a run of consecutive window rows that are all null in
  /// this column (a constant-null input batch): O(1) while the column is
  /// still all-null in this partition, per-row otherwise.
  inline void
  setNullRun(uint32_t pid, uint32_t col, uint32_t startRow, uint32_t count) {
    Slot& slot = slotFor(pid, col);
    if (startRow == slot.prefix) {
      slot.prefix += count;
      return;
    }
    for (uint32_t i = 0; i < count; ++i) {
      setNull(pid, col, startRow + i);
    }
  }

  struct Summary {
    NullTag tag;
    uint32_t nonNullCount;
  };

  /// Summarizes (pid, col) over the first rowCount rows of the window.
  Summary summarize(uint32_t pid, uint32_t col, uint32_t rowCount) const;

  /// Writes the null body bitmap for (pid, col): ceil(rowCount / 8) bytes,
  /// bit 1 = non-null, unused bits of the last byte zeroed. Only
  /// meaningful when summarize() returned kRawNull, but valid for any tag.
  void emitBitmap(uint32_t pid, uint32_t col, uint32_t rowCount, uint8_t* out)
      const;

  /// Drops all tables and bitmaps and returns to the all-non-null state
  /// (checkpoint window close).
  void reset();

  /// Releases one partition's table and bitmaps to the freelists (RSS
  /// partial flush seam).
  void releasePartition(uint32_t pid);

  /// Bytes of the slots' bitmaps; input to the checkpoint trigger. Not in
  /// it, as both stay bounded whatever the window's size: the slot tables,
  /// 16 bytes per (partition, column) of a partition with a null, and the
  /// outgrown arena blocks on the freelists, under 112 bytes per bitmap.
  int64_t allocatedBytes() const {
    return bitmapBytes_;
  }

  /// Bytes of slot tables held.
  int64_t tableBytes() const {
    return tableBytes_;
  }

 private:
  struct Slot {
    /// Rows [0, prefix) of the window are null with no bitmap backing.
    /// Frozen once a non-null row appears (rows arrive in order, so a later
    /// null misses the prefix match).
    uint32_t prefix;
    /// Bytes of bits; 0 while the column has no bitmap.
    uint32_t capBytes;
    uint8_t* bits;
  };

  /// Power-of-two bitmap sizes from 16 bytes (128 rows); up to 64 bytes
  /// (512 rows) they come from the arena. The bound keeps a slot's outgrown
  /// arena blocks under 112 bytes: slots grow in step, so few are reused.
  static constexpr uint32_t kMinBitmapLog2 = 4;
  static constexpr uint32_t kMaxArenaBitmapLog2 = 6;
  static constexpr uint32_t kMaxArenaBitmapBytes = 1u << kMaxArenaBitmapLog2;
  static constexpr uint32_t kNumSizeClasses =
      kMaxArenaBitmapLog2 - kMinBitmapLog2 + 1;

  FOLLY_ALWAYS_INLINE Slot& slotFor(uint32_t pid, uint32_t col) {
    Slot* table = tables_[pid];
    if (FOLLY_UNLIKELY(table == nullptr)) {
      table = openTable(pid);
    }
    return table[col];
  }

  const Slot* findSlot(uint32_t pid, uint32_t col) const {
    const Slot* table = tables_[pid];
    return table == nullptr ? nullptr : table + col;
  }

  Slot* openTable(uint32_t pid);
  void grow(Slot& slot, uint32_t rowInWindow);
  /// A bitmap block of 2^log2 bytes.
  uint8_t* allocateBitmap(uint32_t log2);
  void freeBitmap(uint8_t* bits, uint32_t capBytes);
  /// `bytes` (a multiple of 8) from the arena.
  uint8_t* carve(size_t bytes);
  void freeChunks();

  memory::MemoryPool* const pool_;
  const uint32_t numPartitions_;
  const uint32_t numColumns_;

  /// Per partition: its slot table, nullptr until its first null.
  std::vector<Slot*> tables_;
  /// Intrusive freelists: a free block's first 8 bytes link the next one.
  uint8_t* freeBitmaps_[kNumSizeClasses] = {};
  uint8_t* freeTables_{nullptr};

  struct Chunk {
    uint8_t* data;
    size_t size;
  };
  std::vector<Chunk> chunks_;
  uint8_t* chunkFree_{nullptr};
  size_t chunkLeft_{0};

  int64_t bitmapBytes_{0};
  int64_t tableBytes_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
