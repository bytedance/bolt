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

#include <folly/Likely.h>

#include "bolt/shuffle/sparksql/cell/CellTypes.h"
#include "bolt/shuffle/sparksql/cell/ChunkAllocator.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// L1 of the Cell shuffle writer: the (partition, stream) -> cell-chain
/// directory. Cells express data ownership; all real memory lives in the
/// ChunkAllocator below.
///
/// A chain's cells are all full except the last (tailUsed bytes). Chain
/// order is append order, which is the byte order of the stream.
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
  /// beforeGrow is forwarded to the allocator's chunk-grow choke point.
  void append(
      uint32_t pid,
      uint32_t stream,
      const void* data,
      uint32_t bytes,
      const ChunkAllocator::GrowCallback& beforeGrow);

  /// Total bytes buffered for (pid, stream).
  uint64_t bytes(uint32_t pid, uint32_t stream) const {
    const auto& info = infos_[chainIndex(pid, stream)];
    return info.numCells == 0
        ? 0
        : (static_cast<uint64_t>(info.numCells - 1) * allocator_->cellBytes()) +
            info.tailUsed;
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
    fn(allocator_->cellData(id), info.tailUsed);
  }

  /// Clears every chain. The caller resets or shrinks the allocator; ids
  /// held by this directory are invalid afterwards.
  void reset();

  /// Recycles the cells of one partition back to the allocator (RSS partial
  /// flush seam; unused on the ESS full-drain path).
  void releasePartition(uint32_t pid);

  uint64_t totalBytes() const {
    return totalBytes_;
  }

 private:
  struct ChainInfo {
    uint32_t firstCell{ChunkAllocator::kInvalidCell};
    uint32_t lastCell{ChunkAllocator::kInvalidCell};
    uint32_t tailUsed{0};
    uint32_t numCells{0};
  };
  static_assert(sizeof(ChainInfo) == 16);

  /// Stream-major: one column's chains are contiguous, matching the cache
  /// layer's per-column flush pattern.
  size_t chainIndex(uint32_t pid, uint32_t stream) const {
    return static_cast<size_t>(stream) * numPartitions_ + pid;
  }

  /// Appends a fresh cell to the chain and returns its id.
  uint32_t appendCell(
      ChainInfo& info,
      const ChunkAllocator::GrowCallback& beforeGrow);

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

/// Null bitmaps per (partition, logical column) for the current checkpoint
/// window, kept in the writer's semantics: bit 1 = non-null, bit 0 = null
/// (spec section 4.2).
///
/// Storage is lazy: a partition allocates nothing until its first null.
/// Untouched (partition, column) pairs cost zero memory and summarize as
/// NO_NULL, which realizes the "all rows non-null by default, never touched"
/// design. Rows beyond a partition's allocated capacity are implicitly
/// non-null.
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
  /// column col in partition pid as null. Rows are visited in non-decreasing
  /// order per partition; capacity grows on demand.
  inline void setNull(uint32_t pid, uint32_t col, uint32_t rowInWindow) {
    uint32_t cap = capBytes_[pid];
    if (FOLLY_UNLIKELY((rowInWindow >> 3) >= cap)) {
      grow(pid, rowInWindow);
      cap = capBytes_[pid];
    }
    base_[pid][static_cast<size_t>(col) * cap + (rowInWindow >> 3)] &=
        static_cast<char>(~(1u << (rowInWindow & 7)));
    hasNull_[static_cast<size_t>(pid) * numColumns_ + col] = 1;
  }

  struct Summary {
    NullTag tag;
    uint32_t nonNullCount;
  };

  /// Summarizes (pid, col) over the first rowCount rows of the window.
  Summary summarize(uint32_t pid, uint32_t col, uint32_t rowCount) const;

  /// Writes the spec-form bitmap for (pid, col): ceil(rowCount / 8) bytes,
  /// bit 1 = non-null, unused bits of the last byte zeroed (spec 4.2). Only
  /// meaningful when summarize() returned kRawNull, but valid for any tag.
  void emitBitmap(uint32_t pid, uint32_t col, uint32_t rowCount, uint8_t* out)
      const;

  /// Drops all bitmaps and returns to the all-non-null state (checkpoint
  /// window close).
  void reset();

  /// Releases one partition's bitmaps (RSS partial flush seam).
  void releasePartition(uint32_t pid);

  /// Bytes currently held; input to the checkpoint trigger.
  int64_t allocatedBytes() const {
    return allocatedBytes_;
  }

 private:
  void grow(uint32_t pid, uint32_t rowInWindow);

  memory::MemoryPool* const pool_;
  const uint32_t numPartitions_;
  const uint32_t numColumns_;

  /// Per partition: bitmap block of numColumns * capBytes_[pid] bytes,
  /// column-major with stride capBytes_[pid]; nullptr until first null.
  std::vector<char*> base_;
  std::vector<uint32_t> capBytes_;
  /// Per (pid, col): 1 once a null was recorded in this window.
  std::vector<uint8_t> hasNull_;
  int64_t allocatedBytes_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
