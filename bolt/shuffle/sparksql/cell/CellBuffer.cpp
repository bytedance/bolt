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

#include "bolt/shuffle/sparksql/cell/CellBuffer.h"

#include <algorithm>
#include <cstring>
#include <functional>
#include <utility>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Allocation.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

ChunkAllocator::ChunkAllocator(
    memory::MemoryPool* pool,
    uint32_t chunkBytes,
    uint32_t cellBytes,
    GrowCallback beforeGrow)
    : pool_(pool),
      beforeGrow_(std::move(beforeGrow)),
      chunkBytes_(chunkBytes),
      cellBytes_(cellBytes) {
  BOLT_CHECK_NOT_NULL(pool_);
  BOLT_CHECK(
      chunkBytes_ > 0 && bits::isPowerOfTwo(chunkBytes_),
      "chunkBytes must be a power of two: {}",
      chunkBytes_);
  BOLT_CHECK(
      cellBytes_ > 0 && bits::isPowerOfTwo(cellBytes_) &&
          cellBytes_ <= chunkBytes_,
      "cellBytes must be a power of two <= chunkBytes: {} vs {}",
      cellBytes_,
      chunkBytes_);
  BOLT_CHECK_EQ(
      chunkBytes_ % memory::AllocationTraits::kPageSize,
      0,
      "chunkBytes must be page-aligned");
  cellBytesShift_ = static_cast<uint32_t>(__builtin_ctz(cellBytes_));
  cellsPerChunkShift_ =
      static_cast<uint32_t>(__builtin_ctz(chunkBytes_ / cellBytes_));
  cellsPerChunkMask_ = (1u << cellsPerChunkShift_) - 1;
}

ChunkAllocator::~ChunkAllocator() {
  for (auto& chunk : chunks_) {
    if (!chunk.empty()) {
      pool_->freeContiguous(chunk);
    }
  }
}

uint32_t ChunkAllocator::allocCell() {
  if (FOLLY_LIKELY(!freeList_.empty())) {
    const uint32_t id = freeList_.back();
    freeList_.pop_back();
    ++chunkLiveCells_[id >> cellsPerChunkShift_];
    ++usedCells_;
    return id;
  }
  if (bumpChunk_ == kNoChunk) {
    findBumpChunk();
  }
  if (bumpChunk_ == kNoChunk) {
    if (beforeGrow_) {
      beforeGrow_();
      // The callback may have spilled and recycled cells.
      if (!freeList_.empty()) {
        const uint32_t id = freeList_.back();
        freeList_.pop_back();
        ++chunkLiveCells_[id >> cellsPerChunkShift_];
        ++usedCells_;
        return id;
      }
      findBumpChunk();
    }
    if (bumpChunk_ == kNoChunk) {
      growChunk();
    }
  }
  const uint32_t slot = chunkBump_[bumpChunk_]++;
  const uint32_t id = (bumpChunk_ << cellsPerChunkShift_) | slot;
  ++chunkLiveCells_[bumpChunk_];
  ++usedCells_;
  if (chunkBump_[bumpChunk_] == cellsPerChunk()) {
    bumpChunk_ = kNoChunk;
  }
  return id;
}

void ChunkAllocator::packFreelist() {
  // Descending ids: allocation pops from the back, so the lowest chunks
  // are consumed first and refills stay dense.
  std::sort(freeList_.begin(), freeList_.end(), std::greater<uint32_t>());
}

void ChunkAllocator::recycle(uint32_t cellId) {
  BOLT_DCHECK_LT(cellId >> cellsPerChunkShift_, chunks_.size());
  BOLT_DCHECK_GT(chunkLiveCells_[cellId >> cellsPerChunkShift_], 0);
  freeList_.push_back(cellId);
  --chunkLiveCells_[cellId >> cellsPerChunkShift_];
  --usedCells_;
}

void ChunkAllocator::resetAll() {
  freeList_.clear();
  std::fill(chunkBump_.begin(), chunkBump_.end(), 0);
  std::fill(chunkLiveCells_.begin(), chunkLiveCells_.end(), 0);
  usedCells_ = 0;
  bumpChunk_ = kNoChunk;
  findBumpChunk();
}

int64_t ChunkAllocator::shrink() {
  int64_t released = 0;
  bool anyReleased = false;
  for (uint32_t i = 0; i < chunks_.size(); ++i) {
    if (chunks_[i].empty() || chunkLiveCells_[i] != 0) {
      continue;
    }
    pool_->freeContiguous(chunks_[i]);
    chunkBump_[i] = 0;
    --liveChunks_;
    released += chunkBytes_;
    anyReleased = true;
    if (bumpChunk_ == i) {
      bumpChunk_ = kNoChunk;
    }
  }
  if (anyReleased && !freeList_.empty()) {
    // Drop freelist entries that lived in released chunks.
    size_t kept = 0;
    for (const uint32_t id : freeList_) {
      if (!chunks_[id >> cellsPerChunkShift_].empty()) {
        freeList_[kept++] = id;
      }
    }
    freeList_.resize(kept);
  }
  return released;
}

void ChunkAllocator::growChunk() {
  uint32_t slot = kNoChunk;
  for (uint32_t i = 0; i < chunks_.size(); ++i) {
    if (chunks_[i].empty()) {
      slot = i;
      break;
    }
  }
  if (slot == kNoChunk) {
    chunks_.emplace_back();
    chunkBump_.push_back(0);
    chunkLiveCells_.push_back(0);
    slot = static_cast<uint32_t>(chunks_.size() - 1);
  }
  pool_->allocateContiguous(
      memory::AllocationTraits::numPages(chunkBytes_), chunks_[slot]);
  chunkBump_[slot] = 0;
  chunkLiveCells_[slot] = 0;
  ++liveChunks_;
  bumpChunk_ = slot;
}

void ChunkAllocator::findBumpChunk() {
  for (uint32_t i = 0; i < chunks_.size(); ++i) {
    if (!chunks_[i].empty() && chunkBump_[i] < cellsPerChunk()) {
      bumpChunk_ = i;
      return;
    }
  }
  bumpChunk_ = kNoChunk;
}

DataCells::DataCells(
    memory::MemoryPool* pool,
    ChunkAllocator* allocator,
    uint32_t numPartitions,
    uint32_t numStreams)
    : pool_(pool),
      allocator_(allocator),
      numPartitions_(numPartitions),
      numStreams_(numStreams) {
  const size_t numChains = static_cast<size_t>(numPartitions_) * numStreams_;
  if (numChains == 0) {
    return;
  }
  infos_ = reinterpret_cast<ChainInfo*>(
      pool_->allocate(numChains * sizeof(ChainInfo)));
  // The pool aligns to 64 bytes: every ChainInfo stays in one cache line.
  BOLT_DCHECK_EQ(reinterpret_cast<uintptr_t>(infos_) % alignof(ChainInfo), 0);
  if (numChains != 0) {
    std::fill(infos_, infos_ + numChains, ChainInfo{});
  }
}

DataCells::~DataCells() {
  const size_t numChains = static_cast<size_t>(numPartitions_) * numStreams_;
  if (infos_ != nullptr) {
    pool_->free(infos_, numChains * sizeof(ChainInfo));
  }
  if (next_ != nullptr) {
    pool_->free(next_, static_cast<size_t>(nextCapacity_) * sizeof(uint32_t));
  }
}

void DataCells::linkCell(ChainInfo& info, uint32_t id) {
  if (FOLLY_UNLIKELY(id >= nextCapacity_)) {
    const uint32_t newCapacity = allocator_->cellIdCapacity();
    BOLT_CHECK_LT(id, newCapacity);
    auto* grown = reinterpret_cast<uint32_t*>(
        pool_->allocate(static_cast<size_t>(newCapacity) * sizeof(uint32_t)));
    if (next_ != nullptr) {
      ::memcpy(
          grown, next_, static_cast<size_t>(nextCapacity_) * sizeof(uint32_t));
      pool_->free(next_, static_cast<size_t>(nextCapacity_) * sizeof(uint32_t));
    }
    next_ = grown;
    nextCapacity_ = newCapacity;
  }
  next_[id] = ChunkAllocator::kInvalidCell;
  if (info.numCells == 0) {
    info.firstCell = id;
  } else {
    next_[info.lastCell] = id;
  }
  info.lastCell = id;
  info.tail = allocator_->cellData(id);
  info.tailLeft = allocator_->cellBytes();
  ++info.numCells;
}

void DataCells::appendSlow(
    uint32_t pid,
    uint32_t stream,
    const void* data,
    uint32_t bytes) {
  if (bytes == 0) {
    return;
  }
  const uint32_t cellBytes = allocator_->cellBytes();
  // Phase 1: allocate every cell this append could need, before touching the
  // chain or copying a byte. The allocator's callback or pool arbitration
  // during chunk growth may spill and release all chains here; the held ids
  // are unlinked and survive. One spare covers the tail the spill may have
  // taken away.
  const uint32_t maxNeeded = bytes / cellBytes + 2;
  uint32_t held[2];
  std::vector<uint32_t> heldOverflow;
  uint32_t heldCount = 0;
  const auto holdCell = [&](uint32_t id) {
    if (heldCount < 2) {
      held[heldCount] = id;
    } else {
      heldOverflow.push_back(id);
    }
    ++heldCount;
  };
  for (uint32_t i = 0; i < maxNeeded; ++i) {
    holdCell(allocator_->allocCell());
  }
  const auto heldAt = [&](uint32_t i) {
    return i < 2 ? held[i] : heldOverflow[i - 2];
  };

  // Phase 2: re-read the chain (a spill may have emptied it), link held
  // cells as needed, copy, and recycle the surplus.
  auto& info = infos_[chainIndex(pid, stream)];
  const char* src = reinterpret_cast<const char*>(data);
  totalBytes_ += bytes;
  uint32_t nextHeld = 0;
  while (bytes > 0) {
    if (info.tailLeft == 0) {
      const uint32_t id = heldAt(nextHeld++);
      linkCell(info, id);
    }
    const uint32_t copy = bytes < info.tailLeft ? bytes : info.tailLeft;
    ::memcpy(info.tail, src, copy);
    info.tail += copy;
    info.tailLeft -= copy;
    src += copy;
    bytes -= copy;
  }
  for (uint32_t i = nextHeld; i < heldCount; ++i) {
    allocator_->recycle(heldAt(i));
  }
}

void DataCells::releaseAll() {
  for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
    releasePartition(pid);
  }
}

void DataCells::reset() {
  const size_t numChains = static_cast<size_t>(numPartitions_) * numStreams_;
  if (numChains != 0) {
    std::fill(infos_, infos_ + numChains, ChainInfo{});
  }
  totalBytes_ = 0;
}

void DataCells::releasePartition(uint32_t pid) {
  for (uint32_t stream = 0; stream < numStreams_; ++stream) {
    auto& info = infos_[chainIndex(pid, stream)];
    uint32_t id = info.firstCell;
    while (id != ChunkAllocator::kInvalidCell) {
      const uint32_t following = next_[id];
      allocator_->recycle(id);
      id = following;
    }
    totalBytes_ -= chainBytes(info);
    info = ChainInfo{};
  }
}

NullCells::NullCells(
    memory::MemoryPool* pool,
    uint32_t numPartitions,
    uint32_t numColumns)
    : pool_(pool),
      numPartitions_(numColumns == 0 ? 0 : numPartitions),
      numColumns_(numColumns),
      tables_(numPartitions_, nullptr) {}

NullCells::~NullCells() {
  reset();
}

uint8_t* NullCells::carve(size_t bytes) {
  // Chunks double from 64KB to 4MB, so a writer with a few nulls stays
  // small; a bitmap above the cap gets a chunk of its own size.
  constexpr size_t kMinChunkBytes = 64 << 10;
  constexpr size_t kMaxChunkBytes = 4 << 20;
  if (chunkLeft_ < bytes) {
    const size_t next = chunks_.empty()
        ? kMinChunkBytes
        : std::min(kMaxChunkBytes, chunks_.back().size * 2);
    const size_t size = std::max(next, bytes);
    auto* data = reinterpret_cast<uint8_t*>(pool_->allocate(size));
    chunks_.push_back({data, size});
    chunkFree_ = data;
    chunkLeft_ = size;
  }
  uint8_t* out = chunkFree_;
  chunkFree_ += bytes;
  chunkLeft_ -= bytes;
  return out;
}

NullCells::Slot* NullCells::openTable(uint32_t pid) {
  const size_t bytes = sizeof(Slot) * numColumns_;
  uint8_t* table = freeTables_;
  if (table != nullptr) {
    ::memcpy(&freeTables_, table, sizeof(uint8_t*));
  } else {
    table = carve(bytes);
    tableBytes_ += static_cast<int64_t>(bytes);
  }
  ::memset(table, 0, bytes);
  tables_[pid] = reinterpret_cast<Slot*>(table);
  return tables_[pid];
}

uint8_t* NullCells::allocateBitmap(uint32_t log2) {
  const size_t bytes = size_t{1} << log2;
  bitmapBytes_ += static_cast<int64_t>(bytes);
  if (log2 > kMaxArenaBitmapLog2) {
    return reinterpret_cast<uint8_t*>(pool_->allocate(bytes));
  }
  uint8_t*& head = freeBitmaps_[log2 - kMinBitmapLog2];
  if (head != nullptr) {
    uint8_t* block = head;
    ::memcpy(&head, block, sizeof(uint8_t*));
    return block;
  }
  return carve(bytes);
}

void NullCells::freeBitmap(uint8_t* bits, uint32_t capBytes) {
  bitmapBytes_ -= capBytes;
  if (capBytes > kMaxArenaBitmapBytes) {
    pool_->free(bits, capBytes);
    return;
  }
  // Arena blocks stay held until the window resets.
  uint8_t*& head = freeBitmaps_[31 - __builtin_clz(capBytes) - kMinBitmapLog2];
  ::memcpy(bits, &head, sizeof(uint8_t*));
  head = bits;
}

void NullCells::grow(Slot& slot, uint32_t rowInWindow) {
  const uint32_t needBytes = (rowInWindow >> 3) + 1;
  // ceil(log2(needBytes)), at least the 16-byte minimum (128 rows).
  const uint32_t log2 = std::max<uint32_t>(
      kMinBitmapLog2, needBytes <= 1 ? 0 : 32 - __builtin_clz(needBytes - 1));
  const uint32_t newCap = 1u << log2;
  uint8_t* grown = allocateBitmap(log2);
  const uint32_t oldCap = slot.capBytes;
  if (oldCap > 0) {
    ::memcpy(grown, slot.bits, oldCap);
    freeBitmap(slot.bits, oldCap);
  }
  ::memset(grown + oldCap, 0xFF, newCap - oldCap);
  slot.bits = grown;
  slot.capBytes = newCap;
}

NullCells::Summary
NullCells::summarize(uint32_t pid, uint32_t col, uint32_t rowCount) const {
  const Slot* slot = findSlot(pid, col);
  if (rowCount == 0 || slot == nullptr ||
      (slot->prefix == 0 && slot->capBytes == 0)) {
    return {NullTag::kNoNull, rowCount};
  }
  const uint32_t prefix = std::min(slot->prefix, rowCount);
  if (prefix == rowCount) {
    return {NullTag::kAllNull, 0}; // pure counting, no storage was touched
  }
  uint32_t nonNull = rowCount - prefix;
  if (slot->capBytes != 0) {
    const uint8_t* bits = slot->bits;
    // Count set (non-null) bits in [prefix, coveredRows); rows past the
    // allocated capacity are implicitly non-null. Bits below the prefix are
    // untouched defaults, so counting from the prefix is exact.
    const uint32_t coveredRows =
        std::min<uint64_t>(rowCount, uint64_t(slot->capBytes) * 8);
    if (prefix < coveredRows) {
      uint32_t setBits = 0;
      uint32_t byte = prefix >> 3;
      const uint32_t lastByte = (coveredRows - 1) >> 3;
      uint8_t first =
          bits[byte] & static_cast<uint8_t>(~((1u << (prefix & 7)) - 1));
      if (byte == lastByte) {
        if ((coveredRows & 7) != 0) {
          first &= static_cast<uint8_t>((1u << (coveredRows & 7)) - 1);
        }
        setBits = __builtin_popcount(first);
      } else {
        setBits = __builtin_popcount(first);
        ++byte;
        for (; byte + 8 <= lastByte; byte += 8) {
          uint64_t word;
          ::memcpy(&word, bits + byte, 8);
          setBits += static_cast<uint32_t>(__builtin_popcountll(word));
        }
        for (; byte < lastByte; ++byte) {
          setBits += __builtin_popcount(bits[byte]);
        }
        uint8_t last = bits[lastByte];
        if ((coveredRows & 7) != 0) {
          last &= static_cast<uint8_t>((1u << (coveredRows & 7)) - 1);
        }
        setBits += __builtin_popcount(last);
      }
      nonNull = (rowCount - coveredRows) + setBits;
    }
  }
  if (nonNull == 0) {
    return {NullTag::kAllNull, 0};
  }
  if (nonNull == rowCount) {
    return {NullTag::kNoNull, rowCount};
  }
  return {NullTag::kRawNull, nonNull};
}

void NullCells::emitBitmap(
    uint32_t pid,
    uint32_t col,
    uint32_t rowCount,
    uint8_t* out) const {
  const uint32_t outBytes = (rowCount + 7) >> 3;
  const Slot* slot = findSlot(pid, col);
  uint32_t covered = 0;
  if (slot != nullptr && slot->capBytes != 0) {
    covered = std::min(outBytes, slot->capBytes);
    ::memcpy(out, slot->bits, covered);
  }
  if (covered < outBytes) {
    ::memset(out + covered, 0xFF, outBytes - covered);
  }
  // The counted null prefix is not in the storage: overlay it as zeros.
  const uint32_t prefix =
      slot == nullptr ? 0 : std::min(slot->prefix, rowCount);
  ::memset(out, 0, prefix >> 3);
  if ((prefix & 7) != 0) {
    out[prefix >> 3] &= static_cast<uint8_t>(~((1u << (prefix & 7)) - 1));
  }
  if ((rowCount & 7) != 0) {
    // Unused bits of the last byte must be zero.
    out[outBytes - 1] &= static_cast<uint8_t>((1u << (rowCount & 7)) - 1);
  }
}

void NullCells::freeChunks() {
  for (const auto& chunk : chunks_) {
    pool_->free(chunk.data, chunk.size);
  }
  chunks_.clear();
  chunkFree_ = nullptr;
  chunkLeft_ = 0;
}

void NullCells::reset() {
  for (auto& table : tables_) {
    if (table == nullptr) {
      continue;
    }
    for (uint32_t col = 0; col < numColumns_; ++col) {
      if (table[col].capBytes > kMaxArenaBitmapBytes) {
        pool_->free(table[col].bits, table[col].capBytes);
      }
    }
    table = nullptr;
  }
  std::fill(std::begin(freeBitmaps_), std::end(freeBitmaps_), nullptr);
  freeTables_ = nullptr;
  freeChunks();
  bitmapBytes_ = 0;
  tableBytes_ = 0;
}

void NullCells::releasePartition(uint32_t pid) {
  if (numColumns_ == 0 || tables_[pid] == nullptr) {
    return;
  }
  Slot* table = tables_[pid];
  for (uint32_t col = 0; col < numColumns_; ++col) {
    if (table[col].capBytes != 0) {
      freeBitmap(table[col].bits, table[col].capBytes);
    }
  }
  auto* block = reinterpret_cast<uint8_t*>(table);
  ::memcpy(block, &freeTables_, sizeof(uint8_t*));
  freeTables_ = block;
  tables_[pid] = nullptr;
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
