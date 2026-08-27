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

#include "bolt/shuffle/sparksql/cell/ChunkAllocator.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Allocation.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

ChunkAllocator::ChunkAllocator(
    memory::MemoryPool* pool,
    uint32_t chunkBytes,
    uint32_t cellBytes)
    : pool_(pool), chunkBytes_(chunkBytes), cellBytes_(cellBytes) {
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

uint32_t ChunkAllocator::allocCell(const GrowCallback& beforeGrow) {
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
    if (beforeGrow) {
      beforeGrow();
      // beforeGrow may have spilled and recycled cells.
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

} // namespace bytedance::bolt::shuffle::sparksql::cell
