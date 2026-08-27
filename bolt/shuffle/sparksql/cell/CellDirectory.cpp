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

#include "bolt/shuffle/sparksql/cell/CellDirectory.h"

#include <cstring>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

DataCells::DataCells(
    memory::MemoryPool* pool,
    ChunkAllocator* allocator,
    uint32_t numPartitions,
    uint32_t numStreams)
    : pool_(pool),
      allocator_(allocator),
      numPartitions_(numPartitions),
      numStreams_(numStreams) {
  const size_t numChains =
      static_cast<size_t>(numPartitions_) * numStreams_;
  infos_ = reinterpret_cast<ChainInfo*>(
      pool_->allocate(numChains * sizeof(ChainInfo)));
  std::fill(infos_, infos_ + numChains, ChainInfo{});
}

DataCells::~DataCells() {
  const size_t numChains =
      static_cast<size_t>(numPartitions_) * numStreams_;
  pool_->free(infos_, numChains * sizeof(ChainInfo));
  if (next_ != nullptr) {
    pool_->free(next_, static_cast<size_t>(nextCapacity_) * sizeof(uint32_t));
  }
}

uint32_t DataCells::appendCell(
    ChainInfo& info,
    const ChunkAllocator::GrowCallback& beforeGrow) {
  const uint32_t id = allocator_->allocCell(beforeGrow);
  if (FOLLY_UNLIKELY(id >= nextCapacity_)) {
    const uint32_t newCapacity = allocator_->cellIdCapacity();
    BOLT_CHECK_LT(id, newCapacity);
    auto* grown = reinterpret_cast<uint32_t*>(pool_->allocate(
        static_cast<size_t>(newCapacity) * sizeof(uint32_t)));
    if (next_ != nullptr) {
      ::memcpy(
          grown, next_, static_cast<size_t>(nextCapacity_) * sizeof(uint32_t));
      pool_->free(
          next_, static_cast<size_t>(nextCapacity_) * sizeof(uint32_t));
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
  info.tailUsed = 0;
  ++info.numCells;
  return id;
}

void DataCells::append(
    uint32_t pid,
    uint32_t stream,
    const void* data,
    uint32_t bytes,
    const ChunkAllocator::GrowCallback& beforeGrow) {
  auto& info = infos_[chainIndex(pid, stream)];
  const uint32_t cellBytes = allocator_->cellBytes();
  const char* src = reinterpret_cast<const char*>(data);
  totalBytes_ += bytes;
  while (bytes > 0) {
    if (info.numCells == 0 || info.tailUsed == cellBytes) {
      appendCell(info, beforeGrow);
    }
    const uint32_t space = cellBytes - info.tailUsed;
    const uint32_t copy = bytes < space ? bytes : space;
    ::memcpy(allocator_->cellData(info.lastCell) + info.tailUsed, src, copy);
    info.tailUsed += copy;
    src += copy;
    bytes -= copy;
  }
}

void DataCells::reset() {
  const size_t numChains =
      static_cast<size_t>(numPartitions_) * numStreams_;
  std::fill(infos_, infos_ + numChains, ChainInfo{});
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
    totalBytes_ -= info.numCells == 0
        ? 0
        : (static_cast<uint64_t>(info.numCells - 1) * allocator_->cellBytes()) +
            info.tailUsed;
    info = ChainInfo{};
  }
}

NullCells::NullCells(
    memory::MemoryPool* pool,
    uint32_t numPartitions,
    uint32_t numColumns)
    : pool_(pool),
      numPartitions_(numPartitions),
      numColumns_(numColumns),
      base_(numPartitions, nullptr),
      capBytes_(numPartitions, 0),
      hasNull_(static_cast<size_t>(numPartitions) * numColumns, 0) {}

NullCells::~NullCells() {
  reset();
}

void NullCells::grow(uint32_t pid, uint32_t rowInWindow) {
  constexpr uint32_t kMinCapBytes = 16; // 128 rows, the design's NullCell
  const uint32_t needBytes = (rowInWindow >> 3) + 1;
  const uint32_t newCap = std::max(
      kMinCapBytes,
      static_cast<uint32_t>(bits::nextPowerOfTwo(needBytes)));
  const uint32_t oldCap = capBytes_[pid];
  char* grown = reinterpret_cast<char*>(
      pool_->allocate(static_cast<size_t>(numColumns_) * newCap));
  for (uint32_t col = 0; col < numColumns_; ++col) {
    char* dst = grown + static_cast<size_t>(col) * newCap;
    if (oldCap > 0) {
      ::memcpy(dst, base_[pid] + static_cast<size_t>(col) * oldCap, oldCap);
    }
    ::memset(dst + oldCap, 0xFF, newCap - oldCap);
  }
  if (base_[pid] != nullptr) {
    pool_->free(base_[pid], static_cast<size_t>(numColumns_) * oldCap);
    allocatedBytes_ -= static_cast<int64_t>(numColumns_) * oldCap;
  }
  base_[pid] = grown;
  capBytes_[pid] = newCap;
  allocatedBytes_ += static_cast<int64_t>(numColumns_) * newCap;
}

NullCells::Summary NullCells::summarize(
    uint32_t pid,
    uint32_t col,
    uint32_t rowCount) const {
  if (rowCount == 0 ||
      hasNull_[static_cast<size_t>(pid) * numColumns_ + col] == 0) {
    return {NullTag::kNoNull, rowCount};
  }
  const uint32_t cap = capBytes_[pid];
  const char* bits = base_[pid] + static_cast<size_t>(col) * cap;
  // Count non-null (set) bits over the first rowCount rows; rows past the
  // allocated capacity are implicitly non-null.
  const uint32_t coveredRows = std::min<uint64_t>(rowCount, uint64_t(cap) * 8);
  uint32_t nonNull = rowCount - coveredRows;
  uint32_t i = 0;
  for (; i + 8 <= (coveredRows >> 3); i += 8) {
    uint64_t word;
    ::memcpy(&word, bits + i, 8);
    nonNull += static_cast<uint32_t>(__builtin_popcountll(word));
  }
  for (; i < (coveredRows >> 3); ++i) {
    nonNull += static_cast<uint32_t>(
        __builtin_popcount(static_cast<uint8_t>(bits[i])));
  }
  if ((coveredRows & 7) != 0) {
    const uint8_t mask = static_cast<uint8_t>((1u << (coveredRows & 7)) - 1);
    nonNull += static_cast<uint32_t>(
        __builtin_popcount(static_cast<uint8_t>(bits[i]) & mask));
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
  const uint32_t cap = capBytes_[pid];
  const uint32_t covered = std::min(outBytes, cap);
  if (covered > 0) {
    ::memcpy(out, base_[pid] + static_cast<size_t>(col) * cap, covered);
  }
  if (covered < outBytes) {
    ::memset(out + covered, 0xFF, outBytes - covered);
  }
  if ((rowCount & 7) != 0) {
    // Unused bits of the last byte must be zero (spec section 4.2).
    out[outBytes - 1] &= static_cast<uint8_t>((1u << (rowCount & 7)) - 1);
  }
}

void NullCells::reset() {
  for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
    if (base_[pid] != nullptr) {
      pool_->free(
          base_[pid], static_cast<size_t>(numColumns_) * capBytes_[pid]);
      base_[pid] = nullptr;
      capBytes_[pid] = 0;
    }
  }
  std::fill(hasNull_.begin(), hasNull_.end(), 0);
  allocatedBytes_ = 0;
}

void NullCells::releasePartition(uint32_t pid) {
  if (base_[pid] != nullptr) {
    pool_->free(base_[pid], static_cast<size_t>(numColumns_) * capBytes_[pid]);
    allocatedBytes_ -= static_cast<int64_t>(numColumns_) * capBytes_[pid];
    base_[pid] = nullptr;
    capBytes_[pid] = 0;
  }
  std::fill_n(hasNull_.begin() + static_cast<size_t>(pid) * numColumns_,
              numColumns_, 0);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
