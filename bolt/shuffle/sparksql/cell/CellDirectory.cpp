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

void DataCells::linkCell(ChainInfo& info, uint32_t id) {
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
}

void DataCells::append(
    uint32_t pid,
    uint32_t stream,
    const void* data,
    uint32_t bytes,
    const ChunkAllocator::GrowCallback& beforeGrow) {
  if (bytes == 0) {
    return;
  }
  const uint32_t cellBytes = allocator_->cellBytes();
  {
    // Fast path: the bytes fit the current tail cell. Nothing is allocated,
    // so no spill can fire mid-append and the chain cannot move under us.
    // With blocks of at most kMaxBlockBytes and cells of >= 256 bytes this
    // is the overwhelmingly common case.
    auto& info = infos_[chainIndex(pid, stream)];
    if (info.numCells != 0 && info.tailUsed + bytes <= cellBytes) {
      ::memcpy(allocator_->cellData(info.lastCell) + info.tailUsed, data, bytes);
      info.tailUsed += bytes;
      totalBytes_ += bytes;
      return;
    }
  }
  // Phase 1: allocate every cell this append could need, before touching the
  // chain or copying a byte. beforeGrow (or pool arbitration inside a chunk
  // grow) may spill and release all chains here; the held ids are unlinked
  // and survive. One spare covers the tail the spill may have taken away.
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
    holdCell(allocator_->allocCell(beforeGrow));
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
    if (info.numCells == 0 || info.tailUsed == cellBytes) {
      const uint32_t id = heldAt(nextHeld++);
      linkCell(info, id);
    }
    const uint32_t space = cellBytes - info.tailUsed;
    const uint32_t copy = bytes < space ? bytes : space;
    ::memcpy(allocator_->cellData(info.lastCell) + info.tailUsed, src, copy);
    info.tailUsed += copy;
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
      hasNull_(static_cast<size_t>(numPartitions) * numColumns, 0),
      nullPrefix_(static_cast<size_t>(numPartitions) * numColumns, 0) {}

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
  const size_t slot = static_cast<size_t>(pid) * numColumns_ + col;
  if (rowCount == 0 || hasNull_[slot] == 0) {
    return {NullTag::kNoNull, rowCount};
  }
  const uint32_t prefix = std::min(nullPrefix_[slot], rowCount);
  if (prefix == rowCount) {
    return {NullTag::kAllNull, 0}; // pure counting, no storage was touched
  }
  uint32_t nonNull = rowCount - prefix;
  const uint32_t cap = capBytes_[pid];
  if (base_[pid] != nullptr) {
    const auto* bits = reinterpret_cast<const uint8_t*>(
        base_[pid] + static_cast<size_t>(col) * cap);
    // Count set (non-null) bits in [prefix, coveredRows); rows past the
    // allocated capacity are implicitly non-null. Bits below the prefix are
    // untouched defaults, so counting from the prefix is exact.
    const uint32_t coveredRows =
        std::min<uint64_t>(rowCount, uint64_t(cap) * 8);
    if (prefix < coveredRows) {
      uint32_t setBits = 0;
      uint32_t byte = prefix >> 3;
      const uint32_t lastByte = (coveredRows - 1) >> 3;
      uint8_t first = bits[byte] &
          static_cast<uint8_t>(~((1u << (prefix & 7)) - 1));
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
  const uint32_t cap = capBytes_[pid];
  const uint32_t covered =
      base_[pid] == nullptr ? 0 : std::min(outBytes, cap);
  if (covered > 0) {
    ::memcpy(out, base_[pid] + static_cast<size_t>(col) * cap, covered);
  }
  if (covered < outBytes) {
    ::memset(out + covered, 0xFF, outBytes - covered);
  }
  // The counted null prefix is not in the storage: overlay it as zeros.
  const size_t slot = static_cast<size_t>(pid) * numColumns_ + col;
  const uint32_t prefix = std::min(nullPrefix_[slot], rowCount);
  ::memset(out, 0, prefix >> 3);
  if ((prefix & 7) != 0) {
    out[prefix >> 3] &= static_cast<uint8_t>(~((1u << (prefix & 7)) - 1));
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
  std::fill(nullPrefix_.begin(), nullPrefix_.end(), 0);
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
  std::fill_n(
      nullPrefix_.begin() + static_cast<size_t>(pid) * numColumns_,
      numColumns_,
      0);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
