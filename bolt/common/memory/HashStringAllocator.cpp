/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/common/base/Portability.h"
#include "bolt/common/base/SimdUtil.h"
namespace bytedance::bolt {

namespace {
/// Returns the size of the previous free block. The size is stored in the
/// last 4 bytes of the free block, e.g. 4 bytes just before the current
/// header.
uint32_t* previousFreeSize(HashStringAllocator::Header* header) {
  return reinterpret_cast<uint32_t*>(header) - 1;
}

/// Returns the header of the previous free block or nullptr if previous block
/// is not free.
HashStringAllocator::Header* FOLLY_NULLABLE
getPreviousFree(HashStringAllocator::Header* FOLLY_NONNULL header) {
  if (!header->isPreviousFree()) {
    return nullptr;
  }
  auto numBytes = *previousFreeSize(header);
  auto previous = reinterpret_cast<HashStringAllocator::Header*>(
      header->begin() - numBytes - 2 * sizeof(HashStringAllocator::Header));
  BOLT_CHECK_EQ(previous->size(), numBytes);
  BOLT_CHECK(previous->isFree());
  BOLT_CHECK(!previous->isPreviousFree());
  return previous;
}

/// Sets kFree flag in the 'header' and writes the size of the block to the
/// last 4 bytes of the block. Sets kPreviousFree flag in the next block's
/// 'header'.
void markAsFree(HashStringAllocator::Header* FOLLY_NONNULL header) {
  header->setFree();
  auto nextHeader = header->next();
  if (nextHeader) {
    nextHeader->setPreviousFree();
    *previousFreeSize(nextHeader) = header->size();
  }
}
} // namespace

std::string HashStringAllocator::Header::toString() {
  std::ostringstream out;
  if (isFree()) {
    out << "|free| ";
  }
  if (isContinued()) {
    out << "|multipart| ";
  }
  out << "size: " << size();
  if (isContinued()) {
    auto next = nextContinued();
    out << " [" << next->size();
    while (next->isContinued()) {
      next = next->nextContinued();
      out << ", " << next->size();
    }
    out << "]";
  }
  if (isPreviousFree()) {
    out << ", previous is free (" << *previousFreeSize(this) << " bytes)";
  }
  if (next() == nullptr) {
    out << ", at end";
  }
  return out.str();
}

HashStringAllocator::~HashStringAllocator() {
  clear();
}

void HashStringAllocator::clear() {
  numFree_ = 0;
  freeBytes_ = 0;
  cumulativeBytes_ = 0;
  std::fill(std::begin(freeNonEmpty_), std::end(freeNonEmpty_), 0);
  for (auto& pair : allocationsFromPool_) {
    pool()->free(pair.first, pair.second);
  }
  allocationsFromPool_.clear();
  sizeFromPool_ = 0;
  for (auto i = 0; i < kNumFreeLists; ++i) {
    new (&free_[i]) CompactDoubleList();
  }
  pool_.clear();
}

void* HashStringAllocator::allocateFromPool(size_t size) {
  auto ptr = pool()->allocate(size);
  cumulativeBytes_ += size;
  allocationsFromPool_[ptr] = size;
  sizeFromPool_ += size;
  return ptr;
}

void HashStringAllocator::freeToPool(void* ptr, size_t size) {
  auto it = allocationsFromPool_.find(ptr);
  BOLT_CHECK(
      it != allocationsFromPool_.end(),
      "freeToPool for block not allocated from pool of HashStringAllocator");
  BOLT_CHECK_EQ(
      size, it->second, "Bad size in HashStringAllocator::freeToPool()");
  allocationsFromPool_.erase(it);
  sizeFromPool_ -= size;
  cumulativeBytes_ -= size;
  pool()->free(ptr, size);
}

// static
std::unique_ptr<ByteInputStream> HashStringAllocator::prepareRead(
    const Header* begin,
    size_t maxBytes) {
  std::vector<ByteRange> ranges;
  auto header = const_cast<Header*>(begin);

  size_t totalBytes = 0;

  for (;;) {
    ranges.push_back(ByteRange{
        reinterpret_cast<uint8_t*>(header->begin()), header->usableSize(), 0});
    totalBytes += ranges.back().size;
    if (!header->isContinued()) {
      break;
    }

    if (totalBytes >= maxBytes) {
      break;
    }

    header = header->nextContinued();
  }
  return std::make_unique<ByteInputStream>(std::move(ranges));
}

HashStringAllocator::Position HashStringAllocator::newWrite(
    ByteOutputStream& stream,
    int32_t preferredSize) {
  BOLT_CHECK(
      !currentHeader_,
      "Do not call newWrite before finishing the previous write to "
      "HashStringAllocator");
  currentHeader_ = allocate(preferredSize, false);

  stream.setRange(
      ByteRange{
          reinterpret_cast<uint8_t*>(currentHeader_->begin()),
          currentHeader_->size(),
          0},
      0);

  startPosition_ = Position::atOffset(currentHeader_, 0);

  return startPosition_;
}

void HashStringAllocator::extendWrite(
    Position position,
    ByteOutputStream& stream) {
  auto header = position.header;
  const auto offset = position.offset();
  BOLT_CHECK_GE(offset, 0, "Starting extendWrite outside of the current range");
  BOLT_CHECK_LE(
      offset,
      header->usableSize(),
      "Starting extendWrite outside of the current range");

  if (header->isContinued()) {
    free(header->nextContinued());
    header->clearContinued();
  }

  stream.setRange(
      ByteRange{
          reinterpret_cast<uint8_t*>(position.header->begin()),
          position.header->size(),
          static_cast<int32_t>(position.position - position.header->begin())},
      0);
  currentHeader_ = header;
  startPosition_ = position;
}

std::pair<HashStringAllocator::Position, HashStringAllocator::Position>
HashStringAllocator::finishWrite(
    ByteOutputStream& stream,
    int32_t numReserveBytes) {
  BOLT_CHECK(
      currentHeader_, "Must call newWrite or extendWrite before finishWrite");
  auto writePosition = stream.writePosition();
  const auto offset = writePosition - currentHeader_->begin();

  BOLT_CHECK_GE(
      offset, 0, "finishWrite called with writePosition out of range");
  BOLT_CHECK_LE(
      offset,
      currentHeader_->usableSize(),
      "finishWrite called with writePosition out of range");

  Position currentPosition = Position::atOffset(currentHeader_, offset);
  if (currentHeader_->isContinued()) {
    free(currentHeader_->nextContinued());
    currentHeader_->clearContinued();
  }
  // Free remainder of block if there is a lot left over.
  freeRestOfBlock(
      currentHeader_,
      writePosition - currentHeader_->begin() + numReserveBytes);
  currentHeader_ = nullptr;

  // The starting position may have shifted if it was at the end of the block
  // and the block was extended. Calculate the new position.
  if (startPosition_.header->isContinued()) {
    auto header = startPosition_.header;
    const auto offset = startPosition_.offset();
    const auto extra = offset - header->usableSize();
    if (extra > 0) {
      auto newHeader = header->nextContinued();
      auto newPosition = newHeader->begin() + extra;
      startPosition_ = {newHeader, newPosition};
    }
  }
  return {startPosition_, currentPosition};
}

void HashStringAllocator::newSlab() {
  constexpr int32_t kSimdPadding = simd::kPadding - sizeof(Header);
  const int64_t needed = pool_.allocatedBytes() >= pool_.hugePageThreshold()
      ? memory::AllocationTraits::kHugePageSize
      : kUnitSize;
  auto run = pool_.allocateFixed(needed);
  // We check we got exactly the requested amount. checkConsistency()
  // depends on slabs made here coinciding with ranges from
  // AllocationPool::rangeAt(). Sometimes the last range can be
  // several huge pages for several huge page sized arenas but
  // checkConsistency() can interpret that.
  BOLT_CHECK_EQ(0, pool_.freeBytes());
  auto available = needed - sizeof(Header) - kSimdPadding;

  BOLT_CHECK_NOT_NULL(run);
  BOLT_CHECK_GT(available, 0);
  // Write end  marker.
  *reinterpret_cast<uint32_t*>(run + available) = Header::kArenaEnd;
  cumulativeBytes_ += available - sizeof(Header);

  // Add the new memory to the free list: Placement construct a header
  // that covers the space from start to the end marker and add this
  // to free list.
  free(new (run) Header(available - sizeof(Header)));
}

void HashStringAllocator::newRange(
    int32_t bytes,
    ByteRange* lastRange,
    ByteRange* range,
    bool contiguous) {
  // Allocates at least kMinContiguous or to the end of the current
  // run. At the end of the write the unused space will be made
  // free.
  BOLT_CHECK(
      currentHeader_,
      "Must have called newWrite or extendWrite before newRange");
  auto newHeader = allocate(bytes, contiguous);

  auto lastWordPtr = reinterpret_cast<void**>(
      currentHeader_->end() - Header::kContinuedPtrSize);
  *reinterpret_cast<void**>(newHeader->begin()) = *lastWordPtr;
  *lastWordPtr = newHeader;
  currentHeader_->setContinued();
  currentHeader_ = newHeader;
  if (lastRange) {
    // The last bytes of the last range are no longer payload. So do not
    // count them in size and do not overwrite them if overwriting the
    // multirange entry. Set position at the new end.
    lastRange->size -= Header::kContinuedPtrSize;
    lastRange->position = std::min(lastRange->size, lastRange->position);
  }
  *range = ByteRange{
      reinterpret_cast<uint8_t*>(currentHeader_->begin()),
      currentHeader_->size(),
      Header::kContinuedPtrSize};
}

void HashStringAllocator::newRange(
    int32_t bytes,
    ByteRange* lastRange,
    ByteRange* range) {
  newRange(bytes, lastRange, range, false);
}

void HashStringAllocator::newContiguousRange(int32_t bytes, ByteRange* range) {
  newRange(bytes, nullptr, range, true);
}

// static
StringView HashStringAllocator::contiguousString(
    StringView view,
    std::string& storage) {
  if (view.isInline()) {
    return view;
  }
  auto header = headerOf(view.data());
  if (view.size() <= header->size()) {
    return view;
  }

  auto stream = prepareRead(headerOf(view.data()));
  storage.resize(view.size());
  stream->readBytes(storage.data(), view.size());
  return StringView(storage);
}

void HashStringAllocator::freeRestOfBlock(Header* header, int32_t keepBytes) {
  keepBytes = std::max(keepBytes, kMinAlloc);
  int32_t freeSize = header->size() - keepBytes - sizeof(Header);
  if (freeSize <= kMinAlloc) {
    return;
  }

  header->setSize(keepBytes);
  auto newHeader = new (header->end()) Header(freeSize);
  free(newHeader);
  // Subtract the size of the new free header
  cumulativeBytes_ -= sizeof(Header);
}

int32_t HashStringAllocator::freeListIndex(int size) {
  return std::min(size - kMinAlloc, kNumFreeLists - 1);
}

void HashStringAllocator::removeFromFreeList(Header* header) {
  BOLT_CHECK(header->isFree());
  header->clearFree();
  auto index = freeListIndex(header->size());
  reinterpret_cast<CompactDoubleList*>(header->begin())->remove();
  if (free_[index].empty()) {
    bits::clearBit(freeNonEmpty_, index);
  }
}

HashStringAllocator::Header* FOLLY_NULLABLE
HashStringAllocator::allocate(int32_t size, bool exactSize) {
  if (size > kMaxAlloc && exactSize) {
    BOLT_CHECK_LE(size, Header::kSizeMask);
    auto header =
        reinterpret_cast<Header*>(allocateFromPool(size + sizeof(Header)));
    // In allocateFromPool(), it accounts for sizeof(Header).
    cumulativeBytes_ -= sizeof(Header);
    new (header) Header(size);
    return header;
  }
  auto header = allocateFromFreeLists(size, exactSize, exactSize);
  if (!header) {
    newSlab();
    header = allocateFromFreeLists(size, exactSize, exactSize);
    BOLT_CHECK(header != nullptr);
    BOLT_CHECK_GT(header->size(), 0);
  }

  return header;
}

HashStringAllocator::Header* FOLLY_NULLABLE
HashStringAllocator::allocateFromFreeLists(
    int32_t preferredSize,
    bool mustHaveSize,
    bool isFinalSize) {
  if (!numFree_) {
    return nullptr;
  }
  preferredSize = std::max(kMinAlloc, preferredSize);
  const auto index = freeListIndex(preferredSize);
  auto available = bits::findFirstBit(freeNonEmpty_, index, kNumFreeLists);
  if (!mustHaveSize && available == -1) {
    available = bits::findLastBit(freeNonEmpty_, 0, index);
  }
  if (available == -1) {
    return nullptr;
  }
  auto* header =
      allocateFromFreeList(preferredSize, mustHaveSize, isFinalSize, available);
  BOLT_CHECK_NOT_NULL(header);
  return header;
}

HashStringAllocator::Header* FOLLY_NULLABLE
HashStringAllocator::allocateFromFreeList(
    int32_t preferredSize,
    bool mustHaveSize,
    bool isFinalSize,
    int32_t freeListIndex) {
  auto* item = free_[freeListIndex].next();
  if (item == &free_[freeListIndex]) {
    return nullptr;
  }
  auto found = headerOf(item);
  BOLT_CHECK(
      found->isFree() && (!mustHaveSize || found->size() >= preferredSize));
  --numFree_;
  freeBytes_ -= found->size() + sizeof(Header);
  removeFromFreeList(found);
  auto next = found->next();
  if (next) {
    next->clearPreviousFree();
  }
  cumulativeBytes_ += found->size();
  if (isFinalSize) {
    freeRestOfBlock(found, preferredSize);
  }
  return found;
}

void HashStringAllocator::free(Header* _header) {
  Header* header = _header;

  do {
    Header* continued = nullptr;
    if (header->isContinued()) {
      continued = header->nextContinued();
      header->clearContinued();
    }
    if (header->size() > kMaxAlloc && !pool_.isInCurrentRange(header) &&
        allocationsFromPool_.find(header) != allocationsFromPool_.end()) {
      freeToPool(header, header->size() + sizeof(Header));
      // In `freeToPool()`, it accounts for sizeof(Header).
      cumulativeBytes_ += sizeof(Header);
    } else {
      BOLT_CHECK(!header->isFree());
      freeBytes_ += header->size() + sizeof(Header);
      cumulativeBytes_ -= header->size();
      Header* next = header->next();
      if (next) {
        BOLT_CHECK(!next->isPreviousFree());
        if (next->isFree()) {
          --numFree_;
          removeFromFreeList(next);
          header->setSize(header->size() + next->size() + sizeof(Header));
          next = reinterpret_cast<Header*>(header->end());
          BOLT_CHECK(next->isArenaEnd() || !next->isFree());
        }
      }
      if (header->isPreviousFree()) {
        auto previousFree = getPreviousFree(header);
        removeFromFreeList(previousFree);
        previousFree->setSize(
            previousFree->size() + header->size() + sizeof(Header));

        header = previousFree;
      } else {
        ++numFree_;
      }
      auto freedSize = header->size();
      auto freeIndex = freeListIndex(freedSize);
      bits::setBit(freeNonEmpty_, freeIndex);
      free_[freeIndex].insert(
          reinterpret_cast<CompactDoubleList*>(header->begin()));
      markAsFree(header);
    }
    header = continued;
  } while (header);
}

// static
int64_t HashStringAllocator::offset(
    Header* FOLLY_NONNULL header,
    Position position) {
  static const int64_t kOutOfRange = -1;
  if (!position.isSet()) {
    return kOutOfRange;
  }

  int64_t size = 0;
  for (;;) {
    assert(header);
    const auto length = header->usableSize();
    const auto offset = position.position - header->begin();
    if (offset >= 0 && offset <= length) {
      return size + offset;
    }
    if (!header->isContinued()) {
      return kOutOfRange;
    }
    size += length;
    header = header->nextContinued();
  }
}

// static
HashStringAllocator::Position HashStringAllocator::seek(
    Header* FOLLY_NONNULL header,
    int64_t offset) {
  int64_t size = 0;
  for (;;) {
    assert(header);
    auto length = header->usableSize();
    if (offset <= size + length) {
      return Position::atOffset(header, offset - size);
    }
    if (!header->isContinued()) {
      return Position::null();
    }
    size += length;
    header = header->nextContinued();
  }
}

// static
int64_t HashStringAllocator::available(const Position& position) {
  auto header = position.header;
  const auto startOffset = position.offset();
  // startOffset bytes from the first block are already used.
  int64_t size = -startOffset;
  for (;;) {
    assert(header);
    size += header->usableSize();
    if (!header->isContinued()) {
      return size;
    }
    header = header->nextContinued();
  }
}

void HashStringAllocator::ensureAvailable(int32_t bytes, Position& position) {
  if (available(position) >= bytes) {
    return;
  }

  ByteOutputStream stream(this);
  extendWrite(position, stream);
  static char data[128];
  while (bytes) {
    auto written = std::min<size_t>(bytes, sizeof(data));
    stream.append(folly::StringPiece(data, written));
    bytes -= written;
  }
  position = finishWrite(stream, 0).first;
}

inline bool HashStringAllocator::storeStringFast(
    const char* bytes,
    int32_t numBytes,
    char* destination) {
  auto roundedBytes = std::max(numBytes, kMinAlloc);
  Header* header = nullptr;
  if (free_[kNumFreeLists - 1].empty()) {
    if (roundedBytes >= kMaxAlloc) {
      return false;
    }
    auto index = freeListIndex(roundedBytes);
    auto available = bits::findFirstBit(freeNonEmpty_, index, kNumFreeLists);
    if (available < 0) {
      return false;
    }
    header = allocateFromFreeList(roundedBytes, true, true, available);
    BOLT_CHECK_NOT_NULL(header);
  } else {
    auto& freeList = free_[kNumFreeLists - 1];
    header = headerOf(freeList.next());
    const auto spaceTaken = roundedBytes + sizeof(Header);
    if (spaceTaken > header->size()) {
      return false;
    }
    if (header->size() - spaceTaken > kMaxAlloc) {
      // The entry after allocation stays in the largest free list.
      // The size at the end of the block is changed in place.
      reinterpret_cast<int32_t*>(header->end())[-1] -= spaceTaken;
      auto freeHeader = new (header->begin() + roundedBytes)
          Header(header->size() - spaceTaken);
      freeHeader->setFree();
      header->clearFree();
      memcpy(freeHeader->begin(), header->begin(), sizeof(CompactDoubleList));
      freeList.nextMoved(
          reinterpret_cast<CompactDoubleList*>(freeHeader->begin()));
      header->setSize(roundedBytes);
      freeBytes_ -= spaceTaken;
      cumulativeBytes_ += roundedBytes;
    } else {
      header =
          allocateFromFreeList(roundedBytes, true, true, kNumFreeLists - 1);
      if (!header) {
        return false;
      }
    }
  }
  simd::memcpy(header->begin(), bytes, numBytes);
  *reinterpret_cast<StringView*>(destination) =
      StringView(reinterpret_cast<char*>(header->begin()), numBytes);
  return true;
}

void HashStringAllocator::copyMultipartNoInline(
    const StringView& srcStr,
    char* group,
    int32_t offset) {
  const auto numBytes = srcStr.size();
  if (storeStringFast(srcStr.data(), numBytes, group + offset)) {
    return;
  }
  // Write the string as non-contiguous chunks.
  ByteOutputStream stream(this, false, false);
  auto position = newWrite(stream, numBytes);
  stream.appendStringView(srcStr);
  finishWrite(stream, 0);

  // The stringView has a pointer to the first byte and the total
  // size. Read with contiguousString().
  *reinterpret_cast<StringView*>(group + offset) =
      StringView(reinterpret_cast<char*>(position.position), numBytes);
}

std::string HashStringAllocator::toString() const {
  std::ostringstream out;

  out << "allocated: " << cumulativeBytes_ << " bytes" << std::endl;
  out << "free: " << freeBytes_ << " bytes in " << numFree_ << " blocks"
      << std::endl;
  out << "standalone allocations: " << sizeFromPool_ << " bytes in "
      << allocationsFromPool_.size() << " allocations" << std::endl;
  out << "ranges: " << pool_.numRanges() << std::endl;

  static const auto kHugePageSize = memory::AllocationTraits::kHugePageSize;

  for (auto i = 0; i < pool_.numRanges(); ++i) {
    auto topRange = pool_.rangeAt(i);
    auto topRangeSize = topRange.size();

    out << "range " << i << ": " << topRangeSize << " bytes" << std::endl;

    // Some ranges are short and contain one arena. Some are multiples of huge
    // page size and contain one arena per huge page.
    for (int64_t subRangeStart = 0; subRangeStart < topRangeSize;
         subRangeStart += kHugePageSize) {
      auto range = folly::Range<char*>(
          topRange.data() + subRangeStart,
          std::min<int64_t>(topRangeSize, kHugePageSize));
      auto size = range.size() - simd::kPadding;

      auto end = reinterpret_cast<Header*>(range.data() + size);
      auto header = reinterpret_cast<Header*>(range.data());
      while (header != nullptr && header != end) {
        out << "\t" << header->toString() << std::endl;
        header = header->next();
      }
    }
  }

  return out.str();
}

int64_t HashStringAllocator::checkConsistency() const {
  static const auto kHugePageSize = memory::AllocationTraits::kHugePageSize;

  uint64_t numFree = 0;
  uint64_t freeBytes = 0;
  int64_t allocatedBytes = 0;
  for (auto i = 0; i < pool_.numRanges(); ++i) {
    auto topRange = pool_.rangeAt(i);
    auto topRangeSize = topRange.size();
    if (topRangeSize >= kHugePageSize) {
      BOLT_CHECK_EQ(0, topRangeSize % kHugePageSize);
    }
    // Some ranges are short and contain one arena. Some are multiples of huge
    // page size and contain one arena per huge page.
    for (int64_t subRangeStart = 0; subRangeStart < topRangeSize;
         subRangeStart += kHugePageSize) {
      auto range = folly::Range<char*>(
          topRange.data() + subRangeStart,
          std::min<int64_t>(topRangeSize, kHugePageSize));
      auto size = range.size() - simd::kPadding;
      bool previousFree = false;
      auto end = reinterpret_cast<Header*>(range.data() + size);
      auto header = reinterpret_cast<Header*>(range.data());
      while (header != end) {
        BOLT_CHECK_GE(reinterpret_cast<char*>(header), range.data());
        BOLT_CHECK_LT(
            reinterpret_cast<char*>(header), reinterpret_cast<char*>(end));
        BOLT_CHECK_LE(
            reinterpret_cast<char*>(header->end()),
            reinterpret_cast<char*>(end));
        BOLT_CHECK_EQ(header->isPreviousFree(), previousFree);

        if (header->isFree()) {
          BOLT_CHECK(!previousFree);
          BOLT_CHECK(!header->isContinued());
          if (header->next()) {
            BOLT_CHECK_EQ(
                header->size(),
                *(reinterpret_cast<int32_t*>(header->end()) - 1));
          }
          ++numFree;
          freeBytes += sizeof(Header) + header->size();
        } else if (header->isContinued()) {
          // If the content of the header is continued, check the
          // continue header is readable and not free.
          auto continued = header->nextContinued();
          BOLT_CHECK(!continued->isFree());
          allocatedBytes += header->size() - Header::kContinuedPtrSize;
        } else {
          allocatedBytes += header->size();
        }
        previousFree = header->isFree();
        header = reinterpret_cast<Header*>(header->end());
      }
    }
  }

  BOLT_CHECK_EQ(numFree, numFree_);
  BOLT_CHECK_EQ(freeBytes, freeBytes_);
  uint64_t numInFreeList = 0;
  uint64_t bytesInFreeList = 0;
  for (auto i = 0; i < kNumFreeLists; ++i) {
    bool hasData = bits::isBitSet(freeNonEmpty_, i);
    bool listNonEmpty = !free_[i].empty();
    BOLT_CHECK_EQ(hasData, listNonEmpty);
    for (auto free = free_[i].next(); free != &free_[i]; free = free->next()) {
      ++numInFreeList;
      BOLT_CHECK(
          free->next()->previous() == free,
          "free list previous link inconsistent");
      auto size = headerOf(free)->size();
      BOLT_CHECK_GE(size, kMinAlloc);
      if (size - kMinAlloc < kNumFreeLists - 1) {
        BOLT_CHECK_EQ(size - kMinAlloc, i);
      } else {
        BOLT_CHECK_GE(size - kMinAlloc, kNumFreeLists - 1);
      }
      bytesInFreeList += size + sizeof(Header);
    }
  }

  BOLT_CHECK_EQ(numInFreeList, numFree_);
  BOLT_CHECK_EQ(bytesInFreeList, freeBytes_);
  return allocatedBytes;
}

bool HashStringAllocator::isEmpty() const {
  return sizeFromPool_ == 0 && checkConsistency() == 0;
}

void HashStringAllocator::checkEmpty() const {
  BOLT_CHECK_EQ(0, sizeFromPool_);
  BOLT_CHECK_EQ(0, checkConsistency());
}

// Note that, make sure that you have compare the prefix via bswap
// no bswap opt in this function.
int HashStringAllocator::FastRowStringViewCompareAsc(
    char* l,
    char* r) noexcept {
  const StringView* left = (const StringView*)(l);
  const StringView* right = (const StringView*)(r);

  Header* leftHead = reinterpret_cast<Header*>(l);
  Header* rightHead = reinterpret_cast<Header*>(r);

  if (!left->isInline()) {
    leftHead = headerOf(left->data());
  }
  if (!right->isInline()) {
    rightHead = headerOf(right->data());
  }

  uint32_t leftLen = left->size();
  uint32_t rightLen = right->size();
  const char* leftData = left->data();
  const char* rightData = right->data();

  // a contiguous fragment
  uint32_t leftContLen = std::min((uint32_t)(leftHead->usableSize()), leftLen);
  uint32_t rightContLen =
      std::min((uint32_t)(rightHead->usableSize()), rightLen);

  do {
    auto useLen = leftContLen < rightContLen ? leftContLen : rightContLen;
    int res = memcmp(leftData, rightData, useLen);
    if (res != 0) {
      return res;
    }

    leftLen -= useLen;
    rightLen -= useLen;

    leftContLen -= useLen;
    rightContLen -= useLen;
    if (leftLen == 0 || rightLen == 0) {
      return leftLen - rightLen;
    }

    if (leftContLen == 0 && leftHead->isContinued()) {
      // left_con_len means it is not inline, it is ok to call next()
      leftHead = leftHead->nextContinued();
      leftContLen = std::min((uint32_t)(leftHead->usableSize()), leftLen);
      leftData = leftHead->begin();
    } else {
      leftData += useLen; // still contiguous
    }

    if (rightContLen == 0 && rightHead->isContinued()) {
      rightHead = rightHead->nextContinued();
      rightContLen = std::min((uint32_t)(rightHead->usableSize()), rightLen);
      rightData = rightHead->begin();
    } else {
      rightData += useLen; // still contiguous
    }

    if (useLen == 0) {
      return leftContLen - rightContLen;
    }
  } while (true);

  return 0; // should not be here
}

} // namespace bytedance::bolt
