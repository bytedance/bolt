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

#include "bolt/common/memory/AllocationPool.h"
#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/MemoryAllocator.h"
namespace bytedance::bolt::memory {

folly::Range<char*> AllocationPool::rangeAt(int32_t index) const {
  if (index < allocations_.size()) {
    if (allocationStates_[index].released) {
      return folly::Range<char*>(static_cast<char*>(nullptr), size_t{0});
    }
    auto run = allocations_[index].runAt(0);
    return folly::Range<char*>(
        run.data<char>(),
        run.data<char>() == startOfRun_ ? currentOffset_ : run.numBytes());
  }
  const auto largeIndex = index - allocations_.size();
  if (largeIndex < largeAllocations_.size()) {
    if (largeAllocationStates_[largeIndex].released) {
      return folly::Range<char*>(static_cast<char*>(nullptr), size_t{0});
    }
    auto range = largeAllocations_[largeIndex].hugePageRange().value();
    if (range.data() == startOfRun_) {
      return folly::Range<char*>(range.data(), currentOffset_);
    }
    return range;
  }
  BOLT_FAIL("Out of range index for rangeAt(): {}", index);
}

void AllocationPool::clear() {
  allocations_.clear();
  largeAllocations_.clear();
  allocationStates_.clear();
  largeAllocationStates_.clear();
  startOfRun_ = nullptr;
  bytesInRun_ = 0;
  currentOffset_ = 0;
  usedBytes_ = 0;
  currentRunIsLarge_ = false;
  largeRunsStarted_ = false;
}

char* AllocationPool::allocateFixed(uint64_t bytes, int32_t alignment) {
  BOLT_CHECK_GT(bytes, 0, "Cannot allocate zero bytes");
  if (freeAddressableBytes() >= bytes && alignment == 1) {
    auto* result = startOfRun_ + currentOffset_;
    currentOffset_ += bytes;
    if (currentOffset_ > endOfReservedRun()) {
      growLastAllocation();
    }
    recordAllocation();
    return result;
  }
  BOLT_CHECK_EQ(
      __builtin_popcount(alignment), 1, "Alignment can only be power of 2");

  auto numPages = AllocationTraits::numPages(bytes + alignment - 1);

  if (freeAddressableBytes() == 0) {
    newRunImpl(numPages);
  } else {
    auto alignedBytes = bytes + alignmentPadding(firstFreeInRun(), alignment);
    if (freeAddressableBytes() < alignedBytes) {
      newRunImpl(numPages);
    }
  }
  currentOffset_ += alignmentPadding(firstFreeInRun(), alignment);
  BOLT_CHECK_LE(bytes + currentOffset_, bytesInRun_);
  auto* result = startOfRun_ + currentOffset_;
  BOLT_CHECK_EQ(reinterpret_cast<uintptr_t>(result) % alignment, 0);
  currentOffset_ += bytes;
  if (currentOffset_ > endOfReservedRun()) {
    growLastAllocation();
  }
  recordAllocation();
  return result;
}

AllocationPool::ReleasedRangeStats AllocationPool::releaseRangesBefore(
    int32_t rangeIndex) {
  rangeIndex = std::min(rangeIndex, numRanges());
  ReleasedRangeStats stats;
  const auto numSmallRanges = static_cast<int32_t>(allocations_.size());
  for (auto i = 0; i < std::min(rangeIndex, numSmallRanges); ++i) {
    if (allocationStates_[i].released) {
      continue;
    }
    stats.bytes += allocations_[i].byteSize();
    stats.allocations += allocationStates_[i].allocations;
    pool_->freeNonContiguous(allocations_[i]);
    allocationStates_[i].released = true;
  }

  const auto largeEnd = std::max<int32_t>(0, rangeIndex - numSmallRanges);
  const auto numLargeRanges = static_cast<int32_t>(largeAllocations_.size());
  for (auto i = 0; i < std::min(largeEnd, numLargeRanges); ++i) {
    if (largeAllocationStates_[i].released) {
      continue;
    }
    stats.bytes += largeAllocations_[i].size();
    stats.allocations += largeAllocationStates_[i].allocations;
    pool_->freeContiguous(largeAllocations_[i]);
    largeAllocationStates_[i].released = true;
  }
  usedBytes_ -= stats.bytes;
  if (rangeIndex == numRanges()) {
    startOfRun_ = nullptr;
    bytesInRun_ = 0;
    currentOffset_ = 0;
    currentRunIsLarge_ = false;
  }
  return stats;
}

void AllocationPool::growLastAllocation() {
  BOLT_CHECK_GT(bytesInRun_, AllocationTraits::kHugePageSize);
  const auto bytesToReserve = bits::roundUp(
      currentOffset_ - endOfReservedRun(), AllocationTraits::kHugePageSize);
  largeAllocations_.back().grow(AllocationTraits::numPages(bytesToReserve));
  usedBytes_ += bytesToReserve;
}

void AllocationPool::newRunImpl(MachinePageCount numPages) {
  if (largeRunsStarted_ || usedBytes_ >= hugePageThreshold_ ||
      numPages > pool_->sizeClasses().back()) {
    // At least 16 huge pages, no more than kMaxMmapBytes. The next is
    // double the previous. Because the previous is a hair under the
    // power of two because of fractional pages at ends of allocation,
    // add an extra huge page size.
    int64_t nextSize = std::min(
        kMaxMmapBytes,
        std::max<int64_t>(
            16 * AllocationTraits::kHugePageSize,
            bits::nextPowerOfTwo(
                usedBytes_ + AllocationTraits::kHugePageSize)));
    // Round 'numPages' to no of pages in huge page. Allocating this plus an
    // extra huge page guarantees that 'numPages' worth of contiguous aligned
    // huge pages will be founfd in the allocation.
    numPages = bits::roundUp(numPages, AllocationTraits::numPagesInHugePage());
    if (AllocationTraits::pageBytes(numPages) +
            AllocationTraits::kHugePageSize >
        nextSize) {
      // Extra large single request.
      nextSize = AllocationTraits::pageBytes(numPages) +
          AllocationTraits::kHugePageSize;
    }

    ContiguousAllocation largeAlloc;
    const MachinePageCount pagesToAlloc =
        AllocationTraits::numPagesInHugePage();
    pool_->allocateContiguous(
        pagesToAlloc, largeAlloc, AllocationTraits::numPages(nextSize));

    auto range = largeAlloc.hugePageRange().value();
    startOfRun_ = range.data();
    bytesInRun_ = range.size();
    largeAllocations_.emplace_back(std::move(largeAlloc));
    largeAllocationStates_.emplace_back();
    currentOffset_ = 0;
    currentRunIsLarge_ = true;
    largeRunsStarted_ = true;
    usedBytes_ += AllocationTraits::pageBytes(pagesToAlloc);
    return;
  }

  Allocation allocation;
  auto roundedPages = std::max<int32_t>(kMinPages, numPages);
  pool_->allocateNonContiguous(roundedPages, allocation, roundedPages);
  BOLT_CHECK_EQ(allocation.numRuns(), 1);
  startOfRun_ = allocation.runAt(0).data<char>();
  bytesInRun_ = allocation.runAt(0).numBytes();
  currentOffset_ = 0;
  currentRunIsLarge_ = false;
  allocations_.push_back(std::move(allocation));
  allocationStates_.emplace_back();
  usedBytes_ += bytesInRun_;
}

void AllocationPool::newRun(int64_t preferredSize) {
  newRunImpl(AllocationTraits::numPages(preferredSize));
}

void AllocationPool::recordAllocation() {
  if (currentRunIsLarge_) {
    ++largeAllocationStates_.back().allocations;
  } else {
    ++allocationStates_.back().allocations;
  }
}

} // namespace bytedance::bolt::memory
