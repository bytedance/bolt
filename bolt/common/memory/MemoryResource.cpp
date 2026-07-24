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

#include "bolt/common/memory/MemoryResource.h"

#include <algorithm>
#include <cstdlib>
#include <new>

namespace bytedance::bolt::memory {
namespace {

constexpr std::size_t kReservationQuantum = 1 << 20;

// For <=32K allocations, jemalloc performs well. For larger allocations, we
// delegate to the memory pool.
constexpr std::size_t kLargeAllocationThreshold = 32 << 10;
constexpr std::size_t kContiguousAllocationThreshold = 256 << 10;

} // namespace

struct MonotonicMemoryResource::Chunk {
  void* data{nullptr};
  std::size_t size{0};
  std::size_t alignment{0};
  bool contiguous{false};
  ContiguousAllocation allocation;
};

SlabAllocatorState::SlabAllocatorState(MemoryPool* pool) : pool_{pool} {}

SlabMemoryResource::SlabMemoryResource(MemoryPool* pool) : state_{pool} {}

SlabMemoryResource::SlabMemoryResource(MemoryPool& pool)
    : SlabMemoryResource(&pool) {}

SlabMemoryResource::~SlabMemoryResource() {
  releaseReservation();
}

void* SlabMemoryResource::allocate(std::size_t bytes, std::size_t alignment) {
  if (bytes >= kLargeAllocationThreshold) [[unlikely]] {
    return poolChecked()->allocate(bytes);
  }

  const auto free = freeBytes();
  std::size_t newReservedBytes{0};
  while (bytes > free + newReservedBytes)
    [[unlikely]] {
      newReservedBytes += kReservationQuantum;
    }

  if (newReservedBytes > 0) [[unlikely]] {
    poolImpl()->reserve(newReservedBytes);
    state_.reservedBytes += newReservedBytes;
  }

  void* p = nullptr;
  if (alignment > alignof(std::max_align_t)) {
    p = std::aligned_alloc(alignment, bytes);
  } else {
    p = std::malloc(bytes);
  }
  if (p == nullptr) [[unlikely]] {
    if (newReservedBytes > 0) {
      state_.reservedBytes -= newReservedBytes;
    }
    throw std::bad_alloc();
  }

  state_.usedBytes += bytes;
  return p;
}

void SlabMemoryResource::deallocate(
    void* p,
    std::size_t bytes,
    std::size_t /*alignment*/) {
  if (p == nullptr) [[unlikely]] {
    return;
  }

  if (bytes >= kLargeAllocationThreshold) [[unlikely]] {
    poolChecked()->free(p, bytes);
    return;
  }

  const auto freeBytes = this->freeBytes();

  std::size_t recycleBytes{0};
  while (freeBytes + bytes >= recycleBytes + kReservationQuantum)
    [[unlikely]] {
      recycleBytes += kReservationQuantum;
    }

  state_.usedBytes -= bytes;
  if (state_.usedBytes == 0 && state_.reservedBytes > recycleBytes)
      [[unlikely]] {
    recycleBytes = state_.reservedBytes;
  }
  if (recycleBytes > 0) [[unlikely]] {
    state_.reservedBytes -= recycleBytes;
    poolImpl()->release(recycleBytes, false);
  }
  std::free(p);
}

MemoryPool* SlabMemoryResource::pool() const noexcept {
  return state_.pool_;
}

std::size_t SlabMemoryResource::freeBytes() const noexcept {
  return reservedBytes() - usedBytes();
}

std::size_t SlabMemoryResource::usedBytes() const noexcept {
  return state_.usedBytes;
}

std::size_t SlabMemoryResource::reservedBytes() const noexcept {
  return state_.reservedBytes;
}

void SlabMemoryResource::releaseReservation() noexcept {
  if (state_.reservedBytes == 0) {
    return;
  }

  BOLT_DCHECK_EQ(state_.usedBytes, 0);
  const auto reserved = state_.reservedBytes;
  state_.reservedBytes = 0;
  poolImpl()->release(reserved, false);
}

MemoryPool* SlabMemoryResource::poolChecked() const {
  auto* pool = this->pool();
  BOLT_CHECK_NOT_NULL(pool);
  return pool;
}

MemoryPoolImpl* SlabMemoryResource::poolImpl() const {
  auto* impl = dynamic_cast<MemoryPoolImpl*>(state_.pool_);
  BOLT_CHECK_NOT_NULL(impl);
  return impl;
}

MonotonicMemoryResource::MonotonicMemoryResource(
    MemoryPool* pool,
    std::size_t /*initialBufferSize*/)
    : pool_{pool} {
  BOLT_CHECK_NOT_NULL(pool);
}

MonotonicMemoryResource::MonotonicMemoryResource(
    MemoryPool& pool,
    std::size_t initialBufferSize)
    : MonotonicMemoryResource(&pool, initialBufferSize) {}

MonotonicMemoryResource::~MonotonicMemoryResource() {
  release();
}

void* MonotonicMemoryResource::allocate(
    std::size_t bytes,
    std::size_t /*alignment*/) {
  const auto allocationBytes = bytes;
  auto* p = static_cast<std::byte*>(curr_);
  if (currChunk_ == nullptr ||
      p + allocationBytes >=
          static_cast<std::byte*>(currChunk_) + currChunkSize_) [[unlikely]] {
    addChunk(allocationBytes, 1);
    p = static_cast<std::byte*>(curr_);
  }
  BOLT_DCHECK_LE(
      reinterpret_cast<std::uintptr_t>(p + allocationBytes),
      reinterpret_cast<std::uintptr_t>(
          static_cast<std::byte*>(currChunk_) + currChunkSize_));
  BOLT_DCHECK_NOT_NULL(currChunk_);
  curr_ = p + allocationBytes;
  usedBytes_ += allocationBytes;
  return p;
}

void MonotonicMemoryResource::deallocate(
    void* /*p*/,
    std::size_t /*bytes*/,
    std::size_t /*alignment*/) {}

MemoryPool* MonotonicMemoryResource::pool() const noexcept {
  return pool_;
}

std::size_t MonotonicMemoryResource::usedBytes() const noexcept {
  return usedBytes_;
}

std::size_t MonotonicMemoryResource::reservedBytes() const noexcept {
  return reservedBytes_;
}

void MonotonicMemoryResource::addChunk(
    std::size_t bytes,
    std::size_t alignment) {
  chunks_.emplace_back();
  auto& chunk = chunks_.back();
  try {
    if (reservedBytes_ > kContiguousAllocationThreshold ||
        bytes >= AllocationTraits::kHugePageSize) {
      pool_->allocateContiguous(
          AllocationTraits::numPages(
              std::max<std::size_t>(bytes, AllocationTraits::kHugePageSize)),
          chunk.allocation);
      chunk.data = chunk.allocation.data();
      chunk.size = chunk.allocation.size();
      chunk.contiguous = true;
    } else if (bytes >= kLargeRequestThreshold) {
      // when the used bytes is limited(<256K), but requested bytes > 64K,
      // allocate a chuck for this request
      chunk.alignment =
          std::max<std::size_t>(alignment, MemoryAllocator::kMinAlignment);
      chunk.size = chunk.alignment == MemoryAllocator::kMinAlignment
          ? bytes
          : bits::roundUp(bytes, chunk.alignment);
      chunk.data = pool_->allocate(chunk.size, chunk.alignment);
    } else {
      // at the beginning, allocate a chuck that is not very big for small
      // requests to avoid memory waste.
      chunk.alignment =
          std::max<std::size_t>(alignment, MemoryAllocator::kMinAlignment);
      chunk.size = bits::roundUp(
          std::max(bytes, kLargeRequestThreshold), chunk.alignment);
      chunk.data = pool_->allocate(chunk.size, chunk.alignment);
    }
  } catch (...) {
    chunks_.pop_back();
    throw;
  }
  currChunk_ = chunk.data;
  currChunkSize_ = chunk.size;
  curr_ = chunk.data;
  reservedBytes_ += chunk.size;
}

void MonotonicMemoryResource::release() noexcept {
  for (auto& chunk : chunks_) {
    if (chunk.contiguous) {
      pool_->freeContiguous(chunk.allocation);
    } else {
      pool_->free(chunk.data, chunk.size, chunk.alignment);
    }
  }
  chunks_.clear();
  currChunk_ = nullptr;
  currChunkSize_ = 0;
  curr_ = nullptr;
  usedBytes_ = 0;
  reservedBytes_ = 0;
}

} // namespace bytedance::bolt::memory
