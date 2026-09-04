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

#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>
#include <vector>

#include "bolt/common/memory/MemoryPool.h"

namespace bytedance::bolt::memory {

template <typename Resource>
concept MemoryResource = requires(
    Resource& resource,
    void* p,
    std::size_t bytes,
    std::size_t alignment) {
  { resource.allocate(bytes, alignment) } -> std::same_as<void*>;
  { resource.deallocate(p, bytes, alignment) } -> std::same_as<void>;
};

struct SlabAllocatorState {
  explicit SlabAllocatorState(MemoryPool* pool = nullptr);

  MemoryPool* pool_{nullptr};
  std::size_t usedBytes{0};
  std::size_t reservedBytes{0};
};

class SlabMemoryResource {
 public:
  explicit SlabMemoryResource(MemoryPool* pool = nullptr);

  explicit SlabMemoryResource(MemoryPool& pool);

  SlabMemoryResource(const SlabMemoryResource&) = delete;
  SlabMemoryResource& operator=(const SlabMemoryResource&) = delete;

  ~SlabMemoryResource();

  [[nodiscard]] void* allocate(std::size_t bytes, std::size_t alignment);

  void deallocate(void* p, std::size_t bytes, std::size_t alignment);

  MemoryPool* pool() const noexcept;

  std::size_t freeBytes() const noexcept;

  std::size_t usedBytes() const noexcept;

  std::size_t reservedBytes() const noexcept;

 private:
  void releaseReservation() noexcept;

  MemoryPool* poolChecked() const;

  MemoryPoolImpl* poolImpl() const;

  SlabAllocatorState state_;
};

static_assert(MemoryResource<SlabMemoryResource>);

class MonotonicMemoryResource {
  struct Chunk;

 public:
  explicit MonotonicMemoryResource(
      MemoryPool* pool,
      std::size_t initialBufferSize = kLargeRequestThreshold);

  explicit MonotonicMemoryResource(
      MemoryPool& pool,
      std::size_t initialBufferSize = kLargeRequestThreshold);

  MonotonicMemoryResource(const MonotonicMemoryResource&) = delete;
  MonotonicMemoryResource& operator=(const MonotonicMemoryResource&) = delete;

  ~MonotonicMemoryResource();

  [[nodiscard]] void* allocate(std::size_t bytes, std::size_t alignment = 1);

  void deallocate(void* p, std::size_t bytes, std::size_t alignment);

  MemoryPool* pool() const noexcept;

  std::size_t usedBytes() const noexcept;

  std::size_t reservedBytes() const noexcept;

 private:
  void addChunk(std::size_t bytes, std::size_t alignment);

  void release() noexcept;

  static constexpr std::size_t kLargeRequestThreshold = 64 << 10;

  MemoryPool* pool_{nullptr};
  std::size_t usedBytes_{0};
  std::size_t reservedBytes_{0};
  void* currChunk_{nullptr};
  std::size_t currChunkSize_{0};
  void* curr_{nullptr};
  std::vector<Chunk> chunks_;
};

static_assert(MemoryResource<MonotonicMemoryResource>);

/// STL allocator backed by a MonotonicMemoryResource. Individual
/// deallocations are intentionally ignored. The resource must outlive every
/// container using the allocator, and releases all allocations together on
/// destruction.
template <typename T, std::size_t Alignment>
requires(
    Alignment > 0 &&
    (Alignment & (Alignment - 1)) == 0) class MonotonicAllocator {
 public:
  using value_type = T;
  using pointer = T*;
  using const_pointer = const T*;
  using void_pointer = void*;
  using const_void_pointer = const void*;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;

  template <typename U>
  struct rebind {
    using other = MonotonicAllocator<U, Alignment>;
  };

  using propagate_on_container_copy_assignment = std::false_type;
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;
  using is_always_equal = std::false_type;

  MonotonicAllocator() noexcept = default;

  explicit MonotonicAllocator(MonotonicMemoryResource* resource)
      : resource_{resource} {}

  explicit MonotonicAllocator(MonotonicMemoryResource& resource)
      : MonotonicAllocator(&resource) {}

  MonotonicAllocator(const MonotonicAllocator& other) = default;

  MonotonicAllocator(MonotonicAllocator&& other) noexcept
      : resource_{std::exchange(other.resource_, nullptr)} {}

  template <typename U>
  MonotonicAllocator(const MonotonicAllocator<U, Alignment>& other)
      : resource_{other.resource_} {}

  MonotonicAllocator& operator=(const MonotonicAllocator& other) = default;

  MonotonicAllocator& operator=(MonotonicAllocator&& other) noexcept {
    resource_ = std::exchange(other.resource_, nullptr);
    return *this;
  }

  friend void swap(MonotonicAllocator& lhs, MonotonicAllocator& rhs) noexcept {
    lhs.swap(rhs);
  }

  ~MonotonicAllocator() = default;

  [[nodiscard]] T* allocate(std::size_t n) {
    const auto bytes = allocationBytes(n);
    return static_cast<T*>(resource_->allocate(bytes));
  }

  void deallocate(T* p, std::size_t n) {
    if (p == nullptr) [[unlikely]] {
      return;
    }
    resource_->deallocate(p, allocationBytes(n), alignment());
  }

  template <typename U>
  bool operator==(
      const MonotonicAllocator<U, Alignment>& other) const noexcept {
    return resource_ == other.resource_;
  }

  template <typename U>
  bool operator!=(
      const MonotonicAllocator<U, Alignment>& other) const noexcept {
    return !(*this == other);
  }

 private:
  template <typename U, std::size_t OtherAlignment>
  requires(
      OtherAlignment > 0 &&
      (OtherAlignment & (OtherAlignment - 1)) ==
          0) friend class MonotonicAllocator;

  void swap(MonotonicAllocator& other) noexcept {
    std::swap(resource_, other.resource_);
  }

  std::size_t allocationBytes(std::size_t n) const {
    const auto bytes = n * sizeof(T);
    const auto allocationAlignment = alignment();
    const auto roundedBytes = checkedPlus(bytes, allocationAlignment - 1);
    return roundedBytes / allocationAlignment * allocationAlignment;
  }

  static constexpr std::size_t alignment() {
    return Alignment > alignof(T) ? Alignment : alignof(T);
  }

  MonotonicMemoryResource* resource_{nullptr};
};

template <typename T, std::size_t Alignment>
requires(
    Alignment > 0 && (Alignment & (Alignment - 1)) == 0) class SlabAllocator {
 public:
  using value_type = T;
  using pointer = T*;
  using const_pointer = const T*;
  using void_pointer = void*;
  using const_void_pointer = const void*;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;

  template <typename U>
  struct rebind {
    using other = SlabAllocator<U, Alignment>;
  };

  // Copy assignment copies values only and does not propagate the resource.
  // Move assignment and swap propagate the resource pointer.
  // Allocators backed by different pools are not equivalent.
  using propagate_on_container_copy_assignment = std::false_type;
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;
  using is_always_equal = std::false_type;

  SlabAllocator() noexcept = default;

  explicit SlabAllocator(SlabMemoryResource* resource) : resource_{resource} {}

  explicit SlabAllocator(SlabMemoryResource& resource)
      : SlabAllocator(&resource) {}

  SlabAllocator(const SlabAllocator& other) = default;

  SlabAllocator(SlabAllocator&& other) noexcept
      : resource_{std::exchange(other.resource_, nullptr)} {}

  template <typename U>
  SlabAllocator(const SlabAllocator<U, Alignment>& other)
      : resource_{other.resource_} {}

  SlabAllocator& operator=(const SlabAllocator& other) = default;

  SlabAllocator& operator=(SlabAllocator&& other) noexcept {
    resource_ = std::exchange(other.resource_, nullptr);
    return *this;
  }

  friend void swap(SlabAllocator& lhs, SlabAllocator& rhs) noexcept {
    lhs.swap(rhs);
  }

  ~SlabAllocator() = default;

  [[nodiscard]] T* allocate(std::size_t n) {
    const std::size_t bytes = allocationBytes(n);
    return static_cast<T*>(resource_->allocate(bytes, alignment()));
  }

  void deallocate(T* p, std::size_t n) {
    if (p == nullptr) [[unlikely]] {
      return;
    }

    const std::size_t bytes = allocationBytes(n);
    resource_->deallocate(p, bytes, alignment());
  }

  template <typename U>
  bool operator==(const SlabAllocator<U, Alignment>& other) const noexcept {
    return resource_ == other.resource_;
  }

  template <typename U>
  bool operator!=(const SlabAllocator<U, Alignment>& other) const noexcept {
    return !(*this == other);
  }

 private:
  template <typename U, std::size_t OtherAlignment>
  requires(
      OtherAlignment > 0 &&
      (OtherAlignment & (OtherAlignment - 1)) == 0) friend class SlabAllocator;

  void swap(SlabAllocator& other) noexcept {
    std::swap(resource_, other.resource_);
  }

  std::size_t allocationBytes(std::size_t n) const {
    const auto bytes = checkedMultiply(n, sizeof(T));
    const auto roundedBytes = checkedPlus(bytes, Alignment - 1);
    return roundedBytes / Alignment * Alignment;
  }

  static constexpr std::size_t alignment() {
    return Alignment > alignof(T) ? Alignment : alignof(T);
  }

  SlabMemoryResource* resource_{nullptr};
};

} // namespace bytedance::bolt::memory
