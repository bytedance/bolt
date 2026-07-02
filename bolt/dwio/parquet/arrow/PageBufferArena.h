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
#include <memory>

#include "bolt/dwio/parquet/arrow/Platform.h"

namespace bytedance::bolt::parquet::arrow {

namespace detail {

class FixedCapacityResizableBuffer : public ResizableBuffer {
 public:
  FixedCapacityResizableBuffer(
      const std::shared_ptr<Buffer>& parent,
      int64_t offset,
      int64_t size,
      int64_t capacity)
      : ResizableBuffer(
            parent->mutable_data() + offset,
            size,
            parent->memory_manager()) {
    parent_ = parent;
    offset_ = offset;
    capacity_ = capacity;
  }

  ::arrow::Status Resize(int64_t new_size, bool /*shrink_to_fit*/) override {
    if (new_size > capacity_) {
      return ::arrow::Status::CapacityError(
          "FixedCapacityResizableBuffer cannot grow beyond capacity");
    }
    size_ = new_size;
    return ::arrow::Status::OK();
  }

  ::arrow::Status Reserve(int64_t new_capacity) override {
    if (new_capacity > capacity_) {
      return ::arrow::Status::CapacityError(
          "FixedCapacityResizableBuffer cannot reserve beyond capacity");
    }
    return ::arrow::Status::OK();
  }

  ::arrow::Status ShrinkCapacity(int64_t new_capacity) {
    if (new_capacity < 0 || new_capacity > capacity_) {
      return ::arrow::Status::CapacityError(
          "FixedCapacityResizableBuffer cannot change to invalid capacity");
    }
    if (size_ > new_capacity) {
      return ::arrow::Status::CapacityError(
          "FixedCapacityResizableBuffer size exceeds requested capacity");
    }
    capacity_ = new_capacity;
    return ::arrow::Status::OK();
  }

  int64_t offset() const {
    return offset_;
  }

  int64_t end_offset() const {
    return offset_ + capacity_;
  }

  const std::shared_ptr<Buffer>& parent() const {
    return parent_;
  }

 private:
  int64_t offset_;
};

} // namespace detail

class PageBufferArena {
 public:
  explicit PageBufferArena(MemoryPool* pool, int64_t size)
      : pool_(pool), capacity_(size) {
    if (capacity_ > 0) {
      buffer_ = AllocateBuffer(pool_, capacity_);
    }
  }

  bool enabled() const {
    return buffer_ != nullptr;
  }

  int64_t capacity() const {
    return capacity_;
  }

  int64_t bytes_reserved() const {
    return next_offset_;
  }

  int64_t available_bytes() const {
    if (!enabled()) {
      return 0;
    }
    const int64_t aligned_offset = AlignTo(next_offset_, kAllocationAlignment);
    if (aligned_offset >= capacity_) {
      return 0;
    }
    return capacity_ - aligned_offset;
  }

  bool Contains(const Buffer* buffer) const {
    if (!enabled() || buffer == nullptr || buffer->capacity() <= 0 ||
        buffer->data() == nullptr) {
      return false;
    }
    const auto* arenaBegin = buffer_->data();
    const auto* arenaEnd = arenaBegin + buffer_->capacity();
    const auto* bufferBegin = buffer->data();
    const auto* bufferEnd = bufferBegin + buffer->capacity();
    return arenaBegin <= bufferBegin && bufferEnd <= arenaEnd;
  }

  void Reset() {
    next_offset_ = 0;
  }

  std::shared_ptr<ResizableBuffer> TryAllocate(int64_t size) {
    return TryAllocate(size, size);
  }

  std::shared_ptr<ResizableBuffer> TryAllocate(int64_t size, int64_t capacity) {
    if (!enabled() || size < 0 || capacity <= 0 || size > capacity) {
      return nullptr;
    }

    const int64_t aligned_offset = AlignTo(next_offset_, kAllocationAlignment);
    if (aligned_offset > capacity_ || capacity_ - aligned_offset < capacity) {
      return nullptr;
    }

    next_offset_ = aligned_offset + capacity;
    return std::make_shared<detail::FixedCapacityResizableBuffer>(
        buffer_, aligned_offset, size, capacity);
  }

  ::arrow::Status CommitLastAllocation(
      const std::shared_ptr<ResizableBuffer>& buffer) {
    if (!enabled()) {
      return ::arrow::Status::Invalid("PageBufferArena is disabled");
    }
    auto* fixed_buffer =
        dynamic_cast<detail::FixedCapacityResizableBuffer*>(buffer.get());
    if (fixed_buffer == nullptr ||
        fixed_buffer->parent().get() != buffer_.get()) {
      return ::arrow::Status::Invalid(
          "Buffer does not belong to this PageBufferArena");
    }
    if (fixed_buffer->end_offset() != next_offset_) {
      return ::arrow::Status::Invalid(
          "Only the last PageBufferArena allocation can be committed");
    }
    const auto status = fixed_buffer->ShrinkCapacity(buffer->size());
    if (!status.ok()) {
      return status;
    }
    next_offset_ = fixed_buffer->offset() + buffer->size();
    return ::arrow::Status::OK();
  }

 private:
  static constexpr int64_t kAllocationAlignment = 64;

  static int64_t AlignTo(int64_t value, int64_t alignment) {
    return ((value + alignment - 1) / alignment) * alignment;
  }

  MemoryPool* pool_;
  int64_t capacity_;
  std::shared_ptr<ResizableBuffer> buffer_;
  // The arena is only attached to buffered row groups with thread-parallel
  // writes disabled, so callers serialize allocation and reset.
  int64_t next_offset_{0};
};

} // namespace bytedance::bolt::parquet::arrow
