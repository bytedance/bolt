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

#include <cstring>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/memory/MemoryPool.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// A growable byte buffer backed by the engine MemoryPool, so every scratch
/// and stream buffer of the cell shuffle is visible to task accounting and
/// the arbitrator - a plain std::string here would be memory the operator
/// consumes but never reports. Capacity is retained across clear() for
/// reuse and returned to the pool by reset() or destruction.
class PoolBytes {
 public:
  explicit PoolBytes(memory::MemoryPool* pool) : pool_(pool) {}

  PoolBytes(PoolBytes&& other) noexcept
      : pool_(other.pool_),
        data_(other.data_),
        size_(other.size_),
        capacity_(other.capacity_) {
    other.data_ = nullptr;
    other.size_ = 0;
    other.capacity_ = 0;
  }

  PoolBytes(const PoolBytes&) = delete;
  PoolBytes& operator=(const PoolBytes&) = delete;
  PoolBytes& operator=(PoolBytes&&) = delete;

  ~PoolBytes() {
    reset();
  }

  char* data() {
    return data_;
  }

  const char* data() const {
    return data_;
  }

  uint8_t* udata() {
    return reinterpret_cast<uint8_t*>(data_);
  }

  const uint8_t* udata() const {
    return reinterpret_cast<const uint8_t*>(data_);
  }

  size_t size() const {
    return size_;
  }

  bool empty() const {
    return size_ == 0;
  }

  /// Keeps capacity.
  void clear() {
    size_ = 0;
  }

  /// Grows the logical size; existing bytes are preserved, new bytes are
  /// uninitialized.
  void resize(size_t newSize) {
    ensure(newSize);
    size_ = newSize;
  }

  void reserve(size_t newCapacity) {
    ensure(newCapacity);
  }

  void append(const void* src, size_t bytes) {
    ensure(size_ + bytes);
    ::memcpy(data_ + size_, src, bytes);
    size_ += bytes;
  }

  void push_back(char value) {
    append(&value, 1);
  }

  /// Returns the capacity to the pool.
  void reset() {
    if (data_ != nullptr) {
      pool_->free(data_, capacity_);
      data_ = nullptr;
      capacity_ = 0;
    }
    size_ = 0;
  }

 private:
  void ensure(size_t needed) {
    if (needed <= capacity_) {
      return;
    }
    const size_t newCapacity = std::max<size_t>(
        64, bits::nextPowerOfTwo(static_cast<uint64_t>(needed)));
    char* grown = reinterpret_cast<char*>(pool_->allocate(newCapacity));
    if (size_ > 0) {
      ::memcpy(grown, data_, size_);
    }
    if (data_ != nullptr) {
      pool_->free(data_, capacity_);
    }
    data_ = grown;
    capacity_ = newCapacity;
  }

  memory::MemoryPool* const pool_;
  char* data_{nullptr};
  size_t size_{0};
  size_t capacity_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
