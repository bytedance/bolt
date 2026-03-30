/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>

#include "bolt/common/memory/MemoryAllocationListener.h"
#include "bolt/common/memory/sparksql/AllocationListener.h"

#define BOLT_ROUND_TO_LINE(n, round) (((n) + (round)-1) & ~((round)-1))
namespace bytedance::bolt::memory::sparksql {

class MemoryAllocator;
using MemoryAllocatorPtr = std::shared_ptr<MemoryAllocator>;

class MemoryAllocator {
 public:
  enum MemoryAllocatorType { kDefault, kListenable, kStd, kHbm };

  virtual ~MemoryAllocator() = default;

  virtual bool allocate(int64_t size, void** out) = 0;
  virtual bool allocateZeroFilled(int64_t nmemb, int64_t size, void** out) = 0;
  virtual bool
  allocateAligned(uint64_t alignment, int64_t size, void** out) = 0;

  virtual bool
  reallocate(void* p, int64_t size, int64_t newSize, void** out) = 0;
  virtual bool reallocateAligned(
      void* p,
      uint64_t alignment,
      int64_t size,
      int64_t newSize,
      void** out) = 0;

  virtual bool free(void* p, int64_t size) = 0;

  virtual int64_t getBytes() const = 0;
};

class StdMemoryAllocator final : public MemoryAllocator {
 public:
  bool allocate(int64_t size, void** out) override {
    *out = std::malloc(size);
    bytes_ += size;
    return true;
  }

  bool allocateZeroFilled(int64_t nmemb, int64_t size, void** out) override {
    *out = std::calloc(nmemb, size);
    bytes_ += size;
    return true;
  }

  bool allocateAligned(uint64_t alignment, int64_t size, void** out) override {
    *out = nullptr;
    if (size == 0) {
      return true;
    }
    if (alignment < sizeof(void*) || alignment % sizeof(void*) != 0 ||
        (alignment & (alignment - 1)) != 0) {
      return false;
    }
    if (posix_memalign(out, alignment, size) != 0) {
      return false;
    }
    bytes_ += size;
    return true;
  }

  bool reallocate(void* p, int64_t size, int64_t newSize, void** out) override {
    *out = std::realloc(p, newSize);
    bytes_ += (newSize - size);
    return true;
  }

  bool reallocateAligned(
      void* p,

      uint64_t alignment,
      int64_t size,
      int64_t newSize,
      void** out) override {
    *out = nullptr;

    if (alignment < sizeof(void*) || alignment % sizeof(void*) != 0 ||
        (alignment & (alignment - 1)) != 0) {
      return false;
    }

    if (newSize == 0) {
      if (p != nullptr) {
        ::free(p);
      }
      bytes_ -= size;
      return true;
    }

    auto roundedNewSize = BOLT_ROUND_TO_LINE(newSize, alignment);

    void* reallocatedP = nullptr;
    if (posix_memalign(&reallocatedP, alignment, roundedNewSize) != 0) {
      return false;
    }

    if (p != nullptr) {
      std::memcpy(reallocatedP, p, std::min<int64_t>(size, newSize));
      ::free(p);
    }

    *out = reallocatedP;
    bytes_ += (newSize - size);
    return true;
  }

  bool free(void* p, int64_t size) override {
    std::free(p);
    bytes_ -= size;
    return true;
  }

  int64_t getBytes() const override {
    return bytes_;
  }

 private:
  std::atomic_int64_t bytes_{0};
};

class ListenableMemoryAllocator final : public MemoryAllocator {
 public:
  explicit ListenableMemoryAllocator(
      MemoryAllocatorPtr& delegated,
      AllocationListenerPtr& listener,
      const std::string& poolRegex,
      const int64_t singleAllocationThreshold,
      const int64_t accumulativeAllocationThreshold)
      : delegated_(delegated), listener_(listener) {
    boltListener_ =
        std::make_unique<bytedance::bolt::memory::MemoryAllocationListener>(
            "arrow",
            nullptr,
            poolRegex,
            singleAllocationThreshold,
            accumulativeAllocationThreshold);
  }

  bool allocate(int64_t size, void** out) override {
    listener_->allocationChanged(size);
    bool succeed = delegated_->allocate(size, out);
    if (!succeed) {
      listener_->allocationChanged(-size);
    }
    if (succeed) {
      boltListener_->recordAlloc(*out, size);
      bytes_ += size;
    }
    return succeed;
  }

  bool allocateZeroFilled(int64_t nmemb, int64_t size, void** out) override {
    listener_->allocationChanged(size * nmemb);
    bool succeed = delegated_->allocateZeroFilled(nmemb, size, out);
    if (!succeed) {
      listener_->allocationChanged(-size * nmemb);
    }
    if (succeed) {
      boltListener_->recordAlloc(*out, size * nmemb);
      bytes_ += size * nmemb;
    }
    return succeed;
  }

  bool allocateAligned(uint64_t alignment, int64_t size, void** out) override {
    listener_->allocationChanged(size);
    bool succeed = delegated_->allocateAligned(alignment, size, out);
    if (!succeed) {
      listener_->allocationChanged(-size);
    }
    if (succeed) {
      boltListener_->recordAlloc(*out, size);
      bytes_ += size;
    }
    return succeed;
  }

  bool reallocate(void* p, int64_t size, int64_t newSize, void** out) override {
    int64_t diff = newSize - size;
    listener_->allocationChanged(diff);
    bool succeed = delegated_->reallocate(p, size, newSize, out);
    if (!succeed) {
      listener_->allocationChanged(-diff);
    }
    if (succeed) {
      if (newSize == 0 || *out == nullptr) {
        boltListener_->recordFree(p, size);
      } else {
        boltListener_->recordGrow(p, *out, size, newSize);
      }
      bytes_ += diff;
    }
    return succeed;
  }

  bool reallocateAligned(
      void* p,
      uint64_t alignment,
      int64_t size,
      int64_t newSize,
      void** out) override {
    int64_t diff = newSize - size;
    listener_->allocationChanged(diff);
    bool succeed =
        delegated_->reallocateAligned(p, alignment, size, newSize, out);
    if (!succeed) {
      listener_->allocationChanged(-diff);
    }
    if (succeed) {
      if (newSize == 0 || *out == nullptr) {
        boltListener_->recordFree(p, size);
      } else {
        boltListener_->recordGrow(p, *out, size, newSize);
      }
      bytes_ += diff;
    }
    return succeed;
  }

  bool free(void* p, int64_t size) override {
    listener_->allocationChanged(-size);
    bool succeed = delegated_->free(p, size);
    if (!succeed) {
      listener_->allocationChanged(size);
    }
    if (succeed) {
      boltListener_->recordFree(p, size);
      bytes_ -= size;
    }
    return succeed;
  }

  int64_t getBytes() const override {
    return bytes_;
  }

 private:
  MemoryAllocatorPtr delegated_;
  AllocationListenerPtr listener_;
  std::unique_ptr<bytedance::bolt::memory::MemoryAllocationListener>
      boltListener_;
  std::atomic_int64_t bytes_{0};
};

class DefaultMemoryAllocatorGetter final {
 public:
  static MemoryAllocatorPtr defaultMemoryAllocator() {
    static MemoryAllocatorPtr alloc = std::make_shared<StdMemoryAllocator>();
    return alloc;
  }
};

} // namespace bytedance::bolt::memory::sparksql
