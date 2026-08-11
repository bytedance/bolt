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

#include <arrow/buffer.h>
#include <arrow/memory_pool.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bolt/common/memory/Memory.h"
#include "bolt/shuffle/sparksql/Payload.h"
#include "bolt/shuffle/sparksql/Spill.h"

namespace bytedance::bolt::shuffle::sparksql::test {
namespace {

class CheckingMemoryPool final : public arrow::MemoryPool {
 public:
  static constexpr int64_t kPaddingSize = 64;

  explicit CheckingMemoryPool(
      std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool)
      : pool_(std::move(pool)) {}

  using arrow::MemoryPool::Allocate;
  using arrow::MemoryPool::Free;
  using arrow::MemoryPool::Reallocate;

  arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out)
      override {
    if (size < 0 || size > std::numeric_limits<int64_t>::max() - kPaddingSize) {
      return arrow::Status::Invalid("Invalid allocation size: ", size);
    }

    *out =
        static_cast<uint8_t*>(pool_->allocate(size + kPaddingSize, alignment));
    initializePadding(*out, size);

    std::lock_guard<std::mutex> lock(mutex_);
    if (!allocations_.emplace(*out, Allocation{size, alignment}).second) {
      pool_->free(*out, size + kPaddingSize, alignment);
      *out = nullptr;
      return arrow::Status::Invalid("Duplicate allocation address");
    }
    bytesAllocated_ += size;
    maxMemory_ = std::max(maxMemory_, bytesAllocated_);
    totalBytesAllocated_ += size;
    ++numAllocations_;
    return arrow::Status::OK();
  }

  arrow::Status Reallocate(
      int64_t oldSize,
      int64_t newSize,
      int64_t alignment,
      uint8_t** ptr) override {
    if (ptr == nullptr || *ptr == nullptr) {
      return arrow::Status::Invalid("Cannot reallocate a null allocation");
    }
    if (newSize < 0 ||
        newSize > std::numeric_limits<int64_t>::max() - kPaddingSize) {
      return arrow::Status::Invalid("Invalid reallocation size: ", newSize);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto allocation = allocations_.find(*ptr);
    if (allocation == allocations_.end()) {
      return arrow::Status::Invalid("Cannot reallocate an unknown allocation");
    }
    if (oldSize != allocation->second.size) {
      return arrow::Status::Invalid(
          "Reallocation size mismatch: expected ",
          allocation->second.size,
          ", got ",
          oldSize);
    }
    if (alignment != allocation->second.alignment) {
      return arrow::Status::Invalid(
          "Reallocation alignment mismatch: expected ",
          allocation->second.alignment,
          ", got ",
          alignment);
    }

    checkPaddingLocked(*ptr, allocation->second);
    auto* oldPtr = *ptr;
    *ptr = static_cast<uint8_t*>(pool_->reallocate(
        *ptr, oldSize + kPaddingSize, newSize + kPaddingSize, alignment));

    allocations_.erase(oldPtr);
    initializePadding(*ptr, newSize);
    allocations_.emplace(*ptr, Allocation{newSize, alignment});
    bytesAllocated_ += newSize - oldSize;
    maxMemory_ = std::max(maxMemory_, bytesAllocated_);
    if (newSize > oldSize) {
      totalBytesAllocated_ += newSize - oldSize;
    }
    ++numAllocations_;
    return arrow::Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
    if (buffer == nullptr) {
      return;
    }

    Allocation allocation;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = allocations_.find(buffer);
      if (it == allocations_.end()) {
        recordErrorLocked("Cannot free an unknown allocation");
        return;
      }

      allocation = it->second;
      checkPaddingLocked(buffer, allocation);
      if (size != allocation.size) {
        recordErrorLocked(
            "Free size mismatch: expected " + std::to_string(allocation.size) +
            ", got " + std::to_string(size));
      }
      if (alignment != allocation.alignment) {
        recordErrorLocked(
            "Free alignment mismatch: expected " +
            std::to_string(allocation.alignment) + ", got " +
            std::to_string(alignment));
      }
      allocations_.erase(it);
      bytesAllocated_ -= allocation.size;
    }

    pool_->free(buffer, allocation.size + kPaddingSize, allocation.alignment);
  }

  void ReleaseUnused() override {
    pool_->release();
  }

  int64_t bytes_allocated() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return bytesAllocated_;
  }

  int64_t max_memory() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return maxMemory_;
  }

  int64_t total_bytes_allocated() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return totalBytesAllocated_;
  }

  int64_t num_allocations() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return numAllocations_;
  }

  std::string backend_name() const override {
    return "bolt-checking-memory-pool";
  }

  arrow::Status check() const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [buffer, allocation] : allocations_) {
      checkPaddingLocked(buffer, allocation);
    }
    if (!firstError_.empty()) {
      return arrow::Status::Invalid(firstError_);
    }
    return arrow::Status::OK();
  }

  int64_t numActiveAllocations() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return allocations_.size();
  }

 private:
  struct Allocation {
    int64_t size;
    int64_t alignment;
  };

  static uint8_t paddingByte(int64_t offset) {
    return static_cast<uint8_t>(0xA5U ^ (offset * 0x9DU));
  }

  static void initializePadding(uint8_t* buffer, int64_t size) {
    for (int64_t i = 0; i < kPaddingSize; ++i) {
      buffer[size + i] = paddingByte(i);
    }
  }

  void checkPaddingLocked(const uint8_t* buffer, const Allocation& allocation)
      const {
    for (int64_t i = 0; i < kPaddingSize; ++i) {
      if (buffer[allocation.size + i] != paddingByte(i)) {
        recordErrorLocked(
            "Allocation padding was modified at offset " +
            std::to_string(allocation.size + i));
        return;
      }
    }
  }

  void recordErrorLocked(std::string error) const {
    if (firstError_.empty()) {
      firstError_ = std::move(error);
    }
  }

  std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool_;
  mutable std::mutex mutex_;
  std::unordered_map<uint8_t*, Allocation> allocations_;
  mutable std::string firstError_;
  int64_t bytesAllocated_{0};
  int64_t maxMemory_{0};
  int64_t totalBytesAllocated_{0};
  int64_t numAllocations_{0};
};

class PayloadTest : public testing::Test {
 protected:
  void SetUp() override {
    pool_ = bytedance::bolt::memory::memoryManager()->addLeafPool();
  }

  std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool_;
};

TEST_F(PayloadTest, SpillTracksBytesWritten) {
  Spill spill(Spill::kSequentialSpill, 2, "");
  spill.insertPayload(
      0,
      Payload::Type::kCompressed,
      1,
      nullptr,
      123,
      arrow::default_memory_pool(),
      nullptr);
  spill.insertPayload(1, 1, 456);

  EXPECT_EQ(spill.bytesWritten(), 579);
}

TEST_F(PayloadTest, CheckingMemoryPoolTracksEveryAllocation) {
  CheckingMemoryPool checkingPool(pool_);
  auto first = arrow::AllocateResizableBuffer(37, &checkingPool).ValueOrDie();
  auto second = arrow::AllocateResizableBuffer(129, &checkingPool).ValueOrDie();

  EXPECT_EQ(checkingPool.numActiveAllocations(), 2);
  EXPECT_EQ(
      checkingPool.bytes_allocated(), first->capacity() + second->capacity());
  EXPECT_TRUE(checkingPool.check().ok());

  ASSERT_TRUE(first->Resize(257).ok());
  EXPECT_EQ(checkingPool.numActiveAllocations(), 2);
  EXPECT_EQ(
      checkingPool.bytes_allocated(), first->capacity() + second->capacity());
  EXPECT_TRUE(checkingPool.check().ok());

  second.reset();
  first.reset();
  EXPECT_EQ(checkingPool.numActiveAllocations(), 0);
  EXPECT_EQ(checkingPool.bytes_allocated(), 0);
  EXPECT_EQ(pool_->usedBytes(), 0);
  EXPECT_TRUE(checkingPool.check().ok());
}

TEST_F(PayloadTest, CheckingMemoryPoolPreservesCorruptionDetectedOnFree) {
  CheckingMemoryPool checkingPool(pool_);
  uint8_t* buffer = nullptr;
  constexpr int64_t kSize = 32;
  ASSERT_TRUE(checkingPool.Allocate(kSize, &buffer).ok());

  buffer[kSize + CheckingMemoryPool::kPaddingSize - 1] ^= 1;
  checkingPool.Free(buffer, kSize);

  auto status = checkingPool.check();
  EXPECT_FALSE(status.ok());
  EXPECT_NE(
      status.ToString().find("Allocation padding was modified"),
      std::string::npos);
  EXPECT_EQ(pool_->usedBytes(), 0);
}

TEST_F(PayloadTest, CompressedBufferCapacityExcludesHeader) {
  CodecOptions codecOptions{};
  codecOptions.backend = CodecBackend::NONE;
  auto codec = Codec::create(CodecType::ZSTD, codecOptions);
  std::vector<uint8_t> input(290);
  uint32_t state = 1266;
  for (auto& byte : input) {
    state = state * 1664525U + 1013904223U;
    byte = static_cast<uint8_t>(state >> 24);
  }
  CheckingMemoryPool outputPool(pool_);
  std::vector<bool> isValidityBuffer{false};
  std::vector<std::shared_ptr<arrow::Buffer>> buffers{
      arrow::Buffer::FromVector(std::move(input))};

  auto payload = BlockPayload::fromBuffers(
                     Payload::Type::kCompressed,
                     /*numRows=*/1,
                     std::move(buffers),
                     &isValidityBuffer,
                     &outputPool,
                     codec.get(),
                     Payload::Mode::kBuffer,
                     /*hasComplexType=*/false)
                     .ValueOrDie();
  ASSERT_NE(payload, nullptr);
  auto status = outputPool.check();
  EXPECT_TRUE(status.ok()) << status.ToString();
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::test

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  bytedance::bolt::memory::MemoryManager::initialize({});
  return RUN_ALL_TESTS();
}
