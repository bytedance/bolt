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

#include <gtest/gtest.h>
#include <memory>

#include "bolt/common/memory/sparksql/AllocationListener.h"

#define private public
#include "bolt/common/memory/sparksql/MemoryAllocator.h"
#undef private

namespace bytedance::bolt::memory::sparksql {

namespace {

class NoopAllocationListener final : public AllocationListener {
 public:
  int64_t allocationChanged(int64_t size) override {
    usedBytes_ += size;
    return size;
  }

  int64_t getUsedBytes() override {
    return usedBytes_;
  }

 private:
  int64_t usedBytes_{0};
};

std::string emptyStackReport() {
  return "******************************\n"
         "******************************\n";
}

} // namespace

TEST(MemoryAllocatorTest, reallocateToZeroRecordsFree) {
  MemoryAllocatorPtr delegated = std::make_shared<StdMemoryAllocator>();
  AllocationListenerPtr listener = std::make_shared<NoopAllocationListener>();
  ListenableMemoryAllocator allocator(delegated, listener, ".*", 0, 1);

  void* out = nullptr;
  ASSERT_TRUE(allocator.allocate(128, &out));
  ASSERT_NE(nullptr, out);
  EXPECT_NE(emptyStackReport(), allocator.boltListener_->getAllStack());

  void* reallocated = reinterpret_cast<void*>(0x1);
  ASSERT_TRUE(allocator.reallocate(out, 128, 0, &reallocated));
  EXPECT_EQ(0, allocator.getBytes());
  EXPECT_EQ(0, listener->getUsedBytes());
  EXPECT_EQ(emptyStackReport(), allocator.boltListener_->getAllStack());
}

TEST(MemoryAllocatorTest, reallocateAlignedToZeroRecordsFree) {
  MemoryAllocatorPtr delegated = std::make_shared<StdMemoryAllocator>();
  AllocationListenerPtr listener = std::make_shared<NoopAllocationListener>();
  ListenableMemoryAllocator allocator(delegated, listener, ".*", 0, 1);

  void* out = nullptr;
  ASSERT_TRUE(allocator.allocateAligned(64, 128, &out));
  ASSERT_NE(nullptr, out);
  EXPECT_NE(emptyStackReport(), allocator.boltListener_->getAllStack());

  void* reallocated = reinterpret_cast<void*>(0x1);
  ASSERT_TRUE(allocator.reallocateAligned(out, 64, 128, 0, &reallocated));
  EXPECT_EQ(nullptr, reallocated);
  EXPECT_EQ(0, allocator.getBytes());
  EXPECT_EQ(0, listener->getUsedBytes());
  EXPECT_EQ(emptyStackReport(), allocator.boltListener_->getAllStack());
}

} // namespace bytedance::bolt::memory::sparksql
