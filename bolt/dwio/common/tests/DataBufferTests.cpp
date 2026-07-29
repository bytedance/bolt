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

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/dwio/common/BufferUtil.h"
#include "bolt/dwio/common/DataBuffer.h"
namespace bytedance {
namespace bolt {
namespace dwio {
namespace common {
using namespace bytedance::bolt::memory;
using namespace testing;
using MemoryPool = bytedance::bolt::memory::MemoryPool;

class DataBufferTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    MemoryManager::testingSetInstance(MemoryManager::Options{});
  }

  const std::shared_ptr<MemoryPool> pool_ = memoryManager()->addLeafPool();
};

TEST_F(DataBufferTest, ZeroOut) {
  const uint8_t VALUE = 13;
  DataBuffer<uint8_t> buffer(*pool_, 16);
  for (auto i = 0; i < buffer.size(); i++) {
    auto data = buffer.data();
    ASSERT_EQ(data[i], 0);
    data[i] = VALUE;
  }

  buffer.resize(8);
  for (auto i = 0; i < buffer.size(); i++) {
    auto data = buffer.data();
    ASSERT_EQ(data[i], VALUE);
  }

  auto currentSize = buffer.size();
  ASSERT_EQ(currentSize, 8);
  buffer.resize(32);
  for (auto i = 0; i < buffer.size(); i++) {
    auto data = buffer.data();
    if (i < currentSize) {
      ASSERT_EQ(data[i], VALUE);
    } else {
      ASSERT_EQ(data[i], 0);
    }
  }
}

TEST_F(DataBufferTest, At) {
  DataBuffer<uint8_t> buffer{*pool_};
  for (auto i = 0; i != 15; ++i) {
    buffer.append(i);
  }
  ASSERT_EQ(15, buffer.size());

  for (auto i = 0; i != 15; ++i) {
    EXPECT_EQ(i, buffer.at(i));
  }

  buffer.resize(8);
  for (auto i = 0; i != 8; ++i) {
    EXPECT_EQ(i, buffer.at(i));
  }
  for (auto i = 8; i != 42; ++i) {
    BOLT_ASSERT_THROW(buffer.at(i), "Accessing index out of range");
  }
}

TEST_F(DataBufferTest, Reset) {
  DataBuffer<uint8_t> buffer{*pool_};
  buffer.reserve(16);
  for (auto i = 0; i != 15; ++i) {
    buffer.append(i);
  }
  ASSERT_EQ(15, buffer.size());
  ASSERT_EQ(16, buffer.capacity());
  {
    buffer.clear();
    EXPECT_EQ(0, buffer.size());
    EXPECT_EQ(0, buffer.capacity());

    buffer.reserve(12);
    EXPECT_EQ(0, buffer.size());
    EXPECT_EQ(12, buffer.capacity());
    for (auto i = 0; i != 11; ++i) {
      buffer.append(i);
    }
    EXPECT_EQ(11, buffer.size());
    EXPECT_EQ(12, buffer.capacity());
  }

  {
    buffer.clear();
    EXPECT_EQ(0, buffer.size());
    EXPECT_EQ(0, buffer.capacity());

    buffer.reserve(16);
    EXPECT_EQ(0, buffer.size());
    EXPECT_EQ(16, buffer.capacity());
    for (auto i = 0; i != 15; ++i) {
      buffer.append(i);
    }
    EXPECT_EQ(15, buffer.size());
    EXPECT_EQ(16, buffer.capacity());
  }

  {
    buffer.clear();
    EXPECT_EQ(0, buffer.size());
    EXPECT_EQ(0, buffer.capacity());

    buffer.reserve(32);
    EXPECT_EQ(0, buffer.size());
    EXPECT_EQ(32, buffer.capacity());
    for (auto i = 0; i != 31; ++i) {
      buffer.append(i);
    }
    EXPECT_EQ(31, buffer.size());
    EXPECT_EQ(32, buffer.capacity());
  }
}

TEST_F(DataBufferTest, Wrap) {
  auto size = 26;
  auto buffer = bolt::AlignedBuffer::allocate<char>(size, pool_.get());
  auto raw = buffer->asMutable<char>();
  for (size_t i = 0; i < size; ++i) {
    raw[i] = 'a' + i;
  }
  auto dataBuffer = DataBuffer<char>::wrap(buffer);
  buffer = nullptr;
  ASSERT_EQ(size, dataBuffer->size());
  ASSERT_EQ(size, dataBuffer->capacity());
  for (size_t i = 0; i < size; ++i) {
    ASSERT_EQ((*dataBuffer)[i], 'a' + i);
  }
}

TEST_F(DataBufferTest, Move) {
  {
    DataBuffer<uint8_t> buffer{*pool_};
    buffer.reserve(16);
    for (auto i = 0; i != 15; ++i) {
      buffer.append(i);
    }
    ASSERT_EQ(15, buffer.size());
    ASSERT_EQ(16, buffer.capacity());
    const auto usedBytes = pool_->currentBytes();

    // Expect no double freeing from memory pool.
    DataBuffer<uint8_t> newBuffer{std::move(buffer)};
    ASSERT_EQ(15, newBuffer.size());
    ASSERT_EQ(16, newBuffer.capacity());
    ASSERT_EQ(usedBytes, pool_->currentBytes());
  }
  ASSERT_EQ(0, pool_->currentBytes());
}

TEST_F(DataBufferTest, EnsureCapacityReleasesOldBufferBeforeAllocatingNewOne) {
  constexpr size_t kInitialSize = 4 << 20;
  const auto oldAllocationBytes =
      pool_->preferredSize(kInitialSize + AlignedBuffer::kPaddedSize);
  const auto oldCapacity = oldAllocationBytes - AlignedBuffer::kPaddedSize;
  const auto requestedCapacity = oldCapacity + 1;
  const auto newAllocationBytes =
      pool_->preferredSize(requestedCapacity + AlignedBuffer::kPaddedSize);

  auto rootPool = memoryManager()->addRootPool(
      "ensureCapacityReleaseBeforeAlloc", newAllocationBytes);
  auto leafPool = rootPool->addLeafChild("leaf");
  BufferPtr buffer =
      AlignedBuffer::allocate<char>(kInitialSize, leafPool.get());
  ASSERT_EQ(buffer->capacity(), oldCapacity);

  dwio::common::ensureCapacity<char>(buffer, requestedCapacity, leafPool.get());

  EXPECT_GE(buffer->capacity(), requestedCapacity);
  EXPECT_LE(leafPool->currentBytes(), newAllocationBytes);
}
} // namespace common
} // namespace dwio
} // namespace bolt
} // namespace bytedance
