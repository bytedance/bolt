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

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BlockStateMachine.h"
#include "bolt/common/memory/bm/compress/CompressionManager.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"

#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <future>
#include <string>

#include <fmt/format.h>
#include <gtest/gtest.h>

#define private public
#include "bolt/common/memory/bm/BlockHandle.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/BufferManagerStats.h"
#undef private

namespace bytedance::bolt::memory::bm {
namespace {

constexpr uint64_t kMaxMemory = 64 * 1024 * 1024;

class BufferManagerPrivateStateTest : public testing::Test {
 protected:
  void SetUp() override {
    root_ = manager_.addRootPool(
        fmt::format(
            "bm-private-state-root-{}",
            testing::UnitTest::GetInstance()->current_test_info()->name()),
        kMaxMemory,
        MemoryReclaimer::create());
  }

  std::shared_ptr<BufferManager> makeBufferManager(const std::string& name) {
    const auto directory =
        test::UniqueTempDir(fmt::format("bolt-bm-private-state-{}", name));
    std::filesystem::remove_all(directory);

    BufferManagerConfig config;
    config.poolName = fmt::format("bm-private-state-{}", name);
    config.spillStoreConfig.fileAllocatorConfig =
        test::ValidConfigWithDirectory(directory);
    return BufferManager::Create(*root_, std::move(config));
  }

  MemoryManager manager_;
  std::shared_ptr<MemoryPool> root_;
};

bool IsIoUringUnavailable(const std::exception& e) {
  return std::string(e.what()).find("io_uring_queue_init failed") !=
      std::string::npos;
}

} // namespace

TEST_F(BufferManagerPrivateStateTest, PinRejectsSpillingAndUnknownStates) {
  auto bm = makeBufferManager("pin-state");
  auto block = testingCreateBlockHandle(4096, MemoryTag::kTesting);

  block->memory_->state = BlockMemoryState::kSpilling;
  EXPECT_FALSE(bm->IsSpilled(block));
  EXPECT_THROW((void)bm->Pin(block), std::exception);

  block->memory_->state = static_cast<BlockMemoryState>(99);
  EXPECT_FALSE(bm->IsSpilled(block));
  EXPECT_THROW((void)bm->Pin(block), std::exception);
}

TEST_F(BufferManagerPrivateStateTest, IsSpilledTracksReclaimAndPrefetch) {
  auto bm = makeBufferManager("is-spilled");
  auto handle = bm->Allocate(4096, MemoryTag::kTesting);
  auto block = handle.block();
  handle.Ptr()[0] = 42;
  EXPECT_FALSE(bm->IsSpilled(block));
  handle = BufferHandle{};
  EXPECT_FALSE(bm->IsSpilled(block));
  try {
    ASSERT_EQ(4096, bm->Reclaim(4096));
  } catch (const std::exception& e) {
    if (IsIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }
  EXPECT_TRUE(bm->IsSpilled(block));
  std::array<std::shared_ptr<BlockHandle>, 1> blocks{block};
  bm->Prefetch(blocks);
  EXPECT_FALSE(bm->IsSpilled(block));
  handle = bm->Pin(block);
  EXPECT_EQ(42, handle.Ptr()[0]);
  EXPECT_FALSE(bm->IsSpilled(block));
  handle = BufferHandle{};
  EXPECT_FALSE(bm->IsSpilled(block));
  EXPECT_EQ(4096, bm->DiscardCleanResidentBlocks(blocks));
  EXPECT_TRUE(bm->IsSpilled(block));
}

TEST_F(
    BufferManagerPrivateStateTest,
    ExplicitPathDoesNotCreateDefaultLeafAndOldPathReusesIt) {
  auto bm = makeBufferManager("default-leaf-lifecycle");
  EXPECT_EQ(MemoryPool::Kind::kAggregate, bm->aggregatePool_->kind());
  EXPECT_EQ(0, bm->aggregatePool_->getChildCount());

  auto operatorPool = bm->addOperatorPool("explicit-operator");
  EXPECT_EQ(1, bm->aggregatePool_->getChildCount());
  {
    auto handle = bm->Allocate(4096, MemoryTag::kTesting, operatorPool);
    EXPECT_EQ(1, bm->aggregatePool_->getChildCount());
  }

  auto firstOld = bm->Allocate(4096, MemoryTag::kTesting);
  auto defaultPool = bm->defaultAllocationPool();
  EXPECT_EQ(2, bm->aggregatePool_->getChildCount());
  auto secondOld = bm->Allocate(4096, MemoryTag::kTesting);
  EXPECT_EQ(defaultPool, bm->defaultAllocationPool());
  EXPECT_EQ(2, bm->aggregatePool_->getChildCount());
  EXPECT_EQ(2 * 4096, defaultPool->usedBytes());
}

TEST_F(
    BufferManagerPrivateStateTest,
    AggregateReclaimerReleasesReservationsAcrossLeaves) {
  auto bm = makeBufferManager("aggregate-reclaim-cross-leaf");
  auto firstPool = bm->addOperatorPool("first-operator");
  auto secondPool = bm->addOperatorPool("second-operator");
  std::shared_ptr<BlockHandle> firstBlock;
  std::shared_ptr<BlockHandle> secondBlock;

  {
    auto first = bm->Allocate(4096, MemoryTag::kTesting, firstPool);
    auto second = bm->Allocate(4096, MemoryTag::kTesting, secondPool);
    firstBlock = first.block();
    secondBlock = second.block();
    std::memset(first.Ptr(), 1, first.block()->size());
    std::memset(second.Ptr(), 2, second.block()->size());
  }
  ASSERT_NE(nullptr, firstBlock);
  ASSERT_NE(nullptr, secondBlock);

  const auto before = bm->aggregatePool_->reservedBytes();
  ASSERT_GT(before, 0);

  MemoryReclaimer::Stats stats;
  uint64_t reclaimed = 0;
  try {
    ScopedMemoryArbitrationContext arbitrationContext(bm->aggregatePool_.get());
    reclaimed = bm->aggregatePool_->reclaim(0, 0, stats);
  } catch (const std::exception& e) {
    if (IsIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }

  EXPECT_EQ(before, reclaimed);
  EXPECT_EQ(reclaimed, stats.reclaimedBytes);
  EXPECT_EQ(0, bm->aggregatePool_->reservedBytes());
  EXPECT_EQ(0, firstPool->reservedBytes());
  EXPECT_EQ(0, secondPool->reservedBytes());
  EXPECT_EQ(8192, bm->stats().spilledBytes);
}

TEST_F(BufferManagerPrivateStateTest, PrefetchSubmitFailurePropagates) {
  auto bm = makeBufferManager("prefetch-failure");
  auto block = testingCreateBlockHandle(4096, MemoryTag::kTesting);
  block->memory_->state = BlockMemoryState::kSpilled;

  std::array<std::shared_ptr<BlockHandle>, 1> blocks{block};
  EXPECT_THROW(bm->Prefetch(blocks), std::exception);

  EXPECT_EQ(0, bm->stats().prefetchSubmitFailures);
}

TEST_F(BufferManagerPrivateStateTest, PinPrefetchingReportsReadFailure) {
  auto bm = makeBufferManager("prefetch-read-failure");
  auto block = testingCreateBlockHandle(4096, MemoryTag::kTesting);

  IoResult failedRead;
  failedRead.error = IoErrorCode::BackendIoError;
  failedRead.nativeErrorCode = EIO;
  std::promise<IoResult> promise;
  promise.set_value(std::move(failedRead));
  block->memory_->state = BlockMemoryState::kPrefetching;
  block->memory_->allocationPool = bm->defaultAllocationPool();
  block->memory_->prefetchFuture = SpillReadFuture{
      promise.get_future(),
      std::make_shared<compress::CompressionManager>(
          bm->config_.spillStoreConfig.compressionConfig),
      block->memory_->allocationPool.get(),
      block->size()};
  bm->accounting_->OnReadSubmitted(*block->memory_);

  EXPECT_THROW((void)bm->Pin(block), std::exception);
  EXPECT_EQ(BlockMemoryState::kSpilled, block->memory_->state);
  EXPECT_EQ(1, bm->stats().readIoFailures);
}

TEST_F(BufferManagerPrivateStateTest, BatchPinSubmitsReadsForSpilledBlocks) {
  auto bm = makeBufferManager("batch-pin-spilled");
  std::shared_ptr<BlockHandle> first;
  std::shared_ptr<BlockHandle> second;
  {
    auto firstHandle = bm->Allocate(4096, MemoryTag::kTesting);
    auto secondHandle = bm->Allocate(4096, MemoryTag::kTesting);
    first = firstHandle.block();
    second = secondHandle.block();
    std::memset(firstHandle.Ptr(), 1, first->size());
    std::memset(secondHandle.Ptr(), 2, second->size());
  }

  try {
    ASSERT_EQ(8192, bm->Reclaim(0));
  } catch (const std::exception& e) {
    if (IsIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }

  std::array<std::shared_ptr<BlockHandle>, 2> blocks{first, second};
  auto handles = bm->BatchPin(blocks);

  ASSERT_EQ(2, handles.size());
  EXPECT_EQ(1, handles[0].Ptr()[0]);
  EXPECT_EQ(2, handles[1].Ptr()[0]);
}

} // namespace bytedance::bolt::memory::bm
