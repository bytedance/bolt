#include "bolt/common/memory/bm/SpillWriteCoordinator.h"

#include "bolt/common/memory/bm/BlockMemory.h"
#include "bolt/common/memory/bm/BlockStateMachine.h"
#include "bolt/common/memory/bm/BufferManagerAccounting.h"
#include "bolt/common/memory/bm/io/IoBuffer.h"

#include <cerrno>
#include <future>
#include <memory>
#include <stdexcept>

#include <gtest/gtest.h>

namespace bytedance::bolt::memory::bm {
namespace {

std::shared_ptr<BlockMemory> makeUnpinnedResidentBlock(size_t size) {
  auto block = std::make_shared<BlockMemory>(7, size, MemoryTag::kTesting);
  block->payload = IoBuffer::allocateFromMalloc(size);
  block->pinCount = 1;
  BlockStateMachine::Unpin(*block);
  return block;
}

SpillWriteFuture makeCompletedWrite(size_t size) {
  std::promise<IoResult> promise;
  promise.set_value(IoResult{size});

  SpillWriteMetadata metadata;
  metadata.rawBytes = size;
  metadata.physicalBytes = size;
  return SpillWriteFuture{promise.get_future(), {}, metadata};
}

SpillWriteFuture makeFailedWrite() {
  std::promise<IoResult> promise;
  promise.set_value(IoResult{0, IoErrorCode::BackendIoError, EIO});

  return SpillWriteFuture{promise.get_future(), {}, {}};
}

} // namespace

TEST(SpillWriteCoordinatorTest, SubmitAndHarvestCompletesSpillLifecycle) {
  BufferManagerAccounting accounting;
  auto block = makeUnpinnedResidentBlock(4096);
  accounting.RecordAllocate(*block);
  accounting.OnResidentUnpinned(*block);

  SpillWriteCoordinator coordinator{
      2,
      IoPriority::Medium,
      [](IoBuffer& payload, size_t rawSize, IoPriority priority) {
        EXPECT_TRUE(payload.valid());
        EXPECT_EQ(4096, rawSize);
        EXPECT_EQ(IoPriority::Medium, priority);
        return makeCompletedWrite(rawSize);
      },
      accounting};

  coordinator.Submit(block);

  EXPECT_EQ(BlockMemoryState::kSpilling, block->state);
  EXPECT_FALSE(block->payload.has_value());
  EXPECT_EQ(1, coordinator.pendingCount());

  const auto result = coordinator.HarvestNext();

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(block, result.memory);
  EXPECT_EQ(4096, result.reclaimedBytes);
  EXPECT_EQ(BlockMemoryState::kSpilled, block->state);
  EXPECT_TRUE(block->segment.has_value());
  EXPECT_EQ(0, coordinator.pendingCount());
}

TEST(SpillWriteCoordinatorTest, SubmitFailurePropagatesWithoutRollback) {
  BufferManagerAccounting accounting;
  auto block = makeUnpinnedResidentBlock(4096);
  accounting.RecordAllocate(*block);
  accounting.OnResidentUnpinned(*block);

  SpillWriteCoordinator coordinator{
      2,
      IoPriority::Medium,
      [](IoBuffer&, size_t, IoPriority) -> SpillWriteFuture {
        throw std::runtime_error("submit failed");
      },
      accounting};

  EXPECT_THROW(coordinator.Submit(block), std::runtime_error);

  EXPECT_EQ(BlockMemoryState::kSpilling, block->state);
  EXPECT_FALSE(block->payload.has_value());
  EXPECT_EQ(0, coordinator.pendingCount());
}

TEST(SpillWriteCoordinatorTest, HarvestIoFailurePropagatesWithoutRollback) {
  BufferManagerAccounting accounting;
  auto first = makeUnpinnedResidentBlock(4096);
  auto second = makeUnpinnedResidentBlock(8192);
  accounting.RecordAllocate(*first);
  accounting.RecordAllocate(*second);
  accounting.OnResidentUnpinned(*first);
  accounting.OnResidentUnpinned(*second);

  size_t submitCount = 0;
  SpillWriteCoordinator coordinator{
      2,
      IoPriority::Medium,
      [&submitCount](IoBuffer&, size_t rawSize, IoPriority) {
        ++submitCount;
        if (submitCount == 1) {
          return makeFailedWrite();
        }
        return makeCompletedWrite(rawSize);
      },
      accounting};

  coordinator.Submit(first);
  coordinator.Submit(second);

  const auto result = coordinator.HarvestNext();

  ASSERT_FALSE(result.ok());
  EXPECT_EQ(first, result.memory);
  EXPECT_EQ(BlockMemoryState::kSpilling, first->state);
  EXPECT_EQ(BlockMemoryState::kSpilling, second->state);
  EXPECT_FALSE(first->payload.has_value());
  EXPECT_FALSE(second->payload.has_value());
  EXPECT_EQ(1, coordinator.pendingCount());
}

} // namespace bytedance::bolt::memory::bm
