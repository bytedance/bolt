#include "bolt/common/memory/bm/SpillWriteCoordinator.h"

#include "bolt/common/memory/bm/BlockMemory.h"
#include "bolt/common/memory/bm/BlockStateMachine.h"
#include "bolt/common/memory/bm/BufferManagerAccounting.h"
#include "bolt/common/memory/bm/EvictionQueue.h"
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
  EvictionQueue evictionQueue;
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
      accounting,
      evictionQueue};

  coordinator.Submit(block);

  EXPECT_EQ(BlockMemoryState::kSpilling, block->state);
  EXPECT_FALSE(block->payload.has_value());
  EXPECT_EQ(1, coordinator.pendingCount());

  const auto result = coordinator.HarvestNext();

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(block, result.memory);
  EXPECT_EQ(4096, result.reclaimedBytes);
  EXPECT_EQ(BlockMemoryState::kSpilled, block->state);
  EXPECT_TRUE(block->extent.has_value());
  EXPECT_EQ(0, coordinator.pendingCount());
}

TEST(SpillWriteCoordinatorTest, SubmitFailureRollsBackAndRequeuesBlock) {
  BufferManagerAccounting accounting;
  EvictionQueue evictionQueue;
  auto block = makeUnpinnedResidentBlock(4096);
  accounting.RecordAllocate(*block);
  accounting.OnResidentUnpinned(*block);

  SpillWriteCoordinator coordinator{
      2,
      IoPriority::Medium,
      [](IoBuffer&, size_t, IoPriority) -> SpillWriteFuture {
        throw std::runtime_error("submit failed");
      },
      accounting,
      evictionQueue};

  EXPECT_THROW(coordinator.Submit(block), std::runtime_error);

  EXPECT_EQ(BlockMemoryState::kInMemory, block->state);
  ASSERT_TRUE(block->payload.has_value());
  EXPECT_TRUE(block->payload->valid());
  EXPECT_EQ(block, evictionQueue.PopEvictable());
  EXPECT_EQ(0, coordinator.pendingCount());
}

TEST(SpillWriteCoordinatorTest, HarvestFailureRollsBackCurrentAndPending) {
  BufferManagerAccounting accounting;
  EvictionQueue evictionQueue;
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
      accounting,
      evictionQueue};

  coordinator.Submit(first);
  coordinator.Submit(second);

  const auto result = coordinator.HarvestNext();

  ASSERT_FALSE(result.ok());
  EXPECT_EQ(first, result.memory);
  EXPECT_EQ(BlockMemoryState::kInMemory, first->state);
  EXPECT_EQ(BlockMemoryState::kInMemory, second->state);
  ASSERT_TRUE(first->payload.has_value());
  ASSERT_TRUE(second->payload.has_value());
  EXPECT_TRUE(first->payload->valid());
  EXPECT_TRUE(second->payload->valid());
  EXPECT_EQ(0, coordinator.pendingCount());
  EXPECT_EQ(first, evictionQueue.PopEvictable());
  EXPECT_EQ(second, evictionQueue.PopEvictable());
}

} // namespace bytedance::bolt::memory::bm
