#include "bolt/common/memory/bm/ReclaimWriteWindow.h"

#include "bolt/common/memory/bm/BlockMemory.h"
#include "bolt/common/memory/bm/BlockStateMachine.h"
#include "bolt/common/memory/bm/BufferManagerStats.h"
#include "bolt/common/memory/bm/SpillCandidateProvider.h"
#include "bolt/common/memory/bm/SpillWriteDriver.h"
#include "bolt/common/memory/bm/io/IoBuffer.h"

#include <cerrno>
#include <future>
#include <memory>
#include <stdexcept>
#include <vector>

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

TEST(ReclaimWriteWindowTest, SubmitAndHarvestCompletesSpillLifecycle) {
  BufferManagerStatsCollector accounting;
  auto block = makeUnpinnedResidentBlock(4096);
  accounting.RecordAllocate(*block);
  accounting.OnResidentUnpinned(*block);

  ReclaimWriteWindow window{
      2,
      IoPriority::Medium,
      [](IoBuffer& payload, size_t rawSize, IoPriority priority) {
        EXPECT_TRUE(payload.valid());
        EXPECT_EQ(4096, rawSize);
        EXPECT_EQ(IoPriority::Medium, priority);
        return makeCompletedWrite(rawSize);
      },
      accounting};

  window.Submit(block);

  EXPECT_EQ(BlockMemoryState::kSpilling, block->state);
  EXPECT_FALSE(block->payload.has_value());
  EXPECT_EQ(1, window.pendingCount());

  const auto result = window.HarvestNext();

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(block, result.memory);
  EXPECT_EQ(4096, result.reclaimedBytes);
  EXPECT_EQ(BlockMemoryState::kSpilled, block->state);
  EXPECT_TRUE(block->segment.has_value());
  EXPECT_EQ(0, window.pendingCount());
}

TEST(ReclaimWriteWindowTest, SubmitFailurePropagatesWithoutRollback) {
  BufferManagerStatsCollector accounting;
  auto block = makeUnpinnedResidentBlock(4096);
  accounting.RecordAllocate(*block);
  accounting.OnResidentUnpinned(*block);

  ReclaimWriteWindow window{
      2,
      IoPriority::Medium,
      [](IoBuffer&, size_t, IoPriority) -> SpillWriteFuture {
        throw std::runtime_error("submit failed");
      },
      accounting};

  EXPECT_THROW(window.Submit(block), std::runtime_error);

  EXPECT_EQ(BlockMemoryState::kSpilling, block->state);
  EXPECT_FALSE(block->payload.has_value());
  EXPECT_EQ(0, window.pendingCount());
}

TEST(ReclaimWriteWindowTest, HarvestIoFailurePropagatesWithoutRollback) {
  BufferManagerStatsCollector accounting;
  auto first = makeUnpinnedResidentBlock(4096);
  auto second = makeUnpinnedResidentBlock(8192);
  accounting.RecordAllocate(*first);
  accounting.RecordAllocate(*second);
  accounting.OnResidentUnpinned(*first);
  accounting.OnResidentUnpinned(*second);

  size_t submitCount = 0;
  ReclaimWriteWindow window{
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

  window.Submit(first);
  window.Submit(second);

  const auto result = window.HarvestNext();

  ASSERT_FALSE(result.ok());
  EXPECT_EQ(first, result.memory);
  EXPECT_EQ(BlockMemoryState::kSpilling, first->state);
  EXPECT_EQ(BlockMemoryState::kSpilling, second->state);
  EXPECT_FALSE(first->payload.has_value());
  EXPECT_FALSE(second->payload.has_value());
  EXPECT_EQ(1, window.pendingCount());
}

TEST(SpillCandidateProviderTest, SkipsPinnedAndReturnsUnpinnedResidentBlocks) {
  auto pinned = std::make_shared<BlockMemory>(1, 4096, MemoryTag::kTesting);
  pinned->payload = IoBuffer::allocateFromMalloc(4096);
  pinned->pinCount = 1;
  auto pinnedHandle = std::make_shared<BlockHandle>(pinned);

  auto unpinned = makeUnpinnedResidentBlock(8192);
  auto unpinnedHandle = std::make_shared<BlockHandle>(unpinned);

  std::vector<std::shared_ptr<BlockHandle>> blocks{
      pinnedHandle, nullptr, unpinnedHandle};
  auto provider = MakeBlockHandleSpillCandidateProvider(blocks);

  EXPECT_EQ(unpinned, provider());
  EXPECT_EQ(nullptr, provider());
}

TEST(SpillWriteDriverTest, SpillsCandidatesWithFakeWrites) {
  BufferManagerStatsCollector accounting;
  auto first = makeUnpinnedResidentBlock(4096);
  auto second = makeUnpinnedResidentBlock(8192);
  accounting.RecordAllocate(*first);
  accounting.RecordAllocate(*second);
  accounting.OnResidentUnpinned(*first);
  accounting.OnResidentUnpinned(*second);

  std::vector<std::shared_ptr<BlockMemory>> candidates{first, second};
  size_t index = 0;
  size_t submitCount = 0;

  SpillWriteDriver driver{
      1,
      IoPriority::Medium,
      [&submitCount](IoBuffer& payload, size_t rawSize, IoPriority priority) {
        EXPECT_TRUE(payload.valid());
        EXPECT_EQ(IoPriority::Medium, priority);
        ++submitCount;
        return makeCompletedWrite(rawSize);
      },
      accounting};

  const auto reclaimed = driver.Spill(0, [&]() -> std::shared_ptr<BlockMemory> {
    if (index == candidates.size()) {
      return nullptr;
    }
    return candidates[index++];
  });

  EXPECT_EQ(2, submitCount);
  EXPECT_EQ(4096 + 8192, reclaimed);
  EXPECT_EQ(BlockMemoryState::kSpilled, first->state);
  EXPECT_EQ(BlockMemoryState::kSpilled, second->state);

  const auto stats = accounting.stats();
  EXPECT_EQ(2, stats.reclaimAttemptedBlocks);
  EXPECT_EQ(2, stats.spillWriteCount);
  EXPECT_EQ(4096 + 8192, stats.reclaimedBytes);
}

} // namespace bytedance::bolt::memory::bm
