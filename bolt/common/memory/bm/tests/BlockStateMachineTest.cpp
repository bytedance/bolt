#include "bolt/common/memory/bm/BlockMemory.h"
#include "bolt/common/memory/bm/BlockStateMachine.h"
#include "bolt/common/memory/bm/compress/CompressionManager.h"
#include "bolt/common/memory/Memory.h"

#include <cerrno>
#include <cstring>
#include <future>
#include <memory>

#include <gtest/gtest.h>

namespace bytedance::bolt::memory::bm {
namespace {

IoBuffer MakePayload(size_t size, char value) {
  auto payload = IoBuffer::allocateFromMalloc(size);
  std::memset(payload.data(), value, payload.length());
  return payload;
}

class BlockStateMachineTest : public testing::Test {
 protected:
  void SetUp() override {
    root_ = manager_.addRootPool(
        testing::UnitTest::GetInstance()->current_test_info()->name(),
        1024 * 1024,
        MemoryReclaimer::create());
  }

  SpillReadFuture MakeFailedReadFuture() {
    std::promise<IoResult> promise;
    IoResult result;
    result.error = IoErrorCode::BackendIoError;
    result.nativeErrorCode = EIO;
    promise.set_value(std::move(result));

    return SpillReadFuture{
        promise.get_future(),
        std::make_shared<compress::CompressionManager>(compress::CompressionConfig{}),
        root_.get(),
        4096};
  }

  MemoryManager manager_;
  std::shared_ptr<MemoryPool> root_;
};

} // namespace

TEST_F(BlockStateMachineTest, PinAndUnpinResidentBlockUpdateCountsAndSequence) {
  BlockMemory memory{7, 4096, MemoryTag::kTesting};
  memory.payload = MakePayload(memory.size, 'a');

  BlockStateMachine::PinResident(memory);
  EXPECT_EQ(1, memory.pinCount);
  EXPECT_EQ(1, memory.evictionSequence);

  BlockStateMachine::Unpin(memory);
  EXPECT_EQ(0, memory.pinCount);
  EXPECT_EQ(2, memory.evictionSequence);
}

TEST_F(BlockStateMachineTest, BeginRollbackAndCompleteSpillMovePayloadAndState) {
  BlockMemory memory{8, 4096, MemoryTag::kTesting};
  memory.payload = MakePayload(memory.size, 'b');

  auto payload = BlockStateMachine::BeginSpill(memory);
  EXPECT_FALSE(memory.payload.has_value());
  EXPECT_EQ(BlockMemoryState::kSpilling, memory.state);
  ASSERT_TRUE(payload.valid());
  EXPECT_EQ('b', payload.data()[0]);

  BlockStateMachine::RollbackSpill(memory, std::move(payload));
  EXPECT_TRUE(memory.payload.has_value());
  EXPECT_EQ(BlockMemoryState::kInMemory, memory.state);

  payload = BlockStateMachine::BeginSpill(memory);
  ManagedFileSegment segment;
  BlockStateMachine::CompleteSpill(memory, std::move(segment));
  EXPECT_FALSE(memory.payload.has_value());
  EXPECT_TRUE(memory.segment.has_value());
  EXPECT_EQ(BlockMemoryState::kSpilled, memory.state);
  EXPECT_EQ(1, memory.evictionSequence);
}

TEST_F(BlockStateMachineTest, SubmitConsumeFailAndCompleteReadTransitions) {
  BlockMemory memory{9, 4096, MemoryTag::kTesting};
  memory.state = BlockMemoryState::kSpilled;
  memory.segment.emplace();

  BlockStateMachine::SubmitRead(memory, MakeFailedReadFuture());
  EXPECT_EQ(BlockMemoryState::kPrefetching, memory.state);
  EXPECT_TRUE(memory.prefetchFuture.has_value());

  auto failed = BlockStateMachine::ConsumePrefetch(memory);
  EXPECT_FALSE(failed.ok());
  EXPECT_EQ(IoErrorCode::BackendIoError, failed.io.error);
  EXPECT_FALSE(memory.prefetchFuture.has_value());

  BlockStateMachine::MarkReadFailed(memory);
  EXPECT_EQ(BlockMemoryState::kSpilled, memory.state);

  auto oldSegment =
      BlockStateMachine::CompleteRead(memory, MakePayload(memory.size, 'c'));
  EXPECT_FALSE(oldSegment.valid());
  EXPECT_FALSE(memory.segment.has_value());
  ASSERT_TRUE(memory.payload.has_value());
  EXPECT_EQ('c', memory.payload->data()[0]);
  EXPECT_EQ(BlockMemoryState::kInMemory, memory.state);
  EXPECT_EQ(1, memory.pinCount);
  EXPECT_EQ(1, memory.evictionSequence);
}

} // namespace bytedance::bolt::memory::bm
