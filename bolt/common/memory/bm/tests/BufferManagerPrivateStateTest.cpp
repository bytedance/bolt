#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BlockStateMachine.h"
#include "bolt/common/memory/bm/file/tests/FileBlockAllocatorTestUtil.h"

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
#include "bolt/common/memory/bm/BufferManagerAccounting.h"
#include "bolt/common/memory/bm/BufferManager.h"
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
  EXPECT_THROW((void)bm->Pin(block), std::exception);

  block->memory_->state = static_cast<BlockMemoryState>(99);
  EXPECT_THROW((void)bm->Pin(block), std::exception);
}

TEST_F(BufferManagerPrivateStateTest, PrefetchRecordsSubmitFailure) {
  auto bm = makeBufferManager("prefetch-failure");
  auto block = testingCreateBlockHandle(4096, MemoryTag::kTesting);
  block->memory_->state = BlockMemoryState::kSpilled;

  std::array<std::shared_ptr<BlockHandle>, 1> blocks{block};
  bm->Prefetch(blocks);

  EXPECT_EQ(1, bm->stats().prefetchSubmitFailures);
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
  block->memory_->prefetchFuture = SpillReadFuture{
      promise.get_future(),
      bm->pool_.get(),
      bm->config_.spillStoreConfig.compressionConfig,
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
