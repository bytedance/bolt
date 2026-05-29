#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/file/tests/FileBlockAllocatorTestUtil.h"

#include <array>
#include <cstring>
#include <filesystem>
#include <string>
#include <type_traits>

#include <fmt/format.h>
#include <gtest/gtest.h>

namespace bytedance::bolt::memory::bm {
namespace {

class BufferManagerTest : public testing::Test {
 protected:
  void SetUp() override {
    root_ = manager_.addRootPool(
        fmt::format("bm-root-{}", testing::UnitTest::GetInstance()
                                      ->current_test_info()
                                      ->name()),
        kMaxMemory,
        MemoryReclaimer::create());
  }

  std::shared_ptr<BufferManager> makeBufferManager(const std::string& name) {
    const auto directory =
        test::UniqueTempDir(fmt::format("bolt-bm-buffer-manager-{}", name));
    std::filesystem::remove_all(directory);

    BufferManagerConfig config;
    config.poolName = fmt::format("bm-{}", name);
    config.fileAllocatorConfig = test::ValidConfigWithDirectory(directory);
    return BufferManager::Create(*root_, std::move(config));
  }

  MemoryManager manager_;
  std::shared_ptr<MemoryPool> root_;
};

static_assert(!std::is_copy_constructible_v<BufferHandle>);
static_assert(!std::is_copy_assignable_v<BufferHandle>);
static_assert(std::is_move_constructible_v<BufferHandle>);

bool IsIoUringUnavailable(const std::exception& e) {
  return std::string(e.what()).find("io_uring_queue_init failed") !=
      std::string::npos;
}

} // namespace

TEST(BufferManagerApiTest, MemoryTagHasStableNames) {
  EXPECT_STREQ("Unknown", toString(MemoryTag::kUnknown));
  EXPECT_STREQ("Testing", toString(MemoryTag::kTesting));
}

TEST(BufferManagerHandleTest, BlockHandleExposesSizeAndTag) {
  auto block = testingCreateBlockHandle(4096, MemoryTag::kTesting);
  EXPECT_EQ(4096, block->size());
  EXPECT_EQ(MemoryTag::kTesting, block->tag());
}

TEST_F(BufferManagerTest, AllocateReturnsPinnedWritablePayload) {
  auto bm = makeBufferManager("allocate");
  std::shared_ptr<BlockHandle> block;
  auto handle = bm->Allocate(4096, MemoryTag::kTesting, &block);

  ASSERT_NE(nullptr, block);
  ASSERT_NE(nullptr, handle.Ptr());
  std::memset(handle.Ptr(), 7, block->size());
  EXPECT_EQ(4096, block->size());
  EXPECT_EQ(MemoryTag::kTesting, block->tag());
}

TEST_F(BufferManagerTest, PinResidentBlockSeesWrittenBytes) {
  auto bm = makeBufferManager("pin-resident");
  std::shared_ptr<BlockHandle> block;
  {
    auto handle = bm->Allocate(4096, MemoryTag::kTesting, &block);
    handle.Ptr()[0] = 42;
  }

  auto repin = bm->Pin(block);
  EXPECT_EQ(42, repin.Ptr()[0]);
}

TEST_F(BufferManagerTest, BatchPinResidentBlocks) {
  auto bm = makeBufferManager("batch-pin-resident");
  std::shared_ptr<BlockHandle> first;
  std::shared_ptr<BlockHandle> second;
  {
    auto firstHandle = bm->Allocate(4096, MemoryTag::kTesting, &first);
    auto secondHandle = bm->Allocate(4096, MemoryTag::kTesting, &second);
    firstHandle.Ptr()[0] = 1;
    secondHandle.Ptr()[0] = 2;
  }

  std::array<std::shared_ptr<BlockHandle>, 2> blocks{first, second};
  auto handles = bm->BatchPin(blocks);
  ASSERT_EQ(2, handles.size());
  EXPECT_EQ(1, handles[0].Ptr()[0]);
  EXPECT_EQ(2, handles[1].Ptr()[0]);
}

TEST_F(BufferManagerTest, ReclaimSpillsAndPinReadsBackPayload) {
  auto bm = makeBufferManager("reclaim-pin");
  std::shared_ptr<BlockHandle> block;
  {
    auto handle = bm->Allocate(4096, MemoryTag::kTesting, &block);
    std::memset(handle.Ptr(), 9, block->size());
  }

  EXPECT_EQ(4096, bm->reclaimableBytes());
  try {
    EXPECT_EQ(4096, bm->ReclaimForTest(4096));
  } catch (const std::exception& e) {
    if (IsIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }
  EXPECT_EQ(0, bm->reclaimableBytes());

  auto repin = bm->Pin(block);
  EXPECT_EQ(9, repin.Ptr()[0]);
  EXPECT_EQ(9, repin.Ptr()[4095]);
}

TEST_F(BufferManagerTest, PrefetchIsHintAndPinHarvestsResult) {
  auto bm = makeBufferManager("prefetch");
  std::shared_ptr<BlockHandle> block;
  {
    auto handle = bm->Allocate(4096, MemoryTag::kTesting, &block);
    std::memset(handle.Ptr(), 11, block->size());
  }
  try {
    ASSERT_EQ(4096, bm->ReclaimForTest(4096));
  } catch (const std::exception& e) {
    if (IsIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }

  std::array<std::shared_ptr<BlockHandle>, 1> blocks{block};
  bm->Prefetch(blocks);

  auto repin = bm->Pin(block);
  EXPECT_EQ(11, repin.Ptr()[0]);
  EXPECT_EQ(0, bm->stats().prefetchSubmitFailures);
}

} // namespace bytedance::bolt::memory::bm
