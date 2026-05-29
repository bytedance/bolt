#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/Memory.h"
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
        fmt::format(
            "bm-root-{}",
            testing::UnitTest::GetInstance()->current_test_info()->name()),
        kMaxMemory,
        MemoryReclaimer::create());
  }

  std::shared_ptr<BufferManager> makeBufferManager(
      const std::string& name,
      compress::CompressionKind compressionKind =
          compress::CompressionKind::kLz4,
      size_t minCompressBytes = 256 * 1024) {
    const auto directory =
        test::UniqueTempDir(fmt::format("bolt-bm-buffer-manager-{}", name));
    std::filesystem::remove_all(directory);

    BufferManagerConfig config;
    config.poolName = fmt::format("bm-{}", name);
    config.spillStoreConfig.fileAllocatorConfig =
        test::ValidConfigWithDirectory(directory);
    config.spillStoreConfig.compressionConfig.kind = compressionKind;
    config.spillStoreConfig.compressionConfig.minCompressBytes =
        minCompressBytes;
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

TEST(BufferManagerApiTest, AllocateSizeMapsToStableByteSizes) {
  EXPECT_EQ(256 * 1024, allocateSizeBytes(AllocateSize::kSmall));
  EXPECT_EQ(1024 * 1024, allocateSizeBytes(AllocateSize::kMedium));
  EXPECT_EQ(4 * 1024 * 1024, allocateSizeBytes(AllocateSize::kLarge));
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
    EXPECT_EQ(4096, bm->Reclaim(4096));
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

TEST_F(BufferManagerTest, ReclaimSubmitFailureKeepsBlockReclaimable) {
  auto bm = makeBufferManager("reclaim-submit-failure");
  std::shared_ptr<BlockHandle> block;
  {
    auto handle = bm->Allocate(4096, MemoryTag::kTesting, &block);
    std::memset(handle.Ptr(), 13, block->size());
  }

  try {
    (void)bm->Reclaim(4096);
  } catch (const std::exception& e) {
    if (!IsIoUringUnavailable(e)) {
      throw;
    }
    EXPECT_EQ(4096, bm->reclaimableBytes());

    try {
      (void)bm->Reclaim(4096);
    } catch (const std::exception& second) {
      if (!IsIoUringUnavailable(second)) {
        throw;
      }
      auto repin = bm->Pin(block);
      EXPECT_EQ(13, repin.Ptr()[0]);
      return;
    }
    FAIL() << "expected unavailable scheduler to retry reclaim submission";
  }

  GTEST_SKIP() << "Disk IO scheduler is available; failure path not exercised";
}

TEST_F(BufferManagerTest, PrefetchIsHintAndPinHarvestsResult) {
  auto bm = makeBufferManager("prefetch");
  std::shared_ptr<BlockHandle> block;
  {
    auto handle = bm->Allocate(4096, MemoryTag::kTesting, &block);
    std::memset(handle.Ptr(), 11, block->size());
  }
  try {
    ASSERT_EQ(4096, bm->Reclaim(4096));
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

TEST_F(BufferManagerTest, StatsTrackPinnedAndUnpinnedResidentBytes) {
  auto bm = makeBufferManager("stats-resident");
  std::shared_ptr<BlockHandle> block;
  auto handle = bm->Allocate(4096, MemoryTag::kTesting, &block);

  auto stats = bm->stats();
  EXPECT_EQ(1, stats.allocatedBlocks);
  EXPECT_EQ(1, stats.liveBlocks);
  EXPECT_EQ(4096, stats.pinnedResidentBytes);
  EXPECT_EQ(0, stats.unpinnedResidentBytes);
  EXPECT_EQ(0, stats.evictionQueueSize);

  handle = BufferHandle{};
  stats = bm->stats();
  EXPECT_EQ(0, stats.pinnedResidentBytes);
  EXPECT_EQ(4096, stats.unpinnedResidentBytes);
  EXPECT_EQ(1, stats.evictionQueueSize);
}

TEST_F(BufferManagerTest, StatsDropTempBlockWhenLastHandleIsDestroyed) {
  auto bm = makeBufferManager("stats-temp-destroy");
  {
    auto handle = bm->Allocate(4096, MemoryTag::kTesting);
    auto stats = bm->stats();
    EXPECT_EQ(1, stats.liveBlocks);
    EXPECT_EQ(4096, stats.pinnedResidentBytes);
  }

  auto stats = bm->stats();
  EXPECT_EQ(0, stats.liveBlocks);
  EXPECT_EQ(0, stats.pinnedResidentBytes);
  EXPECT_EQ(0, stats.unpinnedResidentBytes);
}

TEST_F(BufferManagerTest, TagStatsTrackAllocationSource) {
  auto bm = makeBufferManager("tag-stats");
  std::shared_ptr<BlockHandle> sortBlock;
  std::shared_ptr<BlockHandle> aggBlock;
  auto sortHandle = bm->Allocate(4096, MemoryTag::kSort, &sortBlock);
  auto aggHandle = bm->Allocate(8192, MemoryTag::kAggregation, &aggBlock);
  sortHandle = BufferHandle{};

  const auto tagStats = bm->tagStats();
  const auto findTag = [&](MemoryTag tag) -> const BufferManagerTagStats* {
    for (const auto& stats : tagStats) {
      if (stats.tag == tag) {
        return &stats;
      }
    }
    return nullptr;
  };

  const auto* sortStats = findTag(MemoryTag::kSort);
  ASSERT_NE(nullptr, sortStats);
  EXPECT_EQ(1, sortStats->allocatedBlocks);
  EXPECT_EQ(1, sortStats->liveBlocks);
  EXPECT_EQ(4096, sortStats->residentBytes);
  EXPECT_EQ(4096, sortStats->unpinnedResidentBytes);

  const auto* aggStats = findTag(MemoryTag::kAggregation);
  ASSERT_NE(nullptr, aggStats);
  EXPECT_EQ(1, aggStats->allocatedBlocks);
  EXPECT_EQ(1, aggStats->liveBlocks);
  EXPECT_EQ(8192, aggStats->residentBytes);
  EXPECT_EQ(8192, aggStats->pinnedResidentBytes);
}

TEST_F(BufferManagerTest, StatsTrackSpillAndReadback) {
  auto bm = makeBufferManager("stats-spill-read");
  std::shared_ptr<BlockHandle> block;
  {
    auto handle = bm->Allocate(4096, MemoryTag::kTesting, &block);
    std::memset(handle.Ptr(), 17, block->size());
  }

  try {
    ASSERT_EQ(4096, bm->Reclaim(4096));
  } catch (const std::exception& e) {
    if (IsIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }

  auto stats = bm->stats();
  EXPECT_EQ(0, stats.unpinnedResidentBytes);
  EXPECT_EQ(4096, stats.spilledBytes);
  EXPECT_EQ(4096, stats.reclaimedBytes);
  EXPECT_EQ(1, stats.reclaimCount);
  EXPECT_EQ(1, stats.spillWriteCount);
  EXPECT_EQ(4096, stats.spillWriteBytes);

  auto repin = bm->Pin(block);
  EXPECT_EQ(17, repin.Ptr()[0]);

  stats = bm->stats();
  EXPECT_EQ(4096, stats.pinnedResidentBytes);
  EXPECT_EQ(0, stats.spilledBytes);
  EXPECT_EQ(1, stats.spillReadCount);
  EXPECT_EQ(4096, stats.spillReadBytes);
}

TEST_F(BufferManagerTest, DefaultCompressionSpillsAndReadsBackPayload) {
  auto bm =
      makeBufferManager("default-compression", compress::CompressionKind::kLz4, 1);
  std::shared_ptr<BlockHandle> block;
  {
    auto handle = bm->Allocate(256 * 1024, MemoryTag::kTesting, &block);
    std::memset(handle.Ptr(), 'a', block->size());
  }

  try {
    ASSERT_EQ(256 * 1024, bm->Reclaim(256 * 1024));
  } catch (const std::exception& e) {
    if (IsIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }

  auto stats = bm->stats();
  EXPECT_EQ(1, stats.spillCompressedBlocks);
  EXPECT_LT(stats.spillPhysicalWriteBytes, stats.spillWriteBytes);

  auto repin = bm->Pin(block);
  EXPECT_EQ('a', repin.Ptr()[0]);
  EXPECT_EQ('a', repin.Ptr()[block->size() - 1]);
}

class BufferManagerCompressionKindTest
    : public BufferManagerTest,
      public testing::WithParamInterface<compress::CompressionKind> {};

TEST_P(BufferManagerCompressionKindTest, CompressionKindSpillsAndReadsBack) {
  auto bm = makeBufferManager("compression-kind", GetParam(), 1);
  std::shared_ptr<BlockHandle> block;
  {
    auto handle = bm->Allocate(128 * 1024, MemoryTag::kTesting, &block);
    std::memset(handle.Ptr(), 21, block->size());
  }

  try {
    ASSERT_EQ(128 * 1024, bm->Reclaim(128 * 1024));
  } catch (const std::exception& e) {
    if (IsIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }

  auto repin = bm->Pin(block);
  EXPECT_EQ(21, repin.Ptr()[0]);
  EXPECT_EQ(21, repin.Ptr()[block->size() - 1]);
}

INSTANTIATE_TEST_SUITE_P(
    Algorithms,
    BufferManagerCompressionKindTest,
    testing::Values(
        compress::CompressionKind::kLz4,
        compress::CompressionKind::kZstd,
        compress::CompressionKind::kSnappy));

TEST_F(BufferManagerTest, DebugStringContainsCoreCounters) {
  auto bm = makeBufferManager("debug-string");
  std::shared_ptr<BlockHandle> block;
  auto handle = bm->Allocate(4096, MemoryTag::kTesting, &block);
  handle = BufferHandle{};

  const auto debug = bm->debugString();
  EXPECT_NE(std::string::npos, debug.find("allocated_blocks=1"));
  EXPECT_NE(std::string::npos, debug.find("unpinned_resident_bytes=4096"));
  EXPECT_NE(std::string::npos, debug.find("tag=Testing"));
}

} // namespace bytedance::bolt::memory::bm
