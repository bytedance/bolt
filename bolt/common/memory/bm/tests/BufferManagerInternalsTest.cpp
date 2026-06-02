#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryArbitrator.h"
#include "bolt/common/memory/bm/BlockMemory.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/BufferManagerStats.h"
#include "bolt/common/memory/bm/BufferManagerReclaimer.h"
#include "bolt/common/memory/bm/MemoryTag.h"
#include "bolt/common/memory/bm/compress/CompressionManager.h"
#include "bolt/common/memory/bm/SpillStore.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"

#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <future>
#include <span>

#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

namespace bytedance::bolt::memory::bm {
namespace {

constexpr uint64_t kMaxMemory = 64 * 1024 * 1024;

class BufferManagerInternalsTest : public testing::Test {
 protected:
  void SetUp() override {
    FLAGS_v = 1;
    root_ = manager_.addRootPool(
        fmt::format(
            "bm-internals-root-{}",
            testing::UnitTest::GetInstance()->current_test_info()->name()),
        kMaxMemory,
        MemoryReclaimer::create());
  }

  std::shared_ptr<BufferManager> makeBufferManager(const std::string& name) {
    const auto directory =
        test::UniqueTempDir(fmt::format("bolt-bm-internals-{}", name));
    std::filesystem::remove_all(directory);

    BufferManagerConfig config;
    config.poolName = fmt::format("bm-internals-{}", name);
    config.spillStoreConfig.fileAllocatorConfig =
        test::ValidConfigWithDirectory(directory);
    return BufferManager::Create(*root_, std::move(config));
  }

  const BufferManagerTagStats& findTag(
      const std::vector<BufferManagerTagStats>& tags,
      MemoryTag tag) {
    for (const auto& stats : tags) {
      if (stats.tag == tag) {
        return stats;
      }
    }
    ADD_FAILURE() << "missing tag " << toString(tag);
    return tags.front();
  }

  MemoryManager manager_;
  std::shared_ptr<MemoryPool> root_;
};

BlockMemory makeBlock(size_t size, MemoryTag tag = MemoryTag::kTesting) {
  return BlockMemory{99, size, tag};
}

} // namespace

TEST_F(BufferManagerInternalsTest, AccountingRecordsResidentPinTransitions) {
  BufferManagerStatsCollector accounting;
  auto memory = makeBlock(4096, MemoryTag::kSort);
  memory.pinCount = 1;

  accounting.RecordAllocate(memory);
  accounting.RecordPinRequest(memory.tag);
  accounting.RecordPinInMemory();
  accounting.RecordBatchPin();
  accounting.RecordPrefetch();
  accounting.RecordPrefetchSubmitFailure();
  accounting.RecordReclaim();
  accounting.RecordReclaimAttemptedBlock();
  accounting.RecordReclaimedBytes(2048);
  accounting.RecordWriteIoFailure();
  accounting.RecordReadIoFailure();

  memory.pinCount = 1;
  accounting.OnResidentUnpinned(memory);
  memory.pinCount = 0;
  accounting.OnResidentPinned(memory);
  memory.pinCount = 1;

  const auto stats = accounting.stats();
  EXPECT_EQ(1, stats.allocatedBlocks);
  EXPECT_EQ(1, stats.liveBlocks);
  EXPECT_EQ(4096, stats.pinnedResidentBytes);
  EXPECT_EQ(0, stats.unpinnedResidentBytes);
  EXPECT_EQ(1, stats.pinCount);
  EXPECT_EQ(1, stats.pinInMemoryCount);
  EXPECT_EQ(1, stats.batchPinCount);
  EXPECT_EQ(1, stats.prefetchCount);
  EXPECT_EQ(1, stats.prefetchSubmitFailures);
  EXPECT_EQ(1, stats.reclaimCount);
  EXPECT_EQ(1, stats.reclaimAttemptedBlocks);
  EXPECT_EQ(2048, stats.reclaimedBytes);
  EXPECT_EQ(1, stats.writeIoFailures);
  EXPECT_EQ(1, stats.readIoFailures);
  EXPECT_EQ(1, stats.prefetchIoFailures);

  const auto tags = accounting.tagStats();
  ASSERT_EQ(1, tags.size());
  const auto& sort = findTag(tags, MemoryTag::kSort);
  EXPECT_EQ(1, sort.allocatedBlocks);
  EXPECT_EQ(1, sort.liveBlocks);
  EXPECT_EQ(4096, sort.residentBytes);
  EXPECT_EQ(4096, sort.pinnedResidentBytes);
  EXPECT_EQ(1, sort.pinCount);
}

TEST_F(BufferManagerInternalsTest, AccountingRecordsSpillReadLifecycle) {
  BufferManagerStatsCollector accounting;
  auto memory = makeBlock(8192, MemoryTag::kAggregation);
  memory.pinCount = 1;

  accounting.RecordAllocate(memory);
  accounting.OnResidentUnpinned(memory);
  memory.pinCount = 0;

  accounting.OnSpillStarted(memory);
  EXPECT_EQ(8192, accounting.stats().spillingBytes);
  accounting.OnSpillRolledBack(memory);
  EXPECT_EQ(8192, accounting.stats().unpinnedResidentBytes);

  accounting.OnSpillStarted(memory);
  SpillWriteResult write;
  write.rawBytes = memory.size;
  write.physicalBytes = 1024;
  write.compressionTimeUs = 77;
  write.compressed = true;
  accounting.OnSpillCompleted(memory, write);

  auto stats = accounting.stats();
  EXPECT_EQ(0, stats.spillingBytes);
  EXPECT_EQ(8192, stats.spilledBytes);
  EXPECT_EQ(1, stats.spillWriteCount);
  EXPECT_EQ(8192, stats.spillWriteBytes);
  EXPECT_EQ(1024, stats.spillPhysicalWriteBytes);
  EXPECT_EQ(1, stats.spillCompressedBlocks);
  EXPECT_EQ(77, stats.spillCompressionTimeUs);

  accounting.OnReadSubmitted(memory);
  accounting.OnReadFutureConsumed(memory);
  SpillReadResult read;
  read.physicalBytes = 2048;
  read.decompressionTimeUs = 88;
  accounting.OnReadCompleted(memory, read);

  stats = accounting.stats();
  EXPECT_EQ(0, stats.spilledBytes);
  EXPECT_EQ(8192, stats.pinnedResidentBytes);
  EXPECT_EQ(1, stats.pinReadCount);
  EXPECT_EQ(1, stats.spillReadCount);
  EXPECT_EQ(8192, stats.spillReadBytes);
  EXPECT_EQ(2048, stats.spillPhysicalReadBytes);
  EXPECT_EQ(88, stats.spillDecompressionTimeUs);

  const auto& tag = findTag(accounting.tagStats(), MemoryTag::kAggregation);
  EXPECT_EQ(8192, tag.residentBytes);
  EXPECT_EQ(8192, tag.pinnedResidentBytes);
  EXPECT_EQ(8192, tag.reclaimedBytes);
  EXPECT_EQ(1, tag.spillWriteCount);
  EXPECT_EQ(1, tag.spillReadCount);
}

TEST_F(BufferManagerInternalsTest, AccountingDestroysBlocksInEachState) {
  {
    BufferManagerStatsCollector accounting;
    auto memory = makeBlock(1024, MemoryTag::kTesting);
    memory.pinCount = 1;
    accounting.RecordAllocate(memory);
    accounting.OnBlockMemoryDestroy(memory);
    EXPECT_EQ(0, accounting.stats().liveBlocks);
  }
  {
    BufferManagerStatsCollector accounting;
    auto memory = makeBlock(1024, MemoryTag::kTesting);
    memory.pinCount = 1;
    accounting.RecordAllocate(memory);
    accounting.OnResidentUnpinned(memory);
    memory.pinCount = 0;
    accounting.OnBlockMemoryDestroy(memory);
    EXPECT_EQ(0, accounting.stats().liveBlocks);
  }
  {
    BufferManagerStatsCollector accounting;
    auto memory = makeBlock(1024, MemoryTag::kTesting);
    memory.pinCount = 1;
    accounting.RecordAllocate(memory);
    accounting.OnResidentUnpinned(memory);
    memory.pinCount = 0;
    accounting.OnSpillStarted(memory);
    memory.state = BlockMemoryState::kSpilling;
    accounting.OnBlockMemoryDestroy(memory);
    EXPECT_EQ(0, accounting.stats().liveBlocks);
  }
  {
    BufferManagerStatsCollector accounting;
    auto memory = makeBlock(1024, MemoryTag::kTesting);
    memory.pinCount = 1;
    accounting.RecordAllocate(memory);
    accounting.OnResidentUnpinned(memory);
    memory.pinCount = 0;
    accounting.OnSpillStarted(memory);
    SpillWriteResult write;
    write.rawBytes = memory.size;
    write.physicalBytes = memory.size;
    accounting.OnSpillCompleted(memory, write);
    memory.state = BlockMemoryState::kSpilled;
    accounting.OnBlockMemoryDestroy(memory);
    EXPECT_EQ(0, accounting.stats().liveBlocks);
  }
  {
    BufferManagerStatsCollector accounting;
    auto memory = makeBlock(1024, MemoryTag::kTesting);
    memory.pinCount = 1;
    accounting.RecordAllocate(memory);
    accounting.OnResidentUnpinned(memory);
    memory.pinCount = 0;
    accounting.OnSpillStarted(memory);
    SpillWriteResult write;
    write.rawBytes = memory.size;
    write.physicalBytes = memory.size;
    accounting.OnSpillCompleted(memory, write);
    accounting.OnReadSubmitted(memory);
    memory.state = BlockMemoryState::kPrefetching;
    accounting.OnBlockMemoryDestroy(memory);
    EXPECT_EQ(0, accounting.stats().liveBlocks);
  }
}

TEST_F(BufferManagerInternalsTest, ReclaimerHandlesExpiredAndLiveManagers) {
  BufferManagerReclaimer expired{std::weak_ptr<BufferManager>{}};
  uint64_t reclaimable = 999;
  EXPECT_FALSE(expired.reclaimableBytes(*root_, reclaimable));
  EXPECT_EQ(0, reclaimable);
  MemoryReclaimer::Stats stats;
  EXPECT_EQ(0, expired.reclaim(root_.get(), 4096, 5, stats));

  auto bm = makeBufferManager("reclaimer-live");
  BufferManagerReclaimer live{bm};
  auto handle = bm->Allocate(4096, MemoryTag::kTesting);
  EXPECT_FALSE(live.reclaimableBytes(*root_, reclaimable));
  EXPECT_EQ(0, reclaimable);
  {
    auto leaf = root_->addLeafChild("reclaimer-leaf");
    ScopedMemoryArbitrationContext arbitrationContext(leaf.get());
    EXPECT_EQ(0, live.reclaim(leaf.get(), 4096, 5, stats));
  }

  auto block = handle.block();
  handle = BufferHandle{};
  EXPECT_TRUE(live.reclaimableBytes(*root_, reclaimable));
  EXPECT_EQ(4096, reclaimable);
}

TEST_F(BufferManagerInternalsTest, PublicControlsDoNotRequireSpillPath) {
  BufferManagerConfig invalidConfig;
  EXPECT_THROW(
      (void)BufferManager::Create(*root_, std::move(invalidConfig)),
      std::exception);

  auto bm = makeBufferManager("public-controls");
  EXPECT_TRUE(bm->MaybeReserve(4096));
  bm->ReleaseUnusedReservation();

  EXPECT_EQ(0, bm->Reclaim(4096));
  EXPECT_EQ(1, bm->stats().reclaimCount);
  EXPECT_EQ(0, bm->stats().reclaimAttemptedBlocks);

  auto handle = bm->Allocate(4096, MemoryTag::kTesting);
  auto block = handle.block();
  std::array<std::shared_ptr<BlockHandle>, 2> blocks{nullptr, block};
  EXPECT_THROW(bm->Prefetch(blocks), std::exception);
  const auto stats = bm->stats();
  EXPECT_EQ(1, stats.prefetchCount);
  EXPECT_EQ(0, stats.prefetchSubmitFailures);
}

TEST_F(BufferManagerInternalsTest, PublicValidationFailuresAreReported) {
  auto bm = makeBufferManager("validation-failures");

  EXPECT_THROW((void)bm->Allocate(0, MemoryTag::kTesting), std::exception);

  std::shared_ptr<BlockHandle> nullBlock;
  EXPECT_THROW((void)bm->Pin(nullBlock), std::exception);

  std::array<std::shared_ptr<BlockHandle>, 1> blocks{nullptr};
  EXPECT_THROW((void)bm->BatchPin(blocks), std::exception);

  BufferHandle empty;
  EXPECT_FALSE(empty.valid());
  EXPECT_THROW((void)empty.Ptr(), std::exception);
  EXPECT_THROW((void)empty.block(), std::exception);
}

TEST_F(BufferManagerInternalsTest, SpillReadFutureDecodesRawRecord) {
  constexpr size_t kSize = 4096;
  auto raw = IoBuffer::allocateFromMalloc(kSize);
  std::memset(raw.data(), 'r', raw.length());

  compress::CompressionConfig config;
  config.kind = compress::CompressionKind::kNone;
  compress::CompressionManager codec{config};
  auto record = codec.BuildSpillRecord(std::span<const char>(raw.data(), raw.length()));

  std::promise<IoResult> promise;
  IoResult io;
  io.bytes = record.physicalSize;
  io.buffer = std::move(record.record);
  promise.set_value(std::move(io));

  auto leaf = root_->addLeafChild("spill-read-success");
  SpillReadFuture future{
      promise.get_future(),
      std::make_shared<compress::CompressionManager>(config),
      leaf.get(),
      kSize};
  auto result = future.get();

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(kSize, result.rawBytes);
  EXPECT_EQ(kSize, result.io.bytes);
  ASSERT_TRUE(result.io.buffer.valid());
  EXPECT_EQ('r', result.io.buffer.data()[0]);
  EXPECT_EQ('r', result.io.buffer.data()[kSize - 1]);
}

TEST_F(
    BufferManagerInternalsTest,
    SpillReadFutureReportsFailedReadsAndThrowsInvalidRecords) {
  {
    std::promise<IoResult> promise;
    IoResult failed;
    failed.error = IoErrorCode::BackendIoError;
    failed.nativeErrorCode = EIO;
    failed.bytes = 123;
    promise.set_value(std::move(failed));

    SpillReadFuture future{
        promise.get_future(),
        std::make_shared<compress::CompressionManager>(compress::CompressionConfig{}),
        root_.get(),
        4096};
    auto result = future.get();
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(IoErrorCode::BackendIoError, result.io.error);
    EXPECT_EQ(EIO, result.io.nativeErrorCode);
    EXPECT_EQ(123, result.physicalBytes);
  }

  {
    std::promise<IoResult> promise;
    IoResult invalid;
    invalid.bytes = 8;
    invalid.buffer = IoBuffer::allocateFromMalloc(8);
    std::memset(invalid.buffer.data(), 0x7f, invalid.buffer.length());
    promise.set_value(std::move(invalid));

    SpillReadFuture future{
        promise.get_future(),
        std::make_shared<compress::CompressionManager>(compress::CompressionConfig{}),
        root_.get(),
        4096};
    EXPECT_THROW((void)future.get(), std::exception);
  }
}

TEST_F(BufferManagerInternalsTest, SpillWriteFutureCompletesToResult) {
  std::promise<IoResult> promise;
  promise.set_value(IoResult{512});

  SpillWriteMetadata metadata;
  metadata.rawBytes = 1024;
  metadata.physicalBytes = 512;
  metadata.compressionTimeUs = 7;
  metadata.compressed = true;

  SpillWriteFuture write{promise.get_future(), {}, metadata};
  auto result = write.get();

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(512, result.io.bytes);
  EXPECT_EQ(1024, result.rawBytes);
  EXPECT_EQ(512, result.physicalBytes);
  EXPECT_EQ(7, result.compressionTimeUs);
  EXPECT_TRUE(result.compressed);
}

} // namespace bytedance::bolt::memory::bm
