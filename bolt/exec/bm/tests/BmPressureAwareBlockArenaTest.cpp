#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/AllocateSize.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"
#include "bolt/exec/bm/BmBlockReclaimPolicy.h"
#include "bolt/exec/bm/BmPressureAwareBlockArena.h"

#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

namespace bytedance::bolt::exec {
namespace {

bool isIoUringUnavailable(const std::exception& e) {
  return std::string(e.what()).find("io_uring_queue_init failed") !=
      std::string::npos;
}

uint32_t allocateReservedBlock(
    BmPressureAwareBlockArena& arena,
    const std::shared_ptr<memory::bm::BufferManager>& bufferManager,
    uint32_t capacity) {
  BOLT_CHECK(bufferManager->MaybeReserve(capacity));
  return arena.allocateReservedBlock(capacity);
}

class BmPressureAwareBlockArenaTest : public testing::Test {
 protected:
  void SetUp() override {
    root_ = memoryManager_.addRootPool(
        fmt::format(
            "bm-pressure-aware-arena-root-{}",
            testing::UnitTest::GetInstance()->current_test_info()->name()),
        64 << 20,
        memory::MemoryReclaimer::create());
  }

  std::shared_ptr<memory::bm::BufferManager> makeBufferManager(
      const std::string& name,
      memory::MemoryPool* root = nullptr) {
    const auto directory =
        memory::bm::test::UniqueTempDir(fmt::format("bm-arena-{}", name));
    std::filesystem::remove_all(directory);

    memory::bm::BufferManagerConfig config;
    config.poolName = fmt::format("bm-arena-{}", name);
    config.spillStoreConfig.fileAllocatorConfig =
        memory::bm::test::ValidConfigWithDirectory(directory);
    return memory::bm::BufferManager::Create(
        root == nullptr ? *root_ : *root, std::move(config));
  }

  memory::MemoryManager memoryManager_;
  std::shared_ptr<memory::MemoryPool> root_;
};

class LatestPinnedBlockReclaimPolicy final : public BmBlockReclaimPolicy {
 public:
  std::vector<BlockId> selectVictims(
      const BmBlockReclaimContext& context) const override {
    std::vector<BlockId> victims;
    for (const auto& candidate : context.candidates) {
      if (candidate.pinned) {
        victims.assign({candidate.blockId});
      }
    }
    return victims;
  }
};

class InvalidBlockReclaimPolicy final : public BmBlockReclaimPolicy {
 public:
  std::vector<BlockId> selectVictims(
      const BmBlockReclaimContext& /*context*/) const override {
    return {0};
  }
};

TEST_F(BmPressureAwareBlockArenaTest, AllocateBlockPinsAndTracksBytes) {
  auto bufferManager = makeBufferManager("allocate");
  BmPressureAwareBlockArena arena(bufferManager, memory::bm::MemoryTag::kWindow);

  const auto blockId = allocateReservedBlock(arena, bufferManager, 4096);

  EXPECT_EQ(1, arena.size());
  EXPECT_EQ(4096, arena.allocatedBytes());
  EXPECT_EQ(0, arena.usedBytes());
  EXPECT_NE(nullptr, arena.activeData(blockId));
  EXPECT_TRUE(arena.block(blockId).pinnedHandle.has_value());

  arena.block(blockId).usedBytes = 128;
  EXPECT_EQ(128, arena.usedBytes());
}

TEST_F(BmPressureAwareBlockArenaTest, SpillUsesInjectedPolicy) {
  auto bufferManager = makeBufferManager("policy");
  BmPressureAwareBlockArena arena(
      bufferManager,
      memory::bm::MemoryTag::kWindow,
      std::make_unique<LatestPinnedBlockReclaimPolicy>());

  const auto lruVictim = allocateReservedBlock(arena, bufferManager, 8 << 20);
  const auto policyVictim = allocateReservedBlock(arena, bufferManager, 8 << 20);

  try {
    arena.spillReclaimableBlocks(8 << 20, {});
  } catch (const std::exception& e) {
    if (!isIoUringUnavailable(e)) {
      throw;
    }
  }

  EXPECT_TRUE(arena.block(lruVictim).pinnedHandle.has_value());
  EXPECT_FALSE(arena.block(policyVictim).pinnedHandle.has_value());
}

TEST_F(BmPressureAwareBlockArenaTest, PolicyCannotBypassCanReclaim) {
  auto bufferManager = makeBufferManager("policy-guard");
  BmPressureAwareBlockArena arena(
      bufferManager,
      memory::bm::MemoryTag::kWindow,
      std::make_unique<InvalidBlockReclaimPolicy>());

  const auto protectedBlock =
      allocateReservedBlock(arena, bufferManager, 4096);
  const auto reclaimableBlock =
      allocateReservedBlock(arena, bufferManager, 4096);

  const std::vector<BlockId> protectedBlocks{protectedBlock};

  EXPECT_THROW(
      arena.spillReclaimableBlocks(4096, protectedBlocks),
      BoltException);
  EXPECT_TRUE(arena.block(protectedBlock).pinnedHandle.has_value());
  EXPECT_TRUE(arena.block(reclaimableBlock).pinnedHandle.has_value());
}

TEST_F(BmPressureAwareBlockArenaTest, PinReadDoesNotReclaimProtectedBlocks) {
  auto root = memoryManager_.addRootPool(
      "bm-pressure-aware-arena-protected-pin-root",
      16 << 20,
      memory::MemoryReclaimer::create());
  auto bufferManager = makeBufferManager("reclaim", root.get());
  BmPressureAwareBlockArena arena(bufferManager, memory::bm::MemoryTag::kWindow);

  const auto target = allocateReservedBlock(arena, bufferManager, 8 << 20);
  const auto protectedBlock = allocateReservedBlock(arena, bufferManager, 8 << 20);
  arena.block(target).pinnedHandle.reset();
  arena.block(target).data = nullptr;

  const std::vector<BlockId> protectedBlocks{protectedBlock};

  EXPECT_THROW(arena.pinnedData(target, protectedBlocks), BoltException);
  EXPECT_FALSE(arena.block(target).pinnedHandle.has_value());
  EXPECT_TRUE(arena.block(protectedBlock).pinnedHandle.has_value());
  EXPECT_NE(nullptr, arena.block(protectedBlock).data);
}

TEST_F(BmPressureAwareBlockArenaTest, PinnedDataThrowsOnPressureWithoutVictims) {
  auto root = memoryManager_.addRootPool(
      "bm-pressure-aware-arena-try-pin-root",
      16 << 20,
      memory::MemoryReclaimer::create());
  auto bufferManager = makeBufferManager("try-pin", root.get());
  BmPressureAwareBlockArena arena(bufferManager, memory::bm::MemoryTag::kWindow);

  const auto target = allocateReservedBlock(arena, bufferManager, 8 << 20);
  const auto other = allocateReservedBlock(arena, bufferManager, 8 << 20);
  arena.block(target).pinnedHandle.reset();
  arena.block(target).data = nullptr;

  const std::vector<BlockId> protectedBlocks{other};

  EXPECT_THROW(arena.pinnedData(target, protectedBlocks), BoltException);
  EXPECT_FALSE(arena.block(target).pinnedHandle.has_value());
  EXPECT_TRUE(arena.block(other).pinnedHandle.has_value());
  EXPECT_NE(nullptr, arena.block(other).data);
}

TEST_F(BmPressureAwareBlockArenaTest, PinBlocksBatchPinsResidentBlocks) {
  auto bufferManager = makeBufferManager("pin-blocks");
  BmPressureAwareBlockArena arena(bufferManager, memory::bm::MemoryTag::kWindow);

  const auto blockId = allocateReservedBlock(arena, bufferManager, 4096);
  auto* data = arena.activeData(blockId);
  std::memcpy(data, "arena-pin-blocks", 17);
  arena.block(blockId).usedBytes = 17;

  arena.block(blockId).pinnedHandle.reset();
  arena.block(blockId).data = nullptr;

  const std::vector<uint32_t> blockIds{blockId};
  const auto statsBeforePin = bufferManager->stats();
  arena.pinBlocks(blockIds, {});

  const auto statsAfterPin = bufferManager->stats();
  EXPECT_EQ(statsBeforePin.batchPinCount + 1, statsAfterPin.batchPinCount);
  EXPECT_EQ(0, statsAfterPin.spillReadCount);
  ASSERT_TRUE(arena.block(blockId).pinnedHandle.has_value());
  ASSERT_NE(nullptr, arena.block(blockId).data);
  EXPECT_EQ(0, std::memcmp(arena.block(blockId).data, "arena-pin-blocks", 17));
}

TEST_F(BmPressureAwareBlockArenaTest, TryPinBlocksSkipsBlocksOnPressure) {
  auto root = memoryManager_.addRootPool(
      "bm-pressure-aware-arena-try-pin-blocks-root",
      16 << 20,
      memory::MemoryReclaimer::create());
  auto bufferManager = makeBufferManager("try-pin-blocks", root.get());
  BmPressureAwareBlockArena arena(bufferManager, memory::bm::MemoryTag::kWindow);

  const auto target = allocateReservedBlock(arena, bufferManager, 8 << 20);
  const auto other = allocateReservedBlock(arena, bufferManager, 8 << 20);
  arena.block(target).pinnedHandle.reset();
  arena.block(target).data = nullptr;

  const std::vector<BlockId> blockIds{target};
  const auto statsBeforePin = bufferManager->stats();
  arena.tryPinBlocks(blockIds);

  const auto statsAfterPin = bufferManager->stats();
  EXPECT_EQ(statsBeforePin.spillWriteCount, statsAfterPin.spillWriteCount);
  EXPECT_FALSE(arena.block(target).pinnedHandle.has_value());
  EXPECT_TRUE(arena.block(other).pinnedHandle.has_value());
}

TEST_F(BmPressureAwareBlockArenaTest, SpillReclaimableBlocksAndPinReadback) {
  auto bufferManager = makeBufferManager("spill-readback");
  BmPressureAwareBlockArena arena(bufferManager, memory::bm::MemoryTag::kWindow);

  const auto blockId = allocateReservedBlock(
      arena,
      bufferManager,
      memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge));
  auto* data = arena.activeData(blockId);
  std::memcpy(data, "arena-spill-readback", 21);
  arena.block(blockId).usedBytes = 21;

  try {
    const auto spilled = arena.spillReclaimableBlocks(0, {});
    EXPECT_EQ(1, spilled);
    EXPECT_FALSE(arena.block(blockId).pinnedHandle.has_value());
    EXPECT_EQ(nullptr, arena.block(blockId).data);

    const auto* pinned = arena.pinnedData(blockId, {});
    EXPECT_EQ(0, std::memcmp(pinned, "arena-spill-readback", 21));
    EXPECT_TRUE(arena.block(blockId).pinnedHandle.has_value());
  } catch (const std::exception& e) {
    if (isIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }
}

} // namespace
} // namespace bytedance::bolt::exec
