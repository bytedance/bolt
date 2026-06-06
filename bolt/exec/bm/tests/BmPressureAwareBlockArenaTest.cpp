#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/AllocateSize.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"
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

TEST_F(BmPressureAwareBlockArenaTest, AllocateBlockPinsAndTracksBytes) {
  auto bufferManager = makeBufferManager("allocate");
  BmPressureAwareBlockArena arena(bufferManager, memory::bm::MemoryTag::kWindow);

  const auto canReclaim = [](uint32_t) { return true; };
  const auto blockId = arena.allocateBlock(4096, canReclaim);

  EXPECT_EQ(1, arena.size());
  EXPECT_EQ(4096, arena.allocatedBytes());
  EXPECT_EQ(0, arena.usedBytes());
  EXPECT_NE(nullptr, arena.activeData(blockId));
  EXPECT_TRUE(arena.block(blockId).pinnedHandle.has_value());

  arena.block(blockId).usedBytes = 128;
  EXPECT_EQ(128, arena.usedBytes());
}

TEST_F(BmPressureAwareBlockArenaTest, MakeBlocksReclaimableSkipsPinnedActiveBlock) {
  auto bufferManager = makeBufferManager("reclaim");
  BmPressureAwareBlockArena arena(bufferManager, memory::bm::MemoryTag::kWindow);

  const auto reclaimAll = [](uint32_t) { return true; };
  const auto first = arena.allocateBlock(4096, reclaimAll);
  const auto active = arena.allocateBlock(4096, reclaimAll);
  const auto third = arena.allocateBlock(4096, reclaimAll);

  const auto skipActive = [&](uint32_t blockId) { return blockId != active; };
  const auto released = arena.makeBlocksReclaimable(0, skipActive);

  EXPECT_EQ(8192, released);
  EXPECT_FALSE(arena.block(first).pinnedHandle.has_value());
  EXPECT_TRUE(arena.block(active).pinnedHandle.has_value());
  EXPECT_FALSE(arena.block(third).pinnedHandle.has_value());
  EXPECT_EQ(nullptr, arena.block(first).data);
  EXPECT_NE(nullptr, arena.block(active).data);
  EXPECT_EQ(nullptr, arena.block(third).data);
}

TEST_F(BmPressureAwareBlockArenaTest, TryAllocateBlockReturnsEmptyOnPressure) {
  auto root = memoryManager_.addRootPool(
      "bm-pressure-aware-arena-try-allocate-root",
      16 << 20,
      memory::MemoryReclaimer::create());
  auto bufferManager = makeBufferManager("try-allocate", root.get());
  BmPressureAwareBlockArena arena(bufferManager, memory::bm::MemoryTag::kWindow);

  const auto canReclaim = [](uint32_t) { return true; };
  const auto first = arena.allocateBlock(8 << 20, canReclaim);
  const auto second = arena.allocateBlock(8 << 20, canReclaim);

  const auto failed = arena.tryAllocateBlock(8 << 20);

  EXPECT_FALSE(failed.has_value());
  EXPECT_TRUE(arena.block(first).pinnedHandle.has_value());
  EXPECT_TRUE(arena.block(second).pinnedHandle.has_value());
  EXPECT_NE(nullptr, arena.block(first).data);
  EXPECT_NE(nullptr, arena.block(second).data);
}

TEST_F(BmPressureAwareBlockArenaTest, TryPinnedDataReturnsNullOnPressure) {
  auto root = memoryManager_.addRootPool(
      "bm-pressure-aware-arena-try-pin-root",
      16 << 20,
      memory::MemoryReclaimer::create());
  auto bufferManager = makeBufferManager("try-pin", root.get());
  BmPressureAwareBlockArena arena(bufferManager, memory::bm::MemoryTag::kWindow);

  const auto canReclaim = [](uint32_t) { return true; };
  const auto target = arena.allocateBlock(8 << 20, canReclaim);
  const auto other = arena.allocateBlock(8 << 20, canReclaim);
  arena.block(target).pinnedHandle.reset();
  arena.block(target).data = nullptr;

  const auto* data = arena.tryPinnedData(target);

  EXPECT_EQ(nullptr, data);
  EXPECT_FALSE(arena.block(target).pinnedHandle.has_value());
  EXPECT_TRUE(arena.block(other).pinnedHandle.has_value());
  EXPECT_NE(nullptr, arena.block(other).data);
}

TEST_F(BmPressureAwareBlockArenaTest, PinBlocksBatchPinsResidentBlocks) {
  auto bufferManager = makeBufferManager("pin-blocks");
  BmPressureAwareBlockArena arena(bufferManager, memory::bm::MemoryTag::kWindow);

  const auto canReclaim = [](uint32_t) { return true; };
  const auto blockId = arena.allocateBlock(4096, canReclaim);
  auto* data = arena.activeData(blockId);
  std::memcpy(data, "arena-pin-blocks", 17);
  arena.block(blockId).usedBytes = 17;

  ASSERT_EQ(4096, arena.makeBlocksReclaimable(0, canReclaim));
  ASSERT_FALSE(arena.block(blockId).pinnedHandle.has_value());
  ASSERT_EQ(nullptr, arena.block(blockId).data);

  const std::vector<uint32_t> blockIds{blockId};
  const auto statsBeforePin = bufferManager->stats();
  arena.pinBlocks(blockIds, canReclaim);

  const auto statsAfterPin = bufferManager->stats();
  EXPECT_EQ(statsBeforePin.batchPinCount + 1, statsAfterPin.batchPinCount);
  EXPECT_EQ(0, statsAfterPin.spillReadCount);
  ASSERT_TRUE(arena.block(blockId).pinnedHandle.has_value());
  ASSERT_NE(nullptr, arena.block(blockId).data);
  EXPECT_EQ(0, std::memcmp(arena.block(blockId).data, "arena-pin-blocks", 17));
}

TEST_F(BmPressureAwareBlockArenaTest, SpillReclaimableBlocksAndPinReadback) {
  auto bufferManager = makeBufferManager("spill-readback");
  BmPressureAwareBlockArena arena(bufferManager, memory::bm::MemoryTag::kWindow);

  const auto canReclaim = [](uint32_t) { return true; };
  const auto blockId = arena.allocateBlock(
      memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge),
      canReclaim);
  auto* data = arena.activeData(blockId);
  std::memcpy(data, "arena-spill-readback", 21);
  arena.block(blockId).usedBytes = 21;

  try {
    const auto spilled = arena.spillReclaimableBlocks(0, canReclaim);
    EXPECT_EQ(1, spilled);
    EXPECT_FALSE(arena.block(blockId).pinnedHandle.has_value());
    EXPECT_EQ(nullptr, arena.block(blockId).data);

    const auto* pinned = arena.pinnedData(blockId, canReclaim);
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
