#include "bolt/exec/bm/tests/BmRowContainerTestUtil.h"

namespace bytedance::bolt::exec {
namespace {

TEST_F(BmRowContainerTest, SpillsColdBlocksAndReadsThemBack) {
  auto limitedRoot = memoryManager_.addRootPool(
      "bm-row-container-spill-root",
      64 * 1024 * 1024,
      memory::MemoryReclaimer::create());
  auto bufferManager = makeBufferManager("spill", limitedRoot.get());
  BmRowContainer container({BIGINT()}, {}, bufferManager);

  auto input = makeBigintVector(leaf_.get(), {42});
  DecodedVector decoded(*input);

  std::vector<RowId> sampledRows;
  try {
    while (container.allocatedBytes() < 6 * kLargeBlockBytes) {
      auto row = container.newRow();
      container.store(decoded, 0, row, 0);
      if (sampledRows.empty() ||
          row.blockId != sampledRows.back().blockId) {
        sampledRows.push_back(row);
      }
    }
  } catch (const std::exception& e) {
    if (isIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }

  ASSERT_GE(sampledRows.size(), 6);
  try {
    container.spillAllBlocks();
  } catch (const std::exception& e) {
    if (isIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }
  const auto statsAfterSpill = bufferManager->stats();
  EXPECT_GE(statsAfterSpill.spillWriteCount, 1);
  EXPECT_GE(statsAfterSpill.spillWriteBytes, kLargeBlockBytes);

  for (const auto row : {sampledRows.front(), sampledRows.back()}) {
    auto result = BaseVector::create(BIGINT(), 1, leaf_.get());
    try {
      container.extractColumn(&row, 1, 0, result);
    } catch (const std::exception& e) {
      if (isIoUringUnavailable(e)) {
        GTEST_SKIP() << e.what();
      }
      throw;
    }

    auto* actual = result->asFlatVector<int64_t>();
    EXPECT_FALSE(result->isNullAt(0));
    EXPECT_EQ(42, actual->valueAt(0));
  }
  EXPECT_GE(bufferManager->stats().spillReadCount, 1);
}


TEST_F(BmRowContainerTest, PreloadBatchPinsSpilledBlocksForLaterAccess) {
  auto limitedRoot = memoryManager_.addRootPool(
      "bm-row-container-preload-root",
      64 * 1024 * 1024,
      memory::MemoryReclaimer::create());
  auto bufferManager = makeBufferManager("preload", limitedRoot.get());
  BmRowContainer container(
      {BIGINT()}, {}, bufferManager, memory::bm::MemoryTag::kWindow, 4096);

  auto input = makeBigintVector(leaf_.get(), {99});
  DecodedVector decoded(*input);

  std::vector<RowId> sampledRows;
  while (container.allocatedBytes() < 3 * 4096) {
    auto row = container.newRow();
    container.store(decoded, 0, row, 0);
    if (sampledRows.empty() || row.blockId != sampledRows.back().blockId) {
      sampledRows.push_back(row);
    }
  }
  ASSERT_GE(sampledRows.size(), 3);

  try {
    container.spillAllBlocks();
  } catch (const std::exception& e) {
    if (isIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }
  ASSERT_GE(bufferManager->stats().spillWriteCount, 1);

  std::vector<BlockId> blockIds{sampledRows.front().blockId};
  const auto statsBeforePreload = bufferManager->stats();
  try {
    container.preload(blockIds);
  } catch (const std::exception& e) {
    if (isIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }
  const auto statsAfterPreload = bufferManager->stats();
  EXPECT_EQ(
      statsBeforePreload.batchPinCount + 1,
      statsAfterPreload.batchPinCount);
  EXPECT_GE(
      statsAfterPreload.spillReadCount,
      statsBeforePreload.spillReadCount + 1);

  auto result = BaseVector::create(BIGINT(), 1, leaf_.get());
  container.extractColumn(&sampledRows.front(), 1, 0, result);
  EXPECT_EQ(
      statsAfterPreload.spillReadCount,
      bufferManager->stats().spillReadCount);
  EXPECT_EQ(99, result->asFlatVector<int64_t>()->valueAt(0));
}


TEST_F(BmRowContainerTest, SpillsVariableWidthHeapBlocksAndReadsThemBack) {
  auto limitedRoot = memoryManager_.addRootPool(
      "bm-row-container-heap-spill-root",
      2 * 1024 * 1024,
      memory::MemoryReclaimer::create());
  auto bufferManager = makeBufferManager("heap-spill", limitedRoot.get());
  constexpr uint32_t kBlockSize = 4096;
  BmRowContainer container(
      {VARCHAR()},
      {},
      bufferManager,
      memory::bm::MemoryTag::kWindow,
      kBlockSize,
      kBlockSize);

  const std::string payload(1024, 'h');
  auto input = makeVarcharVector(leaf_.get(), {StringView(payload)});
  DecodedVector decoded(*input);

  std::vector<RowId> rows;
  try {
    while (container.heapAllocatedBytes() < 6 * kBlockSize) {
      auto row = container.newRow();
      container.store(decoded, 0, row, 0);
      if (rows.empty() || row.blockId != rows.back().blockId) {
        rows.push_back(row);
      }
    }
  } catch (const std::exception& e) {
    if (isIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }

  try {
    container.spillAllBlocks();
  } catch (const std::exception& e) {
    if (isIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }
  EXPECT_GE(bufferManager->stats().spillWriteCount, 1);
  auto result = BaseVector::create(VARCHAR(), 1, leaf_.get());
  try {
    container.extractColumn(&rows.front(), 1, 0, result);
  } catch (const std::exception& e) {
    if (isIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }
  EXPECT_EQ(payload, result->asFlatVector<StringView>()->valueAt(0).getString());
  EXPECT_GE(bufferManager->stats().spillReadCount, 1);
}


} // namespace
} // namespace bytedance::bolt::exec
