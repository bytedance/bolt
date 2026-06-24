#include "bolt/exec/bm/tests/BmRowContainerTestBase.h"

#include "bolt/common/memory/MemoryArbitrator.h"

#include <fmt/format.h>

#include <string>
#include <vector>

namespace bytedance::bolt::exec::bm {
namespace {

using bytedance::bolt::memory::bm::MemoryTag;

TEST_F(BmRowContainerTest, BulkReadSessionLoadsStablePointersWhenResident) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeInput();
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  ASSERT_EQ(SegmentState::kFinalizedFlushed, container.segmentState(segment));

  const auto batchPinsBeforeBegin = bufferManager_->stats().batchPinCount;
  ASSERT_TRUE(container.canBulkRead({&segment, 1}));
  auto session = container.beginBulkReadSegments({&segment, 1});
  auto rows = session.loadRows();
  EXPECT_EQ(batchPinsBeforeBegin + 1, bufferManager_->stats().batchPinCount);
  ASSERT_EQ(input->size(), rows.size());
  EXPECT_EQ(0, container.compare(rows[1], rows[3], 0));
  EXPECT_LT(container.compare(rows[1], rows[3], 1), 0);

  auto result = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, result);

  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("delta", flat->valueAt(0).str());
  EXPECT_EQ("alpha", flat->valueAt(1).str());
  EXPECT_EQ("charlie", flat->valueAt(2).str());
  EXPECT_EQ("bravo", flat->valueAt(3).str());
}

TEST_F(BmRowContainerTest, ReadOnlyWindowReadSessionListsAndLoadsRows) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeInput();
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  const auto batchPinsBeforeBegin = bufferManager_->stats().batchPinCount;
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = session.listRowIds();
  EXPECT_EQ(batchPinsBeforeBegin, bufferManager_->stats().batchPinCount);
  ASSERT_EQ(input->size(), rowIds.size());

  std::vector<RowId> reordered{rowIds[2], rowIds[0], rowIds[3], rowIds[1]};
  auto inputRows = session.loadRows({reordered.data(), reordered.size()});
  ASSERT_EQ(reordered.size(), inputRows.size());

  auto result = BaseVector::create(VARCHAR(), reordered.size(), pool());
  container.extractColumnResident(inputRows.data(), inputRows.size(), 1, result);

  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("charlie", flat->valueAt(0).str());
  EXPECT_EQ("delta", flat->valueAt(1).str());
  EXPECT_EQ("bravo", flat->valueAt(2).str());
  EXPECT_EQ("alpha", flat->valueAt(3).str());

  const auto* alpha = session.loadRow(rowIds[1]);
  auto single = BaseVector::create(VARCHAR(), 1, pool());
  const char* alphaRow = alpha;
  container.extractColumnResident(&alphaRow, 1, 1, single);
  EXPECT_EQ("alpha", single->asFlatVector<StringView>()->valueAt(0).str());
}

TEST_F(BmRowContainerTest, ReadOnlyWindowEvictCanReloadRows) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting,
      256 << 10);
  constexpr vector_size_t kRows = 4096;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      makeFlatVector<std::string>(kRows, [](auto row) {
        return fmt::format("reload-string-value-{}", row);
      }),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = window.listRowIds();
  ASSERT_EQ(kRows, rowIds.size());

  auto loaded = window.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());
  const auto writesBeforeEvict = bufferManager_->stats().spillWriteCount;
  EXPECT_GT(window.evictLoadedChunks(), 0);
  // StringView rebase mutates the row block, so eviction must write it back.
  EXPECT_GT(bufferManager_->stats().spillWriteCount, writesBeforeEvict);

  auto reloaded = window.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, reloaded.size());

  auto result = BaseVector::create(VARCHAR(), kRows, pool());
  container.extractColumnResident(reloaded.data(), reloaded.size(), 1, result);
  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("reload-string-value-0", flat->valueAt(0).str());
  EXPECT_EQ("reload-string-value-1024", flat->valueAt(1024).str());
  EXPECT_EQ("reload-string-value-4095", flat->valueAt(4095).str());
}

TEST_F(BmRowContainerTest, ReadOnlyWindowEvictDoesNotRewriteCleanBlocks) {
  BmRowContainer container(
      {BIGINT(), BIGINT()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting,
      256 << 10);
  constexpr vector_size_t kRows = 4096;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      makeFlatVector<int64_t>(kRows, [](auto row) { return row * 2; }),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = window.listRowIds();

  auto loaded = window.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());
  const auto writesBeforeFirstEvict = bufferManager_->stats().spillWriteCount;
  EXPECT_GT(window.evictLoadedChunks(), 0);
  const auto writesAfterFirstEvict = bufferManager_->stats().spillWriteCount;
  EXPECT_EQ(writesBeforeFirstEvict, writesAfterFirstEvict);

  loaded = window.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());
  EXPECT_GT(window.evictLoadedChunks(), 0);
  EXPECT_EQ(writesAfterFirstEvict, bufferManager_->stats().spillWriteCount);
}

TEST_F(BmRowContainerTest, ReadOnlyWindowReleaseUnpinsWithoutReclaiming) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting,
      256 << 10);
  constexpr vector_size_t kRows = 4096;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      makeFlatVector<std::string>(kRows, [](auto row) {
        return fmt::format("release-string-value-{}", row);
      }),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = window.listRowIds();
  ASSERT_EQ(kRows, rowIds.size());

  auto loaded = window.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());
  const auto statsBeforeRelease = bufferManager_->stats();
  EXPECT_GT(statsBeforeRelease.spillReadCount, 0);

  const auto released = window.releaseLoadedChunks();
  EXPECT_GT(released, 0);
  const auto statsAfterRelease = bufferManager_->stats();
  EXPECT_EQ(
      statsBeforeRelease.spillWriteCount, statsAfterRelease.spillWriteCount);
  EXPECT_EQ(
      statsBeforeRelease.spillWriteBytes, statsAfterRelease.spillWriteBytes);
  EXPECT_GT(statsAfterRelease.unpinnedResidentBytes, 0);
  EXPECT_GT(statsAfterRelease.evictionQueueSize, 0);

  loaded = window.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());
  const auto statsAfterReload = bufferManager_->stats();
  EXPECT_EQ(statsAfterRelease.spillReadCount, statsAfterReload.spillReadCount);

  auto result = BaseVector::create(VARCHAR(), kRows, pool());
  container.extractColumnResident(loaded.data(), loaded.size(), 1, result);
  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("release-string-value-0", flat->valueAt(0).str());
  EXPECT_EQ("release-string-value-1024", flat->valueAt(1024).str());
  EXPECT_EQ("release-string-value-4095", flat->valueAt(4095).str());
}

TEST_F(BmRowContainerTest, CanBulkReadReservesForUnpinnedResidentBlocks) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting,
      256 << 10);
  constexpr vector_size_t kRows = 4096;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      makeFlatVector<std::string>(kRows, [](auto row) {
        return fmt::format("can-bulk-read-string-value-{}", row);
      }),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = window.listRowIds();
  ASSERT_EQ(kRows, rowIds.size());

  auto loaded = window.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());
  ASSERT_GT(window.releaseLoadedChunks(), 0);
  ASSERT_GT(bufferManager_->stats().unpinnedResidentBytes, 0);

  memory::MemoryPool* bmPool = nullptr;
  root_->visitChildren([&](memory::MemoryPool* child) {
    bmPool = child;
    return false;
  });
  ASSERT_NE(nullptr, bmPool);

  const auto reservesBefore = bmPool->stats().numReserves;
  EXPECT_TRUE(container.canBulkRead({&segment, 1}));
  EXPECT_EQ(reservesBefore + 1, bmPool->stats().numReserves);
}

TEST_F(BmRowContainerTest, ReadOnlyWindowReloadsAfterMemoryPoolReclaim) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting,
      256 << 10);
  constexpr vector_size_t kRows = 4096;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      makeFlatVector<std::string>(kRows, [](auto row) {
        return fmt::format("reclaim-string-value-{}", row);
      }),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = window.listRowIds();
  ASSERT_EQ(kRows, rowIds.size());

  auto loaded = window.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());
  ASSERT_GT(window.releaseLoadedChunks(), 0);
  ASSERT_GT(bufferManager_->reclaimableBytes(), 0);

  const auto statsBeforeReclaim = bufferManager_->stats();
  memory::MemoryReclaimer::Stats reclaimStats;
  memory::ScopedMemoryArbitrationContext arbitrationContext(root_.get());
  const auto reclaimed =
      root_->reclaim(bufferManager_->reclaimableBytes(), 0, reclaimStats);
  EXPECT_GT(reclaimed, 0);
  const auto statsAfterReclaim = bufferManager_->stats();
  EXPECT_GT(statsAfterReclaim.reclaimCount, statsBeforeReclaim.reclaimCount);
  EXPECT_GT(statsAfterReclaim.reclaimedBytes, statsBeforeReclaim.reclaimedBytes);

  loaded = window.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());
  const auto statsAfterReload = bufferManager_->stats();
  EXPECT_GT(statsAfterReload.spillReadCount, statsAfterReclaim.spillReadCount);

  auto result = BaseVector::create(VARCHAR(), kRows, pool());
  container.extractColumnResident(loaded.data(), loaded.size(), 1, result);
  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("reclaim-string-value-0", flat->valueAt(0).str());
  EXPECT_EQ("reclaim-string-value-1024", flat->valueAt(1024).str());
  EXPECT_EQ("reclaim-string-value-4095", flat->valueAt(4095).str());
}

TEST_F(BmRowContainerTest, ReadOnlyWindowEvictCanLimitTargetBytes) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting,
      64 << 10,
      64 << 10);
  constexpr vector_size_t kRows = 4096;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      makeFlatVector<std::string>(kRows, [](auto row) {
        return fmt::format("limited-evict-string-value-{}", row);
      }),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = window.listRowIds();
  auto loaded = window.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());

  const auto reclaimed = window.evictLoadedChunks(1);
  EXPECT_GT(reclaimed, 0);
  EXPECT_LT(reclaimed, bufferManager_->stats().spillWriteBytes);

  const auto remaining = window.evictLoadedChunks();
  EXPECT_GT(remaining, 0);
}

TEST_F(BmRowContainerTest, WindowReadRebasesStringsAcrossChunks) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting,
      256 << 10);
  constexpr vector_size_t kRows = 30000;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      makeFlatVector<std::string>(kRows, [](auto row) {
        return fmt::format("window-read-string-value-{}", row);
      }),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = session.listRowIds();
  ASSERT_EQ(kRows, rowIds.size());

  auto inputRows = session.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, inputRows.size());
  auto result = BaseVector::create(VARCHAR(), kRows, pool());
  container.extractColumnResident(inputRows.data(), inputRows.size(), 1, result);
  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("window-read-string-value-0", flat->valueAt(0).str());
  EXPECT_EQ("window-read-string-value-1024", flat->valueAt(1024).str());
  EXPECT_EQ("window-read-string-value-2048", flat->valueAt(2048).str());
  EXPECT_EQ("window-read-string-value-29999", flat->valueAt(29999).str());
}

TEST_F(BmRowContainerTest, WindowReadRebasesMultipleStringColumns) {
  BmRowContainer container(
      {BIGINT(), VARCHAR(), VARCHAR()},
      {false, false, false},
      bufferManager_,
      MemoryTag::kTesting,
      4 << 20,
      256);
  constexpr vector_size_t kRows = 32;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      makeFlatVector<std::string>(kRows, [](auto row) {
        return fmt::format("left-string-value-{:04}", row);
      }),
      makeFlatVector<std::string>(kRows, [](auto row) {
        return fmt::format("right-string-value-{:04}", row);
      }),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = session.listRowIds();
  ASSERT_EQ(kRows, rowIds.size());

  auto inputRows = session.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, inputRows.size());

  auto left = BaseVector::create(VARCHAR(), kRows, pool());
  container.extractColumnResident(inputRows.data(), inputRows.size(), 1, left);
  auto leftFlat = left->asFlatVector<StringView>();
  ASSERT_NE(nullptr, leftFlat);

  auto right = BaseVector::create(VARCHAR(), kRows, pool());
  container.extractColumnResident(inputRows.data(), inputRows.size(), 2, right);
  auto rightFlat = right->asFlatVector<StringView>();
  ASSERT_NE(nullptr, rightFlat);

  EXPECT_EQ("left-string-value-0000", leftFlat->valueAt(0).str());
  EXPECT_EQ("right-string-value-0000", rightFlat->valueAt(0).str());
  EXPECT_EQ("left-string-value-0017", leftFlat->valueAt(17).str());
  EXPECT_EQ("right-string-value-0017", rightFlat->valueAt(17).str());
  EXPECT_EQ("left-string-value-0031", leftFlat->valueAt(31).str());
  EXPECT_EQ("right-string-value-0031", rightFlat->valueAt(31).str());
}

} // namespace
} // namespace bytedance::bolt::exec::bm
