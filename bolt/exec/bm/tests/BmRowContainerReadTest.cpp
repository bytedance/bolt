/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "bolt/exec/bm/tests/BmRowContainerTestBase.h"

#include "bolt/common/memory/MemoryArbitrator.h"

#include <fmt/format.h>
#include <folly/ScopeGuard.h>

#include <algorithm>
#include <array>
#include <optional>
#include <string>
#include <vector>

namespace bytedance::bolt::exec::bm {

class ReadOnlyWindowReadSessionTestHelper {
 public:
  static size_t trackedChunks(const ReadOnlyWindowReadSession& session) {
    EXPECT_EQ(session.loadedChunks_.size(), session.loadedChunkKeys_.size());
    for (const auto& [segment, chunk] : session.loadedChunks_) {
      EXPECT_EQ(
          1,
          session.loadedChunkKeys_.count(
              (static_cast<uint64_t>(segment) << 32) | chunk));
    }
    return session.loadedChunks_.size();
  }

  static const ChunkData& leasedChunk(
      const ReadOnlyWindowReadSession& session) {
    return *session.activeLeases_.begin()->second.chunk;
  }
};

namespace {

using bytedance::bolt::memory::bm::MemoryTag;

uint64_t reclaimReadOnlyRows(
    memory::MemoryPool* root,
    memory::bm::BufferManager* bufferManager) {
  const auto before = bufferManager->stats().reclaimedBytes;
  memory::MemoryReclaimer::Stats stats;
  memory::ScopedMemoryArbitrationContext context(root);
  root->reclaim(bufferManager->reclaimableBytes(), 0, stats);
  return bufferManager->stats().reclaimedBytes - before;
}

TEST_F(BmRowContainerDiskIoTest, ComplexMapSurvivesRepeatedSpillReload) {
  auto maps = makeMapVector<int64_t, int64_t>({
      {{2, 20}, {1, 10}},
      {{1, 10}, {2, 20}},
  });
  const std::string largeValue(1 << 20, 'q');
  auto largeArrays = makeArrayVector<std::string>({
      {largeValue},
      {largeValue},
  });
  auto input = makeRowVector({maps, largeArrays});
  BmRowContainer container(
      {maps->type(), largeArrays->type()},
      {false, false},
      1,
      bufferManager_,
      MemoryTag::kTesting,
      128,
      64);
  storeAll(container, input);
  const auto segment = container.spillActiveSegment();

  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto rowIds = session.listRowIds();
  ASSERT_EQ(2, rowIds.size());
  for (int iteration = 0; iteration < 3; ++iteration) {
    auto rows = session.loadRowsScoped({rowIds.data(), rowIds.size()});
    ASSERT_EQ(2, rows.size());
    EXPECT_EQ(0, container.compare(rows.data()[0], rows.data()[1], 0));
    EXPECT_EQ(
        container.hash(rows.data()[0], 0), container.hash(rows.data()[1], 0));

    auto result = BaseVector::create(maps->type(), rows.size(), pool());
    container.extractColumnResident(rows.data(), rows.size(), 0, result, true);
    auto* resultMap = result->as<MapVector>();
    ASSERT_NE(nullptr, resultMap);
    EXPECT_EQ(1, resultMap->mapKeys()->asFlatVector<int64_t>()->valueAt(0));
    EXPECT_EQ(1, resultMap->mapKeys()->asFlatVector<int64_t>()->valueAt(2));

    auto largeResult =
        BaseVector::create(largeArrays->type(), rows.size(), pool());
    container.extractColumnResident(
        rows.data(), rows.size(), 1, largeResult, true);
    test::assertEqualVectors(largeArrays, largeResult);
    rows.reset();
    EXPECT_GT(session.evictLoadedChunks(), 0);
  }
}

TEST_F(BmRowContainerDiskIoTest, OpenZlSpillRoundTripsFixedAndVariableRows) {
  resetBufferManagerCompression(
      memory::bm::compress::CompressionKind::kOpenZlFrame);
  constexpr vector_size_t kRows = 2048;
  std::vector<std::optional<int64_t>> integers;
  std::vector<std::optional<std::string>> strings;
  integers.reserve(kRows);
  strings.reserve(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    if (row % 17 == 0) {
      integers.push_back(std::nullopt);
    } else {
      integers.push_back(static_cast<int64_t>(row) * 101 - 7);
    }
    if (row % 19 == 0) {
      strings.push_back(std::nullopt);
    } else {
      strings.push_back(fmt::format(
          "openzl-row-{}-{}",
          row,
          std::string(row % 2 == 0 ? 8 : 512, 'a' + row % 23)));
    }
  }
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>(integers),
      makeFlatVector<int32_t>(kRows, [](auto row) { return row * 3; }),
      makeFlatVector<double>(
          kRows, [](auto row) { return static_cast<double>(row) / 7.0; }),
      makeNullableFlatVector<std::string>(strings),
  });
  BmRowContainer container(
      {BIGINT(), INTEGER(), DOUBLE(), VARCHAR()},
      {true, false, false, true},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      64 << 10,
      64 << 10);
  storeAll(container, input);

  const auto segment = container.spillActiveSegment();
  auto session = container.beginBulkReadSegments({&segment, 1});
  auto rows = session.loadRows();
  ASSERT_EQ(kRows, rows.size());

  auto integerResult = BaseVector::create(BIGINT(), kRows, pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, integerResult);
  auto integerFlat = integerResult->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, integerFlat);
  EXPECT_TRUE(integerFlat->isNullAt(0));
  EXPECT_EQ(101 * 18 - 7, integerFlat->valueAt(18));
  EXPECT_EQ(101 * 2047 - 7, integerFlat->valueAt(2047));

  auto stringResult = BaseVector::create(VARCHAR(), kRows, pool());
  container.extractColumnResident(rows.data(), rows.size(), 3, stringResult);
  auto stringFlat = stringResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, stringFlat);
  EXPECT_TRUE(stringFlat->isNullAt(0));
  EXPECT_EQ(strings[17]->size(), stringFlat->valueAt(17).size());
  EXPECT_EQ(*strings[1024], stringFlat->valueAt(1024).str());
  EXPECT_EQ(*strings[2047], stringFlat->valueAt(2047).str());

  const auto stats = bufferManager_->stats();
  EXPECT_GT(stats.spillCompressedBlocks, 0);
}

TEST_F(
    BmRowContainerDiskIoTest,
    BulkReadSessionLoadsStablePointersWhenResident) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
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

TEST_F(BmRowContainerTest, ExtractColumnResidentWritesAtResultOffset) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {true, true},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({10, std::nullopt, 30}),
      makeNullableFlatVector<std::string>({"alpha", std::nullopt, "charlie"}),
  });
  auto rows = storeAll(container, input);

  auto numbers = makeNullableFlatVector<int64_t>({-1, -2, -3, -4, -5});
  container.extractColumnResident(rows.data(), rows.size(), 0, 2, numbers);
  ASSERT_EQ(5, numbers->size());
  EXPECT_EQ(-1, numbers->valueAt(0));
  EXPECT_EQ(-2, numbers->valueAt(1));
  EXPECT_EQ(10, numbers->valueAt(2));
  EXPECT_TRUE(numbers->isNullAt(3));
  EXPECT_EQ(30, numbers->valueAt(4));

  auto strings = makeNullableFlatVector<std::string>(
      {"prefix", std::nullopt, "old-0", "old-1", "old-2"});
  container.extractColumnResident(rows.data(), rows.size(), 1, 2, strings);
  ASSERT_EQ(5, strings->size());
  EXPECT_EQ("prefix", strings->valueAt(0).str());
  EXPECT_TRUE(strings->isNullAt(1));
  EXPECT_EQ("alpha", strings->valueAt(2).str());
  EXPECT_TRUE(strings->isNullAt(3));
  EXPECT_EQ("charlie", strings->valueAt(4).str());
}

TEST_F(BmRowContainerDiskIoTest, BulkReadSessionLoadSkipsConsumedChunks) {
  BmRowContainer container(
      {BIGINT()}, {false}, 0, bufferManager_, MemoryTag::kTesting, 64 << 10);
  constexpr vector_size_t kRows = 9000;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  container.popFrontRows(8192);

  auto session = container.beginBulkReadSegments({&segment, 1});
  EXPECT_NO_THROW(session.load());

  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  std::vector<SegmentRowRange> ranges{{segment, 8192, 3}};
  auto rows = window.loadRowsScoped({ranges.data(), ranges.size()});
  ASSERT_EQ(3, rows.size());

  auto result = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, result);
  auto flat = result->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ(8192, flat->valueAt(0));
  EXPECT_EQ(8193, flat->valueAt(1));
  EXPECT_EQ(8194, flat->valueAt(2));
}

TEST_F(BmRowContainerDiskIoTest, BulkReadSessionLoadsSegmentRowRanges) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeInput();
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto session = container.beginBulkReadSegments({&segment, 1});
  session.load();

  std::vector<SegmentRowRange> ranges{
      {segment, 1, 2},
      {segment, 3, 1},
  };
  auto inputRows = session.loadRows({ranges.data(), ranges.size()});
  ASSERT_EQ(3, inputRows.size());

  auto result = BaseVector::create(VARCHAR(), inputRows.size(), pool());
  container.extractColumnResident(
      inputRows.data(), inputRows.size(), 1, result);

  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("alpha", flat->valueAt(0).str());
  EXPECT_EQ("charlie", flat->valueAt(1).str());
  EXPECT_EQ("bravo", flat->valueAt(2).str());
}

TEST_F(BmRowContainerDiskIoTest, ReadOnlyWindowReadSessionListsAndLoadsRows) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
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
  auto inputRows = session.loadRowsScoped({reordered.data(), reordered.size()});
  ASSERT_EQ(reordered.size(), inputRows.size());

  auto result = BaseVector::create(VARCHAR(), reordered.size(), pool());
  container.extractColumnResident(
      inputRows.data(), inputRows.size(), 1, result);

  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("charlie", flat->valueAt(0).str());
  EXPECT_EQ("delta", flat->valueAt(1).str());
  EXPECT_EQ("bravo", flat->valueAt(2).str());
  EXPECT_EQ("alpha", flat->valueAt(3).str());

  auto alpha = session.loadRowsScoped({&rowIds[1], 1});
  auto single = BaseVector::create(VARCHAR(), 1, pool());
  const char* alphaRow = alpha.data()[0];
  container.extractColumnResident(&alphaRow, 1, 1, single);
  EXPECT_EQ("alpha", single->asFlatVector<StringView>()->valueAt(0).str());
}

TEST_F(
    BmRowContainerDiskIoTest,
    ReadOnlyWindowReadSessionLoadsSegmentRowRanges) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeInput();
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  std::vector<SegmentRowRange> ranges{
      {segment, 1, 2},
      {segment, 3, 1},
  };
  auto inputRows = session.loadRowsScoped({ranges.data(), ranges.size()});
  ASSERT_EQ(3, inputRows.size());

  auto result = BaseVector::create(VARCHAR(), inputRows.size(), pool());
  container.extractColumnResident(
      inputRows.data(), inputRows.size(), 1, result);

  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("alpha", flat->valueAt(0).str());
  EXPECT_EQ("charlie", flat->valueAt(1).str());
  EXPECT_EQ("bravo", flat->valueAt(2).str());
}

TEST_F(
    BmRowContainerDiskIoTest,
    ReadOnlyWindowReadSessionLoadsChunkBoundedRows) {
  constexpr vector_size_t kRows = 9'000;
  BmRowContainer container(
      {BIGINT()}, {false}, 0, bufferManager_, MemoryTag::kTesting, 32 << 10);
  storeAll(
      container,
      makeRowVector({
          makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      }));

  const auto segment = container.spillActiveSegment();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto rowIds = session.listRowIds();
  const auto chunkBoundary =
      std::find_if(rowIds.begin() + 1, rowIds.end(), [&](const auto& row) {
        return row.rowBlockId != rowIds.front().rowBlockId;
      });
  ASSERT_NE(rowIds.end(), chunkBoundary);
  const auto boundary = static_cast<RowNumber>(chunkBoundary - rowIds.begin());
  ASSERT_GE(boundary, 2);

  const SegmentRowRange range{segment, boundary - 2, 4};
  {
    auto rows = session.loadChunkBoundedRowsScoped(range);
    ASSERT_EQ(2, rows.size());
    auto result = BaseVector::create(BIGINT(), rows.size(), pool());
    container.extractColumnResident(rows.data(), rows.size(), 0, result);
    EXPECT_EQ(boundary - 2, result->asFlatVector<int64_t>()->valueAt(0));
    EXPECT_EQ(boundary - 1, result->asFlatVector<int64_t>()->valueAt(1));
  }
  EXPECT_EQ(0, bufferManager_->stats().pinnedResidentBytes);
}

TEST_F(BmRowContainerDiskIoTest, ConsumedChunkReleaseAvoidsDirtyWriteback) {
  constexpr vector_size_t kRows = 512;
  for (const bool releaseConsumedChunk : {false, true}) {
    BmRowContainer container(
        {BIGINT(), VARCHAR()},
        {false, false},
        0,
        bufferManager_,
        MemoryTag::kTesting,
        64 << 10,
        32 << 10);
    storeAll(
        container,
        makeRowVector({
            makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
            makeFlatVector<std::string>(
                kRows,
                [](auto row) { return std::string(256, 'a' + row % 26); }),
        }));
    const auto segment = container.spillActiveSegment();
    auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
    auto output = BaseVector::create(VARCHAR(), kRows, pool());
    {
      const SegmentRowRange range{segment, 0, kRows};
      auto rows = session.loadChunkBoundedRowsScoped(range);
      ASSERT_EQ(kRows, rows.size());
      container.extractColumnResident(rows.data(), rows.size(), 1, output);
    }
    ASSERT_EQ(0, bufferManager_->stats().pinnedResidentBytes);
    const auto writesBefore = bufferManager_->stats().spillWriteCount;
    if (releaseConsumedChunk) {
      session.releaseConsumedChunk({segment, 0, kRows});
      EXPECT_EQ(0, bufferManager_->stats().unpinnedResidentBytes);
    } else {
      EXPECT_GT(bufferManager_->stats().unpinnedResidentBytes, 0);
    }
    bufferManager_->Reclaim(kUnlimitedBytes);
    const auto extraWrites =
        bufferManager_->stats().spillWriteCount - writesBefore;
    RecordProperty(
        releaseConsumedChunk ? "released_extra_write_blocks"
                             : "retained_extra_write_blocks",
        static_cast<int>(extraWrites));
    if (releaseConsumedChunk) {
      EXPECT_EQ(0, extraWrites);
    } else {
      EXPECT_GT(extraWrites, 0);
    }
    // The copied output remains valid after either reclaim or destruction of
    // the original chunk, including all of its non-inline string payloads.
    for (vector_size_t row = 0; row < kRows; ++row) {
      EXPECT_EQ(
          std::string(256, 'a' + row % 26),
          output->asFlatVector<StringView>()->valueAt(row).str());
    }
  }
}

TEST_F(
    BmRowContainerDiskIoTest,
    ConsumedChunkReleaseKeepsPartialAndLeasedChunks) {
  constexpr vector_size_t kRows = 512;
  BmRowContainer container(
      {BIGINT()}, {false}, 0, bufferManager_, MemoryTag::kTesting, 64 << 10);
  storeAll(
      container, makeRowVector({makeFlatVector<int64_t>(kRows, [](auto row) {
        return row;
      })}));
  {
    const auto activeSegment = container.activeSegmentId();
    auto activeSession =
        container.beginReadOnlyWindowReadSegments({&activeSegment, 1});
    activeSession.releaseConsumedChunk({activeSegment, 0, kRows});
    // Active storage remains writable and readable even at its current end.
    auto activeRows =
        activeSession.loadChunkBoundedRowsScoped({activeSegment, 0, kRows});
    ASSERT_EQ(kRows, activeRows.size());
    auto output = BaseVector::create(BIGINT(), kRows, pool());
    container.extractColumnResident(activeRows.data(), kRows, 0, output);
    EXPECT_EQ(kRows - 1, output->asFlatVector<int64_t>()->valueAt(kRows - 1));
  }
  const auto segment = container.spillActiveSegment();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rows = session.loadChunkBoundedRowsScoped({segment, 0, kRows});
  ASSERT_EQ(kRows, rows.size());
  session.releaseConsumedChunk({segment, 0, kRows});
  EXPECT_GT(bufferManager_->stats().pinnedResidentBytes, 0);
  rows.reset();
  session.releaseConsumedChunk({segment, 0, kRows / 2});
  EXPECT_GT(bufferManager_->stats().unpinnedResidentBytes, 0);
  {
    // The rest of a shared physical chunk must remain readable, even when
    // the earlier rows belong to a completed partition.
    auto remaining =
        session.loadChunkBoundedRowsScoped({segment, kRows / 2, kRows / 2});
    auto output = BaseVector::create(BIGINT(), remaining.size(), pool());
    container.extractColumnResident(
        remaining.data(), remaining.size(), 0, output);
    for (vector_size_t i = 0; i < remaining.size(); ++i) {
      EXPECT_EQ(kRows / 2 + i, output->asFlatVector<int64_t>()->valueAt(i));
    }
  }
  session.releaseConsumedChunk({segment, kRows / 2, kRows / 2});
  EXPECT_EQ(0, bufferManager_->stats().unpinnedResidentBytes);
}

TEST_F(BmRowContainerTest, PopFrontRowsKeepsLaterSegmentRangesReadable) {
  BmRowContainer container(
      {BIGINT()}, {false}, 0, bufferManager_, MemoryTag::kTesting, 64 << 10);
  constexpr vector_size_t kRows = 9000;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
  });
  storeAll(container, input);

  auto segment = container.activeSegmentId();
  container.popFrontRows(8192);

  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  std::vector<SegmentRowRange> ranges{{segment, 8192, 3}};
  auto inputRows = session.loadRowsScoped({ranges.data(), ranges.size()});
  ASSERT_EQ(3, inputRows.size());

  auto result = BaseVector::create(BIGINT(), inputRows.size(), pool());
  container.extractColumnResident(
      inputRows.data(), inputRows.size(), 0, result);

  auto flat = result->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ(8192, flat->valueAt(0));
  EXPECT_EQ(8193, flat->valueAt(1));
  EXPECT_EQ(8194, flat->valueAt(2));
}

TEST_F(BmRowContainerDiskIoTest, ReadOnlyWindowEvictCanReloadRows) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      256 << 10);
  constexpr vector_size_t kRows = 4096;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      makeFlatVector<std::string>(
          kRows,
          [](auto row) { return fmt::format("reload-string-value-{}", row); }),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = window.listRowIds();
  ASSERT_EQ(kRows, rowIds.size());

  auto loaded = window.loadRowsScoped({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());
  const auto writesBeforeEvict = bufferManager_->stats().spillWriteCount;
  loaded.reset();
  EXPECT_GT(window.evictLoadedChunks(), 0);
  // StringView rebase mutates the row block, so eviction must write it back.
  EXPECT_GT(bufferManager_->stats().spillWriteCount, writesBeforeEvict);

  auto reloaded = window.loadRowsScoped({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, reloaded.size());

  auto result = BaseVector::create(VARCHAR(), kRows, pool());
  container.extractColumnResident(reloaded.data(), reloaded.size(), 1, result);
  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("reload-string-value-0", flat->valueAt(0).str());
  EXPECT_EQ("reload-string-value-1024", flat->valueAt(1024).str());
  EXPECT_EQ("reload-string-value-4095", flat->valueAt(4095).str());
}

TEST_F(BmRowContainerDiskIoTest, ReadOnlyWindowEvictDoesNotRewriteCleanBlocks) {
  BmRowContainer container(
      {BIGINT(), BIGINT()},
      {false, false},
      0,
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

  auto loaded = window.loadRowsScoped({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());
  const auto writesBeforeFirstEvict = bufferManager_->stats().spillWriteCount;
  loaded.reset();
  EXPECT_GT(window.evictLoadedChunks(), 0);
  const auto writesAfterFirstEvict = bufferManager_->stats().spillWriteCount;
  EXPECT_EQ(writesBeforeFirstEvict, writesAfterFirstEvict);

  loaded = window.loadRowsScoped({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());
  loaded.reset();
  EXPECT_GT(window.evictLoadedChunks(), 0);
  EXPECT_EQ(writesAfterFirstEvict, bufferManager_->stats().spillWriteCount);
}

TEST_F(
    BmRowContainerDiskIoTest,
    ReadOnlyWindowFixedWidthRowsSurviveRepeatedReclaim) {
  BmRowContainer container(
      {BIGINT(), BIGINT()},
      {false, false},
      0,
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

  for (int iteration = 0; iteration < 3; ++iteration) {
    auto loaded = window.loadRowsScoped({rowIds.data(), rowIds.size()});
    ASSERT_EQ(kRows, loaded.size());
    auto result = BaseVector::create(BIGINT(), kRows, pool());
    container.extractColumnResident(loaded.data(), loaded.size(), 1, result);
    auto* flat = result->asFlatVector<int64_t>();
    ASSERT_NE(nullptr, flat);
    EXPECT_EQ(0, flat->valueAt(0));
    EXPECT_EQ(2048, flat->valueAt(1024));
    EXPECT_EQ(8190, flat->valueAt(4095));
    loaded.reset();
    EXPECT_EQ(0, bufferManager_->stats().pinnedResidentBytes);
    EXPECT_GT(reclaimReadOnlyRows(root_.get(), bufferManager_.get()), 0);
  }
}

TEST_F(BmRowContainerDiskIoTest, ReadOnlyWindowReleaseUnpinsWithoutReclaiming) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      256 << 10);
  constexpr vector_size_t kRows = 4096;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      makeFlatVector<std::string>(
          kRows,
          [](auto row) { return fmt::format("release-string-value-{}", row); }),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = window.listRowIds();
  ASSERT_EQ(kRows, rowIds.size());

  auto loaded = window.loadRowsScoped({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());
  const auto statsBeforeRelease = bufferManager_->stats();
  EXPECT_GT(statsBeforeRelease.spillReadCount, 0);

  loaded.reset();
  EXPECT_EQ(0, bufferManager_->stats().pinnedResidentBytes);
  const auto statsAfterRelease = bufferManager_->stats();
  EXPECT_EQ(
      statsBeforeRelease.spillWriteCount, statsAfterRelease.spillWriteCount);
  EXPECT_EQ(
      statsBeforeRelease.spillWriteBytes, statsAfterRelease.spillWriteBytes);
  EXPECT_GT(statsAfterRelease.unpinnedResidentBytes, 0);
  EXPECT_GT(statsAfterRelease.evictionQueueSize, 0);

  loaded = window.loadRowsScoped({rowIds.data(), rowIds.size()});
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

TEST_F(BmRowContainerDiskIoTest, ScopedRowsSharePinsAndReleaseAllHeapBlocks) {
  constexpr vector_size_t kRows = 4096;
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      32 << 10,
      16 << 10);
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      makeFlatVector<std::string>(
          kRows,
          [](auto row) {
            return fmt::format("scoped-{}-{}", row, std::string(256, 'x'));
          }),
  });
  storeAll(container, input);
  const auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto ids = window.listRowIds();
  memory::MemoryPool* bmPool = nullptr;
  root_->visitChildren([&](memory::MemoryPool* child) {
    bmPool = child;
    return false;
  });
  ASSERT_NE(nullptr, bmPool);

  auto outer = window.loadRowsScoped({ids.data(), 1});
  const auto outerStats = bufferManager_->stats();
  // One row pins the entire chunk, including several separate heap blocks.
  ASSERT_GT(outerStats.pinnedResidentBytes, (32 + 16) << 10);
  const auto reserves = bmPool->stats().numReserves;
  {
    const RowId repeated[] = {ids.front(), ids.front()};
    auto inner = window.loadRowsScoped({repeated, 2});
    EXPECT_EQ(outer.data()[0], inner.data()[0]);
    EXPECT_EQ(inner.data()[0], inner.data()[1]);
    EXPECT_EQ(outerStats.pinCount, bufferManager_->stats().pinCount);
    EXPECT_EQ(reserves, bmPool->stats().numReserves);
    EXPECT_EQ(0, window.evictLoadedChunks());
  }
  EXPECT_EQ(
      outerStats.pinnedResidentBytes,
      bufferManager_->stats().pinnedResidentBytes);
  {
    auto distinct = window.loadRowsScoped({&ids.back(), 1});
    EXPECT_GT(
        bufferManager_->stats().pinnedResidentBytes,
        outerStats.pinnedResidentBytes);
  }
  EXPECT_EQ(
      outerStats.pinnedResidentBytes,
      bufferManager_->stats().pinnedResidentBytes);
  ASSERT_GT(bufferManager_->reclaimableBytes(), 0);
  {
    const auto reclaimedBefore = bufferManager_->stats().reclaimedBytes;
    memory::MemoryReclaimer::Stats stats;
    memory::ScopedMemoryArbitrationContext context(root_.get());
    root_->reclaim(bufferManager_->reclaimableBytes(), 0, stats);
    // Root reservation accounting is coarser than these small chunks.
    EXPECT_GT(bufferManager_->stats().reclaimedBytes, reclaimedBefore);
  }
  // Reclaiming the expired inner scope must not invalidate the outer string.
  auto result = BaseVector::create(VARCHAR(), 0, pool());
  container.extractColumnResident(outer.data(), outer.size(), 1, result);
  const auto expected = fmt::format("scoped-0-{}", std::string(256, 'x'));
  EXPECT_EQ(expected, result->asFlatVector<StringView>()->valueAt(0).str());
  const auto beforeRelease = bufferManager_->stats();
  outer.reset();
  outer.reset();
  EXPECT_TRUE(outer.empty());
  EXPECT_EQ(0, bufferManager_->stats().pinnedResidentBytes);
  EXPECT_EQ(
      beforeRelease.spillWriteCount, bufferManager_->stats().spillWriteCount);
  {
    memory::MemoryReclaimer::Stats stats;
    memory::ScopedMemoryArbitrationContext context(root_.get());
    EXPECT_GT(root_->reclaim(bufferManager_->reclaimableBytes(), 0, stats), 0);
  }
  EXPECT_EQ(expected, result->asFlatVector<StringView>()->valueAt(0).str());
  auto reloaded = window.loadRowsScoped({ids.data(), 1});
  container.extractColumnResident(reloaded.data(), reloaded.size(), 1, result);
  EXPECT_EQ(expected, result->asFlatVector<StringView>()->valueAt(0).str());
  EXPECT_GT(
      bufferManager_->stats().spillReadCount, beforeRelease.spillReadCount);
}

TEST_F(BmRowContainerDiskIoTest, ScopedRowsBoundedEvictionPrunesHistory) {
  constexpr vector_size_t kRows = 16384;
  BmRowContainer container(
      {BIGINT()}, {false}, 0, bufferManager_, MemoryTag::kTesting, 32 << 10);
  storeAll(
      container,
      makeRowVector({
          makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      }));
  const auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  SegmentRowRange remaining{segment, 0, kRows};
  size_t chunks = 0;
  while (remaining.count > 0) {
    auto loaded = window.loadChunkBoundedRowsScoped(remaining);
    ASSERT_GT(loaded.size(), 0);
    ASSERT_EQ(1, ReadOnlyWindowReadSessionTestHelper::trackedChunks(window));
    const auto count = static_cast<RowNumber>(loaded.size());
    loaded.reset();
    EXPECT_GT(window.evictLoadedChunks(1), 0);
    // Do not retain a growing prefix of already reclaimed chunks.
    EXPECT_EQ(0, ReadOnlyWindowReadSessionTestHelper::trackedChunks(window));
    remaining.begin += count;
    remaining.count -= count;
    ++chunks;
  }
  EXPECT_GT(chunks, 1);
}

TEST_F(BmRowContainerDiskIoTest, ScopedRowsEvictionPrunesExternalReclaim) {
  constexpr vector_size_t kRows = 4096;
  BmRowContainer container(
      {BIGINT()}, {false}, 0, bufferManager_, MemoryTag::kTesting, 256 << 10);
  storeAll(
      container,
      makeRowVector({
          makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      }));
  const auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto ids = window.listRowIds();
  auto loaded = window.loadRowsScoped({ids.data(), ids.size()});
  ASSERT_EQ(1, ReadOnlyWindowReadSessionTestHelper::trackedChunks(window));
  loaded.reset();
  EXPECT_GT(reclaimReadOnlyRows(root_.get(), bufferManager_.get()), 0);
  EXPECT_EQ(0, window.evictLoadedChunks(1));
  EXPECT_EQ(0, ReadOnlyWindowReadSessionTestHelper::trackedChunks(window));

  // Both registry structures must allow the same chunk to be registered again.
  loaded = window.loadRowsScoped({ids.data(), ids.size()});
  EXPECT_EQ(1, ReadOnlyWindowReadSessionTestHelper::trackedChunks(window));
  auto result = BaseVector::create(BIGINT(), kRows, pool());
  container.extractColumnResident(loaded.data(), loaded.size(), 0, result);
  EXPECT_EQ(kRows - 1, result->asFlatVector<int64_t>()->valueAt(kRows - 1));
  loaded.reset();
  EXPECT_GT(window.evictLoadedChunks(1), 0);
  EXPECT_EQ(0, ReadOnlyWindowReadSessionTestHelper::trackedChunks(window));
}

TEST_F(
    BmRowContainerDiskIoTest,
    ScopedRowsEvictionRetainsUnreclaimedHeapBlocks) {
  constexpr vector_size_t kRows = 64;
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      32 << 10,
      16 << 10);
  storeAll(
      container,
      makeRowVector({
          makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
          makeFlatVector<std::string>(
              kRows,
              [](auto row) {
                return fmt::format(
                    "retained-{}-{}", row, std::string(2048, 'x'));
              }),
      }));
  const auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto ids = window.listRowIds();
  auto loaded = window.loadRowsScoped({ids.data(), ids.size()});
  ASSERT_EQ(1, ReadOnlyWindowReadSessionTestHelper::trackedChunks(window));
  const auto& chunk = ReadOnlyWindowReadSessionTestHelper::leasedChunk(window);
  ASSERT_GT(chunk.heapBlocks.size(), 1);
  auto rowBlock = chunk.rowBlock.block;
  auto heapBlock = chunk.heapBlocks.front().block;
  loaded.reset();

  // Reclaim the row block while a separate BM handle protects one heap block.
  auto heapPin = bufferManager_->Pin(heapBlock);
  const std::array rowBlocks{rowBlock};
  bufferManager_->SpillBlocks(rowBlocks);
  ASSERT_TRUE(bufferManager_->IsSpilled(rowBlock));
  EXPECT_GT(window.evictLoadedChunks(1), 0);
  EXPECT_FALSE(bufferManager_->IsSpilled(heapBlock));
  EXPECT_EQ(1, ReadOnlyWindowReadSessionTestHelper::trackedChunks(window));

  heapPin = memory::bm::BufferHandle{};
  EXPECT_GT(window.evictLoadedChunks(1), 0);
  EXPECT_EQ(0, ReadOnlyWindowReadSessionTestHelper::trackedChunks(window));
  loaded = window.loadRowsScoped({ids.data(), 1});
  auto result = BaseVector::create(VARCHAR(), 1, pool());
  container.extractColumnResident(loaded.data(), loaded.size(), 1, result);
  EXPECT_EQ(
      "retained-0-" + std::string(2048, 'x'),
      result->asFlatVector<StringView>()->valueAt(0).str());
}

TEST_F(BmRowContainerDiskIoTest, ScopedRowsExplicitEvictionProtectsOuterBatch) {
  constexpr vector_size_t kRows = 4096;
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      32 << 10,
      16 << 10);
  storeAll(
      container,
      makeRowVector({
          makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
          makeFlatVector<std::string>(
              kRows,
              [](auto row) {
                return fmt::format("evict-{}-{}", row, std::string(256, 'x'));
              }),
      }));
  const auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto ids = window.listRowIds();
  { auto candidate = window.loadRowsScoped({ids.data(), 1}); }
  auto outer = window.loadRowsScoped({&ids.back(), 1});
  const auto pinned = bufferManager_->stats().pinnedResidentBytes;
  EXPECT_EQ(0, window.evictLoadedChunks(0));
  EXPECT_GT(window.evictLoadedChunks(1), 0);
  EXPECT_EQ(pinned, bufferManager_->stats().pinnedResidentBytes);
  EXPECT_EQ(1, ReadOnlyWindowReadSessionTestHelper::trackedChunks(window));
  {
    auto candidate = window.loadRowsScoped({&ids[kRows / 2], 1});
    ASSERT_GT(bufferManager_->stats().pinnedResidentBytes, pinned);
    EXPECT_EQ(0, window.evictLoadedChunks(1));
  }
  // The first candidate is already on disk and must not consume the byte
  // target before the next expired candidate can be reclaimed.
  EXPECT_GT(window.evictLoadedChunks(1), 0);
  EXPECT_EQ(pinned, bufferManager_->stats().pinnedResidentBytes);
  auto result = BaseVector::create(VARCHAR(), 0, pool());
  container.extractColumnResident(outer.data(), outer.size(), 1, result);
  const auto expected = fmt::format("evict-4095-{}", std::string(256, 'x'));
  EXPECT_EQ(expected, result->asFlatVector<StringView>()->valueAt(0).str());
  outer.reset();
  EXPECT_GT(window.evictLoadedChunks(), 0);
  EXPECT_EQ(0, window.evictLoadedChunks());
  auto reloaded = window.loadRowsScoped({&ids.back(), 1});
  container.extractColumnResident(reloaded.data(), reloaded.size(), 1, result);
  EXPECT_EQ(expected, result->asFlatVector<StringView>()->valueAt(0).str());
}

TEST_F(BmRowContainerDiskIoTest, ScopedRowsEvictionSkipsConsumedChunks) {
  constexpr vector_size_t kRows = 9000;
  BmRowContainer container(
      {BIGINT()}, {false}, 0, bufferManager_, MemoryTag::kTesting, 32 << 10);
  storeAll(
      container,
      makeRowVector({
          makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      }));
  const auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto ids = window.listRowIds();
  { auto loaded = window.loadRowsScoped({ids.data(), 1}); }
  container.popFrontRows(8192);
  EXPECT_THROW(window.loadRowsScoped({ids.data(), 1}), BoltException);
  { auto loaded = window.loadRowsScoped({&ids.back(), 1}); }
  EXPECT_GT(window.evictLoadedChunks(1), 0);
  EXPECT_EQ(0, ReadOnlyWindowReadSessionTestHelper::trackedChunks(window));
  EXPECT_EQ(0, window.evictLoadedChunks());
}

TEST_F(BmRowContainerDiskIoTest, ScopedRowsMoveAndUnwind) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  storeAll(container, makeInput());
  const auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto ids = window.listRowIds();
  {
    auto first = window.loadRowsScoped({ids.data(), ids.size()});
    const auto pinned = bufferManager_->stats().pinnedResidentBytes;
    auto second = std::move(first);
    EXPECT_TRUE(first.empty());
    auto third = window.loadRowsScoped({ids.data(), 1});
    third = std::move(second);
    EXPECT_TRUE(second.empty());
    auto* self = &third;
    third = std::move(*self);
    EXPECT_EQ(ids.size(), third.size());
    EXPECT_EQ(pinned, bufferManager_->stats().pinnedResidentBytes);
    EXPECT_THROW(
        ([&]() {
          auto nested = window.loadRowsScoped({ids.data(), 1});
          BOLT_FAIL("unwind scoped rows");
        }()),
        BoltException);
    EXPECT_EQ(pinned, bufferManager_->stats().pinnedResidentBytes);
    const SegmentRowRange invalid{
        segment, static_cast<RowNumber>(ids.size()), 1};
    EXPECT_THROW(window.loadRowsScoped({&invalid, 1}), BoltException);
  }
  EXPECT_EQ(0, bufferManager_->stats().pinnedResidentBytes);
  auto movedWindow = std::move(window);
  auto loaded = movedWindow.loadRowsScoped({ids.data(), 1});
  EXPECT_EQ(1, loaded.size());
}

TEST_F(BmRowContainerDiskIoTest, ScopedRowsPreserveBulkReaderPins) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  storeAll(container, makeInput());
  const auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto ids = window.listRowIds();
  // The session has seen this chunk before the bulk reader acquires it.
  {
    auto owned = window.loadRowsScoped({ids.data(), 1});
    EXPECT_GT(bufferManager_->stats().pinnedResidentBytes, 0);
  }
  EXPECT_EQ(0, bufferManager_->stats().pinnedResidentBytes);
  auto bulk = container.beginBulkReadSegments({&segment, 1});
  auto raw = bulk.loadRows();
  {
    auto scoped = window.loadRowsScoped({ids.data(), 1});
    EXPECT_EQ(raw.front(), scoped.data()[0]);
  }
  const auto pinned = bufferManager_->stats().pinnedResidentBytes;
  EXPECT_GT(pinned, 0);
  EXPECT_EQ(0, window.evictLoadedChunks());
  EXPECT_EQ(pinned, bufferManager_->stats().pinnedResidentBytes);
  auto result = BaseVector::create(VARCHAR(), raw.size(), pool());
  container.extractColumnResident(raw.data(), raw.size(), 1, result);
  EXPECT_EQ("alpha", result->asFlatVector<StringView>()->valueAt(1).str());
}

TEST_F(BmRowContainerDiskIoTest, ScopedRowsRollBackLeasesWhenReservationFails) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      32 << 10,
      16 << 10);
  constexpr vector_size_t kRows = 4096;
  storeAll(
      container,
      makeRowVector({
          makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
          makeFlatVector<std::string>(
              kRows, [](auto row) { return std::string(256, 'a' + row % 26); }),
      }));
  const auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto ids = window.listRowIds();
  {
    auto pressurePool = root_->addLeafChild("scoped-load-pressure");
    const auto pressureBytes = root_->freeBytes() - (64 << 10);
    auto* pressure = pressurePool->allocate(pressureBytes);
    auto guard = folly::makeGuard(
        [&]() { pressurePool->free(pressure, pressureBytes); });
    EXPECT_THROW(
        window.loadRowsScoped({ids.data(), ids.size()}), BoltException);
    EXPECT_EQ(0, bufferManager_->stats().pinnedResidentBytes);
  }
  // No stale count from the failed batch may prevent the successful batch
  // from releasing the same chunks.
  {
    auto loaded = window.loadRowsScoped({ids.data(), ids.size()});
    EXPECT_EQ(ids.size(), loaded.size());
    EXPECT_GT(bufferManager_->stats().pinnedResidentBytes, 0);
  }
  EXPECT_EQ(0, bufferManager_->stats().pinnedResidentBytes);
}

TEST_F(BmRowContainerDiskIoTest, CanBulkReadReservesForUnpinnedResidentBlocks) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      256 << 10);
  constexpr vector_size_t kRows = 4096;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      makeFlatVector<std::string>(
          kRows,
          [](auto row) {
            return fmt::format("can-bulk-read-string-value-{}", row);
          }),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = window.listRowIds();
  ASSERT_EQ(kRows, rowIds.size());

  auto loaded = window.loadRowsScoped({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());
  loaded.reset();
  ASSERT_EQ(0, bufferManager_->stats().pinnedResidentBytes);
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

TEST_F(BmRowContainerDiskIoTest, CanBulkReadRequiresProcessingHeadroom) {
  constexpr vector_size_t size = 50'000;
  constexpr uint32_t blockSize = 32 << 10;
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      blockSize,
      blockSize);
  auto input = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      makeFlatVector<std::string>(
          size, [](auto row) { return std::string(256, 'a' + row % 26); }),
  });
  storeAll(container, input);
  const auto segment = container.spillActiveSegment();

  const auto unloadedBytes = bufferManager_->stats().spilledBytes;
  ASSERT_GT(unloadedBytes, 0);
  const auto readSlack = unloadedBytes + unloadedBytes / 2;
  const auto freeBeforePressure = root_->freeBytes();
  ASSERT_GT(freeBeforePressure, readSlack);
  const auto pressureBytes = freeBeforePressure - readSlack;
  auto pressurePool = root_->addLeafChild("bulk-read-headroom-pressure");
  auto* pressure = pressurePool->allocate(pressureBytes);
  auto pressureGuard =
      folly::makeGuard([&]() { pressurePool->free(pressure, pressureBytes); });

  EXPECT_TRUE(container.canBulkRead({&segment, 1}, 1));
  EXPECT_FALSE(container.canBulkRead({&segment, 1}));

  auto session = container.beginBulkReadSegments({&segment, 1});
  EXPECT_NO_THROW(session.load());
  EXPECT_GT(bufferManager_->stats().pinnedResidentBytes, 0);
}

TEST_F(BmRowContainerDiskIoTest, ReadOnlyWindowReloadsAfterMemoryPoolReclaim) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      256 << 10);
  constexpr vector_size_t kRows = 4096;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      makeFlatVector<std::string>(
          kRows,
          [](auto row) { return fmt::format("reclaim-string-value-{}", row); }),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = window.listRowIds();
  ASSERT_EQ(kRows, rowIds.size());

  auto loaded = window.loadRowsScoped({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());
  loaded.reset();
  ASSERT_EQ(0, bufferManager_->stats().pinnedResidentBytes);
  ASSERT_GT(bufferManager_->reclaimableBytes(), 0);

  const auto statsBeforeReclaim = bufferManager_->stats();
  memory::MemoryReclaimer::Stats reclaimStats;
  memory::ScopedMemoryArbitrationContext arbitrationContext(root_.get());
  const auto reclaimed =
      root_->reclaim(bufferManager_->reclaimableBytes(), 0, reclaimStats);
  EXPECT_GT(reclaimed, 0);
  const auto statsAfterReclaim = bufferManager_->stats();
  EXPECT_GT(statsAfterReclaim.reclaimCount, statsBeforeReclaim.reclaimCount);
  EXPECT_GT(
      statsAfterReclaim.reclaimedBytes, statsBeforeReclaim.reclaimedBytes);

  loaded = window.loadRowsScoped({rowIds.data(), rowIds.size()});
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

TEST_F(BmRowContainerDiskIoTest, ReadOnlyWindowEvictCanLimitTargetBytes) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      64 << 10,
      64 << 10);
  constexpr vector_size_t kRows = 4096;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      makeFlatVector<std::string>(
          kRows,
          [](auto row) {
            return fmt::format("limited-evict-string-value-{}", row);
          }),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = window.listRowIds();
  auto loaded = window.loadRowsScoped({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());

  loaded.reset();
  const auto reclaimed = window.evictLoadedChunks(1);
  EXPECT_GT(reclaimed, 0);
  EXPECT_LT(reclaimed, bufferManager_->stats().spillWriteBytes);

  const auto remaining = window.evictLoadedChunks();
  EXPECT_GT(remaining, 0);
}

TEST_F(BmRowContainerDiskIoTest, WindowReadRebasesStringsAcrossChunks) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      256 << 10);
  constexpr vector_size_t kRows = 30000;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      makeFlatVector<std::string>(
          kRows,
          [](auto row) {
            return fmt::format("window-read-string-value-{}", row);
          }),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = session.listRowIds();
  ASSERT_EQ(kRows, rowIds.size());

  auto inputRows = session.loadRowsScoped({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, inputRows.size());
  auto result = BaseVector::create(VARCHAR(), kRows, pool());
  container.extractColumnResident(
      inputRows.data(), inputRows.size(), 1, result);
  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("window-read-string-value-0", flat->valueAt(0).str());
  EXPECT_EQ("window-read-string-value-1024", flat->valueAt(1024).str());
  EXPECT_EQ("window-read-string-value-2048", flat->valueAt(2048).str());
  EXPECT_EQ("window-read-string-value-29999", flat->valueAt(29999).str());
}

TEST_F(BmRowContainerDiskIoTest, WindowReadRebasesMultipleStringColumns) {
  BmRowContainer container(
      {BIGINT(), VARCHAR(), VARCHAR()},
      {false, false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      4 << 20,
      256);
  constexpr vector_size_t kRows = 32;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
      makeFlatVector<std::string>(
          kRows,
          [](auto row) { return fmt::format("left-string-value-{:04}", row); }),
      makeFlatVector<std::string>(
          kRows,
          [](auto row) {
            return fmt::format("right-string-value-{:04}", row);
          }),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = session.listRowIds();
  ASSERT_EQ(kRows, rowIds.size());

  auto inputRows = session.loadRowsScoped({rowIds.data(), rowIds.size()});
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
