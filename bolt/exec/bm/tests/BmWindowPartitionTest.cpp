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

#include "bolt/exec/bm/BmWindowPartition.h"

#include "bolt/exec/WindowFunction.h"
#include "bolt/exec/bm/tests/BmRowContainerTestBase.h"

#include "bolt/common/base/BitUtil.h"

#include <fmt/format.h>
#include <folly/ScopeGuard.h>

#include <numeric>

namespace bytedance::bolt::exec::bm {
namespace {

using bytedance::bolt::core::SortOrder;
using bytedance::bolt::memory::bm::MemoryTag;

struct StoredRows {
  std::vector<char*> rowPointers;
};

StoredRows storeRows(BmRowContainer& container, const RowVectorPtr& input) {
  SelectivityVector rows(input->size());
  std::vector<DecodedVector> decoded(input->childrenSize());
  for (auto i = 0; i < input->childrenSize(); ++i) {
    decoded[i].decode(*input->childAt(i), rows);
  }

  StoredRows stored;
  stored.rowPointers.reserve(input->size());
  for (auto row = 0; row < input->size(); ++row) {
    auto context = container.appendRow();
    for (auto column = 0; column < input->childrenSize(); ++column) {
      container.store(context, decoded[column], row, column);
    }
    stored.rowPointers.push_back(context.row());
  }
  return stored;
}

bytedance::bolt::exec::window::BmWindowPartitionDescriptor makeDescriptor(
    vector_size_t numRows,
    SegmentId segment,
    bool spilled,
    std::vector<char*> residentRows = {}) {
  bytedance::bolt::exec::window::BmWindowPartitionDescriptor descriptor;
  descriptor.numRows = numRows;
  descriptor.ranges.push_back({segment, 0, static_cast<RowNumber>(numRows)});
  descriptor.residentRows = std::move(residentRows);
  descriptor.hasSpilledRows = spilled;
  return descriptor;
}

TEST_F(BmRowContainerTest, BmWindowPartitionExtractsFromResidentPointers) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {true, true},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({10, std::nullopt, 30, 40}),
      makeNullableFlatVector<std::string>(
          {"alpha", "bravo", std::nullopt, "delta"}),
  });
  auto stored = storeRows(container, input);
  auto descriptor = makeDescriptor(
      input->size(),
      container.activeSegmentId(),
      false,
      std::move(stored.rowPointers));

  bytedance::bolt::exec::window::BmWindowPartition partition(
      &container,
      pool(),
      {BIGINT(), VARCHAR()},
      std::move(descriptor),
      {0, 1},
      {{0, SortOrder{true, true}}});

  auto values = BaseVector::create(BIGINT(), 0, pool());
  partition.extractColumn(0, 1, 2, 0, values);
  ASSERT_EQ(2, values->size());
  EXPECT_TRUE(values->isNullAt(0));
  EXPECT_EQ(30, values->asFlatVector<int64_t>()->valueAt(1));

  std::vector<vector_size_t> rowNumbers{3, WindowFunction::kNullRow, 0};
  auto names = BaseVector::create(VARCHAR(), 0, pool());
#ifndef NDEBUG
  bytedance::bolt::exec::window::resetBmWindowPartitionTestStats();
#endif
  partition.extractColumn(1, {rowNumbers.data(), rowNumbers.size()}, 0, names);
  ASSERT_EQ(3, names->size());
  EXPECT_EQ("delta", names->asFlatVector<StringView>()->valueAt(0).str());
  EXPECT_TRUE(names->isNullAt(1));
  EXPECT_EQ("alpha", names->asFlatVector<StringView>()->valueAt(2).str());
#ifndef NDEBUG
  auto stats = bytedance::bolt::exec::window::bmWindowPartitionTestStats();
  EXPECT_EQ(1, stats.rowNumberResidentExtractCalls);
  EXPECT_EQ(rowNumbers.size(), stats.rowNumberResidentExtractRows);
#endif

  auto nulls = AlignedBuffer::allocate<bool>(input->size(), pool());
  partition.extractNulls(0, 0, input->size(), nulls);
  auto* rawNulls = nulls->as<uint64_t>();
  EXPECT_FALSE(bits::isBitSet(rawNulls, 0));
  EXPECT_TRUE(bits::isBitSet(rawNulls, 1));
  EXPECT_FALSE(bits::isBitSet(rawNulls, 2));
  EXPECT_FALSE(bits::isBitSet(rawNulls, 3));
}

TEST_F(BmRowContainerTest, BmWindowPartitionCanUseResidentRowsFromDescriptor) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {true, true},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({10, std::nullopt, 30, 40}),
      makeNullableFlatVector<std::string>(
          {"alpha", "bravo", std::nullopt, "delta"}),
  });
  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);
  auto descriptor = makeDescriptor(
      input->size(), container.activeSegmentId(), false, std::move(rows));

  bytedance::bolt::exec::window::BmWindowPartition partition(
      &container,
      pool(),
      {BIGINT(), VARCHAR()},
      std::move(descriptor),
      {0, 1},
      {{0, SortOrder{true, true}}});

  EXPECT_TRUE(partition.hasResidentRows());
  EXPECT_EQ(input->size(), partition.numRows());

  auto names = BaseVector::create(VARCHAR(), 0, pool());
  partition.extractColumn(1, 0, input->size(), 0, names);
  ASSERT_EQ(4, names->size());
  EXPECT_EQ("alpha", names->asFlatVector<StringView>()->valueAt(0).str());
  EXPECT_EQ("bravo", names->asFlatVector<StringView>()->valueAt(1).str());
  EXPECT_TRUE(names->isNullAt(2));
  EXPECT_EQ("delta", names->asFlatVector<StringView>()->valueAt(3).str());
}

TEST_F(BmRowContainerTest, BmRowContainerComparesDifferentResidentColumns) {
  BmRowContainer container(
      {BIGINT(), BIGINT()},
      {true, true},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({10, 20, std::nullopt}),
      makeNullableFlatVector<int64_t>({15, 20, 30}),
  });
  auto stored = storeRows(container, input);

  EXPECT_LT(
      container.compare(
          stored.rowPointers[0],
          stored.rowPointers[0],
          0,
          1,
          CompareFlags{.nullsFirst = true, .ascending = true}),
      0);
  EXPECT_EQ(
      container.compare(
          stored.rowPointers[1],
          stored.rowPointers[1],
          0,
          1,
          CompareFlags{.nullsFirst = true, .ascending = true}),
      0);
  EXPECT_LT(
      container.compare(
          stored.rowPointers[2],
          stored.rowPointers[2],
          0,
          1,
          CompareFlags{.nullsFirst = true, .ascending = true}),
      0);
  EXPECT_GT(
      container.compare(
          stored.rowPointers[2],
          stored.rowPointers[2],
          0,
          1,
          CompareFlags{.nullsFirst = false, .ascending = true}),
      0);
}

TEST_F(BmRowContainerTest, BmWindowPartitionExtractNullsUsesBatches) {
  constexpr vector_size_t size = 9'000;
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {true, true},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeFlatVector<int64_t>(
          size,
          [](auto row) { return row; },
          [](auto row) { return row % 7 == 0; }),
      makeFlatVector<std::string>(
          size, [](auto row) { return std::string(32, 'a' + row % 26); }),
  });
  auto stored = storeRows(container, input);
  auto descriptor = makeDescriptor(
      input->size(),
      container.activeSegmentId(),
      false,
      std::move(stored.rowPointers));

  bytedance::bolt::exec::window::BmWindowPartition partition(
      &container,
      pool(),
      {BIGINT(), VARCHAR()},
      std::move(descriptor),
      {0, 1},
      {{0, SortOrder{true, true}}});

#ifndef NDEBUG
  bytedance::bolt::exec::window::resetBmWindowPartitionTestStats();
#endif
  auto nulls = AlignedBuffer::allocate<bool>(input->size(), pool());
  partition.extractNulls(0, 0, input->size(), nulls);

  auto* rawNulls = nulls->as<uint64_t>();
  for (auto row = 0; row < input->size(); ++row) {
    EXPECT_EQ(row % 7 == 0, bits::isBitSet(rawNulls, row));
  }

#ifndef NDEBUG
  const auto stats =
      bytedance::bolt::exec::window::bmWindowPartitionTestStats();
  EXPECT_EQ(size, stats.extractNullRows);
  EXPECT_GE(stats.extractNullCalls, 3);
  EXPECT_LE(stats.maxExtractNullBatchRows, 4'096);
#endif
}

TEST_F(
    BmRowContainerDiskIoTest,
    BmWindowPartitionReloadsFromSegmentRangeAfterSpill) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {true, true},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({10, std::nullopt, 30, 40}),
      makeNullableFlatVector<std::string>(
          {"alpha", "bravo", std::nullopt, "delta"}),
  });
  storeRows(container, input);
  auto segment = container.spillActiveSegment();
  auto descriptor = makeDescriptor(input->size(), segment, true);

  bytedance::bolt::exec::window::BmWindowPartition partition(
      &container,
      pool(),
      {BIGINT(), VARCHAR()},
      std::move(descriptor),
      {0, 1},
      {{0, SortOrder{true, true}}});

  auto names = BaseVector::create(VARCHAR(), 0, pool());
  partition.extractColumn(1, 0, input->size(), 0, names);
  ASSERT_EQ(4, names->size());
  EXPECT_EQ("alpha", names->asFlatVector<StringView>()->valueAt(0).str());
  EXPECT_EQ("bravo", names->asFlatVector<StringView>()->valueAt(1).str());
  EXPECT_TRUE(names->isNullAt(2));
  EXPECT_EQ("delta", names->asFlatVector<StringView>()->valueAt(3).str());

  auto values = BaseVector::create(BIGINT(), 0, pool());
  std::vector<vector_size_t> rowNumbers{2, WindowFunction::kNullRow, 1};
  partition.extractColumn(0, {rowNumbers.data(), rowNumbers.size()}, 0, values);
  ASSERT_EQ(3, values->size());
  EXPECT_EQ(30, values->asFlatVector<int64_t>()->valueAt(0));
  EXPECT_TRUE(values->isNullAt(1));
  EXPECT_TRUE(values->isNullAt(2));

  auto nulls = AlignedBuffer::allocate<bool>(input->size(), pool());
  partition.extractNulls(0, 0, input->size(), nulls);
  auto* rawNulls = nulls->as<uint64_t>();
  EXPECT_FALSE(bits::isBitSet(rawNulls, 0));
  EXPECT_TRUE(bits::isBitSet(rawNulls, 1));
  EXPECT_FALSE(bits::isBitSet(rawNulls, 2));
  EXPECT_FALSE(bits::isBitSet(rawNulls, 3));
}

TEST_F(
    BmRowContainerDiskIoTest,
    BmWindowPartitionUsesBulkReadWhenPartitionFits) {
  constexpr vector_size_t size = 1024;
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {true, true},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      256 << 10);
  auto input = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      makeFlatVector<std::string>(
          size,
          [](auto row) { return fmt::format("partition-bulk-value-{}", row); }),
  });
  storeRows(container, input);
  auto segment = container.spillActiveSegment();

  bytedance::bolt::exec::window::BmWindowPartitionDescriptor descriptor;
  descriptor.numRows = size;
  descriptor.ranges.push_back({segment, 0, static_cast<RowNumber>(size)});
  descriptor.hasSpilledRows = true;

  bytedance::bolt::exec::window::BmWindowPartition partition(
      &container,
      pool(),
      {BIGINT(), VARCHAR()},
      std::move(descriptor),
      {0, 1},
      {{0, SortOrder{true, true}}});

#ifndef NDEBUG
  bytedance::bolt::exec::window::resetBmWindowPartitionTestStats();
#endif
  auto values = BaseVector::create(BIGINT(), 0, pool());
  partition.extractColumn(0, size - 3, 3, 0, values);
  ASSERT_EQ(3, values->size());
  EXPECT_EQ(size - 3, values->asFlatVector<int64_t>()->valueAt(0));
  EXPECT_EQ(size - 2, values->asFlatVector<int64_t>()->valueAt(1));
  EXPECT_EQ(size - 1, values->asFlatVector<int64_t>()->valueAt(2));

#ifndef NDEBUG
  const auto stats =
      bytedance::bolt::exec::window::bmWindowPartitionTestStats();
  EXPECT_EQ(0, stats.loadRowsCalls);
#endif
  EXPECT_EQ(partition.reclaimReadChunks(), 0);
}

TEST_F(BmRowContainerDiskIoTest, BmWindowPartitionKeepsBulkReadOnReclaim) {
  constexpr vector_size_t size = 1024;
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {true, true},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      256 << 10);
  auto input = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      makeFlatVector<std::string>(
          size,
          [](auto row) { return fmt::format("partition-bulk-value-{}", row); }),
  });
  storeRows(container, input);
  auto segment = container.spillActiveSegment();

  bytedance::bolt::exec::window::BmWindowPartitionDescriptor descriptor;
  descriptor.numRows = size;
  descriptor.ranges.push_back({segment, 0, static_cast<RowNumber>(size)});
  descriptor.hasSpilledRows = true;

  bytedance::bolt::exec::window::BmWindowPartition partition(
      &container,
      pool(),
      {BIGINT(), VARCHAR()},
      std::move(descriptor),
      {0, 1},
      {{0, SortOrder{true, true}}});

#ifndef NDEBUG
  bytedance::bolt::exec::window::resetBmWindowPartitionTestStats();
#endif
  auto values = BaseVector::create(BIGINT(), 0, pool());
  partition.extractColumn(0, size - 3, 3, 0, values);
  ASSERT_EQ(3, values->size());
  EXPECT_EQ(size - 3, values->asFlatVector<int64_t>()->valueAt(0));
  EXPECT_EQ(size - 2, values->asFlatVector<int64_t>()->valueAt(1));
  EXPECT_EQ(size - 1, values->asFlatVector<int64_t>()->valueAt(2));
#ifndef NDEBUG
  EXPECT_EQ(
      0,
      bytedance::bolt::exec::window::bmWindowPartitionTestStats()
          .loadRowsCalls);
#endif
  EXPECT_GT(bufferManager_->stats().pinnedResidentBytes, 0);

  const auto pinnedBeforeReclaim = bufferManager_->stats().pinnedResidentBytes;
  EXPECT_EQ(partition.reclaimReadChunks(), 0);
  EXPECT_EQ(pinnedBeforeReclaim, bufferManager_->stats().pinnedResidentBytes);

  auto names = BaseVector::create(VARCHAR(), 0, pool());
  partition.extractColumn(1, size - 2, 2, 0, names);
  ASSERT_EQ(2, names->size());
  EXPECT_EQ(
      fmt::format("partition-bulk-value-{}", size - 2),
      names->asFlatVector<StringView>()->valueAt(0).str());
  EXPECT_EQ(
      fmt::format("partition-bulk-value-{}", size - 1),
      names->asFlatVector<StringView>()->valueAt(1).str());

#ifndef NDEBUG
  const auto statsAfterReclaim =
      bytedance::bolt::exec::window::bmWindowPartitionTestStats();
  EXPECT_EQ(0, statsAfterReclaim.reclaimReadChunksCalls);
  EXPECT_EQ(0, statsAfterReclaim.loadRowsCalls);
#endif
}

TEST_F(
    BmRowContainerDiskIoTest,
    BmWindowPartitionReleasesReadOnlyChunksAfterExtraction) {
  constexpr vector_size_t size = 50'000;
  constexpr uint32_t blockSize = 32 << 10;
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {true, true},
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
  storeRows(container, input);
  auto segment = container.spillActiveSegment();
  auto descriptor = makeDescriptor(input->size(), segment, true);

  bytedance::bolt::exec::window::BmWindowPartition partition(
      &container,
      pool(),
      {BIGINT(), VARCHAR()},
      std::move(descriptor),
      {0, 1},
      {{0, SortOrder{true, true}}});

  constexpr uint64_t kReadSlack = 10 << 20;
  const auto freeBeforePressure = root_->freeBytes();
  ASSERT_GT(freeBeforePressure, kReadSlack);
  const auto pressureBytes = freeBeforePressure - kReadSlack;
  auto pressurePool = root_->addLeafChild("window-read-pressure");
  auto* pressure = pressurePool->allocate(pressureBytes);
  auto pressureGuard =
      folly::makeGuard([&]() { pressurePool->free(pressure, pressureBytes); });
  ASSERT_FALSE(container.canBulkRead({&segment, 1}));

#ifndef NDEBUG
  bytedance::bolt::exec::window::resetBmWindowPartitionTestStats();
#endif
  auto names = BaseVector::create(VARCHAR(), 0, pool());
  partition.extractColumn(1, 0, 1, 0, names);
#ifndef NDEBUG
  auto stats = bytedance::bolt::exec::window::bmWindowPartitionTestStats();
  EXPECT_GT(stats.loadRowsCalls, 0);
  EXPECT_EQ(0, stats.reclaimReadChunksCalls);
#endif

  const auto reclaimed = partition.reclaimReadChunks();
  // Scoped extraction has already unpinned the chunk. Explicit Window reclaim
  // still evicts its resident blocks, so no later BM reclaim is necessary.
  EXPECT_GT(reclaimed, 0);
  EXPECT_EQ(0, bufferManager_->stats().pinnedResidentBytes);
  EXPECT_EQ(0, bufferManager_->stats().unpinnedResidentBytes);
  EXPECT_EQ(0, bufferManager_->Reclaim(bufferManager_->reclaimableBytes()));
  EXPECT_EQ(
      std::string(256, 'a'),
      names->asFlatVector<StringView>()->valueAt(0).str());

  auto values = BaseVector::create(BIGINT(), 0, pool());
  partition.extractColumn(0, size - 2, 2, 0, values);
  ASSERT_EQ(2, values->size());
  EXPECT_EQ(size - 2, values->asFlatVector<int64_t>()->valueAt(0));
  EXPECT_EQ(size - 1, values->asFlatVector<int64_t>()->valueAt(1));
}

TEST_F(
    BmRowContainerDiskIoTest,
    BmWindowPartitionPrecomputedEmptyPeerMetadataDoesNotLoadRows) {
  constexpr vector_size_t size = 9'000;
  BmRowContainer container(
      {BIGINT(), BIGINT()},
      {true, true},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto /*row*/) { return 1; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
  });
  storeRows(container, input);
  auto segment = container.spillActiveSegment();
  auto descriptor = makeDescriptor(input->size(), segment, true);
  descriptor.peerBoundaryMode =
      bytedance::bolt::exec::window::BmPeerBoundaryMode::kPrecomputed;

  bytedance::bolt::exec::window::BmWindowPartition partition(
      &container,
      pool(),
      {BIGINT(), BIGINT()},
      std::move(descriptor),
      {0, 1},
      {{0, SortOrder{true, true}}});

  std::vector<vector_size_t> peerStarts(3);
  std::vector<vector_size_t> peerEnds(3);
#ifndef NDEBUG
  bytedance::bolt::exec::window::resetBmWindowPartitionTestStats();
#endif
  partition.computePeerBuffers(
      10, 13, 0, 0, peerStarts.data(), peerEnds.data());

  EXPECT_EQ((std::vector<vector_size_t>{0, 0, 0}), peerStarts);
  EXPECT_EQ(
      (std::vector<vector_size_t>{size - 1, size - 1, size - 1}), peerEnds);
#ifndef NDEBUG
  const auto stats =
      bytedance::bolt::exec::window::bmWindowPartitionTestStats();
  EXPECT_EQ(0, stats.loadRowsCalls);
  EXPECT_EQ(0, stats.loadedRows);
#endif
}

TEST_F(
    BmRowContainerDiskIoTest,
    BmWindowPartitionNotNeededPeerModeUsesDummyPeers) {
  BmRowContainer container(
      {BIGINT(), BIGINT()},
      {true, true},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({1, 1, 2, 2, 3, 4, 4}),
      makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6}),
  });
  storeRows(container, input);
  auto segment = container.spillActiveSegment();
  auto descriptor = makeDescriptor(input->size(), segment, true);
  descriptor.peerBoundaryMode =
      bytedance::bolt::exec::window::BmPeerBoundaryMode::kNotNeeded;

  bytedance::bolt::exec::window::BmWindowPartition partition(
      &container,
      pool(),
      {BIGINT(), BIGINT()},
      std::move(descriptor),
      {0, 1},
      {{0, SortOrder{true, true}}});

  std::vector<vector_size_t> peerStarts(input->size());
  std::vector<vector_size_t> peerEnds(input->size());
#ifndef NDEBUG
  bytedance::bolt::exec::window::resetBmWindowPartitionTestStats();
#endif
  partition.computePeerBuffers(
      0, input->size(), 0, 0, peerStarts.data(), peerEnds.data());

  EXPECT_EQ((std::vector<vector_size_t>{0, 1, 2, 3, 4, 5, 6}), peerStarts);
  EXPECT_EQ((std::vector<vector_size_t>{0, 1, 2, 3, 4, 5, 6}), peerEnds);
#ifndef NDEBUG
  const auto stats =
      bytedance::bolt::exec::window::bmWindowPartitionTestStats();
  EXPECT_EQ(0, stats.loadRowsCalls);
  EXPECT_EQ(0, stats.loadedRows);
#endif
}

TEST_F(BmRowContainerDiskIoTest, BmWindowPartitionUsesPeerMetadataAfterSpill) {
  constexpr vector_size_t size = 7;
  BmRowContainer container(
      {BIGINT(), BIGINT()},
      {true, true},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 1, 2, 2, 3, 4, 4}),
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
  });
  storeRows(container, input);
  auto segment = container.spillActiveSegment();

  bytedance::bolt::exec::window::BmWindowPartitionDescriptor descriptor;
  descriptor.numRows = size;
  descriptor.ranges.push_back({segment, 0, static_cast<RowNumber>(size)});
  descriptor.hasSpilledRows = true;
  descriptor.peerBoundaryMode =
      bytedance::bolt::exec::window::BmPeerBoundaryMode::kPrecomputed;
  descriptor.peerStartBits.assign(bits::nwords(size), 0);
  bits::setBit(descriptor.peerStartBits.data(), 2);
  bits::setBit(descriptor.peerStartBits.data(), 4);
  bits::setBit(descriptor.peerStartBits.data(), 5);

  bytedance::bolt::exec::window::BmWindowPartition partition(
      &container,
      pool(),
      {BIGINT(), BIGINT()},
      std::move(descriptor),
      {0, 1},
      {{0, SortOrder{true, true}}});

  std::vector<vector_size_t> peerStarts(size);
  std::vector<vector_size_t> peerEnds(size);
#ifndef NDEBUG
  bytedance::bolt::exec::window::resetBmWindowPartitionTestStats();
#endif
  partition.computePeerBuffers(
      0, size, 0, 0, peerStarts.data(), peerEnds.data());

  EXPECT_EQ((std::vector<vector_size_t>{0, 0, 2, 2, 4, 5, 5}), peerStarts);
  EXPECT_EQ((std::vector<vector_size_t>{1, 1, 3, 3, 4, 6, 6}), peerEnds);

#ifndef NDEBUG
  const auto stats =
      bytedance::bolt::exec::window::bmWindowPartitionTestStats();
  EXPECT_EQ(0, stats.loadRowsCalls);
  EXPECT_EQ(0, stats.loadedRows);
#endif
}

TEST_F(
    BmRowContainerDiskIoTest,
    BmWindowPartitionRangeFrameUsesBatchesAfterSpill) {
  constexpr vector_size_t size = 9'000;
  constexpr vector_size_t frameEnd = 8'191;
  BmRowContainer container(
      {BIGINT(), BIGINT()},
      {true, true},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      makeFlatVector<int64_t>(size, [](auto /*row*/) { return frameEnd; }),
  });
  storeRows(container, input);
  auto segment = container.spillActiveSegment();
  auto descriptor = makeDescriptor(input->size(), segment, true);

  bytedance::bolt::exec::window::BmWindowPartition partition(
      &container,
      pool(),
      {BIGINT(), BIGINT()},
      std::move(descriptor),
      {0, 1},
      {{0, SortOrder{true, true}}});

  vector_size_t peerStarts[] = {0};
  vector_size_t frameBounds[] = {0};
#ifndef NDEBUG
  bytedance::bolt::exec::window::resetBmWindowPartitionTestStats();
#endif
  partition.computeKRangeFrameBounds(
      false, false, 1, 0, 1, peerStarts, frameBounds);

  EXPECT_EQ(frameEnd, frameBounds[0]);
#ifndef NDEBUG
  const auto stats =
      bytedance::bolt::exec::window::bmWindowPartitionTestStats();
  EXPECT_LE(stats.maxLoadedRows, 4'096);
  EXPECT_EQ(0, stats.reclaimReadChunksCalls);
#endif
}

TEST_F(
    BmRowContainerDiskIoTest,
    BmWindowPartitionRangeFrameHandlesDescAfterSpill) {
  constexpr vector_size_t size = 9;
  BmRowContainer container(
      {BIGINT(), BIGINT(), BIGINT()},
      {true, true, true},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return 8 - row; }),
      makeNullableFlatVector<int64_t>({10, 9, 8, 7, std::nullopt, 5, 4, 3, 2}),
      makeFlatVector<int64_t>(size, [](auto row) { return 7 - row; }),
  });
  storeRows(container, input);
  auto segment = container.spillActiveSegment();
  auto descriptor = makeDescriptor(input->size(), segment, true);

  bytedance::bolt::exec::window::BmWindowPartition partition(
      &container,
      pool(),
      {BIGINT(), BIGINT(), BIGINT()},
      std::move(descriptor),
      {0, 1, 2},
      {{0, SortOrder{false, true}}});

  std::vector<vector_size_t> peerStarts(size);
  std::vector<vector_size_t> peerEnds(size);
  std::iota(peerStarts.begin(), peerStarts.end(), 0);
  std::iota(peerEnds.begin(), peerEnds.end(), 0);

  std::vector<vector_size_t> frameStarts(size);
  partition.computeKRangeFrameBounds(
      true, true, 1, 0, size, peerStarts.data(), frameStarts.data());
  EXPECT_EQ(
      (std::vector<vector_size_t>{-1, -1, -1, 1, 4, 3, 4, 5, 6}), frameStarts);

  std::vector<vector_size_t> frameEnds(size);
  partition.computeKRangeFrameBounds(
      false, false, 2, 0, size, peerEnds.data(), frameEnds.data());
  EXPECT_EQ(
      (std::vector<vector_size_t>{1, 2, 3, 4, 5, 6, 7, 10, 10}), frameEnds);
}

} // namespace
} // namespace bytedance::bolt::exec::bm
