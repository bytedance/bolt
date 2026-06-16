#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <cstring>

namespace bytedance::bolt::exec::bm {
namespace {

using bytedance::bolt::memory::MemoryPool;
using bytedance::bolt::memory::MemoryReclaimer;
using bytedance::bolt::memory::bm::BufferManager;
using bytedance::bolt::memory::bm::BufferManagerConfig;
using bytedance::bolt::memory::bm::MemoryTag;

class BmRowContainerTest : public testing::Test,
                           public bytedance::bolt::test::VectorTestBase {
 protected:
  void SetUp() override {
    root_ = memory::memoryManager()->addRootPool(
        fmt::format(
            "bm-row-container-{}",
            testing::UnitTest::GetInstance()->current_test_info()->name()),
        64 << 20,
        MemoryReclaimer::create());
    BufferManagerConfig config;
    config.poolName = root_->name();
    config.spillStoreConfig.fileAllocatorConfig =
        memory::bm::test::ValidConfigWithDirectory(
            memory::bm::test::UniqueTempDir(root_->name()));
    bufferManager_ = BufferManager::Create(*root_, std::move(config));
  }

  RowVectorPtr makeInput() {
    return makeRowVector({
        makeFlatVector<int64_t>({10, 3, 7, 3}),
        makeFlatVector<std::string>({"delta", "alpha", "charlie", "bravo"}),
    });
  }

  std::vector<char*> storeAll(BmRowContainer& container, RowVectorPtr input) {
    SelectivityVector rows(input->size());
    std::vector<DecodedVector> decoded(input->childrenSize());
    for (auto i = 0; i < input->childrenSize(); ++i) {
      decoded[i].decode(*input->childAt(i), rows);
    }

    std::vector<char*> rowsOut;
    rowsOut.reserve(input->size());
    for (auto row = 0; row < input->size(); ++row) {
      auto context = container.appendRow();
      for (auto column = 0; column < input->childrenSize(); ++column) {
        container.store(context, decoded[column], row, column);
      }
      rowsOut.push_back(context.row());
    }
    return rowsOut;
  }

  std::shared_ptr<MemoryPool> root_;
  std::shared_ptr<BufferManager> bufferManager_;
};

TEST_F(BmRowContainerTest, ResidentStoreCompareAndExtract) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeInput();
  auto rows = storeAll(container, input);

  EXPECT_GT(container.compare(rows[0], rows[1], 0), 0);
  EXPECT_LT(container.compare(rows[1], rows[2], 0), 0);
  EXPECT_GT(container.compare(rows[0], rows[1], 1), 0);

  auto result = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, result);

  auto flat = result->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ(10, flat->valueAt(0));
  EXPECT_EQ(3, flat->valueAt(1));
  EXPECT_EQ(7, flat->valueAt(2));
  EXPECT_EQ(3, flat->valueAt(3));
}

TEST_F(BmRowContainerTest, AppendRowsAndStringCompare) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<std::string>(
          {"prefix_same_a", "prefix_same_b", "prefix_same_a"}),
  });
  auto rows = storeAll(container, input);

  EXPECT_LT(container.compare(rows[0], rows[1], 1), 0);
  EXPECT_EQ(0, container.compare(rows[0], rows[2], 1));

  auto result = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, result);
  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("prefix_same_a", flat->valueAt(0).str());
  EXPECT_EQ("prefix_same_b", flat->valueAt(1).str());
  EXPECT_EQ("prefix_same_a", flat->valueAt(2).str());
}

TEST_F(BmRowContainerTest, RowWriteContextKeepsCurrentChunkPointers) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting);
  auto context = container.appendRow();

  ASSERT_NE(nullptr, context.segment());
  ASSERT_NE(nullptr, context.chunk());
  EXPECT_EQ(context.segment()->meta.id, context.chunk()->meta.segmentId);
  EXPECT_EQ(context.row(), context.chunk()->rowBlock.ptr);
}

TEST_F(BmRowContainerTest, RowLayoutInitializesOnlyNulls) {
  {
    BmRowLayout layout({BIGINT(), INTEGER()}, {false, false}, 4 << 20);
    std::vector<char> row(layout.rowSize(), static_cast<char>(0x7f));

    layout.initializeNulls(row.data());

    for (auto byte : row) {
      EXPECT_EQ(static_cast<char>(0x7f), byte);
    }
  }

  {
    BmRowLayout layout({BIGINT(), VARCHAR()}, {true, false}, 4 << 20);
    std::vector<char> row(layout.rowSize(), static_cast<char>(0x7f));

    layout.initializeNulls(row.data());

    EXPECT_EQ(0, row[0]);
    for (uint32_t i = layout.column(0).offset;
         i < layout.column(0).offset + layout.column(0).width;
         ++i) {
      EXPECT_EQ(static_cast<char>(0x7f), row[i]);
    }
    for (uint32_t i = layout.column(1).offset;
         i < layout.column(1).offset + layout.column(1).width;
         ++i) {
      EXPECT_EQ(static_cast<char>(0x7f), row[i]);
    }
  }
}

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
  EXPECT_EQ(writesBeforeEvict, bufferManager_->stats().spillWriteCount);

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

TEST_F(BmRowContainerTest, SegmentCollectionDoesNotShareHeapBlocksAcrossChunks) {
  BmRowLayout layout({BIGINT(), VARCHAR()}, {false, false}, 64);
  BmSegmentCollection segments(
      bufferManager_,
      MemoryTag::kTesting,
      &layout,
      layout.rowSize(),
      1024);
  auto& segment = segments.createSegment(kDefaultPartition);

  auto* firstRow = segments.newRowInSegment(segment);
  auto& firstChunk = segments.currentChunk(segment);
  auto& firstHeap = segments.ensureHeapBlockInChunk(firstChunk, 16);
  segments.recordHeapForChunk(firstChunk, firstHeap, firstRow);

  auto* secondRow = segments.newRowInSegment(segment);
  auto& secondChunk = segments.currentChunk(segment);
  auto& secondHeap = segments.ensureHeapBlockInChunk(secondChunk, 16);
  segments.recordHeapForChunk(secondChunk, secondHeap, secondRow);

  ASSERT_EQ(2, segment.chunks.size());
  const auto& firstStoredChunk = *segment.chunks[0];
  const auto& secondStoredChunk = *segment.chunks[1];
  EXPECT_EQ(1, firstStoredChunk.meta.rowCount);
  EXPECT_EQ(layout.rowSize(), firstStoredChunk.rowBlock.used);
  EXPECT_EQ(1, secondStoredChunk.meta.rowCount);
  EXPECT_EQ(layout.rowSize(), secondStoredChunk.rowBlock.used);
  auto secondRowId = segments.rowIdForRowNumber(segment, 1);
  EXPECT_EQ(secondStoredChunk.rowBlock.id, secondRowId.rowBlockId);
  EXPECT_EQ(0, secondRowId.rowOffset);
  ASSERT_EQ(1, firstStoredChunk.heapBlocks.size());
  ASSERT_EQ(1, secondStoredChunk.heapBlocks.size());
  EXPECT_NE(
      firstStoredChunk.heapBlocks[0].id,
      secondStoredChunk.heapBlocks[0].id);
}

TEST_F(BmRowContainerTest, SegmentCollectionLeavesHeapTailUntilFinalize) {
  BmRowLayout layout({BIGINT(), VARCHAR()}, {false, false}, 64);
  BmSegmentCollection segments(
      bufferManager_,
      MemoryTag::kTesting,
      &layout,
      4 << 20,
      64);
  auto& segment = segments.createSegment(kDefaultPartition);
  segments.newRowInSegment(segment);
  auto& chunk = segments.currentChunk(segment);

  auto& firstHeap = segments.ensureHeapBlockInChunk(chunk, 40);
  firstHeap.used = 40;
  auto* const firstHeapPtr = firstHeap.ptr;
  const auto firstHeapUsed = firstHeap.used;
  const auto firstHeapSize = firstHeap.size;
  const auto firstHeapId = firstHeap.id;
  std::memset(
      firstHeapPtr + firstHeapUsed, 0x7f, firstHeapSize - firstHeapUsed);

  auto& secondHeap = segments.ensureHeapBlockInChunk(chunk, 40);
  ASSERT_NE(firstHeapId, secondHeap.id);

  for (uint32_t offset = firstHeapUsed; offset < firstHeapSize; ++offset) {
    EXPECT_EQ(static_cast<char>(0x7f), firstHeapPtr[offset])
        << "offset=" << offset;
  }
}

TEST_F(BmRowContainerTest, SegmentCollectionListsLiveSegmentIdsInIdOrder) {
  BmRowLayout layout({BIGINT()}, {false}, 64);
  BmSegmentCollection segments(
      bufferManager_,
      MemoryTag::kTesting,
      &layout,
      4 << 20,
      64);

  auto firstId = segments.createSegment(std::nullopt).meta.id;
  auto secondId = segments.createSegment(std::nullopt).meta.id;
  auto thirdId = segments.createSegment(std::nullopt).meta.id;

  segments.releaseSegment(secondId);
  auto fourthId = segments.createSegment(std::nullopt).meta.id;

  EXPECT_THROW((void)segments.segmentData(secondId), BoltRuntimeError);
  EXPECT_EQ(
      (std::vector<SegmentId>{firstId, thirdId, fourthId}),
      segments.allSegmentIds());
}

TEST_F(BmRowContainerTest, NullableExtractPreservesNulls) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {true, true},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({10, std::nullopt, 7}),
      makeNullableFlatVector<std::string>({"delta", std::nullopt, "alpha"}),
  });
  auto rows = storeAll(container, input);

  auto bigintResult = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, bigintResult);
  auto bigintFlat = bigintResult->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, bigintFlat);
  EXPECT_FALSE(bigintFlat->isNullAt(0));
  EXPECT_TRUE(bigintFlat->isNullAt(1));
  EXPECT_FALSE(bigintFlat->isNullAt(2));
  EXPECT_EQ(10, bigintFlat->valueAt(0));
  EXPECT_EQ(7, bigintFlat->valueAt(2));

  auto varcharResult = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, varcharResult);
  auto varcharFlat = varcharResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, varcharFlat);
  EXPECT_EQ("delta", varcharFlat->valueAt(0).str());
  EXPECT_TRUE(varcharFlat->isNullAt(1));
  EXPECT_EQ("alpha", varcharFlat->valueAt(2).str());
}

TEST_F(BmRowContainerTest, NullableStoreClearsNullBitForNonNullValue) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {true, true},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({std::nullopt, 42}),
      makeNullableFlatVector<std::string>({std::nullopt, "value"}),
  });
  SelectivityVector rows(input->size());
  DecodedVector bigint;
  DecodedVector varchar;
  bigint.decode(*input->childAt(0), rows);
  varchar.decode(*input->childAt(1), rows);

  auto context = container.appendRow();
  container.store(context, bigint, 0, 0);
  container.store(context, varchar, 0, 1);
  EXPECT_TRUE(context.row()[0] & 0x1);
  EXPECT_TRUE(context.row()[0] & 0x2);

  container.store(context, bigint, 1, 0);
  container.store(context, varchar, 1, 1);

  auto* storedRow = context.row();
  auto bigintResult = BaseVector::create(BIGINT(), 1, pool());
  container.extractColumnResident(&storedRow, 1, 0, bigintResult);
  auto bigintFlat = bigintResult->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, bigintFlat);
  EXPECT_FALSE(bigintFlat->isNullAt(0));
  EXPECT_EQ(42, bigintFlat->valueAt(0));

  auto varcharResult = BaseVector::create(VARCHAR(), 1, pool());
  container.extractColumnResident(&storedRow, 1, 1, varcharResult);
  auto varcharFlat = varcharResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, varcharFlat);
  EXPECT_FALSE(varcharFlat->isNullAt(0));
  EXPECT_EQ("value", varcharFlat->valueAt(0).str());
}

TEST_F(BmRowContainerTest, NullableStringNullSurvivesSpillRead) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, true},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeNullableFlatVector<std::string>({"delta", std::nullopt, "alpha"}),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto session = container.beginBulkReadSegments({&segment, 1});
  auto rows = session.loadRows();

  auto result = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, result);
  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("delta", flat->valueAt(0).str());
  EXPECT_TRUE(flat->isNullAt(1));
  EXPECT_EQ("alpha", flat->valueAt(2).str());
}

TEST_F(BmRowContainerTest, AppendBatchStoresFixedAndVariableRows) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({11, 22, 33}),
      makeFlatVector<std::string>({"alpha", "bravo", "charlie"}),
  });

  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);

  ASSERT_EQ(3, rows.size());
  EXPECT_EQ(3, container.numRows());

  auto bigintResult = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, bigintResult);
  auto bigintFlat = bigintResult->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, bigintFlat);
  EXPECT_EQ(11, bigintFlat->valueAt(0));
  EXPECT_EQ(22, bigintFlat->valueAt(1));
  EXPECT_EQ(33, bigintFlat->valueAt(2));

  auto varcharResult = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, varcharResult);
  auto varcharFlat = varcharResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, varcharFlat);
  EXPECT_EQ("alpha", varcharFlat->valueAt(0).str());
  EXPECT_EQ("bravo", varcharFlat->valueAt(1).str());
  EXPECT_EQ("charlie", varcharFlat->valueAt(2).str());
}

TEST_F(BmRowContainerTest, AppendBatchPreservesNulls) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {true, true},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({10, std::nullopt, 30}),
      makeNullableFlatVector<std::string>({"alpha", std::nullopt, "charlie"}),
  });

  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);

  ASSERT_EQ(3, rows.size());
  auto bigintResult = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, bigintResult);
  auto bigintFlat = bigintResult->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, bigintFlat);
  EXPECT_FALSE(bigintFlat->isNullAt(0));
  EXPECT_TRUE(bigintFlat->isNullAt(1));
  EXPECT_FALSE(bigintFlat->isNullAt(2));
  EXPECT_EQ(10, bigintFlat->valueAt(0));
  EXPECT_EQ(30, bigintFlat->valueAt(2));

  auto varcharResult = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, varcharResult);
  auto varcharFlat = varcharResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, varcharFlat);
  EXPECT_EQ("alpha", varcharFlat->valueAt(0).str());
  EXPECT_TRUE(varcharFlat->isNullAt(1));
  EXPECT_EQ("charlie", varcharFlat->valueAt(2).str());
}

TEST_F(BmRowContainerTest, AppendBatchCanCrossChunks) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting,
      32);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<std::string>({"a", "bb", "ccc", "dddd", "eeeee", "ffffff"}),
  });

  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);

  ASSERT_EQ(6, rows.size());
  auto bigintResult = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, bigintResult);
  auto bigintFlat = bigintResult->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, bigintFlat);
  for (auto i = 0; i < rows.size(); ++i) {
    EXPECT_EQ(i + 1, bigintFlat->valueAt(i));
  }

  auto varcharResult = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, varcharResult);
  auto varcharFlat = varcharResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, varcharFlat);
  EXPECT_EQ("a", varcharFlat->valueAt(0).str());
  EXPECT_EQ("bb", varcharFlat->valueAt(1).str());
  EXPECT_EQ("ccc", varcharFlat->valueAt(2).str());
  EXPECT_EQ("dddd", varcharFlat->valueAt(3).str());
  EXPECT_EQ("eeeee", varcharFlat->valueAt(4).str());
  EXPECT_EQ("ffffff", varcharFlat->valueAt(5).str());
}

TEST_F(BmRowContainerTest, AppendBatchStoresNonInlineStringsAcrossChunks) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting,
      32);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<std::string>({
          std::string(40, 'a'),
          std::string(41, 'b'),
          std::string(42, 'c'),
          std::string(43, 'd'),
          std::string(44, 'e'),
          std::string(45, 'f'),
      }),
  });

  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);

  ASSERT_EQ(6, rows.size());
  auto varcharResult = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, varcharResult);
  auto varcharFlat = varcharResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, varcharFlat);
  EXPECT_EQ(std::string(40, 'a'), varcharFlat->valueAt(0).str());
  EXPECT_EQ(std::string(41, 'b'), varcharFlat->valueAt(1).str());
  EXPECT_EQ(std::string(42, 'c'), varcharFlat->valueAt(2).str());
  EXPECT_EQ(std::string(43, 'd'), varcharFlat->valueAt(3).str());
  EXPECT_EQ(std::string(44, 'e'), varcharFlat->valueAt(4).str());
  EXPECT_EQ(std::string(45, 'f'), varcharFlat->valueAt(5).str());
}

TEST_F(BmRowContainerTest, AppendBatchStoresMultipleNonInlineStringColumns) {
  BmRowContainer container(
      {BIGINT(), VARCHAR(), VARCHAR()},
      {false, false, false},
      bufferManager_,
      MemoryTag::kTesting,
      256,
      64);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4}),
      makeFlatVector<std::string>({
          std::string(40, 'a'),
          std::string(41, 'b'),
          std::string(42, 'c'),
          std::string(43, 'd'),
      }),
      makeFlatVector<std::string>({
          std::string(44, 'e'),
          std::string(45, 'f'),
          std::string(46, 'g'),
          std::string(47, 'h'),
      }),
  });

  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);

  ASSERT_EQ(4, rows.size());
  auto left = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, left);
  auto leftFlat = left->asFlatVector<StringView>();
  ASSERT_NE(nullptr, leftFlat);
  EXPECT_EQ(std::string(40, 'a'), leftFlat->valueAt(0).str());
  EXPECT_EQ(std::string(41, 'b'), leftFlat->valueAt(1).str());
  EXPECT_EQ(std::string(42, 'c'), leftFlat->valueAt(2).str());
  EXPECT_EQ(std::string(43, 'd'), leftFlat->valueAt(3).str());

  auto right = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 2, right);
  auto rightFlat = right->asFlatVector<StringView>();
  ASSERT_NE(nullptr, rightFlat);
  EXPECT_EQ(std::string(44, 'e'), rightFlat->valueAt(0).str());
  EXPECT_EQ(std::string(45, 'f'), rightFlat->valueAt(1).str());
  EXPECT_EQ(std::string(46, 'g'), rightFlat->valueAt(2).str());
  EXPECT_EQ(std::string(47, 'h'), rightFlat->valueAt(3).str());
}

TEST_F(BmRowContainerTest, AppendBatchReloadsMultipleStringColumnsAcrossChunks) {
  BmRowContainer container(
      {BIGINT(), VARCHAR(), VARCHAR()},
      {false, false, false},
      bufferManager_,
      MemoryTag::kTesting,
      128,
      64);
  constexpr vector_size_t kRows = 12;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row + 100; }),
      makeFlatVector<std::string>(kRows, [](auto row) {
        return std::string(40 + row, static_cast<char>('a' + row));
      }),
      makeFlatVector<std::string>(kRows, [](auto row) {
        return std::string(48 + row, static_cast<char>('m' + row));
      }),
  });

  container.appendBatch(input);
  auto segment = container.spillActiveSegment();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = session.listRowIds();
  ASSERT_EQ(kRows, rowIds.size());
  auto rows = session.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, rows.size());

  auto bigint = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, bigint);
  auto bigintFlat = bigint->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, bigintFlat);
  auto left = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, left);
  auto leftFlat = left->asFlatVector<StringView>();
  ASSERT_NE(nullptr, leftFlat);
  auto right = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 2, right);
  auto rightFlat = right->asFlatVector<StringView>();
  ASSERT_NE(nullptr, rightFlat);

  for (auto row = 0; row < kRows; ++row) {
    EXPECT_EQ(row + 100, bigintFlat->valueAt(row));
    EXPECT_EQ(
        std::string(40 + row, static_cast<char>('a' + row)),
        leftFlat->valueAt(row).str());
    EXPECT_EQ(
        std::string(48 + row, static_cast<char>('m' + row)),
        rightFlat->valueAt(row).str());
  }
}

TEST_F(BmRowContainerTest, RejectsUnsupportedComplexTypes) {
  EXPECT_THROW(
      BmRowContainer(
          {ARRAY(BIGINT())}, {false}, bufferManager_, MemoryTag::kTesting),
      BoltRuntimeError);
}

TEST_F(BmRowContainerTest, MergeReadSegmentsReadsMaterializedOrder) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeInput();
  auto rows = storeAll(container, input);

  std::vector<char*> sorted{rows[1], rows[3], rows[2], rows[0]};
  auto segment =
      container.finalizeReorderedSegment({sorted.data(), sorted.size()});
  EXPECT_EQ(SegmentState::kFinalizedFlushed, container.segmentState(segment));

  auto session = container.beginMergeReadSegments({&segment, 1});
  std::vector<char*> batch;
  std::vector<std::string> values;
  while (session.next(batch, 2)) {
    auto result = BaseVector::create(VARCHAR(), batch.size(), pool());
    container.extractColumnResident(batch.data(), batch.size(), 1, result);
    auto flat = result->asFlatVector<StringView>();
    for (auto i = 0; i < batch.size(); ++i) {
      values.push_back(flat->valueAt(i).str());
    }
  }
  EXPECT_EQ(
      (std::vector<std::string>{"alpha", "bravo", "charlie", "delta"}),
      values);

  container.releaseSegment(segment);
  EXPECT_THROW(
      container.beginMergeReadSegments({&segment, 1}), BoltRuntimeError);
}

TEST_F(BmRowContainerTest, MergeReadRejectsUnorderedSegments) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeInput();
  storeAll(container, input);
  auto segment = container.spillActiveSegment();
  EXPECT_THROW(
      container.beginMergeReadSegments({&segment, 1}), BoltRuntimeError);
}

TEST_F(BmRowContainerTest, MergeReadDefaultsToReleasingConsumedChunkBlocks) {
  BmRowContainer container(
      {BIGINT()},
      {false},
      bufferManager_,
      MemoryTag::kTesting,
      8);
  auto input = makeRowVector({makeFlatVector<int64_t>({4, 1, 3, 2})});
  auto rows = storeAll(container, input);

  std::vector<char*> ordered{rows[1], rows[3], rows[2], rows[0]};
  auto segment =
      container.finalizeReorderedSegment({ordered.data(), ordered.size()});
  auto session = container.beginMergeReadSegments({&segment, 1});
  std::vector<char*> batch;
  ASSERT_TRUE(session.next(batch, 1));
  ASSERT_EQ(1, batch.size());
  auto first = BaseVector::create(BIGINT(), 1, pool());
  container.extractColumnResident(batch.data(), batch.size(), 0, first);
  EXPECT_EQ(1, first->asFlatVector<int64_t>()->valueAt(0));

  ASSERT_TRUE(session.next(batch, 1));
  ASSERT_EQ(1, batch.size());
  auto second = BaseVector::create(BIGINT(), 1, pool());
  container.extractColumnResident(batch.data(), batch.size(), 0, second);
  EXPECT_EQ(2, second->asFlatVector<int64_t>()->valueAt(0));

  auto bulk = container.beginBulkReadSegments({&segment, 1});
  EXPECT_THROW(bulk.loadRows(), BoltRuntimeError);
}

TEST_F(BmRowContainerTest, MergeReadWithoutReleaseKeepsConsumedChunksReadable) {
  BmRowContainer container(
      {BIGINT()},
      {false},
      bufferManager_,
      MemoryTag::kTesting,
      8);
  auto input = makeRowVector({makeFlatVector<int64_t>({4, 1, 3, 2})});
  auto rows = storeAll(container, input);

  std::vector<char*> ordered{rows[1], rows[3], rows[2], rows[0]};
  auto segment =
      container.finalizeReorderedSegment({ordered.data(), ordered.size()});
  auto session = container.beginMergeReadSegments({&segment, 1}, false);
  std::vector<char*> batch;
  ASSERT_TRUE(session.next(batch, 2));
  ASSERT_EQ(2, batch.size());

  auto bulk = container.beginBulkReadSegments({&segment, 1});
  auto loadedRows = bulk.loadRows();
  ASSERT_EQ(4, loadedRows.size());

  auto result = BaseVector::create(BIGINT(), loadedRows.size(), pool());
  container.extractColumnResident(
      loadedRows.data(), loadedRows.size(), 0, result);
  auto flat = result->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ(1, flat->valueAt(0));
  EXPECT_EQ(2, flat->valueAt(1));
  EXPECT_EQ(3, flat->valueAt(2));
  EXPECT_EQ(4, flat->valueAt(3));
}

TEST_F(BmRowContainerTest, PartitionCanFlushMultipleSegments) {
  BmRowContainer container(
      {BIGINT()}, {false}, bufferManager_, MemoryTag::kTesting);
  auto input = makeRowVector({makeFlatVector<int64_t>({1, 2})});
  SelectivityVector rows(input->size());
  DecodedVector decoded;
  decoded.decode(*input->childAt(0), rows);

  auto firstContext = container.appendRow(7);
  container.store(firstContext, decoded, 0, 0);
  auto firstSegment = container.spillActivePartitionSegment(7);

  auto secondContext = container.appendRow(7);
  container.store(secondContext, decoded, 1, 0);
  auto secondSegment = container.spillActivePartitionSegment(7);

  EXPECT_NE(firstSegment, secondSegment);
  EXPECT_EQ(
      SegmentState::kFinalizedFlushed, container.segmentState(firstSegment));
  EXPECT_EQ(
      SegmentState::kFinalizedFlushed, container.segmentState(secondSegment));
  EXPECT_EQ(2, container.segmentsForPartition(7).size());
}

TEST_F(BmRowContainerTest, PartitionUsesFixedVectorLimit) {
  BmRowContainer container(
      {BIGINT()}, {false}, bufferManager_, MemoryTag::kTesting);
  auto input = makeRowVector({makeFlatVector<int64_t>({42})});
  SelectivityVector rows(input->size());
  DecodedVector decoded;
  decoded.decode(*input->childAt(0), rows);

  auto context = container.appendRow(255);
  container.store(context, decoded, 0, 0);
  auto segment = container.spillActivePartitionSegment(255);

  EXPECT_EQ(1, container.segmentsForPartition(255).size());
  EXPECT_EQ(segment, container.segmentsForPartition(255)[0]);
  EXPECT_THROW({ (void)container.appendRow(256); }, BoltRuntimeError);
}

} // namespace
} // namespace bytedance::bolt::exec::bm

int main(int argc, char** argv) {
  bytedance::bolt::memory::MemoryManager::testingSetInstance({});
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
