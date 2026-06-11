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
    return container.appendBatch(input);
  }

  std::vector<char*> storeAllByRowWriteContext(
      BmRowContainer& container,
      RowVectorPtr input) {
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
  auto rows = storeAllByRowWriteContext(container, input);

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

TEST_F(BmRowContainerTest, AppendBatchAndStringCompare) {
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

TEST_F(BmRowContainerTest, TryLoadAllReturnsStablePointersWhenResident) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeInput();
  storeAll(container, input);

  auto segment = container.flushActiveSegment();
  ASSERT_EQ(SegmentState::kFinalizedFlushed, container.segmentState(segment));

  const auto batchPinsBeforeBegin = bufferManager_->stats().batchPinCount;
  auto session = container.beginBulkReadSegments({&segment, 1});
  EXPECT_EQ(batchPinsBeforeBegin, bufferManager_->stats().batchPinCount);

  std::vector<char*> rows;
  std::vector<RowId> rowIds;
  ASSERT_EQ(LoadAllResult::kLoadedPointers, session.tryLoadAll(rows, rowIds));
  EXPECT_EQ(batchPinsBeforeBegin + 1, bufferManager_->stats().batchPinCount);
  ASSERT_EQ(input->size(), rows.size());
  ASSERT_TRUE(rowIds.empty());
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

TEST_F(BmRowContainerTest, TryLoadAllReturnsRowIdsForWindowRead) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeInput();
  storeAll(container, input);

  auto segment = container.flushActiveSegment();
  ReadSessionOptions options;
  options.maxPinnedBytes = 1;
  const auto batchPinsBeforeBegin = bufferManager_->stats().batchPinCount;
  auto session = container.beginBulkReadSegments({&segment, 1}, options);
  EXPECT_EQ(batchPinsBeforeBegin, bufferManager_->stats().batchPinCount);

  std::vector<char*> rows;
  std::vector<RowId> rowIds;
  ASSERT_EQ(LoadAllResult::kNeedWindowRead, session.tryLoadAll(rows, rowIds));
  EXPECT_EQ(batchPinsBeforeBegin, bufferManager_->stats().batchPinCount);
  ASSERT_TRUE(rows.empty());
  ASSERT_EQ(input->size(), rowIds.size());

  std::vector<RowId> reordered{rowIds[2], rowIds[0], rowIds[3], rowIds[1]};
  auto window = session.loadRows({reordered.data(), reordered.size()});
  ASSERT_EQ(reordered.size(), window.rows.size());

  auto result = BaseVector::create(VARCHAR(), reordered.size(), pool());
  std::vector<char*> inputRows;
  inputRows.reserve(window.rows.size());
  for (const auto& row : window.rows) {
    inputRows.push_back(row.ptr);
  }
  container.extractColumnResident(inputRows.data(), inputRows.size(), 1, result);

  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("charlie", flat->valueAt(0).str());
  EXPECT_EQ("delta", flat->valueAt(1).str());
  EXPECT_EQ("bravo", flat->valueAt(2).str());
  EXPECT_EQ("alpha", flat->valueAt(3).str());

  auto* alpha = session.loadRow(rowIds[1]);
  auto single = BaseVector::create(VARCHAR(), 1, pool());
  char* alphaRow = alpha;
  container.extractColumnResident(&alphaRow, 1, 1, single);
  EXPECT_EQ("alpha", single->asFlatVector<StringView>()->valueAt(0).str());
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

  auto segment = container.flushActiveSegment();
  ReadSessionOptions options;
  options.maxPinnedBytes = 1;
  auto session = container.beginBulkReadSegments({&segment, 1}, options);

  std::vector<char*> rows;
  std::vector<RowId> rowIds;
  ASSERT_EQ(LoadAllResult::kNeedWindowRead, session.tryLoadAll(rows, rowIds));
  ASSERT_EQ(kRows, rowIds.size());

  auto window = session.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, window.rows.size());

  std::vector<char*> inputRows;
  inputRows.reserve(window.rows.size());
  for (const auto& row : window.rows) {
    inputRows.push_back(row.ptr);
  }
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

  auto segment = container.flushActiveSegment();
  ReadSessionOptions options;
  options.maxPinnedBytes = 1;
  auto session = container.beginBulkReadSegments({&segment, 1}, options);

  std::vector<char*> rows;
  std::vector<RowId> rowIds;
  ASSERT_EQ(LoadAllResult::kNeedWindowRead, session.tryLoadAll(rows, rowIds));
  ASSERT_EQ(kRows, rowIds.size());

  auto window = session.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, window.rows.size());

  std::vector<char*> inputRows;
  inputRows.reserve(window.rows.size());
  for (const auto& row : window.rows) {
    inputRows.push_back(row.ptr);
  }

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

TEST_F(BmRowContainerTest, StorageDoesNotShareHeapBlocksAcrossChunks) {
  BmRowLayout layout({BIGINT(), VARCHAR()}, {false, false}, 64);
  BmRowStorage storage(
      bufferManager_,
      MemoryTag::kTesting,
      &layout,
      layout.rowSize(),
      1024);
  auto& segment = storage.createSegment(kDefaultPartition);

  storage.newRowInSegment(segment);
  auto& firstHeap = storage.ensureHeapBlock(segment, 16);
  storage.recordHeapForCurrentChunk(segment, firstHeap);

  storage.newRowInSegment(segment);
  auto& secondHeap = storage.ensureHeapBlock(segment, 16);
  storage.recordHeapForCurrentChunk(segment, secondHeap);

  ASSERT_EQ(2, segment.chunks.size());
  ASSERT_EQ(1, segment.chunks[0].heapBlocks.size());
  ASSERT_EQ(1, segment.chunks[1].heapBlocks.size());
  EXPECT_NE(
      segment.chunks[0].heapBlocks[0].id,
      segment.chunks[1].heapBlocks[0].id);
}

TEST_F(BmRowContainerTest, StorageZerosHeapTailWhenSwitchingHeapBlocks) {
  BmRowLayout layout({BIGINT(), VARCHAR()}, {false, false}, 64);
  BmRowStorage storage(
      bufferManager_,
      MemoryTag::kTesting,
      &layout,
      4 << 20,
      64);
  auto& segment = storage.createSegment(kDefaultPartition);
  storage.newRowInSegment(segment);

  auto& firstHeap = storage.ensureHeapBlock(segment, 40);
  firstHeap.used = 40;
  auto* const firstHeapPtr = firstHeap.ptr;
  const auto firstHeapUsed = firstHeap.used;
  const auto firstHeapSize = firstHeap.size;
  const auto firstHeapId = firstHeap.id;
  std::memset(
      firstHeapPtr + firstHeapUsed, 0x7f, firstHeapSize - firstHeapUsed);

  auto& secondHeap = storage.ensureHeapBlock(segment, 40);
  ASSERT_NE(firstHeapId, secondHeap.id);

  for (uint32_t offset = firstHeapUsed; offset < firstHeapSize; ++offset) {
    EXPECT_EQ(0, firstHeapPtr[offset]) << "offset=" << offset;
  }
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
  auto cursor = session.makeCursor(segment);
  ASSERT_TRUE(cursor.hasCurrent());

  std::vector<std::string> values;
  while (cursor.hasCurrent()) {
    auto result = BaseVector::create(VARCHAR(), 1, pool());
    auto* row = const_cast<char*>(cursor.currentRow());
    container.extractColumnResident(&row, 1, 1, result);
    values.push_back(result->asFlatVector<StringView>()->valueAt(0).str());
    cursor.advance();
  }
  EXPECT_EQ(
      (std::vector<std::string>{"alpha", "bravo", "charlie", "delta"}),
      values);

  container.releaseSegment(segment);
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
  auto cursor = session.makeCursor(segment);
  ASSERT_TRUE(cursor.hasCurrent());

  auto first = BaseVector::create(BIGINT(), 1, pool());
  auto* firstRow = const_cast<char*>(cursor.currentRow());
  container.extractColumnResident(&firstRow, 1, 0, first);
  EXPECT_EQ(1, first->asFlatVector<int64_t>()->valueAt(0));

  cursor.advance();
  ASSERT_TRUE(cursor.hasCurrent());
  auto second = BaseVector::create(BIGINT(), 1, pool());
  auto* secondRow = const_cast<char*>(cursor.currentRow());
  container.extractColumnResident(&secondRow, 1, 0, second);
  EXPECT_EQ(2, second->asFlatVector<int64_t>()->valueAt(0));

  auto bulk = container.beginBulkReadSegments({&segment, 1});
  std::vector<char*> loadedRows;
  std::vector<RowId> rowIds;
  EXPECT_THROW(bulk.tryLoadAll(loadedRows, rowIds), BoltRuntimeError);
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
  auto cursor = session.makeCursor(segment);
  ASSERT_TRUE(cursor.hasCurrent());
  cursor.advance();
  ASSERT_TRUE(cursor.hasCurrent());

  auto bulk = container.beginBulkReadSegments({&segment, 1});
  std::vector<char*> loadedRows;
  std::vector<RowId> rowIds;
  ASSERT_EQ(LoadAllResult::kLoadedPointers, bulk.tryLoadAll(loadedRows, rowIds));
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
  auto firstSegment = container.flushActivePartitionSegment(7);

  auto secondContext = container.appendRow(7);
  container.store(secondContext, decoded, 1, 0);
  auto secondSegment = container.flushActivePartitionSegment(7);

  EXPECT_NE(firstSegment, secondSegment);
  EXPECT_EQ(
      SegmentState::kFinalizedFlushed, container.segmentState(firstSegment));
  EXPECT_EQ(
      SegmentState::kFinalizedFlushed, container.segmentState(secondSegment));
  EXPECT_EQ(2, container.segmentsForPartition(7).size());
}

} // namespace
} // namespace bytedance::bolt::exec::bm

int main(int argc, char** argv) {
  bytedance::bolt::memory::MemoryManager::testingSetInstance({});
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
