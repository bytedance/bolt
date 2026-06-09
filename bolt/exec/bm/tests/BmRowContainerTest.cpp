#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

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
      auto* target = container.newRow();
      for (auto column = 0; column < input->childrenSize(); ++column) {
        container.store(decoded[column], row, target, column);
      }
      rowsOut.push_back(target);
    }
    return rowsOut;
  }

  std::shared_ptr<MemoryPool> root_;
  std::shared_ptr<BufferManager> bufferManager_;
};

TEST_F(BmRowContainerTest, ResidentStoreCompareAndExtract) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()}, bufferManager_, MemoryTag::kTesting);
  auto input = makeInput();
  auto rows = storeAll(container, input);

  EXPECT_GT(container.compare(rows[0], rows[1], 0), 0);
  EXPECT_LT(container.compare(rows[1], rows[2], 0), 0);
  EXPECT_GT(container.compare(rows[0], rows[1], 1), 0);

  auto result = BaseVector::create(BIGINT(), rows.size(), pool());
  std::vector<const char*> inputRows;
  inputRows.reserve(rows.size());
  for (const auto* row : rows) {
    inputRows.push_back(row);
  }
  container.extractColumnResident(inputRows.data(), inputRows.size(), 0, result);

  auto flat = result->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ(10, flat->valueAt(0));
  EXPECT_EQ(3, flat->valueAt(1));
  EXPECT_EQ(7, flat->valueAt(2));
  EXPECT_EQ(3, flat->valueAt(3));
}

TEST_F(BmRowContainerTest, TryLoadAllReturnsStablePointersWhenResident) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()}, bufferManager_, MemoryTag::kTesting);
  auto input = makeInput();
  storeAll(container, input);

  auto segment = container.flushActiveSegment();
  ASSERT_EQ(SegmentState::kFinalizedFlushed, container.segmentState(segment));

  auto session = container.beginBulkReadSegments({&segment, 1});
  ASSERT_EQ(ReadMode::kFullyResident, session.mode());

  std::vector<char*> rows;
  std::vector<RowId> rowIds;
  ASSERT_EQ(LoadAllResult::kLoadedPointers, session.tryLoadAll(rows, rowIds));
  ASSERT_EQ(input->size(), rows.size());
  ASSERT_TRUE(rowIds.empty());
  EXPECT_EQ(0, container.compare(rows[1], rows[3], 0));
  EXPECT_LT(container.compare(rows[1], rows[3], 1), 0);

  auto result = BaseVector::create(VARCHAR(), rows.size(), pool());
  std::vector<const char*> inputRows(rows.begin(), rows.end());
  container.extractColumnResident(inputRows.data(), inputRows.size(), 1, result);

  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("delta", flat->valueAt(0).str());
  EXPECT_EQ("alpha", flat->valueAt(1).str());
  EXPECT_EQ("charlie", flat->valueAt(2).str());
  EXPECT_EQ("bravo", flat->valueAt(3).str());
}

TEST_F(BmRowContainerTest, TryLoadAllReturnsRowIdsForWindowRead) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()}, bufferManager_, MemoryTag::kTesting);
  auto input = makeInput();
  storeAll(container, input);

  auto segment = container.flushActiveSegment();
  ReadSessionOptions options;
  options.maxPinnedBytes = 1;
  auto session = container.beginBulkReadSegments({&segment, 1}, options);
  ASSERT_EQ(ReadMode::kWindowRead, session.mode());

  std::vector<char*> rows;
  std::vector<RowId> rowIds;
  ASSERT_EQ(LoadAllResult::kNeedWindowRead, session.tryLoadAll(rows, rowIds));
  ASSERT_TRUE(rows.empty());
  ASSERT_EQ(input->size(), rowIds.size());

  std::vector<RowId> reordered{rowIds[2], rowIds[0], rowIds[3], rowIds[1]};
  auto window = session.loadRows({reordered.data(), reordered.size()});
  ASSERT_EQ(reordered.size(), window.rows.size());

  auto result = BaseVector::create(VARCHAR(), reordered.size(), pool());
  std::vector<const char*> inputRows;
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
  const char* alphaRow = alpha;
  container.extractColumnResident(&alphaRow, 1, 1, single);
  EXPECT_EQ("alpha", single->asFlatVector<StringView>()->valueAt(0).str());
}

TEST_F(BmRowContainerTest, SortedRunCursorReadsMaterializedOrder) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()}, bufferManager_, MemoryTag::kTesting);
  auto input = makeInput();
  auto rows = storeAll(container, input);

  std::vector<char*> sorted{rows[1], rows[3], rows[2], rows[0]};
  SortedRunOptions options;
  options.preferredLayout = SortedRunLayout::kMaterializedOrder;
  auto run = container.finalizeSortedRun({sorted.data(), sorted.size()}, options);

  auto session = container.beginMergeReadSegments({&run, 1});
  auto cursor = session.cursor(run);
  ASSERT_TRUE(cursor.hasCurrent());

  std::vector<std::string> values;
  while (cursor.hasCurrent()) {
    auto result = BaseVector::create(VARCHAR(), 1, pool());
    const char* row = cursor.currentRow();
    container.extractColumnResident(&row, 1, 1, result);
    values.push_back(result->asFlatVector<StringView>()->valueAt(0).str());
    cursor.advance();
  }
  EXPECT_EQ(
      (std::vector<std::string>{"alpha", "bravo", "charlie", "delta"}),
      values);
}

TEST_F(BmRowContainerTest, PartitionCanFlushMultipleSegments) {
  BmRowContainer container({BIGINT()}, bufferManager_, MemoryTag::kTesting);
  auto input = makeRowVector({makeFlatVector<int64_t>({1, 2})});
  SelectivityVector rows(input->size());
  DecodedVector decoded;
  decoded.decode(*input->childAt(0), rows);

  auto* first = container.newRow(7);
  container.store(decoded, 0, first, 0);
  auto firstSegment = container.flushActivePartitionSegment(7);

  auto* second = container.newRow(7);
  container.store(decoded, 1, second, 0);
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
