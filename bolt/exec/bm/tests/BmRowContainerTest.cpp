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

  std::vector<RowHandle> storeAll(
      BmRowContainer& container,
      RowVectorPtr input) {
    SelectivityVector rows(input->size());
    std::vector<DecodedVector> decoded(input->childrenSize());
    for (auto i = 0; i < input->childrenSize(); ++i) {
      decoded[i].decode(*input->childAt(i), rows);
    }

    std::vector<RowHandle> handles;
    handles.reserve(input->size());
    for (auto row = 0; row < input->size(); ++row) {
      auto handle = container.newRow();
      for (auto column = 0; column < input->childrenSize(); ++column) {
        container.store(decoded[column], row, handle.ptr, column);
      }
      handles.push_back(handle);
    }
    return handles;
  }

  std::shared_ptr<MemoryPool> root_;
  std::shared_ptr<BufferManager> bufferManager_;
};

TEST_F(BmRowContainerTest, ResidentStoreCompareAndExtract) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()}, bufferManager_, MemoryTag::kTesting);
  auto input = makeInput();
  auto handles = storeAll(container, input);

  EXPECT_GT(container.compare(handles[0].ptr, handles[1].ptr, 0), 0);
  EXPECT_LT(container.compare(handles[1].ptr, handles[2].ptr, 0), 0);
  EXPECT_GT(container.compare(handles[0].ptr, handles[1].ptr, 1), 0);

  auto result = BaseVector::create(BIGINT(), handles.size(), pool());
  std::vector<const char*> rows;
  rows.reserve(handles.size());
  for (const auto& handle : handles) {
    rows.push_back(handle.ptr);
  }
  container.extractColumnResident(rows.data(), rows.size(), 0, result);

  auto flat = result->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ(10, flat->valueAt(0));
  EXPECT_EQ(3, flat->valueAt(1));
  EXPECT_EQ(7, flat->valueAt(2));
  EXPECT_EQ(3, flat->valueAt(3));
}

TEST_F(BmRowContainerTest, FlushBulkLoadResolveRowsAndExtract) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()}, bufferManager_, MemoryTag::kTesting);
  auto input = makeInput();
  auto handles = storeAll(container, input);
  std::vector<RowId> rowIds;
  rowIds.reserve(handles.size());
  for (const auto& handle : handles) {
    rowIds.push_back(handle.id);
  }

  auto segment = container.flushActiveSegment();
  ASSERT_EQ(SegmentState::kFinalizedFlushed, container.segmentState(segment));

  auto session = container.beginBulkReadSegments({&segment, 1});
  ASSERT_EQ(ReadMode::kFullyResident, session.mode());

  std::vector<char*> resolved;
  auto resolvedRange =
      session.resolveRows({rowIds.data(), rowIds.size()}, resolved);
  ASSERT_EQ(rowIds.size(), resolvedRange.size());
  EXPECT_EQ(0, container.compare(resolved[1], resolved[3], 0));
  EXPECT_LT(container.compare(resolved[1], resolved[3], 1), 0);

  auto result = BaseVector::create(VARCHAR(), rowIds.size(), pool());
  session.extractColumn({rowIds.data(), rowIds.size()}, 1, 0, result);

  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("delta", flat->valueAt(0).str());
  EXPECT_EQ("alpha", flat->valueAt(1).str());
  EXPECT_EQ("charlie", flat->valueAt(2).str());
  EXPECT_EQ("bravo", flat->valueAt(3).str());
}

TEST_F(BmRowContainerTest, WindowReadExtractsInCallerOrder) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()}, bufferManager_, MemoryTag::kTesting);
  auto input = makeInput();
  auto handles = storeAll(container, input);
  std::vector<RowId> rowIds;
  rowIds.reserve(handles.size());
  for (const auto& handle : handles) {
    rowIds.push_back(handle.id);
  }

  auto segment = container.flushActiveSegment();
  ReadSessionOptions options;
  options.maxPinnedBytes = 1;
  auto session = container.beginBulkReadSegments({&segment, 1}, options);
  ASSERT_EQ(ReadMode::kWindowRead, session.mode());

  std::vector<RowId> reordered{rowIds[2], rowIds[0], rowIds[3], rowIds[1]};
  auto result = BaseVector::create(VARCHAR(), reordered.size(), pool());
  session.extractColumn({reordered.data(), reordered.size()}, 1, 0, result);

  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("charlie", flat->valueAt(0).str());
  EXPECT_EQ("delta", flat->valueAt(1).str());
  EXPECT_EQ("bravo", flat->valueAt(2).str());
  EXPECT_EQ("alpha", flat->valueAt(3).str());
}

TEST_F(BmRowContainerTest, SortedRunCursorReadsRowIdOrder) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()}, bufferManager_, MemoryTag::kTesting);
  auto input = makeInput();
  auto handles = storeAll(container, input);

  std::vector<RowHandle> sorted{handles[1], handles[3], handles[2], handles[0]};
  SortedRunOptions options;
  options.preferredLayout = SortedRunLayout::kRowIdOrder;
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

  auto first = container.newRow(7);
  container.store(decoded, 0, first.ptr, 0);
  auto firstSegment = container.flushActivePartitionSegment(7);

  auto second = container.newRow(7);
  container.store(decoded, 1, second.ptr, 0);
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
