#include "bolt/exec/bm/tests/BmRowContainerTestBase.h"

#include <string>
#include <vector>

namespace bytedance::bolt::exec::bm {
namespace {

using bytedance::bolt::memory::bm::MemoryTag;

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

} // namespace
} // namespace bytedance::bolt::exec::bm
