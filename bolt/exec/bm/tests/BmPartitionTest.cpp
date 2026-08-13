#include "bolt/exec/bm/tests/BmRowContainerTestBase.h"

namespace bytedance::bolt::exec::bm {
namespace {

using bytedance::bolt::memory::bm::MemoryTag;

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
