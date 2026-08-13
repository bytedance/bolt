#include "bolt/exec/bm/tests/BmRowContainerTestBase.h"

#include "bolt/exec/bm/BmRowLayout.h"
#include "bolt/exec/bm/BmSegmentCollection.h"

#include <cstring>
#include <optional>
#include <vector>

namespace bytedance::bolt::exec::bm {
namespace {

using bytedance::bolt::memory::bm::MemoryTag;

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

} // namespace
} // namespace bytedance::bolt::exec::bm
