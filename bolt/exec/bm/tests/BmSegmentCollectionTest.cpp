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

#include "bolt/exec/bm/BmRowLayout.h"
#include "bolt/exec/bm/BmSegmentCollection.h"

#include <cstring>
#include <optional>
#include <vector>

namespace bytedance::bolt::exec::bm {
namespace {

using bytedance::bolt::memory::bm::MemoryTag;

TEST_F(BmRowContainerTest, ChunkRowDirectoryLocatesDenseBoundariesDirectly) {
  const std::vector<RowNumber> starts{0, 3, 10, 11, 63, 64, 65, 130};
  std::vector<std::unique_ptr<ChunkData>> chunks;
  chunks.reserve(starts.size());
  for (size_t i = 0; i < starts.size(); ++i) {
    auto chunk = std::make_unique<ChunkData>();
    chunk->meta.id = i;
    chunk->meta.firstRowNumber = starts[i];
    chunk->meta.rowCount =
        i + 1 < starts.size() ? starts[i + 1] - starts[i] : 71;
    chunks.push_back(std::move(chunk));
  }

  ChunkRowDirectory directory;
  EXPECT_TRUE(directory.empty());
  directory.rebuild(chunks);
  EXPECT_FALSE(directory.empty());
  ChunkRowDirectory incremental;
  for (const auto& chunk : chunks) {
    incremental.appendChunkStart(chunk->meta);
  }

  size_t expectedChunk = 0;
  for (RowNumber row = 0; row <= 200; ++row) {
    while (expectedChunk + 1 < starts.size() &&
           starts[expectedChunk + 1] <= row) {
      ++expectedChunk;
    }
    EXPECT_EQ(expectedChunk, directory.chunkIndex(row, chunks.size() - 1))
        << "row=" << row;
    EXPECT_EQ(expectedChunk, incremental.chunkIndex(row, chunks.size() - 1))
        << "incremental row=" << row;
  }
}

TEST_F(BmRowContainerTest, IrregularDirectorySurvivesConsumedChunkMetadata) {
  BmRowLayout layout({BIGINT(), VARCHAR()}, {false, false}, 0, 4096);
  BmSegmentCollection segments(
      bufferManager_, MemoryTag::kTesting, &layout, 4096, 64, nullptr, 128);
  auto& segment = segments.createSegment(kDefaultPartition);
  std::vector<BatchAppendRange> ranges;
  segments.reserveRowsInBatch(segment, 0, 32, ranges, nullptr);
  auto& firstChunk = segments.currentChunk(segment);
  auto& firstHeap = segments.ensureHeapBlockInChunk(firstChunk, 64);
  firstHeap.used = firstHeap.size;
  (void)segments.ensureHeapBlockInChunk(firstChunk, 64);
  ASSERT_TRUE(firstChunk.sealRequested);
  segments.sealChunkAtBatchBoundary(segment);
  (void)segments.reserveRowsInCurrentChunk(segment, 32, 1, nullptr);

  ASSERT_FALSE(segment.chunkRowDirectory.empty());
  ASSERT_EQ(2, segment.chunks.size());
  const auto firstBlock = segment.chunks[0]->rowBlock.id;
  const auto secondBlock = segment.chunks[1]->rowBlock.id;
  segments.releaseChunkBlocks(*segment.chunks[0]);

  EXPECT_TRUE(segment.chunks[0]->consumed);
  EXPECT_EQ(firstBlock, segments.rowIdForRowNumber(segment, 31).rowBlockId);
  EXPECT_EQ(secondBlock, segments.rowIdForRowNumber(segment, 32).rowBlockId);
}

TEST_F(
    BmRowContainerTest,
    SegmentCollectionDoesNotShareHeapBlocksAcrossChunks) {
  BmRowLayout layout({BIGINT(), VARCHAR()}, {false, false}, 0, 64);
  BmSegmentCollection segments(
      bufferManager_, MemoryTag::kTesting, &layout, layout.rowSize(), 1024);
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
      firstStoredChunk.heapBlocks[0].id, secondStoredChunk.heapBlocks[0].id);
}

TEST_F(BmRowContainerTest, SegmentCollectionLeavesHeapTailUntilFinalize) {
  BmRowLayout layout({BIGINT(), VARCHAR()}, {false, false}, 0, 64);
  BmSegmentCollection segments(
      bufferManager_, MemoryTag::kTesting, &layout, 4 << 20, 64);
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
  BmRowLayout layout({BIGINT()}, {false}, 0, 64);
  BmSegmentCollection segments(
      bufferManager_, MemoryTag::kTesting, &layout, 4 << 20, 64);

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

TEST_F(BmRowContainerTest, HeapCapacityCountsAllocatedBlocks) {
  BmRowLayout layout({BIGINT(), VARCHAR()}, {false, false}, 0, 4096);
  BmSegmentCollection segments(
      bufferManager_, MemoryTag::kTesting, &layout, 4096, 64);
  auto& segment = segments.createSegment(kDefaultPartition);
  segments.newRowInSegment(segment);
  auto& chunk = segments.currentChunk(segment);

  auto& first = segments.ensureHeapBlockInChunk(chunk, 40);
  EXPECT_EQ(64, first.size);
  first.used += 40;
  EXPECT_EQ(64, chunk.heapAllocatedBytes);

  auto& reused = segments.ensureHeapBlockInChunk(chunk, 16);
  EXPECT_EQ(first.id, reused.id);
  reused.used += 16;
  EXPECT_EQ(64, chunk.heapAllocatedBytes);

  auto& second = segments.ensureHeapBlockInChunk(chunk, 17);
  EXPECT_EQ(64, second.size);
  second.used += 17;
  EXPECT_EQ(128, chunk.heapAllocatedBytes);

  auto& oversized = segments.ensureHeapBlockInChunk(chunk, 160);
  EXPECT_EQ(160, oversized.size);
  oversized.used += 160;
  EXPECT_EQ(288, chunk.heapAllocatedBytes);

  segments.releaseChunkBlocks(chunk);
  EXPECT_EQ(0, chunk.heapAllocatedBytes);
}

} // namespace
} // namespace bytedance::bolt::exec::bm
