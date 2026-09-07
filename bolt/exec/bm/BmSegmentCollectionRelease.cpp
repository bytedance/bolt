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

#include "bolt/exec/bm/BmSegmentCollection.h"

namespace bytedance::bolt::exec::bm {

void BmSegmentCollection::releaseChunkBlocks(ChunkData& chunk) {
  if (chunk.consumed) {
    return;
  }
  chunk.rowBlock.handle = memory::bm::BufferHandle{};
  chunk.rowBlock.ptr = nullptr;
  chunk.rowBlock.block.reset();
  for (auto& block : chunk.heapBlocks) {
    block.handle = memory::bm::BufferHandle{};
    block.ptr = nullptr;
    block.block.reset();
  }
  chunk.heapBlocks.clear();
  chunk.heapBases.clear();
  chunk.consumed = true;
}

void BmSegmentCollection::popFrontRows(uint64_t rowCount) {
  if (rowCount == 0) {
    return;
  }
  BOLT_CHECK_LE(
      rowCount,
      nextGlobalRow_ - frontRowsPopped_,
      "Cannot pop {} rows from BM RowContainer with only {} rows remaining",
      rowCount,
      nextGlobalRow_ - frontRowsPopped_);
  frontRowsPopped_ += rowCount;

  for (const auto& segmentPtr : segments_) {
    if (segmentPtr == nullptr) {
      continue;
    }
    auto& segment = *segmentPtr;
    for (const auto& chunkPtr : segment.chunks) {
      auto& chunk = *chunkPtr;
      if (chunk.consumed) {
        continue;
      }
      const auto chunkBegin =
          segment.meta.firstGlobalRow + chunk.meta.firstRowNumber;
      const auto chunkEnd = chunkBegin + chunk.meta.rowCount;
      if (chunkEnd > frontRowsPopped_) {
        return;
      }
      if (segment.meta.state == SegmentState::kActiveResident &&
          segment.writeCursor.chunk == &chunk) {
        return;
      }
      releaseChunkBlocks(chunk);
    }
  }
}

uint64_t BmSegmentCollection::segmentBytes(const SegmentData& segment) const {
  uint64_t bytes = 0;
  for (const auto& chunkPtr : segment.chunks) {
    const auto& chunk = *chunkPtr;
    if (chunk.consumed) {
      continue;
    }
    bytes += chunk.rowBlock.size;
    for (const auto& block : chunk.heapBlocks) {
      bytes += block.size;
    }
  }
  return bytes;
}

} // namespace bytedance::bolt::exec::bm
