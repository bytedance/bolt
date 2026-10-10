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

#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::exec::bm {

BmRowContainer::BmRowContainer(
    std::vector<TypePtr> types,
    std::vector<bool> nullable,
    uint32_t numKeyColumns,
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    memory::bm::MemoryTag tag,
    uint32_t rowBlockSize,
    uint32_t heapBlockSize)
    : types_(std::move(types)),
      layout_(types_, nullable, numKeyColumns, rowBlockSize),
      bufferManager_(std::move(bufferManager)),
      segments_(bufferManager_, tag, &layout_, rowBlockSize, heapBlockSize),
      blockLoader_(bufferManager_, &layout_, &segments_),
      rowCopier_(&types_, &layout_, &segments_) {
  BOLT_CHECK_NOT_NULL(bufferManager_);
}

RowWriteContext BmRowContainer::appendRow(PartitionId partition) {
  auto& segment = segments_.activeSegment(partition);
  auto* row = segments_.newRowInSegment(segment);
  BOLT_DCHECK_NOT_NULL(segment.writeCursor.chunk);
  auto& chunk = *segment.writeCursor.chunk;
  RowWriteContext context(&segment, &chunk, row);
  // Seed the writable heap tail from the chunk's current last heap block so the
  // speculative complex-value store path can append into existing capacity
  // instead of treating every row as if no heap block existed. The block is
  // already recorded for this chunk, so mark it recorded to avoid a redundant
  // recordHeapForChunk() call on the first write.
  if (!chunk.heapBlocks.empty()) {
    auto& heap = chunk.heapBlocks.back();
    context.currentHeap_ = &heap;
    context.recordedHeapBlock_ = heap.id;
  }
  return context;
}

} // namespace bytedance::bolt::exec::bm
