#include "bolt/exec/bm/BmSegmentCollection.h"

#include "bolt/common/base/Exceptions.h"

#include <algorithm>
#include <cstring>
#include <utility>

namespace bytedance::bolt::exec::bm {
namespace {

void recordHeapBase(ChunkData& chunk, const BlockRef& heap) {
  if (chunk.heapBlocks.empty() || chunk.heapBlocks.back().id != heap.id) {
    // ensureHeapBlock() only reuses heap blocks from the owning chunk. A miss
    // here means a write context no longer matches the row being stored.
    BOLT_DCHECK(
        std::find_if(
            chunk.heapBlocks.begin(),
            chunk.heapBlocks.end(),
            [&](const BlockRef& block) { return block.id == heap.id; }) !=
            chunk.heapBlocks.end(),
        "Heap block {} was not allocated by chunk {}",
        heap.id,
        chunk.meta.id);
  }

  const auto base = reinterpret_cast<uintptr_t>(heap.ptr);
  if (!chunk.heapBases.empty() &&
      chunk.heapBases.back().heapBlockId == heap.id) {
    chunk.heapBases.back().baseAddress = base;
    chunk.heapBases.back().capacity = heap.size;
    return;
  }

  // A non-back duplicate means this chunk observed A->B->A heap usage. That is
  // unexpected for the current append-only writer and should be investigated
  // before adding a more complex lookup structure.
  BOLT_DCHECK(
      std::find_if(
          chunk.heapBases.begin(),
          chunk.heapBases.end(),
          [&](const HeapBaseRef& ref) { return ref.heapBlockId == heap.id; }) ==
          chunk.heapBases.end(),
      "Heap block {} is reused non-contiguously in chunk {}",
      heap.id,
      chunk.meta.id);
  chunk.heapBases.push_back({heap.id, base, heap.size});
}

} // namespace

BlockRef BmSegmentCollection::addBlock(uint32_t blockSize) {
  auto handle = bufferManager_->Allocate(blockSize, tag_);
  BlockRef block;
  block.id = nextBlockId_++;
  block.size = blockSize;
  block.ptr = handle.Ptr();
  block.block = handle.block();
  block.handle = std::move(handle);

  return block;
}

BlockRef& BmSegmentCollection::ensureHeapBlockSlow(
    ChunkData& chunk,
    uint32_t minBytes) {
  BOLT_DCHECK(!chunk.consumed);
  const auto blockSize = std::max(heapBlockSize_, minBytes);
  chunk.heapBlocks.push_back(addBlock(blockSize));
  return chunk.heapBlocks.back();
}

ChunkData& BmSegmentCollection::ensureWritableChunk(SegmentData& segment) {
  if (FOLLY_LIKELY(segment.currentChunk != kNoBlock)) {
    auto& chunk = segment.chunks[segment.currentChunk];
    if (FOLLY_LIKELY(chunk.rowBlock.used + layout().rowSize() <=
                     chunk.rowBlock.size)) {
      return chunk;
    }
  }

  ChunkData chunk;
  chunk.meta.id = segment.chunks.size();
  chunk.meta.segmentId = segment.meta.id;
  chunk.meta.firstRowNumber = segment.nextRowNumber;
  chunk.meta.rowCount = 0;
  chunk.rowBlock = addBlock(rowBlockSize_);

  segment.currentChunk = chunk.meta.id;
  segment.chunks.push_back(std::move(chunk));
  return segment.chunks.back();
}

ChunkData& BmSegmentCollection::currentChunk(SegmentData& segment) {
  BOLT_DCHECK(segment.currentChunk != kNoBlock);
  BOLT_DCHECK_LT(segment.currentChunk, segment.chunks.size());
  return segment.chunks[segment.currentChunk];
}

const ChunkData& BmSegmentCollection::currentChunk(
    const SegmentData& segment) const {
  BOLT_DCHECK(segment.currentChunk != kNoBlock);
  BOLT_DCHECK_LT(segment.currentChunk, segment.chunks.size());
  return segment.chunks[segment.currentChunk];
}

char* BmSegmentCollection::newRowInSegment(SegmentData& segment) {
  BOLT_DCHECK(segment.meta.state == SegmentState::kActiveResident);
  auto& chunk = ensureWritableChunk(segment);
  auto& block = chunk.rowBlock;
  const auto offset = block.used;
  auto* row = block.ptr + offset;
  block.used += layout().rowSize();
  std::memset(row, 0, layout().rowSize());

  RowId rowId;
  rowId.segmentId = segment.meta.id;
  rowId.rowNumber = segment.nextRowNumber++;
  rowId.rowBlockId = block.id;
  rowId.rowOffset = offset;
  ++segment.meta.numRows;
  updateChunkForRow(segment, rowId);
  return row;
}

void BmSegmentCollection::updateChunkForRow(
    SegmentData& segment,
    const RowId& rowId) {
  // Current design keeps a chunk anchored to one row block. This makes window
  // read easier to reason about: loading a chunk pins one fixed-width row block
  // and the heap blocks referenced by rows in that block.
  auto& chunk = currentChunk(segment);
  ++chunk.meta.rowCount;
  BOLT_DCHECK_EQ(chunk.rowBlock.id, rowId.rowBlockId);
}

void BmSegmentCollection::recordHeapForCurrentChunk(
    SegmentData& segment,
    const BlockRef& heap) {
  BOLT_DCHECK(segment.currentChunk != kNoBlock);
  auto& chunk = currentChunk(segment);
  recordHeapBase(chunk, heap);
}

void BmSegmentCollection::recordHeapForChunk(
    SegmentData& segment,
    ChunkId chunkId,
    const BlockRef& heap,
    const char* row) {
  BOLT_DCHECK_LT(chunkId, segment.chunks.size());
  auto& chunk = segment.chunks[chunkId];

  BOLT_DCHECK([&]() {
    const auto rowAddress = reinterpret_cast<uintptr_t>(row);
    const auto& block = chunk.rowBlock;
    const auto blockBegin = reinterpret_cast<uintptr_t>(block.ptr);
    const auto rowOffset = static_cast<uint32_t>(rowAddress - blockBegin);
    return rowOffset < chunk.meta.rowCount * rowStride();
  }());

  recordHeapBase(chunk, heap);
}

} // namespace bytedance::bolt::exec::bm
