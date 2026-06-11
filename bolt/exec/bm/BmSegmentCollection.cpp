#include "bolt/exec/bm/BmSegmentCollection.h"

#include "bolt/common/base/Exceptions.h"

#include <algorithm>
#include <cstring>
#include <span>
#include <utility>

namespace bytedance::bolt::exec::bm {
namespace {

void zeroUnusedHeapTail(BlockRef& block) {
  BOLT_DCHECK_LE(block.used, block.size);
  if (block.ptr != nullptr && block.used < block.size) {
    std::memset(block.ptr + block.used, 0, block.size - block.used);
  }
}

void zeroUnusedHeapTail(ChunkData& chunk) {
  for (auto& block : chunk.heapBlocks) {
    zeroUnusedHeapTail(block);
  }
}

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

const std::vector<SegmentId> BmSegmentCollection::kEmptySegments_{};

BmSegmentCollection::BmSegmentCollection(
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    memory::bm::MemoryTag tag,
    const BmRowLayout* layout,
    uint32_t rowBlockSize,
    uint32_t heapBlockSize)
    : bufferManager_(std::move(bufferManager)),
      tag_(tag),
      layout_(layout),
      rowBlockSize_(rowBlockSize),
      heapBlockSize_(heapBlockSize) {
  BOLT_CHECK_NOT_NULL(bufferManager_);
  BOLT_CHECK_NOT_NULL(layout_);
}

SegmentId BmSegmentCollection::flushActiveSegment() {
  return flushActivePartitionSegment(kDefaultPartition);
}

SegmentId BmSegmentCollection::flushActivePartitionSegment(PartitionId partition) {
  return finalizeAndFlush(partition);
}

void BmSegmentCollection::releaseSegment(SegmentId segment) {
  auto it = segments_.find(segment);
  if (it == segments_.end()) {
    return;
  }
  if (it->second.meta.partitionId.has_value()) {
    const auto partition = *it->second.meta.partitionId;
    auto activeIt = activeSegments_.find(partition);
    if (activeIt != activeSegments_.end() && activeIt->second == segment) {
      activeSegments_.erase(activeIt);
    }
    auto partitionIt = partitionSegments_.find(partition);
    if (partitionIt != partitionSegments_.end()) {
      auto& segments = partitionIt->second;
      segments.erase(
          std::remove(segments.begin(), segments.end(), segment),
          segments.end());
    }
  }
  segments_.erase(it);
}

void BmSegmentCollection::releaseSegments(folly::Range<const SegmentId*> segments) {
  for (auto segment : segments) {
    releaseSegment(segment);
  }
}

SegmentState BmSegmentCollection::segmentState(SegmentId segment) const {
  return segmentData(segment).meta.state;
}

const std::vector<SegmentId>& BmSegmentCollection::segmentsForPartition(
    PartitionId partition) const {
  auto it = partitionSegments_.find(partition);
  if (it == partitionSegments_.end()) {
    return kEmptySegments_;
  }
  return it->second;
}

std::vector<SegmentId> BmSegmentCollection::allSegmentIds() const {
  std::vector<SegmentId> ids;
  ids.reserve(segments_.size());
  for (const auto& [id, _] : segments_) {
    ids.push_back(id);
  }
  return ids;
}

int64_t BmSegmentCollection::numRows() const {
  int64_t rows = 0;
  for (const auto& [_, segment] : segments_) {
    rows += segment.meta.numRows;
  }
  return rows;
}

SegmentData& BmSegmentCollection::activeSegment(PartitionId partition) {
  auto it = activeSegments_.find(partition);
  if (it != activeSegments_.end()) {
    return segmentData(it->second);
  }

  auto& segment = createSegment(partition);
  activeSegments_[partition] = segment.meta.id;
  return segment;
}

SegmentData& BmSegmentCollection::createSegment(
    std::optional<PartitionId> partition) {
  SegmentData segment;
  segment.meta.id = nextSegmentId_++;
  segment.meta.state = SegmentState::kActiveResident;
  segment.meta.partitionId = std::move(partition);
  const auto id = segment.meta.id;
  auto [inserted, _] = segments_.emplace(id, std::move(segment));
  return inserted->second;
}

SegmentId BmSegmentCollection::finalizeAndFlush(PartitionId partition) {
  auto active = activeSegments_.find(partition);
  BOLT_CHECK(active != activeSegments_.end());
  auto& segment = segmentData(active->second);
  const auto id = finalizeAndFlushSegment(segment);
  partitionSegments_[partition].push_back(id);
  activeSegments_.erase(active);
  return id;
}

SegmentId BmSegmentCollection::finalizeAndFlushSegment(SegmentData& segment) {
  BOLT_DCHECK(segment.meta.state == SegmentState::kActiveResident);
  segment.meta.state = SegmentState::kFinalizedResident;

  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  for (auto& chunk : segment.chunks) {
    BOLT_DCHECK(!chunk.consumed);
    blocks.reserve(blocks.size() + 1 + chunk.heapBlocks.size());
    zeroUnusedHeapTail(chunk);
    chunk.rowBlock.handle = memory::bm::BufferHandle{};
    chunk.rowBlock.ptr = nullptr;
    blocks.push_back(chunk.rowBlock.block);
    for (auto& block : chunk.heapBlocks) {
      block.handle = memory::bm::BufferHandle{};
      block.ptr = nullptr;
      blocks.push_back(block.block);
    }
  }
  bufferManager_->SpillBlocks(
      std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
          blocks.data(), blocks.size()));
  segment.meta.state = SegmentState::kFinalizedFlushed;
  return segment.meta.id;
}

SegmentData& BmSegmentCollection::segmentData(SegmentId segment) {
  auto it = segments_.find(segment);
  BOLT_CHECK(it != segments_.end(), "Unknown segment {}", segment);
  return it->second;
}

const SegmentData& BmSegmentCollection::segmentData(SegmentId segment) const {
  auto it = segments_.find(segment);
  BOLT_CHECK(it != segments_.end(), "Unknown segment {}", segment);
  return it->second;
}

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
  zeroUnusedHeapTail(chunk);
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
    zeroUnusedHeapTail(chunk);
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

ChunkData& BmSegmentCollection::chunkForRow(
    SegmentData& segment,
    RowNumber rowNumber) {
  return const_cast<ChunkData&>(
      std::as_const(*this).chunkForRow(segment, rowNumber));
}

const ChunkData& BmSegmentCollection::chunkForRow(
    const SegmentData& segment,
    RowNumber rowNumber) const {
  const auto rowsPerChunk = rowBlockSize_ / rowStride();
  BOLT_DCHECK_GT(rowsPerChunk, 0);
  const auto chunkIndex = rowNumber / rowsPerChunk;
  BOLT_CHECK_LT(
      chunkIndex,
      segment.chunks.size(),
      "Unknown row number {} in segment {}",
      rowNumber,
      segment.meta.id);
  const auto& chunk = segment.chunks[chunkIndex];
  BOLT_CHECK(
      rowNumber >= chunk.meta.firstRowNumber &&
          rowNumber < chunk.meta.firstRowNumber + chunk.meta.rowCount,
      "Unknown row number {} in segment {}",
      rowNumber,
      segment.meta.id);
  return chunk;
}

RowId BmSegmentCollection::rowIdForRowNumber(
    const SegmentData& segment,
    RowNumber rowNumber) const {
  const auto& chunk = chunkForRow(segment, rowNumber);
  auto remaining = rowNumber - chunk.meta.firstRowNumber;
  return {
      segment.meta.id,
      rowNumber,
      chunk.rowBlock.id,
      static_cast<RowOffset>(remaining * rowStride())};
}

void BmSegmentCollection::appendRowIdsForSegment(
    const SegmentData& segment,
    std::vector<RowId>& rows) const {
  rows.reserve(rows.size() + segment.meta.numRows);
  for (const auto& chunk : segment.chunks) {
    BOLT_CHECK(
        !chunk.consumed,
        "Cannot materialize RowIds for consumed chunk {} in segment {}",
        chunk.meta.id,
        segment.meta.id);
    RowNumber rowNumber = chunk.meta.firstRowNumber;
    RowOffset rowOffset = 0;
    const auto rowWidth = rowStride();
    for (uint32_t rowIndex = 0; rowIndex < chunk.meta.rowCount; ++rowIndex) {
      rows.push_back(
          {segment.meta.id,
           rowNumber++,
           chunk.rowBlock.id,
           rowOffset});
      rowOffset += rowWidth;
    }
  }
}

void BmSegmentCollection::appendRowPointersForSegment(
    SegmentData& segment,
    std::vector<char*>& rows,
    BulkLoadMetrics* metrics) {
  rows.reserve(rows.size() + segment.meta.numRows);
  for (const auto& chunk : segment.chunks) {
    BOLT_CHECK(
        !chunk.consumed,
        "Cannot materialize row pointers for consumed chunk {} in segment {}",
        chunk.meta.id,
        segment.meta.id);
    const auto& rowBlock = chunk.rowBlock;
    BOLT_DCHECK_NOT_NULL(rowBlock.ptr);
    if (metrics != nullptr) {
      metrics->pointerRows += chunk.meta.rowCount;
    }
    auto* row = rowBlock.ptr;
    for (uint32_t rowIndex = 0; rowIndex < chunk.meta.rowCount; ++rowIndex) {
      rows.push_back(row);
      row += rowStride();
    }
  }
}

char* BmSegmentCollection::rowPointer(const RowId& id) {
  auto& segment = segmentData(id.segmentId);
  for (auto& chunk : segment.chunks) {
    if (chunk.rowBlock.id == id.rowBlockId) {
      BOLT_CHECK(
          !chunk.consumed,
          "Cannot resolve row pointer for consumed chunk {} in segment {}",
          chunk.meta.id,
          segment.meta.id);
      BOLT_DCHECK_NOT_NULL(chunk.rowBlock.ptr);
      return chunk.rowBlock.ptr + id.rowOffset;
    }
  }
  BOLT_FAIL("Unknown row block {}", id.rowBlockId);
}

const char* BmSegmentCollection::rowPointer(const RowId& id) const {
  return const_cast<BmSegmentCollection*>(this)->rowPointer(id);
}

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

uint64_t BmSegmentCollection::segmentBytes(const SegmentData& segment) const {
  uint64_t bytes = 0;
  for (const auto& chunk : segment.chunks) {
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
