#include "bolt/exec/bm/BmRowStorage.h"

#include "bolt/common/base/Exceptions.h"

#include <algorithm>
#include <cstring>
#include <span>

namespace bytedance::bolt::exec::bm {

const std::vector<SegmentId> BmRowStorage::kEmptySegments_{};

BmRowStorage::BmRowStorage(
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    memory::bm::MemoryTag tag,
    const BmRowLayout* layout,
    uint32_t rowBlockSize,
    uint32_t heapBlockSize,
    uint32_t chunkRowCount)
    : bufferManager_(std::move(bufferManager)),
      tag_(tag),
      layout_(layout),
      rowBlockSize_(rowBlockSize),
      heapBlockSize_(heapBlockSize),
      chunkRowCount_(chunkRowCount) {
  BOLT_CHECK_NOT_NULL(bufferManager_);
  BOLT_CHECK_NOT_NULL(layout_);
}

SegmentId BmRowStorage::flushActiveSegment() {
  return flushActivePartitionSegment(kDefaultPartition);
}

SegmentId BmRowStorage::flushActivePartitionSegment(PartitionId partition) {
  return finalizeAndFlush(partition);
}

void BmRowStorage::releaseSegment(
    SegmentId segment,
    ReleaseReason /*reason*/) {
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
  for (const auto& block : it->second.rowBlocks) {
    blockIndex_.erase(block.id);
  }
  for (const auto& block : it->second.heapBlocks) {
    blockIndex_.erase(block.id);
  }
  segments_.erase(it);
}

void BmRowStorage::releaseSegments(
    folly::Range<const SegmentId*> segments,
    ReleaseReason reason) {
  for (auto segment : segments) {
    releaseSegment(segment, reason);
  }
}

SegmentState BmRowStorage::segmentState(SegmentId segment) const {
  return segmentData(segment).meta.state;
}

const std::vector<SegmentId>& BmRowStorage::segmentsForPartition(
    PartitionId partition) const {
  auto it = partitionSegments_.find(partition);
  if (it == partitionSegments_.end()) {
    return kEmptySegments_;
  }
  return it->second;
}

int64_t BmRowStorage::numRows() const {
  int64_t rows = 0;
  for (const auto& [_, segment] : segments_) {
    rows += segment.meta.numRows;
  }
  return rows;
}

SegmentData& BmRowStorage::activeSegment(PartitionId partition) {
  auto it = activeSegments_.find(partition);
  if (it != activeSegments_.end()) {
    return segmentData(it->second);
  }

  auto& segment = createSegment(partition);
  activeSegments_[partition] = segment.meta.id;
  return segment;
}

SegmentData& BmRowStorage::createSegment(
    std::optional<PartitionId> partition) {
  SegmentData segment;
  segment.meta.id = nextSegmentId_++;
  segment.meta.state = SegmentState::kActiveResident;
  segment.meta.partitionId = std::move(partition);
  const auto id = segment.meta.id;
  auto [inserted, _] = segments_.emplace(id, std::move(segment));
  return inserted->second;
}

SegmentId BmRowStorage::finalizeAndFlush(PartitionId partition) {
  auto active = activeSegments_.find(partition);
  BOLT_CHECK(active != activeSegments_.end());
  auto& segment = segmentData(active->second);
  const auto id = finalizeAndFlushSegment(segment);
  partitionSegments_[partition].push_back(id);
  activeSegments_.erase(active);
  return id;
}

SegmentId BmRowStorage::finalizeAndFlushSegment(SegmentData& segment) {
  BOLT_CHECK(segment.meta.state == SegmentState::kActiveResident);
  segment.meta.state = SegmentState::kFinalizedResident;

  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  blocks.reserve(segment.rowBlocks.size() + segment.heapBlocks.size());
  for (auto& block : segment.rowBlocks) {
    block.handle = memory::bm::BufferHandle{};
    block.ptr = nullptr;
    blocks.push_back(block.block);
  }
  for (auto& block : segment.heapBlocks) {
    block.handle = memory::bm::BufferHandle{};
    blocks.push_back(block.block);
  }
  bufferManager_->SpillBlocks(
      std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
          blocks.data(), blocks.size()));
  segment.meta.state = SegmentState::kFinalizedFlushed;
  return segment.meta.id;
}

SegmentData& BmRowStorage::segmentData(SegmentId segment) {
  auto it = segments_.find(segment);
  BOLT_CHECK(it != segments_.end(), "Unknown segment {}", segment);
  return it->second;
}

const SegmentData& BmRowStorage::segmentData(SegmentId segment) const {
  auto it = segments_.find(segment);
  BOLT_CHECK(it != segments_.end(), "Unknown segment {}", segment);
  return it->second;
}

BlockRef& BmRowStorage::addBlock(
    SegmentData& segment,
    bool isRowBlock,
    uint32_t blockSize) {
  auto handle = bufferManager_->Allocate(blockSize, tag_);
  BlockRef block;
  block.id = nextBlockId_++;
  block.size = blockSize;
  block.ptr = handle.Ptr();
  block.block = handle.block();
  block.handle = std::move(handle);

  auto& blocks = isRowBlock ? segment.rowBlocks : segment.heapBlocks;
  auto& metaBlocks =
      isRowBlock ? segment.meta.rowBlocks : segment.meta.heapBlocks;
  metaBlocks.push_back(block.id);
  blockIndex_[block.id] = {segment.meta.id, isRowBlock};
  blocks.push_back(std::move(block));
  return blocks.back();
}

BlockRef& BmRowStorage::ensureRowBlockSlow(SegmentData& segment) {
  return addBlock(segment, true, rowBlockSize_);
}

BlockRef& BmRowStorage::ensureHeapBlockSlow(
    SegmentData& segment,
    uint32_t minBytes) {
  const auto blockSize = std::max(heapBlockSize_, minBytes);
  return addBlock(segment, false, blockSize);
}

BlockRef& BmRowStorage::blockRef(
    SegmentData& segment,
    BlockId id,
    bool isRowBlock) {
  auto& blocks = isRowBlock ? segment.rowBlocks : segment.heapBlocks;
  for (auto& block : blocks) {
    if (block.id == id) {
      return block;
    }
  }
  BOLT_FAIL("Unknown {} block {}", isRowBlock ? "row" : "heap", id);
}

char* BmRowStorage::newRowInSegment(SegmentData& segment) {
  BOLT_CHECK(segment.meta.state == SegmentState::kActiveResident);
  auto& block = ensureRowBlock(segment);
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

void BmRowStorage::updateChunkForRow(
    SegmentData& segment,
    const RowId& rowId) {
  if (segment.currentChunk == kNoBlock ||
      segment.currentChunkRowCount >= chunkRowCount_) {
    DataChunkMeta chunk;
    chunk.id = segment.chunks.size();
    chunk.segmentId = segment.meta.id;
    chunk.firstRowNumber = rowId.rowNumber;
    chunk.rowCount = 0;
    segment.currentChunk = chunk.id;
    segment.currentChunkRowCount = 0;
    segment.meta.chunks.push_back(chunk.id);
    segment.chunks.push_back(std::move(chunk));

    ChunkPartMeta part;
    part.id = segment.parts.size();
    part.chunkId = segment.currentChunk;
    part.rowBlockId = rowId.rowBlockId;
    part.rowBlockOffset = rowId.rowOffset;
    part.rowCount = 0;
    segment.currentPart = part.id;
    segment.parts.push_back(std::move(part));
    segment.chunks.back().parts.push_back(segment.currentPart);
  }

  auto& chunk = segment.chunks[segment.currentChunk];
  ++chunk.rowCount;
  ++segment.currentChunkRowCount;
  if (std::find(
          chunk.rowBlocks.begin(), chunk.rowBlocks.end(), rowId.rowBlockId) ==
      chunk.rowBlocks.end()) {
    chunk.rowBlocks.push_back(rowId.rowBlockId);
  }

  auto& part = segment.parts[segment.currentPart];
  if (part.rowBlockId != rowId.rowBlockId) {
    ChunkPartMeta newPart;
    newPart.id = segment.parts.size();
    newPart.chunkId = segment.currentChunk;
    newPart.rowBlockId = rowId.rowBlockId;
    newPart.rowBlockOffset = rowId.rowOffset;
    segment.currentPart = newPart.id;
    segment.parts.push_back(std::move(newPart));
    chunk.parts.push_back(segment.currentPart);
  }
  ++segment.parts[segment.currentPart].rowCount;
}

void BmRowStorage::recordHeapForCurrentPart(
    SegmentData& segment,
    const BlockRef& heap) {
  BOLT_CHECK(segment.currentChunk != kNoBlock);
  BOLT_CHECK(segment.currentPart != kNoBlock);
  auto& chunk = segment.chunks[segment.currentChunk];
  if (std::find(chunk.heapBlocks.begin(), chunk.heapBlocks.end(), heap.id) ==
      chunk.heapBlocks.end()) {
    chunk.heapBlocks.push_back(heap.id);
  }

  // Heap block changes do not split ChunkPart. A part owns a contiguous range
  // of rows in one row block and records all heap blocks referenced by those
  // rows so rebasing can repair StringViews after pinning.
  auto& part = segment.parts[segment.currentPart];
  auto it = std::find_if(
      part.heapBases.begin(),
      part.heapBases.end(),
      [&](const HeapBaseRef& ref) { return ref.heapBlockId == heap.id; });
  if (it == part.heapBases.end()) {
    part.heapBases.push_back(
        {heap.id, reinterpret_cast<uintptr_t>(heap.ptr), heap.size});
  } else {
    it->baseAddress = reinterpret_cast<uintptr_t>(heap.ptr);
    it->capacity = heap.size;
  }
}

void BmRowStorage::recordHeapForPart(
    SegmentData& segment,
    ChunkId chunkId,
    PartId partId,
    const BlockRef& heap,
    const char* row) {
  BOLT_DCHECK_LT(chunkId, segment.chunks.size());
  BOLT_DCHECK_LT(partId, segment.parts.size());
  auto& chunk = segment.chunks[chunkId];
  auto& part = segment.parts[partId];
  BOLT_DCHECK_EQ(part.chunkId, chunk.id);
  BOLT_DCHECK(
      std::find(chunk.parts.begin(), chunk.parts.end(), partId) !=
      chunk.parts.end());

  BOLT_DCHECK([&]() {
    const auto rowAddress = reinterpret_cast<uintptr_t>(row);
    const auto& block = blockRef(segment, part.rowBlockId, true);
    const auto blockBegin = reinterpret_cast<uintptr_t>(block.ptr);
    const auto rowOffset = static_cast<uint32_t>(rowAddress - blockBegin);
    return rowOffset >= part.rowBlockOffset &&
        rowOffset < part.rowBlockOffset + part.rowCount * rowStride();
  }());

  if (chunk.heapBlocks.empty() || chunk.heapBlocks.back() != heap.id) {
    // Heap allocation is append-only inside a segment. Once a part records heap
    // block A, then B, it should never go back to A. The release fast path
    // relies on this A->B->A pattern not happening and only checks the back().
    BOLT_DCHECK(
        std::find(chunk.heapBlocks.begin(), chunk.heapBlocks.end(), heap.id) ==
            chunk.heapBlocks.end(),
        "Heap block {} is reused non-contiguously in chunk {}",
        heap.id,
        chunk.id);
    chunk.heapBlocks.push_back(heap.id);
  }

  // See recordHeapForCurrentPart(): parts are row-block ranges, while heap
  // bases are the referenced variable-width storage for pointer rebasing.
  const auto base = reinterpret_cast<uintptr_t>(heap.ptr);
  if (!part.heapBases.empty() &&
      part.heapBases.back().heapBlockId == heap.id) {
    part.heapBases.back().baseAddress = base;
    part.heapBases.back().capacity = heap.size;
    return;
  }

  // Same append-only invariant as chunk.heapBlocks: a non-back duplicate means
  // this part observed A->B->A heap usage, which would make the release fast
  // path append duplicate HeapBaseRef entries.
  BOLT_DCHECK(
      std::find_if(
          part.heapBases.begin(),
          part.heapBases.end(),
          [&](const HeapBaseRef& ref) { return ref.heapBlockId == heap.id; }) ==
          part.heapBases.end(),
      "Heap block {} is reused non-contiguously in part {}",
      heap.id,
      part.id);
  part.heapBases.push_back({heap.id, base, heap.size});
}

const DataChunkMeta& BmRowStorage::chunkForRow(
    const SegmentData& segment,
    RowNumber rowNumber) const {
  for (const auto& chunk : segment.chunks) {
    if (rowNumber >= chunk.firstRowNumber &&
        rowNumber < chunk.firstRowNumber + chunk.rowCount) {
      return chunk;
    }
  }
  BOLT_FAIL("Unknown row number {} in segment {}", rowNumber, segment.meta.id);
}

RowId BmRowStorage::rowIdForRowNumber(
    const SegmentData& segment,
    RowNumber rowNumber) const {
  const auto& chunk = chunkForRow(segment, rowNumber);
  auto remaining = rowNumber - chunk.firstRowNumber;
  for (auto partId : chunk.parts) {
    const auto& part = segment.parts[partId];
    if (remaining < part.rowCount) {
      return {
          segment.meta.id,
          rowNumber,
          part.rowBlockId,
          static_cast<RowOffset>(
              part.rowBlockOffset + remaining * rowStride()),
          kNoBlock};
    }
    remaining -= part.rowCount;
  }
  BOLT_FAIL("Unknown row number {} in segment {}", rowNumber, segment.meta.id);
}

void BmRowStorage::appendRowIdsForSegment(
    const SegmentData& segment,
    std::vector<RowId>& rows) const {
  rows.reserve(rows.size() + segment.meta.numRows);
  for (const auto& chunk : segment.chunks) {
    RowNumber rowNumber = chunk.firstRowNumber;
    for (auto partId : chunk.parts) {
      const auto& part = segment.parts[partId];
      for (uint32_t rowIndex = 0; rowIndex < part.rowCount; ++rowIndex) {
        rows.push_back(
            {segment.meta.id,
             rowNumber++,
             part.rowBlockId,
             static_cast<RowOffset>(
                 part.rowBlockOffset + rowIndex * rowStride()),
             kNoBlock});
      }
    }
  }
}

void BmRowStorage::appendRowPointersForSegment(
    SegmentData& segment,
    std::vector<char*>& rows,
    BulkLoadMetrics* metrics) {
  rows.reserve(rows.size() + segment.meta.numRows);
  for (const auto& chunk : segment.chunks) {
    for (auto partId : chunk.parts) {
      const auto& part = segment.parts[partId];
      auto& rowBlock = blockRef(segment, part.rowBlockId, true);
      BOLT_CHECK_NOT_NULL(rowBlock.ptr);
      if (metrics != nullptr) {
        metrics->pointerRows += part.rowCount;
      }
      for (uint32_t rowIndex = 0; rowIndex < part.rowCount; ++rowIndex) {
        rows.push_back(
            rowBlock.ptr + part.rowBlockOffset + rowIndex * rowStride());
      }
    }
  }
}

char* BmRowStorage::rowPointer(const RowId& id) {
  auto& segment = segmentData(id.segmentId);
  for (auto& block : segment.rowBlocks) {
    if (block.id == id.rowBlockId) {
      BOLT_CHECK_NOT_NULL(block.ptr);
      return block.ptr + id.rowOffset;
    }
  }
  BOLT_FAIL("Unknown row block {}", id.rowBlockId);
}

const char* BmRowStorage::rowPointer(const RowId& id) const {
  return const_cast<BmRowStorage*>(this)->rowPointer(id);
}

uint64_t BmRowStorage::segmentBytes(const SegmentData& segment) const {
  uint64_t bytes = 0;
  for (const auto& block : segment.rowBlocks) {
    bytes += block.size;
  }
  for (const auto& block : segment.heapBlocks) {
    bytes += block.size;
  }
  return bytes;
}

} // namespace bytedance::bolt::exec::bm
