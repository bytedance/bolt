#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <algorithm>
#include <cstring>
#include <span>

namespace bytedance::bolt::exec::bm {

SegmentId BmRowContainer::flushActiveSegment() {
  return flushActivePartitionSegment(kDefaultPartition);
}

SegmentId BmRowContainer::flushActivePartitionSegment(PartitionId partition) {
  return finalizeAndFlush(partition);
}

void BmRowContainer::releaseSegment(
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

void BmRowContainer::releaseSegments(
    folly::Range<const SegmentId*> segments,
    ReleaseReason reason) {
  for (auto segment : segments) {
    releaseSegment(segment, reason);
  }
}

SegmentState BmRowContainer::segmentState(SegmentId segment) const {
  return segmentData(segment).meta.state;
}

const std::vector<SegmentId>& BmRowContainer::segmentsForPartition(
    PartitionId partition) const {
  auto it = partitionSegments_.find(partition);
  if (it == partitionSegments_.end()) {
    return kEmptySegments_;
  }
  return it->second;
}

int64_t BmRowContainer::numRows() const {
  int64_t rows = 0;
  for (const auto& [_, segment] : segments_) {
    rows += segment.meta.numRows;
  }
  return rows;
}

BmRowContainer::SegmentData& BmRowContainer::activeSegment(
    PartitionId partition) {
  auto it = activeSegments_.find(partition);
  if (it != activeSegments_.end()) {
    return segmentData(it->second);
  }

  auto& segment = createSegment(partition);
  activeSegments_[partition] = segment.meta.id;
  return segment;
}

BmRowContainer::SegmentData& BmRowContainer::createSegment(
    std::optional<PartitionId> partition) {
  SegmentData segment;
  segment.meta.id = nextSegmentId_++;
  segment.meta.state = SegmentState::kActiveResident;
  segment.meta.partitionId = std::move(partition);
  const auto id = segment.meta.id;
  auto [inserted, _] = segments_.emplace(id, std::move(segment));
  return inserted->second;
}

SegmentId BmRowContainer::finalizeAndFlush(PartitionId partition) {
  auto active = activeSegments_.find(partition);
  BOLT_CHECK(active != activeSegments_.end());
  auto& segment = segmentData(active->second);
  const auto id = finalizeAndFlushSegment(segment);
  partitionSegments_[partition].push_back(id);
  activeSegments_.erase(active);
  return id;
}

SegmentId BmRowContainer::finalizeAndFlushSegment(SegmentData& segment) {
  BOLT_CHECK(segment.meta.state == SegmentState::kActiveResident);
  segment.meta.state = SegmentState::kFinalizedResident;

  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  blocks.reserve(segment.rowBlocks.size() + segment.heapBlocks.size());
  for (auto& block : segment.rowBlocks) {
    block.handle.Destroy();
    block.ptr = nullptr;
    blocks.push_back(block.block);
  }
  for (auto& block : segment.heapBlocks) {
    block.handle.Destroy();
    blocks.push_back(block.block);
  }
  bufferManager_->SpillBlocks(
      std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
          blocks.data(), blocks.size()));
  segment.meta.state = SegmentState::kFinalizedFlushed;
  return segment.meta.id;
}

BmRowContainer::SegmentData& BmRowContainer::segmentData(SegmentId segment) {
  auto it = segments_.find(segment);
  BOLT_CHECK(it != segments_.end(), "Unknown segment {}", segment);
  return it->second;
}

const BmRowContainer::SegmentData& BmRowContainer::segmentData(
    SegmentId segment) const {
  auto it = segments_.find(segment);
  BOLT_CHECK(it != segments_.end(), "Unknown segment {}", segment);
  return it->second;
}

BmRowContainer::BlockRef& BmRowContainer::addBlock(
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

BmRowContainer::BlockRef& BmRowContainer::ensureRowBlock(SegmentData& segment) {
  if (!segment.rowBlocks.empty() &&
      segment.rowBlocks.back().used + fixedRowSize_ <=
          segment.rowBlocks.back().size) {
    return segment.rowBlocks.back();
  }
  return addBlock(segment, true, rowBlockSize_);
}

BmRowContainer::BlockRef& BmRowContainer::ensureHeapBlock(
    SegmentData& segment,
    uint32_t minBytes) {
  if (!segment.heapBlocks.empty() &&
      segment.heapBlocks.back().used + minBytes <=
          segment.heapBlocks.back().size) {
    return segment.heapBlocks.back();
  }

  const auto blockSize = std::max(heapBlockSize_, minBytes);
  return addBlock(segment, false, blockSize);
}

BmRowContainer::BlockRef& BmRowContainer::blockRef(
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

char* BmRowContainer::newRowInSegment(SegmentData& segment) {
  BOLT_CHECK(segment.meta.state == SegmentState::kActiveResident);
  auto& block = ensureRowBlock(segment);
  const auto offset = block.used;
  auto* row = block.ptr + offset;
  block.used += fixedRowSize_;
  std::memset(row, 0, fixedRowSize_);

  RowId rowId;
  rowId.segmentId = segment.meta.id;
  rowId.rowNumber = segment.nextRowNumber++;
  rowId.rowBlockId = block.id;
  rowId.rowOffset = offset;
  ++segment.meta.numRows;
  updateChunkForRow(segment, rowId);
  return row;
}

char* BmRowContainer::copyRowToSegment(
    SegmentData& segment,
    const char* source) {
  auto* target = newRowInSegment(segment);
  std::memcpy(target, source, fixedRowSize_);

  for (int32_t column = 0; column < types_.size(); ++column) {
    const auto kind = types_[column]->kind();
    if ((kind != TypeKind::VARCHAR && kind != TypeKind::VARBINARY) ||
        isNull(target, column)) {
      continue;
    }
    auto* value = reinterpret_cast<StringView*>(valueAddress(target, column));
    if (value->isInline()) {
      continue;
    }
    auto& heap = ensureHeapBlock(segment, value->size());
    auto* stringTarget = heap.ptr + heap.used;
    std::memcpy(stringTarget, value->data(), value->size());
    heap.used += value->size();
    *value = StringView(stringTarget, value->size());
    recordHeapForCurrentPart(segment, heap);
  }
  return target;
}

void BmRowContainer::updateChunkForRow(
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

void BmRowContainer::recordHeapForCurrentPart(
    SegmentData& segment,
    const BlockRef& heap) {
  BOLT_CHECK(segment.currentChunk != kNoBlock);
  BOLT_CHECK(segment.currentPart != kNoBlock);
  auto& chunk = segment.chunks[segment.currentChunk];
  if (std::find(chunk.heapBlocks.begin(), chunk.heapBlocks.end(), heap.id) ==
      chunk.heapBlocks.end()) {
    chunk.heapBlocks.push_back(heap.id);
  }

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

const DataChunkMeta& BmRowContainer::chunkForRow(
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

BmRowContainer::SegmentData& BmRowContainer::owningActiveSegment(
    const char* row) {
  const auto address = reinterpret_cast<uintptr_t>(row);
  for (auto& [_, segment] : segments_) {
    if (segment.meta.state != SegmentState::kActiveResident) {
      continue;
    }
    for (const auto& block : segment.rowBlocks) {
      const auto begin = reinterpret_cast<uintptr_t>(block.ptr);
      const auto end = begin + block.used;
      if (begin <= address && address < end) {
        return segment;
      }
    }
  }
  BOLT_FAIL("Row pointer does not belong to an active BmRowContainer segment");
}

RowId BmRowContainer::rowIdForRowNumber(
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

void BmRowContainer::appendRowIdsForSegment(
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

void BmRowContainer::appendRowPointersForSegment(
    SegmentData& segment,
    std::vector<char*>& rows) {
  rows.reserve(rows.size() + segment.meta.numRows);
  for (const auto& chunk : segment.chunks) {
    for (auto partId : chunk.parts) {
      const auto& part = segment.parts[partId];
      auto& rowBlock = blockRef(segment, part.rowBlockId, true);
      BOLT_CHECK_NOT_NULL(rowBlock.ptr);
      for (uint32_t rowIndex = 0; rowIndex < part.rowCount; ++rowIndex) {
        rows.push_back(
            rowBlock.ptr + part.rowBlockOffset + rowIndex * rowStride());
      }
    }
  }
}

char* BmRowContainer::rowPointer(const RowId& id) {
  auto& segment = segmentData(id.segmentId);
  for (auto& block : segment.rowBlocks) {
    if (block.id == id.rowBlockId) {
      BOLT_CHECK_NOT_NULL(block.ptr);
      return block.ptr + id.rowOffset;
    }
  }
  BOLT_FAIL("Unknown row block {}", id.rowBlockId);
}

const char* BmRowContainer::rowPointer(const RowId& id) const {
  return const_cast<BmRowContainer*>(this)->rowPointer(id);
}

std::vector<memory::bm::BufferHandle> BmRowContainer::pinSegments(
    folly::Range<const SegmentId*> segments) {
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  for (const auto segmentId : segments) {
    auto& segment = segmentData(segmentId);
    blocks.reserve(
        blocks.size() + segment.rowBlocks.size() + segment.heapBlocks.size());
    for (const auto& block : segment.rowBlocks) {
      blocks.push_back(block.block);
    }
    for (const auto& block : segment.heapBlocks) {
      blocks.push_back(block.block);
    }
  }

  auto pins = bufferManager_->BatchPin(
      std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
          blocks.data(), blocks.size()));
  std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>> heapRebases;
  for (auto& pin : pins) {
    const auto blockId = pin.block()->id();
    for (auto& [_, segment] : segments_) {
      for (auto& rowBlock : segment.rowBlocks) {
        if (rowBlock.block->id() == blockId) {
          rowBlock.ptr = pin.Ptr();
        }
      }
      for (auto& heapBlock : segment.heapBlocks) {
        if (heapBlock.block->id() == blockId) {
          const auto oldBase = reinterpret_cast<uintptr_t>(heapBlock.ptr);
          const auto newBase = reinterpret_cast<uintptr_t>(pin.Ptr());
          if (oldBase != 0 && oldBase != newBase) {
            heapRebases[heapBlock.id] = {oldBase, newBase};
          }
          heapBlock.ptr = pin.Ptr();
        }
      }
    }
  }
  if (!heapRebases.empty()) {
    for (const auto segmentId : segments) {
      rebaseStringViews(segmentData(segmentId), heapRebases);
    }
  }
  return pins;
}

std::vector<memory::bm::BufferHandle> BmRowContainer::pinChunk(
    SegmentData& segment,
    const DataChunkMeta& chunk) {
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  blocks.reserve(chunk.rowBlocks.size() + chunk.heapBlocks.size());
  for (auto blockId : chunk.rowBlocks) {
    blocks.push_back(blockRef(segment, blockId, true).block);
  }
  for (auto blockId : chunk.heapBlocks) {
    blocks.push_back(blockRef(segment, blockId, false).block);
  }

  auto pins = bufferManager_->BatchPin(
      std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
          blocks.data(), blocks.size()));
  std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>> heapRebases;
  for (auto& pin : pins) {
    const auto bmBlockId = pin.block()->id();
    for (auto& rowBlock : segment.rowBlocks) {
      if (rowBlock.block->id() == bmBlockId) {
        rowBlock.ptr = pin.Ptr();
      }
    }
    for (auto& heapBlock : segment.heapBlocks) {
      if (heapBlock.block->id() == bmBlockId) {
        const auto oldBase = reinterpret_cast<uintptr_t>(heapBlock.ptr);
        const auto newBase = reinterpret_cast<uintptr_t>(pin.Ptr());
        if (oldBase != 0 && oldBase != newBase) {
          heapRebases[heapBlock.id] = {oldBase, newBase};
        }
        heapBlock.ptr = pin.Ptr();
      }
    }
  }
  if (!heapRebases.empty()) {
    rebaseChunk(segment, chunk, heapRebases);
  }
  return pins;
}

void BmRowContainer::rebaseStringViews(
    SegmentData& segment,
    const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
        heapRebases) {
  for (const auto& chunk : segment.chunks) {
    rebaseChunk(segment, chunk, heapRebases);
  }
}

void BmRowContainer::rebaseChunk(
    SegmentData& segment,
    const DataChunkMeta& chunk,
    const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
        heapRebases) {
  for (auto partId : chunk.parts) {
    auto& part = segment.parts[partId];
    auto& rowBlock = blockRef(segment, part.rowBlockId, true);
    BOLT_CHECK_NOT_NULL(rowBlock.ptr);
    for (uint32_t rowIndex = 0; rowIndex < part.rowCount; ++rowIndex) {
      auto* row = rowBlock.ptr + part.rowBlockOffset + rowIndex * rowStride();
      for (int32_t column = 0; column < types_.size(); ++column) {
        const auto kind = types_[column]->kind();
        if ((kind != TypeKind::VARCHAR && kind != TypeKind::VARBINARY) ||
            isNull(row, column)) {
          continue;
        }
        auto* value = reinterpret_cast<StringView*>(valueAddress(row, column));
        if (value->isInline()) {
          continue;
        }
        const auto oldAddress = reinterpret_cast<uintptr_t>(value->data());
        for (auto& heapBase : part.heapBases) {
          const auto rebase = heapRebases.find(heapBase.heapBlockId);
          if (rebase == heapRebases.end()) {
            continue;
          }
          const auto oldBase = heapBase.baseAddress;
          const auto newBase = rebase->second.second;
          if (oldBase == 0) {
            continue;
          }
          if (oldAddress >= oldBase &&
              oldAddress < oldBase + heapBase.capacity) {
            *value = StringView(
                reinterpret_cast<const char*>(newBase + oldAddress - oldBase),
                value->size());
            break;
          }
        }
      }
    }
    for (auto& heapBase : part.heapBases) {
      const auto rebase = heapRebases.find(heapBase.heapBlockId);
      if (rebase != heapRebases.end()) {
        heapBase.baseAddress = rebase->second.second;
      }
    }
  }
}

uint64_t BmRowContainer::segmentBytes(const SegmentData& segment) const {
  uint64_t bytes = 0;
  for (const auto& block : segment.rowBlocks) {
    bytes += block.size;
  }
  for (const auto& block : segment.heapBlocks) {
    bytes += block.size;
  }
  return bytes;
}

uint32_t BmRowContainer::rowStride() const {
  return fixedRowSize_;
}

} // namespace bytedance::bolt::exec::bm
