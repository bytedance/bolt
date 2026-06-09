#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <span>

namespace bytedance::bolt::exec::bm {
namespace {

uint64_t nowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

struct HeapRebaseRange {
  HeapBaseRef* heapBase{nullptr};
  uintptr_t oldBase{0};
  uintptr_t newBase{0};
  uintptr_t end{0};
};

} // namespace

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

void BmRowContainer::recordHeapForPart(
    SegmentData& segment,
    ChunkId chunkId,
    PartId partId,
    const BlockRef& heap,
    const char* row) {
  BOLT_CHECK(chunkId < segment.chunks.size());
  BOLT_CHECK(partId < segment.parts.size());
  auto& chunk = segment.chunks[chunkId];
  auto& part = segment.parts[partId];
  BOLT_CHECK_EQ(part.chunkId, chunk.id);
  BOLT_CHECK(
      std::find(chunk.parts.begin(), chunk.parts.end(), partId) !=
      chunk.parts.end());

  const auto rowAddress = reinterpret_cast<uintptr_t>(row);
  const auto& block = blockRef(segment, part.rowBlockId, true);
  const auto blockBegin = reinterpret_cast<uintptr_t>(block.ptr);
  const auto rowOffset = static_cast<uint32_t>(rowAddress - blockBegin);
  BOLT_CHECK_GE(rowOffset, part.rowBlockOffset);
  BOLT_CHECK_LT(rowOffset, part.rowBlockOffset + part.rowCount * rowStride());

  if (std::find(chunk.heapBlocks.begin(), chunk.heapBlocks.end(), heap.id) ==
      chunk.heapBlocks.end()) {
    chunk.heapBlocks.push_back(heap.id);
  }

  // See recordHeapForCurrentPart(): parts are row-block ranges, while heap
  // bases are the referenced variable-width storage for pointer rebasing.
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
    folly::Range<const SegmentId*> segments,
    BulkLoadMetrics* metrics) {
  const auto collectStart = metrics == nullptr ? 0 : nowNs();
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  std::vector<BlockRef*> blockRefs;
  std::vector<bool> isHeapBlock;
  for (const auto segmentId : segments) {
    auto& segment = segmentData(segmentId);
    const auto blockCount = segment.rowBlocks.size() + segment.heapBlocks.size();
    blocks.reserve(blocks.size() + blockCount);
    blockRefs.reserve(blockRefs.size() + blockCount);
    isHeapBlock.reserve(isHeapBlock.size() + blockCount);
    for (auto& block : segment.rowBlocks) {
      blocks.push_back(block.block);
      blockRefs.push_back(&block);
      isHeapBlock.push_back(false);
    }
    for (auto& block : segment.heapBlocks) {
      blocks.push_back(block.block);
      blockRefs.push_back(&block);
      isHeapBlock.push_back(true);
    }
  }
  if (metrics != nullptr) {
    metrics->collectBlocksNs += nowNs() - collectStart;
    metrics->pinnedBlocks += blocks.size();
  }

  const auto batchPinStart = metrics == nullptr ? 0 : nowNs();
  auto pins = bufferManager_->BatchPin(
      std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
          blocks.data(), blocks.size()));
  if (metrics != nullptr) {
    metrics->batchPinNs += nowNs() - batchPinStart;
  }
  BOLT_CHECK_EQ(pins.size(), blockRefs.size());

  const auto updateStart = metrics == nullptr ? 0 : nowNs();
  std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>> heapRebases;
  for (size_t i = 0; i < pins.size(); ++i) {
    auto& block = *blockRefs[i];
    auto* const newPtr = pins[i].Ptr();
    if (isHeapBlock[i]) {
      // Heap payloads can be read back at a different address. Keep the old
      // and new bases so StringView values stored in row blocks can be rebased.
      const auto oldBase = reinterpret_cast<uintptr_t>(block.ptr);
      const auto newBase = reinterpret_cast<uintptr_t>(newPtr);
      if (oldBase != 0 && oldBase != newBase) {
        heapRebases[block.id] = {oldBase, newBase};
      }
    }
    block.ptr = newPtr;
  }
  if (metrics != nullptr) {
    metrics->updateBlockPointersNs += nowNs() - updateStart;
  }
  if (!heapRebases.empty()) {
    for (const auto segmentId : segments) {
      rebaseStringViews(segmentData(segmentId), heapRebases, metrics);
    }
  }
  return pins;
}

std::vector<memory::bm::BufferHandle> BmRowContainer::pinChunk(
    SegmentData& segment,
    const DataChunkMeta& chunk) {
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  std::vector<BlockRef*> blockRefs;
  std::vector<bool> isHeapBlock;
  blocks.reserve(chunk.rowBlocks.size() + chunk.heapBlocks.size());
  blockRefs.reserve(chunk.rowBlocks.size() + chunk.heapBlocks.size());
  isHeapBlock.reserve(chunk.rowBlocks.size() + chunk.heapBlocks.size());
  for (auto blockId : chunk.rowBlocks) {
    auto& block = blockRef(segment, blockId, true);
    blocks.push_back(block.block);
    blockRefs.push_back(&block);
    isHeapBlock.push_back(false);
  }
  for (auto blockId : chunk.heapBlocks) {
    auto& block = blockRef(segment, blockId, false);
    blocks.push_back(block.block);
    blockRefs.push_back(&block);
    isHeapBlock.push_back(true);
  }

  auto pins = bufferManager_->BatchPin(
      std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
          blocks.data(), blocks.size()));
  BOLT_CHECK_EQ(pins.size(), blockRefs.size());

  std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>> heapRebases;
  for (size_t i = 0; i < pins.size(); ++i) {
    auto& block = *blockRefs[i];
    auto* const newPtr = pins[i].Ptr();
    if (isHeapBlock[i]) {
      // Window reads pin only the chunk blocks, but heap StringViews in the
      // chunk still need the same old-base to new-base pointer rebasing.
      const auto oldBase = reinterpret_cast<uintptr_t>(block.ptr);
      const auto newBase = reinterpret_cast<uintptr_t>(newPtr);
      if (oldBase != 0 && oldBase != newBase) {
        heapRebases[block.id] = {oldBase, newBase};
      }
    }
    block.ptr = newPtr;
  }
  if (!heapRebases.empty()) {
    rebaseChunk(segment, chunk, heapRebases, nullptr);
  }
  return pins;
}

void BmRowContainer::rebaseStringViews(
    SegmentData& segment,
    const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
        heapRebases,
    BulkLoadMetrics* metrics) {
  const auto rebaseStart = metrics == nullptr ? 0 : nowNs();
  for (const auto& chunk : segment.chunks) {
    rebaseChunk(segment, chunk, heapRebases, metrics);
  }
  if (metrics != nullptr) {
    metrics->rebaseStringViewsNs += nowNs() - rebaseStart;
  }
}

void BmRowContainer::rebaseChunk(
    SegmentData& segment,
    const DataChunkMeta& chunk,
    const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
        heapRebases,
    BulkLoadMetrics* metrics) {
  if (stringColumns_.empty()) {
    return;
  }
  for (auto partId : chunk.parts) {
    auto& part = segment.parts[partId];
    std::vector<HeapRebaseRange> ranges;
    ranges.reserve(part.heapBases.size());
    for (auto& heapBase : part.heapBases) {
      const auto rebase = heapRebases.find(heapBase.heapBlockId);
      if (rebase == heapRebases.end()) {
        continue;
      }
      const auto oldBase = heapBase.baseAddress;
      if (oldBase == 0) {
        heapBase.baseAddress = rebase->second.second;
        continue;
      }
      ranges.push_back(
          {&heapBase,
           oldBase,
           rebase->second.second,
           oldBase + heapBase.capacity});
    }
    if (ranges.empty()) {
      continue;
    }

    auto& rowBlock = blockRef(segment, part.rowBlockId, true);
    BOLT_CHECK_NOT_NULL(rowBlock.ptr);

    if (ranges.size() == 1) {
      const auto range = ranges[0];
      for (uint32_t rowIndex = 0; rowIndex < part.rowCount; ++rowIndex) {
        auto* row = rowBlock.ptr + part.rowBlockOffset + rowIndex * rowStride();
        for (const auto& column : stringColumns_) {
          if (isNull(row, column)) {
            continue;
          }
          auto* value =
              reinterpret_cast<StringView*>(row + column.offset);
          if (value->isInline()) {
            continue;
          }
          const auto oldAddress = reinterpret_cast<uintptr_t>(value->data());
          if (oldAddress >= range.oldBase && oldAddress < range.end) {
            *value = StringView(
                reinterpret_cast<const char*>(
                    range.newBase + oldAddress - range.oldBase),
                value->size());
            if (metrics != nullptr) {
              ++metrics->rebasedStringViews;
            }
          }
        }
      }
    } else {
      // Multi-heap parts are expected to be uncommon and small because parts
      // are still bounded by row-block continuity. Keep the lookup as a linear
      // scan for now: it is cache-friendly for small vectors and avoids per-part
      // index construction. If metrics show large heapBases vectors in real
      // workloads, add a last-hit cache or sort ranges by oldBase and use
      // upper_bound for interval lookup.
      for (uint32_t rowIndex = 0; rowIndex < part.rowCount; ++rowIndex) {
        auto* row = rowBlock.ptr + part.rowBlockOffset + rowIndex * rowStride();
        for (const auto& column : stringColumns_) {
          if (isNull(row, column)) {
            continue;
          }
          auto* value =
              reinterpret_cast<StringView*>(row + column.offset);
          if (value->isInline()) {
            continue;
          }
          const auto oldAddress = reinterpret_cast<uintptr_t>(value->data());
          for (const auto& range : ranges) {
            if (oldAddress >= range.oldBase && oldAddress < range.end) {
              *value = StringView(
                  reinterpret_cast<const char*>(
                      range.newBase + oldAddress - range.oldBase),
                  value->size());
              if (metrics != nullptr) {
                ++metrics->rebasedStringViews;
              }
              break;
            }
          }
        }
      }
    }

    for (auto& range : ranges) {
      range.heapBase->baseAddress = range.newBase;
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
