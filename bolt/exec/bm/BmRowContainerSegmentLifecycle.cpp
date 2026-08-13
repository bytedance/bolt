#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <cstring>
#include <span>
#include <vector>

namespace bytedance::bolt::exec::bm {
namespace {

void collectReadOnlyEvictBlock(
    BlockRef& block,
    uint64_t& selectedBytes,
    const std::shared_ptr<memory::bm::BufferManager>& bufferManager,
    std::vector<std::shared_ptr<memory::bm::BlockHandle>>& discardBlocks,
    std::vector<std::shared_ptr<memory::bm::BlockHandle>>& spillBlocks) {
  if (!block.handle.valid()) {
    return;
  }
  BOLT_CHECK_NOT_NULL(block.block);
  auto blockHandle = block.block;
  block.handle = memory::bm::BufferHandle{};
  block.ptr = nullptr;

  if (bufferManager->HasSpillBacking(blockHandle) &&
      !bufferManager->IsDirty(blockHandle)) {
    discardBlocks.push_back(std::move(blockHandle));
  } else {
    spillBlocks.push_back(std::move(blockHandle));
  }
  selectedBytes += block.size;
}

void collectReadOnlyEvictChunkBlocks(
    ChunkData& chunk,
    uint64_t& selectedBytes,
    const std::shared_ptr<memory::bm::BufferManager>& bufferManager,
    std::vector<std::shared_ptr<memory::bm::BlockHandle>>& discardBlocks,
    std::vector<std::shared_ptr<memory::bm::BlockHandle>>& spillBlocks) {
  discardBlocks.reserve(discardBlocks.size() + 1 + chunk.heapBlocks.size());
  spillBlocks.reserve(spillBlocks.size() + 1 + chunk.heapBlocks.size());
  // Keep ReadOnlyWindow eviction chunk-granular. Rebased StringView pointers
  // make the row block dirty, so dirty row blocks are written back with heapBases
  // tracking the same heap address space. Clean blocks with existing backing can
  // still be discarded.
  collectReadOnlyEvictBlock(
      chunk.rowBlock, selectedBytes, bufferManager, discardBlocks, spillBlocks);
  for (auto& block : chunk.heapBlocks) {
    collectReadOnlyEvictBlock(
        block, selectedBytes, bufferManager, discardBlocks, spillBlocks);
  }
}

} // namespace

SegmentId BmRowContainer::spillActiveSegment() {
  return segments_.spillActiveSegment();
}

SegmentId BmRowContainer::spillActivePartitionSegment(PartitionId partition) {
  return segments_.spillActivePartitionSegment(partition);
}

void BmRowContainer::releaseSegment(SegmentId segment) {
  segments_.releaseSegment(segment);
}

void BmRowContainer::releaseSegments(folly::Range<const SegmentId*> segments) {
  for (auto segment : segments) {
    releaseSegment(segment);
  }
}

void BmRowContainer::releaseChunk(SegmentId segment, ChunkId chunk) {
  auto& segmentData = segments_.segmentData(segment);
  BOLT_CHECK_LT(chunk, segmentData.chunks.size());
  segments_.releaseChunkBlocks(*segmentData.chunks[chunk]);
}

void BmRowContainer::popFrontRows(uint64_t rowCount) {
  segments_.popFrontRows(rowCount);
}

uint64_t BmRowContainer::evictReadOnlyLoadedChunks(
    folly::Range<const std::pair<SegmentId, ChunkId>*> chunks,
    uint64_t targetBytes) {
  if (chunks.empty() || targetBytes == 0) {
    return 0;
  }

  uint64_t selectedBytes = 0;
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> discardBlocks;
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> spillBlocks;

  for (const auto& [segment, chunk] : chunks) {
    if (selectedBytes >= targetBytes) {
      break;
    }
    auto& segmentData = segments_.segmentData(segment);
    BOLT_CHECK(
        segmentData.meta.state != SegmentState::kActiveResident,
        "Cannot evict loaded blocks from active segment {}",
        segment);
    BOLT_CHECK_LT(chunk, segmentData.chunks.size());
    auto& chunkData = *segmentData.chunks[chunk];
    BOLT_CHECK(
        !chunkData.consumed,
        "Cannot evict consumed chunk {} in segment {}",
        chunk,
        segment);

    collectReadOnlyEvictChunkBlocks(
        chunkData,
        selectedBytes,
        bufferManager_,
        discardBlocks,
        spillBlocks);
  }

  uint64_t reclaimed = 0;
  if (!discardBlocks.empty()) {
    reclaimed += bufferManager_->DiscardCleanResidentBlocks(
        std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
            discardBlocks.data(), discardBlocks.size()));
  }
  if (!spillBlocks.empty()) {
    for (const auto& block : spillBlocks) {
      reclaimed += block->size();
    }
    bufferManager_->SpillBlocks(
        std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
            spillBlocks.data(), spillBlocks.size()));
  }
  return reclaimed;
}

SegmentState BmRowContainer::segmentState(SegmentId segment) const {
  return segments_.segmentState(segment);
}

const std::vector<SegmentId>& BmRowContainer::segmentsForPartition(
    PartitionId partition) const {
  return segments_.segmentsForPartition(partition);
}

SegmentId BmRowContainer::activeSegmentId(PartitionId partition) const {
  return segments_.activeSegmentId(partition);
}

RowNumber BmRowContainer::activeSegmentNextRowNumber(
    PartitionId partition) const {
  return segments_.activeSegmentNextRowNumber(partition);
}

uint32_t BmRowContainer::rowSize() const {
  return layout_.rowSize();
}

void BmRowContainer::copyRowWithDeepColumns(
    const char* row,
    folly::Range<const int32_t*> columns,
    std::vector<char>& rowCopy,
    std::vector<char>& variableCopy) const {
  BOLT_CHECK_NOT_NULL(row);
  rowCopy.resize(layout_.rowSize());
  std::memcpy(rowCopy.data(), row, rowCopy.size());

  uint64_t variableBytes = 0;
  for (auto columnIndex : columns) {
    const auto& column = layout_.column(columnIndex);
    const auto kind = column.type->kind();
    if (kind != TypeKind::VARCHAR && kind != TypeKind::VARBINARY) {
      continue;
    }
    if (layout_.isNull(row, columnIndex)) {
      continue;
    }
    const auto& value =
        *reinterpret_cast<const StringView*>(row + column.offset);
    if (!value.isInline()) {
      variableBytes += value.size();
    }
  }

  variableCopy.resize(variableBytes);
  auto* variable = variableCopy.data();
  for (auto columnIndex : columns) {
    const auto& column = layout_.column(columnIndex);
    const auto kind = column.type->kind();
    if (kind != TypeKind::VARCHAR && kind != TypeKind::VARBINARY) {
      continue;
    }
    if (layout_.isNull(row, columnIndex)) {
      continue;
    }
    auto* target =
        reinterpret_cast<StringView*>(rowCopy.data() + column.offset);
    if (target->isInline()) {
      continue;
    }
    const auto size = target->size();
    std::memcpy(variable, target->data(), size);
    *target = StringView(variable, size);
    variable += size;
  }
}

int64_t BmRowContainer::numRows() const {
  return segments_.numRows();
}

} // namespace bytedance::bolt::exec::bm
