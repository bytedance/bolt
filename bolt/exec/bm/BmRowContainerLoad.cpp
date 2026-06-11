#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <chrono>

namespace bytedance::bolt::exec::bm {
namespace {

uint64_t nowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

bool isLoaded(const BlockRef& block) {
  return block.handle.valid();
}

uint64_t unloadedBytesForChunk(const ChunkData& chunk) {
  if (chunk.consumed) {
    return 0;
  }
  uint64_t bytes = isLoaded(chunk.rowBlock) ? 0 : chunk.rowBlock.size;
  for (const auto& block : chunk.heapBlocks) {
    if (!isLoaded(block)) {
      bytes += block.size;
    }
  }
  return bytes;
}

} // namespace

uint64_t BmRowContainer::unloadedBytes(
    folly::Range<const SegmentId*> segments) const {
  uint64_t bytes = 0;
  for (auto segmentId : segments) {
    const auto& segment = segments_.segmentData(segmentId);
    for (const auto& chunk : segment.chunks) {
      bytes += unloadedBytesForChunk(chunk);
    }
  }
  return bytes;
}

bool BmRowContainer::canLoadAllSegments(
    folly::Range<const SegmentId*> segments) const {
  validateSegments(segments);
  const auto bytes = unloadedBytes(segments);
  if (bytes == 0) {
    return true;
  }
  const auto reserved = bufferManager_->MaybeReserve(bytes);
  bufferManager_->ReleaseUnusedReservation();
  return reserved;
}

void BmRowContainer::ensureSegmentsLoaded(
    folly::Range<const SegmentId*> segments,
    BulkLoadMetrics* metrics) {
  validateSegments(segments);

  const auto estimateStart = metrics == nullptr ? 0 : nowNs();
  const auto bytes = unloadedBytes(segments);
  if (metrics != nullptr) {
    metrics->estimateBytesNs += nowNs() - estimateStart;
    metrics->estimatedBytes += bytes;
  }

  const auto reserveStart = metrics == nullptr ? 0 : nowNs();
  const bool reserved = bytes == 0 || bufferManager_->MaybeReserve(bytes);
  if (metrics != nullptr) {
    metrics->reserveNs += nowNs() - reserveStart;
  }
  BOLT_CHECK(reserved, "Cannot load {} bytes into BM RowContainer", bytes);

  try {
    blockLoader_.loadSegments(segments, metrics);
    bufferManager_->ReleaseUnusedReservation();
  } catch (const std::exception&) {
    bufferManager_->ReleaseUnusedReservation();
    throw;
  }
}

void BmRowContainer::ensureChunksLoaded(
    folly::Range<ChunkData* const*> chunks,
    BulkLoadMetrics* metrics) {
  const auto estimateStart = metrics == nullptr ? 0 : nowNs();
  uint64_t bytes = 0;
  for (auto* chunk : chunks) {
    BOLT_CHECK_NOT_NULL(chunk);
    bytes += unloadedBytesForChunk(*chunk);
  }
  if (metrics != nullptr) {
    metrics->estimateBytesNs += nowNs() - estimateStart;
    metrics->estimatedBytes += bytes;
  }

  const auto reserveStart = metrics == nullptr ? 0 : nowNs();
  const bool reserved = bytes == 0 || bufferManager_->MaybeReserve(bytes);
  if (metrics != nullptr) {
    metrics->reserveNs += nowNs() - reserveStart;
  }
  BOLT_CHECK(reserved, "Cannot load {} bytes into BM RowContainer", bytes);

  try {
    blockLoader_.loadChunks(chunks, metrics);
    bufferManager_->ReleaseUnusedReservation();
  } catch (const std::exception&) {
    bufferManager_->ReleaseUnusedReservation();
    throw;
  }
}

void BmRowContainer::ensureChunkLoaded(ChunkData& chunk) {
  ChunkData* chunkPtr = &chunk;
  ensureChunksLoaded({&chunkPtr, 1}, nullptr);
}

std::vector<char*> BmRowContainer::listRows(
    folly::Range<const SegmentId*> segments,
    BulkLoadMetrics* metrics) {
  ensureSegmentsLoaded(segments, metrics);

  const auto appendStart = metrics == nullptr ? 0 : nowNs();
  std::vector<char*> rows;
  for (auto segment : segments) {
    segments_.appendRowPointersForSegment(
        segments_.segmentData(segment), rows, metrics);
  }
  if (metrics != nullptr) {
    metrics->appendRowPointersNs += nowNs() - appendStart;
  }
  return rows;
}

std::vector<RowId> BmRowContainer::listRowIds(
    folly::Range<const SegmentId*> segments) const {
  validateSegments(segments);
  std::vector<RowId> rowIds;
  for (auto segment : segments) {
    segments_.appendRowIdsForSegment(segments_.segmentData(segment), rowIds);
  }
  return rowIds;
}

WindowReadSession BmRowContainer::beginWindowReadSegments(
    folly::Range<const SegmentId*> segments) {
  std::vector<SegmentId> segmentIds(segments.begin(), segments.end());
  validateSegments({segmentIds.data(), segmentIds.size()});
  return WindowReadSession(this, std::move(segmentIds));
}

} // namespace bytedance::bolt::exec::bm
