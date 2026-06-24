#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::exec::bm {
namespace {

bool isPinnedLoaded(const BlockRef& block) {
  return block.handle.valid();
}

uint64_t unloadedBytesForChunk(const ChunkData& chunk) {
  if (chunk.consumed) {
    return 0;
  }
  uint64_t bytes = isPinnedLoaded(chunk.rowBlock) ? 0 : chunk.rowBlock.size;
  for (const auto& block : chunk.heapBlocks) {
    if (!isPinnedLoaded(block)) {
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
    for (const auto& chunkPtr : segment.chunks) {
      const auto& chunk = *chunkPtr;
      bytes += unloadedBytesForChunk(chunk);
    }
  }
  return bytes;
}

bool BmRowContainer::canBulkRead(
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
    folly::Range<const SegmentId*> segments) {
  validateSegments(segments);

  const auto bytes = unloadedBytes(segments);
  const bool reserved = bytes == 0 || bufferManager_->MaybeReserve(bytes);
  BOLT_CHECK(reserved, "Cannot load {} bytes into BM RowContainer", bytes);

  try {
    blockLoader_.loadSegments(segments);
    bufferManager_->ReleaseUnusedReservation();
  } catch (const std::exception&) {
    bufferManager_->ReleaseUnusedReservation();
    throw;
  }
}

void BmRowContainer::ensureChunksLoaded(
    folly::Range<ChunkData* const*> chunks) {
  uint64_t bytes = 0;
  for (auto* chunk : chunks) {
    BOLT_CHECK_NOT_NULL(chunk);
    bytes += unloadedBytesForChunk(*chunk);
  }

  const bool reserved = bytes == 0 || bufferManager_->MaybeReserve(bytes);
  BOLT_CHECK(reserved, "Cannot load {} bytes into BM RowContainer", bytes);

  try {
    blockLoader_.loadChunks(chunks);
    bufferManager_->ReleaseUnusedReservation();
  } catch (const std::exception&) {
    bufferManager_->ReleaseUnusedReservation();
    throw;
  }
}

void BmRowContainer::ensureChunkLoaded(ChunkData& chunk) {
  ChunkData* chunkPtr = &chunk;
  ensureChunksLoaded({&chunkPtr, 1});
}

std::vector<char*> BmRowContainer::loadAllRows(
    folly::Range<const SegmentId*> segments) {
  ensureSegmentsLoaded(segments);

  std::vector<char*> rows;
  for (auto segment : segments) {
    segments_.appendRowPointersForSegment(
        segments_.segmentData(segment), rows);
  }
  return rows;
}

std::vector<RowId> BmRowContainer::listRowIdsForSegments(
    folly::Range<const SegmentId*> segments) const {
  validateSegments(segments);
  std::vector<RowId> rowIds;
  for (auto segment : segments) {
    segments_.appendRowIdsForSegment(segments_.segmentData(segment), rowIds);
  }
  return rowIds;
}

BulkReadSession BmRowContainer::beginBulkReadSegments(
    folly::Range<const SegmentId*> segments) {
  std::vector<SegmentId> segmentIds(segments.begin(), segments.end());
  validateSegments({segmentIds.data(), segmentIds.size()});
  return BulkReadSession(this, std::move(segmentIds));
}

ReadOnlyWindowReadSession BmRowContainer::beginReadOnlyWindowReadSegments(
    folly::Range<const SegmentId*> segments) {
  std::vector<SegmentId> segmentIds(segments.begin(), segments.end());
  validateSegments({segmentIds.data(), segmentIds.size()});
  return ReadOnlyWindowReadSession(this, std::move(segmentIds));
}

} // namespace bytedance::bolt::exec::bm
