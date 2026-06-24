#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <utility>

namespace bytedance::bolt::exec::bm {

namespace {

uint64_t chunkKey(SegmentId segment, ChunkId chunk) {
  return (static_cast<uint64_t>(segment) << 32) | chunk;
}

bool chunkHasPinnedBlocks(const ChunkData& chunkData) {
  if (chunkData.rowBlock.handle.valid()) {
    return true;
  }
  for (const auto& block : chunkData.heapBlocks) {
    if (block.handle.valid()) {
      return true;
    }
  }
  return false;
}

uint64_t releaseLoadedBlock(BlockRef& block) {
  if (!block.handle.valid()) {
    return 0;
  }

  const auto bytes = block.size;
  block.handle = memory::bm::BufferHandle{};
  block.ptr = nullptr;
  return bytes;
}

uint64_t releaseLoadedChunk(ChunkData& chunkData) {
  uint64_t bytes = releaseLoadedBlock(chunkData.rowBlock);
  for (auto& block : chunkData.heapBlocks) {
    bytes += releaseLoadedBlock(block);
  }
  return bytes;
}

} // namespace

ReadOnlyWindowReadSession::ReadOnlyWindowReadSession(
    BmRowContainer* container,
    std::vector<SegmentId> segments)
    : container_(container),
      segmentOrder_(std::move(segments)),
      segments_(segmentOrder_.begin(), segmentOrder_.end()) {}

std::vector<RowId> ReadOnlyWindowReadSession::listRowIds() const {
  BOLT_CHECK_NOT_NULL(container_);
  return container_->listRowIdsForSegments(
      {segmentOrder_.data(), segmentOrder_.size()});
}

std::vector<const char*> ReadOnlyWindowReadSession::loadRows(
    folly::Range<const RowId*> rows) {
  BOLT_CHECK_NOT_NULL(container_);

  std::vector<ChunkData*> chunks;
  chunks.reserve(rows.size());
  std::unordered_set<uint64_t> seenChunks;
  for (const auto& row : rows) {
    BOLT_CHECK(
        segments_.count(row.segmentId) != 0,
        "Row segment {} is not covered by this read session",
        row.segmentId);
    auto& segment = container_->segments_.segmentData(row.segmentId);
    auto& chunk = container_->segments_.chunkForRow(segment, row.rowNumber);
    const auto key = chunkKey(row.segmentId, chunk.meta.id);
    if (seenChunks.insert(key).second) {
      chunks.push_back(&chunk);
    }
    if (loadedChunkKeys_.insert(key).second) {
      loadedChunks_.emplace_back(row.segmentId, chunk.meta.id);
    }
  }

  container_->ensureChunksLoaded({chunks.data(), chunks.size()});

  std::vector<const char*> result;
  result.reserve(rows.size());
  for (const auto& row : rows) {
    result.push_back(container_->segments_.rowPointer(row));
  }
  return result;
}

const char* ReadOnlyWindowReadSession::loadRow(const RowId& row) {
  auto rows = loadRows({&row, 1});
  BOLT_DCHECK_EQ(1, rows.size());
  return rows[0];
}

uint64_t ReadOnlyWindowReadSession::releaseLoadedChunks(uint64_t targetBytes) {
  BOLT_CHECK_NOT_NULL(container_);
  if (loadedChunks_.empty() || targetBytes == 0) {
    return 0;
  }

  uint64_t released = 0;
  for (const auto& [segment, chunk] : loadedChunks_) {
    if (released >= targetBytes) {
      break;
    }
    auto& segmentData = container_->segments_.segmentData(segment);
    BOLT_CHECK(
        segmentData.meta.state != SegmentState::kActiveResident,
        "Cannot release loaded blocks from active segment {}",
        segment);
    BOLT_CHECK_LT(chunk, segmentData.chunks.size());
    auto& chunkData = *segmentData.chunks[chunk];
    BOLT_CHECK(
        !chunkData.consumed,
        "Cannot release consumed chunk {} in segment {}",
        chunk,
        segment);
    released += releaseLoadedChunk(chunkData);
  }

  std::vector<std::pair<SegmentId, ChunkId>> stillLoaded;
  stillLoaded.reserve(loadedChunks_.size());
  loadedChunkKeys_.clear();
  for (const auto& [segment, chunk] : loadedChunks_) {
    auto& segmentData = container_->segments_.segmentData(segment);
    BOLT_CHECK_LT(chunk, segmentData.chunks.size());
    if (chunkHasPinnedBlocks(*segmentData.chunks[chunk])) {
      stillLoaded.emplace_back(segment, chunk);
      loadedChunkKeys_.insert(chunkKey(segment, chunk));
    }
  }
  loadedChunks_ = std::move(stillLoaded);
  return released;
}

uint64_t ReadOnlyWindowReadSession::evictLoadedChunks(uint64_t targetBytes) {
  BOLT_CHECK_NOT_NULL(container_);
  if (loadedChunks_.empty() || targetBytes == 0) {
    return 0;
  }

  const auto reclaimed = container_->evictReadOnlyLoadedChunks(
      {loadedChunks_.data(), loadedChunks_.size()}, targetBytes);

  std::vector<std::pair<SegmentId, ChunkId>> stillLoaded;
  stillLoaded.reserve(loadedChunks_.size());
  loadedChunkKeys_.clear();
  for (const auto& [segment, chunk] : loadedChunks_) {
    auto& segmentData = container_->segments_.segmentData(segment);
    BOLT_CHECK_LT(chunk, segmentData.chunks.size());
    if (chunkHasPinnedBlocks(*segmentData.chunks[chunk])) {
      stillLoaded.emplace_back(segment, chunk);
      loadedChunkKeys_.insert(chunkKey(segment, chunk));
    }
  }
  loadedChunks_ = std::move(stillLoaded);
  return reclaimed;
}

} // namespace bytedance::bolt::exec::bm
