#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <chrono>
#include <limits>
#include <utility>

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

WindowReadSession::WindowReadSession(
    BmRowContainer* container,
    std::vector<SegmentId> segments)
    : container_(container),
      segmentOrder_(std::move(segments)),
      segments_(segmentOrder_.begin(), segmentOrder_.end()) {}

std::vector<char*> WindowReadSession::loadRows(
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
    const auto key =
        (static_cast<uint64_t>(row.segmentId) << 32) | chunk.meta.id;
    if (seenChunks.insert(key).second) {
      chunks.push_back(&chunk);
    }
  }

  container_->ensureChunksLoaded({chunks.data(), chunks.size()});

  std::vector<char*> result;
  result.reserve(rows.size());
  for (const auto& row : rows) {
    result.push_back(container_->segments_.rowPointer(row));
  }
  return result;
}

char* WindowReadSession::loadRow(const RowId& row) {
  auto rows = loadRows({&row, 1});
  BOLT_DCHECK_EQ(1, rows.size());
  return rows[0];
}

MergeReadSession::MergeReadSession(
    BmRowContainer* container,
    std::vector<SegmentId> segments,
    bool releaseAfterRead)
    : container_(container),
      segments_(segments.begin(), segments.end()),
      releaseAfterRead_(releaseAfterRead) {
  cursors_.reserve(segments.size());
  for (auto segment : segments) {
    cursors_.push_back({segment, 0, 0, nullptr});
    loadCursor(cursors_.back());
  }
}

MergeReadSession::~MergeReadSession() {
  if (container_ != nullptr) {
    releasePendingChunks();
  }
}

MergeReadSession::MergeReadSession(MergeReadSession&& other) noexcept
    : container_(other.container_),
      cursors_(std::move(other.cursors_)),
      segments_(std::move(other.segments_)),
      releaseAfterRead_(other.releaseAfterRead_),
      pendingRelease_(std::move(other.pendingRelease_)) {
  other.container_ = nullptr;
}

MergeReadSession& MergeReadSession::operator=(
    MergeReadSession&& other) noexcept {
  if (this != &other) {
    if (container_ != nullptr) {
      releasePendingChunks();
    }
    container_ = other.container_;
    cursors_ = std::move(other.cursors_);
    segments_ = std::move(other.segments_);
    releaseAfterRead_ = other.releaseAfterRead_;
    pendingRelease_ = std::move(other.pendingRelease_);
    other.container_ = nullptr;
  }
  return *this;
}

void MergeReadSession::releasePendingChunks() {
  if (!releaseAfterRead_) {
    pendingRelease_.clear();
    return;
  }
  BOLT_CHECK_NOT_NULL(container_);
  for (const auto& [segment, chunk] : pendingRelease_) {
    container_->releaseChunk(segment, chunk);
  }
  pendingRelease_.clear();
}

bool MergeReadSession::loadCursor(Cursor& cursor) {
  BOLT_CHECK_NOT_NULL(container_);
  cursor.currentRow = nullptr;
  auto& segment = container_->segments_.segmentData(cursor.segment);
  while (cursor.chunkIndex < segment.chunks.size()) {
    auto& chunk = segment.chunks[cursor.chunkIndex];
    if (chunk.meta.rowCount == 0) {
      ++cursor.chunkIndex;
      continue;
    }
    BOLT_CHECK(
        !chunk.consumed,
        "Cannot read consumed chunk {} in segment {}",
        chunk.meta.id,
        segment.meta.id);
    container_->ensureChunkLoaded(chunk);
    cursor.rowIndexInChunk = 0;
    cursor.currentRow = chunk.rowBlock.ptr;
    return true;
  }
  return false;
}

void MergeReadSession::advanceCursor(Cursor& cursor) {
  auto& segment = container_->segments_.segmentData(cursor.segment);
  auto& chunk = segment.chunks[cursor.chunkIndex];
  ++cursor.rowIndexInChunk;
  if (cursor.rowIndexInChunk < chunk.meta.rowCount) {
    cursor.currentRow =
        chunk.rowBlock.ptr +
        cursor.rowIndexInChunk * container_->segments_.rowStride();
    return;
  }

  pendingRelease_.push_back({segment.meta.id, chunk.meta.id});
  ++cursor.chunkIndex;
  cursor.rowIndexInChunk = 0;
  loadCursor(cursor);
}

int32_t MergeReadSession::compareCursors(
    const Cursor& left,
    const Cursor& right) const {
  BOLT_DCHECK_NOT_NULL(container_);
  BOLT_DCHECK_NOT_NULL(left.currentRow);
  BOLT_DCHECK_NOT_NULL(right.currentRow);
  const auto result =
      container_->compareRows(left.currentRow, right.currentRow, {});
  if (result != 0) {
    return result;
  }
  if (left.segment != right.segment) {
    return left.segment < right.segment ? -1 : 1;
  }
  return left.chunkIndex < right.chunkIndex ? -1 :
      (left.chunkIndex > right.chunkIndex ? 1 : 0);
}

size_t MergeReadSession::bestCursor() const {
  size_t best = std::numeric_limits<size_t>::max();
  for (size_t i = 0; i < cursors_.size(); ++i) {
    if (cursors_[i].currentRow == nullptr) {
      continue;
    }
    if (best == std::numeric_limits<size_t>::max() ||
        compareCursors(cursors_[i], cursors_[best]) < 0) {
      best = i;
    }
  }
  return best;
}

bool MergeReadSession::next(std::vector<char*>& rows, vector_size_t maxRows) {
  BOLT_CHECK_GT(maxRows, 0);
  releasePendingChunks();
  rows.clear();
  rows.reserve(maxRows);

  while (rows.size() < static_cast<size_t>(maxRows)) {
    const auto best = bestCursor();
    if (best == std::numeric_limits<size_t>::max()) {
      break;
    }
    rows.push_back(cursors_[best].currentRow);
    advanceCursor(cursors_[best]);
  }
  return !rows.empty();
}

} // namespace bytedance::bolt::exec::bm
