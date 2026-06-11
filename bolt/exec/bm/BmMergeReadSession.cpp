#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <limits>
#include <utility>

namespace bytedance::bolt::exec::bm {

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
    cursor.currentRow += container_->segments_.rowStride();
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
