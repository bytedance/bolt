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

} // namespace

BulkReadSession::BulkReadSession(
    BmRowContainer* container,
    std::vector<SegmentId> segments,
    ReadSessionOptions options)
    : container_(container),
      segmentOrder_(std::move(segments)),
      segments_(segmentOrder_.begin(), segmentOrder_.end()),
      options_(std::move(options)) {}

LoadAllResult BulkReadSession::tryLoadAll(
    std::vector<char*>& rows,
    std::vector<RowId>& rowIds) {
  BOLT_CHECK_NOT_NULL(container_);
  BOLT_CHECK_NOT_NULL(container_->bufferManager_);
  rows.clear();
  rowIds.clear();
  pins_.clear();

  auto* metrics = options_.bulkLoadMetrics;
  const auto estimateStart = metrics == nullptr ? 0 : nowNs();
  uint64_t bytes = 0;
  for (auto segment : segmentOrder_) {
    bytes += container_->storage_.segmentBytes(
        container_->storage_.segmentData(segment));
  }
  if (metrics != nullptr) {
    metrics->estimateBytesNs += nowNs() - estimateStart;
    metrics->estimatedBytes += bytes;
  }

  const auto returnWindowRead = [&]() {
    const auto appendStart = metrics == nullptr ? 0 : nowNs();
    for (auto segment : segmentOrder_) {
      container_->storage_.appendRowIdsForSegment(
          container_->storage_.segmentData(segment), rowIds);
    }
    if (metrics != nullptr) {
      metrics->appendRowIdsNs += nowNs() - appendStart;
      metrics->rowIdRows += rowIds.size();
    }
    return LoadAllResult::kNeedWindowRead;
  };

  if (options_.maxPinnedBytes != 0 && bytes > options_.maxPinnedBytes) {
    return returnWindowRead();
  }

  const auto reserveStart = metrics == nullptr ? 0 : nowNs();
  const bool reserved =
      bytes == 0 || container_->bufferManager_->MaybeReserve(bytes);
  if (metrics != nullptr) {
    metrics->reserveNs += nowNs() - reserveStart;
  }
  if (!reserved) {
    return returnWindowRead();
  }

  try {
    pins_ = container_->blockLoader_.pinSegments(segmentOrder_, metrics);
    container_->bufferManager_->ReleaseUnusedReservation();
    const auto appendStart = metrics == nullptr ? 0 : nowNs();
    for (auto segment : segmentOrder_) {
      container_->storage_.appendRowPointersForSegment(
          container_->storage_.segmentData(segment), rows, metrics);
    }
    if (metrics != nullptr) {
      metrics->appendRowPointersNs += nowNs() - appendStart;
    }
    return LoadAllResult::kLoadedPointers;
  } catch (const std::exception&) {
    pins_.clear();
    container_->bufferManager_->ReleaseUnusedReservation();
    rows.clear();
    return returnWindowRead();
  }
}

RowWindow BulkReadSession::loadRows(folly::Range<const RowId*> rows) {
  BOLT_CHECK_NOT_NULL(container_);
  windowPins_.clear();
  rowViews_.clear();
  rowViews_.reserve(rows.size());

  std::unordered_set<uint64_t> pinnedChunks;
  for (const auto& row : rows) {
    BOLT_CHECK(
        segments_.count(row.segmentId) != 0,
        "Row segment {} is not covered by this read session",
        row.segmentId);
    auto& segment = container_->storage_.segmentData(row.segmentId);
    auto& chunk =
        container_->storage_.chunkForRow(segment, row.rowNumber);
    const auto key =
        (static_cast<uint64_t>(row.segmentId) << 32) | chunk.meta.id;
    if (pinnedChunks.insert(key).second) {
      auto pins = container_->blockLoader_.pinChunk(chunk);
      for (auto& pin : pins) {
        windowPins_.push_back(std::move(pin));
      }
    }
  }

  for (const auto& row : rows) {
    rowViews_.push_back({row, container_->storage_.rowPointer(row)});
  }
  return {{rowViews_.data(), rowViews_.size()}};
}

char* BulkReadSession::loadRow(const RowId& row) {
  auto window = loadRows({&row, 1});
  BOLT_DCHECK_EQ(1, window.rows.size());
  return window.rows[0].ptr;
}

SegmentCursor::SegmentCursor(
    BmRowContainer* container,
    SegmentId segment,
    bool releaseAfterRead)
    : container_(container),
      segment_(segment),
      releaseAfterRead_(releaseAfterRead) {
  loadCurrentChunk();
}

void SegmentCursor::advance() {
  if (!hasCurrent()) {
    return;
  }
  auto& segment = container_->storage_.segmentData(segment_);
  auto& chunk = segment.chunks[chunkIndex_];

  ++rowIndexInChunk_;
  if (rowIndexInChunk_ < chunk.meta.rowCount) {
    currentRow_ =
        chunk.rowBlock.ptr + rowIndexInChunk_ * container_->storage_.rowStride();
    return;
  }

  currentRow_ = nullptr;
  pins_.clear();
  if (releaseAfterRead_) {
    container_->storage_.releaseChunkBlocks(chunk);
  }

  ++chunkIndex_;
  rowIndexInChunk_ = 0;
  loadCurrentChunk();
}

void SegmentCursor::loadCurrentChunk() {
  currentRow_ = nullptr;
  pins_.clear();
  auto& segment = container_->storage_.segmentData(segment_);
  while (chunkIndex_ < segment.chunks.size()) {
    auto& chunk = segment.chunks[chunkIndex_];
    if (chunk.meta.rowCount == 0) {
      ++chunkIndex_;
      continue;
    }
    BOLT_CHECK(
        !chunk.consumed,
        "Cannot read consumed chunk {} in segment {}",
        chunk.meta.id,
        segment.meta.id);
    pins_ = container_->blockLoader_.pinChunk(chunk);
    rowIndexInChunk_ = 0;
    currentRow_ = chunk.rowBlock.ptr;
    return;
  }
}

MergeReadSession::MergeReadSession(
    BmRowContainer* container,
    std::vector<SegmentId> segments,
    bool releaseAfterRead)
    : container_(container),
      segments_(segments.begin(), segments.end()),
      releaseAfterRead_(releaseAfterRead) {}

SegmentCursor MergeReadSession::makeCursor(SegmentId segment) {
  BOLT_CHECK_NOT_NULL(container_);
  BOLT_CHECK(
      segments_.count(segment) != 0,
      "Merge read segment {} is not covered",
      segment);
  return SegmentCursor(container_, segment, releaseAfterRead_);
}

int32_t MergeReadSession::compareCurrentRows(
    const SegmentCursor& left,
    const SegmentCursor& right,
    const std::vector<CompareFlags>& flags) {
  BOLT_DCHECK_NOT_NULL(container_);
  BOLT_DCHECK(left.hasCurrent());
  BOLT_DCHECK(right.hasCurrent());
  return container_->compareRows(left.currentRow(), right.currentRow(), flags);
}

BulkReadSession BmRowContainer::beginBulkReadSegments(
    folly::Range<const SegmentId*> segments,
    ReadSessionOptions options) {
  std::vector<SegmentId> segmentIds(segments.begin(), segments.end());
  validateSegments({segmentIds.data(), segmentIds.size()});

  return BulkReadSession(this, std::move(segmentIds), options);
}

} // namespace bytedance::bolt::exec::bm
