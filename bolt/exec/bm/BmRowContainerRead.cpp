#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::exec::bm {

BulkReadSession::BulkReadSession(
    BmRowContainer* container,
    ReadMode mode,
    std::vector<memory::bm::BufferHandle> pins,
    std::vector<SegmentId> segments,
    ReadSessionOptions options)
    : container_(container),
      mode_(mode),
      pins_(std::move(pins)),
      segmentOrder_(std::move(segments)),
      segments_(segmentOrder_.begin(), segmentOrder_.end()),
      options_(std::move(options)) {}

LoadAllResult BulkReadSession::tryLoadAll(
    std::vector<char*>& rows,
    std::vector<RowId>& rowIds) {
  BOLT_CHECK_NOT_NULL(container_);
  rows.clear();
  rowIds.clear();
  pins_.clear();

  uint64_t bytes = 0;
  for (auto segment : segmentOrder_) {
    bytes += container_->segmentBytes(container_->segmentData(segment));
  }

  const auto returnWindowRead = [&]() {
    mode_ = ReadMode::kWindowRead;
    for (auto segment : segmentOrder_) {
      container_->appendRowIdsForSegment(
          container_->segmentData(segment), rowIds);
    }
    return LoadAllResult::kNeedWindowRead;
  };

  if (options_.maxPinnedBytes != 0 && bytes > options_.maxPinnedBytes) {
    return returnWindowRead();
  }

  const bool reserved =
      bytes == 0 || container_->bufferManager_->MaybeReserve(bytes);
  if (!reserved) {
    return returnWindowRead();
  }

  try {
    pins_ = container_->pinSegments(segmentOrder_);
    container_->bufferManager_->ReleaseUnusedReservation();
    mode_ = ReadMode::kFullyResident;
    for (auto segment : segmentOrder_) {
      container_->appendRowPointersForSegment(
          container_->segmentData(segment), rows);
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
    auto& segment = container_->segmentData(row.segmentId);
    const auto& chunk = container_->chunkForRow(segment, row.rowNumber);
    const auto key =
        (static_cast<uint64_t>(row.segmentId) << 32) | chunk.id;
    if (pinnedChunks.insert(key).second) {
      auto pins = container_->pinChunk(segment, chunk);
      for (auto& pin : pins) {
        windowPins_.push_back(std::move(pin));
      }
    }
  }

  for (const auto& row : rows) {
    rowViews_.push_back({row, container_->rowPointer(row)});
  }
  return {{rowViews_.data(), rowViews_.size()}};
}

char* BulkReadSession::loadRow(const RowId& row) {
  auto window = loadRows({&row, 1});
  BOLT_CHECK_EQ(1, window.rows.size());
  return window.rows[0].ptr;
}

SegmentCursor::SegmentCursor(
    BmRowContainer* container,
    SortedRunId run,
    ReadSessionOptions options)
    : container_(container), run_(run), options_(std::move(options)) {
  loadCurrent();
}

void SegmentCursor::advance() {
  if (container_ == nullptr) {
    return;
  }
  ++index_;
  loadCurrent();
}

void SegmentCursor::loadCurrent() {
  currentRow_ = nullptr;
  pins_.clear();
  const auto& run = container_->sortedRunData(run_);
  if (index_ >= run.numRows) {
    return;
  }

  BOLT_CHECK(run.layout == SortedRunLayout::kMaterializedOrder);
  auto& segment = container_->segmentData(run.materializedSegment);
  auto rowId =
      container_->rowIdForRowNumber(segment, static_cast<RowNumber>(index_));
  const auto& chunk = container_->chunkForRow(segment, rowId.rowNumber);
  pins_ = container_->pinChunk(segment, chunk);
  currentRow_ = container_->rowPointer(rowId);
}

MergeReadSession::MergeReadSession(
    BmRowContainer* container,
    std::vector<SortedRunId> runs,
    ReadSessionOptions options)
    : container_(container),
      runs_(runs.begin(), runs.end()),
      options_(std::move(options)) {}

SegmentCursor MergeReadSession::cursor(SortedRunId run) {
  BOLT_CHECK_NOT_NULL(container_);
  BOLT_CHECK(runs_.count(run) != 0, "Sorted run {} is not covered", run);
  return SegmentCursor(container_, run, options_);
}

int32_t MergeReadSession::compareCurrentRows(
    const SegmentCursor& left,
    const SegmentCursor& right,
    const std::vector<CompareFlags>& flags) {
  BOLT_CHECK_NOT_NULL(container_);
  BOLT_CHECK(left.hasCurrent());
  BOLT_CHECK(right.hasCurrent());
  return container_->compareRows(left.currentRow(), right.currentRow(), flags);
}

SortedRunId BmRowContainer::finalizeSortedRun(
    folly::Range<char* const*> sortedRows,
    const SortedRunOptions& options) {
  BOLT_CHECK(!sortedRows.empty());
  BOLT_CHECK(options.preferredLayout == SortedRunLayout::kMaterializedOrder);

  auto& materialized = createSegment(std::nullopt);
  const auto materializedSegment = materialized.meta.id;
  for (auto* row : sortedRows) {
    copyRowToSegment(materialized, row);
  }
  finalizeAndFlushSegment(materialized);

  SortedRunMeta meta;
  meta.id = nextSortedRunId_++;
  meta.layout = SortedRunLayout::kMaterializedOrder;
  meta.materializedSegment = materializedSegment;
  meta.numRows = sortedRows.size();

  sortedRuns_.emplace(meta.id, std::move(meta));
  return nextSortedRunId_ - 1;
}

BulkReadSession BmRowContainer::beginBulkReadSegments(
    folly::Range<const SegmentId*> segments,
    ReadSessionOptions options) {
  std::vector<SegmentId> segmentIds(segments.begin(), segments.end());
  for (auto segment : segmentIds) {
    (void)segmentData(segment);
  }

  return BulkReadSession(
      this, ReadMode::kWindowRead, {}, std::move(segmentIds), options);
}

MergeReadSession BmRowContainer::beginMergeReadSegments(
    folly::Range<const SortedRunId*> runs,
    ReadSessionOptions options) {
  std::vector<SortedRunId> runIds(runs.begin(), runs.end());
  for (auto run : runIds) {
    (void)sortedRunData(run);
  }
  return MergeReadSession(this, std::move(runIds), options);
}

SortedRunMeta& BmRowContainer::sortedRunData(SortedRunId run) {
  auto it = sortedRuns_.find(run);
  BOLT_CHECK(it != sortedRuns_.end(), "Unknown sorted run {}", run);
  return it->second;
}

const SortedRunMeta& BmRowContainer::sortedRunData(SortedRunId run) const {
  auto it = sortedRuns_.find(run);
  BOLT_CHECK(it != sortedRuns_.end(), "Unknown sorted run {}", run);
  return it->second;
}

} // namespace bytedance::bolt::exec::bm
