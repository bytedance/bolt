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
      segments_(segments.begin(), segments.end()),
      options_(options) {}

folly::Range<char* const*> BulkReadSession::resolveRows(
    folly::Range<const RowId*> rows,
    std::vector<char*>& result) {
  BOLT_CHECK_NOT_NULL(container_);
  BOLT_CHECK(mode_ == ReadMode::kFullyResident);
  result.clear();
  result.reserve(rows.size());
  for (const auto& row : rows) {
    BOLT_CHECK(
        segments_.count(row.segmentId) != 0,
        "Row segment {} is not covered by this read session",
        row.segmentId);
    result.push_back(container_->rowPointer(row));
  }
  return {result.data(), result.size()};
}

void BulkReadSession::extractColumn(
    folly::Range<const RowId*> rows,
    int32_t column,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) {
  if (mode_ == ReadMode::kFullyResident) {
    std::vector<char*> resolved;
    resolveRows(rows, resolved);
    for (vector_size_t i = 0; i < resolved.size(); ++i) {
      container_->extractOne(
          resolved[i], column, resultOffset + i, result, exactSize);
    }
    return;
  }

  for (vector_size_t i = 0; i < rows.size(); ++i) {
    const auto& row = rows[i];
    BOLT_CHECK(
        segments_.count(row.segmentId) != 0,
        "Row segment {} is not covered by this read session",
        row.segmentId);
    auto& segment = container_->segmentData(row.segmentId);
    const auto& chunk = container_->chunkForRow(segment, row.rowNumber);
    auto pins = container_->pinChunk(segment, chunk);
    container_->extractOne(
        container_->rowPointer(row),
        column,
        resultOffset + i,
        result,
        exactSize);
  }
}

void BulkReadSession::extractNulls(
    folly::Range<const RowId*> rows,
    int32_t column,
    const BufferPtr& result) {
  auto* raw = result->asMutable<uint64_t>();
  if (mode_ == ReadMode::kFullyResident) {
    std::vector<char*> resolved;
    resolveRows(rows, resolved);
    for (vector_size_t i = 0; i < resolved.size(); ++i) {
      bits::setNull(raw, i, container_->isNull(resolved[i], column));
    }
    return;
  }

  for (vector_size_t i = 0; i < rows.size(); ++i) {
    const auto& row = rows[i];
    BOLT_CHECK(
        segments_.count(row.segmentId) != 0,
        "Row segment {} is not covered by this read session",
        row.segmentId);
    auto& segment = container_->segmentData(row.segmentId);
    const auto& chunk = container_->chunkForRow(segment, row.rowNumber);
    auto pins = container_->pinChunk(segment, chunk);
    bits::setNull(
        raw, i, container_->isNull(container_->rowPointer(row), column));
  }
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

  BOLT_CHECK(
      run.layout == SortedRunLayout::kRowIdOrder,
      "Materialized sorted run cursor is not implemented yet");
  currentRowId_ = run.sortedRows[index_];
  auto& segment = container_->segmentData(currentRowId_.segmentId);
  const auto& chunk = container_->chunkForRow(segment, currentRowId_.rowNumber);
  pins_ = container_->pinChunk(segment, chunk);
  currentRow_ = container_->rowPointer(currentRowId_);
}

MergeReadSession::MergeReadSession(
    BmRowContainer* container,
    std::vector<SortedRunId> runs,
    ReadSessionOptions options)
    : container_(container),
      runs_(runs.begin(), runs.end()),
      options_(options) {}

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
    folly::Range<const RowHandle*> sortedRows,
    const SortedRunOptions& options) {
  BOLT_CHECK(!sortedRows.empty());
  const auto segmentId = sortedRows[0].id.segmentId;
  auto& segment = segmentData(segmentId);
  BOLT_CHECK(segment.meta.state == SegmentState::kActiveResident);

  SortedRunMeta meta;
  meta.id = nextSortedRunId_++;
  meta.layout = options.preferredLayout;
  meta.numRows = sortedRows.size();
  meta.sourceSegments.push_back(segmentId);
  meta.sortedRows.reserve(sortedRows.size());
  for (const auto& row : sortedRows) {
    BOLT_CHECK_EQ(segmentId, row.id.segmentId);
    meta.sortedRows.push_back(row.id);
  }

  if (meta.layout == SortedRunLayout::kMaterializedOrder) {
    BOLT_NYI("Materialized sorted run is not implemented yet");
  }

  sortedRuns_.emplace(meta.id, std::move(meta));
  finalizeAndFlush(segment.meta.partitionId.value_or(kDefaultPartition));
  return nextSortedRunId_ - 1;
}

BulkReadSession BmRowContainer::beginBulkReadSegments(
    folly::Range<const SegmentId*> segments,
    ReadSessionOptions options) {
  std::vector<SegmentId> segmentIds(segments.begin(), segments.end());
  uint64_t bytes = 0;
  for (auto segment : segmentIds) {
    bytes += segmentBytes(segmentData(segment));
  }
  if (options.maxPinnedBytes != 0 && bytes > options.maxPinnedBytes) {
    return BulkReadSession(
        this, ReadMode::kWindowRead, {}, std::move(segmentIds), options);
  }

  try {
    auto pins = pinSegments(segments);
    return BulkReadSession(
        this,
        ReadMode::kFullyResident,
        std::move(pins),
        std::move(segmentIds),
        options);
  } catch (const std::exception&) {
    return BulkReadSession(
        this, ReadMode::kWindowRead, {}, std::move(segmentIds), options);
  }
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
