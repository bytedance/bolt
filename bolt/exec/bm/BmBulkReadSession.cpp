#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <utility>

namespace bytedance::bolt::exec::bm {

BulkReadSession::BulkReadSession(
    BmRowContainer* container,
    std::vector<SegmentId> segments)
    : container_(container),
      segmentOrder_(std::move(segments)),
      segments_(segmentOrder_.begin(), segmentOrder_.end()) {}

void BulkReadSession::load() {
  BOLT_CHECK_NOT_NULL(container_);
  if (loaded_) {
    return;
  }
  container_->ensureSegmentsLoaded(
      {segmentOrder_.data(), segmentOrder_.size()});
  loaded_ = true;
}

std::vector<char*> BulkReadSession::loadRows() {
  BOLT_CHECK_NOT_NULL(container_);
  load();
  return container_->loadAllRows({segmentOrder_.data(), segmentOrder_.size()});
}

std::vector<const char*> BulkReadSession::loadRows(
    folly::Range<const SegmentRowRange*> ranges) {
  BOLT_CHECK_NOT_NULL(container_);
  load();

  std::vector<const char*> rows;
  uint64_t numRows = 0;
  for (const auto& range : ranges) {
    numRows += range.count;
  }
  rows.reserve(numRows);

  for (const auto& range : ranges) {
    BOLT_CHECK(
        segments_.count(range.segment) != 0,
        "Range segment {} is not covered by this bulk read session",
        range.segment);
    if (range.count == 0) {
      continue;
    }

    auto& segment = container_->segments_.segmentData(range.segment);
    BOLT_CHECK_LE(
        static_cast<uint64_t>(range.begin) + range.count,
        segment.nextRowNumber,
        "Row range [{}, {}) exceeds segment {} row count {}",
        range.begin,
        range.begin + range.count,
        range.segment,
        segment.nextRowNumber);
    for (RowNumber offset = 0; offset < range.count; ++offset) {
      const auto rowNumber = range.begin + offset;
      const auto& chunk = container_->segments_.chunkForRow(segment, rowNumber);
      BOLT_CHECK(
          !chunk.consumed,
          "Cannot materialize row pointer for consumed chunk {} in segment {}",
          chunk.meta.id,
          range.segment);
      rows.push_back(container_->segments_.rowPointer(
          container_->segments_.rowIdForRowNumber(segment, rowNumber)));
    }
  }
  return rows;
}

} // namespace bytedance::bolt::exec::bm
