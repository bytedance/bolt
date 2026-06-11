#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::exec::bm {

SegmentId BmRowContainer::finalizeReorderedSegment(
    folly::Range<char* const*> rowsInOrder) {
  BOLT_CHECK(!rowsInOrder.empty());

  auto& materialized = storage_.createSegment(std::nullopt);
  const auto materializedSegment = materialized.meta.id;
  for (auto* row : rowsInOrder) {
    rowCopier_.copyRowToSegment(materialized, row);
  }
  storage_.finalizeAndFlushSegment(materialized);
  return materializedSegment;
}

MergeReadSession BmRowContainer::beginMergeReadSegments(
    folly::Range<const SegmentId*> segments,
    bool releaseAfterRead) {
  std::vector<SegmentId> segmentIds(segments.begin(), segments.end());
  for (auto segment : segmentIds) {
    (void)storage_.segmentData(segment);
  }
  return MergeReadSession(this, std::move(segmentIds), releaseAfterRead);
}

} // namespace bytedance::bolt::exec::bm
