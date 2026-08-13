#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::exec::bm {

SegmentId BmRowContainer::finalizeReorderedSegment(
    folly::Range<char* const*> rowsInOrder) {
  BOLT_CHECK(!rowsInOrder.empty());

  auto& materialized = segments_.createSegment(std::nullopt);
  const auto materializedSegment = materialized.meta.id;
  for (auto* row : rowsInOrder) {
    rowCopier_.copyRowToSegment(materialized, row);
  }
  segments_.finalizeAndFlushSegment(materialized);
  materialized.meta.orderedForMerge = true;
  return materializedSegment;
}

MergeReadSession BmRowContainer::beginMergeReadSegments(
    folly::Range<const SegmentId*> segments,
    bool releaseAfterRead) {
  std::vector<SegmentId> segmentIds(segments.begin(), segments.end());
  validateSegments({segmentIds.data(), segmentIds.size()});
  for (auto segment : segmentIds) {
    const auto& data = segments_.segmentData(segment);
    BOLT_CHECK(
        data.meta.orderedForMerge,
        "Segment {} is not ordered for merge read",
        segment);
  }
  return MergeReadSession(this, std::move(segmentIds), releaseAfterRead);
}

} // namespace bytedance::bolt::exec::bm
