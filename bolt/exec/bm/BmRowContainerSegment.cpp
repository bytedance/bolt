#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::exec::bm {

SegmentId BmRowContainer::flushActiveSegment() {
  return storage_.flushActiveSegment();
}

SegmentId BmRowContainer::flushActivePartitionSegment(PartitionId partition) {
  return storage_.flushActivePartitionSegment(partition);
}

void BmRowContainer::releaseSegment(
    SegmentId segment,
    ReleaseReason reason) {
  storage_.releaseSegment(segment, reason);
}

void BmRowContainer::releaseSegments(
    folly::Range<const SegmentId*> segments,
    ReleaseReason reason) {
  storage_.releaseSegments(segments, reason);
}

SegmentState BmRowContainer::segmentState(SegmentId segment) const {
  return storage_.segmentState(segment);
}

const std::vector<SegmentId>& BmRowContainer::segmentsForPartition(
    PartitionId partition) const {
  return storage_.segmentsForPartition(partition);
}

int64_t BmRowContainer::numRows() const {
  return storage_.numRows();
}

} // namespace bytedance::bolt::exec::bm
