/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
