#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::exec::bm {

ReorderedRunId BmRowContainer::finalizeReorderedRun(
    folly::Range<char* const*> rowsInOrder) {
  BOLT_CHECK(!rowsInOrder.empty());

  auto& materialized = storage_.createSegment(std::nullopt);
  const auto materializedSegment = materialized.meta.id;
  for (auto* row : rowsInOrder) {
    rowCopier_.copyRowToSegment(materialized, row);
  }
  storage_.finalizeAndFlushSegment(materialized);

  ReorderedRunMeta meta;
  meta.id = nextReorderedRunId_++;
  meta.materializedSegment = materializedSegment;
  meta.numRows = rowsInOrder.size();

  reorderedRuns_.emplace(meta.id, std::move(meta));
  return nextReorderedRunId_ - 1;
}

MergeReadSession BmRowContainer::beginMergeReadRuns(
    folly::Range<const ReorderedRunId*> runs,
    ReadSessionOptions options) {
  std::vector<ReorderedRunId> runIds(runs.begin(), runs.end());
  for (auto run : runIds) {
    (void)reorderedRunData(run);
  }
  return MergeReadSession(this, std::move(runIds), options);
}

ReorderedRunMeta& BmRowContainer::reorderedRunData(ReorderedRunId run) {
  auto it = reorderedRuns_.find(run);
  BOLT_CHECK(it != reorderedRuns_.end(), "Unknown reordered run {}", run);
  return it->second;
}

const ReorderedRunMeta& BmRowContainer::reorderedRunData(
    ReorderedRunId run) const {
  auto it = reorderedRuns_.find(run);
  BOLT_CHECK(it != reorderedRuns_.end(), "Unknown reordered run {}", run);
  return it->second;
}

} // namespace bytedance::bolt::exec::bm
