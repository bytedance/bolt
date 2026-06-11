#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <utility>

namespace bytedance::bolt::exec::bm {

WindowReadSession::WindowReadSession(
    BmRowContainer* container,
    std::vector<SegmentId> segments)
    : container_(container),
      segmentOrder_(std::move(segments)),
      segments_(segmentOrder_.begin(), segmentOrder_.end()) {}

std::vector<char*> WindowReadSession::loadRows(
    folly::Range<const RowId*> rows) {
  BOLT_CHECK_NOT_NULL(container_);

  std::vector<ChunkData*> chunks;
  chunks.reserve(rows.size());
  std::unordered_set<uint64_t> seenChunks;
  for (const auto& row : rows) {
    BOLT_CHECK(
        segments_.count(row.segmentId) != 0,
        "Row segment {} is not covered by this read session",
        row.segmentId);
    auto& segment = container_->segments_.segmentData(row.segmentId);
    auto& chunk = container_->segments_.chunkForRow(segment, row.rowNumber);
    const auto key =
        (static_cast<uint64_t>(row.segmentId) << 32) | chunk.meta.id;
    if (seenChunks.insert(key).second) {
      chunks.push_back(&chunk);
    }
  }

  container_->ensureChunksLoaded({chunks.data(), chunks.size()});

  std::vector<char*> result;
  result.reserve(rows.size());
  for (const auto& row : rows) {
    result.push_back(container_->segments_.rowPointer(row));
  }
  return result;
}

char* WindowReadSession::loadRow(const RowId& row) {
  auto rows = loadRows({&row, 1});
  BOLT_DCHECK_EQ(1, rows.size());
  return rows[0];
}

} // namespace bytedance::bolt::exec::bm
