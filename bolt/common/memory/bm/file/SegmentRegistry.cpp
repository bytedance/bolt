#include "bolt/common/memory/bm/file/SegmentRegistry.h"

#include <utility>

namespace bytedance::bolt::memory::bm {

uint64_t SegmentRegistry::NextSegmentId() {
  return next_segment_id_++;
}

void SegmentRegistry::Register(SegmentRecord record) {
  records_.emplace(record.segment.id, std::move(record));
}

FileErrorCode SegmentRegistry::Take(uint64_t segment_id, SegmentRecord* record) {
  const auto it = records_.find(segment_id);
  if (it == records_.end()) {
    return FileErrorCode::kDoubleFree;
  }
  *record = it->second;
  records_.erase(it);
  return FileErrorCode::kOk;
}

} // namespace bytedance::bolt::memory::bm
