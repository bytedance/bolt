#include "bolt/common/memory/bm/file/ExtentRegistry.h"

#include <utility>

namespace bytedance::bolt::memory::bm {

uint64_t ExtentRegistry::NextExtentId() {
  std::lock_guard<std::mutex> lock(mutex_);
  return next_extent_id_++;
}

void ExtentRegistry::Register(ExtentRecord record) {
  std::lock_guard<std::mutex> lock(mutex_);
  records_.emplace(record.extent.id, std::move(record));
}

FileErrorCode ExtentRegistry::Take(uint64_t extent_id, ExtentRecord* record) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto it = records_.find(extent_id);
  if (it == records_.end()) {
    return FileErrorCode::kDoubleFree;
  }
  *record = it->second;
  records_.erase(it);
  return FileErrorCode::kOk;
}

} // namespace bytedance::bolt::memory::bm
