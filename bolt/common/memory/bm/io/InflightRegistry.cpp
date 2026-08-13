#include "bolt/common/memory/bm/io/InflightRegistry.h"

#include <utility>

namespace bytedance::bolt::memory::bm {

void InflightRegistry::add(uint64_t requestId, InflightIoRequest request) {
  requests_.emplace(requestId, std::move(request));
}

std::optional<InflightIoRequest> InflightRegistry::take(uint64_t requestId) {
  auto it = requests_.find(requestId);
  if (it == requests_.end()) {
    return std::nullopt;
  }

  auto request = std::move(it->second);
  requests_.erase(it);
  return request;
}

bool InflightRegistry::empty() const {
  return requests_.empty();
}

size_t InflightRegistry::size() const {
  return requests_.size();
}

} // namespace bytedance::bolt::memory::bm
