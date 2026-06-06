#include "bolt/exec/bm/BmBlockReclaimPolicy.h"

#include <algorithm>

namespace bytedance::bolt::exec {

std::vector<BlockId> BmLruBlockReclaimPolicy::selectVictims(
    const BmBlockReclaimContext& context) const {
  std::vector<BmBlockReclaimCandidate> candidates(
      context.candidates.begin(), context.candidates.end());
  std::sort(
      candidates.begin(),
      candidates.end(),
      [](const auto& left, const auto& right) {
        return left.lastAccess < right.lastAccess;
      });

  std::vector<BlockId> victims;
  victims.reserve(candidates.size());
  uint64_t selectedBytes = 0;
  for (const auto& candidate : candidates) {
    victims.push_back(candidate.blockId);
    selectedBytes += candidate.capacity;
    if (context.targetBytes != 0 && selectedBytes >= context.targetBytes) {
      break;
    }
  }
  return victims;
}

} // namespace bytedance::bolt::exec
