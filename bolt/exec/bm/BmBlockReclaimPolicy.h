#pragma once

#include "bolt/exec/bm/BmRowTypes.h"

#include <cstdint>
#include <span>
#include <vector>

namespace bytedance::bolt::exec {

struct BmBlockReclaimCandidate {
  BlockId blockId{0};
  uint32_t capacity{0};
  bool pinned{false};
  uint64_t lastAccess{0};
};

struct BmBlockReclaimContext {
  std::span<const BmBlockReclaimCandidate> candidates;
  uint64_t targetBytes{0};
};

class BmBlockReclaimPolicy {
 public:
  virtual ~BmBlockReclaimPolicy() = default;

  virtual std::vector<BlockId> selectVictims(
      const BmBlockReclaimContext& context) const = 0;
};

class BmLruBlockReclaimPolicy final : public BmBlockReclaimPolicy {
 public:
  std::vector<BlockId> selectVictims(
      const BmBlockReclaimContext& context) const override;
};

} // namespace bytedance::bolt::exec
