#pragma once

#include <array>
#include <cstdint>
#include <optional>

#include "bolt/common/memory/bm/io/IoPriority.h"

namespace bytedance::bolt::memory::bm {

class DrrDispatcher {
 public:
  explicit DrrDispatcher(std::array<uint32_t, kIoPriorityCount> weights);

  std::optional<size_t> pick(
      const std::array<size_t, kIoPriorityCount>& queueSizes);
  void restore(size_t priority);
  void reset(size_t priority);

 private:
  bool hasDispatchableDeficit(
      const std::array<size_t, kIoPriorityCount>& queueSizes) const;
  void refillDeficits(const std::array<size_t, kIoPriorityCount>& queueSizes);

  std::array<uint32_t, kIoPriorityCount> weights_;
  std::array<int64_t, kIoPriorityCount> deficits_{};
  size_t nextPriorityCursor_{0};
};

} // namespace bytedance::bolt::memory::bm
