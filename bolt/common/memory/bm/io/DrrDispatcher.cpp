#include "bolt/common/memory/bm/io/DrrDispatcher.h"

namespace bytedance::bolt::memory::bm {

DrrDispatcher::DrrDispatcher(std::array<uint32_t, kIoPriorityCount> weights)
    : weights_(weights) {}

std::optional<size_t> DrrDispatcher::pick(
    const std::array<size_t, kIoPriorityCount>& queueSizes) {
  if (!hasDispatchableDeficit(queueSizes)) {
    refillDeficits(queueSizes);
  }

  for (size_t i = 0; i < kIoPriorityCount; ++i) {
    const auto priority = nextPriorityCursor_;
    nextPriorityCursor_ = (nextPriorityCursor_ + 1) % kIoPriorityCount;

    if (queueSizes[priority] == 0) {
      deficits_[priority] = 0;
      continue;
    }
    if (deficits_[priority] <= 0) {
      continue;
    }

    --deficits_[priority];
    return priority;
  }

  return std::nullopt;
}

void DrrDispatcher::restore(size_t priority) {
  ++deficits_[priority];
  nextPriorityCursor_ = priority;
}

void DrrDispatcher::reset(size_t priority) {
  deficits_[priority] = 0;
}

bool DrrDispatcher::hasDispatchableDeficit(
    const std::array<size_t, kIoPriorityCount>& queueSizes) const {
  for (size_t i = 0; i < kIoPriorityCount; ++i) {
    if (queueSizes[i] > 0 && deficits_[i] > 0) {
      return true;
    }
  }
  return false;
}

void DrrDispatcher::refillDeficits(
    const std::array<size_t, kIoPriorityCount>& queueSizes) {
  for (size_t i = 0; i < kIoPriorityCount; ++i) {
    if (queueSizes[i] > 0) {
      deficits_[i] += weights_[i];
    } else {
      deficits_[i] = 0;
    }
  }
}

} // namespace bytedance::bolt::memory::bm
