#pragma once

#include <array>
#include <cstdint>

#include "bolt/common/memory/bm/io/IoPriority.h"

namespace bytedance::bolt::memory::bm {

struct DiskIoSchedulerStats {
  std::array<uint64_t, kIoPriorityCount> queuedRequests{{0, 0, 0}};
  std::array<uint64_t, kIoPriorityCount> submittedRequests{{0, 0, 0}};
  std::array<uint64_t, kIoPriorityCount> completedRequestsByPriority{{0, 0, 0}};
  uint64_t inflightRequests{0};
  uint32_t currentDepth{0};
  uint64_t completedRequests{0};
  uint64_t completedBytes{0};
  uint64_t successfulRequests{0};
  uint64_t failedRequests{0};
  double recentThroughputBytesPerSecond{0};
  uint64_t cumulativeLatencyUs{0};
  double averageLatencyUs{0};
};

} // namespace bytedance::bolt::memory::bm
