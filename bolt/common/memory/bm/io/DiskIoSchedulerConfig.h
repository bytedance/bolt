#pragma once

#include <array>
#include <chrono>
#include <cstdint>

#include "bolt/common/memory/bm/io/AdaptiveDepthConfig.h"
#include "bolt/common/memory/bm/io/IoPriority.h"

namespace bytedance::bolt::memory::bm {

struct DiskIoSchedulerConfig {
  uint32_t ringDepth{256};
  std::array<uint32_t, kIoPriorityCount> priorityWeights{{8, 4, 1}};
  AdaptiveDepthConfig adaptiveDepth;
  bool enableStatsLogging{false};
  std::chrono::milliseconds statsLogInterval{std::chrono::seconds(10)};
};

} // namespace bytedance::bolt::memory::bm
