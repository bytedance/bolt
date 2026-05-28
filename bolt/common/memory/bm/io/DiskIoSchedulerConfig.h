#pragma once

#include <array>
#include <chrono>
#include <cstdint>

#include "bolt/common/memory/bm/io/DepthControlConfig.h"
#include "bolt/common/memory/bm/io/IoPriority.h"

namespace bytedance::bolt::memory::bm {

struct DiskIoSchedulerConfig {
  uint32_t ringDepth{256};
  std::array<uint32_t, kIoPriorityCount> priorityWeights{{8, 4, 1}};
  DepthControlConfig depthControl;
  bool enableStatsLogging{false};
  std::chrono::milliseconds statsLogInterval{std::chrono::seconds(10)};
};

} // namespace bytedance::bolt::memory::bm
