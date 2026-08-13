#pragma once

#include <array>
#include <cstdint>

#include "bolt/common/memory/bm/io/DepthControlConfig.h"
#include "bolt/common/memory/bm/io/IoPriority.h"

namespace bytedance::bolt::memory::bm {

struct DiskIoSchedulerConfig {
  uint32_t ringDepth{256};
  std::array<uint32_t, kIoPriorityCount> priorityWeights{{4, 2, 1}};
  DepthControlConfig depthControl;
};

} // namespace bytedance::bolt::memory::bm
