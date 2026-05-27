#pragma once

#include <cstdint>

namespace bytedance::bolt::memory::bm {

struct AdaptiveDepthStats {
  bool enabled{true};
  uint32_t currentDepth{0};
  uint32_t bestDepth{0};
  double recentThroughputBytesPerSecond{0};
  double bestThroughputBytesPerSecond{0};
  bool measuringProbeDepth{false};
  uint64_t completedWindows{0};
  double lastWindowThroughputBytesPerSecond{0};
};

} // namespace bytedance::bolt::memory::bm
