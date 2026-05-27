#pragma once

#include <chrono>
#include <cstdint>

namespace bytedance::bolt::memory::bm {

struct AdaptiveDepthConfig {
  bool enabled{true};
  uint32_t minDepth{1};
  uint32_t initialDepth{64};
  uint32_t maxDepth{256};
  std::chrono::milliseconds controlInterval{200};
  uint32_t increaseStep{4};
  double minThroughputGain{0.02};
  double throughputSmoothingFactor{0.5};
};

} // namespace bytedance::bolt::memory::bm
