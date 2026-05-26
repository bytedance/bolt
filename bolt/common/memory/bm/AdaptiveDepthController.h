#pragma once

#include <cstdint>

#include "bolt/common/memory/bm/DiskIoTypes.h"

namespace bytedance::bolt::memory::bm {

class AdaptiveDepthController {
 public:
  explicit AdaptiveDepthController(AdaptiveDepthConfig config);

  uint32_t currentDepth() const;
  double recentThroughputBytesPerSecond() const;
  void onWindow(double throughputBytesPerSecond, bool hasBacklog);

 private:
  bool throughputImproved(double throughputBytesPerSecond) const;
  uint32_t increasedDepth() const;

  AdaptiveDepthConfig config_;
  uint32_t currentDepth_{0};
  uint32_t bestDepth_{0};
  double recentThroughputBytesPerSecond_{0};
  double bestThroughputBytesPerSecond_{0};
  bool hasBestThroughput_{false};
};

} // namespace bytedance::bolt::memory::bm
