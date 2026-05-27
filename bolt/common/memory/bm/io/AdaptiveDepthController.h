#pragma once

#include <cstdint>
#include <chrono>

#include "bolt/common/memory/bm/io/AdaptiveDepthConfig.h"
#include "bolt/common/memory/bm/io/AdaptiveDepthStats.h"

namespace bytedance::bolt::memory::bm {

class AdaptiveDepthController {
 public:
  explicit AdaptiveDepthController(AdaptiveDepthConfig config);

  uint32_t currentDepth() const;
  double recentThroughputBytesPerSecond() const;
  AdaptiveDepthStats stats() const;
  void onCompletion(
      uint64_t completedBytes,
      bool hasBacklog,
      std::chrono::steady_clock::time_point now);
  void onWindow(double throughputBytesPerSecond, bool hasBacklog);

 private:
  static bool isValidConfig(const AdaptiveDepthConfig& config);
  static bool isValidThroughput(double throughputBytesPerSecond);

  bool throughputImproved(double throughputBytesPerSecond) const;
  void scheduleProbe();

  AdaptiveDepthConfig config_;
  uint32_t currentDepth_{0};
  uint32_t bestDepth_{0};
  std::chrono::steady_clock::time_point windowStart_;
  uint64_t windowCompletedBytes_{0};
  double recentThroughputBytesPerSecond_{0};
  double bestThroughputBytesPerSecond_{0};
  bool hasBestThroughput_{false};
  bool hasRecentThroughput_{false};
  bool measuringProbeDepth_{false};
  uint64_t completedWindows_{0};
  double lastWindowThroughputBytesPerSecond_{0};
};

} // namespace bytedance::bolt::memory::bm
