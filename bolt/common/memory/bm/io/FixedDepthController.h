#pragma once

#include <chrono>
#include <cstdint>

#include "bolt/common/memory/bm/io/DepthControlConfig.h"
#include "bolt/common/memory/bm/io/DepthController.h"

namespace bytedance::bolt::memory::bm {

class FixedDepthController : public DepthController {
 public:
  explicit FixedDepthController(FixedDepthConfig config);

  uint32_t currentDepth() const override;
  double recentThroughputBytesPerSecond() const override;
  DepthControlStatsPtr stats() const override;
  void onCompletion(
      uint64_t completedBytes,
      bool hasQueuedRequests,
      std::chrono::steady_clock::time_point now) override;

 private:
  FixedDepthConfig config_;
  std::chrono::steady_clock::time_point windowStart_;
  uint64_t windowCompletedBytes_{0};
  double recentThroughputBytesPerSecond_{0};
  uint64_t completedWindows_{0};
  double lastWindowThroughputBytesPerSecond_{0};
};

} // namespace bytedance::bolt::memory::bm
