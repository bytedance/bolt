#pragma once

#include <chrono>
#include <cstdint>

#include "bolt/common/memory/bm/io/AdaptiveDepthController.h"
#include "bolt/common/memory/bm/io/AdaptiveDepthStats.h"

namespace bytedance::bolt::memory::bm {

class DepthController {
 public:
  explicit DepthController(AdaptiveDepthConfig config);

  uint32_t currentDepth() const;
  double recentThroughputBytesPerSecond() const;
  AdaptiveDepthStats stats() const;
  void onCompletion(
      uint64_t bytes,
      bool hasQueuedRequests,
      std::chrono::steady_clock::time_point now);

 private:
  AdaptiveDepthController adaptive_;
};

} // namespace bytedance::bolt::memory::bm
