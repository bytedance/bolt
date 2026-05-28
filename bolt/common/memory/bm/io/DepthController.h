#pragma once

#include <chrono>
#include <cstdint>
#include <memory>

#include "bolt/common/memory/bm/io/DepthControlConfig.h"
#include "bolt/common/memory/bm/io/DepthControlStats.h"

namespace bytedance::bolt::memory::bm {

class DepthController {
 public:
  virtual ~DepthController() = default;

  virtual uint32_t currentDepth() const = 0;
  virtual double recentThroughputBytesPerSecond() const = 0;
  virtual DepthControlStatsPtr stats() const = 0;
  virtual void onCompletion(
      uint64_t bytes,
      bool hasQueuedRequests,
      std::chrono::steady_clock::time_point now) = 0;
};

std::unique_ptr<DepthController> createDepthController(
    const DepthControlConfig& config);

} // namespace bytedance::bolt::memory::bm
