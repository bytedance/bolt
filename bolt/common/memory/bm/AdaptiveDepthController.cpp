#include "bolt/common/memory/bm/AdaptiveDepthController.h"

#include <algorithm>

namespace bytedance::bolt::memory::bm {

AdaptiveDepthController::AdaptiveDepthController(AdaptiveDepthConfig config)
    : config_(config),
      currentDepth_(config.initialDepth),
      bestDepth_(config.initialDepth) {}

uint32_t AdaptiveDepthController::currentDepth() const {
  return currentDepth_;
}

double AdaptiveDepthController::recentThroughputBytesPerSecond() const {
  return recentThroughputBytesPerSecond_;
}

void AdaptiveDepthController::onWindow(
    double throughputBytesPerSecond,
    bool hasBacklog) {
  recentThroughputBytesPerSecond_ = throughputBytesPerSecond;

  if (!config_.enabled || !hasBacklog) {
    return;
  }

  if (!hasBestThroughput_ || throughputImproved(throughputBytesPerSecond)) {
    bestThroughputBytesPerSecond_ = throughputBytesPerSecond;
    bestDepth_ = currentDepth_;
    hasBestThroughput_ = true;
    currentDepth_ = increasedDepth();
    return;
  }

  currentDepth_ = bestDepth_;
}

bool AdaptiveDepthController::throughputImproved(
    double throughputBytesPerSecond) const {
  return throughputBytesPerSecond >=
      bestThroughputBytesPerSecond_ * (1.0 + config_.minThroughputGain);
}

uint32_t AdaptiveDepthController::increasedDepth() const {
  const auto availableIncrease = config_.maxDepth - currentDepth_;
  return currentDepth_ + std::min(config_.increaseStep, availableIncrease);
}

} // namespace bytedance::bolt::memory::bm
