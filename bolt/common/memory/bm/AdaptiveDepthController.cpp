#include "bolt/common/memory/bm/AdaptiveDepthController.h"

#include "bolt/common/base/Exceptions.h"

#include <algorithm>
#include <cmath>

namespace bytedance::bolt::memory::bm {

AdaptiveDepthController::AdaptiveDepthController(AdaptiveDepthConfig config)
    : config_(config),
      currentDepth_(config.initialDepth),
      bestDepth_(config.initialDepth) {
  BOLT_CHECK(isValidConfig(config_), "invalid AdaptiveDepthConfig");
}

uint32_t AdaptiveDepthController::currentDepth() const {
  return currentDepth_;
}

double AdaptiveDepthController::recentThroughputBytesPerSecond() const {
  return recentThroughputBytesPerSecond_;
}

void AdaptiveDepthController::onWindow(
    double throughputBytesPerSecond,
    bool hasBacklog) {
  if (std::isfinite(throughputBytesPerSecond) &&
      throughputBytesPerSecond >= 0) {
    recentThroughputBytesPerSecond_ = throughputBytesPerSecond;
  }

  if (!config_.enabled || !hasBacklog) {
    return;
  }

  if (!isValidThroughput(throughputBytesPerSecond)) {
    if (measuringProbeDepth_) {
      currentDepth_ = bestDepth_;
      measuringProbeDepth_ = false;
    }
    return;
  }

  if (!hasBestThroughput_) {
    bestThroughputBytesPerSecond_ = throughputBytesPerSecond;
    bestDepth_ = currentDepth_;
    hasBestThroughput_ = true;
    scheduleProbe();
    return;
  }

  if (throughputImproved(throughputBytesPerSecond)) {
    bestThroughputBytesPerSecond_ = throughputBytesPerSecond;
    bestDepth_ = currentDepth_;
    scheduleProbe();
    return;
  }

  if (!measuringProbeDepth_) {
    scheduleProbe();
    return;
  }

  currentDepth_ = bestDepth_;
  measuringProbeDepth_ = false;
}

bool AdaptiveDepthController::isValidConfig(
    const AdaptiveDepthConfig& config) {
  return config.minDepth > 0 && config.increaseStep > 0 &&
      config.minDepth <= config.initialDepth &&
      config.initialDepth <= config.maxDepth &&
      config.controlInterval.count() > 0 && config.minThroughputGain >= 0;
}

bool AdaptiveDepthController::isValidThroughput(
    double throughputBytesPerSecond) {
  return std::isfinite(throughputBytesPerSecond) &&
      throughputBytesPerSecond > 0;
}

bool AdaptiveDepthController::throughputImproved(
    double throughputBytesPerSecond) const {
  return throughputBytesPerSecond >=
      bestThroughputBytesPerSecond_ * (1.0 + config_.minThroughputGain);
}

void AdaptiveDepthController::scheduleProbe() {
  const auto availableIncrease = config_.maxDepth - currentDepth_;
  const auto nextDepth =
      currentDepth_ + std::min(config_.increaseStep, availableIncrease);
  measuringProbeDepth_ = nextDepth != currentDepth_;
  currentDepth_ = nextDepth;
}

} // namespace bytedance::bolt::memory::bm
