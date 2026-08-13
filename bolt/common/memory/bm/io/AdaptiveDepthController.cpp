#include "bolt/common/memory/bm/io/AdaptiveDepthController.h"

#include "bolt/common/base/Exceptions.h"

#include <algorithm>
#include <cmath>
#include <memory>

namespace bytedance::bolt::memory::bm {
namespace {

constexpr uint32_t kRequiredPressureWindows = 2;
constexpr uint32_t kProbeCooldownWindowsAfterRollback = 1;

} // namespace

AdaptiveDepthController::AdaptiveDepthController(AdaptiveDepthConfig config)
    : config_(config),
      currentDepth_(config.initialDepth),
      bestDepth_(config.initialDepth),
      windowStart_(std::chrono::steady_clock::now()) {
  BOLT_CHECK(isValidConfig(config_), "invalid AdaptiveDepthConfig");
}

uint32_t AdaptiveDepthController::currentDepth() const {
  return currentDepth_;
}

double AdaptiveDepthController::recentThroughputBytesPerSecond() const {
  return recentThroughputBytesPerSecond_;
}

DepthControlStatsPtr AdaptiveDepthController::stats() const {
  return std::make_shared<AdaptiveDepthStats>(
      currentDepth_,
      recentThroughputBytesPerSecond_,
      completedWindows_,
      lastWindowThroughputBytesPerSecond_,
      bestDepth_,
      bestThroughputBytesPerSecond_,
      measuringProbeDepth_);
}

void AdaptiveDepthController::onCompletion(
    uint64_t completedBytes,
    bool hasBacklog,
    std::chrono::steady_clock::time_point now) {
  windowCompletedBytes_ += completedBytes;
  const auto elapsed = now - windowStart_;
  if (elapsed < config_.controlInterval) {
    return;
  }

  const auto seconds = std::chrono::duration<double>(elapsed).count();
  const auto throughputBytesPerSecond =
      seconds > 0 ? static_cast<double>(windowCompletedBytes_) / seconds : 0;
  onWindow(throughputBytesPerSecond, hasBacklog);
  windowCompletedBytes_ = 0;
  windowStart_ = now;
}

void AdaptiveDepthController::onWindow(
    double throughputBytesPerSecond,
    bool hasBacklog) {
  ++completedWindows_;
  if (std::isfinite(throughputBytesPerSecond) &&
      throughputBytesPerSecond >= 0) {
    lastWindowThroughputBytesPerSecond_ = throughputBytesPerSecond;
  }
  const auto validSample =
      std::isfinite(throughputBytesPerSecond) && throughputBytesPerSecond >= 0;
  if (validSample) {
    if (!hasRecentThroughput_) {
      recentThroughputBytesPerSecond_ = throughputBytesPerSecond;
      hasRecentThroughput_ = true;
    } else {
      const auto alpha = config_.throughputSmoothingFactor;
      recentThroughputBytesPerSecond_ = alpha * throughputBytesPerSecond +
          (1.0 - alpha) * recentThroughputBytesPerSecond_;
    }
  }

  if (!hasBacklog) {
    validPressureWindows_ = 0;
    return;
  }

  if (!validSample || !isValidThroughput(throughputBytesPerSecond)) {
    validPressureWindows_ = 0;
    return;
  }

  ++validPressureWindows_;

  if (probeCooldownWindows_ > 0) {
    --probeCooldownWindows_;
    return;
  }

  if (validPressureWindows_ < kRequiredPressureWindows) {
    return;
  }

  if (!hasBestThroughput_) {
    bestThroughputBytesPerSecond_ = recentThroughputBytesPerSecond_;
    bestDepth_ = currentDepth_;
    hasBestThroughput_ = true;
    scheduleProbe();
    return;
  }

  if (throughputImproved(recentThroughputBytesPerSecond_)) {
    bestThroughputBytesPerSecond_ = recentThroughputBytesPerSecond_;
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
  probeCooldownWindows_ = kProbeCooldownWindowsAfterRollback;
  validPressureWindows_ = 0;
}

bool AdaptiveDepthController::isValidConfig(const AdaptiveDepthConfig& config) {
  return config.minDepth > 0 && config.increaseStep > 0 &&
      config.minDepth <= config.initialDepth &&
      config.initialDepth <= config.maxDepth &&
      config.controlInterval.count() > 0 && config.minThroughputGain >= 0 &&
      config.throughputSmoothingFactor > 0 &&
      config.throughputSmoothingFactor <= 1;
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
