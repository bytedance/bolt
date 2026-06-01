#pragma once

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>

#include "bolt/common/memory/bm/io/DepthControlConfig.h"

namespace bytedance::bolt::memory::bm {

struct DepthControlStats {
  DepthControlStats(
      DepthControlMode mode,
      uint32_t currentDepth,
      double recentThroughputBytesPerSecond,
      uint64_t completedWindows,
      double lastWindowThroughputBytesPerSecond)
      : mode(mode),
        currentDepth(currentDepth),
        recentThroughputBytesPerSecond(recentThroughputBytesPerSecond),
        completedWindows(completedWindows),
        lastWindowThroughputBytesPerSecond(lastWindowThroughputBytesPerSecond) {
  }
  virtual ~DepthControlStats() = default;

  std::string toString() const;

  DepthControlMode mode;
  uint32_t currentDepth{0};
  double recentThroughputBytesPerSecond{0};
  uint64_t completedWindows{0};
  double lastWindowThroughputBytesPerSecond{0};

 private:
  virtual void appendFields(std::ostringstream& out) const = 0;
};

using DepthControlStatsPtr = std::shared_ptr<const DepthControlStats>;

struct FixedDepthStats final : public DepthControlStats {
  FixedDepthStats(
      uint32_t currentDepth,
      double recentThroughputBytesPerSecond,
      uint64_t completedWindows,
      double lastWindowThroughputBytesPerSecond,
      uint32_t configuredDepth)
      : DepthControlStats(
            DepthControlMode::Fixed,
            currentDepth,
            recentThroughputBytesPerSecond,
            completedWindows,
            lastWindowThroughputBytesPerSecond),
        configuredDepth(configuredDepth) {}

  uint32_t configuredDepth{0};

 private:
  void appendFields(std::ostringstream& out) const override;
};

struct AdaptiveDepthStats final : public DepthControlStats {
  AdaptiveDepthStats(
      uint32_t currentDepth,
      double recentThroughputBytesPerSecond,
      uint64_t completedWindows,
      double lastWindowThroughputBytesPerSecond,
      uint32_t bestDepth,
      double bestThroughputBytesPerSecond,
      bool measuringProbeDepth)
      : DepthControlStats(
            DepthControlMode::Adaptive,
            currentDepth,
            recentThroughputBytesPerSecond,
            completedWindows,
            lastWindowThroughputBytesPerSecond),
        bestDepth(bestDepth),
        bestThroughputBytesPerSecond(bestThroughputBytesPerSecond),
        measuringProbeDepth(measuringProbeDepth) {}

  uint32_t bestDepth{0};
  double bestThroughputBytesPerSecond{0};
  bool measuringProbeDepth{false};

 private:
  void appendFields(std::ostringstream& out) const override;
};

} // namespace bytedance::bolt::memory::bm
