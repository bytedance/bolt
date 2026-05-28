#pragma once

#include <cstdint>
#include <memory>
#include <sstream>
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

  std::string toString() const {
    std::ostringstream out;
    out << " depth_control_mode=" << depthControlModeName(mode)
        << " depth_current=" << currentDepth
        << " depth_recent_throughput_bytes_per_second="
        << recentThroughputBytesPerSecond
        << " depth_completed_windows=" << completedWindows
        << " depth_last_window_throughput_bytes_per_second="
        << lastWindowThroughputBytesPerSecond;
    appendFields(out);
    return out.str();
  }

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
  void appendFields(std::ostringstream& out) const override {
    out << " depth_fixed_configured_depth=" << configuredDepth;
  }
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
  void appendFields(std::ostringstream& out) const override {
    out << " depth_adaptive_best_depth=" << bestDepth
        << " depth_adaptive_best_throughput_bytes_per_second="
        << bestThroughputBytesPerSecond
        << " depth_adaptive_measuring_probe_depth=" << measuringProbeDepth;
  }
};

} // namespace bytedance::bolt::memory::bm
