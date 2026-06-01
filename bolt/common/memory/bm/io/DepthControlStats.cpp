#include "bolt/common/memory/bm/io/DepthControlStats.h"

#include <sstream>

namespace bytedance::bolt::memory::bm {

std::string DepthControlStats::toString() const {
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

void FixedDepthStats::appendFields(std::ostringstream& out) const {
  out << " depth_fixed_configured_depth=" << configuredDepth;
}

void AdaptiveDepthStats::appendFields(std::ostringstream& out) const {
  out << " depth_adaptive_best_depth=" << bestDepth
      << " depth_adaptive_best_throughput_bytes_per_second="
      << bestThroughputBytesPerSecond
      << " depth_adaptive_measuring_probe_depth=" << measuringProbeDepth;
}

} // namespace bytedance::bolt::memory::bm
