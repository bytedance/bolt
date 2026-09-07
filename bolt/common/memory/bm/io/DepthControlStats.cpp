/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
