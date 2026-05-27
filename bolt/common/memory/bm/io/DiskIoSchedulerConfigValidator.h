#pragma once

#include "bolt/common/memory/bm/io/DiskIoSchedulerConfig.h"
#include "bolt/common/memory/bm/io/IoResult.h"

namespace bytedance::bolt::memory::bm {

inline IoErrorCode validateDiskIoSchedulerConfig(
    const DiskIoSchedulerConfig& config) {
  if (config.ringDepth == 0) {
    return IoErrorCode::InvalidRequest;
  }
  if (config.statsLogInterval.count() <= 0 ||
      config.drainTimeout.count() <= 0) {
    return IoErrorCode::InvalidRequest;
  }
  for (const auto weight : config.priorityWeights) {
    if (weight == 0) {
      return IoErrorCode::InvalidRequest;
    }
  }
  const auto& adaptive = config.adaptiveDepth;
  if (adaptive.minDepth == 0 || adaptive.increaseStep == 0) {
    return IoErrorCode::InvalidRequest;
  }
  if (adaptive.minDepth > adaptive.initialDepth ||
      adaptive.initialDepth > adaptive.maxDepth ||
      adaptive.maxDepth > config.ringDepth) {
    return IoErrorCode::InvalidRequest;
  }
  if (adaptive.controlInterval.count() <= 0 ||
      adaptive.minThroughputGain < 0 ||
      adaptive.throughputSmoothingFactor <= 0 ||
      adaptive.throughputSmoothingFactor > 1) {
    return IoErrorCode::InvalidRequest;
  }
  return IoErrorCode::Ok;
}

} // namespace bytedance::bolt::memory::bm
