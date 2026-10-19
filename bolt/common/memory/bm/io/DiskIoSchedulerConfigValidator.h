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

#pragma once

#include "bolt/common/memory/bm/io/DiskIoSchedulerConfig.h"
#include "bolt/common/memory/bm/io/IoResult.h"

namespace bytedance::bolt::memory::bm {

inline IoErrorCode validateDiskIoSchedulerConfig(
    const DiskIoSchedulerConfig& config) {
  if (config.ringDepth == 0) {
    return IoErrorCode::InvalidRequest;
  }
  for (const auto weight : config.priorityWeights) {
    if (weight == 0) {
      return IoErrorCode::InvalidRequest;
    }
  }

  switch (config.depthControl.mode) {
    case DepthControlMode::Fixed: {
      const auto& fixed = config.depthControl.fixed;
      if (fixed.depth == 0 || fixed.depth > config.ringDepth ||
          fixed.statsWindow.count() <= 0) {
        return IoErrorCode::InvalidRequest;
      }
      break;
    }
    case DepthControlMode::Adaptive: {
      const auto& adaptive = config.depthControl.adaptive;
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
      break;
    }
  }
  return IoErrorCode::Ok;
}

} // namespace bytedance::bolt::memory::bm
