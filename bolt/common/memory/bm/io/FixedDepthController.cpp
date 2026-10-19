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

#include "bolt/common/memory/bm/io/FixedDepthController.h"

#include "bolt/common/base/Exceptions.h"

#include <cmath>
#include <memory>

namespace bytedance::bolt::memory::bm {

FixedDepthController::FixedDepthController(FixedDepthConfig config)
    : config_(config), windowStart_(std::chrono::steady_clock::now()) {
  BOLT_CHECK(config_.depth > 0, "invalid fixed depth");
  BOLT_CHECK(config_.statsWindow.count() > 0, "invalid stats window");
}

uint32_t FixedDepthController::currentDepth() const {
  return config_.depth;
}

double FixedDepthController::recentThroughputBytesPerSecond() const {
  return recentThroughputBytesPerSecond_;
}

DepthControlStatsPtr FixedDepthController::stats() const {
  return std::make_shared<FixedDepthStats>(
      config_.depth,
      recentThroughputBytesPerSecond_,
      completedWindows_,
      lastWindowThroughputBytesPerSecond_,
      config_.depth);
}

void FixedDepthController::onCompletion(
    uint64_t completedBytes,
    bool /*hasQueuedRequests*/,
    std::chrono::steady_clock::time_point now) {
  windowCompletedBytes_ += completedBytes;
  const auto elapsed = now - windowStart_;
  if (elapsed < config_.statsWindow) {
    return;
  }

  const auto seconds = std::chrono::duration<double>(elapsed).count();
  const auto throughputBytesPerSecond =
      seconds > 0 ? static_cast<double>(windowCompletedBytes_) / seconds : 0;
  if (std::isfinite(throughputBytesPerSecond) &&
      throughputBytesPerSecond >= 0) {
    recentThroughputBytesPerSecond_ = throughputBytesPerSecond;
    lastWindowThroughputBytesPerSecond_ = throughputBytesPerSecond;
  }
  ++completedWindows_;
  windowCompletedBytes_ = 0;
  windowStart_ = now;
}

} // namespace bytedance::bolt::memory::bm
