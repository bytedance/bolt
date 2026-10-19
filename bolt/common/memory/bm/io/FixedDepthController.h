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

#include <chrono>
#include <cstdint>

#include "bolt/common/memory/bm/io/DepthControlConfig.h"
#include "bolt/common/memory/bm/io/DepthController.h"

namespace bytedance::bolt::memory::bm {

class FixedDepthController : public DepthController {
 public:
  explicit FixedDepthController(FixedDepthConfig config);

  uint32_t currentDepth() const override;
  double recentThroughputBytesPerSecond() const override;
  DepthControlStatsPtr stats() const override;
  void onCompletion(
      uint64_t completedBytes,
      bool hasQueuedRequests,
      std::chrono::steady_clock::time_point now) override;

 private:
  FixedDepthConfig config_;
  std::chrono::steady_clock::time_point windowStart_;
  uint64_t windowCompletedBytes_{0};
  double recentThroughputBytesPerSecond_{0};
  uint64_t completedWindows_{0};
  double lastWindowThroughputBytesPerSecond_{0};
};

} // namespace bytedance::bolt::memory::bm
