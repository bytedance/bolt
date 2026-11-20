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
#include <memory>

#include "bolt/common/memory/bm/io/DepthControlConfig.h"
#include "bolt/common/memory/bm/io/DepthControlStats.h"

namespace bytedance::bolt::memory::bm {

class DepthController {
 public:
  virtual ~DepthController() = default;

  virtual uint32_t currentDepth() const = 0;
  virtual double recentThroughputBytesPerSecond() const = 0;
  virtual DepthControlStatsPtr stats() const = 0;
  virtual void onCompletion(
      uint64_t bytes,
      bool hasQueuedRequests,
      std::chrono::steady_clock::time_point now) = 0;
};

std::unique_ptr<DepthController> createDepthController(
    const DepthControlConfig& config);

} // namespace bytedance::bolt::memory::bm
