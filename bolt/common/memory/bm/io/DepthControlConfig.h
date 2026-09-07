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

#include "bolt/common/memory/bm/io/AdaptiveDepthConfig.h"

namespace bytedance::bolt::memory::bm {

enum class DepthControlMode : uint8_t {
  Fixed,
  Adaptive,
};

struct FixedDepthConfig {
  uint32_t depth{128};
  std::chrono::milliseconds statsWindow{200};
};

struct DepthControlConfig {
  DepthControlMode mode{DepthControlMode::Fixed};
  FixedDepthConfig fixed;
  AdaptiveDepthConfig adaptive;
};

inline const char* depthControlModeName(DepthControlMode mode) {
  switch (mode) {
    case DepthControlMode::Fixed:
      return "fixed";
    case DepthControlMode::Adaptive:
      return "adaptive";
  }
  return "unknown";
}

} // namespace bytedance::bolt::memory::bm
