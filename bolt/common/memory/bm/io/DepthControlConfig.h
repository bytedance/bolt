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
  uint32_t depth{64};
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
