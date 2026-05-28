#include "bolt/common/memory/bm/io/DepthController.h"

#include "bolt/common/memory/bm/io/AdaptiveDepthController.h"
#include "bolt/common/memory/bm/io/FixedDepthController.h"

namespace bytedance::bolt::memory::bm {

std::unique_ptr<DepthController> createDepthController(
    const DepthControlConfig& config) {
  if (config.mode == DepthControlMode::Adaptive) {
    return std::make_unique<AdaptiveDepthController>(config.adaptive);
  }
  return std::make_unique<FixedDepthController>(config.fixed);
}

} // namespace bytedance::bolt::memory::bm
