#include "bolt/common/memory/bm/io/DepthController.h"

namespace bytedance::bolt::memory::bm {

DepthController::DepthController(AdaptiveDepthConfig config)
    : adaptive_(config) {}

uint32_t DepthController::currentDepth() const {
  return adaptive_.currentDepth();
}

double DepthController::recentThroughputBytesPerSecond() const {
  return adaptive_.recentThroughputBytesPerSecond();
}

AdaptiveDepthStats DepthController::stats() const {
  return adaptive_.stats();
}

void DepthController::onCompletion(
    uint64_t bytes,
    bool hasQueuedRequests,
    std::chrono::steady_clock::time_point now) {
  adaptive_.onCompletion(bytes, hasQueuedRequests, now);
}

} // namespace bytedance::bolt::memory::bm
