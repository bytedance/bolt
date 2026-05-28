#include "bolt/common/memory/bm/io/DepthController.h"

#include <chrono>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

TEST(DepthControllerTest, disabledControllerKeepsInitialDepth) {
  AdaptiveDepthConfig config;
  config.enabled = false;
  config.initialDepth = 8;
  config.maxDepth = 16;

  DepthController controller(config);
  const auto now = std::chrono::steady_clock::now();

  controller.onCompletion(4096, true, now);
  controller.onCompletion(
      4096, true, now + config.controlInterval + std::chrono::milliseconds(1));

  EXPECT_EQ(8, controller.currentDepth());
  EXPECT_FALSE(controller.stats().enabled);
}

TEST(DepthControllerTest, enabledControllerDelegatesAdaptiveGrowth) {
  AdaptiveDepthConfig config;
  config.enabled = true;
  config.initialDepth = 1;
  config.maxDepth = 4;
  config.increaseStep = 1;
  config.minThroughputGain = 0;
  config.controlInterval = std::chrono::milliseconds(1);
  config.throughputSmoothingFactor = 1.0;

  DepthController controller(config);
  const auto now = std::chrono::steady_clock::now();

  controller.onCompletion(4096, true, now);
  controller.onCompletion(
      4096, true, now + config.controlInterval + std::chrono::milliseconds(1));
  controller.onCompletion(
      4096,
      true,
      now + config.controlInterval * 2 + std::chrono::milliseconds(2));

  EXPECT_GT(controller.currentDepth(), 1);
  EXPECT_TRUE(controller.stats().enabled);
}
