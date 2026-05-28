#include "bolt/common/memory/bm/io/DepthController.h"
#include "bolt/common/memory/bm/io/FixedDepthController.h"

#include <chrono>
#include <memory>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

TEST(FixedDepthControllerTest, keepsInitialDepthAndTracksThroughput) {
  FixedDepthConfig config;
  config.depth = 8;
  config.statsWindow = std::chrono::milliseconds(1);

  FixedDepthController controller(config);
  const auto now = std::chrono::steady_clock::now();

  controller.onCompletion(4096, true, now);
  controller.onCompletion(
      4096, true, now + config.statsWindow + std::chrono::milliseconds(1));

  EXPECT_EQ(8, controller.currentDepth());
  auto stats = controller.stats();
  EXPECT_EQ(DepthControlMode::Fixed, stats->mode);
  const auto* fixedStats = dynamic_cast<const FixedDepthStats*>(stats.get());
  ASSERT_NE(nullptr, fixedStats);
  EXPECT_EQ(8, fixedStats->configuredDepth);
  EXPECT_GT(controller.recentThroughputBytesPerSecond(), 0);
}

TEST(DepthControllerFactoryTest, createsFixedControllerWhenAdaptiveDisabled) {
  DepthControlConfig config;
  config.mode = DepthControlMode::Fixed;
  config.fixed.depth = 8;

  std::unique_ptr<DepthController> controller = createDepthController(config);

  ASSERT_NE(nullptr, controller);
  EXPECT_EQ(8, controller->currentDepth());
  auto stats = controller->stats();
  EXPECT_EQ(DepthControlMode::Fixed, stats->mode);
  EXPECT_NE(nullptr, dynamic_cast<const FixedDepthStats*>(stats.get()));
}

TEST(DepthControllerFactoryTest, createsAdaptiveControllerWhenAdaptiveEnabled) {
  DepthControlConfig config;
  config.mode = DepthControlMode::Adaptive;
  config.adaptive.initialDepth = 1;
  config.adaptive.maxDepth = 4;
  config.adaptive.increaseStep = 1;
  config.adaptive.minThroughputGain = 0;
  config.adaptive.controlInterval = std::chrono::milliseconds(1);
  config.adaptive.throughputSmoothingFactor = 1.0;

  std::unique_ptr<DepthController> controller = createDepthController(config);
  const auto now = std::chrono::steady_clock::now();

  controller->onCompletion(4096, true, now);
  controller->onCompletion(
      4096,
      true,
      now + config.adaptive.controlInterval + std::chrono::milliseconds(1));
  controller->onCompletion(
      4096,
      true,
      now + config.adaptive.controlInterval * 2 + std::chrono::milliseconds(2));

  EXPECT_GT(controller->currentDepth(), 1);
  auto stats = controller->stats();
  EXPECT_EQ(DepthControlMode::Adaptive, stats->mode);
  EXPECT_NE(nullptr, dynamic_cast<const AdaptiveDepthStats*>(stats.get()));
}
