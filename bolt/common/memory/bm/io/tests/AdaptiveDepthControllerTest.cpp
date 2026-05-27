#include "bolt/common/memory/bm/io/AdaptiveDepthController.h"

#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/memory/bm/io/AdaptiveDepthConfig.h"

#include <limits>
#include <chrono>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

namespace {

AdaptiveDepthConfig makeConfig() {
  AdaptiveDepthConfig config;
  config.initialDepth = 8;
  config.maxDepth = 16;
  config.increaseStep = 4;
  config.minThroughputGain = 0.10;
  config.throughputSmoothingFactor = 1.0;
  return config;
}

} // namespace

TEST(AdaptiveDepthControllerTest, increasesDepthWhenThroughputImproves) {
  auto config = makeConfig();
  AdaptiveDepthController controller(config);

  EXPECT_EQ(8, controller.currentDepth());

  controller.onWindow(1000.0, true);
  EXPECT_EQ(1000.0, controller.recentThroughputBytesPerSecond());
  EXPECT_EQ(12, controller.currentDepth());

  controller.onWindow(1150.0, true);
  EXPECT_EQ(1150.0, controller.recentThroughputBytesPerSecond());
  EXPECT_EQ(16, controller.currentDepth());
}

TEST(AdaptiveDepthControllerTest, rollsBackWhenThroughputDoesNotImprove) {
  auto config = makeConfig();
  AdaptiveDepthController controller(config);

  controller.onWindow(1000.0, true);
  EXPECT_EQ(12, controller.currentDepth());

  controller.onWindow(1050.0, true);
  EXPECT_EQ(1050.0, controller.recentThroughputBytesPerSecond());
  EXPECT_EQ(8, controller.currentDepth());
}

TEST(AdaptiveDepthControllerTest, resumesProbingAfterRollback) {
  auto config = makeConfig();
  AdaptiveDepthController controller(config);

  controller.onWindow(1000.0, true);
  EXPECT_EQ(12, controller.currentDepth());

  controller.onWindow(1050.0, true);
  EXPECT_EQ(8, controller.currentDepth());

  controller.onWindow(1000.0, true);
  EXPECT_EQ(12, controller.currentDepth());
}

TEST(AdaptiveDepthControllerTest, disabledControllerOnlyUpdatesRecentThroughput) {
  auto config = makeConfig();
  config.enabled = false;
  AdaptiveDepthController controller(config);

  controller.onWindow(1000.0, true);

  EXPECT_EQ(8, controller.currentDepth());
  EXPECT_EQ(1000.0, controller.recentThroughputBytesPerSecond());
}

TEST(AdaptiveDepthControllerTest, completionWindowComputesSmoothedThroughput) {
  auto config = makeConfig();
  config.controlInterval = std::chrono::milliseconds(10);
  config.throughputSmoothingFactor = 0.25;
  AdaptiveDepthController controller(config);
  const auto start = std::chrono::steady_clock::now();

  controller.onCompletion(1000, true, start);
  EXPECT_EQ(0.0, controller.recentThroughputBytesPerSecond());
  EXPECT_EQ(8, controller.currentDepth());

  controller.onCompletion(1000, true, start + std::chrono::milliseconds(10));
  EXPECT_GT(controller.recentThroughputBytesPerSecond(), 100000.0);
  EXPECT_EQ(12, controller.currentDepth());

  const auto firstThroughput = controller.recentThroughputBytesPerSecond();
  controller.onCompletion(0, true, start + std::chrono::milliseconds(20));
  EXPECT_LT(controller.recentThroughputBytesPerSecond(), firstThroughput);
  EXPECT_GT(controller.recentThroughputBytesPerSecond(), 0.0);
}

TEST(AdaptiveDepthControllerTest, noBacklogOnlyUpdatesRecentThroughput) {
  auto config = makeConfig();
  AdaptiveDepthController controller(config);

  controller.onWindow(1000.0, false);

  EXPECT_EQ(8, controller.currentDepth());
  EXPECT_EQ(1000.0, controller.recentThroughputBytesPerSecond());
}

TEST(AdaptiveDepthControllerTest, clampsIncreaseToMaxDepth) {
  auto config = makeConfig();
  config.initialDepth = 14;
  config.maxDepth = 16;
  AdaptiveDepthController controller(config);

  controller.onWindow(1000.0, true);

  EXPECT_EQ(16, controller.currentDepth());
}

TEST(AdaptiveDepthControllerTest, repeatedZeroThroughputDoesNotIncreaseDepth) {
  auto config = makeConfig();
  AdaptiveDepthController controller(config);

  controller.onWindow(0.0, true);
  controller.onWindow(0.0, true);
  controller.onWindow(0.0, true);

  EXPECT_EQ(8, controller.currentDepth());
  EXPECT_EQ(0.0, controller.recentThroughputBytesPerSecond());
}

TEST(AdaptiveDepthControllerTest, negativeThroughputDoesNotIncreaseDepth) {
  auto config = makeConfig();
  AdaptiveDepthController controller(config);

  controller.onWindow(1000.0, false);
  controller.onWindow(-1.0, true);

  EXPECT_EQ(8, controller.currentDepth());
  EXPECT_EQ(1000.0, controller.recentThroughputBytesPerSecond());
}

TEST(AdaptiveDepthControllerTest, nonFiniteThroughputDoesNotIncreaseDepth) {
  auto config = makeConfig();
  AdaptiveDepthController controller(config);

  controller.onWindow(1000.0, true);
  EXPECT_EQ(12, controller.currentDepth());

  controller.onWindow(std::numeric_limits<double>::infinity(), true);

  EXPECT_EQ(8, controller.currentDepth());
  EXPECT_EQ(1000.0, controller.recentThroughputBytesPerSecond());
}

TEST(AdaptiveDepthControllerTest, nanThroughputDoesNotIncreaseDepth) {
  auto config = makeConfig();
  AdaptiveDepthController controller(config);

  controller.onWindow(1000.0, true);
  EXPECT_EQ(12, controller.currentDepth());

  controller.onWindow(std::numeric_limits<double>::quiet_NaN(), true);

  EXPECT_EQ(8, controller.currentDepth());
  EXPECT_EQ(1000.0, controller.recentThroughputBytesPerSecond());
}

TEST(AdaptiveDepthControllerTest, invalidStandaloneConfigThrows) {
  auto config = makeConfig();
  config.minDepth = 0;
  BOLT_ASSERT_THROW(
      [&] { AdaptiveDepthController controller(config); }(),
      "invalid AdaptiveDepthConfig");

  config = makeConfig();
  config.increaseStep = 0;
  BOLT_ASSERT_THROW(
      [&] { AdaptiveDepthController controller(config); }(),
      "invalid AdaptiveDepthConfig");

  config = makeConfig();
  config.initialDepth = config.minDepth - 1;
  BOLT_ASSERT_THROW(
      [&] { AdaptiveDepthController controller(config); }(),
      "invalid AdaptiveDepthConfig");

  config = makeConfig();
  config.initialDepth = config.maxDepth + 1;
  BOLT_ASSERT_THROW(
      [&] { AdaptiveDepthController controller(config); }(),
      "invalid AdaptiveDepthConfig");

  config = makeConfig();
  config.minThroughputGain = -0.1;
  BOLT_ASSERT_THROW(
      [&] { AdaptiveDepthController controller(config); }(),
      "invalid AdaptiveDepthConfig");

  config = makeConfig();
  config.controlInterval = std::chrono::milliseconds(0);
  BOLT_ASSERT_THROW(
      [&] { AdaptiveDepthController controller(config); }(),
      "invalid AdaptiveDepthConfig");

  config = makeConfig();
  config.controlInterval = std::chrono::milliseconds(-1);
  BOLT_ASSERT_THROW(
      [&] { AdaptiveDepthController controller(config); }(),
      "invalid AdaptiveDepthConfig");
}
