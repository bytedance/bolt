#include "bolt/common/memory/bm/AdaptiveDepthController.h"

#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/memory/bm/DiskIoTypes.h"

#include <limits>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

namespace {

AdaptiveDepthConfig makeConfig() {
  AdaptiveDepthConfig config;
  config.initialDepth = 8;
  config.maxDepth = 16;
  config.increaseStep = 4;
  config.minThroughputGain = 0.10;
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

  controller.onWindow(-1.0, true);

  EXPECT_EQ(8, controller.currentDepth());
  EXPECT_EQ(-1.0, controller.recentThroughputBytesPerSecond());
}

TEST(AdaptiveDepthControllerTest, nonFiniteThroughputDoesNotIncreaseDepth) {
  auto config = makeConfig();
  AdaptiveDepthController controller(config);

  controller.onWindow(1000.0, false);
  controller.onWindow(std::numeric_limits<double>::infinity(), true);

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
}
