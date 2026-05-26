#include "bolt/common/memory/bm/AdaptiveDepthController.h"

#include "bolt/common/memory/bm/DiskIoTypes.h"

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
