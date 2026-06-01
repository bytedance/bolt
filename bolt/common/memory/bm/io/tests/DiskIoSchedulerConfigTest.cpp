#include "bolt/common/memory/bm/io/DiskIoSchedulerConfig.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerConfigValidator.h"

#include <chrono>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

TEST(DiskIoSchedulerConfigValidatorTest, validateConfigRejectsInvalidDepth) {
  DiskIoSchedulerConfig config;
  config.ringDepth = 16;
  config.depthControl.mode = DepthControlMode::Fixed;
  config.depthControl.fixed.depth = 32;

  EXPECT_EQ(
      IoErrorCode::InvalidRequest,
      validateDiskIoSchedulerConfig(config));
}

TEST(DiskIoSchedulerConfigTest, defaultConfigUsesFixedDepth) {
  DiskIoSchedulerConfig config;

  EXPECT_EQ(DepthControlMode::Fixed, config.depthControl.mode);
  EXPECT_EQ(128, config.depthControl.fixed.depth);
}

TEST(DiskIoSchedulerConfigValidatorTest, validatesOnlySelectedDepthMode) {
  DiskIoSchedulerConfig config;
  config.depthControl.mode = DepthControlMode::Fixed;
  config.depthControl.fixed.depth = 8;
  config.depthControl.adaptive.minDepth = 0;

  EXPECT_EQ(IoErrorCode::Ok, validateDiskIoSchedulerConfig(config));
}

TEST(DiskIoSchedulerConfigValidatorTest, rejectsNonPositiveStatsLogInterval) {
  DiskIoSchedulerConfig config;
  config.enableStatsLogging = true;
  config.statsLogInterval = std::chrono::milliseconds(0);

  EXPECT_EQ(
      IoErrorCode::InvalidRequest,
      validateDiskIoSchedulerConfig(config));
}
