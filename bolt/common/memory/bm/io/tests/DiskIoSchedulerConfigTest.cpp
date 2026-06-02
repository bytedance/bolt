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

  EXPECT_EQ(IoErrorCode::InvalidRequest, validateDiskIoSchedulerConfig(config));
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

  EXPECT_EQ(IoErrorCode::InvalidRequest, validateDiskIoSchedulerConfig(config));
}

TEST(DiskIoSchedulerConfigValidatorTest, rejectsInvalidCommonFields) {
  DiskIoSchedulerConfig config;
  EXPECT_EQ(IoErrorCode::Ok, validateDiskIoSchedulerConfig(config));

  auto invalid = config;
  invalid.ringDepth = 0;
  EXPECT_EQ(
      IoErrorCode::InvalidRequest, validateDiskIoSchedulerConfig(invalid));

  invalid = config;
  invalid.priorityWeights[0] = 0;
  EXPECT_EQ(
      IoErrorCode::InvalidRequest, validateDiskIoSchedulerConfig(invalid));
}

TEST(DiskIoSchedulerConfigValidatorTest, rejectsInvalidFixedDepthFields) {
  DiskIoSchedulerConfig config;
  config.depthControl.mode = DepthControlMode::Fixed;
  config.depthControl.fixed.depth = 8;
  config.depthControl.fixed.statsWindow = std::chrono::milliseconds(1);
  EXPECT_EQ(IoErrorCode::Ok, validateDiskIoSchedulerConfig(config));

  auto invalid = config;
  invalid.depthControl.fixed.depth = 0;
  EXPECT_EQ(
      IoErrorCode::InvalidRequest, validateDiskIoSchedulerConfig(invalid));

  invalid = config;
  invalid.depthControl.fixed.depth = config.ringDepth + 1;
  EXPECT_EQ(
      IoErrorCode::InvalidRequest, validateDiskIoSchedulerConfig(invalid));

  invalid = config;
  invalid.depthControl.fixed.statsWindow = std::chrono::milliseconds(0);
  EXPECT_EQ(
      IoErrorCode::InvalidRequest, validateDiskIoSchedulerConfig(invalid));
}

TEST(DiskIoSchedulerConfigValidatorTest, rejectsInvalidAdaptiveDepthFields) {
  DiskIoSchedulerConfig config;
  config.depthControl.mode = DepthControlMode::Adaptive;
  config.depthControl.adaptive.minDepth = 1;
  config.depthControl.adaptive.initialDepth = 4;
  config.depthControl.adaptive.maxDepth = 8;
  config.depthControl.adaptive.increaseStep = 1;
  config.depthControl.adaptive.controlInterval = std::chrono::milliseconds(1);
  config.depthControl.adaptive.minThroughputGain = 0;
  config.depthControl.adaptive.throughputSmoothingFactor = 1.0;
  EXPECT_EQ(IoErrorCode::Ok, validateDiskIoSchedulerConfig(config));

  auto invalid = config;
  invalid.depthControl.adaptive.minDepth = 0;
  EXPECT_EQ(
      IoErrorCode::InvalidRequest, validateDiskIoSchedulerConfig(invalid));

  invalid = config;
  invalid.depthControl.adaptive.increaseStep = 0;
  EXPECT_EQ(
      IoErrorCode::InvalidRequest, validateDiskIoSchedulerConfig(invalid));

  invalid = config;
  invalid.depthControl.adaptive.minDepth = 5;
  invalid.depthControl.adaptive.initialDepth = 4;
  EXPECT_EQ(
      IoErrorCode::InvalidRequest, validateDiskIoSchedulerConfig(invalid));

  invalid = config;
  invalid.depthControl.adaptive.initialDepth = 9;
  invalid.depthControl.adaptive.maxDepth = 8;
  EXPECT_EQ(
      IoErrorCode::InvalidRequest, validateDiskIoSchedulerConfig(invalid));

  invalid = config;
  invalid.depthControl.adaptive.maxDepth = config.ringDepth + 1;
  EXPECT_EQ(
      IoErrorCode::InvalidRequest, validateDiskIoSchedulerConfig(invalid));

  invalid = config;
  invalid.depthControl.adaptive.controlInterval = std::chrono::milliseconds(0);
  EXPECT_EQ(
      IoErrorCode::InvalidRequest, validateDiskIoSchedulerConfig(invalid));

  invalid = config;
  invalid.depthControl.adaptive.minThroughputGain = -0.01;
  EXPECT_EQ(
      IoErrorCode::InvalidRequest, validateDiskIoSchedulerConfig(invalid));

  invalid = config;
  invalid.depthControl.adaptive.throughputSmoothingFactor = 0;
  EXPECT_EQ(
      IoErrorCode::InvalidRequest, validateDiskIoSchedulerConfig(invalid));

  invalid = config;
  invalid.depthControl.adaptive.throughputSmoothingFactor = 1.01;
  EXPECT_EQ(
      IoErrorCode::InvalidRequest, validateDiskIoSchedulerConfig(invalid));
}
