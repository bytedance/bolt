#include "bolt/common/memory/bm/io/DiskIoSchedulerConfig.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerConfigValidator.h"

#include <chrono>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

TEST(DiskIoSchedulerConfigValidatorTest, validateConfigRejectsInvalidDepth) {
  DiskIoSchedulerConfig config;
  config.ringDepth = 16;
  config.adaptiveDepth.minDepth = 1;
  config.adaptiveDepth.initialDepth = 32;
  config.adaptiveDepth.maxDepth = 32;

  EXPECT_EQ(
      IoErrorCode::InvalidRequest,
      validateDiskIoSchedulerConfig(config));
}

TEST(DiskIoSchedulerConfigTest, defaultConfigUsesFixedDepth) {
  DiskIoSchedulerConfig config;

  EXPECT_FALSE(config.adaptiveDepth.enabled);
  EXPECT_EQ(config.adaptiveDepth.initialDepth, config.adaptiveDepth.maxDepth);
}

TEST(DiskIoSchedulerConfigValidatorTest, rejectsNonPositiveStatsLogInterval) {
  DiskIoSchedulerConfig config;
  config.enableStatsLogging = true;
  config.statsLogInterval = std::chrono::milliseconds(0);

  EXPECT_EQ(
      IoErrorCode::InvalidRequest,
      validateDiskIoSchedulerConfig(config));
}
