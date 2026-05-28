#include "bolt/common/memory/bm/file/FileBlockAllocatorConfig.h"
#include "bolt/common/memory/bm/file/tests/FileBlockAllocatorTestUtil.h"

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;
using namespace bytedance::bolt::memory::bm::test;

TEST(FileBlockAllocatorConfigTest, AcceptsValidConfig) {
  EXPECT_EQ(FileErrorCode::kOk, ValidateFileBlockAllocatorConfig(ValidConfig()));
}

TEST(FileBlockAllocatorConfigTest, RejectsEmptyDirectory) {
  auto config = ValidConfig();
  config.directory.clear();
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, RejectsEmptyBuckets) {
  auto config = ValidConfig();
  config.bucket_sizes.clear();
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, RejectsNonIncreasingBuckets) {
  auto config = ValidConfig();
  config.bucket_sizes = {4 * 1024, 16 * 1024, 8 * 1024};
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, RejectsDuplicateBuckets) {
  auto config = ValidConfig();
  config.bucket_sizes = {4 * 1024, 8 * 1024, 8 * 1024};
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, RejectsNonAlignedBucket) {
  auto config = ValidConfig();
  config.bucket_sizes = {4 * 1024, 6 * 1024};
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, RejectsSmallFileLimit) {
  auto config = ValidConfig();
  config.file_size_limit_bytes = 8 * 1024;
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, RejectsZeroOpenFileLimit) {
  auto config = ValidConfig();
  config.max_open_files_per_bucket = 0;
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileBlockAllocatorConfig(config));
}
