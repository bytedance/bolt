#include "bolt/common/memory/bm/file/FileSegmentAllocatorConfig.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;
using namespace bytedance::bolt::memory::bm::test;

TEST(FileSegmentAllocatorConfigTest, AcceptsValidConfig) {
  EXPECT_EQ(
      FileErrorCode::kOk, ValidateFileSegmentAllocatorConfig(ValidConfig()));
}

TEST(FileSegmentAllocatorConfigTest, DefaultsToBufferedIoMode) {
  FileSegmentAllocatorConfig config;
  EXPECT_EQ(FileIoMode::kBuffered, config.ioMode);
}

TEST(FileSegmentAllocatorConfigTest, AcceptsDirectIoMode) {
  auto config = ValidConfig();
  config.ioMode = FileIoMode::kDirect;
  EXPECT_EQ(FileErrorCode::kOk, ValidateFileSegmentAllocatorConfig(config));
}

TEST(FileSegmentAllocatorConfigTest, RejectsEmptyDirectory) {
  auto config = ValidConfig();
  config.directory.clear();
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileSegmentAllocatorConfig(config));
}

TEST(FileSegmentAllocatorConfigTest, RejectsEmptyBuckets) {
  auto config = ValidConfig();
  config.bucket_sizes.clear();
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileSegmentAllocatorConfig(config));
}

TEST(FileSegmentAllocatorConfigTest, RejectsNonIncreasingBuckets) {
  auto config = ValidConfig();
  config.bucket_sizes = {4 * 1024, 16 * 1024, 8 * 1024};
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileSegmentAllocatorConfig(config));
}

TEST(FileSegmentAllocatorConfigTest, RejectsDuplicateBuckets) {
  auto config = ValidConfig();
  config.bucket_sizes = {4 * 1024, 8 * 1024, 8 * 1024};
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileSegmentAllocatorConfig(config));
}

TEST(FileSegmentAllocatorConfigTest, RejectsNonAlignedBucket) {
  auto config = ValidConfig();
  config.bucket_sizes = {4 * 1024, 6 * 1024};
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileSegmentAllocatorConfig(config));
}

TEST(FileSegmentAllocatorConfigTest, RejectsSmallFileLimit) {
  auto config = ValidConfig();
  config.file_size_limit_bytes = 8 * 1024;
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileSegmentAllocatorConfig(config));
}

TEST(FileSegmentAllocatorConfigTest, RejectsZeroOpenFileLimit) {
  auto config = ValidConfig();
  config.max_open_files_per_bucket = 0;
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileSegmentAllocatorConfig(config));
}
