#include "bolt/common/memory/bm/file/FileSegmentAllocatorImpl.h"
#include "bolt/common/memory/bm/file/BucketSegmentAllocator.h"
#include "bolt/common/memory/bm/file/DedicatedFileAllocator.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"

#include <filesystem>
#include <fstream>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;
using namespace bytedance::bolt::memory::bm::test;

TEST(FileSegmentAllocatorImplTest, PreservesExistingBaseDirectoryContents) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-existing");
  std::filesystem::remove_all(directory);
  std::filesystem::create_directories(directory);
  {
    std::ofstream old_file(directory + "/old-file");
    old_file << "stale";
  }

  FileSegmentAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  EXPECT_TRUE(std::filesystem::exists(directory));
  EXPECT_TRUE(std::filesystem::exists(directory + "/old-file"));
  EXPECT_FALSE(OnlyAllocatorDirectory(directory).empty());
}

TEST(FileSegmentAllocatorImplTest, DoesNotCreateBucketFilesDuringInit) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-lazy-init");
  std::filesystem::remove_all(directory);

  FileSegmentAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  const auto allocator_directory = OnlyAllocatorDirectory(directory);
  ASSERT_FALSE(allocator_directory.empty());
  EXPECT_TRUE(std::filesystem::is_empty(allocator_directory));
}

TEST(FileSegmentAllocatorImplTest, AllocatesRequestToFirstFittingBucket) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-bucket-fit");
  std::filesystem::remove_all(directory);
  FileSegmentAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  auto result = allocator.Allocate(6 * 1024);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(6 * 1024, result.segment.requested_size);
  EXPECT_EQ(8 * 1024, result.segment.allocated_size);
  EXPECT_EQ(FileSegmentKind::kBucket, result.segment.kind);
  EXPECT_EQ(0, result.segment.offset);
  EXPECT_GE(result.segment.fd, 0);
}

TEST(FileSegmentAllocatorImplTest, AllocatesSequentialOffsetsInSameBucket) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-sequential");
  std::filesystem::remove_all(directory);
  FileSegmentAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  auto first = allocator.Allocate(4 * 1024);
  auto second = allocator.Allocate(4 * 1024);

  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  EXPECT_EQ(first.segment.fd, second.segment.fd);
  EXPECT_EQ(0, first.segment.offset);
  EXPECT_EQ(4 * 1024, second.segment.offset);
}

TEST(FileSegmentAllocatorImplTest, CreatesNextBucketFileWhenCurrentFileIsFull) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-rollover");
  std::filesystem::remove_all(directory);
  auto config = ValidConfigWithDirectory(directory);
  config.bucket_sizes = {4 * 1024};
  config.file_size_limit_bytes = 8 * 1024;
  config.max_open_files_per_bucket = 2;
  FileSegmentAllocatorImpl allocator(config);

  auto first = allocator.Allocate(4 * 1024);
  auto second = allocator.Allocate(4 * 1024);
  auto third = allocator.Allocate(4 * 1024);

  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(third.ok());
  EXPECT_EQ(first.segment.fd, second.segment.fd);
  EXPECT_NE(first.segment.fd, third.segment.fd);
  EXPECT_EQ(0, third.segment.offset);
}

TEST(FileSegmentAllocatorImplTest, RespectsMaxOpenFilesPerBucket) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-open-limit");
  std::filesystem::remove_all(directory);
  auto config = ValidConfigWithDirectory(directory);
  config.bucket_sizes = {4 * 1024};
  config.file_size_limit_bytes = 4 * 1024;
  config.max_open_files_per_bucket = 1;
  FileSegmentAllocatorImpl allocator(config);

  auto first = allocator.Allocate(4 * 1024);
  auto second = allocator.Allocate(4 * 1024);

  ASSERT_TRUE(first.ok());
  EXPECT_EQ(FileErrorCode::kTooManyOpenFiles, second.error);
}

TEST(FileSegmentAllocatorImplTest, AllocatesDedicatedFileForLargeRequest) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-dedicated");
  std::filesystem::remove_all(directory);
  FileSegmentAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  auto result = allocator.Allocate(128 * 1024);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(FileSegmentKind::kDedicated, result.segment.kind);
  EXPECT_EQ(0, result.segment.offset);
  EXPECT_EQ(128 * 1024, result.segment.requested_size);
  EXPECT_EQ(128 * 1024, result.segment.allocated_size);
  EXPECT_GE(result.segment.fd, 0);
}

TEST(FileSegmentAllocatorImplTest, RejectsInvalidSizesAndUnknownFreeSegments) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-invalid");
  std::filesystem::remove_all(directory);
  FileSegmentAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  EXPECT_EQ(FileErrorCode::kInvalidSize, allocator.Allocate(0).error);
  EXPECT_EQ(FileErrorCode::kInvalidSize, allocator.Allocate(-1).error);

  FileSegment unknown;
  unknown.id = 999;
  unknown.kind = FileSegmentKind::kBucket;
  EXPECT_EQ(FileErrorCode::kDoubleFree, allocator.Free(unknown).error);
}

TEST(FileSegmentAllocatorImplTest, ReportsCreateFileErrors) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-create-error");
  std::filesystem::remove_all(directory);
  std::ofstream file(directory);
  file << "not a directory";
  file.close();

  auto config = ValidConfigWithDirectory(directory);
  EXPECT_THROW((void)FileSegmentAllocatorImpl(config), std::exception);
}

TEST(FileSegmentAllocatorImplTest, BucketAllocatorReportsCreateFileErrors) {
  const auto directory = UniqueTempDir("bolt-bm-bucket-create-error");
  std::filesystem::remove_all(directory);
  std::ofstream file(directory);
  file << "not a directory";
  file.close();

  BucketSegmentAllocator allocator(directory, 4096, 4096, 1);
  auto allocation = allocator.Allocate(4096, 1);

  EXPECT_FALSE(allocation.result.ok());
  EXPECT_EQ(FileErrorCode::kIoError, allocation.result.error);
}

TEST(FileSegmentAllocatorImplTest, DedicatedAllocatorReportsCreateAndFreeErrors) {
  const auto directory = UniqueTempDir("bolt-bm-dedicated-create-error");
  std::filesystem::remove_all(directory);
  std::ofstream file(directory);
  file << "not a directory";
  file.close();

  DedicatedFileAllocator allocator(directory);
  auto allocation = allocator.Allocate(128 * 1024, 7);

  EXPECT_FALSE(allocation.result.ok());
  EXPECT_EQ(FileErrorCode::kIoError, allocation.result.error);

  SegmentRecord unknown;
  unknown.segment.id = 999;
  EXPECT_EQ(FileErrorCode::kInvalidSegment, allocator.Free(unknown).error);
}

TEST(FileSegmentAllocatorImplTest, ResultOkReflectsErrorCode) {
  FileAllocateResult allocateResult;
  EXPECT_TRUE(allocateResult.ok());
  allocateResult.error = FileErrorCode::kInvalidConfig;
  EXPECT_FALSE(allocateResult.ok());
  allocateResult.error = FileErrorCode::kInvalidSize;
  EXPECT_FALSE(allocateResult.ok());
  allocateResult.error = FileErrorCode::kInvalidSegment;
  EXPECT_FALSE(allocateResult.ok());
  allocateResult.error = FileErrorCode::kDoubleFree;
  EXPECT_FALSE(allocateResult.ok());
  allocateResult.error = FileErrorCode::kTooManyOpenFiles;
  EXPECT_FALSE(allocateResult.ok());
  allocateResult.error = FileErrorCode::kIoError;
  EXPECT_FALSE(allocateResult.ok());
  allocateResult.error = FileErrorCode::kShutdown;
  EXPECT_FALSE(allocateResult.ok());

  FileFreeResult freeResult;
  EXPECT_TRUE(freeResult.ok());
  freeResult.error = FileErrorCode::kInvalidConfig;
  EXPECT_FALSE(freeResult.ok());
  freeResult.error = FileErrorCode::kInvalidSize;
  EXPECT_FALSE(freeResult.ok());
  freeResult.error = FileErrorCode::kInvalidSegment;
  EXPECT_FALSE(freeResult.ok());
  freeResult.error = FileErrorCode::kDoubleFree;
  EXPECT_FALSE(freeResult.ok());
  freeResult.error = FileErrorCode::kTooManyOpenFiles;
  EXPECT_FALSE(freeResult.ok());
  freeResult.error = FileErrorCode::kIoError;
  EXPECT_FALSE(freeResult.ok());
  freeResult.error = FileErrorCode::kShutdown;
  EXPECT_FALSE(freeResult.ok());
}
