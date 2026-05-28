#include "bolt/common/memory/bm/file/FileBlockAllocatorImpl.h"
#include "bolt/common/memory/bm/file/tests/FileBlockAllocatorTestUtil.h"

#include <filesystem>
#include <fstream>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;
using namespace bytedance::bolt::memory::bm::test;

TEST(FileBlockAllocatorImplTest, RemovesAndRecreatesExistingDirectory) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-existing");
  std::filesystem::remove_all(directory);
  std::filesystem::create_directories(directory);
  {
    std::ofstream old_file(directory + "/old-file");
    old_file << "stale";
  }

  FileBlockAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  EXPECT_TRUE(std::filesystem::exists(directory));
  EXPECT_FALSE(std::filesystem::exists(directory + "/old-file"));
}

TEST(FileBlockAllocatorImplTest, DoesNotCreateBucketFilesDuringInit) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-lazy-init");
  std::filesystem::remove_all(directory);

  FileBlockAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  EXPECT_TRUE(std::filesystem::is_empty(directory));
}

TEST(FileBlockAllocatorImplTest, AllocatesRequestToFirstFittingBucket) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-bucket-fit");
  std::filesystem::remove_all(directory);
  FileBlockAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  auto result = allocator.Allocate(6 * 1024);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(6 * 1024, result.extent.requested_size);
  EXPECT_EQ(8 * 1024, result.extent.allocated_size);
  EXPECT_EQ(FileExtentKind::kBucket, result.extent.kind);
  EXPECT_EQ(0, result.extent.offset);
  EXPECT_GE(result.extent.fd, 0);
}

TEST(FileBlockAllocatorImplTest, AllocatesSequentialOffsetsInSameBucket) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-sequential");
  std::filesystem::remove_all(directory);
  FileBlockAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  auto first = allocator.Allocate(4 * 1024);
  auto second = allocator.Allocate(4 * 1024);

  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  EXPECT_EQ(first.extent.fd, second.extent.fd);
  EXPECT_EQ(0, first.extent.offset);
  EXPECT_EQ(4 * 1024, second.extent.offset);
}

TEST(FileBlockAllocatorImplTest, CreatesNextBucketFileWhenCurrentFileIsFull) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-rollover");
  std::filesystem::remove_all(directory);
  auto config = ValidConfigWithDirectory(directory);
  config.bucket_sizes = {4 * 1024};
  config.file_size_limit_bytes = 8 * 1024;
  config.max_open_files_per_bucket = 2;
  FileBlockAllocatorImpl allocator(config);

  auto first = allocator.Allocate(4 * 1024);
  auto second = allocator.Allocate(4 * 1024);
  auto third = allocator.Allocate(4 * 1024);

  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(third.ok());
  EXPECT_EQ(first.extent.fd, second.extent.fd);
  EXPECT_NE(first.extent.fd, third.extent.fd);
  EXPECT_EQ(0, third.extent.offset);
}

TEST(FileBlockAllocatorImplTest, RespectsMaxOpenFilesPerBucket) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-open-limit");
  std::filesystem::remove_all(directory);
  auto config = ValidConfigWithDirectory(directory);
  config.bucket_sizes = {4 * 1024};
  config.file_size_limit_bytes = 4 * 1024;
  config.max_open_files_per_bucket = 1;
  FileBlockAllocatorImpl allocator(config);

  auto first = allocator.Allocate(4 * 1024);
  auto second = allocator.Allocate(4 * 1024);

  ASSERT_TRUE(first.ok());
  EXPECT_EQ(FileErrorCode::kTooManyOpenFiles, second.error);
}

TEST(FileBlockAllocatorImplTest, AllocatesDedicatedFileForLargeRequest) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-dedicated");
  std::filesystem::remove_all(directory);
  FileBlockAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  auto result = allocator.Allocate(128 * 1024);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(FileExtentKind::kDedicated, result.extent.kind);
  EXPECT_EQ(0, result.extent.offset);
  EXPECT_EQ(128 * 1024, result.extent.requested_size);
  EXPECT_EQ(128 * 1024, result.extent.allocated_size);
  EXPECT_GE(result.extent.fd, 0);
}
