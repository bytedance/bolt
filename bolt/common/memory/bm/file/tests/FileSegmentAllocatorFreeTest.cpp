#include "bolt/common/memory/bm/file/FileSegmentAllocatorImpl.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"

#include <array>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>

#include <fcntl.h>
#include <unistd.h>

using namespace bytedance::bolt::memory::bm;
using namespace bytedance::bolt::memory::bm::test;

TEST(FileSegmentAllocatorFreeTest, ReusesFreedBucketOffsetWhenFileStillActive) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-reuse");
  std::filesystem::remove_all(directory);
  FileSegmentAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  auto first = allocator.Allocate(4 * 1024);
  auto second = allocator.Allocate(4 * 1024);
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  auto free_result = allocator.Free(first.segment);
  ASSERT_TRUE(free_result.ok());
  auto third = allocator.Allocate(4 * 1024);

  ASSERT_TRUE(third.ok());
  EXPECT_EQ(first.segment.fd, third.segment.fd);
  EXPECT_EQ(first.segment.offset, third.segment.offset);
}

TEST(FileSegmentAllocatorFreeTest, RejectsDoubleFree) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-double-free");
  std::filesystem::remove_all(directory);
  FileSegmentAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  auto allocation = allocator.Allocate(4 * 1024);
  ASSERT_TRUE(allocation.ok());

  EXPECT_TRUE(allocator.Free(allocation.segment).ok());
  EXPECT_EQ(FileErrorCode::kDoubleFree, allocator.Free(allocation.segment).error);
}

TEST(FileSegmentAllocatorFreeTest, DeletesDedicatedFileOnFree) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-free-dedicated");
  std::filesystem::remove_all(directory);
  FileSegmentAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  auto allocation = allocator.Allocate(128 * 1024);
  ASSERT_TRUE(allocation.ok());
  const auto path = OnlyAllocatorDirectory(directory) /
      ("dedicated_" + std::to_string(allocation.segment.id) + ".bm");
  ASSERT_TRUE(std::filesystem::exists(path));

  EXPECT_TRUE(allocator.Free(allocation.segment).ok());
  EXPECT_FALSE(std::filesystem::exists(path));
}

TEST(FileSegmentAllocatorFreeTest, DeletesEmptyBucketFileAndReleasesOpenFileSlot) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-delete-empty");
  std::filesystem::remove_all(directory);
  auto config = ValidConfigWithDirectory(directory);
  config.bucket_sizes = {4 * 1024};
  config.file_size_limit_bytes = 4 * 1024;
  config.max_open_files_per_bucket = 1;
  FileSegmentAllocatorImpl allocator(config);

  auto first = allocator.Allocate(4 * 1024);
  ASSERT_TRUE(first.ok());
  const auto first_path = OnlyAllocatorDirectory(directory) / "bucket_4096_0.bm";
  ASSERT_TRUE(std::filesystem::exists(first_path));
  EXPECT_TRUE(allocator.Free(first.segment).ok());
  EXPECT_FALSE(std::filesystem::exists(first_path));

  auto second = allocator.Allocate(4 * 1024);
  EXPECT_TRUE(second.ok());
}

TEST(FileSegmentAllocatorFreeTest, SupportsOutOfOrderExplicitOffsetWrites) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-offset-write");
  std::filesystem::remove_all(directory);
  auto config = ValidConfigWithDirectory(directory);
  config.bucket_sizes = {4 * 1024};
  config.file_size_limit_bytes = 8 * 1024;
  config.max_open_files_per_bucket = 1;
  FileSegmentAllocatorImpl allocator(config);

  auto first = allocator.Allocate(4 * 1024);
  auto second = allocator.Allocate(4 * 1024);
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_EQ(first.segment.fd, second.segment.fd);
  ASSERT_EQ(0, first.segment.offset);
  ASSERT_EQ(4 * 1024, second.segment.offset);

  const int flags = ::fcntl(first.segment.fd, F_GETFL);
  ASSERT_GE(flags, 0);
  EXPECT_EQ(0, flags & O_APPEND);

  std::array<char, 4 * 1024> first_data;
  std::array<char, 4 * 1024> second_data;
  first_data.fill('a');
  second_data.fill('b');

  ASSERT_EQ(
      static_cast<ssize_t>(second_data.size()),
      ::pwrite(
          second.segment.fd,
          second_data.data(),
          second_data.size(),
          second.segment.offset));
  ASSERT_EQ(
      static_cast<ssize_t>(first_data.size()),
      ::pwrite(
          first.segment.fd,
          first_data.data(),
          first_data.size(),
          first.segment.offset));

  std::array<char, 4 * 1024> first_read;
  std::array<char, 4 * 1024> second_read;
  ASSERT_EQ(
      static_cast<ssize_t>(first_read.size()),
      ::pread(
          first.segment.fd,
          first_read.data(),
          first_read.size(),
          first.segment.offset));
  ASSERT_EQ(
      static_cast<ssize_t>(second_read.size()),
      ::pread(
          second.segment.fd,
          second_read.data(),
          second_read.size(),
          second.segment.offset));

  EXPECT_EQ(first_data, first_read);
  EXPECT_EQ(second_data, second_read);
}
