#include "bolt/common/memory/bm/file/FileBlockAllocatorImpl.h"
#include "bolt/common/memory/bm/file/tests/FileBlockAllocatorTestUtil.h"

#include <array>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>

#include <fcntl.h>
#include <unistd.h>

using namespace bytedance::bolt::memory::bm;
using namespace bytedance::bolt::memory::bm::test;

TEST(FileBlockAllocatorFreeTest, ReusesFreedBucketOffsetWhenFileStillActive) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-reuse");
  std::filesystem::remove_all(directory);
  FileBlockAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  auto first = allocator.Allocate(4 * 1024);
  auto second = allocator.Allocate(4 * 1024);
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  auto free_result = allocator.Free(first.extent);
  ASSERT_TRUE(free_result.ok());
  auto third = allocator.Allocate(4 * 1024);

  ASSERT_TRUE(third.ok());
  EXPECT_EQ(first.extent.fd, third.extent.fd);
  EXPECT_EQ(first.extent.offset, third.extent.offset);
}

TEST(FileBlockAllocatorFreeTest, RejectsDoubleFree) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-double-free");
  std::filesystem::remove_all(directory);
  FileBlockAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  auto allocation = allocator.Allocate(4 * 1024);
  ASSERT_TRUE(allocation.ok());

  EXPECT_TRUE(allocator.Free(allocation.extent).ok());
  EXPECT_EQ(FileErrorCode::kDoubleFree, allocator.Free(allocation.extent).error);
}

TEST(FileBlockAllocatorFreeTest, DeletesDedicatedFileOnFree) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-free-dedicated");
  std::filesystem::remove_all(directory);
  FileBlockAllocatorImpl allocator(ValidConfigWithDirectory(directory));

  auto allocation = allocator.Allocate(128 * 1024);
  ASSERT_TRUE(allocation.ok());
  const auto path =
      directory + "/dedicated_" + std::to_string(allocation.extent.id) + ".bm";
  ASSERT_TRUE(std::filesystem::exists(path));

  EXPECT_TRUE(allocator.Free(allocation.extent).ok());
  EXPECT_FALSE(std::filesystem::exists(path));
}

TEST(FileBlockAllocatorFreeTest, DeletesEmptyBucketFileAndReleasesOpenFileSlot) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-delete-empty");
  std::filesystem::remove_all(directory);
  auto config = ValidConfigWithDirectory(directory);
  config.bucket_sizes = {4 * 1024};
  config.file_size_limit_bytes = 4 * 1024;
  config.max_open_files_per_bucket = 1;
  FileBlockAllocatorImpl allocator(config);

  auto first = allocator.Allocate(4 * 1024);
  ASSERT_TRUE(first.ok());
  const auto first_path = directory + "/bucket_4096_0.bm";
  ASSERT_TRUE(std::filesystem::exists(first_path));
  EXPECT_TRUE(allocator.Free(first.extent).ok());
  EXPECT_FALSE(std::filesystem::exists(first_path));

  auto second = allocator.Allocate(4 * 1024);
  EXPECT_TRUE(second.ok());
}

TEST(FileBlockAllocatorFreeTest, SupportsOutOfOrderExplicitOffsetWrites) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-offset-write");
  std::filesystem::remove_all(directory);
  auto config = ValidConfigWithDirectory(directory);
  config.bucket_sizes = {4 * 1024};
  config.file_size_limit_bytes = 8 * 1024;
  config.max_open_files_per_bucket = 1;
  FileBlockAllocatorImpl allocator(config);

  auto first = allocator.Allocate(4 * 1024);
  auto second = allocator.Allocate(4 * 1024);
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_EQ(first.extent.fd, second.extent.fd);
  ASSERT_EQ(0, first.extent.offset);
  ASSERT_EQ(4 * 1024, second.extent.offset);

  const int flags = ::fcntl(first.extent.fd, F_GETFL);
  ASSERT_GE(flags, 0);
  EXPECT_EQ(0, flags & O_APPEND);

  std::array<char, 4 * 1024> first_data;
  std::array<char, 4 * 1024> second_data;
  first_data.fill('a');
  second_data.fill('b');

  ASSERT_EQ(
      static_cast<ssize_t>(second_data.size()),
      ::pwrite(
          second.extent.fd,
          second_data.data(),
          second_data.size(),
          second.extent.offset));
  ASSERT_EQ(
      static_cast<ssize_t>(first_data.size()),
      ::pwrite(
          first.extent.fd,
          first_data.data(),
          first_data.size(),
          first.extent.offset));

  std::array<char, 4 * 1024> first_read;
  std::array<char, 4 * 1024> second_read;
  ASSERT_EQ(
      static_cast<ssize_t>(first_read.size()),
      ::pread(
          first.extent.fd,
          first_read.data(),
          first_read.size(),
          first.extent.offset));
  ASSERT_EQ(
      static_cast<ssize_t>(second_read.size()),
      ::pread(
          second.extent.fd,
          second_read.data(),
          second_read.size(),
          second.extent.offset));

  EXPECT_EQ(first_data, first_read);
  EXPECT_EQ(second_data, second_read);
}
