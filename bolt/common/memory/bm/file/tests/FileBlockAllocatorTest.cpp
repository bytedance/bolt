#include "bolt/common/memory/bm/file/FileBlockAllocator.h"
#include "bolt/common/memory/bm/file/FileBlockAllocatorConfig.h"
#include "bolt/common/memory/bm/file/FileBlockAllocatorImpl.h"

#include "bolt/common/base/BoltException.h"

#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <fcntl.h>
#include <unistd.h>

using namespace bytedance::bolt::memory::bm;

namespace {

FileBlockAllocatorConfig validConfig() {
  FileBlockAllocatorConfig config;
  config.directory = "/tmp/bolt-bm-file-allocator-test";
  config.bucketSizes = {4 * 1024, 8 * 1024, 16 * 1024};
  config.fileSizeLimitBytes = 64 * 1024;
  config.maxOpenFilesPerBucket = 2;
  return config;
}

std::string uniqueTempDir(const std::string& name) {
  return (std::filesystem::temp_directory_path() / name).string();
}

FileBlockAllocatorConfig validConfigWithDirectory(const std::string& path) {
  auto config = validConfig();
  config.directory = path;
  return config;
}

} // namespace

TEST(FileBlockAllocatorConfigTest, acceptsValidConfig) {
  EXPECT_EQ(FileErrorCode::Ok, validateFileBlockAllocatorConfig(validConfig()));
}

TEST(FileBlockAllocatorConfigTest, rejectsEmptyDirectory) {
  auto config = validConfig();
  config.directory.clear();
  EXPECT_EQ(
      FileErrorCode::InvalidConfig,
      validateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, rejectsEmptyBuckets) {
  auto config = validConfig();
  config.bucketSizes.clear();
  EXPECT_EQ(
      FileErrorCode::InvalidConfig,
      validateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, rejectsNonIncreasingBuckets) {
  auto config = validConfig();
  config.bucketSizes = {4 * 1024, 16 * 1024, 8 * 1024};
  EXPECT_EQ(
      FileErrorCode::InvalidConfig,
      validateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, rejectsDuplicateBuckets) {
  auto config = validConfig();
  config.bucketSizes = {4 * 1024, 8 * 1024, 8 * 1024};
  EXPECT_EQ(
      FileErrorCode::InvalidConfig,
      validateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, rejectsNonAlignedBucket) {
  auto config = validConfig();
  config.bucketSizes = {4 * 1024, 6 * 1024};
  EXPECT_EQ(
      FileErrorCode::InvalidConfig,
      validateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, rejectsSmallFileLimit) {
  auto config = validConfig();
  config.fileSizeLimitBytes = 8 * 1024;
  EXPECT_EQ(
      FileErrorCode::InvalidConfig,
      validateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, rejectsZeroOpenFileLimit) {
  auto config = validConfig();
  config.maxOpenFilesPerBucket = 0;
  EXPECT_EQ(
      FileErrorCode::InvalidConfig,
      validateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorImplTest, removesAndRecreatesExistingDirectory) {
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-existing");
  std::filesystem::remove_all(directory);
  std::filesystem::create_directories(directory);
  {
    std::ofstream oldFile(directory + "/old-file");
    oldFile << "stale";
  }

  FileBlockAllocatorImpl allocator(validConfigWithDirectory(directory));

  EXPECT_TRUE(std::filesystem::exists(directory));
  EXPECT_FALSE(std::filesystem::exists(directory + "/old-file"));
}

TEST(FileBlockAllocatorImplTest, doesNotCreateBucketFilesDuringInit) {
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-lazy-init");
  std::filesystem::remove_all(directory);

  FileBlockAllocatorImpl allocator(validConfigWithDirectory(directory));

  EXPECT_TRUE(std::filesystem::is_empty(directory));
}

TEST(FileBlockAllocatorImplTest, allocatesRequestToFirstFittingBucket) {
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-bucket-fit");
  std::filesystem::remove_all(directory);
  FileBlockAllocatorImpl allocator(validConfigWithDirectory(directory));

  auto result = allocator.allocate(6 * 1024);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(6 * 1024, result.extent.requestedSize);
  EXPECT_EQ(8 * 1024, result.extent.allocatedSize);
  EXPECT_EQ(FileExtentKind::Bucket, result.extent.kind);
  EXPECT_EQ(0, result.extent.offset);
  EXPECT_GE(result.extent.fd, 0);
}

TEST(FileBlockAllocatorImplTest, allocatesSequentialOffsetsInSameBucket) {
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-sequential");
  std::filesystem::remove_all(directory);
  FileBlockAllocatorImpl allocator(validConfigWithDirectory(directory));

  auto first = allocator.allocate(4 * 1024);
  auto second = allocator.allocate(4 * 1024);

  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  EXPECT_EQ(first.extent.fd, second.extent.fd);
  EXPECT_EQ(0, first.extent.offset);
  EXPECT_EQ(4 * 1024, second.extent.offset);
}

TEST(FileBlockAllocatorImplTest, createsNextBucketFileWhenCurrentFileIsFull) {
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-rollover");
  std::filesystem::remove_all(directory);
  auto config = validConfigWithDirectory(directory);
  config.bucketSizes = {4 * 1024};
  config.fileSizeLimitBytes = 8 * 1024;
  config.maxOpenFilesPerBucket = 2;
  FileBlockAllocatorImpl allocator(config);

  auto first = allocator.allocate(4 * 1024);
  auto second = allocator.allocate(4 * 1024);
  auto third = allocator.allocate(4 * 1024);

  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(third.ok());
  EXPECT_EQ(first.extent.fd, second.extent.fd);
  EXPECT_NE(first.extent.fd, third.extent.fd);
  EXPECT_EQ(0, third.extent.offset);
}

TEST(FileBlockAllocatorImplTest, respectsMaxOpenFilesPerBucket) {
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-open-limit");
  std::filesystem::remove_all(directory);
  auto config = validConfigWithDirectory(directory);
  config.bucketSizes = {4 * 1024};
  config.fileSizeLimitBytes = 4 * 1024;
  config.maxOpenFilesPerBucket = 1;
  FileBlockAllocatorImpl allocator(config);

  auto first = allocator.allocate(4 * 1024);
  auto second = allocator.allocate(4 * 1024);

  ASSERT_TRUE(first.ok());
  EXPECT_EQ(FileErrorCode::TooManyOpenFiles, second.error);
}

TEST(FileBlockAllocatorImplTest, allocatesDedicatedFileForLargeRequest) {
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-dedicated");
  std::filesystem::remove_all(directory);
  FileBlockAllocatorImpl allocator(validConfigWithDirectory(directory));

  auto result = allocator.allocate(128 * 1024);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(FileExtentKind::Dedicated, result.extent.kind);
  EXPECT_EQ(0, result.extent.offset);
  EXPECT_EQ(128 * 1024, result.extent.requestedSize);
  EXPECT_EQ(128 * 1024, result.extent.allocatedSize);
  EXPECT_GE(result.extent.fd, 0);
}

TEST(FileBlockAllocatorImplTest, reusesFreedBucketOffsetWhenFileStillActive) {
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-reuse");
  std::filesystem::remove_all(directory);
  FileBlockAllocatorImpl allocator(validConfigWithDirectory(directory));

  auto first = allocator.allocate(4 * 1024);
  auto second = allocator.allocate(4 * 1024);
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  auto freeResult = allocator.free(first.extent);
  ASSERT_TRUE(freeResult.ok());
  auto third = allocator.allocate(4 * 1024);

  ASSERT_TRUE(third.ok());
  EXPECT_EQ(first.extent.fd, third.extent.fd);
  EXPECT_EQ(first.extent.offset, third.extent.offset);
}

TEST(FileBlockAllocatorImplTest, rejectsDoubleFree) {
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-double-free");
  std::filesystem::remove_all(directory);
  FileBlockAllocatorImpl allocator(validConfigWithDirectory(directory));

  auto allocation = allocator.allocate(4 * 1024);
  ASSERT_TRUE(allocation.ok());

  EXPECT_TRUE(allocator.free(allocation.extent).ok());
  EXPECT_EQ(FileErrorCode::DoubleFree, allocator.free(allocation.extent).error);
}

TEST(FileBlockAllocatorImplTest, deletesDedicatedFileOnFree) {
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-free-dedicated");
  std::filesystem::remove_all(directory);
  FileBlockAllocatorImpl allocator(validConfigWithDirectory(directory));

  auto allocation = allocator.allocate(128 * 1024);
  ASSERT_TRUE(allocation.ok());
  const auto path =
      directory + "/dedicated_" + std::to_string(allocation.extent.id) + ".bm";
  ASSERT_TRUE(std::filesystem::exists(path));

  EXPECT_TRUE(allocator.free(allocation.extent).ok());
  EXPECT_FALSE(std::filesystem::exists(path));
}

TEST(FileBlockAllocatorImplTest, deletesEmptyBucketFileAndReleasesOpenFileSlot) {
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-delete-empty");
  std::filesystem::remove_all(directory);
  auto config = validConfigWithDirectory(directory);
  config.bucketSizes = {4 * 1024};
  config.fileSizeLimitBytes = 4 * 1024;
  config.maxOpenFilesPerBucket = 1;
  FileBlockAllocatorImpl allocator(config);

  auto first = allocator.allocate(4 * 1024);
  ASSERT_TRUE(first.ok());
  const auto firstPath = directory + "/bucket_4096_0.bm";
  ASSERT_TRUE(std::filesystem::exists(firstPath));
  EXPECT_TRUE(allocator.free(first.extent).ok());
  EXPECT_FALSE(std::filesystem::exists(firstPath));

  auto second = allocator.allocate(4 * 1024);
  EXPECT_TRUE(second.ok());
}

TEST(FileBlockAllocatorImplTest, supportsOutOfOrderExplicitOffsetWrites) {
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-offset-write");
  std::filesystem::remove_all(directory);
  auto config = validConfigWithDirectory(directory);
  config.bucketSizes = {4 * 1024};
  config.fileSizeLimitBytes = 8 * 1024;
  config.maxOpenFilesPerBucket = 1;
  FileBlockAllocatorImpl allocator(config);

  auto first = allocator.allocate(4 * 1024);
  auto second = allocator.allocate(4 * 1024);
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_EQ(first.extent.fd, second.extent.fd);
  ASSERT_EQ(0, first.extent.offset);
  ASSERT_EQ(4 * 1024, second.extent.offset);

  const int flags = ::fcntl(first.extent.fd, F_GETFL);
  ASSERT_GE(flags, 0);
  EXPECT_EQ(0, flags & O_APPEND);

  std::array<char, 4 * 1024> firstData;
  std::array<char, 4 * 1024> secondData;
  firstData.fill('a');
  secondData.fill('b');

  ASSERT_EQ(
      static_cast<ssize_t>(secondData.size()),
      ::pwrite(
          second.extent.fd,
          secondData.data(),
          secondData.size(),
          second.extent.offset));
  ASSERT_EQ(
      static_cast<ssize_t>(firstData.size()),
      ::pwrite(
          first.extent.fd,
          firstData.data(),
          firstData.size(),
          first.extent.offset));

  std::array<char, 4 * 1024> firstRead;
  std::array<char, 4 * 1024> secondRead;
  ASSERT_EQ(
      static_cast<ssize_t>(firstRead.size()),
      ::pread(
          first.extent.fd,
          firstRead.data(),
          firstRead.size(),
          first.extent.offset));
  ASSERT_EQ(
      static_cast<ssize_t>(secondRead.size()),
      ::pread(
          second.extent.fd,
          secondRead.data(),
          secondRead.size(),
          second.extent.offset));

  EXPECT_EQ(firstData, firstRead);
  EXPECT_EQ(secondData, secondRead);
}

TEST(FileBlockAllocatorSingletonTest, allocatesThroughSingleton) {
  shutdownFileBlockAllocator();
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-singleton");
  std::filesystem::remove_all(directory);
  initFileBlockAllocator(validConfigWithDirectory(directory));

  auto allocation = fileBlockAllocator().allocate(4 * 1024);

  EXPECT_TRUE(allocation.ok());
  shutdownFileBlockAllocator();
}

TEST(FileBlockAllocatorSingletonTest, rejectsRepeatedInitWithoutShutdown) {
  shutdownFileBlockAllocator();
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-repeat-init");
  std::filesystem::remove_all(directory);
  initFileBlockAllocator(validConfigWithDirectory(directory));

  EXPECT_THROW(
      initFileBlockAllocator(validConfigWithDirectory(directory)),
      bytedance::bolt::BoltException);
  shutdownFileBlockAllocator();
}

TEST(FileBlockAllocatorImplTest, concurrentSameBucketAllocationsAreUnique) {
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-concurrent");
  std::filesystem::remove_all(directory);
  auto config = validConfigWithDirectory(directory);
  config.bucketSizes = {4 * 1024};
  config.fileSizeLimitBytes = 1024 * 1024;
  config.maxOpenFilesPerBucket = 8;
  FileBlockAllocatorImpl allocator(config);

  constexpr size_t kThreadCount = 8;
  constexpr size_t kAllocationsPerThread = 128;
  std::mutex resultsMutex;
  std::vector<FileExtent> extents;
  std::vector<std::thread> threads;

  for (size_t thread = 0; thread < kThreadCount; ++thread) {
    threads.emplace_back([&] {
      std::vector<FileExtent> local;
      for (size_t i = 0; i < kAllocationsPerThread; ++i) {
        auto result = allocator.allocate(4 * 1024);
        ASSERT_TRUE(result.ok());
        local.push_back(result.extent);
      }
      std::lock_guard<std::mutex> lock(resultsMutex);
      extents.insert(extents.end(), local.begin(), local.end());
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  std::set<std::pair<int, uint64_t>> locations;
  for (const auto& extent : extents) {
    EXPECT_TRUE(locations.emplace(extent.fd, extent.offset).second);
  }
}

TEST(FileBlockAllocatorImplTest, concurrentAllocateFreeCompletes) {
  const auto directory =
      uniqueTempDir("bolt-bm-file-allocator-concurrent-free");
  std::filesystem::remove_all(directory);
  auto config = validConfigWithDirectory(directory);
  config.bucketSizes = {4 * 1024, 8 * 1024};
  config.fileSizeLimitBytes = 1024 * 1024;
  config.maxOpenFilesPerBucket = 8;
  FileBlockAllocatorImpl allocator(config);

  constexpr size_t kThreadCount = 8;
  constexpr size_t kIterations = 256;
  std::vector<std::thread> threads;

  for (size_t thread = 0; thread < kThreadCount; ++thread) {
    threads.emplace_back([&, thread] {
      for (size_t i = 0; i < kIterations; ++i) {
        const int64_t size = (thread % 2 == 0) ? 4 * 1024 : 8 * 1024;
        auto result = allocator.allocate(size);
        ASSERT_TRUE(result.ok());
        EXPECT_TRUE(allocator.free(result.extent).ok());
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}
