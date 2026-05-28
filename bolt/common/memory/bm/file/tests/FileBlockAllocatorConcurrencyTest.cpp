#include "bolt/common/memory/bm/file/FileBlockAllocatorImpl.h"
#include "bolt/common/memory/bm/file/tests/FileBlockAllocatorTestUtil.h"

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;
using namespace bytedance::bolt::memory::bm::test;

TEST(FileBlockAllocatorConcurrencyTest, SameBucketAllocationsAreUnique) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-concurrent");
  std::filesystem::remove_all(directory);
  auto config = ValidConfigWithDirectory(directory);
  config.bucket_sizes = {4 * 1024};
  config.file_size_limit_bytes = 1024 * 1024;
  config.max_open_files_per_bucket = 8;
  FileBlockAllocatorImpl allocator(config);

  constexpr size_t kThreadCount = 8;
  constexpr size_t kAllocationsPerThread = 128;
  std::mutex results_mutex;
  std::vector<FileExtent> extents;
  std::vector<std::thread> threads;

  for (size_t thread = 0; thread < kThreadCount; ++thread) {
    threads.emplace_back([&] {
      std::vector<FileExtent> local;
      for (size_t i = 0; i < kAllocationsPerThread; ++i) {
        auto result = allocator.Allocate(4 * 1024);
        ASSERT_TRUE(result.ok());
        local.push_back(result.extent);
      }
      std::lock_guard<std::mutex> lock(results_mutex);
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

TEST(FileBlockAllocatorConcurrencyTest, AllocateFreeCompletes) {
  const auto directory =
      UniqueTempDir("bolt-bm-file-allocator-concurrent-free");
  std::filesystem::remove_all(directory);
  auto config = ValidConfigWithDirectory(directory);
  config.bucket_sizes = {4 * 1024, 8 * 1024};
  config.file_size_limit_bytes = 1024 * 1024;
  config.max_open_files_per_bucket = 8;
  FileBlockAllocatorImpl allocator(config);

  constexpr size_t kThreadCount = 8;
  constexpr size_t kIterations = 256;
  std::vector<std::thread> threads;

  for (size_t thread = 0; thread < kThreadCount; ++thread) {
    threads.emplace_back([&, thread] {
      for (size_t i = 0; i < kIterations; ++i) {
        const int64_t size = (thread % 2 == 0) ? 4 * 1024 : 8 * 1024;
        auto result = allocator.Allocate(size);
        ASSERT_TRUE(result.ok());
        EXPECT_TRUE(allocator.Free(result.extent).ok());
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}
