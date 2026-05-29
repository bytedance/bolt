#include "bolt/common/memory/bm/file/FileBlockAllocator.h"
#include "bolt/common/memory/bm/file/tests/FileBlockAllocatorTestUtil.h"

#include <filesystem>
#include <memory>
#include <type_traits>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;
using namespace bytedance::bolt::memory::bm::test;

TEST(FileBlockAllocatorFactoryTest, CreatesAllocatorThroughFactory) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-factory");
  std::filesystem::remove_all(directory);

  auto allocator =
      CreateFileBlockAllocator(ValidConfigWithDirectory(directory));

  auto allocation = allocator->Allocate(4 * 1024);

  EXPECT_TRUE(allocation.ok());
}

TEST(FileBlockAllocatorFactoryTest, CreateReturnsSharedAllocator) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-shared");
  std::filesystem::remove_all(directory);

  auto allocator =
      CreateFileBlockAllocator(ValidConfigWithDirectory(directory));
  static_assert(std::is_same_v<decltype(allocator), std::shared_ptr<FileBlockAllocator>>);
  ASSERT_NE(nullptr, allocator);

  auto other = allocator;
  EXPECT_EQ(2, allocator.use_count());
}

TEST(FileBlockAllocatorFactoryTest, UsesUniqueDirectoryPerAllocator) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-unique-dir");
  std::filesystem::remove_all(directory);

  auto first = CreateFileBlockAllocator(ValidConfigWithDirectory(directory));
  auto second = CreateFileBlockAllocator(ValidConfigWithDirectory(directory));

  ASSERT_TRUE(first->Allocate(4 * 1024).ok());
  ASSERT_TRUE(second->Allocate(4 * 1024).ok());

  const auto directories = ListDirectories(directory);
  ASSERT_EQ(2, directories.size());
  EXPECT_NE(directories[0].filename(), directories[1].filename());
  for (const auto& allocator_directory : directories) {
    EXPECT_TRUE(
        std::filesystem::exists(allocator_directory / "bucket_4096_0.bm"));
  }
}

TEST(FileBlockAllocatorFactoryTest, DestroysOnlyOwnedDirectory) {
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-owned-dir");
  std::filesystem::remove_all(directory);

  auto first = CreateFileBlockAllocator(ValidConfigWithDirectory(directory));
  auto second = CreateFileBlockAllocator(ValidConfigWithDirectory(directory));
  ASSERT_TRUE(first->Allocate(4 * 1024).ok());
  ASSERT_TRUE(second->Allocate(4 * 1024).ok());

  const auto directories_before_destroy = ListDirectories(directory);
  ASSERT_EQ(2, directories_before_destroy.size());

  first.reset();

  const auto directories_after_destroy = ListDirectories(directory);
  ASSERT_EQ(1, directories_after_destroy.size());
  EXPECT_TRUE(std::filesystem::exists(directories_after_destroy[0]));
  EXPECT_TRUE(std::filesystem::exists(
      directories_after_destroy[0] / "bucket_4096_0.bm"));
}
