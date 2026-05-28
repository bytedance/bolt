#include "bolt/common/memory/bm/file/FileBlockAllocator.h"
#include "bolt/common/memory/bm/file/tests/FileBlockAllocatorTestUtil.h"

#include "bolt/common/base/BoltException.h"

#include <filesystem>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;
using namespace bytedance::bolt::memory::bm::test;

TEST(FileBlockAllocatorSingletonTest, AllocatesThroughSingleton) {
  ShutdownFileBlockAllocator();
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-singleton");
  std::filesystem::remove_all(directory);
  InitFileBlockAllocator(ValidConfigWithDirectory(directory));

  auto allocation = GetFileBlockAllocator().Allocate(4 * 1024);

  EXPECT_TRUE(allocation.ok());
  ShutdownFileBlockAllocator();
}

TEST(FileBlockAllocatorSingletonTest, RejectsRepeatedInitWithoutShutdown) {
  ShutdownFileBlockAllocator();
  const auto directory = UniqueTempDir("bolt-bm-file-allocator-repeat-init");
  std::filesystem::remove_all(directory);
  InitFileBlockAllocator(ValidConfigWithDirectory(directory));

  EXPECT_THROW(
      InitFileBlockAllocator(ValidConfigWithDirectory(directory)),
      bytedance::bolt::BoltException);
  ShutdownFileBlockAllocator();
}
