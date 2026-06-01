#include "bolt/common/memory/bm/file/OwnedFileFactory.h"
#include "bolt/common/memory/bm/file/tests/FileBlockAllocatorTestUtil.h"

#include <filesystem>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;
using namespace bytedance::bolt::memory::bm::test;

TEST(OwnedFileFactoryTest, createExclusiveReadWriteFileReturnsOwnedFile) {
  const auto directory = UniqueTempDir("bolt-bm-owned-file-factory");
  std::filesystem::remove_all(directory);
  std::filesystem::create_directories(directory);
  const auto path = (std::filesystem::path(directory) / "spill-file").string();

  auto result = CreateExclusiveReadWriteOwnedFile(path);

  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result.file.valid());
  EXPECT_GE(result.file.fd(), 0);
  EXPECT_TRUE(std::filesystem::exists(path));
}

TEST(OwnedFileFactoryTest, createExclusiveReadWriteFileReportsNativeError) {
  const auto directory = UniqueTempDir("bolt-bm-owned-file-factory-existing");
  std::filesystem::remove_all(directory);
  std::filesystem::create_directories(directory);
  const auto path = (std::filesystem::path(directory) / "spill-file").string();
  {
    auto result = CreateExclusiveReadWriteOwnedFile(path);
    ASSERT_TRUE(result.ok());
  }

  auto result = CreateExclusiveReadWriteOwnedFile(path);

  EXPECT_EQ(FileErrorCode::kIoError, result.error);
  EXPECT_NE(0, result.native_error_code);
  EXPECT_FALSE(result.file.valid());
}
