#include "bolt/common/memory/bm/file/ManagedOpenFileFactory.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"

#include <filesystem>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;
using namespace bytedance::bolt::memory::bm::test;

TEST(ManagedOpenFileFactoryTest, createExclusiveReadWriteFileReturnsManagedOpenFile) {
  const auto directory = UniqueTempDir("bolt-bm-owned-file-factory");
  std::filesystem::remove_all(directory);
  std::filesystem::create_directories(directory);
  const auto path = (std::filesystem::path(directory) / "spill-file").string();

  auto result = CreateExclusiveReadWriteManagedOpenFile(path);

  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result.file.valid());
  EXPECT_GE(result.file.fd(), 0);
  EXPECT_TRUE(std::filesystem::exists(path));
}

TEST(ManagedOpenFileFactoryTest, createExclusiveReadWriteFileReportsNativeError) {
  const auto directory = UniqueTempDir("bolt-bm-owned-file-factory-existing");
  std::filesystem::remove_all(directory);
  std::filesystem::create_directories(directory);
  const auto path = (std::filesystem::path(directory) / "spill-file").string();
  {
    auto result = CreateExclusiveReadWriteManagedOpenFile(path);
    ASSERT_TRUE(result.ok());
  }

  auto result = CreateExclusiveReadWriteManagedOpenFile(path);

  EXPECT_EQ(FileErrorCode::kIoError, result.error);
  EXPECT_NE(0, result.native_error_code);
  EXPECT_FALSE(result.file.valid());
}
