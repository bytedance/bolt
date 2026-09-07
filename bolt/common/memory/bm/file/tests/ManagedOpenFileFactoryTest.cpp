/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "bolt/common/memory/bm/file/ManagedOpenFileFactory.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"

#include <filesystem>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;
using namespace bytedance::bolt::memory::bm::test;

TEST(
    ManagedOpenFileFactoryTest,
    createExclusiveReadWriteFileReturnsManagedOpenFile) {
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

TEST(
    ManagedOpenFileFactoryTest,
    createExclusiveReadWriteFileReportsNativeError) {
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
