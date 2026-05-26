/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/connectors/hive/FileHandle.h"

#include "bolt/common/caching/SimpleLRUCache.h"
#include "bolt/common/file/File.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/testutil/TempFilePath.h"
#include "gtest/gtest.h"
using namespace bytedance::bolt;

TEST(FileHandleTest, localFile) {
  filesystems::registerLocalFileSystem();

  auto tempFile = ::bytedance::bolt::test::TempFilePath::create();
  const auto& filename = tempFile->path;
  remove(filename.c_str());

  {
    LocalWriteFile writeFile(filename);
    writeFile.append("foo");
  }

  FileHandleFactory factory(
      std::make_unique<
          SimpleLRUCache<std::string, std::shared_ptr<FileHandle>>>(1000),
      std::make_unique<FileHandleGenerator>());
  filesystems::FileOptions options;
  auto fileHandle = factory.generate(filename, options).second;
  ASSERT_EQ(fileHandle->file->size(), 3);
  char buffer[3];
  ASSERT_EQ(fileHandle->file->pread(0, 3, &buffer), "foo");

  // Clean up
  remove(filename.c_str());
}
