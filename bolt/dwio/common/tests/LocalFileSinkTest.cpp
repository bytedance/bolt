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

#include "bolt/common/base/Fs.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/dwio/common/FileSink.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"

#include <gtest/gtest.h>

using namespace ::testing;
using namespace bytedance::bolt::exec::test;
namespace bytedance::bolt::dwio::common {

class LocalFileSinkTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    bolt::filesystems::registerLocalFileSystem();
  }

  void runTest() {
    auto root = TempDirectoryPath::create();
    auto filePath = fs::path(root->path) / "xxx/yyy/zzz/test_file.ext";

    ASSERT_FALSE(fs::exists(filePath.string()));

    auto localFileSink = FileSink::create(
        fmt::format("file:{}", filePath.string()), {.pool = pool_.get()});
    ASSERT_TRUE(localFileSink->isBuffered());
    localFileSink->close();

    EXPECT_TRUE(fs::exists(filePath.string()));
  }

  std::shared_ptr<bolt::memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};

TEST_F(LocalFileSinkTest, missingRegistration) {
  BOLT_ASSERT_THROW(
      FileSink::create("unregistered:", {.pool = pool_.get()}),
      "FileSink is not registered for unregistered:");
}

TEST_F(LocalFileSinkTest, create) {
  LocalFileSink::registerFactory();
  runTest();
}

} // namespace bytedance::bolt::dwio::common
