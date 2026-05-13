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
#include <gtest/gtest.h>
#include "bolt/common/testutil/TempDirectoryPath.h"
#include "boost/filesystem.hpp"
namespace bytedance::bolt::common {

class FsTest : public testing::Test {};

TEST_F(FsTest, createDirectory) {
  auto rootPath =
      bytedance::bolt::test::TempDirectoryPath::createTempDirectory();
  auto tmpDirectoryPath = rootPath + "/first/second/third";
  // First time should generate directory successfully.
  EXPECT_FALSE(fs::exists(tmpDirectoryPath.c_str()));
  EXPECT_TRUE(generateFileDirectory(tmpDirectoryPath.c_str()));
  EXPECT_TRUE(fs::exists(tmpDirectoryPath.c_str()));

  // Directory already exist, not creating but should return success.
  EXPECT_TRUE(generateFileDirectory(tmpDirectoryPath.c_str()));
  EXPECT_TRUE(fs::exists(tmpDirectoryPath.c_str()));
  boost::filesystem::remove_all(rootPath);
  EXPECT_FALSE(fs::exists(rootPath.c_str()));
}

} // namespace bytedance::bolt::common
