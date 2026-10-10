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

#include <gtest/gtest.h>
#include "bolt/common/config/Config.h"
#include "bolt/common/file/File.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/connectors/hive/storage_adapters/tos/RegisterTosFileSystem.h"

namespace bytedance::bolt::filesystems {
TEST(TosFileSystemRegistrationTest, registersTosScheme) {
  registerTosFileSystem();
  ASSERT_NO_THROW(getFileSystem("tos://test-bucket/object", nullptr));
}
TEST(TosFileSystemRegistrationTest, rejectsMalformedUris) {
  registerTosFileSystem();
  for (const auto* path :
       {"tos://",
        "tos://bucket",
        "tos:///object",
        "tos://bucket/",
        "tos://user@bucket/object"}) {
    EXPECT_ANY_THROW(getFileSystem(path, nullptr));
  }
}

TEST(TosFileSystemRegistrationTest, rejectsUnsupportedEndpointsBeforeSdkCall) {
  registerTosFileSystem();
  for (const auto* endpoint :
       {"http://127.0.0.1",
        "http://localhost:1234",
        "http://localhost:0",
        "https://tos-s3-cn-beijing.volces.com"}) {
    auto config = std::make_shared<config::ConfigBase>(
        std::unordered_map<std::string, std::string>{
            {"tos.endpoint", endpoint},
            {"tos.region", "cn-beijing"},
            {"tos.access.key", "test-access"},
            {"tos.secret.key", "test-secret"}});
    auto fs = getFileSystem("tos://test-bucket/object", config);
    EXPECT_ANY_THROW(fs->openFileForRead("tos://test-bucket/object"));
    EXPECT_ANY_THROW(fs->openFileForWrite("tos://test-bucket/object"));
  }
}
} // namespace bytedance::bolt::filesystems
