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

#include <ClientConfig.h>

#include <memory>
#include <stdexcept>
#include <unordered_map>

#include <gtest/gtest.h>

#include "bolt/common/config/Config.h"
#include "bolt/connectors/hive/storage_adapters/tos/TosConfig.h"
#include "bolt/connectors/hive/storage_adapters/tos/TosFileSystemExtension.h"

namespace bytedance::bolt::filesystems {
namespace {

class ThrowingTosFileSystemExtension final : public TosFileSystemExtension {
 public:
  void customizeClientConfig(
      VolcengineTos::ClientConfig& /*config*/) const override {
    throw std::runtime_error("client config failure");
  }

  void notifyBucketAccess(
      const TosSessionConfig& /*sessionConfig*/,
      const config::ConfigBase& /*config*/) const override {
    throw std::runtime_error("bucket access failure");
  }
};

TEST(TosFileSystemExtensionRegistryTest, IsOptionalAndIsolatesFailures) {
  VolcengineTos::ClientConfig clientConfig;
  TosSessionConfig sessionConfig;
  config::ConfigBase config(std::unordered_map<std::string, std::string>{});

  EXPECT_NO_THROW(customizeTosClientConfig(clientConfig));
  EXPECT_NO_THROW(notifyTosBucketAccess(sessionConfig, config));

  ASSERT_TRUE(registerTosFileSystemExtension(
      std::make_shared<ThrowingTosFileSystemExtension>()));
  EXPECT_FALSE(registerTosFileSystemExtension(
      std::make_shared<ThrowingTosFileSystemExtension>()));

  testing::internal::CaptureStderr();
  EXPECT_NO_THROW(customizeTosClientConfig(clientConfig));
  EXPECT_NO_THROW(notifyTosBucketAccess(sessionConfig, config));
  EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());
}

} // namespace
} // namespace bytedance::bolt::filesystems
