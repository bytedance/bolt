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

#include <gtest/gtest.h>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/connectors/hive/storage_adapters/abfs/AzureClientProviderFactories.h"
#include "bolt/connectors/hive/storage_adapters/abfs/AzureClientProviderImpl.h"
#include "bolt/connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::filesystems;

namespace {

class DummyAzureClientProvider final : public AzureClientProvider {
 public:
  std::unique_ptr<AzureBlobClient> getReadFileClient(
      const std::shared_ptr<AbfsPath>& path,
      const config::ConfigBase& config) override {
    BOLT_FAIL("DummyAzureClientProvider: Not implemented.");
  }

  std::unique_ptr<AzureDataLakeFileClient> getWriteFileClient(
      const std::shared_ptr<AbfsPath>& path,
      const config::ConfigBase& config) override {
    BOLT_FAIL("DummyAzureClientProvider: Not implemented.");
  }
};

} // namespace

TEST(AzureClientProviderFactoriesTest, registerFromConfig) {
  const auto abfsPath = std::make_shared<AbfsPath>(
      "abfss://abc@efg.dfs.core.windows.net/file/test.txt");

  {
    // OAuth auth type.
    const config::ConfigBase config(
        {{"fs.azure.account.auth.type.efg.dfs.core.windows.net", "OAuth"},
         {"fs.azure.account.oauth2.client.id.efg.dfs.core.windows.net", "123"},
         {"fs.azure.account.oauth2.client.secret.efg.dfs.core.windows.net",
          "456"},
         {"fs.azure.account.oauth2.client.endpoint.efg.dfs.core.windows.net",
          "https://login.microsoftonline.com/{TENANTID}/oauth2/token"}},
        false);
    registerAzureClientProvider(config);

    ASSERT_NE(
        AzureClientProviderFactories::getReadFileClient(abfsPath, config),
        nullptr);
    ASSERT_NE(
        AzureClientProviderFactories::getWriteFileClient(abfsPath, config),
        nullptr);
  }

  {
    // SharedKey auth type.
    const config::ConfigBase config(
        {{"fs.azure.account.auth.type.efg.dfs.core.windows.net", "SharedKey"},
         {"fs.azure.account.key.efg.dfs.core.windows.net", "456"}},
        false);
    registerAzureClientProvider(config);

    ASSERT_NE(
        AzureClientProviderFactories::getReadFileClient(abfsPath, config),
        nullptr);
    ASSERT_NE(
        AzureClientProviderFactories::getWriteFileClient(abfsPath, config),
        nullptr);
  }

  {
    // SAS auth type.
    const config::ConfigBase config(
        {{"fs.azure.account.auth.type.efg.dfs.core.windows.net", "SAS"},
         {"fs.azure.sas.fixed.token.efg.dfs.core.windows.net", "456"}},
        false);
    registerAzureClientProvider(config);

    ASSERT_NE(
        AzureClientProviderFactories::getReadFileClient(abfsPath, config),
        nullptr);
    ASSERT_NE(
        AzureClientProviderFactories::getWriteFileClient(abfsPath, config),
        nullptr);
  }

  {
    // Invalid auth type.
    const config::ConfigBase config(
        {{"fs.azure.account.auth.type.efg.dfs.core.windows.net", "Custom"},
         {"fs.azure.account.key.efg.dfs.core.windows.net", "456"}},
        false);
    BOLT_ASSERT_THROW(
        registerAzureClientProvider(config),
        "Unsupported auth type Custom, supported auth types are SharedKey, OAuth and SAS.");
  }

  {
    // Invalid config key.
    const config::ConfigBase config(
        {{"fs.azure.account.auth.type.efg", "SharedKey"},
         {"fs.azure.account.key.efg.dfs.core.windows.net", "456"}},
        false);
    BOLT_ASSERT_THROW(
        registerAzureClientProvider(config),
        "Invalid Azure account auth type key: fs.azure.account.auth.type.efg");
  }
}

TEST(AzureClientProviderFactoriesTest, registerCustomFactory) {
  static const std::string path = "abfs://test@efg.dfs.core.windows.net/test";
  const auto abfsPath = std::make_shared<AbfsPath>(path);

  registerAzureClientProviderFactory(
      "efg",
      [](const std::string& account) -> std::unique_ptr<AzureClientProvider> {
        return std::make_unique<DummyAzureClientProvider>();
      });

  ASSERT_NO_THROW(AzureClientProviderFactories::getClientFactory("efg"));
  BOLT_ASSERT_THROW(
      AzureClientProviderFactories::getReadFileClient(
          abfsPath, config::ConfigBase({})),
      "DummyAzureClientProvider: Not implemented.");
  BOLT_ASSERT_THROW(
      AzureClientProviderFactories::getWriteFileClient(
          abfsPath, config::ConfigBase({})),
      "DummyAzureClientProvider: Not implemented.");

  BOLT_ASSERT_THROW(
      AzureClientProviderFactories::getClientFactory("efg2"),
      "No AzureClientProviderFactory registered for account 'efg2'.");
}
