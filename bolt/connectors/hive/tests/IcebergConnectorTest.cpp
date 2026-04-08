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
 * 2026-03-30.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/connectors/hive/iceberg/IcebergConnector.h"

#include <gtest/gtest.h>
#include "bolt/connectors/hive/HiveConfig.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"

using namespace bytedance::bolt;

namespace bytedance::bolt::connector::hive::iceberg {
namespace {

static constexpr const char* kIcebergConnectorId = "test-iceberg";

class IcebergConnectorTest : public exec::test::HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    CheckIcebergConnectorFactoryInit<>();
    auto icebergConnector =
        connector::getConnectorFactory(
            IcebergConnectorFactory::kIcebergConnectorName)
            ->newConnector(
                kIcebergConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>{}),
                ioExecutor_.get());
    connector::registerConnector(icebergConnector);
  }

  void TearDown() override {
    connector::unregisterConnector(kIcebergConnectorId);
    HiveConnectorTestBase::TearDown();
  }

  static void resetIcebergConnector(
      const std::shared_ptr<const config::ConfigBase>& config,
      folly::Executor* executor) {
    connector::unregisterConnector(kIcebergConnectorId);
    auto icebergConnector =
        connector::getConnectorFactory(
            IcebergConnectorFactory::kIcebergConnectorName)
            ->newConnector(kIcebergConnectorId, config, executor);
    connector::registerConnector(icebergConnector);
  }
};

TEST_F(IcebergConnectorTest, connectorConfiguration) {
  auto customConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{
          {hive::HiveConfig::kEnableFileHandleCache, "true"},
          {hive::HiveConfig::kNumCacheFileHandles, "1000"},
          {IcebergConfig::kFunctionPrefixConfig, "$test.iceberg."}});

  resetIcebergConnector(customConfig, ioExecutor_.get());

  auto icebergConnector = connector::getConnector(kIcebergConnectorId);
  ASSERT_NE(icebergConnector, nullptr);

  auto config = icebergConnector->connectorConfig();
  ASSERT_NE(config, nullptr);

  hive::HiveConfig hiveConfig(config);
  ASSERT_TRUE(hiveConfig.isFileHandleCacheEnabled());
  ASSERT_EQ(hiveConfig.numCacheFileHandles(), 1000);

  IcebergConfig icebergConfig(config);
  ASSERT_EQ(icebergConfig.functionPrefix(), "$test.iceberg.");
}

TEST_F(IcebergConnectorTest, connectorProperties) {
  auto icebergConnector = connector::getConnector(kIcebergConnectorId);
  ASSERT_NE(icebergConnector, nullptr);

  ASSERT_TRUE(icebergConnector->canAddDynamicFilter());
  ASSERT_TRUE(icebergConnector->supportsSplitPreload());
  ASSERT_NE(icebergConnector->executor(), nullptr);
}

} // namespace
} // namespace bytedance::bolt::connector::hive::iceberg
