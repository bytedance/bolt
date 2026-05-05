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

#pragma once

#include "bolt/connectors/Connector.h"

namespace bytedance::bolt::connector::paimon {

class PaimonConnector : public Connector {
 public:
  PaimonConnector(
      const std::string& id,
      const std::shared_ptr<const config::ConfigBase>& config,
      folly::Executor* executor)
      : Connector(id), config_(config), executor_(executor) {}

  std::unique_ptr<DataSource> createDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>&
          columnHandles,
      std::shared_ptr<ConnectorQueryCtx> queryCtx,
      const core::QueryConfig& queryConfig) override;

  std::unique_ptr<DataSink> createDataSink(
      std::shared_ptr<const RowType> /*inputType*/,
      std::shared_ptr<ConnectorInsertTableHandle> /*insertHandle*/,
      ConnectorQueryCtx* /*queryCtx*/,
      CommitStrategy /*commitStrategy*/,
      const core::QueryConfig& /*queryConfig*/) override {
    BOLT_FAIL("paimon connector does not support writes");
  }

 private:
  std::shared_ptr<const config::ConfigBase> config_;
  folly::Executor* executor_;
};

class PaimonConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* kPaimonConnectorName{"paimon"};

  PaimonConnectorFactory();

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* executor) override {
    return std::make_shared<PaimonConnector>(id, config, executor);
  }

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const Config> /* config */,
      folly::Executor* FOLLY_NULLABLE executor) override {
    // Adapt legacy Config API to ConfigBase by creating a nullptr ConfigBase.
    // The PaimonConnector does not currently depend on specific config fields.
    std::shared_ptr<const config::ConfigBase> cfgBase;
    return std::make_shared<PaimonConnector>(id, cfgBase, executor);
  }
};

} // namespace bytedance::bolt::connector::paimon
