/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "bolt/connectors/Connector.h"
#include "bolt/connectors/paimon/PaimonTableHandle.h"

namespace bytedance::bolt::connector::paimon {

class PaimonConnector : public Connector {
 public:
  PaimonConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* executor)
      : Connector(id), executor_(executor) {}

  std::unique_ptr<DataSource> createDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<ColumnHandle>>& columnHandles,
      std::shared_ptr<ConnectorQueryCtx> queryCtx,
      const core::QueryConfig& queryConfig) override;

  std::unique_ptr<DataSink> createDataSink(
      std::shared_ptr<const RowType> inputType,
      std::shared_ptr<ConnectorInsertTableHandle> insertHandle,
      ConnectorQueryCtx* queryCtx,
      CommitStrategy commitStrategy,
      const core::QueryConfig& queryConfig) override {
     return nullptr;
  }

 private:
  folly::Executor* executor_;
};

class PaimonConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* kPaimonConnectorName{"paimon"};

  PaimonConnectorFactory() : ConnectorFactory(kPaimonConnectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* executor = nullptr) override {
    return std::make_shared<PaimonConnector>(id, config, executor);
  }
};

} // namespace bytedance::bolt::connector::paimon
