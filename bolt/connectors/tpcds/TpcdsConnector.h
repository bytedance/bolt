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
 */
/*
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
#pragma once
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/tpcds/TpcdsConnectorSplit.h"
#include "bolt/tpcds/gen/TpcdsGen.h"
namespace bytedance::bolt::connector::tpcds {
class TpcdsConnector;
// TPC-DS column handle only needs the column name (all columns are generated
// in the same way).
class TpcdsColumnHandle : public bolt::connector::ColumnHandle {
 public:
  explicit TpcdsColumnHandle(const std::string& name) : name_(name) {}
  const std::string& name() const {
    return name_;
  }

 private:
  const std::string name_;
};
// TPC-DS table handle uses the underlying enum to describe the target table.
class TpcdsTableHandle : public ConnectorTableHandle {
 public:
  explicit TpcdsTableHandle(
      std::string connectorId,
      bolt::tpcds::Table table,
      double scaleFactor = 0.01)
      : ConnectorTableHandle(std::move(connectorId)),
        table_(table),
        name_(toTableName(table)),
        scaleFactor_(scaleFactor) {
    BOLT_CHECK_GT(scaleFactor, 0.0, "Tpcds scale factor must be non-negative");
  }
  const std::string& name() const override {
    return name_;
  }
  std::string toString() const override;
  bolt::tpcds::Table getTpcdsTable() const {
    return table_;
  }
  double getScaleFactor() const {
    return scaleFactor_;
  }

 private:
  const bolt::tpcds::Table table_;
  const std::string name_;
  const double scaleFactor_;
};
class TpcdsDataSource : public bolt::connector::DataSource {
 public:
  TpcdsDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<const bolt::connector::ConnectorTableHandle>&
          tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<bolt::connector::ColumnHandle>>& columnHandles,
      bolt::memory::MemoryPool* FOLLY_NONNULL pool);
  void addSplit(std::shared_ptr<ConnectorSplit> split) override;
  void addDynamicFilter(
      column_index_t /*outputChannel*/,
      const std::shared_ptr<common::Filter>& /*filter*/) override {
    BOLT_NYI("Dynamic filters not supported by TpcdsConnector.");
  }
  std::optional<RowVectorPtr> next(uint64_t size, bolt::ContinueFuture& future)
      override;
  uint64_t getCompletedRows() override {
    return completedRows_;
  }
  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }
  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    return {};
  }

 private:
  RowVectorPtr projectOutputColumns(RowVectorPtr vector);
  bolt::tpcds::Table table_;
  double scaleFactor_{0.01};
  size_t rowCount_{0};
  RowTypePtr outputType_;
  // Mapping between output columns and their indices (column_index_t) in the
  // dsdgen generated datasets.
  std::vector<column_index_t> outputColumnMappings_;
  std::shared_ptr<connector::tpcds::TpcdsConnectorSplit> currentSplit_;
  // Offset of the first row in current split.
  uint64_t splitOffset_{0};
  // Offset of the last row in current split.
  uint64_t splitEnd_{0};
  size_t completedRows_{0};
  size_t completedBytes_{0};
  memory::MemoryPool* FOLLY_NONNULL pool_;
};
class TpcdsConnector final : public bolt::connector::Connector {
 public:
  TpcdsConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* FOLLY_NULLABLE /*executor*/)
      : Connector(id) {}
  std::unique_ptr<DataSource> createDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      std::shared_ptr<ConnectorQueryCtx> connectorQueryCtx,
      const core::QueryConfig& queryConfig) override final {
    return std::make_unique<TpcdsDataSource>(
        outputType,
        tableHandle,
        columnHandles,
        connectorQueryCtx->memoryPool());
  }
  std::unique_ptr<DataSink> createDataSink(
      RowTypePtr /*inputType*/,
      std::shared_ptr<
          ConnectorInsertTableHandle> /*connectorInsertTableHandle*/,
      ConnectorQueryCtx* /*connectorQueryCtx*/,
      CommitStrategy /*commitStrategy*/,
      const core::QueryConfig& /*queryConfig*/) override final {
    BOLT_NYI("TpcdsConnector does not support data sink.");
  }
};
class TpcdsConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* kTpcdsConnectorName{"tpcds"};
  TpcdsConnectorFactory() : ConnectorFactory(kTpcdsConnectorName) {}
  explicit TpcdsConnectorFactory(const char* connectorName)
      : ConnectorFactory(connectorName) {}
  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* executor = nullptr) override {
    return std::make_shared<TpcdsConnector>(id, config, executor);
  }
  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const Config> config,
      folly::Executor* FOLLY_NULLABLE executor = nullptr) override {
    std::shared_ptr<const config::ConfigBase> convertedConfig;
    convertedConfig = config == nullptr
        ? nullptr
        : std::make_shared<config::ConfigBase>(config->valuesCopy());
    return newConnector(id, convertedConfig, executor);
  }
};
} // namespace bytedance::bolt::connector::tpcds
