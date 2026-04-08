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
#pragma once

#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/connectors/hive/iceberg/IcebergConfig.h"
#include "bolt/connectors/hive/iceberg/IcebergDataSink.h"

namespace bytedance::bolt::connector::hive::iceberg {

/// Provides Iceberg table format support.
/// - Creates IcebergDataSource instances for reading Iceberg tables with
///   support for delete files and schema evolution.
/// - Creates IcebergDataSink instances for writing data with Iceberg-specific
///   partition transforms and commit metadata.
class IcebergConnector final : public HiveConnector {
 public:
  IcebergConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* executor);

  /// Creates IcebergDataSource for reading from Iceberg tables.
  ///
  /// @param outputType The schema of the output data to read.
  /// @param tableHandle The table handle containing table metadata.
  /// @param columnHandles Map of column names to column handles.
  /// @param connectorQueryCtx Query context for the read operation.
  /// @return IcebergDataSource instance configured for the read operation.
  std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      std::shared_ptr<ConnectorQueryCtx> connectorQueryCtx,
      const core::QueryConfig& queryConfig) override;

  /// Creates IcebergDataSink for writing to Iceberg tables.
  ///
  /// @param inputType The schema of the input data to write.
  /// @param connectorInsertTableHandle Must be an IcebergInsertTableHandle
  ///        containing Iceberg-specific write configuration.
  /// @param connectorQueryCtx Query context for the write operation.
  /// @param commitStrategy Strategy for committing the write operation. Files
  ///        are written directly with their final names and commit metadata is
  ///        returned for the coordinator to update the Iceberg metadata tables.
  /// @return IcebergDataSink instance configured for the write operation.
  std::unique_ptr<DataSink> createDataSink(
      RowTypePtr inputType,
      std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
      ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy,
      const core::QueryConfig& queryConfig) override;

 private:
  const std::shared_ptr<IcebergConfig> icebergConfig_;
};

class IcebergConnectorFactory final : public hive::HiveConnectorFactory {
 public:
  static constexpr const char* kIcebergConnectorName = "iceberg";

  IcebergConnectorFactory() : HiveConnectorFactory(kIcebergConnectorName) {}

  /// Creates a new IcebergConnector instance.
  ///
  /// @param id Unique identifier for this connector instance.
  /// @param config Connector configuration properties.
  /// @param executor Optional executor for asynchronous I/O operations.
  /// @return Shared pointer to the newly created IcebergConnector instance.
  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* executor = nullptr) override {
    return std::make_shared<IcebergConnector>(id, config, executor);
  }
};

template <typename T = IcebergConnectorFactory>
bool CheckIcebergConnectorFactoryInit() {
  static bool init = bytedance::bolt::connector::registerConnectorFactory(
      std::make_shared<T>());
  return init;
}

} // namespace bytedance::bolt::connector::hive::iceberg
