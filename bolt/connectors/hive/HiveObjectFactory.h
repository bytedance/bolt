/*
 * Copyright (c) International Business Machines Corporation
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

#include "bolt/connectors/ConnectorNames.h"
#include "bolt/connectors/ConnectorObjectFactory.h"
#include "bolt/dwio/common/Options.h"

namespace bytedance::bolt::connector::hive {

/// Hive-specific implementation of ConnectorObjectFactory. Deserializes Hive
/// connector objects (splits, column handles, table handles, insert handles,
/// and location handles) from a DynamicConnectorOptions payload. The
/// connectorId passed at construction is embedded in objects that require it
/// (e.g. HiveConnectorSplit, HiveTableHandle, LocationHandle).
class HiveObjectFactory : public connector::ConnectorObjectFactory {
 public:
  /// @param connectorId The id of the Hive connector instance this factory
  ///                    serves (e.g. "hive", "hive1").
  explicit HiveObjectFactory(std::string connectorId)
      : ConnectorObjectFactory(
            connector::kHiveConnectorName,
            std::move(connectorId)) {}

  ~HiveObjectFactory() override;

  /// Deserializes a HiveConnectorSplit. Reads fileFormat, partitionKeys,
  /// tableBucketNumber, customSplitInfo, extraFileInfo, serdeParameters,
  /// fileSize, rowIdProperties, and infoColumns from @p options.
  std::shared_ptr<connector::ConnectorSplit> makeConnectorSplit(
      const std::string& filePath,
      uint64_t start,
      uint64_t length,
      const ConnectorOptions& options) const override;

  /// Deserializes a HiveColumnHandle. Reads columnType, hiveType, and
  /// requiredSubfields from @p options. If @p options is null, returns a
  /// regular column handle with dataType as both data and hive type.
  std::shared_ptr<connector::ColumnHandle> makeColumnHandle(
      const std::string& name,
      const TypePtr& type,
      const ConnectorOptions& options) const override;

  /// Deserializes a HiveTableHandle. Reads filterPushdownEnabled,
  /// subfieldFilters, remainingFilter, and tableParameters from @p options.
  /// The dataColumns schema is derived from @p columnHandles.
  std::shared_ptr<ConnectorTableHandle> makeTableHandle(
      const std::string& tableName,
      const std::vector<std::shared_ptr<const connector::ColumnHandle>>&
          columnHandles,
      const ConnectorOptions& options) const override;

  /// Deserializes a HiveInsertTableHandle. Reads storageFormat, bucketProperty,
  /// compressionKind, serdeParameters, and writerOptions from @p options.
  std::shared_ptr<ConnectorInsertTableHandle> makeInsertTableHandle(
      const std::vector<std::shared_ptr<const connector::ColumnHandle>>&
          inputColumns,
      const std::shared_ptr<const ConnectorLocationHandle>& locationHandle,
      const ConnectorOptions& options) const override;

  /// Deserializes a LocationHandle. Reads targetPath, writePath, and
  /// optionally targetFileName from @p options.
  std::shared_ptr<connector::ConnectorLocationHandle> makeLocationHandle(
      connector::ConnectorLocationHandle::TableType tableType,
      const ConnectorOptions& options) const override;

 private:
  dwio::common::FileFormat defaultFileFormat_{
      dwio::common::FileFormat::PARQUET};
};

} // namespace bytedance::bolt::connector::hive
