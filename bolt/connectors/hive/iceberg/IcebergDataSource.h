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

#include "bolt/connectors/hive/HiveDataSource.h"

namespace bytedance::bolt::connector::hive::iceberg {

/// Iceberg-specific data source that extends HiveDataSource.
///
/// Provides Iceberg table format support by creating
/// IcebergSplitReader instances that handle:
/// - Positional delete files for row-level deletes.
/// - Schema evolution with column adaptation.
/// - Iceberg-specific metadata columns.
class IcebergDataSource : public HiveDataSource {
 public:
  IcebergDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& assignments,
      FileHandleFactory* fileHandleFactory,
      const core::QueryConfig& queryConfig,
      folly::Executor* executor,
      const std::shared_ptr<ConnectorQueryCtx>& connectorQueryCtx,
      const std::shared_ptr<HiveConfig>& hiveConfig);

 protected:
  /// Creates an IcebergSplitReader for reading Iceberg data files.
  ///
  /// Unlike the base HiveDataSource which creates a generic SplitReader,
  /// this method creates an IcebergSplitReader that handles Iceberg-specific
  /// features like positional delete files and schema evolution.
  std::unique_ptr<SplitReader> createSplitReader(
      const std::shared_ptr<HiveConnectorSplit>& split,
      bool isPartOfPaimonSplit) override;
};

} // namespace bytedance::bolt::connector::hive::iceberg
