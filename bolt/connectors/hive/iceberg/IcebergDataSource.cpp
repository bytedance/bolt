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

#include "bolt/connectors/hive/iceberg/IcebergDataSource.h"

#include "bolt/connectors/hive/iceberg/IcebergSplitReader.h"

namespace bytedance::bolt::connector::hive::iceberg {

IcebergDataSource::IcebergDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& assignments,
    FileHandleFactory* fileHandleFactory,
    const core::QueryConfig& queryConfig,
    folly::Executor* executor,
    const std::shared_ptr<ConnectorQueryCtx>& connectorQueryCtx,
    const std::shared_ptr<HiveConfig>& hiveConfig)
    : HiveDataSource(
          outputType,
          tableHandle,
          assignments,
          fileHandleFactory,
          queryConfig,
          executor,
          connectorQueryCtx,
          hiveConfig) {}

std::unique_ptr<SplitReader> IcebergDataSource::createSplitReader(
    const std::shared_ptr<HiveConnectorSplit>& split,
    bool isPartOfPaimonSplit) {
  BOLT_CHECK(
      !isPartOfPaimonSplit, "IcebergDataSource doesn't support Paimon split");
  return std::make_unique<IcebergSplitReader>(
      split,
      hiveTableHandle_,
      topLevelFieldIdToHandle_,
      &partitionKeys_,
      connectorQueryCtx_.get(),
      hiveConfig_,
      readerOutputType_,
      ioStats_,
      fileHandleFactory_,
      executor_,
      scanSpec_);
}

} // namespace bytedance::bolt::connector::hive::iceberg
