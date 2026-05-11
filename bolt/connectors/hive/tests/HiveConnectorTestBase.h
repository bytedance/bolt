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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "bolt/connectors/hive/HiveDataSink.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/exec/tests/utils/ConnectorTestBase.h"

namespace bytedance::bolt::connector::hive {

class HiveConnectorTestBase : public exec::test::ConnectorTestBase {
 protected:
  std::shared_ptr<LocationHandle> makeLocationHandle(
      std::string targetDirectory,
      const std::optional<std::string>& writeDirectory = std::nullopt,
      LocationHandle::TableType tableType =
          LocationHandle::TableType::kNew) const;

  std::shared_ptr<HiveTableHandle> makeTableHandle(
      common::SubfieldFilters subfieldFilters = {},
      const core::TypedExprPtr& remainingFilter = nullptr,
      const std::string& tableName = "hive_table",
      const RowTypePtr& dataColumns = nullptr,
      bool filterPushdownEnabled = true) const;

  std::shared_ptr<HiveColumnHandle> makeColumnHandle(
      const std::string& name,
      const TypePtr& type,
      const std::vector<std::string>& requiredSubfields = {}) const;

  std::shared_ptr<HiveColumnHandle> makeColumnHandle(
      const std::string& name,
      const TypePtr& dataType,
      const TypePtr& hiveType,
      const std::vector<std::string>& requiredSubfields = {}) const;

  std::shared_ptr<HiveInsertTableHandle> makeHiveInsertTableHandle(
      const std::vector<std::string>& tableColumnNames,
      const std::vector<TypePtr>& tableColumnTypes,
      const std::vector<std::string>& partitionedBy,
      const std::shared_ptr<HiveBucketProperty>& bucketProperty,
      const std::shared_ptr<LocationHandle>& locationHandle,
      dwio::common::FileFormat tableStorageFormat =
          dwio::common::FileFormat::DWRF,
      std::optional<common::CompressionKind> compressionKind =
          std::nullopt) const;

  std::shared_ptr<HiveInsertTableHandle> makeHiveInsertTableHandle(
      const std::vector<std::string>& tableColumnNames,
      const std::vector<TypePtr>& tableColumnTypes,
      const std::vector<std::string>& partitionedBy,
      const std::shared_ptr<LocationHandle>& locationHandle,
      dwio::common::FileFormat tableStorageFormat =
          dwio::common::FileFormat::DWRF,
      std::optional<common::CompressionKind> compressionKind =
          std::nullopt) const;
};

} // namespace bytedance::bolt::connector::hive
