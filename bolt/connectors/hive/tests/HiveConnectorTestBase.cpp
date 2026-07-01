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

#include "bolt/connectors/hive/tests/HiveConnectorTestBase.h"

#include <algorithm>
#include <utility>

#include "bolt/common/base/Exceptions.h"
#include "bolt/connectors/ConnectorOptions.h"

namespace bytedance::bolt::connector::hive {

std::shared_ptr<LocationHandle> HiveConnectorTestBase::makeLocationHandle(
    std::string targetDirectory,
    const std::optional<std::string>& writeDirectory,
    LocationHandle::TableType tableType) const {
  auto writePath = writeDirectory.value_or(targetDirectory);
  auto locationOptions = connector::makeOptions({
      {"targetPath", std::move(targetDirectory)},
      {"writePath", std::move(writePath)},
  });

  auto locationHandle = std::dynamic_pointer_cast<LocationHandle>(
      connectorObjectFactory()->makeLocationHandle(tableType, locationOptions));
  BOLT_CHECK_NOT_NULL(locationHandle);
  return locationHandle;
}

std::shared_ptr<HiveTableHandle> HiveConnectorTestBase::makeTableHandle(
    common::SubfieldFilters subfieldFilters,
    const core::TypedExprPtr& remainingFilter,
    const std::string& tableName,
    const RowTypePtr& dataColumns,
    bool filterPushdownEnabled) const {
  return std::make_shared<HiveTableHandle>(
      connectorId(),
      tableName,
      filterPushdownEnabled,
      std::move(subfieldFilters),
      remainingFilter,
      dataColumns);
}

std::shared_ptr<HiveColumnHandle> HiveConnectorTestBase::makeColumnHandle(
    const std::string& name,
    const TypePtr& type,
    const std::vector<std::string>& requiredSubfields) const {
  return makeColumnHandle(name, type, type, requiredSubfields);
}

std::shared_ptr<HiveColumnHandle> HiveConnectorTestBase::makeColumnHandle(
    const std::string& name,
    const TypePtr& dataType,
    const TypePtr& hiveType,
    const std::vector<std::string>& requiredSubfields) const {
  auto columnOptions = connector::makeOptions({
      {"columnType", static_cast<int>(HiveColumnHandle::ColumnType::kRegular)},
      {"hiveType", hiveType->serialize()},
  });
  columnOptions.options["requiredSubfields"] = folly::dynamic::array();
  for (const auto& subfield : requiredSubfields) {
    columnOptions.options["requiredSubfields"].push_back(subfield);
  }

  auto columnHandle = std::dynamic_pointer_cast<HiveColumnHandle>(
      connectorObjectFactory()->makeColumnHandle(
          name, dataType, columnOptions));
  BOLT_CHECK_NOT_NULL(columnHandle);
  return columnHandle;
}

std::shared_ptr<HiveInsertTableHandle>
HiveConnectorTestBase::makeHiveInsertTableHandle(
    const std::vector<std::string>& tableColumnNames,
    const std::vector<TypePtr>& tableColumnTypes,
    const std::vector<std::string>& partitionedBy,
    const std::shared_ptr<HiveBucketProperty>& bucketProperty,
    const std::shared_ptr<LocationHandle>& locationHandle,
    dwio::common::FileFormat tableStorageFormat,
    std::optional<common::CompressionKind> compressionKind) const {
  BOLT_CHECK_EQ(tableColumnNames.size(), tableColumnTypes.size());

  std::vector<std::string> bucketedBy;
  std::vector<std::shared_ptr<const HiveSortingColumn>> sortedBy;
  if (bucketProperty != nullptr) {
    bucketedBy = bucketProperty->bucketedBy();
    sortedBy = bucketProperty->sortedBy();
  }

  std::vector<std::shared_ptr<const connector::ColumnHandle>> columnHandles;
  columnHandles.reserve(tableColumnNames.size());
  int32_t numPartitionColumns{0};
  int32_t numSortingColumns{0};
  int32_t numBucketColumns{0};
  for (int i = 0; i < static_cast<int>(tableColumnNames.size()); ++i) {
    for (const auto& bucketColumn : bucketedBy) {
      if (bucketColumn == tableColumnNames[i]) {
        ++numBucketColumns;
      }
    }
    for (const auto& sortingColumn : sortedBy) {
      if (sortingColumn->sortColumn() == tableColumnNames[i]) {
        ++numSortingColumns;
      }
    }

    const auto columnType = std::find(
                                partitionedBy.cbegin(),
                                partitionedBy.cend(),
                                tableColumnNames[i]) != partitionedBy.cend()
        ? HiveColumnHandle::ColumnType::kPartitionKey
        : HiveColumnHandle::ColumnType::kRegular;
    if (columnType == HiveColumnHandle::ColumnType::kPartitionKey) {
      ++numPartitionColumns;
    }

    auto columnOptions = connector::makeOptions({
        {"columnType", static_cast<int>(columnType)},
        {"hiveType", tableColumnTypes[i]->serialize()},
    });
    columnHandles.push_back(
        connectorObjectFactory()->makeColumnHandle(
            tableColumnNames[i], tableColumnTypes[i], columnOptions));
  }
  BOLT_CHECK_EQ(numPartitionColumns, static_cast<int>(partitionedBy.size()));
  BOLT_CHECK_EQ(numBucketColumns, static_cast<int>(bucketedBy.size()));
  BOLT_CHECK_EQ(numSortingColumns, static_cast<int>(sortedBy.size()));

  auto insertOptions = connector::makeOptions({
      {"storageFormat", static_cast<int>(tableStorageFormat)},
  });
  if (bucketProperty != nullptr) {
    insertOptions.options["bucketProperty"] = bucketProperty->serialize();
  }
  if (compressionKind.has_value()) {
    insertOptions.options["compressionKind"] =
        static_cast<int>(compressionKind.value());
  }

  auto insertTableHandle = std::dynamic_pointer_cast<HiveInsertTableHandle>(
      connectorObjectFactory()->makeInsertTableHandle(
          std::move(columnHandles), locationHandle, insertOptions));
  BOLT_CHECK_NOT_NULL(insertTableHandle);
  return insertTableHandle;
}

std::shared_ptr<HiveInsertTableHandle>
HiveConnectorTestBase::makeHiveInsertTableHandle(
    const std::vector<std::string>& tableColumnNames,
    const std::vector<TypePtr>& tableColumnTypes,
    const std::vector<std::string>& partitionedBy,
    const std::shared_ptr<LocationHandle>& locationHandle,
    dwio::common::FileFormat tableStorageFormat,
    std::optional<common::CompressionKind> compressionKind) const {
  return makeHiveInsertTableHandle(
      tableColumnNames,
      tableColumnTypes,
      partitionedBy,
      nullptr,
      locationHandle,
      tableStorageFormat,
      compressionKind);
}

} // namespace bytedance::bolt::connector::hive
