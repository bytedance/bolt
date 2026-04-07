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
#include "bolt/connectors/hive/HiveObjectFactory.h"

#include <string>

#include <folly/dynamic.h>

#include "bolt/connectors/ConnectorNames.h"
#include "bolt/connectors/ConnectorOptions.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/connectors/hive/HiveDataSink.h"

namespace bytedance::bolt::connector::hive {

HiveObjectFactory::~HiveObjectFactory() = default;

using bytedance::bolt::common::Filter;
using bytedance::bolt::common::Subfield;

std::shared_ptr<ConnectorSplit> HiveObjectFactory::makeConnectorSplit(
    const std::string& filePath,
    uint64_t start,
    uint64_t length,
    const ConnectorOptions& optsBase) const {
  auto* dyn = dynamic_cast<const DynamicConnectorOptions*>(&optsBase);
  BOLT_CHECK(dyn != nullptr, "Expected DynamicConnectorOptions");
  const auto& options = dyn->options;
  dwio::common::FileFormat fileFormat =
      (!options.isNull() && options.count("fileFormat"))
      ? static_cast<dwio::common::FileFormat>(options["fileFormat"].asInt())
      : defaultFileFormat_;

  std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
  if (!options.isNull() && options.count("partitionKeys")) {
    for (auto& kv : options["partitionKeys"].items()) {
      partitionKeys[kv.first.asString()] = kv.second.isNull()
          ? std::nullopt
          : std::optional<std::string>(kv.second.asString());
    }
  }

  std::optional<int32_t> tableBucketNumber;
  if (!options.isNull() && options.count("tableBucketNumber")) {
    tableBucketNumber = options["tableBucketNumber"].asInt();
  }

  std::unordered_map<std::string, std::string> customSplitInfo;
  if (!options.isNull() && options.count("customSplitInfo")) {
    for (auto& kv : options["customSplitInfo"].items()) {
      customSplitInfo[kv.first.asString()] = kv.second.asString();
    }
  }

  std::shared_ptr<std::string> extraFileInfo;
  if (!options.isNull() && options.count("extraFileInfo")) {
    extraFileInfo = options["extraFileInfo"].isNull()
        ? std::shared_ptr<std::string>()
        : std::make_shared<std::string>(options["extraFileInfo"].asString());
  }

  std::unordered_map<std::string, std::string> serdeParameters;
  if (!options.isNull() && options.count("serdeParameters")) {
    for (auto& kv : options["serdeParameters"].items()) {
      serdeParameters[kv.first.asString()] = kv.second.asString();
    }
  }

  std::unique_ptr<HiveConnectorSplitCacheLimit> hiveConnectorSplitCacheLimit;
  if (!options.isNull() && options.count("hiveConnectorSplitCacheLimit")) {
    hiveConnectorSplitCacheLimit = HiveConnectorSplitCacheLimit::create(
        options["hiveConnectorSplitCacheLimit"]);
  }

  uint64_t fileSize = 0;
  if (!options.isNull() && options.count("fileProperties")) {
    const auto& propertiesOption = options["fileProperties"];
    if (propertiesOption.count("fileSize") &&
        !propertiesOption["fileSize"].isNull()) {
      fileSize = propertiesOption["fileSize"].asInt();
    }
  }

  std::optional<RowIdProperties> rowIdProperties;
  if (!options.isNull() && options.count("rowIdProperties")) {
    RowIdProperties props;
    const auto& rowIdPropertiesOption = options["rowIdProperties"];
    props.metadataVersion = rowIdPropertiesOption["metadataVersion"].asInt();
    props.partitionId = rowIdPropertiesOption["partitionId"].asInt();
    props.tableGuid = rowIdPropertiesOption["tableGuid"].asString();
    rowIdProperties = props;
  }

  std::unordered_map<std::string, std::string> infoColumns;
  if (!options.isNull() && options.count("infoColumns")) {
    for (auto& kv : options["infoColumns"].items()) {
      infoColumns[kv.first.asString()] = kv.second.asString();
    }
  }

  return std::make_shared<HiveConnectorSplit>(
      connectorId(),
      filePath,
      fileFormat,
      start,
      length,
      partitionKeys,
      tableBucketNumber,
      std::move(hiveConnectorSplitCacheLimit),
      customSplitInfo,
      extraFileInfo,
      serdeParameters,
      fileSize,
      rowIdProperties,
      infoColumns);
}

std::shared_ptr<connector::ColumnHandle> HiveObjectFactory::makeColumnHandle(
    const std::string& name,
    const TypePtr& dataType,
    const ConnectorOptions& optsBase) const {
  auto* dyn = dynamic_cast<const DynamicConnectorOptions*>(&optsBase);
  BOLT_CHECK(dyn != nullptr, "Expected DynamicConnectorOptions");
  const auto& options = dyn->options;
  using HiveColumnType = hive::HiveColumnHandle::ColumnType;

  if (options.isNull()) {
    return std::make_shared<HiveColumnHandle>(
        name, HiveColumnType::kRegular, dataType, dataType);
  }

  HiveColumnType hiveColumnType;

  // columnType would be serialized as int32_t
  int32_t columnType =
      options
          .getDefault("columnType", static_cast<int>(HiveColumnType::kRegular))
          .asInt();

  switch (columnType) {
    case static_cast<int>(HiveColumnType::kRegular):
      hiveColumnType = HiveColumnType::kRegular;
      break;
    case static_cast<int>(HiveColumnType::kPartitionKey):
      hiveColumnType = HiveColumnType::kPartitionKey;
      break;
    case static_cast<int>(HiveColumnType::kSynthesized):
      hiveColumnType = HiveColumnType::kSynthesized;
      break;

    default:
      BOLT_UNSUPPORTED("Unsupported ColumnType ", columnType);
  }

  TypePtr hiveType = ISerializable::deserialize<Type>(
      options.getDefault("hiveType", dataType->serialize()), nullptr);

  //  subfields would be serialized as a vector of strings;
  std::vector<common::Subfield> subfields;
  if (auto rs = options.get_ptr("requiredSubfields")) {
    subfields.reserve(rs->size());
    for (auto& v : *rs) {
      subfields.emplace_back(v.asString());
    }
  }

  return std::make_shared<HiveColumnHandle>(
      name, hiveColumnType, dataType, hiveType, std::move(subfields));
}

std::shared_ptr<ConnectorTableHandle> HiveObjectFactory::makeTableHandle(
    const std::string& tableName,
    const std::vector<std::shared_ptr<const connector::ColumnHandle>>&
        columnHandles,
    const ConnectorOptions& optsBase) const {
  auto* dyn = dynamic_cast<const DynamicConnectorOptions*>(&optsBase);
  BOLT_CHECK(dyn != nullptr, "Expected DynamicConnectorOptions");
  const auto& options = dyn->options;
  bool filterPushdownEnabled =
      options.getDefault("filterPushdownEnabled", true).asBool();

  common::SubfieldFilters subfieldFilters;
  if (auto sf = options.get_ptr("subfieldFilters")) {
    subfieldFilters.reserve(sf->size());

    for (auto& kv : sf->items()) {
      // 1) Parse the key string into a Subfield
      //    (uses Subfield(const std::string&) and default separators)
      Subfield subfield(kv.first.asString());

      // 2) Deserialize the Filter from its dynamic form.
      //    Assumes every Filter subclass registered a SerDe entry in
      //    Filter::registerSerDe().
      auto filter = ISerializable::deserialize<Filter>(
          kv.second, /* context = */ nullptr);

      subfieldFilters.emplace(std::move(subfield), filter->clone());
    }
  }

  core::TypedExprPtr remainingFilter = nullptr;
  if (auto rf = options.get_ptr("remainingFilter")) {
    // assuming rf["expr"] holds the serialized expression
    remainingFilter = ISerializable::deserialize<core::ITypedExpr>(*rf);
  }

  std::unordered_map<std::string, std::string> tableParameters;
  if (auto tp = options.get_ptr("tableParameters")) {
    for (auto& kv : tp->items()) {
      tableParameters.emplace(kv.first.asString(), kv.second.asString());
    }
  }

  // build RowTypePtr from columnHandles
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(columnHandles.size());
  types.reserve(columnHandles.size());
  for (auto& col : columnHandles) {
    auto hiveCol = std::static_pointer_cast<const HiveColumnHandle>(col);
    names.push_back(hiveCol->name());
    types.push_back(hiveCol->dataType());
  }
  auto dataColumns = ROW(std::move(names), std::move(types));

  return std::make_shared<HiveTableHandle>(
      connectorId(),
      tableName,
      filterPushdownEnabled,
      std::move(subfieldFilters),
      std::move(remainingFilter),
      dataColumns,
      std::move(tableParameters));
}

std::shared_ptr<ConnectorInsertTableHandle>
HiveObjectFactory::makeInsertTableHandle(
    const std::vector<std::shared_ptr<const connector::ColumnHandle>>&
        inputColumns,
    const std::shared_ptr<const ConnectorLocationHandle>& locationHandle,
    const ConnectorOptions& optsBase) const {
  auto* dyn = dynamic_cast<const DynamicConnectorOptions*>(&optsBase);
  BOLT_CHECK(dyn != nullptr, "Expected DynamicConnectorOptions");
  const auto& options = dyn->options;
  // Convert inputColumns
  std::vector<std::shared_ptr<const HiveColumnHandle>> inputHiveColumns;
  inputHiveColumns.reserve(inputColumns.size());
  for (const auto& handle : inputColumns) {
    inputHiveColumns.push_back(
        std::static_pointer_cast<const HiveColumnHandle>(handle));
  }

  auto hiveLoc =
      std::dynamic_pointer_cast<const hive::LocationHandle>(locationHandle);
  BOLT_CHECK(
      hiveLoc,
      "HiveObjectFactory::makeInsertTableHandle: "
      "expected HiveLocationHandle");

  auto fmt =
      options
          .getDefault(
              "storageFormat", static_cast<int>(dwio::common::FileFormat::DWRF))
          .asInt();
  auto storageFormat = static_cast<dwio::common::FileFormat>(fmt);

  std::shared_ptr<HiveBucketProperty> bucketProperty = nullptr;
  if (auto bp = options.get_ptr("bucketProperty")) {
    bucketProperty = HiveBucketProperty::deserialize(*bp, nullptr);
  }

  std::optional<common::CompressionKind> compressionKind;
  if (auto ck = options.get_ptr("compressionKind")) {
    compressionKind = static_cast<common::CompressionKind>(ck->asInt());
  }

  std::unordered_map<std::string, std::string> serdeParameters;
  if (auto sp = options.get_ptr("serdeParameters")) {
    for (auto& kv : sp->items()) {
      serdeParameters.emplace(kv.first.asString(), kv.second.asString());
    }
  }

  std::shared_ptr<dwio::common::WriterOptions> writerOptions = nullptr;
  if (auto wo = options.get_ptr("writerOptions")) {
    writerOptions = dwio::common::WriterOptions::create(*wo);
  }

  return std::make_shared<HiveInsertTableHandle>(
      std::move(inputHiveColumns),
      hiveLoc,
      storageFormat,
      std::move(bucketProperty),
      compressionKind,
      std::move(serdeParameters),
      std::move(writerOptions));
}

std::shared_ptr<ConnectorLocationHandle> HiveObjectFactory::makeLocationHandle(
    LocationHandle::TableType tableType,
    const ConnectorOptions& options) const {
  auto* dyn = dynamic_cast<const DynamicConnectorOptions*>(&options);
  BOLT_CHECK(dyn != nullptr, "Expected DynamicConnectorOptions");
  const auto& dynOptions = dyn->options;
  BOLT_CHECK(dynOptions.isObject(), "Expected options to be a dynamic object");

  // Required fields
  auto targetPath = dynOptions.at("targetPath").asString();
  auto writePath = dynOptions.at("writePath").asString();

  // Optional field: targetFileName
  std::string targetFileName;
  if (dynOptions.count("targetFileName") &&
      !dynOptions.at("targetFileName").isNull()) {
    targetFileName = dynOptions.at("targetFileName").asString();
  }

  return std::make_shared<LocationHandle>(
      std::move(targetPath),
      std::move(writePath),
      tableType,
      std::move(targetFileName),
      connectorId());
}

} // namespace bytedance::bolt::connector::hive
