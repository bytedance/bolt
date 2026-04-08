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
 *
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

#include "bolt/connectors/hive/HiveConnectorSplit.h"
namespace bytedance::bolt::connector::hive {

folly::dynamic HiveConnectorSplitCacheLimit::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["tableAndDataPartitionRangeOpen"] = tableAndDataPartitionRangeOpen;

  folly::dynamic tableAndDataPartitionRangeMapObj = folly::dynamic::object;
  for (const auto& [key, value] : tableAndDataPartitionRangeMap) {
    tableAndDataPartitionRangeMapObj[key] = value;
  }
  obj["tableAndDataPartitionRangeMap"] = tableAndDataPartitionRangeMapObj;

  obj["splitSoftAffinityCacheable"] = splitSoftAffinityCacheable;

  folly::dynamic tableAndColumnCacheMapObj = folly::dynamic::object;
  for (const auto& [key, values] : tableAndColumnCacheMap) {
    folly::dynamic values_ = folly::dynamic::array;

    for (const auto& item : values) {
      values_.push_back(item);
    }
    tableAndColumnCacheMapObj[key] = values_;
  }
  obj["tableAndColumnCacheMap"] = tableAndColumnCacheMapObj;

  return obj;
}

// static
std::unique_ptr<HiveConnectorSplitCacheLimit>
HiveConnectorSplitCacheLimit::create(const folly::dynamic& obj) {
  auto tableAndDataPartitionRangeOpen =
      obj["tableAndDataPartitionRangeOpen"].asBool();

  std::unordered_map<std::string, int> tableAndDataPartitionRangeMap;
  for (const auto& [key, value] :
       obj["tableAndDataPartitionRangeMap"].items()) {
    tableAndDataPartitionRangeMap[key.asString()] = value.asInt();
  }

  auto splitSoftAffinityCacheable = obj["splitSoftAffinityCacheable"].asBool();

  std::unordered_map<std::string, std::vector<int>> tableAndColumnCacheMap;
  for (const auto& [key, values] : obj["tableAndColumnCacheMap"].items()) {
    std::vector<int> valueVec;

    if (values.isArray()) {
      for (const auto& item : values) {
        valueVec.push_back(item.asInt());
      }
    }

    tableAndColumnCacheMap[key.asString()] = valueVec;
  }

  return std::make_unique<HiveConnectorSplitCacheLimit>(
      tableAndDataPartitionRangeOpen,
      tableAndDataPartitionRangeMap,
      splitSoftAffinityCacheable,
      tableAndColumnCacheMap);
}

// static
std::shared_ptr<HiveConnectorSplit> HiveConnectorSplit::create(
    const folly::dynamic& obj) {
  const auto connectorId = obj["connectorId"].asString();
  const auto splitWeight = obj["splitWeight"].asInt();
  const auto filePath = obj["filePath"].asString();
  const auto fileFormat =
      dwio::common::toFileFormat(obj["fileFormat"].asString());
  const auto start = static_cast<uint64_t>(obj["start"].asInt());
  const auto length = static_cast<uint64_t>(obj["length"].asInt());

  std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
  for (const auto& [key, value] : obj["partitionKeys"].items()) {
    partitionKeys[key.asString()] = value.isNull()
        ? std::nullopt
        : std::optional<std::string>(value.asString());
  }

  const auto tableBucketNumber = obj["tableBucketNumber"].isNull()
      ? std::nullopt
      : std::optional<int32_t>(obj["tableBucketNumber"].asInt());

  std::unordered_map<std::string, std::string> customSplitInfo;
  for (const auto& [key, value] : obj["customSplitInfo"].items()) {
    customSplitInfo[key.asString()] = value.asString();
  }

  std::shared_ptr<std::string> extraFileInfo = obj["extraFileInfo"].isNull()
      ? nullptr
      : std::make_shared<std::string>(obj["extraFileInfo"].asString());
  std::unordered_map<std::string, std::string> serdeParameters;
  for (const auto& [key, value] : obj["serdeParameters"].items()) {
    serdeParameters[key.asString()] = value.asString();
  }

  std::unordered_map<std::string, std::string> infoColumns;
  const auto& infoColumnsObj = obj.getDefault("infoColumns", nullptr);
  if (infoColumnsObj != nullptr) {
    for (const auto& [key, value] : infoColumnsObj.items()) {
      infoColumns[key.asString()] = value.asString();
    }
  }

  uint64_t fileSize = static_cast<uint64_t>(obj["fileSize"].asInt());

  std::unique_ptr<HiveConnectorSplitCacheLimit> hiveConnectorSplitCacheLimit =
      nullptr;
  const auto& hiveConnectorSplitCacheLimitObj =
      obj.getDefault("hiveConnectorSplitCacheLimit", nullptr);
  if (hiveConnectorSplitCacheLimitObj != nullptr) {
    hiveConnectorSplitCacheLimit =
        HiveConnectorSplitCacheLimit::create(hiveConnectorSplitCacheLimitObj);
  }

  std::optional<RowIdProperties> rowIdProperties = std::nullopt;
  const auto& rowIdObj = obj.getDefault("rowIdProperties", nullptr);
  if (rowIdObj != nullptr) {
    rowIdProperties = RowIdProperties{
        .metadataVersion = rowIdObj["metadataVersion"].asInt(),
        .partitionId = rowIdObj["partitionId"].asInt(),
        .tableGuid = rowIdObj["tableGuid"].asString()};
  }

  return std::make_shared<HiveConnectorSplit>(
      connectorId,
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

// static
void HiveConnectorSplit::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("HiveConnectorSplit", HiveConnectorSplit::create);
}
} // namespace bytedance::bolt::connector::hive
