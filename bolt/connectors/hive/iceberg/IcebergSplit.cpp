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

#include "bolt/connectors/hive/iceberg/IcebergSplit.h"

#include "bolt/connectors/hive/iceberg/IcebergDeleteFile.h"
#include "folly/Conv.h"
#include "folly/json.h"

namespace bytedance::bolt::connector::hive::iceberg {

namespace {

folly::dynamic serializeDeleteFile(const IcebergDeleteFile& deleteFile) {
  folly::dynamic obj = folly::dynamic::object;
  obj["content"] = static_cast<int64_t>(deleteFile.content);
  obj["filePath"] = deleteFile.filePath;
  obj["fileFormat"] = dwio::common::toString(deleteFile.fileFormat);
  obj["recordCount"] = static_cast<int64_t>(deleteFile.recordCount);
  obj["fileSizeInBytes"] = static_cast<int64_t>(deleteFile.fileSizeInBytes);
  obj["equalityFieldIds"] =
      ISerializable::serialize(deleteFile.equalityFieldIds);

  folly::dynamic lowerBounds = folly::dynamic::object;
  for (const auto& [key, value] : deleteFile.lowerBounds) {
    lowerBounds[std::to_string(key)] = value;
  }
  obj["lowerBounds"] = std::move(lowerBounds);

  folly::dynamic upperBounds = folly::dynamic::object;
  for (const auto& [key, value] : deleteFile.upperBounds) {
    upperBounds[std::to_string(key)] = value;
  }
  obj["upperBounds"] = std::move(upperBounds);

  return obj;
}

IcebergDeleteFile deserializeDeleteFile(const folly::dynamic& obj) {
  std::vector<int32_t> equalityFieldIds;
  const auto& equalityFieldIdsObj = obj.getDefault("equalityFieldIds", nullptr);
  if (equalityFieldIdsObj != nullptr) {
    for (const auto& fieldId : equalityFieldIdsObj) {
      equalityFieldIds.push_back(fieldId.asInt());
    }
  }

  std::unordered_map<int32_t, std::string> lowerBounds;
  const auto& lowerBoundsObj = obj.getDefault("lowerBounds", nullptr);
  if (lowerBoundsObj != nullptr) {
    for (const auto& [key, value] : lowerBoundsObj.items()) {
      lowerBounds[folly::to<int32_t>(key.asString())] = value.asString();
    }
  }

  std::unordered_map<int32_t, std::string> upperBounds;
  const auto& upperBoundsObj = obj.getDefault("upperBounds", nullptr);
  if (upperBoundsObj != nullptr) {
    for (const auto& [key, value] : upperBoundsObj.items()) {
      upperBounds[folly::to<int32_t>(key.asString())] = value.asString();
    }
  }

  return IcebergDeleteFile(
      static_cast<FileContent>(obj["content"].asInt()),
      obj["filePath"].asString(),
      dwio::common::toFileFormat(obj["fileFormat"].asString()),
      static_cast<uint64_t>(obj["recordCount"].asInt()),
      static_cast<uint64_t>(obj["fileSizeInBytes"].asInt()),
      std::move(equalityFieldIds),
      std::move(lowerBounds),
      std::move(upperBounds));
}

std::shared_ptr<std::string> serializeDeleteFiles(
    const std::vector<IcebergDeleteFile>& deleteFiles) {
  folly::dynamic payload = folly::dynamic::object;
  payload["deleteFiles"] = folly::dynamic::array;
  for (const auto& deleteFile : deleteFiles) {
    payload["deleteFiles"].push_back(serializeDeleteFile(deleteFile));
  }
  return std::make_shared<std::string>(folly::toJson(payload));
}

std::vector<IcebergDeleteFile> deserializeDeleteFiles(
    const std::shared_ptr<std::string>& extraFileInfo) {
  if (!extraFileInfo || extraFileInfo->empty()) {
    return {};
  }

  auto payload = folly::parseJson(*extraFileInfo);
  const auto& deleteFilesObj = payload.getDefault("deleteFiles", nullptr);
  if (deleteFilesObj == nullptr) {
    return {};
  }

  std::vector<IcebergDeleteFile> deleteFiles;
  deleteFiles.reserve(deleteFilesObj.size());
  for (const auto& deleteFileObj : deleteFilesObj) {
    deleteFiles.push_back(deserializeDeleteFile(deleteFileObj));
  }
  return deleteFiles;
}

} // namespace

HiveIcebergSplit::HiveIcebergSplit(
    const std::string& connectorId,
    const std::string& filePath,
    dwio::common::FileFormat fileFormat,
    uint64_t start,
    uint64_t length,
    const std::unordered_map<std::string, std::optional<std::string>>&
        partitionKeys,
    std::optional<int32_t> tableBucketNumber,
    const std::unordered_map<std::string, std::string>& customSplitInfo,
    const std::shared_ptr<std::string>& extraFileInfo,
    bool /*cacheable*/,
    const std::unordered_map<std::string, std::string>& infoColumns,
    std::optional<FileProperties> /*properties*/)
    : HiveConnectorSplit(
          connectorId,
          filePath,
          fileFormat,
          start,
          length,
          partitionKeys,
          tableBucketNumber,
          nullptr,
          customSplitInfo,
          extraFileInfo,
          /*serdeParameters=*/{},
          /*fileSize=*/0,
          std::nullopt,
          infoColumns),
      deleteFiles(deserializeDeleteFiles(extraFileInfo)) {}

// For tests only
HiveIcebergSplit::HiveIcebergSplit(
    const std::string& connectorId,
    const std::string& filePath,
    dwio::common::FileFormat fileFormat,
    uint64_t start,
    uint64_t length,
    const std::unordered_map<std::string, std::optional<std::string>>&
        partitionKeys,
    std::optional<int32_t> tableBucketNumber,
    const std::unordered_map<std::string, std::string>& customSplitInfo,
    const std::shared_ptr<std::string>& extraFileInfo,
    bool /*cacheable*/,
    std::vector<IcebergDeleteFile> deletes,
    const std::unordered_map<std::string, std::string>& infoColumns,
    std::optional<FileProperties> /*properties*/)
    : HiveConnectorSplit(
          connectorId,
          filePath,
          fileFormat,
          start,
          length,
          partitionKeys,
          tableBucketNumber,
          nullptr,
          customSplitInfo,
          extraFileInfo,
          /*serdeParameters=*/{},
          /*fileSize=*/0,
          std::nullopt,
          infoColumns),
      deleteFiles(std::move(deletes)) {}

folly::dynamic HiveIcebergSplit::serialize() const {
  auto obj = HiveConnectorSplit::serialize();
  obj["name"] = "HiveIcebergSplit";
  obj["extraFileInfo"] = deleteFiles.empty()
      ? (extraFileInfo == nullptr ? nullptr : folly::dynamic(*extraFileInfo))
      : folly::dynamic(*serializeDeleteFiles(deleteFiles));
  return obj;
}

// static
std::shared_ptr<HiveIcebergSplit> HiveIcebergSplit::create(
    const folly::dynamic& obj) {
  BOLT_CHECK_EQ(obj["name"].asString(), "HiveIcebergSplit");

  const auto connectorId = obj["connectorId"].asString();
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

  std::unordered_map<std::string, std::string> infoColumns;
  const auto& infoColumnsObj = obj.getDefault("infoColumns", nullptr);
  if (infoColumnsObj != nullptr) {
    for (const auto& [key, value] : infoColumnsObj.items()) {
      infoColumns[key.asString()] = value.asString();
    }
  }

  return std::make_shared<HiveIcebergSplit>(
      connectorId,
      filePath,
      fileFormat,
      start,
      length,
      partitionKeys,
      tableBucketNumber,
      customSplitInfo,
      extraFileInfo,
      true,
      infoColumns);
}

// static
void HiveIcebergSplit::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("HiveIcebergSplit", HiveIcebergSplit::create);
}
} // namespace bytedance::bolt::connector::hive::iceberg
