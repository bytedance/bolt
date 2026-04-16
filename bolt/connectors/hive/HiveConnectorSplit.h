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

#pragma once

#include <optional>
#include <unordered_map>
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/hive/FileProperties.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/dwio/common/Options.h"
namespace bytedance::bolt::connector::hive {
inline const std::string KPaimonDeletionFilePath = "paimon.deletion.file.path";
inline const std::string KPaimonDeletionBinOffset =
    "paimon.deletion.bin.offset";
inline const std::string KPaimonDeletionBinSize = "paimon.deletion.bin.size";
constexpr std::string_view kDefaultHiveConnectorId = "hive-default";

struct HiveConnectorSplitCacheLimit : public ISerializable {
  // record table is or not cache, and table partition range limit;
  bool tableAndDataPartitionRangeOpen;
  const std::unordered_map<std::string, int> tableAndDataPartitionRangeMap;
  bool splitSoftAffinityCacheable;
  const std::unordered_map<std::string, std::vector<int>>
      tableAndColumnCacheMap;
  HiveConnectorSplitCacheLimit(
      bool _tableAndDataPartitionRangeOpen,
      const std::unordered_map<std::string, int>&
          _tableAndDataPartitionRangeMap,
      bool _splitSoftAffinityCacheable,
      const std::unordered_map<std::string, std::vector<int>>&
          _tableAndColumnCacheMap)
      : tableAndDataPartitionRangeOpen(_tableAndDataPartitionRangeOpen),
        tableAndDataPartitionRangeMap(_tableAndDataPartitionRangeMap),
        splitSoftAffinityCacheable(_splitSoftAffinityCacheable),
        tableAndColumnCacheMap(_tableAndColumnCacheMap) {}

  static std::unique_ptr<HiveConnectorSplitCacheLimit> create(
      const folly::dynamic& obj);

  folly::dynamic serialize() const override;
};

struct RowIdProperties {
  int64_t metadataVersion;
  int64_t partitionId;
  std::string tableGuid;
};

struct HiveConnectorSplit : public connector::ConnectorSplit {
  const std::string filePath;
  dwio::common::FileFormat fileFormat;
  const uint64_t start;
  const uint64_t length;
  const std::unordered_map<std::string, std::optional<std::string>>
      partitionKeys;
  std::optional<int32_t> tableBucketNumber;
  std::unordered_map<std::string, std::string> customSplitInfo;
  std::shared_ptr<std::string> extraFileInfo;
  std::unordered_map<std::string, std::string> serdeParameters;
  std::unique_ptr<HiveConnectorSplitCacheLimit> hiveConnectorSplitCacheLimit;
  const uint64_t fileSize;
  std::optional<RowIdProperties> rowIdProperties;

  /// These represent columns like $file_size, $file_modified_time that are
  /// associated with the HiveSplit.
  std::unordered_map<std::string, std::string> infoColumns;

  HiveConnectorSplit(
      const std::string& connectorId,
      const std::string& _filePath,
      dwio::common::FileFormat _fileFormat,
      uint64_t _start = 0,
      uint64_t _length = std::numeric_limits<uint64_t>::max(),
      const std::unordered_map<std::string, std::optional<std::string>>&
          _partitionKeys = {},
      std::optional<int32_t> _tableBucketNumber = std::nullopt,
      std::unique_ptr<HiveConnectorSplitCacheLimit>
          _hiveConnectorSplitCacheLimit = nullptr,
      const std::unordered_map<std::string, std::string>& _customSplitInfo = {},
      const std::shared_ptr<std::string>& _extraFileInfo = {},
      const std::unordered_map<std::string, std::string>& _serdeParameters = {},
      uint64_t _fileSize = 0,
      std::optional<RowIdProperties> _rowIdProperties = std::nullopt,
      const std::unordered_map<std::string, std::string>& _infoColumns = {})
      : ConnectorSplit(connectorId),
        filePath(_filePath),
        fileFormat(_fileFormat),
        start(_start),
        length(_length),
        partitionKeys(_partitionKeys),
        tableBucketNumber(_tableBucketNumber),
        customSplitInfo(_customSplitInfo),
        extraFileInfo(_extraFileInfo),
        serdeParameters(_serdeParameters),
        hiveConnectorSplitCacheLimit(std::move(_hiveConnectorSplitCacheLimit)),
        fileSize(_fileSize),
        rowIdProperties(_rowIdProperties),
        infoColumns(_infoColumns) {}

  int64_t splitSizeBytes() const override {
    if (length != std::numeric_limits<uint64_t>::max()) {
      return static_cast<int64_t>(length);
    }
    return fileSize > start ? static_cast<int64_t>(fileSize - start) : 0;
  }

  std::string toString() const override {
    if (tableBucketNumber.has_value()) {
      return fmt::format(
          "Hive: {} {} - {} {}",
          filePath,
          start,
          length,
          tableBucketNumber.value());
    }
    return fmt::format("Hive: {} {} - {}", filePath, start, length);
  }

  std::string getFileName() const {
    const auto i = filePath.rfind('/');
    return i == std::string::npos ? filePath : filePath.substr(i + 1);
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "HiveConnectorSplit";
    obj["connectorId"] = connectorId;
    obj["splitWeight"] = splitWeight;
    obj["filePath"] = filePath;
    obj["fileFormat"] = dwio::common::toString(fileFormat);
    obj["start"] = start;
    obj["length"] = length;

    folly::dynamic partitionKeysObj = folly::dynamic::object;
    for (const auto& [key, value] : partitionKeys) {
      partitionKeysObj[key] =
          value.has_value() ? folly::dynamic(value.value()) : nullptr;
    }
    obj["partitionKeys"] = partitionKeysObj;

    obj["tableBucketNumber"] = tableBucketNumber.has_value()
        ? folly::dynamic(tableBucketNumber.value())
        : nullptr;

    folly::dynamic customSplitInfoObj = folly::dynamic::object;
    for (const auto& [key, value] : customSplitInfo) {
      customSplitInfoObj[key] = value;
    }
    obj["customSplitInfo"] = customSplitInfoObj;
    obj["extraFileInfo"] =
        extraFileInfo == nullptr ? nullptr : folly::dynamic(*extraFileInfo);

    folly::dynamic serdeParametersObj = folly::dynamic::object;
    for (const auto& [key, value] : serdeParameters) {
      serdeParametersObj[key] = value;
    }
    obj["serdeParameters"] = serdeParametersObj;

    if (hiveConnectorSplitCacheLimit)
      obj["hiveConnectorSplitCacheLimit"] =
          hiveConnectorSplitCacheLimit->serialize();

    obj["fileSize"] = fileSize;

    if (rowIdProperties.has_value()) {
      folly::dynamic rowIdObj = folly::dynamic::object;
      rowIdObj["metadataVersion"] = rowIdProperties->metadataVersion;
      rowIdObj["partitionId"] = rowIdProperties->partitionId;
      rowIdObj["tableGuid"] = rowIdProperties->tableGuid;
      obj["rowIdProperties"] = rowIdObj;
    }

    return obj;
  }

  static std::shared_ptr<HiveConnectorSplit> create(const folly::dynamic& obj);

  static void registerSerDe();
};

/// Builder for HiveConnectorSplit. Allows constructing splits incrementally
/// without specifying every constructor argument.
class HiveConnectorSplitBuilder {
 public:
  explicit HiveConnectorSplitBuilder(std::string filePath)
      : filePath_{std::move(filePath)} {}

  HiveConnectorSplitBuilder& connectorId(const std::string& connectorId) {
    connectorId_ = connectorId;
    return *this;
  }

  HiveConnectorSplitBuilder& start(uint64_t start) {
    start_ = start;
    return *this;
  }

  HiveConnectorSplitBuilder& length(uint64_t length) {
    length_ = length;
    return *this;
  }

  HiveConnectorSplitBuilder& fileFormat(dwio::common::FileFormat format) {
    fileFormat_ = format;
    return *this;
  }

  HiveConnectorSplitBuilder& partitionKey(
      std::string name,
      std::optional<std::string> value) {
    partitionKeys_.emplace(std::move(name), std::move(value));
    return *this;
  }

  HiveConnectorSplitBuilder& tableBucketNumber(int32_t bucket) {
    tableBucketNumber_ = bucket;
    return *this;
  }

  HiveConnectorSplitBuilder& cacheLimit(HiveConnectorSplitCacheLimit cl) {
    cacheLimit_ = std::make_shared<HiveConnectorSplitCacheLimit>(std::move(cl));
    return *this;
  }

  HiveConnectorSplitBuilder& customSplitInfo(
      std::unordered_map<std::string, std::string> info) {
    customSplitInfo_ = std::move(info);
    return *this;
  }

  HiveConnectorSplitBuilder& extraFileInfo(
      std::shared_ptr<std::string> extraFileInfo) {
    extraFileInfo_ = std::move(extraFileInfo);
    return *this;
  }

  HiveConnectorSplitBuilder& serdeParameters(
      std::unordered_map<std::string, std::string> params) {
    serdeParameters_ = std::move(params);
    return *this;
  }

  HiveConnectorSplitBuilder& fileSize(uint64_t size) {
    fileSize_ = size;
    return *this;
  }

  HiveConnectorSplitBuilder& rowIdProperties(RowIdProperties props) {
    rowIdProperties_ = std::move(props);
    return *this;
  }

  HiveConnectorSplitBuilder& infoColumn(
      const std::string& name,
      const std::string& value) {
    infoColumns_.emplace(name, value);
    return *this;
  }

  std::shared_ptr<HiveConnectorSplit> build() const {
    const auto effectivePath =
        filePath_.find('/') == 0 ? "file:" + filePath_ : filePath_;
    return std::make_shared<HiveConnectorSplit>(
        connectorId_,
        effectivePath,
        fileFormat_,
        start_,
        length_,
        partitionKeys_,
        tableBucketNumber_,
        cacheLimit_
            ? std::make_unique<HiveConnectorSplitCacheLimit>(*cacheLimit_)
            : nullptr,
        customSplitInfo_,
        extraFileInfo_,
        serdeParameters_,
        fileSize_,
        rowIdProperties_,
        infoColumns_);
  }

 private:
  std::string filePath_;
  std::string connectorId_{kDefaultHiveConnectorId};
  dwio::common::FileFormat fileFormat_{dwio::common::FileFormat::PARQUET};
  uint64_t start_{0};
  uint64_t length_{std::numeric_limits<uint64_t>::max()};
  std::unordered_map<std::string, std::optional<std::string>> partitionKeys_;
  std::optional<int32_t> tableBucketNumber_;
  std::shared_ptr<HiveConnectorSplitCacheLimit> cacheLimit_;
  std::unordered_map<std::string, std::string> customSplitInfo_;
  std::shared_ptr<std::string> extraFileInfo_;
  std::unordered_map<std::string, std::string> serdeParameters_;
  uint64_t fileSize_{0};
  std::optional<RowIdProperties> rowIdProperties_;
  std::unordered_map<std::string, std::string> infoColumns_;
};

} // namespace bytedance::bolt::connector::hive
