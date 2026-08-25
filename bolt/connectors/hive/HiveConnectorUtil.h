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
#include <folly/Executor.h>
#include <folly/container/F14Map.h>

#include "bolt/connectors/Connector.h"
#include "bolt/connectors/hive/FileHandle.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/Reader.h"
namespace bytedance::bolt::connector::hive {

class HiveColumnHandle;
class HiveConfig;
struct HiveConnectorSplit;

using SubfieldFilters =
    std::unordered_map<common::Subfield, std::unique_ptr<common::Filter>>;

constexpr const char* kPath = "$path";
constexpr const char* kBucket = "$bucket";

const std::string& getColumnName(const common::Subfield& subfield);

void checkColumnNameLowerCase(const std::shared_ptr<const Type>& type);

void checkColumnNameLowerCase(
    const SubfieldFilters& filters,
    const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>&
        infoColumns);

void checkColumnNameLowerCase(const core::TypedExprPtr& typeExpr);

std::shared_ptr<common::ScanSpec> makeScanSpec(
    const RowTypePtr& rowType,
    const folly::F14FastMap<std::string, std::vector<const common::Subfield*>>&
        outputSubfields,
    const SubfieldFilters& filters,
    const RowTypePtr& dataColumns,
    const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>&
        partitionKeys,
    const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>&
        infoColumns,
    memory::MemoryPool* pool,
    core::ExpressionEvaluator* expressionEvaluator = nullptr,
    dwio::common::RuntimeStatistics* statis = nullptr,
    const std::vector<std::tuple<size_t, std::optional<std::string>>>&
        rowIndexColumns = {});

void configureReaderOptions(
    dwio::common::ReaderOptions& readerOptions,
    const std::shared_ptr<HiveConfig>& config,
    const config::ConfigBase* sessionProperties,
    const RowTypePtr& fileSchema,
    std::shared_ptr<HiveConnectorSplit> hiveSplit);

void configureRowReaderOptions(
    dwio::common::RowReaderOptions& rowReaderOptions,
    const std::unordered_map<std::string, std::string>& tableParameters,
    std::shared_ptr<common::ScanSpec> scanSpec,
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    const RowTypePtr& rowType,
    const std::shared_ptr<const HiveConnectorSplit>& hiveSplit,
    const std::shared_ptr<const HiveConfig>& hiveConfig = nullptr,
    const config::ConfigBase* sessionProperties = nullptr);

bool isHiveNull(const std::string& source);

bool shouldAdjustPartitionTimestampToTimezone();

template <TypeKind ToKind>
bolt::variant convertFromString(
    const std::optional<std::string>& value,
    bool adjustTimestampToTimezone) {
  if (value.has_value()) {
    if constexpr (ToKind == TypeKind::VARCHAR) {
      return bolt::variant(value.value());
    }
    if constexpr (ToKind == TypeKind::VARBINARY) {
      return bolt::variant::binary((value.value()));
    }
    auto result = bolt::util::Converter<ToKind>::cast(value.value(), nullptr);
    if constexpr (ToKind == TypeKind::TIMESTAMP) {
      if (adjustTimestampToTimezone) {
        result.toGMT(Timestamp::defaultTimezone());
      }
    }
    return bolt::variant(result);
  }
  return bolt::variant(ToKind);
}

template <TypeKind ToKind>
bolt::variant convertFromString(const std::optional<std::string>& value) {
  if constexpr (ToKind == TypeKind::TIMESTAMP) {
    return convertFromString<ToKind>(
        value, shouldAdjustPartitionTimestampToTimezone());
  } else {
    return convertFromString<ToKind>(value, false);
  }
}

bool testFilters(
    common::ScanSpec* scanSpec,
    dwio::common::Reader* reader,
    const std::string& filePath,
    const std::unordered_map<std::string, std::optional<std::string>>&
        partitionKey,
    std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
        partitionKeysHandle);

std::unique_ptr<dwio::common::BufferedInput> createBufferedInput(
    const FileHandle& fileHandle,
    const dwio::common::ReaderOptions& readerOpts,
    const ConnectorQueryCtx* connectorQueryCtx,
    std::shared_ptr<io::IoStatistics> ioStats,
    folly::Executor* executor,
    bool judgeCache,
    std::vector<int>& columnCacheBlackList,
    const HiveConnectorSplitCacheLimit* hiveConnectorSplitCacheLimit);

core::TypedExprPtr extractFiltersFromRemainingFilter(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator,
    common::SubfieldFilters& filters);
} // namespace bytedance::bolt::connector::hive
