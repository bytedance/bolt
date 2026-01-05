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

#include "bolt/connectors/hive/HiveDataSource.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <exception>
#include <string>
#include <unordered_map>

#include "bolt/connectors/hive/HiveConfig.h"
#include "bolt/connectors/hive/HiveConnectorUtil.h"
#include "bolt/connectors/hive/IgnoreCorruptFile.h"
#include "bolt/connectors/hive/PaimonMiscHelpers.h"
#include "bolt/connectors/hive/PaimonSplitReader.h"
#include "bolt/connectors/hive/SplitReader.h"
#include "bolt/dwio/common/ReaderFactory.h"
#include "bolt/dwio/common/exception/Exception.h"
#include "bolt/expression/EvalCtx.h"
#include "bolt/expression/FieldReference.h"
#include "bolt/type/Filter.h"
namespace bytedance::bolt::connector::hive {

class HiveTableHandle;
class HiveColumnHandle;

namespace {
bool tryMakeMapKeyPruningExpr(
    const RowTypePtr& readerOutputType,
    const std::string& columnName,
    const common::Filter* keyFilter,
    column_index_t& channel,
    core::TypedExprPtr& out) {
  if (keyFilter == nullptr) {
    return false;
  }

  auto childIndex = readerOutputType->getChildIdxIfExists(columnName);
  if (!childIndex.has_value()) {
    return false;
  }

  auto mapType = readerOutputType->childAt(childIndex.value());
  if (!mapType->isMap()) {
    return false;
  }

  auto keyType = mapType->childAt(0);
  auto valueType = mapType->childAt(1);

  // Helper lambda to build constant expressions from a vector of values
  auto buildInArgs = [&](const auto& values) {
    std::vector<core::TypedExprPtr> inArgs;
    inArgs.reserve(values.size() + 1);
    auto kExpr = std::make_shared<core::FieldAccessTypedExpr>(keyType, "k");
    inArgs.push_back(kExpr);
    for (const auto& key : values) {
      inArgs.push_back(
          std::make_shared<core::ConstantTypedExpr>(keyType, variant(key)));
    }
    return inArgs;
  };

  // Helper to build map_filter expression
  auto buildMapFilter = [&](core::TypedExprPtr body) {
    auto mapExpr =
        std::make_shared<core::FieldAccessTypedExpr>(mapType, columnName);
    auto lambda = std::make_shared<core::LambdaTypedExpr>(
        ROW({"k", "v"}, {keyType, valueType}), std::move(body));
    return std::make_shared<core::CallTypedExpr>(
        mapType,
        "map_filter",
        std::vector<core::TypedExprPtr>{mapExpr, lambda});
  };

  // Handle numeric keys (bigint)
  if (keyType->isBigint()) {
    std::vector<int64_t> allowedKeys;
    bool hasKeys = false;

    if (auto* range = dynamic_cast<const common::BigintRange*>(keyFilter)) {
      const int64_t lower = range->lower();
      const int64_t upper = range->upper();
      // Increased range limit from 1024 to 4096 for better pruning
      const int64_t MAX_RANGE_SIZE = 4096;
      if (lower <= upper && upper - lower <= MAX_RANGE_SIZE) {
        allowedKeys.reserve(upper - lower + 1);
        for (int64_t v = lower; v <= upper; ++v) {
          allowedKeys.push_back(v);
        }
        hasKeys = true;
      }
    } else if (
        auto* valuesFilter =
            dynamic_cast<const common::IFilterWithValues<int64_t>*>(
                keyFilter)) {
      allowedKeys = valuesFilter->values();
      hasKeys = !allowedKeys.empty();
    }

    if (hasKeys) {
      auto inArgs = buildInArgs(allowedKeys);
      auto inCall = std::make_shared<core::CallTypedExpr>(
          BOOLEAN(), "in", std::move(inArgs));
      out = buildMapFilter(std::move(inCall));
      channel = childIndex.value();
      return true;
    }
  }
  // Handle string keys (varchar)
  else if (keyType->isVarchar()) {
    std::vector<std::string> allowedKeys;
    bool hasKeys = false;

    if (auto* valuesFilter =
            dynamic_cast<const common::IFilterWithValues<std::string>*>(
                keyFilter)) {
      allowedKeys = valuesFilter->values();
      hasKeys = !allowedKeys.empty();
    }

    if (hasKeys) {
      auto inArgs = buildInArgs(allowedKeys);
      auto inCall = std::make_shared<core::CallTypedExpr>(
          BOOLEAN(), "in", std::move(inArgs));
      out = buildMapFilter(std::move(inCall));
      channel = childIndex.value();
      return true;
    }
    // For string ranges, we can use the between operator
    else if (auto* range = dynamic_cast<const common::BytesRange*>(keyFilter)) {
      auto kExpr = std::make_shared<core::FieldAccessTypedExpr>(keyType, "k");
      auto lowerExpr = std::make_shared<core::ConstantTypedExpr>(
          keyType, variant(range->lower()));
      auto upperExpr = std::make_shared<core::ConstantTypedExpr>(
          keyType, variant(range->upper()));

      std::vector<core::TypedExprPtr> betweenArgs;
      betweenArgs.push_back(kExpr);
      betweenArgs.push_back(lowerExpr);
      betweenArgs.push_back(upperExpr);

      auto betweenCall = std::make_shared<core::CallTypedExpr>(
          BOOLEAN(), "between", std::move(betweenArgs));
      out = buildMapFilter(std::move(betweenCall));
      channel = childIndex.value();
      return true;
    }
  }

  return false;
}

int64_t extractAttemptNumber(const std::string& taskId) {
  const std::string prefix = "ATTEMPT_";
  size_t prefixPos = taskId.find(prefix);

  if (prefixPos == std::string::npos) {
    return -1;
  }

  size_t numStart = prefixPos + prefix.length();
  size_t numEnd = numStart;

  while (numEnd < taskId.length() && std::isdigit(taskId[numEnd])) {
    numEnd++;
  }

  if (numEnd == numStart) {
    return -1;
  }

  try {
    return std::stoi(taskId.substr(numStart, numEnd - numStart));
  } catch (...) {
    return -1;
  }
  return -1;
}

void collectScanSpecRowGroupFilters(
    const common::ScanSpec& spec,
    std::vector<std::string>& path,
    common::SubfieldFilters& out,
    std::vector<std::pair<std::string, std::unique_ptr<common::Filter>>>&
        mapKeysRowGroupFilters) {
  const bool pushed = spec.fieldName() != "root" &&
      spec.fieldName() != common::ScanSpec::kMapKeysFieldName &&
      spec.fieldName() != common::ScanSpec::kMapValuesFieldName &&
      spec.fieldName() != common::ScanSpec::kArrayElementsFieldName;
  if (pushed) {
    path.push_back(spec.fieldName());
  }

  if (auto* filter = spec.rowGroupFilter()) {
    const bool isMapKeysPruningFilter =
        spec.fieldName() == common::ScanSpec::kMapKeysFieldName &&
        spec.channel() == common::ScanSpec::kNoChannel;
    if (isMapKeysPruningFilter && !path.empty()) {
      mapKeysRowGroupFilters.emplace_back(path.front(), filter->clone());
    } else if (!path.empty()) {
      std::string subfieldPath = path.front();
      for (size_t i = 1; i < path.size(); ++i) {
        subfieldPath.append(".").append(path[i]);
      }
      out[common::Subfield(subfieldPath)] = filter->clone();
    }
  }

  for (const auto& child : spec.children()) {
    collectScanSpecRowGroupFilters(*child, path, out, mapKeysRowGroupFilters);
  }

  if (pushed) {
    path.pop_back();
  }
}

void buildMapKeyPruningExprs(
    const std::shared_ptr<common::ScanSpec>& scanSpec,
    const RowTypePtr& readerOutputType,
    std::vector<std::pair<column_index_t, core::TypedExprPtr>>& out) {
  out.clear();

  common::SubfieldFilters scanSpecRowGroupFilters;
  std::vector<std::string> scanSpecPath;
  std::vector<std::pair<std::string, std::unique_ptr<common::Filter>>>
      mapKeysRowGroupFilters;
  collectScanSpecRowGroupFilters(
      *scanSpec, scanSpecPath, scanSpecRowGroupFilters, mapKeysRowGroupFilters);

  for (auto& [columnName, keyFilter] : mapKeysRowGroupFilters) {
    column_index_t channel;
    core::TypedExprPtr expr;
    if (tryMakeMapKeyPruningExpr(
            readerOutputType, columnName, keyFilter.get(), channel, expr)) {
      out.emplace_back(channel, std::move(expr));
    }
  }
}

#ifdef BOLT_ENABLE_HDFS
void enrichExceptionSetFromConf(
    std::string exceptionStr,
    std::vector<std::string>& exceptionKeyWords) {
  // only init exceptionSet exactly once
  if (exceptionStr.empty() || !exceptionKeyWords.empty()) {
    return;
  }

  const char delimeter = exceptionStr.back();
  exceptionStr.pop_back();
  folly::split(delimeter, exceptionStr, exceptionKeyWords);

  LOG(INFO) << "Split exception str by " << static_cast<char>(delimeter)
            << ", and split result is: "
            << fmt::format("{}", fmt::join(exceptionKeyWords, ", "));
}
#endif

} // namespace

HiveDataSource::HiveDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    FileHandleFactory* fileHandleFactory,
    const core::QueryConfig& queryConfig,
    folly::Executor* executor,
    const std::shared_ptr<ConnectorQueryCtx>& connectorQueryCtx,
    const std::shared_ptr<HiveConfig>& hiveConfig)
    : pool_(connectorQueryCtx->memoryPool()),
      fileHandleFactory_(fileHandleFactory),
      executor_(executor),
      connectorQueryCtx_(connectorQueryCtx),
      hiveConfig_(hiveConfig),
      outputType_(outputType),
      expressionEvaluator_(connectorQueryCtx->expressionEvaluator()),
      runtimeStats_(std::make_unique<dwio::common::RuntimeStatistics>()),
      columnHandles_(columnHandles) {
  for (const auto& key : HiveConfig::hms_session_key) {
    std::optional<std::string> value = queryConfig.get<std::string>(key);
    if (value.has_value()) {
      fsSessionConfig_.values[key] = value.value();
    }
  }
  fsSessionConfig_.bufferSize = static_cast<size_t>(hiveConfig_->loadQuantum());
  native_cache_enabled = queryConfig.isNativeCacheEnabled();
  IgnoreCorruptFileHelper::globalInitialize(
      queryConfig.taskMaxFailures(),
      queryConfig.ignoreCorruptFiles(),
      queryConfig.canBeTreatedAsCorruptedFileExceptions());

  enable_parquet_rownum_and_filename =
      queryConfig.isDataRetentionUpdateEnabled() &&
      queryConfig.isDataRetentionShuffleBased();

  // Column handled keyed on the column alias, the name used in the query.
  for (const auto& [canonicalizedName, columnHandle] : columnHandles) {
    auto handle = std::dynamic_pointer_cast<HiveColumnHandle>(columnHandle);
    BOLT_CHECK_NOT_NULL(
        handle,
        "ColumnHandle must be an instance of HiveColumnHandle for {}",
        canonicalizedName);

    if (handle->columnType() == HiveColumnHandle::ColumnType::kPartitionKey) {
      partitionKeys_.emplace(handle->name(), handle);
    }

    if (handle->columnType() == HiveColumnHandle::ColumnType::kSynthesized) {
      infoColumns_.emplace(handle->name(), handle);
    }
  }

  std::vector<std::string> readerRowNames;
  auto readerRowTypes = outputType_->children();
  folly::F14FastMap<std::string, std::vector<const common::Subfield*>>
      subfields;
  for (const auto& outputName : outputType_->names()) {
    auto it = columnHandles.find(outputName);
    BOLT_CHECK(
        it != columnHandles.end(),
        "ColumnHandle is missing for output column: {}",
        outputName);

    auto* handle = static_cast<const HiveColumnHandle*>(it->second.get());
    readerRowNames.push_back(handle->name());
    for (auto& subfield : handle->requiredSubfields()) {
      BOLT_USER_CHECK_EQ(
          getColumnName(subfield),
          handle->name(),
          "Required subfield does not match column name");
      subfields[handle->name()].push_back(&subfield);
    }
  }

  hiveTableHandle_ = std::dynamic_pointer_cast<HiveTableHandle>(tableHandle);
  BOLT_CHECK_NOT_NULL(
      hiveTableHandle_, "TableHandle must be an instance of HiveTableHandle");
  if (hiveConfig_->isFileColumnNamesReadAsLowerCase(
          connectorQueryCtx->sessionProperties())) {
    checkColumnNameLowerCase(outputType_);
    checkColumnNameLowerCase(hiveTableHandle_->subfieldFilters(), infoColumns_);
    checkColumnNameLowerCase(hiveTableHandle_->remainingFilter());
  }

  SubfieldFilters filters;
  for (const auto& [k, v] : hiveTableHandle_->subfieldFilters()) {
    filters.emplace(k.clone(), v->clone());
  }
  auto remainingFilter = hiveTableHandle_->remainingFilter();
  auto remainingFilterRet = hiveTableHandle_->remainingFilter();

  if (hiveTableHandle_->isFilterPushdownEnabled()) {
    remainingFilterRet = hive::extractFiltersFromRemainingFilter(
        remainingFilter, expressionEvaluator_, filters);
  }

  // User requested to evaluate all filters in the remaining filter.
  // So we just use the original filter as remaining filter, incorporating both
  // subfield filters (that were extracted) and the residual remaining filters.
  // We also explicitly reconstruct expressions from subfield filters to ensure
  // everything is covered.

  remainingFilter = convertFiltersToExpr(
      filters,
      columnHandles,
      hiveTableHandle_->dataColumns(),
      remainingFilterRet);

  currentRemainingFilter_ = remainingFilter;

  std::vector<common::Subfield> remainingFilterSubfields;
  if (remainingFilter) {
    auto remainingFilterExprSet =
        expressionEvaluator_->compile(remainingFilter);
    auto& remainingFilterExpr = remainingFilterExprSet->expr(0);
    folly::F14FastSet<std::string> columnNames(
        readerRowNames.begin(), readerRowNames.end());
    for (auto& input : remainingFilterExpr->distinctFields()) {
      if (columnNames.count(input->field()) > 0) {
        continue;
      }
      // Remaining filter may reference columns that are not used otherwise,
      // e.g. are not being projected out and are not used in range filters.
      // Make sure to add these columns to readerOutputType_.
      readerRowNames.push_back(input->field());
      readerRowTypes.push_back(input->type());
    }
    remainingFilterSubfields = remainingFilterExpr->extractSubfields();
    if (VLOG_IS_ON(1)) {
      VLOG(1) << fmt::format(
          "Extracted subfields from remaining filter: [{}]",
          fmt::join(remainingFilterSubfields, ", "));
    }
    if (remainingFilterRet) {
      for (auto& subfield : remainingFilterSubfields) {
        auto& name = getColumnName(subfield);
        auto it = subfields.find(name);
        if (it != subfields.end()) {
          // Only subfields of the column are projected out.
          it->second.push_back(&subfield);
        } else if (columnNames.count(name) == 0) {
          // Column appears only in remaining filter.
          subfields[name].push_back(&subfield);
        }
      }
    }
  }

  readerOutputType_ = ROW(std::move(readerRowNames), std::move(readerRowTypes));
  const auto& names = readerOutputType_->names();
  const auto readColumnsAsLowercase =
      hiveConfig_->isFileColumnNamesReadAsLowerCase(
          connectorQueryCtx_->sessionProperties());
  // Each element is a pair of column index and column name
  std::vector<std::tuple<size_t, std::optional<std::string>>> rowIndexColumns;
  for (int i = 0; i < names.size(); ++i) {
    const auto& name = names[i];
    if (paimon::kColumnNameRowIndex == name) {
      rowIndexColumns.emplace_back(i, std::nullopt);
    } else if (
        (!readColumnsAsLowercase && paimon::kColumnNameRowID == name) ||
        (readColumnsAsLowercase &&
         boost::algorithm::iequals(paimon::kColumnNameRowID, name))) {
      rowIndexColumns.emplace_back(i, paimon::kColumnNameRowID);
    }
  }
  scanSpec_ = makeScanSpec(
      readerOutputType_,
      subfields,
      filters,
      hiveTableHandle_->dataColumns(),
      partitionKeys_,
      infoColumns_,
      pool_,
      expressionEvaluator_,
      runtimeStats_.get(),
      rowIndexColumns);

  buildMapKeyPruningExprs(scanSpec_, readerOutputType_, mapKeyPruningExprs_);
  if (remainingFilterRet) {
    bool enableMapSubscriptFilter =
        queryConfig.mapSubscriptFilterPushdownEnabled();
    metadataFilter_ = std::make_shared<common::MetadataFilter>(
        *scanSpec_,
        *remainingFilterRet,
        expressionEvaluator_,
        enableMapSubscriptFilter);
  }

  rebuildPostScanExprSet();
  recalculateRepDefConf(readerOutputType_, queryConfig);
  ioStats_ = std::make_shared<io::IoStatistics>();
}

void HiveDataSource::rebuildPostScanExprSet() {
  postScanExprSet_.reset();
  postScanPruningChannels_.clear();
  postScanFilterExprIndex_ = -1;

  std::vector<core::TypedExprPtr> exprs;
  exprs.reserve(mapKeyPruningExprs_.size() + (currentRemainingFilter_ ? 1 : 0));
  postScanPruningChannels_.reserve(mapKeyPruningExprs_.size());

  for (auto& [channel, expr] : mapKeyPruningExprs_) {
    postScanPruningChannels_.push_back(channel);
    exprs.push_back(expr);
  }

  if (currentRemainingFilter_) {
    postScanFilterExprIndex_ = exprs.size();
    exprs.push_back(currentRemainingFilter_);
  }

  if (exprs.empty()) {
    return;
  }

  if (exprs.size() == 1) {
    postScanExprSet_ = expressionEvaluator_->compile(exprs[0]);
    return;
  }

  auto seed = expressionEvaluator_->compile(exprs[0]);
  auto* execCtx = seed->execCtx();
  if (dynamic_cast<exec::ExprSetSimplified*>(seed.get()) != nullptr) {
    postScanExprSet_ =
        std::make_unique<exec::ExprSetSimplified>(exprs, execCtx);
    return;
  }
  postScanExprSet_ = std::make_unique<exec::ExprSet>(exprs, execCtx);
}

bool judgeTableAndPartitionInRange(
    std::string pathStr,
    std::unordered_map<std::string, int> cacheTableMap) {
  std::vector<std::string> result;
  std::string delimiter = "/";
  size_t pos = 0;
  std::string token;
  while ((pos = pathStr.find(delimiter)) != std::string::npos) {
    token = pathStr.substr(0, pos);
    result.push_back(token);
    pathStr.erase(0, pos + delimiter.length());
  }
  result.push_back(pathStr);
  std::string dataPartition;
  int dataPartitionNumber = 0;
  for (auto s : result) {
    dataPartitionNumber++;
    if (s.find("date=") != std::string::npos) {
      dataPartition = s.substr(5);
      break;
    }
  }
  std::string tableName = result[dataPartitionNumber - 2];
  std::string dbName = result[dataPartitionNumber - 3];
  if (dbName.find(".db") != std::string::npos) {
    dbName = dbName.substr(0, dbName.size() - 3);
  }
  tableName = dbName + "." + tableName;

  if (cacheTableMap.count(tableName) == 0) {
    // If table not in cacheTableMap, don't use memory cache & ssd cache
    return false;
  } else {
    int judgeDataDistance = cacheTableMap[tableName];
    if (-1 == judgeDataDistance) {
      // If table in cacheTableMap, and value(judgeDataDistance) = -1, means use
      // cache and not time partition limit;
      return true;
    } else {
      // If table in cacheTableMap, and value(judgeDataDistance) != -1, means
      // use cache and need time partition limit;
      std::tm tm = {};
      std::istringstream ss(dataPartition);
      ss >> std::get_time(&tm, "%Y%m%d");
      std::time_t t = std::mktime(&tm);
      std::time_t now = std::chrono::system_clock::to_time_t(
          std::chrono::system_clock::now());
      return std::difftime(now, t) < judgeDataDistance * 24 * 60 * 60;
    }
  }
}

std::string getTableName(std::string pathStr) {
  std::vector<std::string> result;
  std::string delimiter = "/";
  size_t pos = 0;
  std::string token;
  while ((pos = pathStr.find(delimiter)) != std::string::npos) {
    token = pathStr.substr(0, pos);
    result.push_back(token);
    pathStr.erase(0, pos + delimiter.length());
  }
  result.push_back(pathStr);
  std::string dataPartition = "";
  int dataPartitionNumber = 0;
  for (auto s : result) {
    dataPartitionNumber++;
    if (s.find("date=") != std::string::npos) {
      dataPartition = s.substr(5);
      break;
    }
  }
  std::string tableName = result[dataPartitionNumber - 2];
  std::string dbName = result[dataPartitionNumber - 3];
  if (dbName.find(".db") != std::string::npos) {
    dbName = dbName.substr(0, dbName.size() - 3);
  }
  tableName = dbName + "." + tableName;
  return tableName;
}

std::unique_ptr<SplitReader> HiveDataSource::createSplitReader(
    const std::shared_ptr<HiveConnectorSplit>& split,
    const bool isPartOfPaimonSplit) {
  return SplitReader::create(
      split,
      hiveTableHandle_,
      scanSpec_,
      readerOutputType_,
      &partitionKeys_,
      fileHandleFactory_,
      executor_,
      connectorQueryCtx_.get(),
      hiveConfig_,
      ioStats_,
      isPartOfPaimonSplit);
}

bool isPaimonConnectorSplit(const std::shared_ptr<ConnectorSplit>& split) {
  return std::dynamic_pointer_cast<PaimonConnectorSplit>(split) != nullptr;
}

std::unique_ptr<SplitReader> HiveDataSource::createConfiguredSplitReader(
    const std::shared_ptr<HiveConnectorSplit>& split,
    const bool isPartOfPaimonSplit) {
  std::vector<int> columnCacheBlackList;

  auto& hiveConnectorSplitCacheLimit = split->hiveConnectorSplitCacheLimit;
  auto customSplitInfo = split->customSplitInfo;
  auto iter = customSplitInfo.find("split_cache_enabled");
  bool splitCacheEnabled =
      (iter == customSplitInfo.end() || iter->second.empty()
           ? true
           : iter->second == "true" || iter->second == "false");
  bool finalCacheEnabled = splitCacheEnabled && native_cache_enabled;

  if (split->fileSize > 0) {
    fsSessionConfig_.fileSize = split->fileSize;
  }

  auto splitReader = createSplitReader(split, isPartOfPaimonSplit);
  splitReader->configureReaderOptions();
  splitReader->rowReaderOptions().setAppendParquetRowNumberAndFileName(
      enable_parquet_rownum_and_filename);

  const std::string& filePath = split->filePath;
  if (splitReader->rowReaderOptions().getAppendParquetRowNumberAndFileName() &&
      filePath.find(dwio::common::DRFileSuffix) != std::string::npos) {
    auto partitionId = split->customSplitInfo.find(dwio::common::DRPartitionID);
    BOLT_CHECK(
        partitionId != split->customSplitInfo.end(),
        "Expect spark_partition_id in customSplitInfo in DR update mode!")
    splitReader->rowReaderOptions().setFileName(filePath);
    splitReader->rowReaderOptions().setFileId(std::stoi(partitionId->second));
    LOG(INFO) << "filename = "
              << splitReader->rowReaderOptions().getFileName().data()
              << ", fileId = " << splitReader->rowReaderOptions().getFileId();
  }

  splitReader->rowReaderOptions().setDecodeRepDefPageCount(
      decodeRepDefPageCount_);
  splitReader->rowReaderOptions().setParquetRepDefMemoryLimit(
      parquetRepDefMemoryLimit_);

  TRY_WITH_IGNORE(
      connectorQueryCtx_->taskId(),
      splitReader->prepareSplit(
          metadataFilter_,
          *runtimeStats_,
          fsSessionConfig_,
          finalCacheEnabled,
          columnCacheBlackList,
          hiveConnectorSplitCacheLimit.get()),
      {
        ignoredFileSizes_ += split->length;
        resetSplit();
      });

  return splitReader;
}

void HiveDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  BOLT_CHECK(
      split_ == nullptr,
      "Previous split has not been processed yet. Call next to process the split.");

  LOG(INFO) << "Adding split " << split->toString();

  if (isPaimonConnectorSplit(split)) {
    addSplit(std::dynamic_pointer_cast<PaimonConnectorSplit>(split));
    return;
  }

  addSplit(std::dynamic_pointer_cast<HiveConnectorSplit>(split));
}

bolt::RowTypePtr HiveDataSource::getRowTypeForFile(
    std::shared_ptr<PaimonConnectorSplit> split) {
  std::vector<int> columnCacheBlackList;
  auto hiveSplit = split->hiveSplits[0];
  auto fileHandle =
      fileHandleFactory_->generate(hiveSplit->filePath, fsSessionConfig_)
          .second;

  dwio::common::ReaderOptions baseReaderOpts{connectorQueryCtx_->memoryPool()};

  hive::configureReaderOptions(
      baseReaderOpts,
      hiveConfig_,
      connectorQueryCtx_->sessionProperties(),
      hiveTableHandle_->dataColumns(),
      hiveSplit);

  auto& hiveConnectorSplitCacheLimit = hiveSplit->hiveConnectorSplitCacheLimit;
  auto baseFileInput = hive::createBufferedInput(
      *fileHandle,
      baseReaderOpts,
      connectorQueryCtx_.get(),
      ioStats_,
      executor_,
      false,
      columnCacheBlackList,
      nullptr);

  auto baseReader =
      dwio::common::getReaderFactory(baseReaderOpts.getFileFormat())
          ->createReader(std::move(baseFileInput), baseReaderOpts);

  auto& fileType = baseReader->rowType();

  return fileType;
}

std::vector<std::string> HiveDataSource::getPaimonPrimaryKeys(
    const std::unordered_map<std::string, std::string>& tableParameters) {
  auto primaryKeyIter = tableParameters.find(connector::paimon::kPrimaryKey);
  if (primaryKeyIter == tableParameters.end()) {
    throw std::invalid_argument(fmt::format(
        "No primary key in table {}", hiveTableHandle_->tableName()));
  }

  std::string primaryKeys = primaryKeyIter->second;
  return splitByComma(primaryKeys);
}

std::vector<std::string> HiveDataSource::getPaimonSequenceFields(
    const std::unordered_map<std::string, std::string>& tableParameters) {
  auto sequenceFieldIter =
      tableParameters.find(connector::paimon::kSequenceField);
  std::string sequenceFields = "";

  if (sequenceFieldIter != tableParameters.end()) {
    sequenceFields = sequenceFieldIter->second;
  }

  auto result = splitByComma(sequenceFields);
  std::string sequenceField = paimon::kSEQUENCE_NUMBER;
  if (hiveConfig_->isFileColumnNamesReadAsLowerCase(
          connectorQueryCtx_->sessionProperties())) {
    folly::toLowerAscii(sequenceField);
  }
  result.push_back(sequenceField);

  return result;
}

std::string HiveDataSource::getPaimonRowKind(
    const std::unordered_map<std::string, std::string>& tableParameters) {
  std::string rowKind = connector::paimon::kVALUE_KIND;
  if (hiveConfig_->isFileColumnNamesReadAsLowerCase(
          connectorQueryCtx_->sessionProperties())) {
    folly::toLowerAscii(rowKind);
  }
  return rowKind;
}

std::vector<int> HiveDataSource::addColumnsIfNotExists(
    std::vector<std::string>& names,
    std::vector<TypePtr>& types,
    const std::vector<std::string>& fileColNames,
    const std::vector<TypePtr>& fileColTypes,
    const std::vector<std::string>& otherNames) {
  std::vector<int> result;
  for (const auto& otherName : otherNames) {
    auto iter = std::find(names.begin(), names.end(), otherName);
    if (iter != names.end()) {
      result.push_back(iter - names.begin());
    } else {
      names.push_back(otherName);
      auto otherColIndex =
          std::find(fileColNames.begin(), fileColNames.end(), otherName) -
          fileColNames.begin();
      auto otherType = fileColTypes[otherColIndex];
      types.push_back(otherType);
      scanSpec_->addFieldRecursively(otherName, *otherType, names.size() - 1);
      result.push_back(names.size() - 1);
    }
  }

  return result;
}

void HiveDataSource::addSplit(
    const std::shared_ptr<PaimonConnectorSplit>& split) {
  split_ = split;
  std::vector<int> columnCacheBlackList;

  if (splitReader_) {
    splitReader_.reset();
  }

  const auto& tableParameters = hiveTableHandle_->tableParameters();
  // append-only tables don't need to read additional fields
  if (tableParameters.find(paimon::kPrimaryKey) == tableParameters.end()) {
    BOLT_USER_CHECK(
        split->hiveSplits.size() == 1,
        "Append-only tables should only have a single split");
    splitReader_ = createConfiguredSplitReader(split->hiveSplits[0], false);
    return;
  }

  auto fileType = getRowTypeForFile(split);
  const auto& fileColNames = fileType->names();
  const auto& fileColTypes = fileType->children();

  auto names = readerOutputType_->names();
  auto types = readerOutputType_->children();

  std::vector<int> valueIndices(names.size());
  std::iota(valueIndices.begin(), valueIndices.end(), 0);

  auto primaryKeyNames = getPaimonPrimaryKeys(tableParameters);
  auto primaryKeyIndices = addColumnsIfNotExists(
      names, types, fileColNames, fileColTypes, primaryKeyNames);

  auto sequenceFieldNames = getPaimonSequenceFields(tableParameters);
  auto sequenceNumberIndices = addColumnsIfNotExists(
      names, types, fileColNames, fileColTypes, sequenceFieldNames);

  auto rowkindFieldName = getPaimonRowKind(tableParameters);
  auto rowkindFieldIndex = addColumnsIfNotExists(
      names, types, fileColNames, fileColTypes, {rowkindFieldName})[0];

  auto sequenceGroupInfo = getSequenceGroupInfo(tableParameters, names);
  auto sequenceGroupKeys = getSequenceGroupKeys(sequenceGroupInfo);
  auto groupKeyIndices = addColumnsIfNotExists(
      names, types, fileColNames, fileColTypes, sequenceGroupKeys);

  auto oldReaderOutputType = readerOutputType_;
  readerOutputType_ = ROW(std::move(names), std::move(types));

  std::vector<std::unique_ptr<SplitReader>> hiveSplitReaders;
  for (auto& hiveSplit : split->hiveSplits) {
    hiveSplitReaders.push_back(createConfiguredSplitReader(hiveSplit, true));
  }

  splitReader_ = std::make_unique<PaimonSplitReader>(
      std::move(hiveSplitReaders),
      readerOutputType_,
      std::move(primaryKeyIndices),
      std::move(sequenceNumberIndices),
      rowkindFieldIndex,
      std::move(valueIndices),
      tableParameters,
      ioStats_);

  readerOutputType_ = oldReaderOutputType;
}

void HiveDataSource::addSplit(
    const std::shared_ptr<HiveConnectorSplit>& split) {
  BOLT_CHECK(split, "Wrong type of split");
  split_ = split;

  // will be used in evaluateRemainingFilter
  currentSplitStr_ = split->filePath;

  if (splitReader_) {
    splitReader_.reset();
  }

  splitReader_ = createConfiguredSplitReader(split, false);
}

vector_size_t getLength(std::shared_ptr<ConnectorSplit>& split) {
  auto paimonConnectorSplit =
      std::dynamic_pointer_cast<PaimonConnectorSplit>(split);
  if (paimonConnectorSplit) {
    vector_size_t result = 0;
    for (const auto& hiveSplit : paimonConnectorSplit->hiveSplits) {
      result += hiveSplit->length;
    }
    return result;
  }

  auto hiveConnectorSplit =
      std::dynamic_pointer_cast<HiveConnectorSplit>(split);
  if (hiveConnectorSplit) {
    return hiveConnectorSplit->length;
  }

  BOLT_FAIL("Unsupported split type for getting length");
}

std::optional<RowVectorPtr> HiveDataSource::next(
    uint64_t size,
    bolt::ContinueFuture& /*future*/) {
  // first check whether the prepared split is corrupted and should be ignored.
  if (IgnoreCorruptFileHelper::isIgnoreCorruptFilesEnabled() &&
      split_ == nullptr) {
    LOG(WARNING) << "ignore corrupt split";
    return nullptr;
  }
  BOLT_CHECK(split_ != nullptr, "No split to process. Call addSplit first.");

  common::testutil::TestValue::adjust(
      "bytedance::bolt::connector::hive::HiveDataSource::next", this);

  if (splitReader_ && splitReader_->emptySplit()) {
    resetSplit();
    return nullptr;
  }

  if (!output_) {
    output_ = BaseVector::create(readerOutputType_, 0, pool_);
  }

  // TODO Check if remaining filter has a conjunct that doesn't depend on
  // any column, e.g. rand() < 0.1. Evaluate that conjunct first, then scan
  // only rows that passed.
  uint64_t rowsScanned = 0;
  TRY_WITH_IGNORE(
      connectorQueryCtx_->taskId(),
      rowsScanned = splitReader_->next(size, output_),
      {
        ignoredFileSizes_ += getLength(split_);
        resetSplit();
        return nullptr;
      });

  completedRows_ += rowsScanned;

  if (rowsScanned) {
    BOLT_CHECK(
        !output_->mayHaveNulls(), "Top-level row vector cannot have nulls");
    auto rowsRemaining = output_->size();
    if (rowsRemaining == 0) {
      // no rows passed the pushed down filters.
      output_->prepareForReuse();
      return getEmptyOutput();
    }

    auto rowVector = std::dynamic_pointer_cast<RowVector>(output_);
    // In case there is a remaining filter that excludes some but not all
    // rows, collect the indices of the passing rows. If there is no filter,
    // or it passes on all rows, leave this as null and let exec::wrap skip
    // wrapping the results.
    BufferPtr remainingIndices;
    if (postScanExprSet_) {
      std::vector<VectorPtr> results(postScanExprSet_->size());
      bool initialized = false;

      const int32_t pruningCount =
          static_cast<int32_t>(postScanPruningChannels_.size());
      if (pruningCount > 0) {
        SelectivityVector rows(rowVector->size());
        rows.setAll();
        exec::EvalCtx ctx(
            postScanExprSet_->execCtx(),
            postScanExprSet_.get(),
            rowVector.get(),
            currentSplitStr_);
        postScanExprSet_->eval(0, pruningCount, true, rows, ctx, results);
        initialized = true;
        for (int32_t i = 0; i < pruningCount; ++i) {
          rowVector->childAt(postScanPruningChannels_[i]) = results[i];
        }
      }

      if (postScanFilterExprIndex_ != -1) {
        const auto filterStartMicros = getCurrentTimeMicro();
        filterRows_.resize(rowVector->size());
        filterRows_.setAll();
        exec::EvalCtx ctx(
            postScanExprSet_->execCtx(),
            postScanExprSet_.get(),
            rowVector.get(),
            currentSplitStr_);
        postScanExprSet_->eval(
            postScanFilterExprIndex_,
            postScanFilterExprIndex_ + 1,
            !initialized,
            filterRows_,
            ctx,
            results);
        filterResult_ = results[postScanFilterExprIndex_];
        rowsRemaining = exec::processFilterResults(
            filterResult_, filterRows_, filterEvalCtx_, pool_);
        totalRemainingFilterTime_.fetch_add(
            (getCurrentTimeMicro() - filterStartMicros) * 1000,
            std::memory_order_relaxed);

        BOLT_CHECK_LE(rowsRemaining, rowsScanned);
        if (rowsRemaining == 0) {
          // No rows passed the remaining filter.
          output_->prepareForReuse();
          return getEmptyOutput();
        }

        if (rowsRemaining < rowVector->size()) {
          // Some, but not all rows passed the remaining filter.
          remainingIndices = filterEvalCtx_.selectedIndices;
        }
      }
    }

    if (outputType_->size() == 0) {
      return std::make_shared<RowVector>(
          pool_,
          outputType_,
          BufferPtr(nullptr),
          rowsRemaining,
          std::vector<VectorPtr>{});
    }

    std::vector<VectorPtr> outputColumns;
    outputColumns.reserve(outputType_->size());
    for (int i = 0; i < outputType_->size(); i++) {
      auto& child = rowVector->childAt(i);
      if (remainingIndices) {
        // Disable dictionary values caching in expression eval so that we
        // don't need to reallocate the result for every batch.
        child->disableMemo();
      }
      outputColumns.emplace_back(
          exec::wrapChild(rowsRemaining, remainingIndices, child));
    }

    return std::make_shared<RowVector>(
        pool_, outputType_, BufferPtr(nullptr), rowsRemaining, outputColumns);
  }

  resetSplit();
  return nullptr;
}

void HiveDataSource::addDynamicFilter(
    column_index_t outputChannel,
    const std::shared_ptr<common::Filter>& filter) {
  auto& fieldSpec = scanSpec_->getChildByChannel(outputChannel);
  fieldSpec.addFilter(*filter);
  scanSpec_->resetCachedValues(true);
  if (splitReader_) {
    splitReader_->resetFilterCaches();
  }

  // Convert dynamic filter to expression and stack it
  auto columnName = outputType_->nameOf(outputChannel);
  common::Subfield subfield(columnName);
  common::SubfieldFilters filters;
  filters.emplace(std::move(subfield), filter->clone());

  RowTypePtr schema = hiveTableHandle_->dataColumns();
  if (!schema || schema->size() == 0) {
    LOG(WARNING)
        << "HiveTableHandle dataColumns is empty. Falling back to outputType for dynamic filter generation.";
    schema = outputType_;
  }

  auto dynamicExpr = convertFiltersToExpr(filters, columnHandles_, schema);

  if (!dynamicExpr) {
    LOG(WARNING) << "Failed to generate dynamic expression for column: "
                 << columnName;
  }

  if (dynamicExpr) {
    if (currentRemainingFilter_) {
      currentRemainingFilter_ = std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          "and",
          std::vector<core::TypedExprPtr>{
              currentRemainingFilter_, dynamicExpr});
    } else {
      currentRemainingFilter_ = dynamicExpr;
    }

    rebuildPostScanExprSet();
  }
}

std::unordered_map<std::string, RuntimeCounter> HiveDataSource::runtimeStats() {
  auto res = runtimeStats_->toMap();
  auto& readStats = ioStats_->readStats();
  res.insert({
      {"numPrefetch", RuntimeCounter(ioStats_->prefetch().count())},
      {"prefetchBytes",
       RuntimeCounter(
           ioStats_->prefetch().sum(), RuntimeCounter::Unit::kBytes)},
      {"numStorageRead", RuntimeCounter(ioStats_->read().count())},
      {"storageReadBytes",
       RuntimeCounter(ioStats_->read().sum(), RuntimeCounter::Unit::kBytes)},
      {"numLocalRead", RuntimeCounter(ioStats_->ssdRead().count())},
      {"localReadBytes",
       RuntimeCounter(ioStats_->ssdRead().sum(), RuntimeCounter::Unit::kBytes)},
      {"numRamRead", RuntimeCounter(ioStats_->ramHit().count())},
      {"ramReadBytes",
       RuntimeCounter(ioStats_->ramHit().sum(), RuntimeCounter::Unit::kBytes)},
      {"totalScanTime",
       RuntimeCounter(ioStats_->totalScanTime(), RuntimeCounter::Unit::kNanos)},
      {"totalMergeTime",
       RuntimeCounter(
           ioStats_->totalMergeTime(), RuntimeCounter::Unit::kNanos)},
      {"loadMetaDataTime",
       RuntimeCounter(
           ioStats_->loadFileMetaDataTimeNs(), RuntimeCounter::Unit::kNanos)},
      {"totalRemainingFilterTime",
       RuntimeCounter(
           totalRemainingFilterTime_.load(std::memory_order_relaxed),
           RuntimeCounter::Unit::kNanos)},
      {"ioWaitWallNanos",
       RuntimeCounter(
           ioStats_->queryThreadIoLatency().sum() * 1000,
           RuntimeCounter::Unit::kNanos)},
      {"ioWaitNanosSync",
       RuntimeCounter(
           ioStats_->queryThreadIoLatencySync().sum() * 1000,
           RuntimeCounter::Unit::kNanos)},
      {"ioWaitNanosAsync",
       RuntimeCounter(
           ioStats_->queryThreadIoLatencyAsync().sum() * 1000,
           RuntimeCounter::Unit::kNanos)},
      {"maxSingleIoWaitNanos",
       RuntimeCounter(
           ioStats_->queryThreadIoLatency().max() * 1000,
           RuntimeCounter::Unit::kNanos)},
      {"overreadBytes",
       RuntimeCounter(
           ioStats_->rawOverreadBytes(), RuntimeCounter::Unit::kBytes)},
      {"ignoredCorruptFiles",
       RuntimeCounter(ignoredFileSizes_, RuntimeCounter::Unit::kBytes)},
      {"queryThreadIoLatency",
       RuntimeCounter(ioStats_->queryThreadIoLatency().count())},
      {"rawBytesRead",
       RuntimeCounter(ioStats_->rawBytesRead(), RuntimeCounter::Unit::kBytes)},
      {"rawBytesRead<4k",
       RuntimeCounter(
           readStats.rawBytesReads_[0], RuntimeCounter::Unit::kBytes)},
      {"rawBytesRead<32k",
       RuntimeCounter(
           readStats.rawBytesReads_[1], RuntimeCounter::Unit::kBytes)},
      {"rawBytesRead<128k",
       RuntimeCounter(
           readStats.rawBytesReads_[2], RuntimeCounter::Unit::kBytes)},
      {"rawBytesRead>=128k",
       RuntimeCounter(
           readStats.rawBytesReads_[3], RuntimeCounter::Unit::kBytes)},
      {"cntRead<4k",
       RuntimeCounter(readStats.cntReads_[0], RuntimeCounter::Unit::kNone)},
      {"cntRead<32k",
       RuntimeCounter(readStats.cntReads_[1], RuntimeCounter::Unit::kNone)},
      {"cntRead<128k",
       RuntimeCounter(readStats.cntReads_[2], RuntimeCounter::Unit::kNone)},
      {"cntRead>=128k",
       RuntimeCounter(readStats.cntReads_[3], RuntimeCounter::Unit::kNone)},
      {"totalTimeRead<4k",
       RuntimeCounter(
           readStats.totalTimeReads_[0], RuntimeCounter::Unit::kNanos)},
      {"totalTimeRead<32k",
       RuntimeCounter(
           readStats.totalTimeReads_[1], RuntimeCounter::Unit::kNanos)},
      {"totalTimeRead<128k",
       RuntimeCounter(
           readStats.totalTimeReads_[2], RuntimeCounter::Unit::kNanos)},
      {"totalTimeRead>=128k",
       RuntimeCounter(
           readStats.totalTimeReads_[3], RuntimeCounter::Unit::kNanos)},
  });
  return res;
}

void HiveDataSource::setFromDataSource(
    std::unique_ptr<DataSource> sourceUnique) {
  auto source = dynamic_cast<HiveDataSource*>(sourceUnique.get());
  BOLT_CHECK(source, "Bad DataSource type");

  split_ = std::move(source->split_);
  if (source->splitReader_ && source->splitReader_->emptySplit()) {
    runtimeStats_->skippedSplits += source->runtimeStats_->skippedSplits;
    runtimeStats_->skippedSplitBytes +=
        source->runtimeStats_->skippedSplitBytes;
    return;
  }
  source->scanSpec_->moveAdaptationFrom(*scanSpec_);
  scanSpec_ = std::move(source->scanSpec_);
  splitReader_ = std::move(source->splitReader_);
  // New io will be accounted on the stats of 'source'. Add the existing
  // balance to that.
  source->ioStats_->merge(*ioStats_);
  ioStats_ = std::move(source->ioStats_);
  source->runtimeStats_->merge(*runtimeStats_);
  runtimeStats_ = std::move(source->runtimeStats_);
  connectorQueryCtx_ = std::move(source->connectorQueryCtx_);
}

int64_t HiveDataSource::estimatedRowSize() {
  if (!splitReader_) {
    return kUnknownRowSize;
  }
  return splitReader_->estimatedRowSize();
}

void HiveDataSource::resetSplit() {
  if (splitReader_) {
    splitReader_->updateRuntimeStats(*runtimeStats_);
    splitReader_->resetSplit();
    // Keep readers around to hold adaptation.
  }
  split_.reset();
}

void HiveDataSource::recalculateRepDefConf(
    const RowTypePtr& rowType,
    const core::QueryConfig& queryConfig) {
  int32_t complexTypeCount = 0;
  constexpr int32_t kOnePageSampleThreshold = 10UL << 20;
  constexpr int32_t kMaxRepDefBufferSize = 16UL << 20;
  for (const auto& type : rowType->children()) {
    complexTypeCount += static_cast<int>(type->isArray() || type->isMap());
  }
  if (!complexTypeCount) {
    return;
  }
  decodeRepDefPageCount_ = hiveConfig_->decodeRepDefPageCount();
  int32_t repDefSingleLimit =
      queryConfig.parquetRepDefMemoryLimit() / complexTypeCount / 2;
  // INT32_MAX indicates decoding rep/def for whole rowgroup
  if (repDefSingleLimit < kOnePageSampleThreshold &&
      decodeRepDefPageCount_ != std::numeric_limits<int32_t>::max()) {
    decodeRepDefPageCount_ = std::max(1UL, repDefSingleLimit / (1UL << 20));
  }
  parquetRepDefMemoryLimit_ = std::min(kMaxRepDefBufferSize, repDefSingleLimit);
  LOG(INFO) << "recalculateRepDefConf complexTypeCount = " << complexTypeCount
            << ", repDefSingleLimit = " << succinctBytes(repDefSingleLimit)
            << ", decodeRepDefPageCount_ = " << decodeRepDefPageCount_;
}

} // namespace bytedance::bolt::connector::hive
