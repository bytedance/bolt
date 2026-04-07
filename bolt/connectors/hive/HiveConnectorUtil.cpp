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

#include "bolt/connectors/hive/HiveConnectorUtil.h"

#include "bolt/connectors/hive/FileHandle.h"
#include "bolt/connectors/hive/HiveConfig.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/CachedBufferedInput.h"
#include "bolt/dwio/common/DirectBufferedInput.h"
#include "bolt/dwio/common/Reader.h"
#include "bolt/expression/Expr.h"
#include "bolt/expression/ExprToSubfieldFilter.h"
#include "bolt/type/TimestampConversion.h"

namespace bytedance::bolt::connector::hive {

namespace {

struct SubfieldSpec {
  const common::Subfield* subfield;
  bool filterOnly;
};

template <typename T>
void deduplicate(std::vector<T>& values) {
  std::sort(values.begin(), values.end());
  values.erase(std::unique(values.begin(), values.end()), values.end());
}

// Floating point map key subscripts are truncated toward 0 in Presto.  For
// example given `a' as a map with floating point key, if user queries a[0.99],
// Presto coordinator will generate a required subfield a[0]; for a[-1.99] it
// will generate a[-1]; for anything larger than 9223372036854775807, it
// generates a[9223372036854775807]; for anything smaller than
// -9223372036854775808 it generates a[-9223372036854775808].
template <typename T>
std::unique_ptr<common::Filter> makeFloatingPointMapKeyFilter(
    const std::vector<int64_t>& subscripts) {
  std::vector<std::unique_ptr<common::Filter>> filters;
  for (auto subscript : subscripts) {
    T lower = subscript;
    T upper = subscript;
    bool lowerUnbounded = subscript == std::numeric_limits<int64_t>::min();
    bool upperUnbounded = subscript == std::numeric_limits<int64_t>::max();
    bool lowerExclusive = false;
    bool upperExclusive = false;
    if (lower <= 0 && !lowerUnbounded) {
      if (lower > subscript - 1) {
        lower = subscript - 1;
      } else {
        lower = std::nextafter(lower, -std::numeric_limits<T>::infinity());
      }
      lowerExclusive = true;
    }
    if (upper >= 0 && !upperUnbounded) {
      if (upper < subscript + 1) {
        upper = subscript + 1;
      } else {
        upper = std::nextafter(upper, std::numeric_limits<T>::infinity());
      }
      upperExclusive = true;
    }
    if (lowerUnbounded && upperUnbounded) {
      continue;
    }
    filters.push_back(std::make_unique<common::FloatingPointRange<T>>(
        lower,
        lowerUnbounded,
        lowerExclusive,
        upper,
        upperUnbounded,
        upperExclusive,
        false));
  }
  if (filters.size() == 1) {
    return std::move(filters[0]);
  }
  return std::make_unique<common::MultiRange>(std::move(filters), false, false);
}

// Recursively add subfields to scan spec.
void addSubfields(
    const Type& type,
    std::vector<SubfieldSpec>& subfields,
    int level,
    memory::MemoryPool* pool,
    common::ScanSpec& spec) {
  int newSize = 0;
  for (int i = 0; i < subfields.size(); ++i) {
    if (level < subfields[i].subfield->path().size()) {
      subfields[newSize++] = subfields[i];
    } else if (!subfields[i].filterOnly) {
      spec.addAllChildFields(type);
      return;
    }
  }
  subfields.resize(newSize);
  switch (type.kind()) {
    case TypeKind::ROW: {
      folly::F14FastMap<std::string, std::vector<SubfieldSpec>> required;
      for (auto& subfield : subfields) {
        auto* element = subfield.subfield->path()[level].get();
        auto* nestedField =
            dynamic_cast<const common::Subfield::NestedField*>(element);
        BOLT_CHECK(
            nestedField,
            "Unsupported for row subfields pruning: {}",
            element->toString());
        required[nestedField->name()].push_back(subfield);
      }
      auto& rowType = type.asRow();
      for (int i = 0; i < rowType.size(); ++i) {
        auto& childName = rowType.nameOf(i);
        auto& childType = rowType.childAt(i);
        auto* child = spec.addField(childName, i);
        auto it = required.find(childName);
        if (it == required.end()) {
          child->setConstantValue(
              BaseVector::createNullConstant(childType, 1, pool));
        } else {
          addSubfields(*childType, it->second, level + 1, pool, *child);
        }
      }
      break;
    }
    case TypeKind::MAP: {
      auto& keyType = type.childAt(0);
      auto* keys = spec.addMapKeyFieldRecursively(*keyType);
      addSubfields(
          *type.childAt(1),
          subfields,
          level + 1,
          pool,
          *spec.addMapValueField());
      if (subfields.empty()) {
        return;
      }
      bool stringKey = keyType->isVarchar() || keyType->isVarbinary();
      std::vector<std::string> stringSubscripts;
      std::vector<int64_t> longSubscripts;
      for (auto& subfield : subfields) {
        auto* element = subfield.subfield->path()[level].get();
        if (dynamic_cast<const common::Subfield::AllSubscripts*>(element)) {
          return;
        }
        if (stringKey) {
          auto* subscript =
              dynamic_cast<const common::Subfield::StringSubscript*>(element);
          BOLT_CHECK(
              subscript,
              "Unsupported for string map pruning: {}",
              element->toString());
          stringSubscripts.push_back(subscript->index());
        } else {
          auto* subscript =
              dynamic_cast<const common::Subfield::LongSubscript*>(element);
          BOLT_CHECK(
              subscript,
              "Unsupported for long map pruning: {}",
              element->toString());
          longSubscripts.push_back(subscript->index());
        }
      }
      std::unique_ptr<common::Filter> filter;
      if (stringKey) {
        deduplicate(stringSubscripts);
        filter = std::make_unique<common::BytesValues>(stringSubscripts, false);
      } else {
        deduplicate(longSubscripts);
        if (keyType->isReal()) {
          filter = makeFloatingPointMapKeyFilter<float>(longSubscripts);
        } else if (keyType->isDouble()) {
          filter = makeFloatingPointMapKeyFilter<double>(longSubscripts);
        } else {
          filter = common::createBigintValues(longSubscripts, false);
        }
      }
      keys->setFilter(std::move(filter));
      break;
    }
    case TypeKind::ARRAY: {
      addSubfields(
          *type.childAt(0),
          subfields,
          level + 1,
          pool,
          *spec.addArrayElementField());
      if (subfields.empty()) {
        return;
      }
      constexpr long kMaxIndex = std::numeric_limits<vector_size_t>::max();
      long maxIndex = -1;
      for (auto& subfield : subfields) {
        auto* element = subfield.subfield->path()[level].get();
        if (dynamic_cast<const common::Subfield::AllSubscripts*>(element)) {
          return;
        }
        auto* subscript =
            dynamic_cast<const common::Subfield::LongSubscript*>(element);
        BOLT_CHECK(
            subscript,
            "Unsupported for array pruning: {}",
            element->toString());
        BOLT_USER_CHECK_GT(
            subscript->index(),
            0,
            "Non-positive array subscript cannot be push down");
        maxIndex = std::max(maxIndex, std::min(kMaxIndex, subscript->index()));
      }
      spec.setMaxArrayElementsCount(maxIndex);
      break;
    }
    default:
      break;
  }
}

inline uint8_t parseDelimiter(const std::string& delim) {
  for (char const& ch : delim) {
    if (!std::isdigit(ch)) {
      return delim[0];
    }
  }
  return stoi(delim);
}

inline bool isSynthesizedColumn(
    const std::string& name,
    const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>&
        infoColumns) {
  return name == kPath || name == kBucket || infoColumns.count(name) != 0;
}

} // namespace

const std::string& getColumnName(const common::Subfield& subfield) {
  BOLT_CHECK_GT(subfield.path().size(), 0);
  auto* field = dynamic_cast<const common::Subfield::NestedField*>(
      subfield.path()[0].get());
  BOLT_CHECK_NOT_NULL(field);
  return field->name();
}

void checkColumnNameLowerCase(const std::shared_ptr<const Type>& type) {
  switch (type->kind()) {
    case TypeKind::ARRAY:
      checkColumnNameLowerCase(type->asArray().elementType());
      break;
    case TypeKind::MAP: {
      checkColumnNameLowerCase(type->asMap().keyType());
      checkColumnNameLowerCase(type->asMap().valueType());

    } break;
    case TypeKind::ROW: {
      for (const auto& outputName : type->asRow().names()) {
        BOLT_CHECK(!std::any_of(outputName.begin(), outputName.end(), isupper));
      }
      for (auto& childType : type->asRow().children()) {
        checkColumnNameLowerCase(childType);
      }
    } break;
    default:
      VLOG(3) << "No need to check type lowercase mode:" << type->toString();
  }
}

void checkColumnNameLowerCase(
    const SubfieldFilters& filters,
    const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>&
        infoColumns) {
  for (auto& pair : filters) {
    if (auto name = pair.first.toString();
        isSynthesizedColumn(name, infoColumns)) {
      continue;
    }
    const auto& path = pair.first.path();

    for (int i = 0; i < path.size(); ++i) {
      auto* nestedField =
          dynamic_cast<const common::Subfield::NestedField*>(path[i].get());
      if (nestedField == nullptr) {
        continue;
      }
      BOLT_CHECK(!std::any_of(
          nestedField->name().begin(), nestedField->name().end(), isupper));
    }
  }
}

void checkColumnNameLowerCase(const core::TypedExprPtr& typeExpr) {
  if (typeExpr == nullptr) {
    return;
  }
  // for function like from_json(col1, 'array<struct<index:string,
  // dataType:string>>') RowType's column name 'dataType' has upper case but it
  // is correct so skip check for from_json 's type
  if (!typeExpr->skipLowerCaseCheck()) {
    checkColumnNameLowerCase(typeExpr->type());
  }
  for (auto& type : typeExpr->inputs()) {
    checkColumnNameLowerCase(type);
  }
}

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
    core::ExpressionEvaluator* expressionEvaluator,
    dwio::common::RuntimeStatistics* statis,
    const std::vector<std::tuple<size_t, std::optional<std::string>>>&
        rowIndexColumns) {
  auto spec = std::make_shared<common::ScanSpec>("root");
  spec->setRuntimeStatistics(statis);
  spec->setExpressionEvaluator(expressionEvaluator);
  folly::F14FastMap<std::string, std::vector<const common::Subfield*>>
      filterSubfields;
  std::vector<SubfieldSpec> subfieldSpecs;
  for (auto& [subfield, _] : filters) {
    if (auto name = subfield.toString();
        !isSynthesizedColumn(name, infoColumns) &&
        partitionKeys.count(name) == 0) {
      filterSubfields[getColumnName(subfield)].push_back(&subfield);
    }
  }
  auto rowIdxIter = rowIndexColumns.begin();
  // Process columns that will be projected out.
  for (int i = 0; i < rowType->size(); ++i) {
    const auto& name = rowType->nameOf(i);
    const auto& type = rowType->childAt(i);
    const bool isRowIndexColumn =
        rowIdxIter != rowIndexColumns.end() && i == std::get<0>(*rowIdxIter);
    auto it = outputSubfields.find(name);
    if (it == outputSubfields.end()) {
      const auto& childSpec = spec->addFieldRecursively(name, *type, i);
      if (isRowIndexColumn) {
        childSpec->setIsRowIndex(true);
        childSpec->setRowIndexColumnName(std::get<1>(*rowIdxIter));
        ++rowIdxIter;
      }
      filterSubfields.erase(name);
      continue;
    }
    for (auto* subfield : it->second) {
      subfieldSpecs.push_back({subfield, false});
    }
    it = filterSubfields.find(name);
    if (it != filterSubfields.end()) {
      for (auto* subfield : it->second) {
        subfieldSpecs.push_back({subfield, true});
      }
      filterSubfields.erase(it);
    }
    auto* childSpec = spec->addField(name, i);
    if (isRowIndexColumn) {
      childSpec->setIsRowIndex(true);
      childSpec->setRowIndexColumnName(std::get<1>(*rowIdxIter));
      ++rowIdxIter;
    }
    addSubfields(*type, subfieldSpecs, 1, pool, *childSpec);
    subfieldSpecs.clear();
  }

  // Now process the columns that will not be projected out.
  if (!filterSubfields.empty()) {
    BOLT_CHECK_NOT_NULL(dataColumns);
    for (auto& [fieldName, subfields] : filterSubfields) {
      for (auto* subfield : subfields) {
        subfieldSpecs.push_back({subfield, true});
      }
      auto& type = dataColumns->findChild(fieldName);
      auto* fieldSpec = spec->getOrCreateChild(fieldName);
      addSubfields(*type, subfieldSpecs, 1, pool, *fieldSpec);
      subfieldSpecs.clear();
    }
  }

  for (auto& pair : filters) {
    // SelectiveColumnReader doesn't support constant columns with filters,
    // hence, we can't have a filter for a $path or $bucket column.
    //
    // Unfortunately, Presto happens to specify a filter for $path, $file_size,
    // $file_modified_time or $bucket column. This filter is redundant and needs
    // to be removed.
    // TODO Remove this check when Presto is fixed to not specify a filter
    // on $path and $bucket column.
    if (auto name = pair.first.toString();
        isSynthesizedColumn(name, infoColumns)) {
      continue;
    }
    auto fieldSpec = spec->getOrCreateChild(pair.first);
    fieldSpec->addFilter(*pair.second);
  }

  return spec;
}

std::unique_ptr<dwio::common::SerDeOptions> parseSerdeParameters(
    const std::unordered_map<std::string, std::string>& serdeParameters) {
  auto fieldIt = serdeParameters.find(dwio::common::SerDeOptions::kFieldDelim);
  if (fieldIt == serdeParameters.end()) {
    fieldIt = serdeParameters.find("serialization.format");
  }
  auto collectionIt =
      serdeParameters.find(dwio::common::SerDeOptions::kCollectionDelim);
  if (collectionIt == serdeParameters.end()) {
    // For collection delimiter, Hive 1.x, 2.x uses "colelction.delim", but
    // Hive 3.x uses "collection.delim".
    // See: https://issues.apache.org/jira/browse/HIVE-16922)
    collectionIt = serdeParameters.find("colelction.delim");
  }
  auto mapKeyIt =
      serdeParameters.find(dwio::common::SerDeOptions::kMapKeyDelim);

  if (fieldIt == serdeParameters.end() &&
      collectionIt == serdeParameters.end() &&
      mapKeyIt == serdeParameters.end()) {
    return nullptr;
  }

  uint8_t fieldDelim = '\1';
  uint8_t collectionDelim = '\2';
  uint8_t mapKeyDelim = '\3';
  if (fieldIt != serdeParameters.end()) {
    fieldDelim = parseDelimiter(fieldIt->second);
  }
  if (collectionIt != serdeParameters.end()) {
    collectionDelim = parseDelimiter(collectionIt->second);
  }
  if (mapKeyIt != serdeParameters.end()) {
    mapKeyDelim = parseDelimiter(mapKeyIt->second);
  }
  auto serDeOptions = std::make_unique<dwio::common::SerDeOptions>(
      fieldDelim, collectionDelim, mapKeyDelim);
  return serDeOptions;
}

void configureReaderOptions(
    dwio::common::ReaderOptions& readerOptions,
    const std::shared_ptr<HiveConfig>& hiveConfig,
    const config::ConfigBase* sessionProperties,
    const RowTypePtr& fileSchema,
    std::shared_ptr<HiveConnectorSplit> hiveSplit) {
  readerOptions.setMaxCoalesceBytes(
      hiveConfig->maxCoalescedBytes(sessionProperties));
  readerOptions.setMaxCoalesceDistance(
      hiveConfig->maxCoalescedDistanceBytes(sessionProperties));
  readerOptions.setFileColumnNamesReadAsLowerCase(
      hiveConfig->isFileColumnNamesReadAsLowerCase(sessionProperties));
  bool useColumnNames = false;
  if (hiveSplit->fileFormat == dwio::common::FileFormat::ORC ||
      hiveSplit->fileFormat == dwio::common::FileFormat::DWRF) {
    useColumnNames = hiveConfig->isOrcUseColumnNames(sessionProperties);
  } else if (hiveSplit->fileFormat == dwio::common::FileFormat::PARQUET) {
    useColumnNames = hiveConfig->isParquetUseColumnNames(sessionProperties);
  }
  readerOptions.setUseColumnNamesForColumnMapping(useColumnNames);
  readerOptions.setFileSchema(fileSchema);
  readerOptions.setFooterEstimatedSize(hiveConfig->footerEstimatedSize());
  readerOptions.setFilePreloadThreshold(hiveConfig->filePreloadThreshold());
  readerOptions.setPrefetchRowGroups(hiveConfig->prefetchRowGroups());
  readerOptions.setLoadQuantum(hiveConfig->loadQuantum());
  readerOptions.setPrefetchMemoryPercent(hiveConfig->prefetchMemoryPercent());

  if (hiveSplit->fileFormat == dwio::common::FileFormat::ORC) {
    readerOptions.setUseNestedColumnNamesForColumnMapping(
        hiveConfig->isOrcUseNestedColumnNames(sessionProperties));
  }

  auto serDeOptions = parseSerdeParameters(hiveSplit->serdeParameters);
  if (serDeOptions) {
    readerOptions.setSerDeOptions(*serDeOptions);
  }
  if (readerOptions.getFileFormat() != dwio::common::FileFormat::UNKNOWN) {
    BOLT_CHECK(
        readerOptions.getFileFormat() == hiveSplit->fileFormat,
        "HiveDataSource received splits of different formats: {} and {}",
        dwio::common::toString(readerOptions.getFileFormat()),
        dwio::common::toString(hiveSplit->fileFormat));
  } else {
    readerOptions.setFileFormat(hiveSplit->fileFormat);
  }
  // std::cout << readerOptions.toString() << std::endl;
}

void configureRowReaderOptions(
    dwio::common::RowReaderOptions& rowReaderOptions,
    const std::unordered_map<std::string, std::string>& tableParameters,
    std::shared_ptr<common::ScanSpec> scanSpec,
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    const RowTypePtr& rowType,
    const std::shared_ptr<const HiveConnectorSplit>& hiveSplit,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    const config::ConfigBase* sessionProperties) {
  auto skipRowsIt =
      tableParameters.find(dwio::common::TableParameter::kSkipHeaderLineCount);
  if (skipRowsIt != tableParameters.end()) {
    rowReaderOptions.setSkipRows(folly::to<uint64_t>(skipRowsIt->second));
  }

  rowReaderOptions.setScanSpec(scanSpec);
  rowReaderOptions.setMetadataFilter(metadataFilter);
  std::vector<std::string> columnNames;
  for (auto& spec : scanSpec->children()) {
    if (!spec->isConstant()) {
      columnNames.push_back(spec->fieldName());
    }
  }
  std::shared_ptr<dwio::common::ColumnSelector> cs;
  if (columnNames.empty()) {
    static const RowTypePtr kEmpty{ROW({}, {})};
    cs = std::make_shared<dwio::common::ColumnSelector>(kEmpty);
  } else {
    cs = std::make_shared<dwio::common::ColumnSelector>(rowType, columnNames);
  }
  rowReaderOptions.select(cs).range(hiveSplit->start, hiveSplit->length);
  if (hiveSplit->fileFormat == dwio::common::FileFormat::ORC ||
      hiveSplit->fileFormat == dwio::common::FileFormat::DWRF) {
    rowReaderOptions.setUseColumnNamesForColumnMapping(
        hiveConfig->isOrcUseColumnNames(sessionProperties));
  } else if (hiveSplit->fileFormat == dwio::common::FileFormat::PARQUET) {
    rowReaderOptions.setUseColumnNamesForColumnMapping(
        hiveConfig->isParquetUseColumnNames(sessionProperties));
  }
  if (hiveConfig && sessionProperties) {
    rowReaderOptions.setTimestampPrecision(static_cast<TimestampPrecision>(
        hiveConfig->readTimestampUnit(sessionProperties)));
  }

  auto isDictionaryFilterEnabled = hiveConfig->isDictionaryFilterEnabled();
  rowReaderOptions.setEnableDictionaryFilter(isDictionaryFilterEnabled);

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "RowReaderOptions values:" << rowReaderOptions.toString();
  }
}

namespace {
bool applyPartitionFilter(
    const TypePtr& type,
    const std::string& partitionValue,
    common::Filter* filter) {
  try {
    if (type->isDate()) {
      const auto result = util::castFromDateString(
          StringView(partitionValue), false /*isIso8601*/);
      BOLT_CHECK(result.has_value());
      return applyFilter(*filter, result.value());
    }

    switch (type->kind()) {
      case TypeKind::BIGINT:
      case TypeKind::INTEGER:
      case TypeKind::SMALLINT:
      case TypeKind::TINYINT: {
        return applyFilter(*filter, folly::to<int64_t>(partitionValue));
      }
      case TypeKind::REAL:
      case TypeKind::DOUBLE: {
        return applyFilter(*filter, folly::to<double>(partitionValue));
      }
      case TypeKind::BOOLEAN: {
        return applyFilter(*filter, folly::to<bool>(partitionValue));
      }
      case TypeKind::VARCHAR: {
        return applyFilter(*filter, partitionValue);
      }
      default:
        BOLT_FAIL(
            "Bad type {} for partition value: {}",
            type->kind(),
            partitionValue);
        break;
    }
  } catch (const std::exception& ex) {
    BOLT_FAIL(
        "applyPartitionFilter throw exception while convert partition value {} from string to {}, errmsg {}",
        partitionValue,
        type->kind(),
        ex.what());
  }
}
} // namespace

bool isHiveNull(const std::string& source) {
  return (source.size() == 2 && source.compare("\\N") == 0);
}

bool testFilters(
    common::ScanSpec* scanSpec,
    dwio::common::Reader* reader,
    const std::string& filePath,
    const std::unordered_map<std::string, std::optional<std::string>>&
        partitionKeys,
    std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
        partitionKeysHandle) {
  const auto totalRows = reader->numberOfRows();
  const auto& fileTypeWithId = reader->typeWithId();
  const auto& rowType = reader->rowType();
  for (const auto& child : scanSpec->children()) {
    if (child->filter()) {
      const auto& name = child->fieldName();
      if (!rowType->containsChild(name)) {
        // If missing column is partition key.
        auto iter = partitionKeys.find(name);
        if (iter != partitionKeys.end() && iter->second.has_value()) {
          if (isHiveNull(iter->second.value())) {
            if (!child->filter()->testNull()) {
              return false;
            }
          } else {
            if (!applyPartitionFilter(
                    (*partitionKeysHandle)[name]->dataType(),
                    iter->second.value(),
                    child->filter())) {
              return false;
            }
          }
          continue;
        }
        // Column is missing. Most likely due to schema evolution.
        if (child->filter()->isDeterministic() &&
            !child->filter()->testNull()) {
          return false;
        }
      } else {
        const auto& typeWithId = fileTypeWithId->childByName(name);
        const auto columnStats = reader->columnStatistics(typeWithId->id());
        if (columnStats != nullptr &&
            !testFilter(
                child->filter(),
                columnStats.get(),
                totalRows.value(),
                typeWithId->type())) {
          VLOG(1) << "Skipping " << filePath
                  << " based on stats and filter for column "
                  << child->fieldName();
          return false;
        }
      }
    }
  }

  return true;
}

std::unique_ptr<dwio::common::BufferedInput> createBufferedInput(
    const FileHandle& fileHandle,
    const dwio::common::ReaderOptions& readerOpts,
    const ConnectorQueryCtx* connectorQueryCtx,
    std::shared_ptr<io::IoStatistics> ioStats,
    folly::Executor* executor,
    bool judgeCache,
    std::vector<int>& columnCacheBlackList,
    const HiveConnectorSplitCacheLimit* hiveConnectorSplitCacheLimit) {
  if (connectorQueryCtx->cache() && judgeCache) {
    return std::make_unique<dwio::common::CachedBufferedInput>(
        fileHandle.file,
        dwio::common::MetricsLog::voidLog(),
        fileHandle.uuid.id(),
        connectorQueryCtx->cache(),
        Connector::getTracker(
            connectorQueryCtx->scanId(), readerOpts.loadQuantum()),
        fileHandle.groupId.id(),
        ioStats,
        executor,
        readerOpts,
        hiveConnectorSplitCacheLimit
            ? hiveConnectorSplitCacheLimit->splitSoftAffinityCacheable
            : true,
        std::move(columnCacheBlackList));
  }
  return std::make_unique<dwio::common::DirectBufferedInput>(
      fileHandle.file,
      dwio::common::MetricsLog::voidLog(),
      fileHandle.uuid.id(),
      Connector::getTracker(
          connectorQueryCtx->scanId(), readerOpts.loadQuantum()),
      fileHandle.groupId.id(),
      ioStats,
      executor,
      readerOpts,
      connectorQueryCtx->asyncThreadCtx());
}

namespace {

core::CallTypedExprPtr replaceInputs(
    const core::CallTypedExpr* call,
    std::vector<core::TypedExprPtr>&& inputs) {
  return std::make_shared<core::CallTypedExpr>(
      call->type(), std::move(inputs), call->name());
}

bool endWith(const std::string& str, const char* suffix) {
  int len = strlen(suffix);
  if (str.size() < len) {
    return false;
  }
  for (int i = 0, j = str.size() - len; i < len; ++i, ++j) {
    if (str[j] != suffix[i]) {
      return false;
    }
  }
  return true;
}

bool isNotExpr(
    const core::TypedExprPtr& expr,
    const core::CallTypedExpr* call,
    core::ExpressionEvaluator* evaluator) {
  if (!endWith(call->name(), "not")) {
    return false;
  }
  auto exprs = evaluator->compile(expr);
  BOLT_CHECK_EQ(exprs->size(), 1);
  auto& compiled = exprs->expr(0);
  return compiled->vectorFunction() &&
      compiled->vectorFunction()->getCanonicalName() ==
      exec::FunctionCanonicalName::kNot;
}

core::TypedExprPtr extractFiltersFromRemainingFilter(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator,
    bool negated,
    common::SubfieldFilters& filters) {
  auto* call = dynamic_cast<const core::CallTypedExpr*>(expr.get());
  if (call == nullptr) {
    return expr;
  }
  common::Filter* oldFilter = nullptr;
  try {
    if (auto subfieldAndFilter =
            exec::ExprToSubfieldFilterParser::getInstance()
                ->leafCallToSubfieldFilter(*call, evaluator, negated)) {
      auto& [subfield, filter] = subfieldAndFilter.value();
      if (auto it = filters.find(subfield); it != filters.end()) {
        oldFilter = it->second.get();
        filter = filter->mergeWith(oldFilter);
      }
      filters.insert_or_assign(std::move(subfield), std::move(filter));
      return nullptr;
    }
  } catch (const BoltException&) {
    LOG(WARNING) << "Unexpected failure when extracting filter for: "
                 << expr->toString();
    if (oldFilter) {
      LOG(WARNING) << "Merging with " << oldFilter->toString();
    }
  }

  if (isNotExpr(expr, call, evaluator)) {
    auto inner = extractFiltersFromRemainingFilter(
        call->inputs()[0], evaluator, !negated, filters);
    return inner ? replaceInputs(call, {inner}) : nullptr;
  }

  if ((call->name() == "and" && !negated) ||
      (call->name() == "or" && negated)) {
    std::vector<core::TypedExprPtr> args;
    args.reserve(call->inputs().size());
    for (const auto& input : call->inputs()) {
      if (auto arg = extractFiltersFromRemainingFilter(
              input, evaluator, negated, filters)) {
        args.push_back(std::move(arg));
      }
      // If extractFiltersFromRemainingFilter returns nullptr, it means
      // everything in input is converted to filters.
    }
    if (args.empty()) {
      return nullptr;
    }
    if (args.size() == 1) {
      return std::move(args[0]);
    }
    return replaceInputs(call, std::move(args));
  }

  return expr;
}

} // namespace

core::TypedExprPtr extractFiltersFromRemainingFilter(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator,
    common::SubfieldFilters& filters) {
  return extractFiltersFromRemainingFilter(
      expr, evaluator, /*negated=*/false, filters);
}

} // namespace bytedance::bolt::connector::hive
