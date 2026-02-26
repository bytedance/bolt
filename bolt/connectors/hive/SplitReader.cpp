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

#include "bolt/connectors/hive/SplitReader.h"
#include <cstdint>

#include "bolt/common/caching/CacheTTLController.h"
#include "bolt/connectors/hive/HiveConfig.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/connectors/hive/HiveConnectorUtil.h"
#include "bolt/connectors/hive/PaimonConstants.h"
#include "bolt/connectors/hive/PaimonMetadataColumn.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/dwio/common/ReaderFactory.h"
#include "bolt/dwio/paimon/deletionvectors/DeletionFileReader.h"
#include "bolt/type/Conversions.h"

namespace bytedance::bolt::connector::hive {

namespace {
template <TypeKind kind>
VectorPtr newConstantFromString(
    const TypePtr& type,
    const std::optional<std::string>& value,
    vector_size_t size,
    bolt::memory::MemoryPool* pool) {
  using T = typename TypeTraits<kind>::NativeType;
  if (!value.has_value()) {
    return std::make_shared<ConstantVector<T>>(pool, size, true, type, T());
  }

  if (type->isDate()) {
    auto copy =
        util::castFromDateString(StringView(value.value()), true /*isIso8601*/);
    return std::make_shared<ConstantVector<int32_t>>(
        pool, size, false, type, copy.has_value() ? *copy : 0);
  }

  if constexpr (std::is_same_v<T, StringView>) {
    return std::make_shared<ConstantVector<StringView>>(
        pool, size, false, type, StringView(value.value()));
  } else {
    auto copy = bolt::util::Converter<kind>::cast(value.value(), nullptr);
    // It is guaranteed that the value string is in UTC timezone.
    // if constexpr (kind == TypeKind::TIMESTAMP) {
    //   copy.toGMT(Timestamp::defaultTimezone());
    // }
    return std::make_shared<ConstantVector<T>>(
        pool, size, false, type, std::move(copy));
  }
}
} // namespace

namespace {

bool applyPartitionFilter(
    TypeKind kind,
    const std::string& partitionValue,
    common::Filter* filter) {
  switch (kind) {
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
      BOLT_FAIL("Bad type {} for partition value: {}", kind, partitionValue);
      break;
  }
}

} // namespace

std::unique_ptr<SplitReader> SplitReader::create(
    const std::shared_ptr<bolt::connector::hive::HiveConnectorSplit>& hiveSplit,
    const std::shared_ptr<HiveTableHandle>& hiveTableHandle,
    const std::shared_ptr<common::ScanSpec>& scanSpec,
    const RowTypePtr& readerOutputType,
    std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
        partitionKeys,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<HiveConfig>& hiveConfig,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const bool isPartOfPaimonSplit) {
  return std::make_unique<SplitReader>(
      hiveSplit,
      hiveTableHandle,
      scanSpec,
      readerOutputType,
      partitionKeys,
      fileHandleFactory,
      executor,
      connectorQueryCtx,
      hiveConfig,
      ioStats,
      isPartOfPaimonSplit);
}

SplitReader::SplitReader(
    const std::shared_ptr<bolt::connector::hive::HiveConnectorSplit>& hiveSplit,
    const std::shared_ptr<HiveTableHandle>& hiveTableHandle,
    const std::shared_ptr<common::ScanSpec>& scanSpec,
    const RowTypePtr& readerOutputType,
    std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
        partitionKeys,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<HiveConfig>& hiveConfig,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const bool isPartOfPaimonSplit)
    : hiveSplit_(hiveSplit),
      hiveTableHandle_(hiveTableHandle),
      scanSpec_(scanSpec),
      readerOutputType_(readerOutputType),
      partitionKeys_(partitionKeys),
      pool_(connectorQueryCtx->memoryPool()),
      fileHandleFactory_(fileHandleFactory),
      executor_(executor),
      connectorQueryCtx_(connectorQueryCtx),
      hiveConfig_(hiveConfig),
      ioStats_(ioStats),
      baseReaderOpts_(connectorQueryCtx->memoryPool()),
      isPartOfPaimonSplit_(isPartOfPaimonSplit) {
  if ((hiveSplit->fileFormat == dwio::common::FileFormat::TEXT) &&
      hiveTableHandle->isFilterPushdownEnabled()) {
    BOLT_FAIL(
        "{} reader does not support filter pushdown yet!",
        hiveSplit->fileFormat);
  }
  computeMetadataColumns();
}

SplitReader::~SplitReader() = default;

void SplitReader::computeMetadataColumns() {
  auto isReadAsLowercase = hiveConfig_->isFileColumnNamesReadAsLowerCase(
      connectorQueryCtx_->sessionProperties());
  std::string rowIdColumnName = paimon::kColumnNameRowID;
  std::string sequenceNumberColumnName = paimon::kSEQUENCE_NUMBER;
  if (isReadAsLowercase) {
    folly::toLowerAscii(rowIdColumnName);
    folly::toLowerAscii(sequenceNumberColumnName);
  }
  if (scanSpec_->childByName(rowIdColumnName) != nullptr) {
    const auto& firstRowIDIter =
        hiveSplit_->customSplitInfo.find(paimon::kFileMetaFirstRowID);
    if (firstRowIDIter != hiveSplit_->customSplitInfo.end()) {
      char* end;
      const auto firstRowID =
          std::strtol(firstRowIDIter->second.c_str(), &end, 10);
      BOLT_CHECK_NE(
          end,
          nullptr,
          "Failed to convert _FIRST_ROW_ID {} to long",
          firstRowIDIter->second);
      auto* const rowIDChildSpec = scanSpec_->childByName(rowIdColumnName);
      rowIDChildSpec->setRowIndexBase(firstRowID);
    }
  }

  const auto& children = readerOutputType_->names();
  for (auto i = 0; i < children.size(); ++i) {
    const auto& name = children[i];
    if (paimon::kColumnNameFilePath == name) {
      metadataColumns_[i] = std::make_shared<paimon::MetadataColumnFilePath>(
          hiveSplit_->filePath);
    } else if (paimon::kColumnNameBucket == name) {
      BOLT_CHECK(
          hiveSplit_->tableBucketNumber.has_value(),
          "Bucket value is not set when paimon bucket column is requested");
      metadataColumns_[i] = std::make_shared<paimon::MetadataColumnBucket>(
          hiveSplit_->tableBucketNumber.value());
    } else if (paimon::kColumnNamePartition == name) {
      auto partType = readerOutputType_->childAt(i);
      metadataColumns_[i] = std::make_shared<paimon::MetadataColumnPartition>(
          partType, hiveSplit_->partitionKeys, pool_);
    } else if (sequenceNumberColumnName == name) {
      auto maxSequenceNumberIter =
          hiveSplit_->customSplitInfo.find(paimon::kFileMetaMaxSequenceNumber);
      if (maxSequenceNumberIter == hiveSplit_->customSplitInfo.end()) {
        continue;
      }
      char* end;
      auto maxSequenceNumber =
          std::strtol(maxSequenceNumberIter->second.c_str(), &end, 10);
      BOLT_CHECK_NE(
          end,
          nullptr,
          "Failed to convert _MAX_SEQUENCE_NUMBER {} to long",
          maxSequenceNumberIter->second);
      metadataColumns_[i] =
          std::make_shared<paimon::MetadataColumnSequenceNumber>(
              maxSequenceNumber);
    }
  }
}

void SplitReader::configureReaderOptions() {
  hive::configureReaderOptions(
      baseReaderOpts_,
      hiveConfig_,
      connectorQueryCtx_->sessionProperties(),
      hiveTableHandle_->dataColumns(),
      hiveSplit_);
}

void SplitReader::prepareSplit(
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    dwio::common::RuntimeStatistics& runtimeStats,
    filesystems::FileOptions& options,
    bool judgeCache,
    std::vector<int>& columnCacheBlackList,
    const HiveConnectorSplitCacheLimit* hiveConnectorSplitCacheLimit) {
  BOLT_CHECK_NE(
      baseReaderOpts_.getFileFormat(), dwio::common::FileFormat::UNKNOWN);

  std::shared_ptr<FileHandle> fileHandle;
  try {
    fileHandle =
        fileHandleFactory_->generate(hiveSplit_->filePath, options).second;
  } catch (BoltRuntimeError& e) {
    if (e.errorCode() == error_code::kFileNotFound.c_str() &&
        hiveConfig_->ignoreMissingFiles(
            connectorQueryCtx_->sessionProperties())) {
      emptySplit_ = true;
      return;
    } else {
      throw;
    }
  }
  // Here we keep adding new entries to CacheTTLController when new fileHandles
  // are generated, if CacheTTLController was created. Creator of
  // CacheTTLController needs to make sure a size control strategy was available
  // such as removing aged out entries.
  if (auto* cacheTTLController = cache::CacheTTLController::getInstance()) {
    cacheTTLController->addOpenFileInfo(fileHandle->uuid.id());
  }
  auto baseFileInput = createBufferedInput(
      *fileHandle,
      baseReaderOpts_,
      connectorQueryCtx_,
      ioStats_,
      executor_,
      judgeCache,
      columnCacheBlackList,
      hiveConnectorSplitCacheLimit);

  baseReader_ = dwio::common::getReaderFactory(baseReaderOpts_.getFileFormat())
                    ->createReader(std::move(baseFileInput), baseReaderOpts_);

  // only for testing
  if (UNLIKELY(
          !FLAGS_testing_only_set_scan_exception_mesg_for_prepare.empty())) {
    throw std::runtime_error(
        FLAGS_testing_only_set_scan_exception_mesg_for_prepare);
  }

  // Note that this doesn't apply to Hudi tables.
  emptySplit_ = false;
  if (baseReader_->numberOfRows() == 0) {
    emptySplit_ = true;
    return;
  }

  // Check filters and see if the whole split can be skipped.
  // Not all formats or tables support this (e.g. Hudi, TXT)
  if ((baseReaderOpts_.getFileFormat() != dwio::common::FileFormat::TEXT) &&
      !testFilters(
          scanSpec_.get(),
          baseReader_.get(),
          hiveSplit_->filePath,
          hiveSplit_->partitionKeys,
          partitionKeys_)) {
    emptySplit_ = true;
    ++runtimeStats.skippedSplits;
    runtimeStats.skippedSplitBytes += hiveSplit_->length;
    return;
  }
  ++runtimeStats.processedSplits;
  auto& fileType = baseReader_->rowType();
  auto columnTypes = adaptColumns(fileType, baseReaderOpts_.getFileSchema());

  configureRowReaderOptions(
      baseRowReaderOpts_,
      hiveTableHandle_->tableParameters(),
      scanSpec_,
      metadataFilter,
      ROW(std::vector<std::string>(fileType->names()), std::move(columnTypes)),
      hiveSplit_,
      hiveConfig_,
      connectorQueryCtx_->sessionProperties());
  // NOTE: we firstly reset the finished 'baseRowReader_' of previous split
  // before setting up for the next one to avoid doubling the peak memory usage.
  baseRowReader_.reset();
  baseRowReader_ = baseReader_->createRowReader(baseRowReaderOpts_);

  checkAndCreatePaimonDeletionFileReader(options, judgeCache);
}

std::vector<TypePtr> SplitReader::adaptColumns(
    const RowTypePtr& fileType,
    const std::shared_ptr<const bolt::RowType>& tableSchema) {
  // Keep track of schema types for columns in file, used by ColumnSelector.
  std::vector<TypePtr> columnTypes = fileType->children();
  auto isRangePartitionColumn =
      [&](const std::string& fieldName,
          const std::optional<std::string>& partitionValue) {
        return (
            partitionValue.has_value() && partitionValue->size() >= 1 &&
            partitionValue->at(0) == '~' && fileType->containsChild(fieldName));
      };

  const auto& childrenSpecs = scanSpec_->children();
  for (size_t i = 0; i < childrenSpecs.size(); ++i) {
    auto* childSpec = childrenSpecs[i].get();
    const std::string& fieldName = childSpec->fieldName();
    const auto readerOutputIdx =
        readerOutputType_->getChildIdxIfExists(fieldName);
    const auto& metadataColumn = readerOutputIdx.has_value()
        ? metadataColumns_.find(readerOutputIdx.value())
        : metadataColumns_.end();
    std::string sequenceNumberColumnName = paimon::kSEQUENCE_NUMBER;
    if (hiveConfig_->isFileColumnNamesReadAsLowerCase(
            connectorQueryCtx_->sessionProperties())) {
      folly::toLowerAscii(sequenceNumberColumnName);
    }
    auto iter = hiveSplit_->partitionKeys.find(fieldName);
    if (iter != hiveSplit_->partitionKeys.end()) {
      if (isRangePartitionColumn(fieldName, iter->second)) {
        childSpec->setConstantValue(nullptr);
      } else {
        if (partitionKeys_->find(fieldName) == partitionKeys_->end()) {
          BOLT_CHECK(
              isPartOfPaimonSplit_,
              "ColumnHandle is missing for partition key in non paimon case {}",
              fieldName);
          auto dataType = fileType->findChild(fieldName);
          partitionKeys_->emplace(
              fieldName,
              std::make_shared<HiveColumnHandle>(
                  fieldName,
                  HiveColumnHandle::ColumnType::kPartitionKey,
                  dataType,
                  dataType));
        }

        setPartitionValue(
            childSpec,
            fieldName,
            iter->second,
            iter->second.has_value() ? isHiveNull(iter->second.value())
                                     : false);
      }
    } else if (fieldName == kPath) {
      auto constantVec = std::make_shared<ConstantVector<StringView>>(
          connectorQueryCtx_->memoryPool(),
          1,
          false,
          VARCHAR(),
          StringView(hiveSplit_->filePath));
      childSpec->setConstantValue(constantVec);
    } else if (fieldName == kBucket) {
      if (hiveSplit_->tableBucketNumber.has_value()) {
        int32_t bucket = hiveSplit_->tableBucketNumber.value();
        auto constantVec = std::make_shared<ConstantVector<int32_t>>(
            connectorQueryCtx_->memoryPool(),
            1,
            false,
            INTEGER(),
            std::move(bucket));
        childSpec->setConstantValue(constantVec);
      }
    } else if (
        metadataColumn != metadataColumns_.end() &&
        !(sequenceNumberColumnName == childSpec->fieldName() &&
          fileType->containsChild(sequenceNumberColumnName))) {
      // if column is sequence number, check that it's in the file
      // if it is in the file, we can skip this section and read it directly.
      // otherwise, set it as a constant.
      const auto& [idx, column] = *metadataColumn;
      auto baseVec = BaseVector::create(
          column->type(), 1, connectorQueryCtx_->memoryPool());
      column->populateVector(baseVec);
      childSpec->setConstantValue(baseVec);
    } else if (auto iter = hiveSplit_->infoColumns.find(fieldName);
               iter != hiveSplit_->infoColumns.end()) {
      auto infoColumnType =
          readerOutputType_->childAt(readerOutputType_->getChildIdx(fieldName));
      auto constant = BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          newConstantFromString,
          infoColumnType->kind(),
          infoColumnType,
          iter->second,
          1,
          connectorQueryCtx_->memoryPool());
      childSpec->setConstantValue(constant);
    } else {
      auto fileTypeIdx = fileType->getChildIdxIfExists(fieldName);
      if (!fileTypeIdx.has_value()) {
        // If field name exists in the user-specified output type,
        // set the column as null constant.
        // Related PR: https://github.com/facebookincubator/velox/pull/6427.
        auto outputTypeIdx = readerOutputType_->getChildIdxIfExists(fieldName);
        if (outputTypeIdx.has_value()) {
          setNullConstantValue(
              childSpec, readerOutputType_->childAt(outputTypeIdx.value()));
        } else {
          // Column is missing. Most likely due to schema evolution.
          BOLT_CHECK(tableSchema);
          setNullConstantValue(childSpec, tableSchema->findChild(fieldName));
        }
      } else {
        // Column no longer missing, reset constant value set on the spec.
        childSpec->setConstantValue(nullptr);
        auto outputTypeIdx = readerOutputType_->getChildIdxIfExists(fieldName);
        if (outputTypeIdx.has_value()) {
          // We know the fieldName exists in the file, make the type at that
          // position match what we expect in the output.
          columnTypes[fileTypeIdx.value()] =
              readerOutputType_->childAt(*outputTypeIdx);
        }
      }
    }
  }

  scanSpec_->resetCachedValues(false);

  return columnTypes;
}

void SplitReader::checkAndCreatePaimonDeletionFileReader(
    filesystems::FileOptions& options,
    bool judgeCache) {
  if (hiveSplit_->customSplitInfo.count(KPaimonDeletionFilePath) == 0 ||
      hiveSplit_->customSplitInfo[KPaimonDeletionFilePath].empty()) {
    return;
  }
  BOLT_CHECK_GT(hiveSplit_->customSplitInfo.count(KPaimonDeletionBinOffset), 0);
  BOLT_CHECK_GT(hiveSplit_->customSplitInfo.count(KPaimonDeletionBinSize), 0);

  auto pamonDeletionFilePath =
      hiveSplit_->customSplitInfo[KPaimonDeletionFilePath];
  int64_t binOffset{0}, binSize{0};
  try {
    binOffset =
        std::stol(hiveSplit_->customSplitInfo[KPaimonDeletionBinOffset]);
    binSize = std::stol(hiveSplit_->customSplitInfo[KPaimonDeletionBinSize]);
  } catch (const std::exception& e) {
    LOG(ERROR) << "error: " << e.what()
               << ", split: " << folly::toJson(hiveSplit_->serialize());
    BOLT_FAIL(
        "convert to integer error: {}, binOffset: {}, binSize: {}",
        e.what(),
        hiveSplit_->customSplitInfo[KPaimonDeletionBinOffset],
        hiveSplit_->customSplitInfo[KPaimonDeletionBinSize]);
  }

  auto deletionFileHandle =
      fileHandleFactory_->generate(pamonDeletionFilePath, options).second;
  std::vector<int> columnCacheBlackList;
  auto deleteFileInput = createBufferedInput(
      *deletionFileHandle,
      baseReaderOpts_,
      connectorQueryCtx_,
      ioStats_,
      executor_,
      judgeCache,
      columnCacheBlackList,
      nullptr);
  bolt::paimon::DeletionFileReader::Options deletionFileReaderOptions{
      .offset = binOffset, .size = binSize, .memoryPool = pool_};
  paimonDeletionFileReader_ =
      std::make_unique<bolt::paimon::DeletionFileReader>(
          std::move(deleteFileInput), deletionFileReaderOptions);
}

uint64_t SplitReader::nextWithPaimonDeletionVector(
    int64_t size,
    VectorPtr& output) {
  auto nextRowNumber = baseRowReader_->nextRowNumber();
  if (nextRowNumber == dwio::common::RowReader::kAtEnd) {
    return 0;
  }
  auto readSize = baseRowReader_->nextReadSize(size);
  paimonDeletionFileReader_->getDeletionVector(
      nextRowNumber, readSize, &paimonDeletionVector_);
  dwio::common::Mutation mutation;
  mutation.deletedRows = paimonDeletionVector_->as<uint64_t>();
  return baseRowReader_->next(size, output, &mutation);
}

uint64_t SplitReader::next(int64_t size, VectorPtr& output) {
  uint64_t rows;
  if (paimonDeletionFileReader_) {
    rows = nextWithPaimonDeletionVector(size, output);
  } else {
    rows = baseRowReader_->next(size, output);
  }
  populatePaimonMetadataColumns(output);

  // only for testing
  if (UNLIKELY(!FLAGS_testing_only_set_scan_exception_mesg_for_next.empty())) {
    throw std::runtime_error(
        FLAGS_testing_only_set_scan_exception_mesg_for_next);
  }

  return rows;
}

// Adds constant metadata columns into the result.
// Paimon splits are made of multiple hive splits, but they all share the same
// scanSpec reference. The scanSpec constants are overwritten when there are
// multiple hive splits in a paimon split. Calling this function will correctly
// populate the output with the metadata columns for this particular split.
void SplitReader::populatePaimonMetadataColumns(VectorPtr& output) {
  if (isPartOfPaimonSplit_) {
    for (const auto& [idx, metadataColumn] : metadataColumns_) {
      metadataColumn->populateVector(output->as<RowVector>()->childAt(idx));
    }
  }
}

void SplitReader::resetFilterCaches() {
  if (baseRowReader_) {
    baseRowReader_->resetFilterCaches();
  }
}

bool SplitReader::emptySplit() const {
  return emptySplit_;
}

void SplitReader::resetSplit() {
  hiveSplit_.reset();
  baseReader_.reset(); // release memory immediately
}

int64_t SplitReader::estimatedRowSize() const {
  if (!baseRowReader_) {
    return DataSource::kUnknownRowSize;
  }

  auto size = baseRowReader_->estimatedRowSize();
  if (size.has_value()) {
    return size.value();
  }
  return DataSource::kUnknownRowSize;
}

void SplitReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& stats) const {
  if (baseRowReader_) {
    baseRowReader_->updateRuntimeStats(stats);
  }
}

bool SplitReader::allPrefetchIssued() const {
  return baseRowReader_ && baseRowReader_->allPrefetchIssued();
}

void SplitReader::setConstantValue(
    common::ScanSpec* spec,
    const TypePtr& type,
    const bolt::variant& value) const {
  spec->setConstantValue(BaseVector::createConstant(
      type, value, 1, connectorQueryCtx_->memoryPool()));
}

void SplitReader::setNullConstantValue(
    common::ScanSpec* spec,
    const TypePtr& type) const {
  spec->setConstantValue(BaseVector::createNullConstant(
      type, 1, connectorQueryCtx_->memoryPool()));
}

void SplitReader::setPartitionValue(
    common::ScanSpec* spec,
    const std::string& partitionKey,
    const std::optional<std::string>& value,
    const bool isNull) const {
  auto it = partitionKeys_->find(partitionKey);
  BOLT_CHECK(
      it != partitionKeys_->end(),
      "ColumnHandle is missing for partition key {}",
      partitionKey);
  bolt::variant constValue;
  if (isNull) {
    setNullConstantValue(spec, it->second->dataType());
  } else {
    try {
      if (it->second->dataType()->isDate()) {
        // TODO: need to align with query config for isIso8601.
        if (value.has_value()) {
          constValue = bolt::variant(
              bolt::util::castFromDateString(StringView(value.value()), false)
                  .value());
        } else {
          constValue = bolt::variant(TypeKind::INTEGER);
        }
      } else {
        constValue = BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
            convertFromString, it->second->dataType()->kind(), value);
      }
      setConstantValue(spec, it->second->dataType(), constValue);
    } catch (const std::exception& e) {
      LOG(ERROR)
          << fmt::format(
                 "Failed to convert partition column value: {} to type {}. Partition column {} is set to NULL. ",
                 value.has_value() ? value.value() : "",
                 mapTypeKindToName(it->second->dataType()->kind()),
                 partitionKey)
          << e.what();
      setNullConstantValue(spec, it->second->dataType());
    }
  }
}

std::string SplitReader::toString() const {
  std::vector<std::string> keys;
  keys.reserve(partitionKeys_->size());
  for (const auto& [_, column] : *partitionKeys_) {
    keys.emplace_back(column->toString());
  }

  return fmt::format(
      "SplitReader: hiveSplit_{} scanSpec_{} readerOutputType_{} partitionKeys_ {} reader{} rowReader{}",
      hiveSplit_->toString(),
      scanSpec_->toString(),
      readerOutputType_->toString(),
      fmt::join(keys, " "),
      static_cast<const void*>(baseReader_.get()),
      static_cast<const void*>(baseRowReader_.get()));
}

} // namespace bytedance::bolt::connector::hive

template <>
struct fmt::formatter<bytedance::bolt::dwio::common::FileFormat>
    : formatter<std::string> {
  auto format(
      bytedance::bolt::dwio::common::FileFormat fmt,
      format_context& ctx) {
    return formatter<std::string>::format(
        bytedance::bolt::dwio::common::toString(fmt), ctx);
  }
};
