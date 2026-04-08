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

#include "bolt/connectors/hive/iceberg/IcebergSplitReader.h"

#include <folly/lang/Bits.h>

#include "bolt/common/encode/Base64.h"
#include "bolt/connectors/hive/HiveConnectorUtil.h"
#include "bolt/connectors/hive/iceberg/IcebergDeleteFile.h"
#include "bolt/connectors/hive/iceberg/IcebergMetadataColumns.h"
#include "bolt/connectors/hive/iceberg/IcebergSplit.h"
#include "bolt/dwio/common/BufferUtil.h"

using namespace bytedance::bolt::dwio::common;

namespace bytedance::bolt::connector::hive::iceberg {

namespace {

// Bolt uses the generic Hive string conversion helpers for metadata info
// columns, while Bolt routes these through newConstantFromString().
template <TypeKind kind>
bolt::variant infoColumnValue(const std::string& value) {
  return hive::convertFromString<kind>(std::make_optional(value));
}

} // namespace

IcebergSplitReader::IcebergSplitReader(
    const std::shared_ptr<const hive::HiveConnectorSplit>& hiveSplit,
    const std::shared_ptr<HiveTableHandle>& hiveTableHandle,
    std::unordered_map<int32_t, std::shared_ptr<HiveColumnHandle>>
        topLevelFieldIdToHandle,
    std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
        partitionKeys,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<HiveConfig>& hiveConfig,
    const RowTypePtr& readerOutputType,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    FileHandleFactory* const fileHandleFactory,
    folly::Executor* executor,
    const std::shared_ptr<common::ScanSpec>& scanSpec)
    : SplitReader(
          std::const_pointer_cast<HiveConnectorSplit>(hiveSplit),
          hiveTableHandle,
          scanSpec,
          readerOutputType,
          partitionKeys,
          fileHandleFactory,
          executor,
          connectorQueryCtx,
          hiveConfig,
          ioStats,
          false),
      baseReadOffset_(0),
      splitOffset_(0),
      deleteBitmap_(nullptr) {}

void IcebergSplitReader::configureReaderOptions() {
  // Keep the hook explicit even though the current Iceberg path just inherits
  // the base configuration logic from SplitReader.
  SplitReader::configureReaderOptions();
}

void IcebergSplitReader::prepareSplit(
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    dwio::common::RuntimeStatistics& runtimeStats,
    filesystems::FileOptions& options,
    bool judgeCache,
    std::vector<int>& columnCacheBlackList,
    const HiveConnectorSplitCacheLimit* hiveConnectorSplitCacheLimit) {
  SplitReader::prepareSplit(
      std::move(metadataFilter),
      runtimeStats,
      options,
      judgeCache,
      columnCacheBlackList,
      hiveConnectorSplitCacheLimit);
  if (emptySplit() || !baseRowReader_) {
    return;
  }
  auto icebergSplit =
      std::dynamic_pointer_cast<const HiveIcebergSplit>(hiveSplit_);
  BOLT_CHECK_NOT_NULL(icebergSplit, "Expected HiveIcebergSplit");
  baseReadOffset_ = 0;
  splitOffset_ = baseRowReader_->nextRowNumber();
  positionalDeleteFileReaders_.clear();

  const auto& deleteFiles = icebergSplit->deleteFiles;
  for (const auto& deleteFile : deleteFiles) {
    if (deleteFile.content == FileContent::kPositionalDeletes) {
      if (deleteFile.recordCount > 0) {
        // Skip the delete file if all delete positions are before this split.
        // TODO: Skip delete files where all positions are after the split, if
        // split row count becomes available.
        if (auto iter =
                deleteFile.upperBounds.find(IcebergMetadataColumn::kPosId);
            iter != deleteFile.upperBounds.end()) {
          auto decodedBound = encoding::Base64::decode(iter->second);
          BOLT_CHECK_EQ(
              decodedBound.size(),
              sizeof(uint64_t),
              "Unexpected decoded size for positional delete upper bound.");
          uint64_t posDeleteUpperBound;
          std::memcpy(
              &posDeleteUpperBound, decodedBound.data(), sizeof(uint64_t));
          posDeleteUpperBound = folly::Endian::little(posDeleteUpperBound);
          if (posDeleteUpperBound < splitOffset_) {
            continue;
          }
        }
        positionalDeleteFileReaders_.push_back(
            std::make_unique<PositionalDeleteFileReader>(
                deleteFile,
                hiveSplit_->filePath,
                fileHandleFactory_,
                connectorQueryCtx_,
                executor_,
                hiveConfig_,
                ioStats_,
                runtimeStats,
                splitOffset_,
                hiveSplit_->connectorId));
      }
    } else {
      BOLT_NYI();
    }
  }
}

uint64_t IcebergSplitReader::next(int64_t size, VectorPtr& output) {
  dwio::common::Mutation mutation;
  mutation.deletedRows = nullptr;

  const auto actualSize = baseRowReader_->nextReadSize(size);
  baseReadOffset_ = baseRowReader_->nextRowNumber() - splitOffset_;
  if (actualSize == dwio::common::RowReader::kAtEnd) {
    return 0;
  }

  if (!positionalDeleteFileReaders_.empty()) {
    auto numBytes = bits::nbytes(actualSize);
    dwio::common::ensureCapacity<int8_t>(
        deleteBitmap_, numBytes, connectorQueryCtx_->memoryPool());
    std::memset(deleteBitmap_->asMutable<int8_t>(), 0, numBytes);

    for (auto iter = positionalDeleteFileReaders_.begin();
         iter != positionalDeleteFileReaders_.end();) {
      (*iter)->readDeletePositions(baseReadOffset_, actualSize, deleteBitmap_);

      if ((*iter)->noMoreData()) {
        iter = positionalDeleteFileReaders_.erase(iter);
      } else {
        ++iter;
      }
    }
  }

  mutation.deletedRows = deleteBitmap_ && deleteBitmap_->size() > 0
      ? deleteBitmap_->as<uint64_t>()
      : nullptr;

  auto rowsScanned = baseRowReader_->next(actualSize, output, &mutation);

  return rowsScanned;
}

std::vector<TypePtr> IcebergSplitReader::adaptColumns(
    const RowTypePtr& fileType,
    const std::shared_ptr<const bolt::RowType>& tableSchema) {
  std::vector<TypePtr> columnTypes = fileType->children();
  auto& childrenSpecs = scanSpec_->children();
  // Iceberg table stores all column's data in data file.
  for (const auto& childSpec : childrenSpecs) {
    const std::string& fieldName = childSpec->fieldName();
    if (auto iter = hiveSplit_->infoColumns.find(fieldName);
        iter != hiveSplit_->infoColumns.end()) {
      auto infoColumnType = readerOutputType_->findChild(fieldName);
      auto constantValue = BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
          infoColumnValue, infoColumnType->kind(), iter->second);
      setConstantValue(childSpec.get(), infoColumnType, constantValue);
    } else {
      auto fileTypeIdx = fileType->getChildIdxIfExists(fieldName);
      auto outputTypeIdx = readerOutputType_->getChildIdxIfExists(fieldName);
      if (outputTypeIdx.has_value() && fileTypeIdx.has_value()) {
        childSpec->setConstantValue(nullptr);
        auto& outputType = readerOutputType_->childAt(*outputTypeIdx);
        columnTypes[*fileTypeIdx] = outputType;
      } else if (!fileTypeIdx.has_value()) {
        // Handle columns missing from the data file in two scenarios:
        // 1. Schema evolution: Column was added after the data file was
        // written and doesn't exist in older data files.
        // 2. Partition columns: Hive migrated table. In Hive-written data
        // files, partition column values are stored in partition metadata
        // rather than in the data file itself, following Hive's partitioning
        // convention.
        if (auto it = hiveSplit_->partitionKeys.find(fieldName);
            it != hiveSplit_->partitionKeys.end()) {
          setPartitionValue(
              childSpec.get(),
              fieldName,
              it->second,
              it->second.has_value() ? hive::isHiveNull(it->second.value())
                                     : false);
        } else {
          setNullConstantValue(
              childSpec.get(), tableSchema->findChild(fieldName));
        }
      }
    }
  }

  scanSpec_->resetCachedValues(false);

  return columnTypes;
}

} // namespace bytedance::bolt::connector::hive::iceberg
