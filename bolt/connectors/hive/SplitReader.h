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

#include "bolt/connectors/hive/FileHandle.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/connectors/hive/HiveSplitReaderBase.h"
#include "bolt/connectors/hive/PaimonMetadataColumn.h"
#include "bolt/dwio/common/Options.h"

DECLARE_string(testing_only_set_scan_exception_mesg_for_prepare);
DECLARE_string(testing_only_set_scan_exception_mesg_for_next);

namespace bytedance::bolt {
class BaseVector;
class variant;
using VectorPtr = std::shared_ptr<BaseVector>;
namespace paimon {
class DeletionFileReader;
} // namespace paimon
} // namespace bytedance::bolt
namespace bytedance::bolt::common {
class MetadataFilter;
class ScanSpec;
} // namespace bytedance::bolt::common
namespace bytedance::bolt::connector {
class ConnectorQueryCtx;
} // namespace bytedance::bolt::connector
namespace bytedance::bolt::dwio::common {
class Reader;
class RowReader;
struct RuntimeStatistics;
} // namespace bytedance::bolt::dwio::common
namespace bytedance::bolt::memory {
class MemoryPool;
}
namespace bytedance::bolt::connector::hive {

struct HiveConnectorSplit;
class HiveTableHandle;
class HiveColumnHandle;
class HiveConfig;

class SplitReader : public HiveSplitReaderBase {
 public:
  static std::unique_ptr<SplitReader> create(
      const std::shared_ptr<bolt::connector::hive::HiveConnectorSplit>&
          hiveSplit,
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
      const bool isPartOfPaimonSplit);

  SplitReader(
      const std::shared_ptr<bolt::connector::hive::HiveConnectorSplit>&
          hiveSplit,
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
      const bool isPartOfPaimonSplit);

  virtual ~SplitReader();

  void configureReaderOptions();

  dwio::common::ReaderOptions& readerOptions() {
    return baseReaderOpts_;
  }

  dwio::common::RowReaderOptions& rowReaderOptions() {
    return baseRowReaderOpts_;
  }

  /// This function is used by different table formats like Iceberg and Hudi to
  /// do additional preparations before reading the split, e.g. Open delete
  /// files or log files, and add column adaptations for metadata columns
  virtual void prepareSplit(
      std::shared_ptr<common::MetadataFilter> metadataFilter,
      dwio::common::RuntimeStatistics& runtimeStats,
      filesystems::FileOptions& options,
      bool judgeCache,
      std::vector<int>& columnCacheBlackList,
      const HiveConnectorSplitCacheLimit* hiveConnectorSplitCacheLimit);

  virtual uint64_t next(int64_t size, VectorPtr& output);

  void resetFilterCaches();

  bool emptySplit() const;

  void resetSplit();

  int64_t estimatedRowSize() const;

  void updateRuntimeStats(dwio::common::RuntimeStatistics& stats) const;

  bool allPrefetchIssued() const;

  std::string toString() const;

  memory::MemoryPool* pool() {
    return pool_;
  }

 protected:
  // Different table formats may have different metadata columns. This function
  // will be used to update the scanSpec for these columns.
  virtual std::vector<TypePtr> adaptColumns(
      const RowTypePtr& fileType,
      const std::shared_ptr<const bolt::RowType>& tableSchema);

  void setConstantValue(
      common::ScanSpec* FOLLY_NONNULL spec,
      const TypePtr& type,
      const bolt::variant& value) const;

  void setNullConstantValue(
      common::ScanSpec* FOLLY_NONNULL spec,
      const TypePtr& type) const;

  void setPartitionValue(
      common::ScanSpec* FOLLY_NONNULL spec,
      const std::string& partitionKey,
      const std::optional<std::string>& value,
      const bool isNull) const;

  void checkAndCreatePaimonDeletionFileReader(
      filesystems::FileOptions& options,
      bool judgeCache);

  uint64_t nextWithPaimonDeletionVector(int64_t size, VectorPtr& output);
  void populatePaimonMetadataColumns(VectorPtr& output);
  void computeMetadataColumns();

  std::shared_ptr<HiveConnectorSplit> hiveSplit_;
  std::shared_ptr<HiveTableHandle> hiveTableHandle_;
  std::shared_ptr<common::ScanSpec> scanSpec_;
  RowTypePtr readerOutputType_;
  std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
      partitionKeys_;
  memory::MemoryPool* const pool_;
  std::unique_ptr<dwio::common::Reader> baseReader_;
  std::unique_ptr<dwio::common::RowReader> baseRowReader_;
  FileHandleFactory* const fileHandleFactory_;
  folly::Executor* const executor_;
  const ConnectorQueryCtx* const connectorQueryCtx_;
  const std::shared_ptr<HiveConfig> hiveConfig_;
  std::shared_ptr<io::IoStatistics> ioStats_;
  dwio::common::ReaderOptions baseReaderOpts_;
  dwio::common::RowReaderOptions baseRowReaderOpts_;

  std::unique_ptr<bytedance::bolt::paimon::DeletionFileReader>
      paimonDeletionFileReader_;
  BufferPtr paimonDeletionVector_;

  // Map of output index to metadata column type
  std::unordered_map<size_t, std::shared_ptr<paimon::MetadataColumn>>
      metadataColumns_{};

 private:
  bool emptySplit_;
  bool isPartOfPaimonSplit_;
};

} // namespace bytedance::bolt::connector::hive
