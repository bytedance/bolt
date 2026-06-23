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

#include "bolt/common/io/IoStatistics.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/hive/FileHandle.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/connectors/hive/PaimonConnectorSplit.h"
#include "bolt/connectors/hive/SplitReader.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/Reader.h"
#include "bolt/dwio/common/ScanSpec.h"
#include "bolt/dwio/common/Statistics.h"
#include "bolt/exec/OperatorUtils.h"
#include "bolt/expression/Expr.h"
namespace bytedance::bolt::connector::hive {

class HiveConfig;

using SubfieldFilters =
    std::unordered_map<common::Subfield, std::unique_ptr<common::Filter>>;

class HiveDataSource : public DataSource {
 public:
  HiveDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      FileHandleFactory* fileHandleFactory,
      const core::QueryConfig& queryConfig,
      folly::Executor* executor,
      const std::shared_ptr<ConnectorQueryCtx>& connectorQueryCtx,
      const std::shared_ptr<HiveConfig>& hiveConfig);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  std::optional<RowVectorPtr> next(uint64_t size, bolt::ContinueFuture& future)
      override;

  void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override;

  uint64_t getCompletedBytes() override {
    return ioStats_->rawBytesRead();
  }

  std::vector<uint64_t> getCompletedBytesReads() override {
    return ioStats_->rawBytesReads();
  }

  std::vector<uint64_t> getCompletedCntReads() override {
    return ioStats_->cntReads();
  }

  std::vector<uint64_t> getCompletedScanTimeReads() override {
    return ioStats_->scanTimeReads();
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override;

  bool allPrefetchIssued() const override {
    return splitReader_ && splitReader_->allPrefetchIssued();
  }

  void setFromDataSource(std::unique_ptr<DataSource> sourceUnique) override;

  int64_t estimatedRowSize() override;

  std::unique_ptr<SplitReader> createConfiguredSplitReader(
      const std::shared_ptr<HiveConnectorSplit>& split,
      const bool isPartOfPaimonSplit);

  void close() override {
    if (splitReader_) {
      splitReader_.reset();
    }
  }

 protected:
  // Creates a split reader, which reads the split given as param
  virtual std::unique_ptr<SplitReader> createSplitReader(
      const std::shared_ptr<HiveConnectorSplit>& split,
      bool isPartOfPaimonSplit);

  std::shared_ptr<ConnectorSplit> split_;
  std::shared_ptr<HiveTableHandle> hiveTableHandle_;
  memory::MemoryPool* pool_;
  std::shared_ptr<common::ScanSpec> scanSpec_;
  VectorPtr output_;
  std::unique_ptr<HiveSplitReaderBase> splitReader_;

  // Output type from file reader.  This is different from outputType_ that it
  // contains column names before assignment, and columns that only used in
  // remaining filter.
  RowTypePtr readerOutputType_;

  // Column handles for the partition key columns keyed on partition key
  // column name.
  std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>
      partitionKeys_;

  FileHandleFactory* const fileHandleFactory_;
  folly::Executor* const executor_;
  std::shared_ptr<ConnectorQueryCtx> connectorQueryCtx_ = nullptr;
  const std::shared_ptr<HiveConfig> hiveConfig_;
  std::shared_ptr<io::IoStatistics> ioStats_;

 private:
  // Evaluates remainingFilter_ on the specified vector. Returns number of
  // rows passed. Populates filterEvalCtx_.selectedIndices and selectedBits
  // if only some rows passed the filter. If none or all rows passed
  // filterEvalCtx_.selectedIndices and selectedBits are not updated.
  vector_size_t evaluateRemainingFilter(RowVectorPtr& rowVector);

  bolt::RowTypePtr getRowTypeForFile(
      std::shared_ptr<PaimonConnectorSplit> split);

  // Clear split_ after split has been fully processed.  Keep readers around
  // to hold adaptation.
  void resetSplit();

  const RowVectorPtr& getEmptyOutput() {
    if (!emptyOutput_) {
      emptyOutput_ = RowVector::createEmpty(outputType_, pool_);
    }
    return emptyOutput_;
  }

  void addSplit(const std::shared_ptr<PaimonConnectorSplit>& split);

  void addSplit(const std::shared_ptr<HiveConnectorSplit>& split);

  std::vector<std::string> getPaimonPrimaryKeys(
      const std::unordered_map<std::string, std::string>& tableParameters);
  std::vector<std::string> getPaimonSequenceFields(
      const std::unordered_map<std::string, std::string>& tableParameters);
  std::string getPaimonRowKind(
      const std::unordered_map<std::string, std::string>& tableParameters);

  std::vector<int> addColumnsIfNotExists(
      std::vector<std::string>& names,
      std::vector<TypePtr>& types,
      const std::vector<std::string>& fileColNames,
      const std::vector<TypePtr>& fileColTypes,
      const std::vector<std::string>& otherNames);

  void recalculateRepDefConf(
      const RowTypePtr& rowType,
      const core::QueryConfig& queryConfig);
#ifdef BOLT_ENABLE_HDFS
  bool isLastRetry();
#endif

  // The row type for the data source output, not including filter-only
  // columns
  const RowTypePtr outputType_;
  core::ExpressionEvaluator* const expressionEvaluator_;

  // Column handles for the Split info columns keyed on their column names.
  std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>
      infoColumns_;

  std::shared_ptr<common::MetadataFilter> metadataFilter_;
  std::unique_ptr<exec::ExprSet> remainingFilterExprSet_;
  RowVectorPtr emptyOutput_;
  bool emptySplit_;
  bool native_cache_enabled;

  bool enable_parquet_rownum_and_filename = false;

  std::atomic<uint64_t> totalRemainingFilterTime_{0};
  uint64_t completedRows_ = 0;

  // Reusable memory for remaining filter evaluation.
  VectorPtr filterResult_;
  SelectivityVector filterRows_;
  exec::FilterEvalCtx filterEvalCtx_;

  RowVectorPtr emptyResult_;
  // session config for cfs
  filesystems::FileOptions fsSessionConfig_;

  std::string currentSplitStr_;

  std::unique_ptr<dwio::common::RuntimeStatistics> runtimeStats_;

  int64_t ignoredFileSizes_{0};

  int32_t parquetRepDefMemoryLimit_{16UL << 20};
  int32_t decodeRepDefPageCount_{10};
  int64_t parquetMaxBatchBytes_{0};
};

} // namespace bytedance::bolt::connector::hive
