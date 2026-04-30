/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/connectors/paimon/PaimonDataSource.h"
#include <folly/detail/ThreadLocalDetail.h>
#include <folly/json.h>
#include <paimon/defs.h>
#include <paimon/read_context.h>
#include <paimon/table/source/data_split.h>
#include <paimon/table/source/split.h>
#include <paimon/table/source/table_read.h>
#include <paimon/type_fwd.h>
#include <algorithm>
#include "bolt/common/base/Exceptions.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/connectors/paimon/BoltMemoryPool.h"
#include "bolt/connectors/paimon/PaimonConfig.h"
#include "bolt/connectors/paimon/PaimonFilterTranslator.h"
#include "bolt/type/StringView.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/SelectivityVector.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"

namespace bytedance::bolt::connector::paimon {

PaimonDataSource::PaimonDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<ConnectorTableHandle>& tableHandle,
    const std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>&
        columnHandles,
    const std::shared_ptr<ConnectorQueryCtx>& queryCtx,
    const core::QueryConfig& queryConfig,
    const std::shared_ptr<PaimonConfig>& paimonConfig)
    : outputType_(outputType),
      tableHandle_(std::dynamic_pointer_cast<PaimonTableHandle>(tableHandle)),
      pool_(queryCtx->memoryPool()) {
  // Wrap the query-context pool for Paimon's native memory management.
  paimonPool_ = std::make_shared<BoltPaimonMemoryPool>(pool_);

  ::paimon::ReadContextBuilder ctxBuilder(tableHandle_->tablePath());
  std::vector<std::string> columns;
  columns.reserve(outputType_->size());
  for (const auto& outName : outputType_->names()) {
    auto it = columnHandles.find(outName);
    BOLT_CHECK(
        it != columnHandles.end(),
        "Could not find column handle with name: {}",
        outName);
    columns.push_back(it->second->name());
  }
  VLOG(1) << "PaimonDataSource::PaimonDataSource(): Read schema: "
          << folly::join(", ", columns);
  ctxBuilder.SetReadSchema(columns);
  ctxBuilder.EnableMultiThreadRowToBatch(paimonConfig->multiThreadRowToBatch());
  if (paimonConfig->multiThreadRowToBatch()) {
    ctxBuilder.SetRowToBatchThreadNumber(
        paimonConfig->rowToBatchThreadNumber());
  }
  ctxBuilder.WithMemoryPool(paimonPool_);
  ctxBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "local");

#ifdef BOLT_ENABLE_HDFS
  // Enable hdfs:// scheme resolution to Bolt-backed paimon filesystem.
  ctxBuilder.WithFileSystemSchemeToIdentifierMap(
      std::map<std::string, std::string>{{"hdfs", "bolt_hdfs"}});
#endif
  for (const auto& [key, value] : tableHandle_->tableProperties()) {
    ctxBuilder.AddOption(key, value);
    VLOG(1) << "Added table option <" << key << "=" << value << ">";
  }

  // Pass read.batch-size through the options map so the paimon library uses
  // it when creating FileBatchReaders.
  ctxBuilder.AddOption(
      ::paimon::Options::READ_BATCH_SIZE,
      std::to_string(paimonConfig->readBatchSize()));

  // Propagate I/O tuning options through paimon's options map so they reach
  // PaimonReadFile (constructed deep inside paimon's internal reader pipeline).
  // Query config overrides connector config; connector config provides
  // defaults.
  ctxBuilder.AddOption(
      PaimonConfig::kNaturalReadSize,
      std::to_string(queryConfig.get<uint64_t>(
          PaimonConfig::kNaturalReadSize, paimonConfig->naturalReadSize())));
  ctxBuilder.AddOption(
      PaimonConfig::kCoalesceReads,
      queryConfig.get<bool>(
          PaimonConfig::kCoalesceReads, paimonConfig->coalesceReads())
          ? "true"
          : "false");

  // Propagate timestamp read precision so PaimonParquetReader can set it on
  // bolt's ParquetReader (ReaderOptions::setTimestampPrecision). This ensures
  // the paimon connector truncates timestamps to the same precision as the hive
  // connector for a given session property value.
  ctxBuilder.AddOption(
      PaimonConfig::kReadTimestampUnit,
      std::to_string(queryConfig.get<uint8_t>(
          PaimonConfig::kReadTimestampUnit,
          paimonConfig->readTimestampUnit())));

  ctxBuilder.EnablePredicateFilter(paimonConfig->predicateFilterEnabled());

  // Prefetch tuning — disabled by default; when enabled, overlaps I/O with
  // computation for high-latency storage backends (S3, HDFS, OSS).
  ctxBuilder.EnablePrefetch(paimonConfig->prefetchEnabled());
  if (paimonConfig->prefetchEnabled()) {
    ctxBuilder.SetPrefetchBatchCount(paimonConfig->prefetchBatchCount());
    ctxBuilder.SetPrefetchMaxParallelNum(paimonConfig->prefetchMaxParallel());
  }

  // Translate the filter expression from PaimonTableHandle into a
  // paimon-native Predicate for pushdown into the parquet reader.
  //
  // The filter expression references columns by their *real* paimon schema
  // names (e.g., "id", "name") as set by the SparkSQL/gluten planner, but
  // outputType_ carries internal alias names (e.g., "n0_0", "n0_1").
  // We must build a RowType whose names match the filter's field references
  // so that extractFieldInfo can resolve field indices correctly.
  if (tableHandle_->filter()) {
    std::vector<std::string> realNames;
    std::vector<TypePtr> realTypes;
    realNames.reserve(outputType_->size());
    realTypes.reserve(outputType_->size());
    for (size_t i = 0; i < outputType_->size(); ++i) {
      const auto& outName = outputType_->nameOf(i);
      auto it = columnHandles.find(outName);
      BOLT_CHECK(
          it != columnHandles.end(),
          "Could not find column handle with name: {}",
          outName);
      realNames.push_back(it->second->name()); // real paimon name
      realTypes.push_back(outputType_->childAt(i)); // preserve type
    }
    auto filterRowType = ROW(std::move(realNames), std::move(realTypes));

    auto result = PaimonFilterTranslator::translate(
        tableHandle_->filter(), filterRowType);
    if (result.ok()) {
      VLOG(1) << "PaimonDataSource: Translated filter to paimon Predicate: "
              << result.value->ToString();
      ctxBuilder.SetPredicate(result.value);
    } else {
      LOG(WARNING)
          << "PaimonDataSource: Could not fully translate filter expression "
          << tableHandle_->filter()->toString()
          << " to paimon Predicate: " << result.reason
          << "; filter pushdown will not be applied";
      ctxBuilder.SetPredicate(nullptr);
    }
  } else {
    ctxBuilder.SetPredicate(nullptr);
  }
  auto ctxBuildResult = ctxBuilder.Finish();
  BOLT_CHECK(
      ctxBuildResult.ok(),
      "ReadContextBuilder.Finish() failed: {}",
      ctxBuildResult.status().ToString());
  auto readCtx = std::move(ctxBuildResult).value();
  auto tableReadStatus = ::paimon::TableRead::Create(std::move(readCtx));
  BOLT_CHECK(
      tableReadStatus.ok(),
      "TableRead::Create() failed: {}",
      tableReadStatus.status().ToString());
  tableRead_ = std::move(tableReadStatus).value();
}

PaimonDataSource::~PaimonDataSource() = default;

void PaimonDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  auto paimonConnectorSplit =
      std::dynamic_pointer_cast<PaimonConnectorSplit>(split);
  BOLT_CHECK_NOT_NULL(
      paimonConnectorSplit, "Split was not paimon connector split");

  // Deserialize the split bytes into a ::paimon::Split
  auto paimonSplit = ::paimon::Split::Deserialize(
      paimonConnectorSplit->splitBytes_.data(),
      paimonConnectorSplit->splitBytes_.length(),
      paimonPool_);
  BOLT_CHECK(
      paimonSplit.ok(),
      "Failed to deserialize Paimon split: {}",
      paimonSplit.status().ToString());
  inputSplits_.push_back(std::move(paimonSplit).value());
}

std::optional<RowVectorPtr> PaimonDataSource::next(
    uint64_t /*size*/,
    ContinueFuture& /* future */) {
  // If we've already encountered EOF (inputSplits_ are cleared and no reader),
  // don't try to do anything else
  if (inputSplits_.empty() && !reader_) {
    return nullptr;
  }

  // Lazily create the BatchReader using accumulated splits.
  if (!reader_ && !inputSplits_.empty()) {
    VLOG(1) << "PaimonDataSource::next(): Creating reader with "
            << inputSplits_.size() << " split(s)";
    auto&& readerCreateStatus = tableRead_->CreateReader(inputSplits_);
    if (!readerCreateStatus.ok()) {
      VLOG(1) << "PaimonDataSource::next(): CreateReader error: "
              << readerCreateStatus.status().ToString();
      BOLT_FAIL(
          "create reader error: {}", readerCreateStatus.status().ToString());
    }
    reader_ = std::move(readerCreateStatus).value();
  }

  if (!reader_) {
    VLOG(1)
        << "PaimonDataSource::next(): No reader available, returning nullopt";
    return nullptr;
  }

  VLOG(1) << "PaimonDataSource::next(): Calling reader->NextBatch() at "
          << reader_.get();
  auto batchRes = reader_->NextBatch();
  if (!batchRes.ok()) {
    VLOG(1) << "PaimonDataSource::next(): NextBatch NOT ok: "
            << batchRes.status().ToString();
    reader_->Close();
    reader_.reset();
    inputSplits_.clear();
    BOLT_FAIL("failed to get next batch: {}", batchRes.status().ToString());
  }

  VLOG(1) << "PaimonDataSource::next(): NextBatch successful, getting value";
  auto pair = std::move(batchRes).value();

  if (::paimon::BatchReader::IsEofBatch(pair)) {
    VLOG(1) << "PaimonDataSource::next(): IsEofBatch: true";
    if (pair.first && pair.first->release) {
      pair.first->release(pair.first.get());
    }
    if (pair.second && pair.second->release) {
      pair.second->release(pair.second.get());
    }
    reader_->Close();
    reader_.reset();
    inputSplits_.clear();
    return nullptr;
  }
  VLOG(1) << "PaimonDataSource::next(): Not an EOF batch, attempting import";
  ArrowArray& arr = *pair.first;
  ArrowSchema& sch = *pair.second;

  VLOG(1) << "PaimonDataSource::next(): Schema has " << sch.n_children
          << " children";
  if (sch.n_children > 0) {
    for (int i = 0; i < sch.n_children; ++i) {
      VLOG(1) << "PaimonDataSource::next(): child " << i
              << ": name=" << (sch.children[i] ? sch.children[i]->name : "null")
              << ", type="
              << (sch.children[i] ? sch.children[i]->format : "null");
    }
  }

  ArrowOptions opts;
  auto vec = bytedance::bolt::importFromArrowAsOwner(sch, arr, opts, pool_);

  const auto& row = std::dynamic_pointer_cast<RowVector>(vec);
  BOLT_CHECK(row != nullptr, "Imported vector is not a RowVector");
  const auto& rowType = row->type()->asRow();

  VLOG(1) << "Imported RowVector size: " << row->size()
          << ", number of fields: " << rowType.size();

  // If we have _VALUE_KIND as the first field, drop it - but only if the
  // original Parquet reader actually exported it. Check if the data actually
  // exists in the child vector before proceeding.
  auto firstChild = row->childAt(0);
  VLOG(1) << "First child vector type: " << firstChild->type()->toString()
          << ", elements: " << firstChild->size();
  if (rowType.nameOf(0) == "_VALUE_KIND" && rowType.size() > 1) {
    VLOG(1) << "Dropping _VALUE_KIND field";

    // Create a new row vector without the _VALUE_KIND field
    std::vector<VectorPtr> newChildren;
    for (int i = 1; i < rowType.size(); ++i) {
      newChildren.push_back(row->childAt(i));
    }

    std::vector<std::string> newNames;
    std::vector<TypePtr> newTypes;
    for (int i = 1; i < rowType.size(); ++i) {
      newNames.push_back(rowType.nameOf(i));
      newTypes.push_back(rowType.childAt(i));
    }

    const auto& newRowType = ROW(std::move(newNames), std::move(newTypes));
    auto newRowVec = std::make_shared<RowVector>(
        pool_, newRowType, nullptr, row->size(), newChildren);

    // Copy null information
    newRowVec->setNulls(row->nulls());

    VLOG(1) << "New RowVector size: " << newRowVec->size()
            << ", number of fields: " << newRowType->size();
    // VLOG(1) << newRowVec->toPrettyString();
    completedRows_ += newRowVec->size();

    // Re-wrap with outputType_ to ensure internal alias names match upstream
    // operators' expectations (same as the non-_VALUE_KIND path below).
    std::vector<VectorPtr> outputColumns;
    outputColumns.reserve(outputType_->size());
    for (size_t i = 0; i < outputType_->size(); ++i) {
      outputColumns.push_back(newRowVec->childAt(i));
    }
    return std::make_shared<RowVector>(
        pool_,
        outputType_,
        nullptr,
        newRowVec->size(),
        std::move(outputColumns));
  }

  if (VLOG_IS_ON(1)) {
    auto idColumn = row->childAt(0);
    auto* idFlat = idColumn->asFlatVector<int64_t>();
    for (int i = 0; i < row->size(); ++i) {
      if (idColumn->isNullAt(i)) {
        VLOG(1) << "Row " << i << ": id = NULL";
      } else {
        VLOG(1) << "Row " << i << ": id = " << idFlat->valueAt(i);
      }
    }
  }

  completedRows_ += row->size();
  VLOG(1) << "Returning row vector of size " << row->size();

  // Wrap the imported vector in a new RowVector that uses outputType_ as its
  // type. The raw Arrow import carries real column names (e.g. "id", "name")
  // from the Paimon schema, but upstream operators (FilterProject, ProjectNode)
  // expect internal alias names (e.g. "n0_0", "n0_1") from outputType_. Re-wrap
  // by index position to preserve the correct naming contract.
  std::vector<VectorPtr> outputColumns;
  outputColumns.reserve(outputType_->size());
  for (size_t i = 0; i < outputType_->size(); ++i) {
    outputColumns.push_back(row->childAt(i));
  }
  return std::make_shared<RowVector>(
      pool_, outputType_, nullptr, row->size(), std::move(outputColumns));
}

} // namespace bytedance::bolt::connector::paimon
