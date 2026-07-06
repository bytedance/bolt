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
#include <paimon/predicate/predicate_builder.h>
#include <paimon/read_context.h>
#include <paimon/table/source/data_split.h>
#include <paimon/table/source/split.h>
#include <paimon/table/source/table_read.h>
#include <paimon/type_fwd.h>
#include <algorithm>
#include <unordered_set>
#include <utility>
#include "bolt/common/base/Exceptions.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/connectors/paimon/BoltMemoryPool.h"
#include "bolt/connectors/paimon/PaimonConfig.h"
#include "bolt/connectors/paimon/PaimonFilterTranslator.h"
#include "bolt/expression/Expr.h"
#include "bolt/expression/ExprToSubfieldFilter.h"
#include "bolt/type/StringView.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/SelectivityVector.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"

namespace bytedance::bolt::connector::paimon {
namespace {

std::shared_ptr<PaimonColumnHandle> paimonColumnHandle(
    const std::shared_ptr<ColumnHandle>& columnHandle,
    const std::string& outputName) {
  auto handle = std::dynamic_pointer_cast<PaimonColumnHandle>(columnHandle);
  BOLT_CHECK_NOT_NULL(
      handle,
      "ColumnHandle must be an instance of PaimonColumnHandle for {}",
      outputName);
  return handle;
}

std::shared_ptr<PaimonColumnHandle> findPaimonColumnHandle(
    const std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>&
        columnHandles,
    const std::string& fieldName) {
  auto it = columnHandles.find(fieldName);
  if (it != columnHandles.end()) {
    return paimonColumnHandle(it->second, fieldName);
  }

  for (const auto& [outputName, columnHandle] : columnHandles) {
    auto handle = paimonColumnHandle(columnHandle, outputName);
    if (handle->name() == fieldName) {
      return handle;
    }
  }
  return nullptr;
}

} // namespace

namespace {

core::TypedExprPtr combineConjuncts(std::vector<core::TypedExprPtr> conjuncts) {
  if (conjuncts.empty()) {
    return nullptr;
  }
  if (conjuncts.size() == 1) {
    return std::move(conjuncts.front());
  }
  return std::make_shared<core::CallTypedExpr>(
      BOOLEAN(), std::move(conjuncts), "and");
}

void collectFieldNames(
    const core::TypedExprPtr& expression,
    std::vector<std::string>& fieldNames,
    std::unordered_set<std::string>& seen) {
  std::vector<core::TypedExprPtr> pending;
  if (expression) {
    pending.push_back(expression);
  }

  while (!pending.empty()) {
    auto current = std::move(pending.back());
    pending.pop_back();
    if (!current) {
      continue;
    }

    const auto* field =
        dynamic_cast<const core::FieldAccessTypedExpr*>(current.get());
    if (field != nullptr &&
        (field->inputs().empty() ||
         dynamic_cast<const core::InputTypedExpr*>(
             field->inputs().front().get()) != nullptr) &&
        seen.insert(field->name()).second) {
      fieldNames.push_back(field->name());
    }

    const auto& inputs = current->inputs();
    for (size_t i = inputs.size(); i > 0; --i) {
      pending.push_back(inputs[i - 1]);
    }
  }
}

std::vector<std::string> collectFieldNames(
    const core::TypedExprPtr& expression) {
  std::vector<std::string> fieldNames;
  std::unordered_set<std::string> seen;
  collectFieldNames(expression, fieldNames, seen);
  return fieldNames;
}

std::pair<std::shared_ptr<::paimon::Predicate>, core::TypedExprPtr> planFilter(
    const core::TypedExprPtr& expression,
    const RowTypePtr& rowType,
    core::ExpressionEvaluator* evaluator) {
  std::vector<core::TypedExprPtr> conjuncts;
  exec::flattenTopLevelConjuncts(expression, conjuncts);

  std::vector<std::shared_ptr<::paimon::Predicate>> predicates;
  std::vector<core::TypedExprPtr> remainingConjuncts;
  for (const auto& conjunct : conjuncts) {
    auto result =
        PaimonFilterTranslator::translate(conjunct, rowType, evaluator);
    if (result.ok()) {
      predicates.push_back(std::move(result.value));
    } else {
      VLOG(1) << "PaimonDataSource: Keeping filter conjunct "
              << conjunct->toString()
              << " as remaining filter: " << result.reason;
      remainingConjuncts.push_back(conjunct);
    }
  }

  std::shared_ptr<::paimon::Predicate> predicate;
  if (predicates.size() == 1) {
    predicate = std::move(predicates.front());
  } else if (!predicates.empty()) {
    auto result = ::paimon::PredicateBuilder::And(predicates);
    BOLT_CHECK(
        result.ok(), "PredicateBuilder::And failed for translated conjuncts");
    predicate = std::move(result).value();
  }

  return {
      std::move(predicate), combineConjuncts(std::move(remainingConjuncts))};
}

} // namespace

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
      expressionEvaluator_(queryCtx->expressionEvaluator()),
      pool_(queryCtx->memoryPool()) {
  // Wrap the query-context pool for Paimon's native memory management.
  paimonPool_ =
      std::make_shared<BoltPaimonMemoryPool>(pool_, expressionEvaluator_);

  ::paimon::ReadContextBuilder ctxBuilder(tableHandle_->tablePath());
  std::vector<std::string> columns;
  columns.reserve(outputType_->size());
  std::vector<TypePtr> filterTypes;
  filterTypes.reserve(outputType_->size());
  std::unordered_set<std::string> readColumnNames;
  for (const auto& outName : outputType_->names()) {
    auto it = columnHandles.find(outName);
    BOLT_CHECK(
        it != columnHandles.end(),
        "Could not find column handle with name: {}",
        outName);
    const auto& columnName = paimonColumnHandle(it->second, outName)->name();
    columns.push_back(columnName);
    readColumnNames.insert(columnName);
  }
  for (const auto& type : outputType_->children()) {
    filterTypes.push_back(type);
  }

  if (tableHandle_->filter()) {
    for (const auto& fieldName : collectFieldNames(tableHandle_->filter())) {
      auto columnHandle = findPaimonColumnHandle(columnHandles, fieldName);
      BOLT_CHECK_NOT_NULL(
          columnHandle,
          "Could not find column handle for filter field: {}",
          fieldName);
      if (readColumnNames.insert(columnHandle->name()).second) {
        columns.push_back(columnHandle->name());
        filterTypes.push_back(columnHandle->type());
      }
    }
  }
  auto filterNames = columns;
  filterRowType_ = ROW(std::move(filterNames), std::move(filterTypes));
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

  if (tableHandle_->filter()) {
    auto filterPlan = planFilter(
        tableHandle_->filter(), filterRowType_, expressionEvaluator_);
    if (filterPlan.first) {
      VLOG(1) << "PaimonDataSource: Translated filter to paimon Predicate: "
              << filterPlan.first->ToString();
    }
    ctxBuilder.SetPredicate(filterPlan.first);
    if (filterPlan.second) {
      remainingFilterExprSet_ =
          expressionEvaluator_->compile(filterPlan.second);
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
  if (inputSplits_.empty() && !currentReader_) {
    return nullptr;
  }

  // Lazily create the BatchReader using accumulated splits.
  if (!currentReader_ && !inputSplits_.empty()) {
    VLOG(1) << "PaimonDataSource::next(): Creating reader with "
            << inputSplits_.size() << " split(s)";
    auto&& readerCreateStatus = tableRead_->CreateReader(inputSplits_);
    if (!readerCreateStatus.ok()) {
      VLOG(1) << "PaimonDataSource::next(): CreateReader error: "
              << readerCreateStatus.status().ToString();
      BOLT_FAIL(
          "create reader error: {}", readerCreateStatus.status().ToString());
    }
    currentReader_ = std::move(readerCreateStatus).value();
  }

  if (!currentReader_) {
    VLOG(1)
        << "PaimonDataSource::next(): No reader available, returning nullopt";
    return nullptr;
  }

  VLOG(1) << "PaimonDataSource::next(): Calling reader->NextBatch() at "
          << currentReader_.get();
  auto batchRes = currentReader_->NextBatch();
  if (!batchRes.ok()) {
    VLOG(1) << "PaimonDataSource::next(): NextBatch NOT ok: "
            << batchRes.status().ToString();
    currentReader_->Close();
    holdReader_.push_back(std::move(currentReader_));
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
    currentReader_->Close();
    holdReader_.push_back(std::move(currentReader_));
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
  auto row = std::dynamic_pointer_cast<RowVector>(
      bytedance::bolt::importFromArrowAsOwner(sch, arr, opts, pool_));
  BOLT_CHECK(row != nullptr, "Imported vector is not a RowVector");
  const auto& rowType = row->type()->asRow();

  VLOG(1) << "Imported RowVector size: " << row->size()
          << ", number of fields: " << rowType.size();

  if (rowType.nameOf(0) == "_VALUE_KIND" && rowType.size() > 1) {
    VLOG(1) << "Dropping _VALUE_KIND field";

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

    row = std::make_shared<RowVector>(
        pool_,
        ROW(std::move(newNames), std::move(newTypes)),
        row->nulls(),
        row->size(),
        std::move(newChildren));
  }

  completedRows_ += row->size();
  auto rowsRemaining = row->size();
  BufferPtr remainingIndices;
  if (remainingFilterExprSet_) {
    auto filterInput = std::make_shared<RowVector>(
        pool_, filterRowType_, row->nulls(), row->size(), row->children());
    rowsRemaining = evaluateRemainingFilter(filterInput);
    if (rowsRemaining == 0) {
      return RowVector::createEmpty(outputType_, pool_);
    }
    if (rowsRemaining < row->size()) {
      remainingIndices = filterEvalCtx_.selectedIndices;
    }
  }

  std::vector<VectorPtr> outputColumns;
  outputColumns.reserve(outputType_->size());
  for (size_t i = 0; i < outputType_->size(); ++i) {
    outputColumns.push_back(
        exec::wrapChild(rowsRemaining, remainingIndices, row->childAt(i)));
  }
  return std::make_shared<RowVector>(
      pool_, outputType_, nullptr, rowsRemaining, std::move(outputColumns));
}

vector_size_t PaimonDataSource::evaluateRemainingFilter(
    RowVectorPtr& rowVector) {
  filterRows_.resizeFill(rowVector->size());
  expressionEvaluator_->evaluate(
      remainingFilterExprSet_.get(), filterRows_, *rowVector, filterResult_);
  return exec::processFilterResults(
      filterResult_, filterRows_, filterEvalCtx_, pool_);
}

} // namespace bytedance::bolt::connector::paimon
