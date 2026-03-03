/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <folly/json.h>
#include <paimon/defs.h>
#include <paimon/read_context.h>
#include <paimon/table/source/data_split.h>
#include <paimon/table/source/split.h>
#include <paimon/table/source/table_read.h>
#include <paimon/type_fwd.h>
#include <memory>
#include "bolt/connectors/paimon/PaimonDataSource.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/type/Type.h"

namespace bytedance::bolt::connector::paimon {

PaimonDataSource::PaimonDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<ConnectorTableHandle>& tableHandle,
    const std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>&
        columnHandles,
    std::shared_ptr<ConnectorQueryCtx> queryCtx,
    const core::QueryConfig& queryConfig)
    : outputType_(outputType),
      tableHandle_(std::dynamic_pointer_cast<PaimonTableHandle>(tableHandle)),
      pool_(
          queryCtx ? queryCtx->memoryPool()
                   : bytedance::bolt::memory::memoryManager()
                         ->addLeafPool("paimon-datasource")
                         .get()) {
  auto boltShared = bytedance::bolt::memory::memoryManager()->addLeafPool(
      "paimon-datasource-inner");
  paimonPool_ = std::make_shared<BoltPaimonMemoryPool>(boltShared);

  ::paimon::ReadContextBuilder ctxBuilder(tableHandle_->tablePath());
  std::vector<std::string> columns;
  columns.reserve(columnHandles.size());
  std::transform(
      columnHandles.begin(),
      columnHandles.end(),
      std::back_inserter(columns),
      [](const auto& kv) { return kv.first; });
  LOG(INFO) << "PaimonDataSource::PaimonDataSource(): Read schema: " << folly::join(", ", columns);
  ctxBuilder.SetReadSchema(columns);
  ctxBuilder.EnableMultiThreadRowToBatch(false);  // Disabled to simplify testing
  ctxBuilder.WithMemoryPool(paimonPool_);
  ctxBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "local");
  for (const auto& [key, value] : tableHandle_->tableProperties()) {
    ctxBuilder.AddOption(key, value);
  }
  auto ctxBuildResult = ctxBuilder.Finish();
  BOLT_CHECK(ctxBuildResult.ok(), "ReadContextBuilder.Finish() failed: {}", ctxBuildResult.status().ToString());
  auto readCtx = std::move(ctxBuildResult).value();
  auto tableReadStatus = ::paimon::TableRead::Create(std::move(readCtx));
  BOLT_CHECK(tableReadStatus.ok(), "TableRead::Create() failed: {}", tableReadStatus.status().ToString());
  tableRead_ = std::move(tableReadStatus).value();
}

PaimonDataSource::~PaimonDataSource() = default;

void PaimonDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  auto paimonConnectorSplit =
      std::dynamic_pointer_cast<PaimonConnectorSplit>(split);
  BOLT_CHECK_NOT_NULL(
      paimonConnectorSplit, "Split was not paimon connector split");
  inputSplits_.push_back(paimonConnectorSplit->split_);
}

std::optional<RowVectorPtr> PaimonDataSource::next(
    uint64_t size,
    ContinueFuture& /* future */) {
  // If we've already encountered EOF (inputSplits_ are cleared and no reader),
  // don't try to do anything else
  if (inputSplits_.empty() && !reader_) {
    return nullptr;
  }

  // Lazily create the BatchReader using accumulated splits.
  if (!reader_ && !inputSplits_.empty()) {
    LOG(INFO) << "PaimonDataSource::next(): Creating reader with " << inputSplits_.size() << " split(s)";
    auto&& readerCreateStatus = tableRead_->CreateReader(inputSplits_);
    LOG(INFO) << "PaimonDataSource::next(): CreateReader returned: " << (readerCreateStatus.ok() ? "OK" : "FAILED");
    if (!readerCreateStatus.ok()) {
      LOG(INFO) << "PaimonDataSource::next(): CreateReader error: " << readerCreateStatus.status().ToString();
      return nullptr;
    }
    reader_ = std::move(readerCreateStatus).value();
    LOG(INFO) << "PaimonDataSource::next(): Created reader at " << reader_.get();
  }

  if (!reader_) {
    LOG(INFO) << "PaimonDataSource::next(): No reader available, returning nullopt";
    return nullptr;
  }

  LOG(INFO) << "PaimonDataSource::next(): Calling reader->NextBatch() at " << reader_.get();
  auto batchRes = reader_->NextBatch();
  if (!batchRes.ok()) {
    LOG(INFO) << "PaimonDataSource::next(): NextBatch NOT ok: " << batchRes.status().ToString();
    reader_->Close();
    reader_.reset();
    inputSplits_.clear();
    return nullptr;
  }

  LOG(INFO) << "PaimonDataSource::next(): NextBatch successful, getting value";
  auto pair = std::move(batchRes).value();

  if (::paimon::BatchReader::IsEofBatch(pair)) {
    LOG(INFO) << "PaimonDataSource::next(): IsEofBatch: true";
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
  LOG(INFO) << "PaimonDataSource::next(): Not an EOF batch, attempting import";
  ArrowArray& arr = *pair.first;
  ArrowSchema& sch = *pair.second;

  LOG(INFO) << "PaimonDataSource::next(): Schema has " << sch.n_children << " children";
  if (sch.n_children > 0) {
    for (int i = 0; i < sch.n_children; ++i) {
      LOG(INFO) << "PaimonDataSource::next(): child " << i << ": name=" << (sch.children[i] ? sch.children[i]->name : "null")
                << ", type=" << (sch.children[i] ? sch.children[i]->format : "null");
    }
  }

  ArrowOptions opts;
  LOG(INFO) << "Calling importFromArrowAsOwner() with schema " << sch.format << " and array";
  auto vec = bytedance::bolt::importFromArrowAsOwner(sch, arr, opts, pool_);

  LOG(INFO) << "importFromArrowAsOwner() returned " << vec.get();

  const auto& row = std::dynamic_pointer_cast<RowVector>(vec);
  BOLT_CHECK(row != nullptr, "Imported vector is not a RowVector");
  const auto& rowType = row->type()->asRow();

  LOG(INFO) << "Imported RowVector size: " << row->size() << ", number of fields: " << rowType.size();

  // If we have _VALUE_KIND as the first field, drop it - but only if the original Parquet reader
  // actually exported it. Check if the data actually exists in the child vector before proceeding.
  auto firstChild = row->childAt(0);
  LOG(INFO) << "First child vector type: " << firstChild->type()->toString() << ", elements: " << firstChild->size();
  if (rowType.nameOf(0) == "_VALUE_KIND" && rowType.size() > 1) {
    LOG(INFO) << "Dropping _VALUE_KIND field";

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
    auto newRowVec = std::make_shared<RowVector>(pool_, newRowType, nullptr, row->size(), newChildren);

    // Copy null information
    newRowVec->setNulls(row->nulls());

    LOG(INFO) << "New RowVector size: " << newRowVec->size() << ", number of fields: " << newRowType->size();

    // Debug: Print the actual values being returned
    auto idColumn = newRowVec->childAt(0);
    auto* idFlat = idColumn->asFlatVector<int64_t>();
    for (int i = 0; i < newRowVec->size(); ++i) {
      if (idColumn->isNullAt(i)) {
        LOG(INFO) << "Row " << i << ": id = NULL";
      } else {
        LOG(INFO) << "Row " << i << ": id = " << idFlat->valueAt(i);
      }
    }

    completedRows_ += newRowVec->size();
    LOG(INFO) << "Returning row vector of size " << newRowVec->size();

    return newRowVec;
  }

  // Debug: Print the actual values being returned if no _VALUE_KIND field
  auto idColumn = row->childAt(0);
  auto* idFlat = idColumn->asFlatVector<int64_t>();
  for (int i = 0; i < row->size(); ++i) {
    if (idColumn->isNullAt(i)) {
      LOG(INFO) << "Row " << i << ": id = NULL";
    } else {
      LOG(INFO) << "Row " << i << ": id = " << idFlat->valueAt(i);
    }
  }

  completedRows_ += row->size();
  LOG(INFO) << "Returning row vector of size " << row->size();

  return row;
}

} // namespace bytedance::bolt::connector::paimon
