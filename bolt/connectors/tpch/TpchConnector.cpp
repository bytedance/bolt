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

#include "bolt/connectors/tpch/TpchConnector.h"
#include "bolt/tpch/gen/TpchGen.h"
namespace bytedance::bolt::connector::tpch {

using bytedance::bolt::tpch::Table;

namespace {

RowVectorPtr getTpchData(
    Table table,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    memory::MemoryPool* pool,
    bool usePrefixColumnName) {
  switch (table) {
    case Table::TBL_PART:
      return bolt::tpch::genTpchPart(
          pool, maxRows, offset, scaleFactor, usePrefixColumnName);
    case Table::TBL_SUPPLIER:
      return bolt::tpch::genTpchSupplier(
          pool, maxRows, offset, scaleFactor, usePrefixColumnName);
    case Table::TBL_PARTSUPP:
      return bolt::tpch::genTpchPartSupp(
          pool, maxRows, offset, scaleFactor, usePrefixColumnName);
    case Table::TBL_CUSTOMER:
      return bolt::tpch::genTpchCustomer(
          pool, maxRows, offset, scaleFactor, usePrefixColumnName);
    case Table::TBL_ORDERS:
      return bolt::tpch::genTpchOrders(
          pool, maxRows, offset, scaleFactor, usePrefixColumnName);
    case Table::TBL_LINEITEM:
      return bolt::tpch::genTpchLineItem(
          pool, maxRows, offset, scaleFactor, usePrefixColumnName);
    case Table::TBL_NATION:
      return bolt::tpch::genTpchNation(
          pool, maxRows, offset, scaleFactor, usePrefixColumnName);
    case Table::TBL_REGION:
      return bolt::tpch::genTpchRegion(
          pool, maxRows, offset, scaleFactor, usePrefixColumnName);
  }
  return nullptr;
}

} // namespace

std::string TpchTableHandle::toString() const {
  return fmt::format(
      "table: {}, scale factor: {}", toTableName(table_), scaleFactor_);
}

TpchDataSource::TpchDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    bolt::memory::MemoryPool* FOLLY_NONNULL pool,
    bool usePrefixedColumnName)
    : pool_(pool), usePrefixedColumnName_(usePrefixedColumnName) {
  auto tpchTableHandle =
      std::dynamic_pointer_cast<TpchTableHandle>(tableHandle);
  BOLT_CHECK_NOT_NULL(
      tpchTableHandle, "TableHandle must be an instance of TpchTableHandle");
  tpchTable_ = tpchTableHandle->getTable();
  scaleFactor_ = tpchTableHandle->getScaleFactor();
  tpchTableRowCount_ = getRowCount(tpchTable_, scaleFactor_);

  auto tpchTableSchema =
      getTableSchema(tpchTableHandle->getTable(), usePrefixedColumnName_);
  BOLT_CHECK_NOT_NULL(tpchTableSchema, "TpchSchema can't be null.");

  outputColumnMappings_.reserve(outputType->size());

  for (const auto& outputName : outputType->names()) {
    auto it = columnHandles.find(outputName);
    BOLT_CHECK(
        it != columnHandles.end(),
        "ColumnHandle is missing for output column '{}' on table '{}'",
        outputName,
        toTableName(tpchTable_));

    auto handle = std::dynamic_pointer_cast<TpchColumnHandle>(it->second);
    BOLT_CHECK_NOT_NULL(
        handle,
        "ColumnHandle must be an instance of TpchColumnHandle "
        "for '{}' on table '{}'",
        it->second->name(),
        toTableName(tpchTable_));

    auto idx = tpchTableSchema->getChildIdxIfExists(handle->name());
    BOLT_CHECK(
        idx != std::nullopt,
        "Column '{}' not found on TPC-H table '{}'.",
        handle->name(),
        toTableName(tpchTable_));
    outputColumnMappings_.emplace_back(*idx);
  }
  outputType_ = outputType;
}

RowVectorPtr TpchDataSource::projectOutputColumns(RowVectorPtr inputVector) {
  std::vector<VectorPtr> children;
  children.reserve(outputColumnMappings_.size());

  for (const auto channel : outputColumnMappings_) {
    children.emplace_back(inputVector->childAt(channel));
  }

  return std::make_shared<RowVector>(
      pool_,
      outputType_,
      BufferPtr(),
      inputVector->size(),
      std::move(children));
}

void TpchDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  BOLT_CHECK_EQ(
      currentSplit_,
      nullptr,
      "Previous split has not been processed yet. Call next() to process the split.");
  currentSplit_ = std::dynamic_pointer_cast<TpchConnectorSplit>(split);
  BOLT_CHECK(currentSplit_, "Wrong type of split for TpchDataSource.");

  size_t partSize =
      std::ceil((double)tpchTableRowCount_ / (double)currentSplit_->totalParts);

  splitOffset_ = partSize * currentSplit_->partNumber;
  splitEnd_ = splitOffset_ + partSize;
}

std::optional<RowVectorPtr> TpchDataSource::next(
    uint64_t size,
    bolt::ContinueFuture& /*future*/) {
  BOLT_CHECK_NOT_NULL(
      currentSplit_, "No split to process. Call addSplit() first.");

  size_t maxRows = std::min(size, (splitEnd_ - splitOffset_));
  auto outputVector = getTpchData(
      tpchTable_,
      maxRows,
      splitOffset_,
      scaleFactor_,
      pool_,
      usePrefixedColumnName_);

  // If the split is exhausted.
  if (!outputVector || outputVector->size() == 0) {
    currentSplit_ = nullptr;
    return nullptr;
  }

  // splitOffset needs to advance based on maxRows passed to getTpchData(), and
  // not the actual number of returned rows in the output vector, as they are
  // not the same for lineitem.
  splitOffset_ += maxRows;
  completedRows_ += outputVector->size();
  completedBytes_ += outputVector->retainedSize();

  return projectOutputColumns(outputVector);
}

// BOLT_REGISTER_CONNECTOR_FACTORY(std::make_shared<TpchConnectorFactory>())

namespace {
static bool FB_ANONYMOUS_VARIABLE(g_ConnectorFactory) =
    CheckTpchConnectorFactoryInit<TpchConnectorFactory>();

} // namespace

} // namespace bytedance::bolt::connector::tpch
