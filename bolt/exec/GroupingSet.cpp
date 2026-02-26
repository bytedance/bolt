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

#include "bolt/exec/GroupingSet.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SpillConfig.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/common/time/Timer.h"
#include "bolt/exec/ContainerRow2RowSerde.h"
#include "bolt/exec/OperatorUtils.h"
#include "bolt/exec/RowToColumnVector.h"
#include "bolt/type/Type.h"
#include "bolt/vector/ComplexVector.h"

using bytedance::bolt::common::testutil::TestValue;
namespace bytedance::bolt::exec {

namespace {
bool allAreSinglyReferenced(
    const std::vector<column_index_t>& argList,
    const std::unordered_map<column_index_t, int>& channelUseCount) {
  return std::all_of(argList.begin(), argList.end(), [&](auto channel) {
    return channelUseCount.find(channel)->second == 1;
  });
}

// Returns true if all vectors are Lazy vectors, possibly wrapped, that haven't
// been loaded yet.
bool areAllLazyNotLoaded(const std::vector<VectorPtr>& vectors) {
  return std::all_of(vectors.begin(), vectors.end(), [](const auto& vector) {
    return isLazyNotLoaded(*vector);
  });
}

} // namespace

GroupingSet::GroupingSet(
    const RowTypePtr& inputType,
    std::vector<std::unique_ptr<VectorHasher>>&& hashers,
    std::vector<column_index_t>&& preGroupedKeys,
    std::vector<AggregateInfo>&& aggregates,
    bool ignoreNullKeys,
    bool isPartial,
    bool isRawInput,
    const std::vector<vector_size_t>& globalGroupingSets,
    const std::optional<column_index_t>& groupIdChannel,
    const common::SpillConfig* spillConfig,
    tsan_atomic<bool>* nonReclaimableSection,
    OperatorCtx* operatorCtx)
    : preGroupedKeyChannels_(std::move(preGroupedKeys)),
      hashers_(std::move(hashers)),
      isGlobal_(hashers_.empty()),
      isPartial_(isPartial),
      isRawInput_(isRawInput),
      queryConfig_(operatorCtx->task()->queryCtx()->queryConfig()),
      aggregates_(std::move(aggregates)),
      masks_(extractMaskChannels(aggregates_)),
      ignoreNullKeys_(ignoreNullKeys),
      spillMemoryThreshold_(operatorCtx->driverCtx()
                                ->queryConfig()
                                .aggregationSpillMemoryThreshold()),
      globalGroupingSets_(globalGroupingSets),
      groupIdChannel_(groupIdChannel),
      spillConfig_(spillConfig),
      nonReclaimableSection_(nonReclaimableSection),
      stringAllocator_(operatorCtx->pool()),
      rows_(operatorCtx->pool()),
      isAdaptive_(queryConfig_.hashAdaptivityEnabled()),
      pool_(*operatorCtx->pool()),
      operatorCtx_(operatorCtx),
      bypassHTDistinctRatio_(queryConfig_.spilledAggregationBypassHTRatio()) {
  BOLT_CHECK_NOT_NULL(nonReclaimableSection_);
  BOLT_CHECK(pool_.trackUsage());

  for (auto& hasher : hashers_) {
    keyChannels_.push_back(hasher->channel());
  }
  std::unordered_map<column_index_t, int> channelUseCount;
  for (const auto& aggregate : aggregates_) {
    for (auto channel : aggregate.inputs) {
      ++channelUseCount[channel];
    }
  }

  for (const auto& aggregate : aggregates_) {
    mayPushdown_.push_back(
        allAreSinglyReferenced(aggregate.inputs, channelUseCount));
  }

  sortedAggregations_ =
      SortedAggregations::create(aggregates_, inputType, &pool_);
  if (isPartial_) {
    BOLT_USER_CHECK_NULL(
        sortedAggregations_,
        "Partial aggregations over sorted inputs are not supported");
  }

  for (auto& aggregate : aggregates_) {
    if (aggregate.distinct) {
      BOLT_USER_CHECK(
          !isPartial_,
          "Partial aggregations over distinct inputs are not supported");
      distinctAggregations_.emplace_back(
          DistinctAggregations::create({&aggregate}, inputType, &pool_));
    } else {
      distinctAggregations_.push_back(nullptr);
    }
  }
}

GroupingSet::~GroupingSet() {
  if (isGlobal_) {
    destroyGlobalAggregations();
  }
}

std::unique_ptr<GroupingSet> GroupingSet::createForMarkDistinct(
    const RowTypePtr& inputType,
    std::vector<std::unique_ptr<VectorHasher>>&& hashers,
    OperatorCtx* operatorCtx,
    tsan_atomic<bool>* nonReclaimableSection) {
  return std::make_unique<GroupingSet>(
      inputType,
      std::move(hashers),
      /*preGroupedKeys*/ std::vector<column_index_t>{},
      /*aggregates*/ std::vector<AggregateInfo>{},
      /*ignoreNullKeys*/ false,
      /*isPartial*/ false,
      /*isRawInput*/ false,
      /*globalGroupingSets*/ std::vector<vector_size_t>{},
      /*groupIdColumn*/ std::nullopt,
      /*spillConfig*/ nullptr,
      nonReclaimableSection,
      operatorCtx);
};

namespace {
bool equalKeys(
    const std::vector<column_index_t>& keys,
    const RowVectorPtr& vector,
    vector_size_t index,
    vector_size_t otherIndex) {
  for (auto key : keys) {
    const auto& child = vector->childAt(key);
    if (!child->equalValueAt(child.get(), index, otherIndex)) {
      return false;
    }
  }

  return true;
}
} // namespace

void GroupingSet::addInput(const RowVectorPtr& input, bool mayPushdown) {
  if (isGlobal_) {
    addGlobalAggregationInput(input, mayPushdown);
    return;
  }

  auto numRows = input->size();
  numInputRows_ += numRows;
  if (!preGroupedKeyChannels_.empty()) {
    if (remainingInput_) {
      addRemainingInput();
    }
    // Look for the last group of pre-grouped keys.
    for (auto i = input->size() - 2; i >= 0; --i) {
      if (!equalKeys(preGroupedKeyChannels_, input, i, i + 1)) {
        // Process that many rows, flush the accumulators and the hash
        // table, then add remaining rows.
        numRows = i + 1;

        remainingInput_ = input;
        firstRemainingRow_ = numRows;
        remainingMayPushdown_ = mayPushdown;
        break;
      }
    }
  }

  activeRows_.resize(numRows);
  activeRows_.setAll();

  addInputForActiveRows(input, mayPushdown);
}

void GroupingSet::noMoreInput() {
  noMoreInput_ = true;

  if (remainingInput_) {
    addRemainingInput();
  }

  // Spill the remaining in-memory state to disk if spilling has been triggered
  // on this grouping set. This is to simplify query OOM prevention when
  // producing output as we don't support to spill during that stage as for now.
  if (hasSpilled()) {
    spill();
    spillSumRowCount_ = spiller_->sumPartitionRowCount();
  }

  ensureOutputFits();
}

bool GroupingSet::hasSpilled() const {
  return spiller_ != nullptr;
}

bool GroupingSet::hasOutput() {
  return noMoreInput_ || remainingInput_;
}

void GroupingSet::addInputForActiveRows(
    const RowVectorPtr& input,
    bool mayPushdown) {
  BOLT_CHECK(!isGlobal_);
  if (!table_) {
    createHashTable();
  }
  ensureInputFits(input);

  BOLT_TEST_ADJUST(
      "bytedance::bolt::exec::GroupingSet::addInputForActiveRows", this);

  if (!bypassProbeHT_) { // probing time
    NanosecondTimer probeTimer(&stats_.aggProbeTimeNs);
    table_->prepareForGroupProbe(
        *lookup_,
        input,
        activeRows_,
        ignoreNullKeys_,
        BaseHashTable::kNoSpillInputStartPartitionBit);
    if (lookup_->rows.empty()) {
      // No rows to probe. Can happen when ignoreNullKeys_ is true and all rows
      // have null keys.
      return;
    }
    table_->groupProbe(*lookup_);
    masks_.addInput(input, activeRows_);
  } else {
    // if there are too many distinct group-by keys and they should be spilled,
    // we can bypass probing the hash table, directly add rows to the table. In
    // the merge phase of output, dong the final agg can get correct results.

    NanosecondTimer probeTimer(&stats_.aggProbeBypassTimeNs);
    stats_.aggProbeBypassCount++;
    table_->directAddRows(*lookup_, input, activeRows_, ignoreNullKeys_);
    if (lookup_->rows.empty()) {
      // No rows to probe. Can happen when ignoreNullKeys_ is true and all rows
      // have null keys.
      return;
    }
    masks_.addInput(input, activeRows_);
  }

  // agg func time
  {
    NanosecondTimer funcTimer(&stats_.aggFunctionTimeNs);
    auto* groups = lookup_->hits.data();
    auto& newGroups = lookup_->newGroups;
    for (auto i = 0; i < aggregates_.size(); ++i) {
      if (!aggregates_[i].sortingKeys.empty()) {
        continue;
      }

      const auto& rows = getSelectivityVector(i);

      if (aggregates_[i].distinct) {
        if (!newGroups.empty()) {
          distinctAggregations_[i]->initializeNewGroups(groups, newGroups);
        }

        if (rows.hasSelections()) {
          distinctAggregations_[i]->addInput(groups, input, rows);
        }
        continue;
      }

      auto& function = aggregates_[i].function;
      if (!newGroups.empty()) {
        function->initializeNewGroups(groups, newGroups);
      }

      // Check is mask is false for all rows.
      if (!rows.hasSelections()) {
        continue;
      }
      populateTempVectors(i, input);
      // TODO(spershin): We disable the pushdown at the moment if selectivity
      // vector has changed after groups generation, we might want to revisit
      // this.
      const bool canPushdown = (&rows == &activeRows_) && mayPushdown &&
          mayPushdown_[i] && areAllLazyNotLoaded(tempVectors_);
      if (isRawInput_) {
        function->addRawInput(groups, rows, tempVectors_, canPushdown);
      } else {
        function->addIntermediateResults(
            groups, rows, tempVectors_, canPushdown);
      }
    }
    tempVectors_.clear();

    if (sortedAggregations_) {
      if (!newGroups.empty()) {
        sortedAggregations_->initializeNewGroups(groups, newGroups);
      }
      sortedAggregations_->addInput(groups, input);
    }
  }
}

void GroupingSet::addRemainingInput() {
  activeRows_.resize(remainingInput_->size());
  activeRows_.clearAll();
  activeRows_.setValidRange(firstRemainingRow_, remainingInput_->size(), true);
  activeRows_.updateBounds();

  addInputForActiveRows(remainingInput_, remainingMayPushdown_);
  remainingInput_.reset();
}

namespace {

void initializeAggregates(
    const std::vector<AggregateInfo>& aggregates,
    RowContainer& rows,
    bool excludeToIntermediate) {
  const auto numKeys = rows.keyTypes().size();
  int i = 0;
  for (auto& aggregate : aggregates) {
    auto& function = aggregate.function;
    if (excludeToIntermediate && function->supportsToIntermediate()) {
      continue;
    }
    function->setAllocator(&rows.stringAllocator());

    const auto rowColumn = rows.columnAt(numKeys + i);
    function->setOffsets(
        rowColumn.offset(),
        rowColumn.nullByte(),
        rowColumn.nullMask(),
        rows.rowSizeOffset());
    ++i;
  }
}
} // namespace

std::vector<Accumulator> GroupingSet::accumulators(bool excludeToIntermediate) {
  std::vector<Accumulator> accumulators;
  accumulators.reserve(aggregates_.size());
  for (auto& aggregate : aggregates_) {
    if (!excludeToIntermediate ||
        !aggregate.function->supportsToIntermediate()) {
      accumulators.push_back(
          Accumulator{aggregate.function.get(), aggregate.intermediateType});
    }
  }

  if (sortedAggregations_ != nullptr) {
    accumulators.push_back(sortedAggregations_->accumulator());
  }

  for (const auto& aggregation : distinctAggregations_) {
    if (aggregation != nullptr) {
      accumulators.push_back(aggregation->accumulator());
    }
  }
  return accumulators;
}

void GroupingSet::createHashTable() {
  bool jitRowEqVectors = queryConfig_.enableJitRowEqVectors();
  if (ignoreNullKeys_) {
    table_ = HashTable<true>::createForAggregation(
        std::move(hashers_),
        accumulators(false),
        &pool_,
        nullptr,
        jitRowEqVectors);
  } else {
    table_ = HashTable<false>::createForAggregation(
        std::move(hashers_),
        accumulators(false),
        &pool_,
        nullptr,
        jitRowEqVectors);
  }

  RowContainer& rows = *table_->rows();
  initializeAggregates(aggregates_, rows, false);

  auto numColumns = rows.keyTypes().size() + aggregates_.size();

  if (sortedAggregations_) {
    sortedAggregations_->setAllocator(&rows.stringAllocator());

    const auto rowColumn = rows.columnAt(numColumns);
    sortedAggregations_->setOffsets(
        rowColumn.offset(),
        rowColumn.nullByte(),
        rowColumn.nullMask(),
        rows.rowSizeOffset());

    ++numColumns;
  }

  for (const auto& aggregation : distinctAggregations_) {
    if (aggregation != nullptr) {
      aggregation->setAllocator(&rows.stringAllocator());

      const auto rowColumn = rows.columnAt(numColumns);
      aggregation->setOffsets(
          rowColumn.offset(),
          rowColumn.nullByte(),
          rowColumn.nullMask(),
          rows.rowSizeOffset());
      ++numColumns;
    }
  }

  lookup_ = std::make_unique<HashLookup>(table_->hashers(), jitRowEqVectors);
  if (!isAdaptive_ && table_->hashMode() != BaseHashTable::HashMode::kHash) {
    table_->forceGenericHashMode();
  }

  rowInfo_ = std::make_optional<RowFormatInfo>(table_->rows(), true);
  LOG(INFO) << __FUNCTION__ << "=============RowContainer details "
            << rows.toString() << ", alignment = " << rows.alignment();
}

void GroupingSet::initializeGlobalAggregation() {
  if (globalAggregationInitialized_) {
    return;
  }

  lookup_ = std::make_unique<HashLookup>(hashers_);
  lookup_->reset(1);

  // Row layout is:
  //  - null flags - one bit per aggregate,
  //  - uint32_t row size,
  //  - fixed-width accumulators - one per aggregate
  //
  // Here we always make space for a row size since we only have one row and no
  // RowContainer.  The whole row is allocated to guarantee that alignment
  // requirements of all aggregate functions are satisfied.
  int32_t rowSizeOffset = bits::nbytes(aggregates_.size());
  int32_t offset = rowSizeOffset + sizeof(int32_t);
  int32_t nullOffset = 0;
  int32_t alignment = 1;

  for (auto& aggregate : aggregates_) {
    auto& function = aggregate.function;

    Accumulator accumulator{
        aggregate.function.get(), aggregate.intermediateType};

    // Accumulator offset must be aligned by their alignment size.
    offset = bits::roundUp(offset, accumulator.alignment());

    function->setAllocator(&stringAllocator_);
    function->setOffsets(
        offset,
        RowContainer::nullByte(nullOffset),
        RowContainer::nullMask(nullOffset),
        rowSizeOffset);

    offset += accumulator.fixedWidthSize();
    ++nullOffset;
    alignment =
        RowContainer::combineAlignments(accumulator.alignment(), alignment);
  }

  if (sortedAggregations_) {
    auto accumulator = sortedAggregations_->accumulator();

    offset = bits::roundUp(offset, accumulator.alignment());

    sortedAggregations_->setAllocator(&stringAllocator_);
    sortedAggregations_->setOffsets(
        offset,
        RowContainer::nullByte(nullOffset),
        RowContainer::nullMask(nullOffset),
        rowSizeOffset);

    offset += accumulator.fixedWidthSize();
    ++nullOffset;
    alignment =
        RowContainer::combineAlignments(accumulator.alignment(), alignment);
  }

  for (const auto& aggregation : distinctAggregations_) {
    if (aggregation != nullptr) {
      auto accumulator = aggregation->accumulator();

      offset = bits::roundUp(offset, accumulator.alignment());

      aggregation->setAllocator(&stringAllocator_);
      aggregation->setOffsets(
          offset,
          RowContainer::nullByte(nullOffset),
          RowContainer::nullMask(nullOffset),
          rowSizeOffset);

      offset += accumulator.fixedWidthSize();
      ++nullOffset;
      alignment =
          RowContainer::combineAlignments(accumulator.alignment(), alignment);
    }
  }

  lookup_->hits[0] = rows_.allocateFixed(offset, alignment);
  const auto singleGroup = std::vector<vector_size_t>{0};
  for (auto& aggregate : aggregates_) {
    if (!aggregate.sortingKeys.empty()) {
      continue;
    }
    aggregate.function->initializeNewGroups(lookup_->hits.data(), singleGroup);
  }

  if (sortedAggregations_) {
    sortedAggregations_->initializeNewGroups(lookup_->hits.data(), singleGroup);
  }

  for (const auto& aggregation : distinctAggregations_) {
    if (aggregation != nullptr) {
      aggregation->initializeNewGroups(lookup_->hits.data(), singleGroup);
    }
  }

  globalAggregationInitialized_ = true;
}

void GroupingSet::addGlobalAggregationInput(
    const RowVectorPtr& input,
    bool mayPushdown) {
  initializeGlobalAggregation();

  auto numRows = input->size();
  activeRows_.resize(numRows);
  activeRows_.setAll();

  masks_.addInput(input, activeRows_);

  auto* group = lookup_->hits[0];

  for (auto i = 0; i < aggregates_.size(); ++i) {
    if (!aggregates_[i].sortingKeys.empty()) {
      continue;
    }
    const auto& rows = getSelectivityVector(i);

    // Check is mask is false for all rows.
    if (!rows.hasSelections()) {
      continue;
    }

    if (aggregates_[i].distinct) {
      distinctAggregations_[i]->addSingleGroupInput(group, input, rows);
      continue;
    }

    auto& function = aggregates_[i].function;

    populateTempVectors(i, input);
    const bool canPushdown =
        mayPushdown && mayPushdown_[i] && areAllLazyNotLoaded(tempVectors_);
    if (isRawInput_) {
      function->addSingleGroupRawInput(group, rows, tempVectors_, canPushdown);
    } else {
      function->addSingleGroupIntermediateResults(
          group, rows, tempVectors_, canPushdown);
    }
  }
  tempVectors_.clear();

  if (sortedAggregations_) {
    sortedAggregations_->addSingleGroupInput(group, input);
  }
}

bool GroupingSet::getGlobalAggregationOutput(
    RowContainerIterator& iterator,
    RowVectorPtr& result) {
  if (iterator.allocationIndex != 0) {
    return false;
  }

  initializeGlobalAggregation();

  auto groups = lookup_->hits.data();
  for (int32_t i = 0; i < aggregates_.size(); ++i) {
    if (!aggregates_[i].sortingKeys.empty()) {
      continue;
    }

    auto& function = aggregates_[i].function;
    if (isPartial_) {
      function->extractAccumulators(groups, 1, &result->childAt(i));
    } else {
      function->extractValues(groups, 1, &result->childAt(i));
    }
  }

  if (sortedAggregations_) {
    sortedAggregations_->extractValues(folly::Range(groups, 1), result);
  }

  for (const auto& aggregation : distinctAggregations_) {
    if (aggregation != nullptr) {
      aggregation->extractValues(folly::Range(groups, 1), result);
    }
  }

  iterator.allocationIndex = std::numeric_limits<int32_t>::max();
  return true;
}

bool GroupingSet::getDefaultGlobalGroupingSetOutput(
    RowContainerIterator& iterator,
    RowVectorPtr& result) {
  BOLT_CHECK(hasDefaultGlobalGroupingSetOutput());

  if (iterator.allocationIndex != 0) {
    return false;
  }
  // Global aggregates don't have grouping keys. But global grouping sets
  // have null values in grouping keys and a groupId column as well. These
  // key fields precede the aggregate columns in the result.
  // This logic builds a row with just aggregate fields to reuse the global
  // aggregate computation from the regular GroupingSet code-path.
  auto outputType = asRowType(result->type());
  auto firstAggregateCol = outputType->size() - aggregates_.size();
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(aggregates_.size());
  types.reserve(aggregates_.size());
  for (auto i = firstAggregateCol; i < outputType->size(); i++) {
    names.push_back(outputType->nameOf(i));
    types.push_back(outputType->childAt(i));
  }
  auto aggregatesType = ROW(std::move(names), std::move(types));
  auto globalAggregatesRow =
      BaseVector::create<RowVector>(aggregatesType, 1, &pool_);

  BOLT_CHECK(getGlobalAggregationOutput(iterator, globalAggregatesRow));

  // There is one output row for each global GroupingSet.
  const auto numGroupingSets = globalGroupingSets_.size();
  result->resize(numGroupingSets);
  BOLT_CHECK(groupIdChannel_.has_value());
  // These first columns are for grouping keys (which could include the
  // GroupId column). For a global grouping set row :
  // i) Non-groupId grouping keys are null.
  // ii) GroupId column is populated with the global grouping set number.
  for (auto i = 0; i < firstAggregateCol; i++) {
    auto column = result->childAt(i);
    if (i == groupIdChannel_.value()) {
      column->resize(numGroupingSets);
      auto* groupIdVector = column->asFlatVector<int64_t>();
      for (auto j = 0; j < numGroupingSets; j++) {
        groupIdVector->set(j, globalGroupingSets_.at(j));
      }
    } else {
      column->resize(numGroupingSets, false);
      for (auto j = 0; j < numGroupingSets; j++) {
        column->setNull(j, true);
      }
    }
  }

  // The remaining aggregate columns are filled from the computed global
  // aggregates.
  for (auto i = firstAggregateCol; i < outputType->size(); i++) {
    auto resultAggregateColumn = result->childAt(i);
    resultAggregateColumn->resize(numGroupingSets);
    auto sourceAggregateColumn =
        globalAggregatesRow->childAt(i - firstAggregateCol);
    for (auto j = 0; j < numGroupingSets; j++) {
      resultAggregateColumn->copy(sourceAggregateColumn.get(), j, 0, 1);
    }
  }

  return true;
}

void GroupingSet::destroyGlobalAggregations() {
  if (!globalAggregationInitialized_) {
    return;
  }
  for (int32_t i = 0; i < aggregates_.size(); ++i) {
    auto& function = aggregates_[i].function;
    auto groups = lookup_->hits.data();
    if (function->accumulatorUsesExternalMemory()) {
      auto groups = lookup_->hits.data();
      function->destroy(folly::Range(groups, 1));
    }
  }
}

void GroupingSet::populateTempVectors(
    int32_t aggregateIndex,
    const RowVectorPtr& input) {
  const auto& channels = aggregates_[aggregateIndex].inputs;
  const auto& constants = aggregates_[aggregateIndex].constantInputs;
  tempVectors_.resize(channels.size());
  for (auto i = 0; i < channels.size(); ++i) {
    if (channels[i] == kConstantChannel) {
      tempVectors_[i] =
          BaseVector::wrapInConstant(input->size(), 0, constants[i]);
    } else {
      // No load of lazy vectors; The aggregate may decide to push down.
      tempVectors_[i] = input->childAt(channels[i]);
    }
  }
}

const SelectivityVector& GroupingSet::getSelectivityVector(
    size_t aggregateIndex) const {
  auto* rows = masks_.activeRows(aggregateIndex);

  // No mask? Use the current selectivity vector for this aggregation.
  if (not rows) {
    return activeRows_;
  }

  return *rows;
}

bool GroupingSet::getOutput(
    int32_t maxOutputRows,
    int32_t maxOutputBytes,
    RowContainerIterator& iterator,
    RowVectorPtr& result) {
  BOLT_TEST_ADJUST("bytedance::bolt::exec::GroupingSet::getOutput", this);
  NanosecondTimer timer(&stats_.aggOutputTimeNs);
  if (isGlobal_) {
    return getGlobalAggregationOutput(iterator, result);
  }

  if (hasDefaultGlobalGroupingSetOutput()) {
    return getDefaultGlobalGroupingSetOutput(iterator, result);
  }

  if (hasSpilled()) {
    return getOutputWithSpill(maxOutputRows, maxOutputBytes, result);
  }
  BOLT_CHECK(!isDistinct());

  // @lint-ignore CLANGTIDY
  char* groups[maxOutputRows];
  const int32_t numGroups = table_
      ? table_->rows()->listRows(
            &iterator, maxOutputRows, maxOutputBytes, groups)
      : 0;
  if (numGroups == 0) {
    if (table_ != nullptr) {
      table_->clear();
    }
    return false;
  }
  if (supportRowBasedOutput_) {
    extractGroupsInRowFormat(folly::Range<char**>(groups, numGroups), result);
  } else {
    extractGroups(folly::Range<char**>(groups, numGroups), result);
  }
  return true;
}

void GroupingSet::extractGroupsInRowFormat(
    folly::Range<char**> groups,
    RowVectorPtr& result) {
  BOLT_DCHECK(!sortedAggregations_ && isPartial_ && rowInfo_.has_value());
  NanosecondTimer timer(&stats_.aggExtractGroupsTimeNs);
  if (groups.empty()) {
    return;
  }
  result->resize(groups.size());
  // 4 bytes for rowSize
  static constexpr int32_t headerSize = sizeof(RowSizeType);
  const auto& rowInfo = rowInfo_.value();
  char *rowStart = nullptr, *newRow = nullptr;
  RowSizeType rowSize = 0, actualRowSize = 0;
  const auto& compositeResult =
      std::dynamic_pointer_cast<CompositeRowVector>(result);
  BOLT_CHECK(compositeResult);
  compositeResult->setRowOffset(sizeof(RowSizeType));

  RowContainer& rows = *table_->rows();
  auto totalKeys = rows.keyTypes().size();
  // extract keys in columnar format
  for (int32_t i = 0; i < totalKeys; ++i) {
    auto keyVector = compositeResult->childAt(i);
    rows.extractColumn(groups.data(), groups.size(), i, 0, keyVector);
  }

  std::vector<std::pair<int32_t, int32_t>> resultRange{{0, groups.size()}};
  convertRowsFromContainerRows(compositeResult, groups, resultRange);
}

void GroupingSet::extractSpilledGroupsInRowFormat(
    folly::Range<char**> groups,
    const RowVectorPtr& result,
    const std::vector<std::pair<int32_t, int32_t>>& resultRanges) {
  BOLT_DCHECK(!sortedAggregations_ && isPartial_ && rowInfo_.has_value());
  NanosecondTimer timer(&stats_.aggExtractGroupsTimeNs);
  if (groups.empty()) {
    return;
  }
  // 4 bytes for rowSize
  static constexpr int32_t headerSize = sizeof(RowSizeType);
  const auto& rowInfo = rowInfo_.value();
  char *rowStart = nullptr, *newRow = nullptr;
  RowSizeType rowSize = 0, actualRowSize = 0;
  const auto& compositeResult =
      std::dynamic_pointer_cast<CompositeRowVector>(result);
  BOLT_CHECK(compositeResult);
  compositeResult->setRowOffset(sizeof(RowSizeType));

  convertRowsFromContainerRows(compositeResult, groups, resultRanges);
}

void GroupingSet::extractGroups(
    folly::Range<char**> groups,
    const RowVectorPtr& result,
    bool excludeKey) {
  NanosecondTimer timer(&stats_.aggExtractGroupsTimeNs);
  result->resize(groups.size());
  if (groups.empty()) {
    return;
  }
  RowContainer& rows = *table_->rows();
  auto totalKeys = rows.keyTypes().size();
  if (!excludeKey) {
    for (int32_t i = 0; i < totalKeys; ++i) {
      auto keyVector = result->childAt(i);
      rows.extractColumn(groups.data(), groups.size(), i, keyVector);
    }
  }
  for (int32_t i = 0; i < aggregates_.size(); ++i) {
    if (!aggregates_[i].sortingKeys.empty()) {
      continue;
    }

    auto& function = aggregates_[i].function;
    auto& aggregateVector = result->childAt(i + totalKeys);
    if (isPartial_) {
      function->extractAccumulators(
          groups.data(), groups.size(), &aggregateVector);
    } else {
      function->extractValues(groups.data(), groups.size(), &aggregateVector);
    }
  }

  if (sortedAggregations_) {
    sortedAggregations_->extractValues(groups, result);
  }

  for (const auto& aggregation : distinctAggregations_) {
    if (aggregation != nullptr) {
      aggregation->extractValues(groups, result);
    }
  }
}

void GroupingSet::resetTable() {
  if (table_ != nullptr) {
    table_->clear();
  }
}
void GroupingSet::resetDistinctNewGroups() {
  if (lookup_) {
    lookup_->distinctNewGroups.clear();
  }
}
bool GroupingSet::isPartialFull(int64_t maxBytes) {
  BOLT_CHECK(isPartial_);

  if (!table_ || pool_.currentBytes() <= maxBytes) {
    return false;
  }
  if (table_->hashMode() != BaseHashTable::HashMode::kArray) {
    // Not a kArray table, no rehashing will shrink this.
    return true;
  }
  auto stats = table_->stats();
  // If we have a large array with sparse data, we rehash this in a
  // mode that turns off value ranges for kArray mode. Large means
  // over 1/16 of the space budget and sparse means under 1 entry
  // per 32 buckets.
  if (stats.capacity * sizeof(void*) > maxBytes / 16 &&
      stats.numDistinct < stats.capacity / 32) {
    table_->decideHashMode(0, true);
  }
  return pool_.currentBytes() > maxBytes;
}

uint64_t GroupingSet::allocatedBytes() const {
  if (table_) {
    return table_->allocatedBytes();
  }

  return stringAllocator_.retainedSize() + rows_.allocatedBytes();
}

const HashLookup& GroupingSet::hashLookup() const {
  return *lookup_;
}

void GroupingSet::ensureInputFits(const RowVectorPtr& input) {
  // Spilling is considered if this is a partial(prefer) or a final or single
  // aggregation and spillPath is set.
  if ((isPartial_ && preferPartialSpill_ == false) || spillConfig_ == nullptr) {
    return;
  }

  const auto numDistinct = table_->numDistinct();
  if (numDistinct == 0) {
    // Table is empty. Nothing to spill.
    return;
  }

  auto* rows = table_->rows();
  auto [freeRows, outOfLineFreeBytes] = rows->freeSpace();
  const auto outOfLineBytes =
      rows->stringAllocator().retainedSize() - outOfLineFreeBytes;
  const auto outOfLineBytesPerRow = outOfLineBytes / numDistinct;
  const int64_t flatBytes = input->usedSize();

  // Test-only spill path.
  if (spillConfig_->testSpillPct > 0 &&
      (folly::hasher<uint64_t>()(++spillTestCounter_)) % 100 <=
          spillConfig_->testSpillPct) {
    spill();
    return;
  }

  const auto currentUsage = pool_.currentBytes();
  if (spillMemoryThreshold_ != 0 && currentUsage > spillMemoryThreshold_) {
    spill();
    return;
  }

  const auto minReservationBytes =
      currentUsage * spillConfig_->minSpillableReservationPct / 100;
  const auto availableReservationBytes = pool_.availableReservation();
  const auto tableIncrementBytes = table_->hashTableSizeIncrease(input->size());
  const auto incrementBytes =
      rows->sizeIncrement(input->size(), outOfLineBytes ? flatBytes * 2 : 0) +
      tableIncrementBytes;

  // First to check if we have sufficient minimal memory reservation.
  if (availableReservationBytes >= minReservationBytes) {
    if ((tableIncrementBytes == 0) && (freeRows > input->size()) &&
        (outOfLineBytes == 0 || outOfLineFreeBytes >= flatBytes * 2)) {
      // Enough free rows for input rows and enough variable length free space
      // for double the flat size of the whole vector. If outOfLineBytes is 0
      // there is no need for variable length space. Double the flat size is a
      // stopgap because the real increase can be higher, specially with
      // aggregates that have stl or folly containers. Make a way to raise the
      // reservation in the spill protected section instead.
      return;
    }

    // If there is variable length data we take double the flat size of the
    // input as a cap on the new variable length data needed. Same condition as
    // in first check. Completely arbitrary. Allow growth in spill protected
    // area instead.
    // There must be at least 2x the increment in reservation.
    if (availableReservationBytes > 2 * incrementBytes) {
      return;
    }
  }

  // Check if we can increase reservation. The increment is the larger of twice
  // the maximum increment from this input and 'spillableReservationGrowthPct_'
  // of the current memory usage.
  const auto targetIncrementBytes = std::max<int64_t>(
      incrementBytes * 2,
      currentUsage * spillConfig_->spillableReservationGrowthPct / 100);
  {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (pool_.maybeReserve(targetIncrementBytes)) {
      return;
    }
  }
  LOG(WARNING) << "Failed to reserve " << succinctBytes(targetIncrementBytes)
               << " for memory pool " << pool_.name()
               << ", usage: " << succinctBytes(pool_.currentBytes())
               << ", reservation: " << succinctBytes(pool_.reservedBytes())
               << ", flatBytes: " << succinctBytes(flatBytes)
               << ", input numRows = " << input->size()
               << ", outOfLineBytes = " << outOfLineBytes;
}

void GroupingSet::ensureOutputFits() {
  // If spilling has already been triggered on this operator, then we don't need
  // to reserve memory for the output as we can't reclaim much memory from this
  // operator itself. The output processing can reclaim memory from the other
  // operator or query through memory arbitration.
  if ((isPartial_ && preferPartialSpill_ == false) || spillConfig_ == nullptr ||
      hasSpilled()) {
    return;
  }

  // Test-only spill path.
  if (spillConfig_->testSpillPct > 0 &&
      (folly::hasher<uint64_t>()(++spillTestCounter_)) % 100 <=
          spillConfig_->testSpillPct) {
    spill(RowContainerIterator{});
    return;
  }

  const uint64_t outputBufferSizeToReserve =
      queryConfig_.preferredOutputBatchBytes() * 1.2;
  {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (pool_.maybeReserve(outputBufferSizeToReserve)) {
      return;
    }
  }
  LOG(WARNING) << "Failed to reserve "
               << succinctBytes(outputBufferSizeToReserve)
               << " for memory pool " << pool_.name()
               << ", usage: " << succinctBytes(pool_.currentBytes())
               << ", reservation: " << succinctBytes(pool_.reservedBytes());
}

RowTypePtr GroupingSet::makeSpillType() const {
  auto rows = table_->rows();
  auto types = rows->keyTypes();

  for (const auto& accumulator : rows->accumulators()) {
    types.push_back(accumulator.spillType());
  }

  std::vector<std::string> names;
  for (auto i = 0; i < types.size(); ++i) {
    names.push_back(fmt::format("s{}", i));
  }

  return ROW(std::move(names), std::move(types));
}

void GroupingSet::spill() {
  // NOTE: if the disk spilling is triggered by the memory arbitrator, then it
  // is possible that the grouping set hasn't processed any input data yet.
  // Correspondingly, 'table_' will not be initialized at that point.
  if (table_ == nullptr || table_->numDistinct() == 0) {
    return;
  }

  auto* spillConf = const_cast<common::SpillConfig*>(spillConfig_);
  operatorCtx_->adjustSpillCompressionKind(spillConf);
  spillConf->needSetNextEqual = bypassProbeHT_;

  if (!hasSpilled()) {
    auto rows = table_->rows();
    BOLT_DCHECK(pool_.trackUsage());
    BOLT_CHECK_EQ(numDistinctSpilledFiles_, 0);
    BOLT_CHECK_GT(rows->probedFlagOffset(), 0);
    spiller_ = std::make_unique<Spiller>(
        Spiller::Type::kAggregateInput,
        rows,
        makeSpillType(),
        rows->keyTypes().size(),
        std::vector<CompareFlags>(),
        spillConfig_);
    spiller_->setSpillConfig(spillConfig_);
    BOLT_CHECK_EQ(spiller_->state().maxPartitions(), 1);
  }
  LOG(INFO) << operatorCtx_->toString()
            << " spill row container, row number: " << table_->rows()->numRows()
            << ", bytes: " << table_->rows()->allocatedBytes()
            << ", probe ratio = " << table_->getDistinctRatio();
  spiller_->spill();
  if (isDistinct() && numDistinctSpilledFiles_ == 0) {
    numDistinctSpilledFiles_ = spiller_->state().numFinishedFiles(0);
    BOLT_CHECK_GT(numDistinctSpilledFiles_, 0);
  }
  if (sortedAggregations_) {
    sortedAggregations_->clear();
  }
  adjustBypassHashTable(spillConf);
  table_->clear();
}

void GroupingSet::adjustBypassHashTable(common::SpillConfig* spillConf) {
  // enable if rowbased spill and
  // 1. less next equal rows in sorted rows if bypassed
  // 2. too many distinct keys from hash tables if not bypassed
  bool oldBypassProbeHT = bypassProbeHT_;
  bypassProbeHT_ = spillConfig_ &&
      spillConfig_->rowBasedSpillMode != common::RowBasedSpillMode::DISABLE &&
      (oldBypassProbeHT
           ? table_->rows()->numRows() - spillConf->aggBypassHTEqualNum >=
               table_->rows()->numRows() * bypassHTDistinctRatio_
           : table_->getDistinctRatio() >= bypassHTDistinctRatio_);
  if (oldBypassProbeHT != bypassProbeHT_) {
    LOG(INFO)
        << "The state of spilled aggregation bypass hash table is changed from "
        << oldBypassProbeHT << " to " << bypassProbeHT_ << " after # "
        << stats_.aggProbeBypassCount
        << ", bypassHTDistinctRatio_ = " << bypassHTDistinctRatio_
        << ", equalNum = " << spillConf->aggBypassHTEqualNum
        << ", spill rows = " << table_->rows()->numRows()
        << ", offset = " << table_->rows()->probedFlagOffset()
        << ", needSetNextEqual = " << spillConf->needSetNextEqual
        << ", bypassHT = " << bypassProbeHT_;
  }
  spillConf->aggBypassHTEqualNum = 0;
}

void GroupingSet::spill(const RowContainerIterator& rowIterator) {
  BOLT_CHECK(!hasSpilled());

  if (table_ == nullptr) {
    return;
  }

  auto* spillConf = const_cast<common::SpillConfig*>(spillConfig_);
  operatorCtx_->adjustSpillCompressionKind(spillConf);
  spillConf->needSetNextEqual = bypassProbeHT_;
  auto* rows = table_->rows();
  BOLT_CHECK(pool_.trackUsage());
  spiller_ = std::make_unique<Spiller>(
      Spiller::Type::kAggregateOutput, rows, makeSpillType(), spillConfig_);
  spiller_->setSpillConfig(spillConfig_);
  spiller_->spill(rowIterator);
  adjustBypassHashTable(spillConf);
  table_->clear();
}

bool GroupingSet::getOutputWithSpill(
    int32_t maxOutputRows,
    int32_t maxOutputBytes,
    const RowVectorPtr& result) {
  if (merge_ == nullptr && rowBasedSpillMerge_ == nullptr) {
    LOG(INFO) << operatorCtx_->toString() << " prepare merge, files number: "
              << spiller_->state().numFinishedFiles(0)
              << " memory: " << pool_.currentBytes()
              << ", reserved: " << pool_.reservedBytes();
    BOLT_CHECK_NULL(mergeRows_);
    BOLT_CHECK(mergeArgs_.empty());

    if (!isDistinct()) {
      mergeArgs_.resize(1);
      std::vector<TypePtr> keyTypes;
      for (auto& hasher : table_->hashers()) {
        keyTypes.push_back(hasher->type());
      }

      mergeRows_ = std::make_unique<RowContainer>(
          keyTypes,
          !ignoreNullKeys_,
          accumulators(false),
          std::vector<TypePtr>(),
          false,
          false,
          true,
          false,
          &pool_,
          table_->rows()->stringAllocatorShared());

      initializeAggregates(aggregates_, *mergeRows_, false);
    }

    BOLT_CHECK_EQ(table_->rows()->numRows(), 0);

    auto spillPartition = spiller_->finishSpill();
    if (spillPartition.numFiles() == 0) {
      BOLT_CHECK(spiller_->type() == Spiller::Type::kAggregateOutput);
      BOLT_CHECK(spiller_->filledZeroRows());
      return false;
    }
    if (spillConfig_->rowBasedSpillMode == common::RowBasedSpillMode::DISABLE) {
      merge_ = spillPartition.createOrderedReader(
          &pool_, spillConfig_->spillUringEnabled);
    } else {
      rowBasedSpillMerge_ = spillPartition.createRowBasedOrderedReader(
          &pool_,
          table_->rows(),
          spillConfig_->getJITenabledForSpill(),
          spillConfig_->spillUringEnabled);
    }
    LOG(INFO) << operatorCtx_->toString()
              << " memory usage after preparing merge: "
              << pool_.currentBytes();
  }
  BOLT_CHECK_EQ(spiller_->state().maxPartitions(), 1);

  if (merge_ == nullptr && rowBasedSpillMerge_ == nullptr) {
    return false;
  }

  return mergeNext(maxOutputRows, maxOutputBytes, result);
}

bool GroupingSet::mergeNext(
    int32_t maxOutputRows,
    int32_t maxOutputBytes,
    const RowVectorPtr& result) {
  if (spillConfig_->rowBasedSpillMode == common::RowBasedSpillMode::DISABLE) {
    if (isDistinct()) {
      return mergeNextWithoutAggregates(maxOutputRows, result);
    } else {
      return mergeNextWithAggregates(maxOutputRows, maxOutputBytes, result);
    }
  } else {
    if (supportRowBasedOutput_) {
      bool ret = mergeNextWithRowBasedSpillInRowFormat(
          maxOutputRows, maxOutputBytes, result);
      return ret;
    } else {
      return mergeNextWithRowBasedSpill(maxOutputRows, maxOutputBytes, result);
    }
  }
}

void GroupingSet::copyKeyAndInitGroup(
    std::vector<char*>& distinctRows,
    std::vector<char*>& groups,
    size_t& initGroupCount,
    const RowContainer* container,
    const RowVectorPtr& result,
    const vector_size_t resultOffset) {
  NanosecondTimer aggTimer(&stats_.aggOutputUpdateTimeNs);
  auto numNewGroups = distinctRows.size() - initGroupCount;
  // has new groups to handle
  if (numNewGroups) {
    // for RowBased output, do not resize!!!
    if (!supportRowBasedOutput_) {
      result->resize(distinctRows.size());
    }
    // key direct copy into result vector
    for (auto i = 0; i < keyChannels_.size(); ++i) {
      rowToColumnVector(
          distinctRows.data() + initGroupCount,
          distinctRows.size() - initGroupCount,
          container->columns().at(i),
          resultOffset,
          result->childAt(i));
    }
    if (!isDistinct()) {
      BOLT_CHECK_EQ(distinctRows.size(), groups.size());
      // init from initGroupCount to groups.size()
      ensureGroupIndices(groups.size());

      if (supportRowBasedOutput_) {
        // need to store keys in columnar format
        SelectivityVector selectivity(result->size(), true);

        for (auto row = 0; row < numNewGroups; ++row) {
          for (auto i = 0; i < keyChannels_.size(); ++i) {
            DecodedVector decoded(*result->childAt(i), selectivity);
            char* group = groups[groupIndicesOfRows_[initGroupCount + row]];
            mergeRows_->store(decoded, resultOffset + row, group, i);
          }
        }
      }

      for (auto& aggregate : aggregates_) {
        BOLT_CHECK(aggregate.sortingKeys.empty());
        aggregate.function->initializeNewGroups(
            groups.data(),
            folly::Range<const vector_size_t*>(
                groupIndicesOfRows_.data() + initGroupCount,
                groups.size() - initGroupCount));
      }
    }
    initGroupCount = distinctRows.size();
  }
}

void GroupingSet::copyKeyAndUpdateGroups(
    std::vector<char*>& rows,
    std::vector<char*>& groupOfRows,
    std::vector<char*>& distinctRows,
    std::vector<char*>& groups,
    size_t& initGroupCount,
    const RowContainer* container,
    const RowVectorPtr& result,
    const vector_size_t resultOffset) {
  if (rows.empty()) {
    return;
  }
  BOLT_CHECK_EQ(rows.size(), groupOfRows.size());
  copyKeyAndInitGroup(
      distinctRows, groups, initGroupCount, container, result, resultOffset);
  NanosecondTimer aggTimer(&stats_.aggOutputUpdateTimeNs);
  // calculate rows into groupOfRows
  SelectivityVector selections(groupOfRows.size());
  auto& accumulators = container->accumulators();
  BOLT_CHECK_EQ(accumulators.size(), aggregates_.size());
  for (auto i = 0; i < aggregates_.size(); ++i) {
    // extract accumulate
    std::vector<VectorPtr> args{BaseVector::create(
        accumulators[i].spillType(), groupOfRows.size(), &pool_)};
    accumulators[i].extractForSpill(
        folly::Range<char**>(rows.data(), groupOfRows.size()), args[0]);
    // update accumulate to groups
    aggregates_[i].function->addIntermediateResults(
        groupOfRows.data(), selections, args, false);
  }
  rows.clear();
  groupOfRows.clear();
}

void GroupingSet::outputUniqueGroups(
    std::vector<char*>& uniqueRows,
    const RowVectorPtr& uniqueRes,
    size_t& uniqueCount,
    size_t& estimateUniqueBytesPerRow) {
  if (!uniqueRows.empty()) {
    NanosecondTimer aggTimer(&stats_.aggOutputUpdateTimeNs);
    if (uniqueRows.size() + uniqueCount > uniqueRes->size()) {
      uniqueRes->resize(uniqueRows.size() + uniqueCount);
    }
    auto* container = spiller_->container();
    for (auto i = 0; i < keyChannels_.size(); ++i) {
      rowToColumnVector(
          uniqueRows.data(),
          uniqueRows.size(),
          container->columns().at(i),
          uniqueCount,
          uniqueRes->childAt(i));
    }

    auto& accumulators = container->accumulators();
    BOLT_CHECK_EQ(accumulators.size(), aggregates_.size());
    for (auto i = 0; i < aggregates_.size(); ++i) {
      // extract accumulate
      auto aggCol = uniqueRes->childAt(i + keyChannels_.size());
      VectorPtr args = BaseVector::create(
          isPartial_ ? accumulators[i].spillType()
                     : accumulators[i].finalOutputType(),
          uniqueRows.size(),
          &pool_);
      BOLT_CHECK(aggCol->type()->equivalent(*args->type()));
      isPartial_
          ? accumulators[i].extractForSpill(
                folly::Range<char**>(uniqueRows.data(), uniqueRows.size()),
                args)
          : accumulators[i].extractForOutput(
                folly::Range<char**>(uniqueRows.data(), uniqueRows.size()),
                args);
      aggCol->copy(args->wrappedVector(), uniqueCount, 0, args->size());
      BOLT_CHECK_EQ(uniqueCount + args->size(), aggCol->size());
    }
    uniqueCount += uniqueRows.size();
    uniqueRows.clear();
    estimateUniqueBytesPerRow = std::max(
        estimateUniqueBytesPerRow,
        (size_t)(uniqueRes->inMemoryBytes() * 1.0 / uniqueRes->size()));
  }
}

void GroupingSet::outputUniqueGroupsInRowFormat(
    std::vector<char*>& uniqueRows,
    const RowVectorPtr& result,
    const int32_t resultOffset,
    size_t& estimateUniqueBytesPerRow,
    const std::vector<int32_t>& rowSizeVec,
    const uint64_t totalUniqueRowSize) {
  if (!uniqueRows.empty()) {
    auto* container = spiller_->container();
    const CompositeRowVectorPtr& compositeResult =
        std::dynamic_pointer_cast<CompositeRowVector>(result);
    NanosecondTimer aggTimer(&stats_.aggOutputUpdateTimeNs);
    // extract keys in RowVector format
    for (auto i = 0; i < keyChannels_.size(); ++i) {
      rowToColumnVector(
          uniqueRows.data(),
          uniqueRows.size(),
          container->columns().at(i),
          resultOffset,
          compositeResult->childAt(i));
    }
    convertRowsFromSpilledRows(
        compositeResult,
        uniqueRows,
        resultOffset,
        rowSizeVec,
        totalUniqueRowSize);
    estimateUniqueBytesPerRow = std::max<size_t>(
        compositeResult->estimateRowSize(), estimateUniqueBytesPerRow);
  }
}

bool GroupingSet::mergeNextWithRowBasedSpillInRowFormat(
    int32_t maxOutputRows,
    int32_t maxOutputBytes,
    const RowVectorPtr& result) {
  // row based spill
  BOLT_CHECK_NOT_NULL(rowBasedSpillMerge_);

  auto* container = spiller_->container();
  // rows read from spill file as agg input
  std::vector<char*> rows;
  // distinct rows to extract keys
  std::vector<char*> distinctRows;
  // the only unique rows
  std::vector<char*> uniqueRows;
  std::vector<int32_t> uniqueRowSize;
  // groups stores agg result
  std::vector<char*> groups;
  // groups of each row
  std::vector<char*> groupOfRows;
  size_t initGroupCount = 0;
  vector_size_t uniqueCount = 0;
  size_t estimateUniqueBytesPerRow = container->fixedRowSize();
  size_t totalUniqueRowSize = 0;

  std::vector<std::pair<int32_t, int32_t>> resultRanges;
  int32_t totalGroupsInResult = 0;

  // True if 'merge_' indicates that the next key is the same as the current
  // one.
  bool nextKeyIsEqual{false};

  const CompositeRowVectorPtr& compositeResult =
      std::dynamic_pointer_cast<CompositeRowVector>(result);
  BOLT_CHECK(compositeResult);
  compositeResult->setRowOffset(sizeof(RowSizeType));

  auto copyUniqueRowsAndKeys = [&]() {
    result->resize(distinctRows.size() + uniqueRows.size() + uniqueCount);
    // copy unique rows to result first
    outputUniqueGroupsInRowFormat(
        uniqueRows,
        result,
        totalGroupsInResult,
        estimateUniqueBytesPerRow,
        uniqueRowSize,
        totalUniqueRowSize);
    totalGroupsInResult += uniqueRows.size();
    uniqueCount += uniqueRows.size();
    uniqueRows.clear();
    uniqueRowSize.clear();
    totalUniqueRowSize = 0;

    auto numNewGroups = distinctRows.size() - initGroupCount;
    if (numNewGroups) {
      resultRanges.emplace_back(
          std::make_pair(totalGroupsInResult, numNewGroups));
    }
    copyKeyAndUpdateGroups(
        rows,
        groupOfRows,
        distinctRows,
        groups,
        initGroupCount,
        container,
        result,
        totalGroupsInResult);
    totalGroupsInResult += numNewGroups;
  };

  while (true) {
    auto next = rowBasedSpillMerge_->nextWithEquals();
    if (next.first == nullptr) {
      // no more data to read
      copyUniqueRowsAndKeys();

      if (initGroupCount) {
        // from RowContainer format
        extractSpillResultInRowFormat(result, resultRanges);
      }

      if (uniqueCount) {
        stats_.aggOutputUniqueRows += uniqueCount;
      }
      return result->size() > 0;
    }
    bool isEndOfBatch = false;
    const auto& currentBatch = next.first->current();
    int32_t rowSize = 0;
    auto index = next.first->currentIndex(&isEndOfBatch, rowSize);
    bool unique = false;
    if (!nextKeyIsEqual) {
      // this row is in new group
      // if not support unique output optimization or next is equal
      // or this is the end of batch(so can not get rowSize), take the row as
      // non-unique
      if (!supportUniqueRowOpt_ || next.second) {
        groups.push_back(mergeRows_->newRow());
        distinctRows.push_back(currentBatch[index]);
      } else {
        unique = true;
        uniqueRows.push_back(currentBatch[index]);
        rowSize = isEndOfBatch ? ContainerRow2RowSerde::rowSize(
                                     currentBatch[index], rowInfo_.value())
                               : rowSize;
        uniqueRowSize.push_back(rowSize);
        totalUniqueRowSize += rowSize;
      }
    }
    nextKeyIsEqual = next.second;
    if (!unique) {
      rows.push_back(currentBatch[index]);
      groupOfRows.push_back(groups.back());
    }
    if (isEndOfBatch) {
      // stream end, should calculate agg with rows in case rows becomes
      // invalid
      copyUniqueRowsAndKeys();
    }
    next.first->pop();
    ++spillOutputRowCount_;
    if (!nextKeyIsEqual &&
        ((groups.size() + uniqueCount + uniqueRows.size() >= maxOutputRows) ||
         (mergeRows_->usedBytes() +
              (uniqueCount + uniqueRows.size()) * estimateUniqueBytesPerRow >=
          maxOutputBytes))) {
      copyUniqueRowsAndKeys();

      if (initGroupCount) {
        extractSpillResultInRowFormat(result, resultRanges);
      }
      if (uniqueCount) {
        stats_.aggOutputUniqueRows += uniqueCount;
      }
      return true;
    }
  }
}

bool GroupingSet::mergeNextWithRowBasedSpill(
    int32_t maxOutputRows,
    int32_t maxOutputBytes,
    const RowVectorPtr& result) {
  // row based spill
  BOLT_CHECK_NOT_NULL(rowBasedSpillMerge_);

  auto* container = spiller_->container();
  // rows read from spill file as agg input
  std::vector<char*> rows;
  // distinct rows to extract keys
  std::vector<char*> distinctRows;
  // the only unique rows
  std::vector<char*> uniqueRows;
  // groups stores agg result
  std::vector<char*> groups;
  // groups of each row
  std::vector<char*> groupOfRows;
  size_t initGroupCount = 0;
  // TODO(fzh): to fix size
  RowVectorPtr uniqueRes = RowVector::createEmpty(result->type(), &pool_);
  size_t uniqueCount = 0;
  size_t estimateUniqueBytesPerRow = container->fixedRowSize();
  size_t estimateUniqueBytes = 0;

  result->resize(0);
  if (isDistinct()) {
    bool newDistinct{true};
    while (distinctRows.size() < maxOutputRows) {
      const auto next = rowBasedSpillMerge_->nextWithEquals();
      auto* stream = next.first;
      if (stream == nullptr) {
        break;
      }
      if (stream->id() < numDistinctSpilledFiles_) {
        // current key exist in memory, so do not output to result
        newDistinct = false;
      }
      if (!next.second) {
        if (newDistinct) {
          // for new key and newDistinct, save current key
          distinctRows.push_back(stream->current()[stream->currentIndex()]);
        }
        newDistinct = true;
      }
      bool isEndOfBatch{false};
      stream->currentIndex(&isEndOfBatch);
      if (isEndOfBatch) {
        copyKeyAndInitGroup(
            distinctRows,
            groups,
            initGroupCount,
            container,
            result,
            initGroupCount);
      }
      stream->pop();
      ++spillOutputRowCount_;
    }
    copyKeyAndInitGroup(
        distinctRows,
        groups,
        initGroupCount,
        container,
        result,
        initGroupCount);
    return result->size() > 0;
  } else {
    // True if 'merge_' indicates that the next key is the same as the current
    // one.
    bool nextKeyIsEqual{false};
    while (true) {
      auto next = rowBasedSpillMerge_->nextWithEquals();
      if (next.first == nullptr) {
        // no more data to read
        copyKeyAndUpdateGroups(
            rows,
            groupOfRows,
            distinctRows,
            groups,
            initGroupCount,
            container,
            result,
            initGroupCount);
        outputUniqueGroups(
            uniqueRows, uniqueRes, uniqueCount, estimateUniqueBytesPerRow);
        if (initGroupCount) {
          extractSpillResult(result, true);
        }
        if (uniqueCount) {
          result->append(uniqueRes->wrappedVector());
          stats_.aggOutputUniqueRows += uniqueCount;
        }
        return result->size() > 0;
      }
      bool isEndOfBatch = false;
      const auto& currentBatch = next.first->current();
      auto index = next.first->currentIndex(&isEndOfBatch);
      bool unique = false;
      if (!nextKeyIsEqual) {
        // this row is in new group
        if (next.second) {
          groups.push_back(mergeRows_->newRow());
          distinctRows.push_back(currentBatch[index]);
        } else {
          unique = true;
          uniqueRows.push_back(currentBatch[index]);
          estimateUniqueBytes += estimateUniqueBytesPerRow;
        }
      }
      nextKeyIsEqual = next.second;
      if (!unique) {
        rows.push_back(currentBatch[index]);
        groupOfRows.push_back(groups.back());
      }
      if (isEndOfBatch) {
        // stream end, should calculate agg with rows in case rows becomes
        // invalid
        copyKeyAndUpdateGroups(
            rows,
            groupOfRows,
            distinctRows,
            groups,
            initGroupCount,
            container,
            result,
            initGroupCount);
        outputUniqueGroups(
            uniqueRows, uniqueRes, uniqueCount, estimateUniqueBytesPerRow);
      }
      next.first->pop();
      ++spillOutputRowCount_;
      if (!nextKeyIsEqual &&
          ((groups.size() + uniqueCount + uniqueRows.size() >= maxOutputRows) ||
           (mergeRows_->usedBytes() + estimateUniqueBytes >= maxOutputBytes))) {
        copyKeyAndUpdateGroups(
            rows,
            groupOfRows,
            distinctRows,
            groups,
            initGroupCount,
            container,
            result,
            initGroupCount);
        outputUniqueGroups(
            uniqueRows, uniqueRes, uniqueCount, estimateUniqueBytesPerRow);

        if (initGroupCount) {
          extractSpillResult(result, true);
        }
        if (uniqueCount) {
          result->append(uniqueRes->wrappedVector());
          stats_.aggOutputUniqueRows += uniqueCount;
        }
        return true;
      }
    }
  }
}

bool GroupingSet::mergeNextWithAggregates(
    int32_t maxOutputRows,
    int32_t maxOutputBytes,
    const RowVectorPtr& result) {
  // Merge all spill data, update aggregated intermediate result into groups,
  // and output the aggregated result. First, read the merged data and
  // temporarily store each row in a vector until the batch of a stream ends or
  // the number of rows reaches the limit. Then, copy all distinct keys to the
  // result and update the intermediate result to the groups until result size
  // reaches limit or all data processing is completed, finally extract result
  // in groups into result vector and return result.
  BOLT_CHECK_NOT_NULL(merge_);
  BOLT_CHECK(!isDistinct());

  // True if 'merge_' indicates that the next key is the same as the current
  // one.
  bool nextKeyIsEqual{false};

  // stores all rows need to be update to groups
  std::vector<const RowVector*> sources;
  std::vector<vector_size_t> sourceIndice;
  std::vector<char*> groupOfRows;

  // all distinct rows to extract keys
  std::vector<const RowVector*> distinctInputs;
  std::vector<vector_size_t> distinctIndices;
  std::vector<char*> groups;

  RowVectorPtr intermediate;
  uint64_t averageResultRowSize = 0;

  // gather all saved keys into result vector and all saved intermediate result
  // into intermediate and update into groups
  auto gatherAndUpdate = [&]() {
    NanosecondTimer aggTimer(&stats_.aggOutputUpdateTimeNs);
    if (!distinctInputs.empty()) {
      // copy keys to result
      auto oldResultSize = result->size();
      result->resize(oldResultSize + distinctInputs.size());
      for (auto i = 0; i < keyChannels_.size(); ++i) {
        gatherCopy(
            result->childAt(i).get(),
            oldResultSize,
            distinctInputs.size(),
            distinctInputs,
            distinctIndices,
            i);
      }

      // initialize rows in row container
      initializeRows(groups);

      distinctInputs.clear();
      distinctIndices.clear();
      groups.clear();
    }

    if (!sources.empty()) {
      // gather intermediate and add intermediate to groups
      if (!intermediate) {
        intermediate = BaseVector::create<RowVector>(
            sources.back()->type(), maxOutputRows, &pool_);
      }
      intermediate->resize(sources.size());
      // only copy intermediate part
      for (auto i = keyChannels_.size(); i < intermediate->childrenSize();
           i++) {
        gatherCopy(
            intermediate->childAt(i).get(),
            0,
            sources.size(),
            sources,
            sourceIndice,
            i);
      }
      updateRows(intermediate, groupOfRows);
      sources.clear();
      sourceIndice.clear();
      groupOfRows.clear();
    }
    if (result->size() != 0) {
      averageResultRowSize = result->estimateFlatSize() / result->size();
    }
  };

  result->resize(0);
  for (;;) {
    auto next = merge_->nextWithEquals();
    // no more data, gather all saved rows and extract result
    if (next.first == nullptr) {
      gatherAndUpdate();
      extractSpillResult(result, true);
      return result->size() > 0;
    }
    if (!nextKeyIsEqual) {
      // new distinct rows
      mergeState_ = mergeRows_->newRow();
      distinctInputs.push_back(&next.first->current());
      distinctIndices.push_back(next.first->currentIndex());
      groups.push_back(mergeState_);
    }
    bool isEndOfRow = false;
    sources.push_back(&next.first->current());
    sourceIndice.push_back(next.first->currentIndex(&isEndOfRow));
    groupOfRows.push_back(mergeState_);
    nextKeyIsEqual = next.second;
    if (isEndOfRow || sources.size() == maxOutputRows) {
      // if isEndOfRow, the memory in RowVector would be invalid after next pop,
      // so we should compute all saved rows.
      gatherAndUpdate();
    }
    next.first->pop();
    ++spillOutputRowCount_;
    if (!nextKeyIsEqual &&
        ((mergeRows_->numRows() >= maxOutputRows) ||
         (mergeRows_->usedBytes() + groupOfRows.size() * averageResultRowSize >=
          maxOutputBytes))) {
      // row count or memory reach the limit, compute and output result
      gatherAndUpdate();
      extractSpillResult(result, true);
      return true;
    }
  }
}

bool GroupingSet::mergeNextWithoutAggregates(
    int32_t maxOutputRows,
    const RowVectorPtr& result) {
  BOLT_CHECK_NOT_NULL(merge_);
  BOLT_CHECK(isDistinct());
  BOLT_CHECK_GT(numDistinctSpilledFiles_, 0);

  // We are looping over sorted rows produced by tree-of-losers. We logically
  // split the stream into runs of duplicate rows. As we process each run we
  // track whether one of the values coming from distinct streams, in which case
  // we should not produce a result from that run. Otherwise, we produce a
  // result at the end of the run (when we know for sure whether the run
  // contains a row from the distinct streams).
  //
  // NOTE: the distinct stream refers to the stream that contains the spilled
  // distinct hash table. A distinct stream contains rows which has already
  // been output as distinct before we trigger spilling. A distinct stream id is
  // less than 'numDistinctSpilledFiles_'.
  result->ensureWritable(SelectivityVector(maxOutputRows));
  std::vector<const RowVector*> sources;
  std::vector<vector_size_t> sourceIndice;
  bool isEndOfBatch{false};
  bool newDistinct{true};
  int32_t numOutputRows{0};

  auto popAndCopyIfNeeded =
      [&](bytedance::bolt::exec::SpillMergeStream* stream) {
        bool isEndOfBatch{false};
        stream->currentIndex(&isEndOfBatch);
        if (isEndOfBatch) {
          NanosecondTimer aggTimer(&stats_.aggOutputUpdateTimeNs);
          gatherCopy(
              result.get(),
              numOutputRows,
              sources.size(),
              sources,
              sourceIndice);
          numOutputRows += sources.size();
          sources.clear();
          sourceIndice.clear();
        }
        ++spillOutputRowCount_;
        stream->pop();
      };

  while (numOutputRows + sources.size() < maxOutputRows) {
    const auto next = merge_->nextWithEquals();
    auto* stream = next.first;
    if (stream == nullptr) {
      break;
    }
    if (stream->id() < numDistinctSpilledFiles_) {
      newDistinct = false;
    }
    if (next.second) {
      popAndCopyIfNeeded(stream);
      continue;
    }
    if (newDistinct) {
      sources.push_back(&stream->current());
      sourceIndice.push_back(stream->currentIndex());
    }
    popAndCopyIfNeeded(stream);
    newDistinct = true;
  }
  if (!sources.empty()) {
    NanosecondTimer aggTimer(&stats_.aggOutputUpdateTimeNs);
    gatherCopy(
        result.get(), numOutputRows, sources.size(), sources, sourceIndice);
    numOutputRows += sources.size();
  }
  result->resize(numOutputRows);
  return numOutputRows > 0;
}

void GroupingSet::ensureGroupIndices(vector_size_t size) {
  vector_size_t originSize = groupIndicesOfRows_.size();
  if (originSize < size) {
    groupIndicesOfRows_.resize(size);
    std::iota(
        groupIndicesOfRows_.begin() + originSize,
        groupIndicesOfRows_.end(),
        originSize);
  }
}

void GroupingSet::initializeRows(std::vector<char*> rows) {
  ensureGroupIndices(rows.size());
  for (auto& aggregate : aggregates_) {
    if (!aggregate.sortingKeys.empty()) {
      continue;
    }
    aggregate.function->initializeNewGroups(
        rows.data(),
        folly::Range<const vector_size_t*>(
            groupIndicesOfRows_.data(), rows.size()));
  }

  if (sortedAggregations_ != nullptr) {
    sortedAggregations_->initializeNewGroups(
        rows.data(),
        folly::Range<const vector_size_t*>(
            groupIndicesOfRows_.data(), rows.size()));
  }
}

void GroupingSet::extractSpillResult(
    const RowVectorPtr& result,
    bool excludeKey) {
  // distinct agg's result only has key, so skip if excludeKey
  std::vector<char*> rows(mergeRows_->numRows());
  RowContainerIterator iter;
  if (!rows.empty()) {
    mergeRows_->listRows(
        &iter, rows.size(), RowContainer::kUnlimited, rows.data());
  }
  extractGroups(
      folly::Range<char**>(rows.data(), rows.size()), result, excludeKey);
  mergeRows_->clear();
}

void GroupingSet::extractSpillResultInRowFormat(
    const RowVectorPtr& result,
    const std::vector<std::pair<int32_t, int32_t>>& ranges) {
  auto numRows = mergeRows_->numRows();
  if (numRows == 0) {
    return;
  }
  std::vector<char*> rows(numRows);
  RowContainerIterator iter;
  uint64_t totalRowSize = 0;
  mergeRows_->listRows(
      &iter, rows.size(), RowContainer::kUnlimited, rows.data());
  // RowContainer to serialized row format
  extractSpilledGroupsInRowFormat(
      folly::Range<char**>(rows.data(), numRows), result, ranges);
  mergeRows_->clear();
}

void GroupingSet::updateRows(
    const RowVectorPtr& input,
    std::vector<char*>& rows) {
  BOLT_CHECK_EQ(input->size(), rows.size());
  mergeSelection_.resizeFill(input->size());
  for (auto i = 0; i < aggregates_.size(); ++i) {
    if (!aggregates_[i].sortingKeys.empty()) {
      continue;
    }
    auto intermediate =
        std::vector<VectorPtr>({input->childAt(i + keyChannels_.size())});
    aggregates_[i].function->addIntermediateResults(
        rows.data(), mergeSelection_, intermediate, false);
  }

  if (sortedAggregations_ != nullptr) {
    const auto& vector =
        input->childAt(aggregates_.size() + keyChannels_.size());
    for (auto i = 0; i < input->size(); i++) {
      // TODO addSpillInput support batch input
      sortedAggregations_->addSingleGroupSpillInput(rows[i], vector, i);
    }
  }
}

void GroupingSet::abandonPartialAggregation() {
  BOLT_CHECK(!hasSpilled())

  abandonedPartialAggregation_ = true;
  allSupportToIntermediate_ = true;
  for (auto& aggregate : aggregates_) {
    if (!aggregate.function->supportsToIntermediate()) {
      allSupportToIntermediate_ = false;
    }
  }

  BOLT_CHECK_EQ(table_->rows()->numRows(), 0);
  intermediateRows_ = std::make_unique<RowContainer>(
      table_->rows()->keyTypes(),
      !ignoreNullKeys_,
      accumulators(true),
      std::vector<TypePtr>(),
      false,
      false,
      true,
      false,
      &pool_,
      table_->rows()->stringAllocatorShared());
  initializeAggregates(aggregates_, *intermediateRows_, true);
  table_.reset();
}

namespace {
// Recursive resize all children.

void recursiveResizeChildren(VectorPtr& vector, vector_size_t newSize) {
  BOLT_CHECK(vector.use_count() == 1);
  if (vector->typeKind() == TypeKind::ROW) {
    auto rowVector = vector->asUnchecked<RowVector>();
    for (auto& child : rowVector->children()) {
      recursiveResizeChildren(child, newSize);
    }
  }
  vector->resize(newSize);
}

} // namespace

void GroupingSet::toIntermediate(
    const RowVectorPtr& input,
    RowVectorPtr& result) {
  BOLT_CHECK(abandonedPartialAggregation_);
  BOLT_CHECK(result.use_count() == 1);
  if (!isRawInput_) {
    result = input;
    return;
  }
  auto numRows = input->size();
  activeRows_.resize(numRows);
  activeRows_.setAll();
  masks_.addInput(input, activeRows_);

  result->resize(numRows);
  if (!allSupportToIntermediate_) {
    intermediateGroups_.resize(numRows);
    for (auto i = 0; i < numRows; ++i) {
      intermediateGroups_[i] = intermediateRows_->newRow();
      intermediateRows_->setAllNull(intermediateGroups_[i]);
    }
    intermediateRowNumbers_.resize(numRows);
    std::iota(
        intermediateRowNumbers_.begin(), intermediateRowNumbers_.end(), 0);
  }

  for (auto i = 0; i < keyChannels_.size(); ++i) {
    result->childAt(i) = input->childAt(keyChannels_[i]);
  }
  // should update lazy info after children changes
  result->updateContainsLazyNotLoaded();
  for (auto i = 0; i < aggregates_.size(); ++i) {
    auto& function = aggregates_[i].function;
    auto& aggregateVector = result->childAt(i + keyChannels_.size());
    recursiveResizeChildren(aggregateVector, input->size());
    const auto& rows = getSelectivityVector(i);

    if (function->supportsToIntermediate()) {
      populateTempVectors(i, input);
      BOLT_DCHECK(aggregateVector);
      function->toIntermediate(rows, tempVectors_, aggregateVector);
      continue;
    }

    // Initialize all groups, even if we only need just one, to make sure bulk
    // free (intermediateRows_->eraseRows) is safe. It is not legal to free a
    // group that hasn't been initialized.
    function->initializeNewGroups(
        intermediateGroups_.data(), intermediateRowNumbers_);

    // Check if mask is false for all rows.
    if (!rows.hasSelections()) {
      // The aggregate produces its initial state for all
      // rows. Initialize one, then read the same data into each
      // element of flat result. This is most often a null but for
      // example count produces a zero, so we use the per-aggregate
      // functions.
      firstGroup_.resize(numRows);
      std::fill(firstGroup_.begin(), firstGroup_.end(), intermediateGroups_[0]);
      function->extractAccumulators(
          firstGroup_.data(), intermediateGroups_.size(), &aggregateVector);
      continue;
    }

    populateTempVectors(i, input);

    function->addRawInput(
        intermediateGroups_.data(), rows, tempVectors_, false);

    function->extractAccumulators(
        intermediateGroups_.data(),
        intermediateGroups_.size(),
        &aggregateVector);
  }
  if (intermediateRows_) {
    intermediateRows_->eraseRowsSkippingKeys(folly::Range<char**>(
        intermediateGroups_.data(), intermediateGroups_.size()));
    if (intermediateRows_->checkFree()) {
      intermediateRows_->stringAllocator().checkEmpty();
    }
  }

  // It's unnecessary to call function->clear() to reset the internal states of
  // aggregation functions because toIntermediate() is already called at the end
  // of HashAggregation::getOutput(). When toIntermediate() is called, the
  // aggregaiton function instances won't be reused after it returns.
  tempVectors_.clear();
}

std::optional<int64_t> GroupingSet::estimateOutputRowSize() const {
  if (table_ == nullptr) {
    return std::nullopt;
  }
  return table_->rows()->estimateRowSize();
}

void GroupingSet::convertCompositeInput(
    const std::vector<AggregateInfo>& extractAggregates,
    const CompositeRowVectorPtr& input,
    const RowVectorPtr& result) {
  if (!table_) {
    createHashTable();
  }
  auto* container = table_->rows();
  // initialize extractAggregates to make sure Aggregate::numNulls_ not 0 if it
  // is used
  if (!extractAggregatesInitialized_) {
    // fixedRowSize should be accuracy
    auto rowSize = container->fixedRowSize() + 16;
    char* rowPtr = static_cast<char*>(pool_.allocate(rowSize));
    const std::vector<char*> tmpGroups{rowPtr};
    const std::vector<vector_size_t> groupIndex{0};
    initializeAggregates(extractAggregates, *container, false);
    for (auto i = 0; i < extractAggregates.size(); ++i) {
      extractAggregates[i].function->initializeNewGroups(
          const_cast<char**>(tmpGroups.data()), groupIndex);
    }
    pool_.free(rowPtr, rowSize);
    extractAggregatesInitialized_ = true;
  }
  auto& inputRows = input->rawRows();
  for (auto& row : inputRows) {
    ContainerRow2RowSerde::deserialize(
        const_cast<char*&>(row), rowInfo_.value(), false);
  }

  for (auto i = 0; i < keyChannels_.size(); ++i) {
    rowToColumnVector(
        const_cast<char**>(inputRows.data()),
        inputRows.size(),
        container->columns().at(i),
        0,
        result->childAt(i));
  }
  for (auto i = 0; i < extractAggregates.size(); ++i) {
    auto& aggCol = result->childAt(i + keyChannels_.size());
    extractAggregates[i].function->extractAccumulators(
        const_cast<char**>(inputRows.data()), inputRows.size(), &aggCol);
  }
}

void GroupingSet::populateOutputValidColumns(
    SelectivityVector* selectedColumns) {
  BOLT_CHECK(selectedColumns != nullptr);
  selectedColumns->clearAll();
  if (table_) {
    auto totalKeys = table_->rows()->keyTypes().size();
    if (totalKeys) {
      selectedColumns->setValidRange(0, totalKeys, true);
      selectedColumns->updateBounds();
    }
  }
}

void GroupingSet::convertRowsFromContainerRows(
    const CompositeRowVectorPtr& compositeResult,
    folly::Range<char**> groups,
    const std::vector<std::pair<int32_t, int32_t>>& resultRanges) {
  const int32_t headerSize = sizeof(RowSizeType);
  int32_t actualTotalRowSize = 0, actualRowSize = 0;
  char *rowStart = nullptr, *newRow = nullptr;
  const auto& rowInfo = rowInfo_.value();

  auto totalRowSizeWithLen =
      rowInfo.getRowSize(groups) + headerSize * groups.size();
  auto* bufferStart = compositeResult->allocateRows(totalRowSizeWithLen);
  auto* bufferEnd = bufferStart + totalRowSizeWithLen;
  int32_t groupIndex = 0;
  for (const auto& [pos, len] : resultRanges) {
    RowInfoTracker tracker(compositeResult.get(), pos, len);
    for (auto i = 0; i < len; ++i) {
      char* row = groups[groupIndex + i];
      rowStart = compositeResult->newRow();
      newRow = rowStart + headerSize;

      // serialize row in RowContainer to newRow
      ContainerRow2RowSerde::serialize(row, newRow, bufferEnd, rowInfo);
      actualRowSize = newRow - rowStart - headerSize;
      *((RowSizeType*)rowStart) = actualRowSize;
      compositeResult->advance(actualRowSize + headerSize);
      actualTotalRowSize += actualRowSize + headerSize;
      compositeResult->store(pos + i, rowStart);
    }
    groupIndex += len;
  }

  BOLT_CHECK_EQ(groupIndex, groups.size());
  BOLT_CHECK_GE(
      totalRowSizeWithLen,
      actualTotalRowSize,
      "total allocated RowSize {} should greater equal than total actualRowSize {}",
      totalRowSizeWithLen,
      actualTotalRowSize);
}

void GroupingSet::convertRowsFromSpilledRows(
    const CompositeRowVectorPtr& compositeResult,
    std::vector<char*>& rows,
    int32_t resultOffset,
    const std::vector<int32_t>& rowSizeVec,
    const int64_t totalUniqueRowSize) {
  char* newRow = nullptr;
  const auto& rowInfo = rowInfo_.value();
  //  std::vector<RowSizeType> rowSizeVec(rows.size(), 0);
  // auto totalRowSize = ContainerRow2RowSerde::rowSize(rows, rowSizeVec,
  // rowInfo);
  auto totalRowSize = totalUniqueRowSize + sizeof(RowSizeType) * rows.size();

  compositeResult->allocateRows(totalRowSize);
  {
    RowInfoTracker tracker(compositeResult.get(), resultOffset, rows.size());
    for (auto i = 0; i < rows.size(); ++i) {
      const auto* row = rows[i];
      auto rowSize = rowSizeVec[i];
      newRow = compositeResult->newRow();
      *((RowSizeType*)(newRow)) = rowSize;
      simd::memcpy(newRow + sizeof(RowSizeType), row, rowSize);
      compositeResult->advance(rowSize + sizeof(RowSizeType));
      compositeResult->store(resultOffset + i, newRow);
    }
  }
}

} // namespace bytedance::bolt::exec
