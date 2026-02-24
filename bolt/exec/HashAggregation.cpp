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

#include "bolt/exec/HashAggregation.h"
#include <folly/ScopeGuard.h>
#include <signal.h>
#include <iostream>
#include <optional>
#include "bolt/common/base/SuccinctPrinter.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/Operator.h"
#include "bolt/exec/OperatorUtils.h"
#include "bolt/exec/SortedAggregations.h"
#include "bolt/exec/Task.h"
#include "bolt/expression/Expr.h"
namespace bytedance::bolt::exec {

HashAggregation::HashAggregation(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::AggregationNode>& aggregationNode)
    : Operator(
          driverCtx,
          aggregationNode->outputType(),
          operatorId,
          aggregationNode->id(),
          aggregationNode->step() == core::AggregationNode::Step::kPartial
              ? "PartialAggregation"
              : "Aggregation",
          aggregationNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId)
              : std::nullopt),
      aggregationNode_(aggregationNode),
      isPartialOutput_(isPartialOutput(aggregationNode->step())),
      isPartialStep_(
          aggregationNode->step() == core::AggregationNode::Step::kPartial),
      isGlobal_(aggregationNode->groupingKeys().empty()),
      isDistinct_(!isGlobal_ && aggregationNode->aggregates().empty()),
      maxExtendedPartialAggregationMemoryUsage_(
          driverCtx->queryConfig().maxExtendedPartialAggregationMemoryUsage()),
      abandonPartialAggregationMinRows_(
          driverCtx->queryConfig().abandonPartialAggregationMinRows()),
      abandonPartialAggregationMinPct_(
          driverCtx->queryConfig().abandonPartialAggregationMinPct()),
      abandonPartialAggregationMinFinalPct_(
          driverCtx->queryConfig().abandonPartialAggregationMinFinalPct()),
      partialAggregationSpillMaxPct_(
          driverCtx->queryConfig().partialAggregationSpillMaxPct()),
      maxPartialAggregationMemoryUsage_(
          driverCtx->queryConfig().maxPartialAggregationMemoryUsage()),
      preferPartialSpill_(
          driverCtx->queryConfig().preferPartialAggregationSpill()),
      skippedDataSizeThreshold_(
          driverCtx->queryConfig().adaptiveSkippedDataSizeThreshold()),
      minOutputRows_((driverCtx->queryConfig().minOutputBatchRows())) {
  if (canSpill()) {
    BOLT_CHECK(
        partialAggregationSpillMaxPct_ <= abandonPartialAggregationMinPct_ &&
            partialAggregationSpillMaxPct_ <=
                abandonPartialAggregationMinFinalPct_,
        "partialAggregationSpillMaxPct_ = {}, abandonPartialAggregationMinPct_ = {}, abandonPartialAggregationMinFinalPct_ = {}",
        partialAggregationSpillMaxPct_,
        abandonPartialAggregationMinPct_,
        abandonPartialAggregationMinFinalPct_);
    adaptiveAdjustment_ = skippedDataSizeThreshold_ > 0;
  }
  this->setRuntimeMetric(
      OperatorMetricKey::kCanUsedToEstimateHashBuildPartitionNum, "true");
  this->setRuntimeMetric(
      OperatorMetricKey::kTotalRowCount, folly::to<std::string>(0));
  this->setRuntimeMetric(
      OperatorMetricKey::kHasBeenProcessedRowCount, folly::to<std::string>(0));
}

void HashAggregation::initialize() {
  Operator::initialize();

  BOLT_CHECK(pool()->trackUsage());

  auto inputType = aggregationNode_->sources()[0]->outputType();

  auto hashers =
      createVectorHashers(inputType, aggregationNode_->groupingKeys());
  auto numHashers = hashers.size();

  std::vector<column_index_t> preGroupedChannels;
  preGroupedChannels.reserve(aggregationNode_->preGroupedKeys().size());
  for (const auto& key : aggregationNode_->preGroupedKeys()) {
    auto channel = exprToChannel(key.get(), inputType);
    preGroupedChannels.push_back(channel);
  }

  std::shared_ptr<core::ExpressionEvaluator> expressionEvaluator;
  std::vector<AggregateInfo> aggregateInfos = toAggregateInfo(
      *aggregationNode_, *operatorCtx_, numHashers, expressionEvaluator);
  for (int i = 0; i < aggregateInfos.size(); i++) {
    if (aggregationNode_->aggregates()[i].call->name().find("vid_split") !=
        std::string::npos) {
      containsVidSplit_ = true;
      adaptiveAdjustment_ = false;
      break;
    }
  }

  bool canUseRowBasedSpill = true;
  bool hasDistinctAgg = false;
  size_t accumulatorSize = 0;
  int alignment = 1;
  // Check that aggregate result type match the output type.
  for (auto i = 0; i < aggregateInfos.size(); i++) {
    accumulatorSize += aggregateInfos[i].function->accumulatorFixedWidthSize();
    const auto& aggResultType = aggregateInfos[i].function->resultType();
    const auto& expectedType = outputType_->childAt(numHashers + i);
    BOLT_CHECK(
        aggResultType->kindEquals(expectedType),
        "Unexpected result type for an aggregation: {}, expected {}, step {}",
        aggResultType->toString(),
        expectedType->toString(),
        core::AggregationNode::stepName(aggregationNode_->step()));
    // only can used row based spill for fix accumulate and no sorting keys
    canUseRowBasedSpill &=
        (aggregateInfos[i].function->isFixedSize() ||
         aggregateInfos[i].function->supportAccumulatorSerde()) &&
        aggregateInfos[i].sortingKeys.empty();
    alignment = RowContainer::combineAlignments(
        aggregateInfos[i].function->accumulatorAlignmentSize(), alignment);
    hasDistinctAgg |= aggregateInfos[i].distinct;
  }
  const auto& queryConf = operatorCtx_->driverCtx()->queryConfig();
  if (canUseRowBasedSpill && spillConfig_) {
    auto rowbasedSpillMode = queryConf.rowBasedSpillMode();
    LOG(INFO) << "Operator " << name()
              << " can use row based spill, spill mode: " << rowbasedSpillMode;
    spillConfig_ = operatorCtx_->driverCtx()->makeSpillConfig(
        operatorCtx_->operatorId(), rowbasedSpillMode);
  }

  addRuntimeStat("isDistinctAggregation", RuntimeCounter(isDistinct_));

  for (auto i = 0; i < hashers.size(); ++i) {
    identityProjections_.emplace_back(hashers[i]->channel(), i);
  }
  std::optional<column_index_t> groupIdChannel;
  if (aggregationNode_->groupId().has_value()) {
    groupIdChannel = outputType_->getChildIdxIfExists(
        aggregationNode_->groupId().value()->name());
    BOLT_CHECK(groupIdChannel.has_value());
  }

  acceptCompositeVectorInput_ = canUseRowBasedSpill && !isGlobal_ &&
      !hasDistinctAgg && (alignment == 1) && !isDistinct_ && (alignment == 1) &&
      queryConf.isHashAggregationCompositeOutputEnabled() &&
      (aggregateInfos.size() >=
       hashers.size() * queryConf.hashAggregationCompositeAccumulatorRatio());

  supportRowBasedOutput_ = isPartialStep_ && acceptCompositeVectorInput_;

  addRuntimeStat(
      "aggregationOutputCompositeVector",
      RuntimeCounter(supportRowBasedOutput_));
  groupingSet_ = std::make_unique<GroupingSet>(
      inputType,
      std::move(hashers),
      std::move(preGroupedChannels),
      std::move(aggregateInfos),
      aggregationNode_->ignoreNullKeys(),
      isPartialOutput_,
      isRawInput(aggregationNode_->step()),
      aggregationNode_->globalGroupingSets(),
      groupIdChannel,
      spillConfig_.has_value() ? &spillConfig_.value() : nullptr,
      &nonReclaimableSection_,
      operatorCtx_.get());

  groupingSet_->setPreferPartialSpill(preferPartialSpill_);
  groupingSet_->setSupportRowBasedOutput(supportRowBasedOutput_);
  groupingSet_->setSupportUniqueRowOptimization(
      operatorCtx_->driverCtx()
          ->queryConfig()
          .isUniqueRowOptimizationEnabled());

  if (!isPartialStep_ && acceptCompositeVectorInput_) {
    std::shared_ptr<core::ExpressionEvaluator> evaluator;
    aggregatesForExtractColumns_ = toAggregateInfo(
        *aggregationNode_, *operatorCtx_, numHashers, evaluator);
  }

  constexpr size_t maxBatchMem = 1024 * 1024;
  maxInputBatchCount_ = isDistinct_
      ? 4096UL
      : std::max(
            (accumulatorSize ? maxBatchMem / accumulatorSize : 0UL), 512UL);
  // aligned with 8 to avoid copy null buffer
  maxInputBatchCount_ = maxInputBatchCount_ / 8 * 8;
  LOG(INFO) << name() << " initialized, "
            << "MaxInputBatchCount: " << maxInputBatchCount_
            << ", supportRowBasedOutput_ = " << supportRowBasedOutput_
            << ", outputType_ = " << outputType_->toString()
            << ", isPartialStep = " << isPartialStep_;

  aggregationNode_.reset();
}

bool HashAggregation::abandonPartialAggregationEarly(int64_t numOutput) const {
  BOLT_CHECK(isPartialOutput_ && !isGlobal_);
  if (groupingSet_->hasSpilled()) {
    // Once spilling kicked in, disable the abandoning code path.
    // This is because spilling only enabled when output/input is small,
    // and abandoning in this case will cause data expansion in shuffle
    return false;
  }
  return numInputRows_ > abandonPartialAggregationMinRows_ &&
      100 * numOutput / numInputRows_ >= abandonPartialAggregationMinPct_;
}

bool HashAggregation::preferPartialSpill(
    int64_t numOutput,
    bool ignoreMinSampleRows) {
  // spill disabled
  if (!isPartialStep_ || !canSpill()) {
    return false;
  }

  if (preferPartialSpill_) {
    return true;
  }

  // reuse abandonPartialAggregationMinRows_ as minimum‌ sampling row number
  preferPartialSpill_ =
      ((numInputRows_ > abandonPartialAggregationMinRows_ ||
        ignoreMinSampleRows) &&
       100 * numOutput / numInputRows_ <= partialAggregationSpillMaxPct_);
  if (preferPartialSpill_) {
    groupingSet_->setPreferPartialSpill(preferPartialSpill_);
  }
  return preferPartialSpill_;
}

void HashAggregation::addInput(RowVectorPtr input) {
  if (!pushdownChecked_) {
    mayPushdown_ = operatorCtx_->driver()->mayPushdownAggregation(this);
    pushdownChecked_ = true;
  }
  if (abandonedPartialAggregation_) {
    input_ = input;
    numInputRows_ += input->size();
    addRuntimeStat("abandonedVectors", RuntimeCounter(1));
    addRuntimeStat("abandonedRows", RuntimeCounter(input_->size()));
    return;
  }
  if (!isPartialStep_ && acceptCompositeVectorInput_) {
    CompositeRowVectorPtr compositeInput =
        std::dynamic_pointer_cast<CompositeRowVector>(input);
    if (compositeInput) {
      if (convertedInput_) {
        VectorPtr converted = std::move(convertedInput_);
        BaseVector::prepareForReuse(converted, input->size());
        convertedInput_ = std::static_pointer_cast<RowVector>(converted);
      } else {
        convertedInput_ =
            BaseVector::create<RowVector>(input->type(), input->size(), pool());
      }
      groupingSet_->convertCompositeInput(
          aggregatesForExtractColumns_, compositeInput, convertedInput_);
      input = convertedInput_;
    }
  }
  // Reset tracking between batches to prevent memory buildup
  groupingSet_->resetDistinctNewGroups();
  if (input->size() > maxInputBatchCount_) {
    if (isLazyNotLoaded(*input)) {
      input->loadedVector();
    }
    for (int64_t i = 0; i < input->size(); i += maxInputBatchCount_) {
      auto length = std::min(input->size() - i, maxInputBatchCount_);
      auto inputSlice =
          std::dynamic_pointer_cast<RowVector>(input->slice(i, length));
      groupingSet_->addInput(inputSlice, mayPushdown_);

      if (isDistinct_) {
        // Only adjust indices for distinct case needing cross-batch tracking
        groupingSet_->putDistinctNewGroupsIndices(i);
      }
    }
  } else {
    groupingSet_->addInput(input, mayPushdown_);
    if (isDistinct_) {
      // Only adjust indices for distinct case needing cross-batch tracking
      groupingSet_->putDistinctNewGroupsIndices(0);
    }
  }
  numInputRows_ += input->size();

  updateRuntimeStats();

  // NOTE: we should not trigger partial output flush in case of global
  // aggregation as the final aggregator will handle it the same way as the
  // partial aggregator. Hence, we have to use more memory anyway.
  const bool abandonPartialEarly = isPartialStep_ && !isGlobal_ &&
      abandonPartialAggregationEarly(groupingSet_->numDistinct());
  if (isPartialStep_ && !isGlobal_ &&
      !preferPartialSpill(groupingSet_->numDistinct(), false) &&
      (abandonPartialEarly ||
       groupingSet_->isPartialFull(maxPartialAggregationMemoryUsage_))) {
    // recalculate preferPartialSpill ignoring min sampling rows
    if (abandonPartialEarly ||
        !preferPartialSpill(groupingSet_->numDistinct(), true)) {
      partialFull_ = true;
    }
  }

  if (isDistinct_) {
    newDistincts_ = !groupingSet_->hashLookup().distinctNewGroups.empty();

    if (newDistincts_) {
      // Save input to use for output in getOutput().
      input_ = input;
    } else {
      // If no new distinct groups (meaning we don't have anything to output),
      // then we need to ensure we 'need input'. For that we need to reset
      // the 'partial full' flag.
      partialFull_ = false;
    }
  }
}

void HashAggregation::updateRuntimeStats() {
  // Report range sizes and number of distinct values for the group-by keys.
  const auto& hashers = groupingSet_->hashLookup().hashers;
  uint64_t asRange;
  uint64_t asDistinct;
  const auto hashTableStats = groupingSet_->hashTableStats();

  auto lockedStats = stats_.wlock();
  auto& runtimeStats = lockedStats->runtimeStats;

  for (auto i = 0; i < hashers.size(); i++) {
    hashers[i]->cardinality(0, asRange, asDistinct);
    if (asRange != VectorHasher::kRangeTooLarge) {
      runtimeStats[fmt::format("rangeKey{}", i)] = RuntimeMetric(asRange);
    }
    if (asDistinct != VectorHasher::kRangeTooLarge) {
      runtimeStats[fmt::format("distinctKey{}", i)] = RuntimeMetric(asDistinct);
    }
  }

  runtimeStats["hashtable.capacity"] = RuntimeMetric(hashTableStats.capacity);
  runtimeStats["hashtable.numRehashes"] =
      RuntimeMetric(hashTableStats.numRehashes);
  runtimeStats["hashtable.numDistinct"] =
      RuntimeMetric(hashTableStats.numDistinct);
  runtimeStats["hashtable.numTombstones"] =
      RuntimeMetric(hashTableStats.numTombstones);
}

void HashAggregation::recordSpillStats() {
  auto spillStatsOr = groupingSet_->spilledStats();
  if (spillStatsOr.has_value()) {
    Operator::recordSpillStats(spillStatsOr.value());
  }
}

void HashAggregation::recordSpillReadStats() {
  auto spillReadStatsOr = groupingSet_->spillReadStats();
  if (spillReadStatsOr.has_value()) {
    Operator::recordSpillReadStats(spillReadStatsOr.value());
  }
}

void HashAggregation::recordRuntimeMetrics() {
  if (groupingSet_ != nullptr && groupingSet_->hasSpilled()) {
    this->setRuntimeMetric(
        OperatorMetricKey::kTotalRowCount,
        folly::to<std::string>(groupingSet_->totalSpillRowCount()));
    this->setRuntimeMetric(
        OperatorMetricKey::kHasBeenProcessedRowCount,
        folly::to<std::string>(groupingSet_->outputSpillRowCount()));
    return;
  }
  this->setRuntimeMetric(
      OperatorMetricKey::kTotalRowCount,
      folly::to<std::string>(groupingSet_->numDistinct()));
  this->setRuntimeMetric(
      OperatorMetricKey::kHasBeenProcessedRowCount,
      folly::to<std::string>(numOutputRows_));
}

void HashAggregation::prepareOutput(
    vector_size_t size,
    bool isCompositeOutput) {
  if (!isCompositeOutput) {
    if (output_ && !RowVector::isComposite(output_)) {
      VectorPtr output = std::move(output_);
      BaseVector::prepareForReuse(output, size);
      output_ = std::static_pointer_cast<RowVector>(output);
    } else {
      output_ = std::static_pointer_cast<RowVector>(
          BaseVector::create(outputType_, size, pool()));
    }
  } else {
    std::unique_ptr<SelectivityVector> validColumns =
        std::make_unique<SelectivityVector>(outputType_->size(), false);
    groupingSet_->populateOutputValidColumns(validColumns.get());
    output_ = CompositeRowVector::create(
        outputType_, size, pool(), std::move(validColumns));
  }
}

void HashAggregation::resetPartialOutputIfNeed() {
  if (!partialFull_) {
    return;
  }
  BOLT_CHECK(
      !isGlobal_ && (groupingSet_ == nullptr || !groupingSet_->hasSpilled()));
  const double aggregationPct =
      numOutputRows_ == 0 ? 0 : (numOutputRows_ * 1.0) / numInputRows_ * 100;
  {
    auto lockedStats = stats_.wlock();
    lockedStats->addRuntimeStat(
        "flushRowCount", RuntimeCounter(numOutputRows_));
    lockedStats->addRuntimeStat("flushTimes", RuntimeCounter(1));
    lockedStats->addRuntimeStat(
        "partialAggregationPct", RuntimeCounter(aggregationPct));
  }

  groupingSet_->resetTable();
  partialFull_ = false;
  totalInputRows_ += numInputRows_;
  if (!finished_) {
    maybeIncreasePartialAggregationMemoryUsage(aggregationPct);
  }
  numOutputRows_ = 0;
  numInputRows_ = 0;
}

void HashAggregation::maybeIncreasePartialAggregationMemoryUsage(
    double aggregationPct) {
  // If more than this many are unique at full memory, give up on partial agg.
  BOLT_DCHECK(isPartialOutput_);
  // If size is at max and there still is not enough reduction, abandon partial
  // aggregation.
  auto calShouldAbandon = [&]() {
    return abandonPartialAggregationEarly(numOutputRows_) ||
        (aggregationPct > abandonPartialAggregationMinFinalPct_ &&
         maxPartialAggregationMemoryUsage_ >=
             maxExtendedPartialAggregationMemoryUsage_);
  };

  bool shouldAbandon = calShouldAbandon();
  if (shouldAbandon && adaptiveAdjustment_) {
    uint64_t totalRowCnt{0}, processedRowCnt{0};
    operatorCtx_->traverseOpToGetRowCount(totalRowCnt, processedRowCnt);
    if (totalRowCnt > 0 && processedRowCnt > 0) {
      // left unprocessed rows * (input->output expansion/filter ratio) * size
      auto calculatedSkippedSize = avgRowSize_ *
          (totalRowCnt - processedRowCnt) * totalInputRows_ / processedRowCnt;
      if (calculatedSkippedSize >= skippedDataSizeThreshold_) {
        *const_cast<int32_t*>(&abandonPartialAggregationMinPct_) =
            std::max((int32_t)abandonPartialAggregationMinPct_, 95);
        *const_cast<int32_t*>(&abandonPartialAggregationMinFinalPct_) =
            std::max((int32_t)abandonPartialAggregationMinFinalPct_, 90);
        *const_cast<int32_t*>(&partialAggregationSpillMaxPct_) =
            std::max((int32_t)partialAggregationSpillMaxPct_, 85);
        adaptiveAdjustment_ = false;
        // recalculate abandon or not
        shouldAbandon = calShouldAbandon();
      }
    }
  }
  if (shouldAbandon) {
    groupingSet_->abandonPartialAggregation();
    pool()->release();
    addRuntimeStat("abandonedPartialAggregation", RuntimeCounter(1));
    abandonedPartialAggregation_ = true;
    LOG(INFO) << __FUNCTION__ << " numInputRows_ = " << numInputRows_
              << ", numOutputRows_ = " << numOutputRows_
              << ", aggregationPct = " << aggregationPct
              << ", abandonPartialAggregationMinPct_ = "
              << abandonPartialAggregationMinPct_
              << ", abandonPartialAggregationMinFinalPct_ = "
              << abandonPartialAggregationMinFinalPct_
              << ", partialAggregationSpillMaxPct_ = "
              << partialAggregationSpillMaxPct_
              << ", shouldAbandon = " << shouldAbandon;
    return;
  }
  const int64_t extendedPartialAggregationMemoryUsage = std::min(
      maxPartialAggregationMemoryUsage_ * 2,
      maxExtendedPartialAggregationMemoryUsage_);
  // Calculate the memory to reserve to bump up the aggregation buffer size. If
  // the memory reservation below succeeds, it ensures the partial aggregator
  // can allocate that much memory in next run.
  const int64_t memoryToReserve = std::max<int64_t>(
      0,
      extendedPartialAggregationMemoryUsage - groupingSet_->allocatedBytes());
  if (!pool()->maybeReserve(memoryToReserve)) {
    return;
  }
  // Update the aggregation memory usage size limit on memory reservation
  // success.
  maxPartialAggregationMemoryUsage_ = extendedPartialAggregationMemoryUsage;
  addRuntimeStat(
      "maxExtendedPartialAggregationMemoryUsage",
      RuntimeCounter(
          maxPartialAggregationMemoryUsage_, RuntimeCounter::Unit::kBytes));
}

void triggerSegfault() {
  int* x = nullptr;
  while (*x) {
    (*x)++;
    x++;
  }
}

RowVectorPtr HashAggregation::getOutput() {
  if (BOLT_TEST_VALUE_ENABLED()) {
    bool injectSegfault = false;
    BOLT_TEST_ADJUST(
        "bytedance::bolt::exec::HashAggregation::getOutput", &injectSegfault);
    if (injectSegfault) {
      triggerSegfault();
    }
  }
  if (finished_) {
    input_ = nullptr;
    return nullptr;
  }
  if (abandonedPartialAggregation_) {
    if (noMoreInput_) {
      finished_ = true;
    }
    if (!input_) {
      return nullptr;
    }
    prepareOutput(input_->size(), false);
    groupingSet_->toIntermediate(input_, output_);
    numOutputRows_ += input_->size();
    input_ = nullptr;
    return output_;
  }

  // Produce results if one of the following is true:
  // - received no-more-input message;
  // - partial aggregation reached memory limit;
  // - distinct aggregation has new keys;
  // - running in partial streaming mode and have some output ready.
  if (!noMoreInput_ && !partialFull_ && !newDistincts_ &&
      !groupingSet_->hasOutput()) {
    input_ = nullptr;
    return nullptr;
  }

  if (isDistinct_) {
    return getDistinctOutput();
  }

  const auto& queryConfig = operatorCtx_->driverCtx()->queryConfig();
  auto maxOutputRows = isGlobal_ ? 1 : outputBatchRows(estimatedOutputRowSize_);
  // if aggregate function is vid_split_agg_metrics, set output rows up to 1k
  if (!isGlobal_ && containsVidSplit_ && numOutputRows_ == 0) {
    maxOutputRows = 5;
  }

  auto beforeMemorySize = pool()->currentBytes();
  auto accumulatorRowSize = groupingSet_->estimateOutputRowSize().value_or(0);
  avgRowSize_ = std::max(accumulatorRowSize, avgRowSize_);

  // Reuse output vectors if possible.
  prepareOutput(maxOutputRows, supportRowBasedOutput_);

  const bool hasData = groupingSet_->getOutput(
      maxOutputRows,
      queryConfig.preferredOutputBatchBytes(),
      resultIterator_,
      output_);
  if (!hasData) {
    resultIterator_.reset();
    if (noMoreInput_) {
      finished_ = true;
      recordSpillReadStats();
    }
    resetPartialOutputIfNeed();
    pool()->release();
    return nullptr;
  }
  numOutputRows_ += output_->size();
  recordRuntimeMetrics();
  auto afterMemorySize = pool()->currentBytes();
  if (containsVidSplit_) {
    int64_t oldEstimatedRowSize = estimatedOutputRowSize_.value_or(0);
    int64_t newEstimatedRowSize =
        (afterMemorySize - beforeMemorySize) / output_->size() +
        accumulatorRowSize;
    estimatedOutputRowSize_ =
        std::max(oldEstimatedRowSize, newEstimatedRowSize);
  }

  return output_;
}

RowVectorPtr HashAggregation::getDistinctOutput() {
  BOLT_CHECK(isDistinct_);
  BOLT_CHECK(!finished_);

  if (newDistincts_) {
    BOLT_CHECK_NOT_NULL(input_);

    const auto& distinctNewGroups =
        groupingSet_->hashLookup().distinctNewGroups;

    // Accumulate results from all saved batches
    const auto size = distinctNewGroups.size();
    BufferPtr indices = allocateIndices(size, operatorCtx_->pool());
    auto indicesPtr = indices->asMutable<vector_size_t>();

    // Copy all batches into indices buffer
    std::copy(distinctNewGroups.begin(), distinctNewGroups.end(), indicesPtr);

    newDistincts_ = false;
    auto output = fillOutput(size, indices);
    numOutputRows_ += size;

    recordRuntimeMetrics();

    // Drop reference to input_ to make it singly-referenced at the producer and
    // allow for memory reuse.
    input_ = nullptr;

    // Clear tracking after processing to prevent memory retention
    groupingSet_->resetDistinctNewGroups();
    resetPartialOutputIfNeed();
    return output;
  }
  BOLT_CHECK(!newDistincts_);

  auto outputGuard = folly::makeGuard([&]() { recordRuntimeMetrics(); });
  if (!groupingSet_->hasSpilled()) {
    if (noMoreInput_) {
      finished_ = true;
      if (auto numRows = groupingSet_->numDefaultGlobalGroupingSetRows()) {
        prepareOutput(numRows.value(), false);
        if (groupingSet_->getDefaultGlobalGroupingSetOutput(
                resultIterator_, output_)) {
          numOutputRows_ += output_->size();
          return output_;
        }
      }
    }
    return nullptr;
  }

  if (!noMoreInput_) {
    return nullptr;
  }

  const auto& queryConfig = operatorCtx_->driverCtx()->queryConfig();
  const auto maxOutputRows = outputBatchRows(estimatedOutputRowSize_);
  prepareOutput(maxOutputRows, false);
  if (!groupingSet_->getOutput(
          maxOutputRows,
          queryConfig.preferredOutputBatchBytes(),
          resultIterator_,
          output_)) {
    finished_ = true;
    return nullptr;
  }
  numOutputRows_ += output_->size();
  return output_;
}

void HashAggregation::noMoreInput() {
  updateEstimatedOutputRowSize();
  groupingSet_->noMoreInput();
  convertedInput_ = nullptr;
  Operator::noMoreInput();
  recordSpillStats();
  // Release the extra reserved memory right after processing all the inputs.
  pool()->release();
}

bool HashAggregation::isFinished() {
  return finished_;
}

void HashAggregation::reclaim(
    uint64_t targetBytes,
    memory::MemoryReclaimer::Stats& stats) {
  BOLT_CHECK(canReclaim());
  BOLT_CHECK(!nonReclaimableSection_);

  if (groupingSet_ == nullptr) {
    return;
  }

  // for partial aggregation, if preferPartialSpill_ is false, do not spill
  if (isPartialStep_ && !preferPartialSpill_) {
    return;
  }

  updateEstimatedOutputRowSize();

  if (noMoreInput_) {
    if (groupingSet_->hasSpilled()) {
      LOG(WARNING)
          << "Can't reclaim from aggregation operator which has spilled and is under output processing, pool "
          << pool()->name()
          << ", memory usage: " << succinctBytes(pool()->currentBytes())
          << ", reservation: " << succinctBytes(pool()->reservedBytes());
      return;
    }
    if (isDistinct_) {
      ++stats.numNonReclaimableAttempts;
      LOG(WARNING)
          << "Can't reclaim from distinct aggregation operator which is under output processing, pool "
          << pool()->name()
          << ", memory usage: " << succinctBytes(pool()->currentBytes())
          << ", reservation: " << succinctBytes(pool()->reservedBytes());
      // Since we have seen all the input, we can safely reset the hash table.
      groupingSet_->resetTable();
      // Release the minimum reserved memory.
      pool()->release();
      return;
    }

    // Spill all the rows starting from the next output row pointed by
    // 'resultIterator_'.
    groupingSet_->spill(resultIterator_);
    // NOTE: we will only spill once during the output processing stage so
    // record stats here.
    recordSpillStats();
  } else {
    BOLT_CHECK(
        !isDistinct_ || (input_ == nullptr && !newDistincts_),
        "Unexpected input when spill distinct aggregation");
    // TODO: support fine-grain disk spilling based on 'targetBytes' after
    // having row container memory compaction support later.
    groupingSet_->spill();
  }
  BOLT_CHECK_EQ(groupingSet_->numRows(), 0);
  BOLT_CHECK_EQ(groupingSet_->numDistinct(), 0);
  // Release the minimum reserved memory.
  pool()->release();
}

void HashAggregation::close() {
  Operator::close();
  if (groupingSet_) {
    Operator::recordGroupingSetStats(groupingSet_->getRuntimeStats());
    groupingSet_.reset();
  }
  output_ = nullptr;
}

void HashAggregation::updateEstimatedOutputRowSize() {
  const auto optionalRowSize = groupingSet_->estimateOutputRowSize();
  if (!optionalRowSize.has_value()) {
    return;
  }

  const auto rowSize = optionalRowSize.value();

  if (!estimatedOutputRowSize_.has_value()) {
    estimatedOutputRowSize_ = rowSize;
  } else if (rowSize > estimatedOutputRowSize_.value()) {
    estimatedOutputRowSize_ = rowSize;
  }
}
} // namespace bytedance::bolt::exec
