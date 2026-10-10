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

  initProjection();
  initRollupAgg();

  aggregationNode_.reset();
}

bool HashAggregation::abandonPartialAggregationEarly(int64_t numOutput) const {
  BOLT_CHECK(isPartialOutput_ && !isGlobal_);
  if (groupingSet_->hasSpilled() || expandNode_) {
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
    if (expandNode_) {
      for (auto&& groupingSet : groupingSetsRollUp_) {
        groupingSet->setPreferPartialSpill(preferPartialSpill_);
      }
    }
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
  numInputRows_ += rollUpNumInputRows_;
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
  rollUpNumInputRows_ = 0;
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
    addRuntimeStat(
        std::string(HashAggregation::kAbandonedPartialAggregationRows),
        RuntimeCounter(input_->size()));
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
    auto currentOutput = getDistinctOutput();
    // Accumulate the generated output using append()
    if (currentOutput) {
      if (!accumulatedOutput_) {
        if (currentOutput->size() >= minOutputRows_) {
          return currentOutput;
        }
        accumulatedOutput_ =
            RowVector::createEmpty(currentOutput->type(), operatorCtx_->pool());
      }
      accumulatedOutput_->append(currentOutput.get());
    }

    // Handle accumulated output when no more input or memory pressure
    if (accumulatedOutput_ &&
        (finished_ || partialFull_ ||
         accumulatedOutput_->size() >= minOutputRows_ ||
         abandonedPartialAggregation_)) {
      auto result = std::move(accumulatedOutput_);
      resetPartialOutputIfNeed();
      return result;
    }
    return nullptr;
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

  if (expandNode_) {
    return getRollupOutput(
        maxOutputRows, queryConfig, beforeMemorySize, accumulatorRowSize);
  }

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
  if (finished_) {
    BOLT_CHECK_NULL(accumulatedOutput_);
  }
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
  if (expandNode_) {
    for (auto& groupingSet : groupingSetsRollUp_) {
      groupingSet.reset();
    }
  }
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

void HashAggregation::initProjection() {
  if (expandNode_ == nullptr) {
    return;
  }
  const auto& groupingKeys = aggregationNode_->groupingKeys();
  const auto numRows = groupingKeys.size();
  fieldProjections_.reserve(numRows);
  constantProjections_.reserve(numRows);
  const auto numColumns = numRows;
  std::vector<column_index_t> expandOutputChannels;
  const auto& expandOutputType = expandNode_->outputType();
  const auto& inputType = aggregationNode_->sources()[0]->outputType();
  std::vector<column_index_t> groupingKeyInputChannels;
  for (auto i = 0; i < groupingKeys.size(); ++i) {
    groupingKeyInputChannels.push_back(
        exprToChannel(groupingKeys[i].get(), inputType));
  }
  if (projectNode_) {
    std::unordered_map<column_index_t, column_index_t> channelMap;
    for (column_index_t i = 0; i < projectNode_->projections().size(); i++) {
      auto& projection = projectNode_->projections()[i];
      if (auto field = core::TypedExprs::asFieldAccess(projection)) {
        const auto& inputs = field->inputs();
        if (inputs.empty() ||
            (inputs.size() == 1 &&
             dynamic_cast<const core::InputTypedExpr*>(inputs[0].get()))) {
          const auto inputChannel =
              expandOutputType->getChildIdx(field->name());
          channelMap[i] = inputChannel;
        }
      }
    }
    for (auto col : groupingKeyInputChannels) {
      expandOutputChannels.push_back(channelMap[col]);
    }
  } else {
    for (auto col : groupingKeyInputChannels) {
      expandOutputChannels.push_back(col);
    }
  }
  for (const auto& rowProjections : expandNode_->projections()) {
    std::vector<column_index_t> rowProjection;
    rowProjection.reserve(numColumns);
    std::vector<std::shared_ptr<const core::ConstantTypedExpr>>
        constantProjection;
    constantProjection.reserve(numColumns);
    for (int i = 0; i < numColumns; i++) {
      const auto& columnProjection = rowProjections[expandOutputChannels[i]];
      if (auto field = core::TypedExprs::asFieldAccess(columnProjection)) {
        rowProjection.push_back(i);
        constantProjection.push_back(nullptr);
      } else if (
          auto constant = core::TypedExprs::asConstant(columnProjection)) {
        rowProjection.push_back(kConstantChannel);
        constantProjection.push_back(constant);
      } else {
        BOLT_USER_FAIL(
            "Expand operator doesn't support this expression. Only column references and constants are supported. {}",
            columnProjection->toString());
      }
    }
    fieldProjections_.emplace_back(std::move(rowProjection));
    constantProjections_.emplace_back(std::move(constantProjection));
  }
}

void HashAggregation::initRollupAgg() {
  if (expandNode_ == nullptr) {
    return;
  }
  groupingSetsRollUp_.resize(fieldProjections_.size());
  auto rollupAggregationNode = createIntermediateOrFinalAggregation(
      core::AggregationNode::Step::kIntermediate, aggregationNode_);
  const auto& inputType = outputType_;
  for (int groupIndex = 0; groupIndex < fieldProjections_.size();
       groupIndex++) {
    if (groupIndex == 0) {
      groupingSetsRollUp_[groupIndex] = groupingSet_;
      continue;
    }
    auto hashers =
        createVectorHashers(inputType, rollupAggregationNode->groupingKeys());
    auto numHashers = hashers.size();

    std::vector<column_index_t> preGroupedChannels;
    preGroupedChannels.reserve(rollupAggregationNode->preGroupedKeys().size());
    for (const auto& key : rollupAggregationNode->preGroupedKeys()) {
      auto channel = exprToChannel(key.get(), inputType);
      preGroupedChannels.push_back(channel);
    }

    std::shared_ptr<core::ExpressionEvaluator> expressionEvaluator;
    std::vector<AggregateInfo> aggregateInfos = toAggregateInfo(
        *rollupAggregationNode, *operatorCtx_, numHashers, expressionEvaluator);

    // Check that aggregate result type match the output type.
    for (auto i = 0; i < aggregateInfos.size(); i++) {
      const auto& aggResultType = aggregateInfos[i].function->resultType();
      const auto& expectedType = outputType_->childAt(numHashers + i);
      BOLT_CHECK(
          aggResultType->kindEquals(expectedType),
          "Unexpected result type for an aggregation: {}, expected {}, step {}",
          aggResultType->toString(),
          expectedType->toString(),
          core::AggregationNode::stepName(rollupAggregationNode->step()));
    }

    std::optional<column_index_t> groupIdChannel;
    if (rollupAggregationNode->groupId().has_value()) {
      groupIdChannel = outputType_->getChildIdxIfExists(
          rollupAggregationNode->groupId().value()->name());
      BOLT_CHECK(groupIdChannel.has_value());
    }

    groupingSetsRollUp_[groupIndex] = std::make_shared<GroupingSet>(
        inputType,
        std::move(hashers),
        std::move(preGroupedChannels),
        std::move(aggregateInfos),
        rollupAggregationNode->ignoreNullKeys(),
        isPartialOutput_,
        isRawInput(rollupAggregationNode->step()),
        rollupAggregationNode->globalGroupingSets(),
        groupIdChannel,
        spillConfig_.has_value() ? &spillConfig_.value() : nullptr,
        &nonReclaimableSection_,
        operatorCtx_.get());

    groupingSetsRollUp_[groupIndex]->setPreferPartialSpill(preferPartialSpill_);
    groupingSetsRollUp_[groupIndex]->setSupportRowBasedOutput(
        supportRowBasedOutput_);
    groupingSetsRollUp_[groupIndex]->setSupportUniqueRowOptimization(
        operatorCtx_->driverCtx()
            ->queryConfig()
            .isUniqueRowOptimizationEnabled());
  }
  rollupAggregationNode.reset();
}

RowVectorPtr HashAggregation::rollupProjection(
    RowVectorPtr input,
    int32_t rowIndex) {
  if (rowIndex >= fieldProjections_.size() || input == nullptr ||
      expandNode_ == nullptr) {
    return nullptr;
  }
  const auto numInput = input->size();

  const auto& rowProjection = fieldProjections_[rowIndex];
  const auto& constantProjection = constantProjections_[rowIndex];
  const auto numColumns = rowProjection.size();
  std::vector<VectorPtr> inputProjection(outputType_->size());
  for (int i = 0; i < input->childrenSize(); i++) {
    inputProjection[i] = input->childAt(i);
  }

  for (auto i = 0; i < numColumns; ++i) {
    if (rowProjection[i] == kConstantChannel) {
      const auto& constantExpr = constantProjection[i];
      if (constantExpr->value().isNull()) {
        // Add null column.
        inputProjection[i] = BaseVector::createNullConstant(
            outputType_->childAt(i), numInput, pool());
      } else {
        // Add constant column.
        inputProjection[i] = BaseVector::createConstant(
            constantExpr->type(), constantExpr->value(), numInput, pool());
      }
    } else {
      inputProjection[i] = input->childAt(rowProjection[i]);
    }
  }
  return std::make_shared<RowVector>(
      pool(), outputType_, nullptr, numInput, std::move(inputProjection));
}

RowVectorPtr HashAggregation::getRollupOutput(
    uint32_t maxOutputRows,
    const core::QueryConfig& queryConfig,
    int64_t beforeMemorySize,
    uint64_t accumulatorRowSize) {
  RowVectorPtr rollupInput;
  bool hasData = true;
  while (true) {
    hasData = groupingSet_->getOutput(
        maxOutputRows,
        queryConfig.preferredOutputBatchBytes(),
        resultIterator_,
        output_);
    if (hasData) {
      if (groupingSetIndex < groupingSetsRollUp_.size() - 1) {
        rollupInput = rollupProjection(output_, groupingSetIndex + 1);
        rollUpNumInputRows_ += rollupInput->size();
        groupingSetsRollUp_[groupingSetIndex + 1]->addInput(
            rollupInput, mayPushdown_);
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
    resultIterator_.reset();
    groupingSet_->resetTable();
    groupingSetIndex++;
    if (noMoreInput_ && groupingSetIndex < groupingSetsRollUp_.size()) {
      groupingSet_ = groupingSetsRollUp_[groupingSetIndex - 1];
      recordSpillReadStats();
      groupingSet_ = groupingSetsRollUp_[groupingSetIndex];
      recordSpillStats();
    }
    if (groupingSetIndex == groupingSetsRollUp_.size()) {
      if (noMoreInput_) {
        finished_ = true;
        groupingSet_ = groupingSetsRollUp_[groupingSetIndex - 1];
        recordSpillReadStats();
      }
      groupingSetIndex = 0;
      groupingSet_ = groupingSetsRollUp_[groupingSetIndex];
      resetPartialOutputIfNeed();
      pool()->release();
      return nullptr;
    }
    prepareOutput(maxOutputRows, supportRowBasedOutput_);
  }
}

std::shared_ptr<const core::AggregationNode>
HashAggregation::createIntermediateOrFinalAggregation(
    core::AggregationNode::Step step,
    std::shared_ptr<const core::AggregationNode> partialAggNode) {
  // Create intermediate or final aggregation using same grouping keys and same
  // aggregate function names.
  const auto& partialAggregates = partialAggNode->aggregates();
  const auto& groupingKeys = partialAggNode->groupingKeys();

  auto numAggregates = partialAggregates.size();
  auto numGroupingKeys = groupingKeys.size();

  std::vector<core::AggregationNode::Aggregate> aggregates;
  aggregates.reserve(numAggregates);
  auto partialOutputType = partialAggNode->outputType();
  for (auto i = 0; i < numAggregates; i++) {
    auto name = partialAggregates[i].call->name();
    auto rawInputs = partialAggregates[i].call->inputs();

    core::AggregationNode::Aggregate aggregate;
    for (auto& rawInput : rawInputs) {
      aggregate.rawInputTypes.push_back(rawInput->type());
    }
    auto inputIndex = numGroupingKeys + i;
    std::vector<core::TypedExprPtr> inputs = {
        std::make_shared<core::FieldAccessTypedExpr>(
            partialOutputType->childAt(inputIndex),
            partialOutputType->names()[inputIndex])};

    // Add lambda inputs.
    for (const auto& rawInput : rawInputs) {
      if (rawInput->type()->kind() == TypeKind::FUNCTION) {
        inputs.push_back(rawInput);
      }
    }

    aggregate.call = std::make_shared<core::CallTypedExpr>(
        partialAggregates[i].call->type(), std::move(inputs), name);
    aggregates.emplace_back(aggregate);
  }

  return std::make_shared<core::AggregationNode>(
      partialAggNode->id(),
      step,
      groupingKeys,
      partialAggNode->preGroupedKeys(),
      partialAggNode->aggregateNames(),
      aggregates,
      partialAggNode->ignoreNullKeys(),
      partialAggNode);
}
} // namespace bytedance::bolt::exec
