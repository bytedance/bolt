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

#include "bolt/exec/Operator.h"
#include <common/base/RuntimeMetrics.h>
#include <common/base/SortStat.h>
#include <common/base/SpillStats.h>
#include <common/base/WindowStat.h>
#include <exec/OperatorMetric.h>
#include <cstdint>
#include "bolt/common/base/Counters.h"
#include "bolt/common/base/HashBuildSpillStats.h"
#include "bolt/common/base/StatsReporter.h"
#include "bolt/common/base/SuccinctPrinter.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/exec/Driver.h"
#include "bolt/exec/OperatorUtils.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/TraceUtil.h"
#include "bolt/expression/Expr.h"

using bytedance::bolt::common::testutil::TestValue;
namespace bytedance::bolt::exec {

OperatorCtx::OperatorCtx(
    DriverCtx* driverCtx,
    const core::PlanNodeId& planNodeId,
    int32_t operatorId,
    const std::string& operatorType)
    : driverCtx_(driverCtx),
      planNodeId_(planNodeId),
      operatorId_(operatorId),
      operatorType_(operatorType),
      pool_(driverCtx_->addOperatorPool(planNodeId, operatorType)) {}

core::ExecCtx* OperatorCtx::execCtx() const {
  if (!execCtx_) {
    execCtx_ = std::make_unique<core::ExecCtx>(
        pool_, driverCtx_->task->queryCtx().get());
  }
  return execCtx_.get();
}

std::shared_ptr<connector::ConnectorQueryCtx>
OperatorCtx::createConnectorQueryCtx(
    const std::string& connectorId,
    const std::string& planNodeId,
    memory::MemoryPool* connectorPool,
    const common::SpillConfig* spillConfig,
    connector::AsyncThreadCtx* const asyncThreadCtx) const {
  return std::make_shared<connector::ConnectorQueryCtx>(
      pool_,
      connectorPool,
      driverCtx_->task->queryCtx()->connectorSessionProperties(connectorId),
      spillConfig,
      asyncThreadCtx,
      std::make_unique<SimpleExpressionEvaluator>(
          execCtx()->queryCtx(), execCtx()->pool()),
      driverCtx_->task->queryCtx()->cache(),
      driverCtx_->task->queryCtx()->queryId(),
      taskId(),
      planNodeId,
      driverCtx_->driverId);
}

Operator::Operator(
    DriverCtx* driverCtx,
    RowTypePtr outputType,
    int32_t operatorId,
    std::string planNodeId,
    std::string operatorType,
    std::optional<common::SpillConfig> spillConfig)
    : operatorCtx_(std::make_unique<OperatorCtx>(
          driverCtx,
          planNodeId,
          operatorId,
          operatorType)),
      outputType_(std::move(outputType)),
      spillConfig_(std::move(spillConfig)),
      stats_(OperatorStats{
          operatorId,
          driverCtx->pipelineId,
          std::move(planNodeId),
          std::move(operatorType),
          driverCtx->isAbtestControlGroup,
          driverCtx->queryConfig().getAbtestMetrics()}) {}

void Operator::maybeSetReclaimer() {
  BOLT_CHECK_NULL(pool()->reclaimer());

  if (pool()->parent()->reclaimer() == nullptr) {
    return;
  }
  pool()->setReclaimer(
      Operator::MemoryReclaimer::create(operatorCtx_->driverCtx(), this));
}

void Operator::maybeSetTracer() {
  const auto& traceConfig = operatorCtx_->driverCtx()->traceConfig();
  if (!traceConfig.has_value()) {
    return;
  }

  const auto nodeId = planNodeId();
  if (traceConfig->queryNodes.count(nodeId) == 0) {
    return;
  }

  auto& tracedOpMap = operatorCtx_->driverCtx()->tracedOperatorMap;
  if (const auto iter = tracedOpMap.find(operatorId());
      iter != tracedOpMap.end()) {
    LOG(WARNING) << "Operator " << iter->first << " with type of "
                 << operatorType() << ", plan node " << nodeId
                 << " might be the auxiliary operator of " << iter->second
                 << " which has the same operator id";
    return;
  }
  tracedOpMap.emplace(operatorId(), operatorType());

  if (!trace::canTrace(operatorType())) {
    BOLT_UNSUPPORTED("{} does not support tracing", operatorType());
  }

  const auto pipelineId = operatorCtx_->driverCtx()->pipelineId;
  const auto driverId = operatorCtx_->driverCtx()->driverId;
  LOG(INFO) << "Trace input for operator type: " << operatorType()
            << ", operator id: " << operatorId() << ", pipeline: " << pipelineId
            << ", driver: " << driverId << ", task: " << taskId();
  const auto opTraceDirPath = trace::getOpTraceDirectory(
      traceConfig->queryTraceDir, planNodeId(), pipelineId, driverId);
  trace::createTraceDirectory(opTraceDirPath);

  if (operatorType() == "TableScan") {
    setupSplitTracer(opTraceDirPath);
  } else {
    setupInputTracer(opTraceDirPath);
  }
}

void Operator::traceInput(const RowVectorPtr& input) {
  if (FOLLY_UNLIKELY(inputTracer_ != nullptr)) {
    inputTracer_->write(input);
  }
}

void Operator::finishTrace() {
  BOLT_CHECK(inputTracer_ == nullptr || splitTracer_ == nullptr);
  if (inputTracer_ != nullptr) {
    inputTracer_->finish();
  }

  if (splitTracer_ != nullptr) {
    splitTracer_->finish();
  }
}

std::vector<std::unique_ptr<Operator::PlanNodeTranslator>>&
Operator::translators() {
  static std::vector<std::unique_ptr<PlanNodeTranslator>> translators;
  return translators;
}

void Operator::setupInputTracer(const std::string& opTraceDirPath) {
  inputTracer_ = std::make_unique<trace::OperatorTraceInputWriter>(
      this,
      opTraceDirPath,
      memory::traceMemoryPool(),
      operatorCtx_->driverCtx()->traceConfig()->updateAndCheckTraceLimitCB);
}

void Operator::setupSplitTracer(const std::string& opTraceDirPath) {
  splitTracer_ =
      std::make_unique<trace::OperatorTraceSplitWriter>(this, opTraceDirPath);
}

// static
std::unique_ptr<Operator> Operator::fromPlanNode(
    DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& planNode,
    std::shared_ptr<ExchangeClient> exchangeClient) {
  BOLT_CHECK_EQ(exchangeClient != nullptr, planNode->requiresExchangeClient());
  for (auto& translator : translators()) {
    std::unique_ptr<Operator> op;
    if (planNode->requiresExchangeClient()) {
      op = translator->toOperator(ctx, id, planNode, exchangeClient);
    } else {
      op = translator->toOperator(ctx, id, planNode);
    }

    if (op) {
      return op;
    }
  }
  return nullptr;
}

// static
std::unique_ptr<JoinBridge> Operator::joinBridgeFromPlanNode(
    const core::PlanNodePtr& planNode) {
  for (auto& translator : translators()) {
    auto joinBridge = translator->toJoinBridge(planNode);
    if (joinBridge) {
      return joinBridge;
    }
  }
  return nullptr;
}

void Operator::initialize() {
  BOLT_CHECK(!initialized_);
  BOLT_CHECK_EQ(
      pool()->currentBytes(),
      0,
      "Unexpected memory usage {} from pool {} before operator init",
      succinctBytes(pool()->currentBytes()),
      pool()->name());
  initialized_ = true;
  maybeSetReclaimer();
  maybeSetTracer();
}

// static
OperatorSupplier Operator::operatorSupplierFromPlanNode(
    const core::PlanNodePtr& planNode) {
  for (auto& translator : translators()) {
    auto supplier = translator->toOperatorSupplier(planNode);
    if (supplier) {
      return supplier;
    }
  }
  return nullptr;
}

// static
void Operator::registerOperator(
    std::unique_ptr<PlanNodeTranslator> translator) {
  translators().emplace_back(std::move(translator));
}

// static
void Operator::unregisterAllOperators() {
  translators().clear();
}

std::optional<uint32_t> Operator::maxDrivers(
    const core::PlanNodePtr& planNode) {
  for (auto& translator : translators()) {
    auto current = translator->maxDrivers(planNode);
    if (current) {
      return current;
    }
  }
  return std::nullopt;
}

const std::string& OperatorCtx::taskId() const {
  return driverCtx_->task->taskId();
}

int32_t OperatorCtx::getConcurrency() const {
  return driverCtx_->task->getConcurrency();
}

std::string OperatorCtx::toString() const {
  return fmt::format(
      "{}#{}[taskId={}, pipelineId={}, driverId={}]",
      operatorType(),
      operatorId(),
      taskId(),
      driverCtx_->pipelineId,
      driverCtx_->driverId);
}

void OperatorCtx::traverseOpToGetRowCount(
    uint64_t& totalRowCount,
    uint64_t& processedRowCount) {
  auto numDrivers = task()->numDrivers(driverCtx());

  if (numDrivers == 1) {
    const auto& operators = driver()->operators();

    VLOG(5) << "operators.size()=" << operators.size()
            << ", operatorId=" << operatorId();

    for (auto i = operatorId() - 1; i >= 0; --i) {
      auto metricValueStr = operators[i]->getRuntimeMetric(
          OperatorMetricKey::kCanUsedToEstimateHashBuildPartitionNum, "false");
      auto metricValue = folly::to<bool>(metricValueStr);

      VLOG(5) << "OperatorIndex=" << i << ", operator is "
              << operators[i]->toString()
              << ", kCanUsedToEstimateHashBuildPartitionNum="
              << (metricValue ? "true" : "false");

      if (metricValue) {
        auto totalRowCountStr =
            operators[i]->getRuntimeMetric(OperatorMetricKey::kTotalRowCount);
        auto hasBeenProcessedRowCountStr = operators[i]->getRuntimeMetric(
            OperatorMetricKey::kHasBeenProcessedRowCount);

        BOLT_CHECK_NE(totalRowCountStr, "", "totalRowCountStr can't be empty");
        BOLT_CHECK_NE(
            hasBeenProcessedRowCountStr,
            "",
            "hasBeenProcessedRowCountStr can't be empty");

        totalRowCount = folly::to<uint64_t>(totalRowCountStr);
        processedRowCount = folly::to<uint64_t>(hasBeenProcessedRowCountStr);

        LOG(INFO) << toString() << " totalRowCountStr = " << totalRowCountStr
                  << ", hasBeenProcessedRowCountStr = "
                  << hasBeenProcessedRowCountStr
                  << ", numDrivers = " << numDrivers;
        break;
      }
    }
  }
}

void OperatorCtx::adjustSpillCompressionKind(
    common::SpillConfig*& spillConfig) {
  if (!isFirstSpill_) {
    return;
  }
  isFirstSpill_ = false;

  if (!spillConfig || !pool_ ||
      spillConfig->compressionKind !=
          common::CompressionKind::CompressionKind_NONE) {
    LOG(INFO) << toString() << " compressionKind is NOT NONE, use setting Kind:"
              << spillConfig->compressionKind;
    return;
  }

  uint64_t totalRowCount{0}, processedRowCount{0};
  traverseOpToGetRowCount(totalRowCount, processedRowCount);

  auto currentUsage = pool_->currentBytes();
  if (totalRowCount <= 0 || processedRowCount <= 0 || currentUsage <= 0) {
    LOG(INFO) << toString() << " DO NOTHING!!!  totalRowCount=" << totalRowCount
              << ", processedRowCount=" << processedRowCount
              << ", currentUsage=" << currentUsage;
    return;
  }

  const auto& queryConfig = task()->queryCtx()->queryConfig();
  auto lowCompressSize = queryConfig.spillLowCompressByteThreshold();
  auto highCompressSize = queryConfig.spillHighCompressByteThreshold();
  auto estimatedSpillSize =
      currentUsage * totalRowCount / (processedRowCount + 1);

  if (estimatedSpillSize >= highCompressSize) {
    spillConfig->compressionKind =
        common::CompressionKind::CompressionKind_ZSTD;
    LOG(INFO) << toString()
              << " Spill compressionKind changed, from NONE to ZSTD"
              << ", estimatedSpillSize:" << estimatedSpillSize
              << ", highCompressSize:" << highCompressSize
              << ", currentUsage:" << currentUsage
              << ", processedRowCount:" << processedRowCount
              << ", totalRowCount:" << totalRowCount;
  } else if (estimatedSpillSize >= lowCompressSize) {
    spillConfig->compressionKind = common::CompressionKind::CompressionKind_LZ4;
    LOG(INFO) << toString()
              << " Spill compressionKind changed, from NONE to LZ4"
              << ", estimatedSpillSize:" << estimatedSpillSize
              << ", lowCompressSize:" << lowCompressSize
              << ", currentUsage:" << currentUsage
              << ", processedRowCount:" << processedRowCount
              << ", totalRowCount:" << totalRowCount;
  }
}

static bool isSequence(
    const vector_size_t* numbers,
    vector_size_t start,
    vector_size_t end) {
  for (vector_size_t i = start; i < end; ++i) {
    if (numbers[i] != i) {
      return false;
    }
  }
  return true;
}

RowVectorPtr Operator::fillOutput(
    vector_size_t size,
    const BufferPtr& mapping,
    const std::vector<VectorPtr>& results) {
  bool wrapResults = true;
  if (size == input_->size() &&
      (!mapping || isSequence(mapping->as<vector_size_t>(), 0, size))) {
    if (isIdentityProjection_) {
      return std::move(input_);
    }
    wrapResults = false;
  }

  std::vector<VectorPtr> projectedChildren(outputType_->size());
  projectChildren(
      projectedChildren,
      input_,
      identityProjections_,
      size,
      wrapResults ? mapping : nullptr);
  projectChildren(
      projectedChildren,
      results,
      resultProjections_,
      size,
      wrapResults ? mapping : nullptr);

  return std::make_shared<RowVector>(
      operatorCtx_->pool(),
      outputType_,
      nullptr,
      size,
      std::move(projectedChildren));
}

RowVectorPtr Operator::fillOutput(
    vector_size_t size,
    const BufferPtr& mapping) {
  return fillOutput(size, mapping, results_);
}

OperatorStats Operator::stats(bool clear) {
  OperatorStats stats;
  if (!clear) {
    stats = *stats_.rlock();
  } else {
    auto lockedStats = stats_.wlock();
    stats = *lockedStats;
    lockedStats->clear();
  }

  stats.memoryStats = MemoryStats::memStatsFromPool(pool());
  return stats;
}

uint32_t Operator::outputBatchRows(
    std::optional<uint64_t> averageRowSize) const {
  const auto& queryConfig = operatorCtx_->task()->queryCtx()->queryConfig();

  if (!averageRowSize.has_value()) {
    return queryConfig.preferredOutputBatchRows();
  }

  const uint64_t rowSize = averageRowSize.value();
  BOLT_CHECK_GE(
      rowSize,
      0,
      "The given average row size of {}.{} is negative.",
      operatorType(),
      operatorId());

  if (rowSize * queryConfig.maxOutputBatchRows() <
      queryConfig.preferredOutputBatchBytes()) {
    return queryConfig.maxOutputBatchRows();
  }
  return std::max<uint32_t>(
      queryConfig.preferredOutputBatchBytes() / rowSize, 1);
}

int32_t Operator::getMaxReadBatchSize(
    int32_t minMaxReadBatchSize,
    int32_t batchSizeAlignment,
    int32_t complexTypeNumberThreshold) {
  const auto& queryConfig = operatorCtx_->task()->queryCtx()->queryConfig();
  int32_t maxReadBatchSize = queryConfig.maxOutputBatchRows();
  if (minMaxReadBatchSize > maxReadBatchSize) {
    // if maxReadBatchSize from config is less than minMaxReadBatchSize,
    // return minMaxReadBatchSize directly
    LOG(INFO) << name() << " max read batch size: " << maxReadBatchSize;
    return maxReadBatchSize;
  }
  int32_t fixWidthLength = 0;
  int32_t complexTypeNumber = 0;
  for (const auto& type : asRowType(outputType_)->children()) {
    fixWidthLength += type->isFixedWidth()
        ? type->cppSizeInBytes()
        : ((type->isVarchar() || type->isVarbinary()) ? sizeof(StringView) : 0);
    complexTypeNumber +=
        (type->isArray() || type->isMap() || type->isRow()) ? 1 : 0;
  }

  uint64_t maxBatchBytes = queryConfig.preferredOutputBatchBytes();
  if (fixWidthLength * maxReadBatchSize > maxBatchBytes) {
    // if fix length is too large, reduce maxReadBatchSize correspondingly
    maxReadBatchSize = maxBatchBytes / fixWidthLength;
  }
  if (maxReadBatchSize > minMaxReadBatchSize) {
    // if contains complex type, reduce maxReadBatchSize correspondingly
    if (complexTypeNumber >= complexTypeNumberThreshold) {
      maxReadBatchSize = minMaxReadBatchSize;
    } else {
      maxReadBatchSize -= (maxReadBatchSize - minMaxReadBatchSize) *
          complexTypeNumber / complexTypeNumberThreshold;
    }
  }
  // align maxReadBatchSize to batchSizeAlignment
  maxReadBatchSize = std::max(
      maxReadBatchSize / batchSizeAlignment * batchSizeAlignment,
      minMaxReadBatchSize);

  LOG(INFO) << name() << " max read batch size: " << maxReadBatchSize
            << ", fix width length: " << fixWidthLength
            << ", complex type number: " << complexTypeNumber;
  return maxReadBatchSize;
}

void Operator::recordBlockingTime(uint64_t start, BlockingReason reason) {
  uint64_t now =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::high_resolution_clock::now().time_since_epoch())
          .count();
  const auto wallNanos = (now - start) * 1000;
  const auto blockReason = blockingReasonToString(reason).substr(1);

  auto lockedStats = stats_.wlock();
  lockedStats->blockedWallNanos += wallNanos;
  lockedStats->addRuntimeStat(
      fmt::format("blocked{}WallNanos", blockReason),
      RuntimeCounter(wallNanos, RuntimeCounter::Unit::kNanos));
  lockedStats->addRuntimeStat(
      fmt::format("blocked{}Times", blockReason), RuntimeCounter(1));
}

void Operator::recordSpillStats(const common::SpillStats& spillStats) {
  BOLT_CHECK(noMoreInput_);
  auto lockedStats = stats_.wlock();
  lockedStats->spilledInputBytes += spillStats.spilledInputBytes;
  lockedStats->spilledBytes += spillStats.spilledBytes;
  lockedStats->spilledRows += spillStats.spilledRows;
  lockedStats->spilledPartitions += spillStats.spilledPartitions;
  lockedStats->spilledFiles += spillStats.spilledFiles;
  lockedStats->spillTotalTime +=
      spillStats.spillTotalTimeUs * Timestamp::kNanosecondsInMicrosecond;

  if (spillStats.spillFillTimeUs != 0) {
    lockedStats->addRuntimeStat(
        "spillFillTime",
        RuntimeCounter{
            static_cast<int64_t>(
                spillStats.spillFillTimeUs *
                Timestamp::kNanosecondsInMicrosecond),
            RuntimeCounter::Unit::kNanos});
  }
  if (spillStats.spillSortTimeUs != 0) {
    lockedStats->addRuntimeStat(
        "spillSortTime",
        RuntimeCounter{
            static_cast<int64_t>(
                spillStats.spillSortTimeUs *
                Timestamp::kNanosecondsInMicrosecond),
            RuntimeCounter::Unit::kNanos});
  }
  if (spillStats.spillConvertTimeUs != 0) {
    lockedStats->addRuntimeStat(
        "spillConvertTime",
        RuntimeCounter{
            static_cast<int64_t>(
                spillStats.spillConvertTimeUs *
                Timestamp::kNanosecondsInMicrosecond),
            RuntimeCounter::Unit::kNanos});
  }
  if (spillStats.spillTotalTimeUs != 0) {
    lockedStats->addRuntimeStat(
        "spillTotalTime",
        RuntimeCounter{
            static_cast<int64_t>(
                spillStats.spillTotalTimeUs *
                Timestamp::kNanosecondsInMicrosecond),
            RuntimeCounter::Unit::kNanos});
  }
  if (spillStats.spillSerializationTimeUs != 0) {
    lockedStats->addRuntimeStat(
        "spillSerializationTime",
        RuntimeCounter{
            static_cast<int64_t>(
                spillStats.spillSerializationTimeUs *
                Timestamp::kNanosecondsInMicrosecond),
            RuntimeCounter::Unit::kNanos});
  }
  if (spillStats.spillFlushTimeUs != 0) {
    lockedStats->addRuntimeStat(
        "spillFlushTime",
        RuntimeCounter{
            static_cast<int64_t>(
                spillStats.spillFlushTimeUs *
                Timestamp::kNanosecondsInMicrosecond),
            RuntimeCounter::Unit::kNanos});
  }
  if (spillStats.spillWrites != 0) {
    lockedStats->addRuntimeStat(
        Operator::kSpillWrites,
        RuntimeCounter{static_cast<int64_t>(spillStats.spillWrites)});
  }
  if (spillStats.spillWriteTimeUs != 0) {
    lockedStats->addRuntimeStat(
        "spillWriteTime",
        RuntimeCounter{
            static_cast<int64_t>(
                spillStats.spillWriteTimeUs *
                Timestamp::kNanosecondsInMicrosecond),
            RuntimeCounter::Unit::kNanos});
  }
  if (spillStats.spillRuns != 0) {
    lockedStats->addRuntimeStat(
        "spillRuns",
        RuntimeCounter{static_cast<int64_t>(spillStats.spillRuns)});
    common::updateGlobalSpillRunStats(spillStats.spillRuns);
  }

  if (spillStats.spillMaxLevelExceededCount != 0) {
    lockedStats->addRuntimeStat(
        "exceededMaxSpillLevel",
        RuntimeCounter{
            static_cast<int64_t>(spillStats.spillMaxLevelExceededCount)});
    common::updateGlobalMaxSpillLevelExceededCount(
        spillStats.spillMaxLevelExceededCount);
  }
}

void Operator::recordSpillReadStats(
    const common::SpillReadStats& spillReadStats) {
  auto lockedStats = stats_.wlock();
  if (spillReadStats.spillReadTimeUs) {
    lockedStats->addRuntimeStat(
        "spillReadTotalTime",
        RuntimeCounter{
            static_cast<int64_t>(
                spillReadStats.spillReadTimeUs *
                Timestamp::kNanosecondsInMicrosecond),
            RuntimeCounter::Unit::kNanos});
  }
  if (spillReadStats.spillDecompressTimeUs) {
    lockedStats->addRuntimeStat(
        "spillDecompressTotalTime",
        RuntimeCounter{
            static_cast<int64_t>(
                spillReadStats.spillDecompressTimeUs *
                Timestamp::kNanosecondsInMicrosecond),
            RuntimeCounter::Unit::kNanos});
  }
  if (spillReadStats.spillReadIOTimeUs) {
    lockedStats->addRuntimeStat(
        "spillReadIOTotalTime",
        RuntimeCounter{
            static_cast<int64_t>(
                spillReadStats.spillReadIOTimeUs *
                Timestamp::kNanosecondsInMicrosecond),
            RuntimeCounter::Unit::kNanos});
  }
}

void Operator::recordSortStats(const common::SortStats& sortStats) {
  auto lockedStats = stats_.wlock();

  lockedStats->sortOutputTime +=
      sortStats.sortOutputTimeUs * Timestamp::kNanosecondsInMicrosecond;
  lockedStats->sortColToRowTime +=
      sortStats.sortColToRowTimeUs * Timestamp::kNanosecondsInMicrosecond;
  lockedStats->sortInSortTime +=
      sortStats.sortInSortTimeUs * Timestamp::kNanosecondsInMicrosecond;

  if (sortStats.sortOutputTimeUs) {
    lockedStats->addRuntimeStat(
        "sortOutputTotalTime",
        RuntimeCounter{
            static_cast<int64_t>(
                sortStats.sortOutputTimeUs *
                Timestamp::kNanosecondsInMicrosecond),
            RuntimeCounter::Unit::kNanos});
  }
  if (sortStats.sortColToRowTimeUs) {
    lockedStats->addRuntimeStat(
        "sortColToRowTotalTime",
        RuntimeCounter{
            static_cast<int64_t>(
                sortStats.sortColToRowTimeUs *
                Timestamp::kNanosecondsInMicrosecond),
            RuntimeCounter::Unit::kNanos});
  }
  if (sortStats.sortInSortTimeUs) {
    lockedStats->addRuntimeStat(
        "sortInSortTotalTime",
        RuntimeCounter{
            static_cast<int64_t>(
                sortStats.sortInSortTimeUs *
                Timestamp::kNanosecondsInMicrosecond),
            RuntimeCounter::Unit::kNanos});
  }
}

void Operator::recordWindowStats(const common::WindowStats& windowStats) {
  auto lockedStats = stats_.wlock();

  lockedStats->windowAddInputTime +=
      windowStats.addInputTimeUs * Timestamp::kNanosecondsInMicrosecond;
  lockedStats->windowComputeWindowFunctionTime +=
      windowStats.computeWindowFunctionTimeUs *
      Timestamp::kNanosecondsInMicrosecond;
  lockedStats->windowExtractColumnTime +=
      windowStats.extractColumnTimeUs * Timestamp::kNanosecondsInMicrosecond;
  lockedStats->windowOutputTime +=
      windowStats.getOutputTimeUs * Timestamp::kNanosecondsInMicrosecond;
  lockedStats->windowHasNextPartitionTime +=
      windowStats.hasNextPartitionUs * Timestamp::kNanosecondsInMicrosecond;
  lockedStats->windowLoadFromSpillTime +=
      windowStats.loadFromSpillTimeUs * Timestamp::kNanosecondsInMicrosecond;
  lockedStats->windowSpillTime +=
      windowStats.windowSpillTimeUs * Timestamp::kNanosecondsInMicrosecond;
  lockedStats->buildPartitionTime +=
      windowStats.buildPartitionTimeUs * Timestamp::kNanosecondsInMicrosecond;

  lockedStats->windowSpilledRows += windowStats.windowSpilledRows;
  lockedStats->windowSpilledFiles += windowStats.windowSpilledFiles;
  lockedStats->windowSpillTotalTime +=
      windowStats.windowSpillTotalTimeUs * Timestamp::kNanosecondsInMicrosecond;

  if (windowStats.addInputTimeUs) {
    lockedStats->addRuntimeStat(
        "windowInputTotalTime",
        RuntimeCounter{
            static_cast<int64_t>(
                windowStats.addInputTimeUs *
                Timestamp::kNanosecondsInMicrosecond),
            RuntimeCounter::Unit::kNanos});
  }
  if (windowStats.extractColumnTimeUs) {
    lockedStats->addRuntimeStat(
        "extractColumnTotalTime",
        RuntimeCounter{
            static_cast<int64_t>(
                windowStats.extractColumnTimeUs *
                Timestamp::kNanosecondsInMicrosecond),
            RuntimeCounter::Unit::kNanos});
  }
  if (windowStats.computeWindowFunctionTimeUs) {
    lockedStats->addRuntimeStat(
        "computeTotalTime",
        RuntimeCounter{
            static_cast<int64_t>(
                windowStats.computeWindowFunctionTimeUs *
                Timestamp::kNanosecondsInMicrosecond),
            RuntimeCounter::Unit::kNanos});
  }
}

void Operator::recordGroupingSetStats(const common::AggregationStats& stats) {
  auto lockedStats = stats_.wlock();
  if (stats.aggExtractGroupsTimeNs) {
    lockedStats->addRuntimeStat(
        "aggExtractGroupsTimeNs",
        RuntimeCounter{
            (int64_t)stats.aggExtractGroupsTimeNs,
            RuntimeCounter::Unit::kNanos});
  }
  if (stats.aggFunctionTimeNs) {
    lockedStats->addRuntimeStat(
        "aggFunctionTimeNs",
        RuntimeCounter{
            (int64_t)stats.aggFunctionTimeNs, RuntimeCounter::Unit::kNanos});
  }
  if (stats.aggOutputUniqueRows) {
    lockedStats->addRuntimeStat(
        "aggOutputUniqueRows",
        RuntimeCounter{(int64_t)stats.aggOutputUniqueRows});
  }
  if (stats.aggOutputTimeNs) {
    lockedStats->addRuntimeStat(
        "aggOutputTimeNs",
        RuntimeCounter{
            (int64_t)stats.aggOutputTimeNs, RuntimeCounter::Unit::kNanos});
  }
  if (stats.aggProbeTimeNs) {
    lockedStats->addRuntimeStat(
        "aggProbeTimeNs",
        RuntimeCounter{
            (int64_t)stats.aggProbeTimeNs, RuntimeCounter::Unit::kNanos});
  }
  if (stats.aggOutputUpdateTimeNs) {
    lockedStats->addRuntimeStat(
        "aggOutputUpdateTimeNs",
        RuntimeCounter{
            (int64_t)stats.aggOutputUpdateTimeNs,
            RuntimeCounter::Unit::kNanos});
  }
  if (stats.aggProbeBypassTimeNs) {
    lockedStats->addRuntimeStat(
        "aggProbeBypassTimeNs",
        RuntimeCounter{
            (int64_t)stats.aggProbeBypassTimeNs, RuntimeCounter::Unit::kNanos});
  }
  if (stats.aggProbeBypassCount) {
    lockedStats->addRuntimeStat(
        "aggProbeBypassCount",
        RuntimeCounter{(int64_t)stats.aggProbeBypassCount});
  }
}

void Operator::recordHashBuildSpillStats(
    const common::HashBuildSpillStats& hashBuildSpillStats) {
  auto lockedStats = stats_.wlock();
  if (hashBuildSpillStats.spillLevel != 0) {
    lockedStats->setRuntimeStat(
        "spillLevel",
        RuntimeCounter{static_cast<int64_t>(hashBuildSpillStats.spillLevel)});
  }

  if (hashBuildSpillStats.useApativeHashBuild != 0) {
    lockedStats->setRuntimeStat(
        "useAdaptiveSpill",
        RuntimeCounter{
            static_cast<int64_t>(hashBuildSpillStats.useApativeHashBuild)});
  }
}

void Operator::setRuntimeMetric(
    OperatorMetricKey key,
    OperatorMetricValue value) {
  runtimeMetric_[key] = value;
}

OperatorMetricValue Operator::getRuntimeMetric(
    OperatorMetricKey key,
    OperatorMetricValue defaultReturnValue) {
  auto it = runtimeMetric_.find(key);
  if (it == runtimeMetric_.end()) {
    return defaultReturnValue;
  } else {
    return it->second;
  }
}

std::string Operator::toString() const {
  std::stringstream out;
  out << operatorType() << "[" << planNodeId() << "] " << operatorId();
  return out.str();
}

std::vector<column_index_t> toChannels(
    const RowTypePtr& rowType,
    const std::vector<core::TypedExprPtr>& exprs) {
  std::vector<column_index_t> channels;
  channels.reserve(exprs.size());
  for (const auto& expr : exprs) {
    auto channel = exprToChannel(expr.get(), rowType);
    channels.push_back(channel);
  }
  return channels;
}

column_index_t exprToChannel(
    const core::ITypedExpr* expr,
    const TypePtr& type) {
  if (auto field = dynamic_cast<const core::FieldAccessTypedExpr*>(expr)) {
    return type->as<TypeKind::ROW>().getChildIdx(field->name());
  }
  if (dynamic_cast<const core::ConstantTypedExpr*>(expr)) {
    return kConstantChannel;
  }
  BOLT_FAIL(
      "Expression must be field access or constant, got: {}", expr->toString());
  return 0; // not reached.
}

std::vector<column_index_t> calculateOutputChannels(
    const RowTypePtr& sourceOutputType,
    const RowTypePtr& targetInputType,
    const RowTypePtr& targetOutputType) {
  // Note that targetInputType may have more columns than sourceOutputType as
  // some columns can be duplicated.
  bool identicalProjection =
      sourceOutputType->size() == targetInputType->size();
  const auto& outputNames = targetInputType->names();

  std::vector<column_index_t> outputChannels;
  outputChannels.resize(outputNames.size());
  for (auto i = 0; i < outputNames.size(); i++) {
    outputChannels[i] = sourceOutputType->getChildIdx(outputNames[i]);
    if (outputChannels[i] != i) {
      identicalProjection = false;
    }
    if (outputNames[i] != targetOutputType->nameOf(i)) {
      identicalProjection = false;
    }
  }
  if (identicalProjection) {
    outputChannels.clear();
  }
  return outputChannels;
}

FOLLY_ALWAYS_INLINE std::string capitalizeWithBasePrefix(
    const std::string& name) {
  // Handle empty string case
  if (name.empty()) {
    return "base";
  }

  // Create a copy of the input string
  std::string result = name;

  // Capitalize the first letter
  result[0] = std::toupper(result[0]);

  // Prepend the "base" prefix
  return "base" + result;
}

void OperatorStats::addRuntimeStat(
    const std::string& name,
    const RuntimeCounter& value) {
  if (UNLIKELY(abMetrics.find(name) != abMetrics.end() && !isControlGroup)) {
    std::string newName = capitalizeWithBasePrefix(name);
    addOperatorRuntimeStats(newName, value, runtimeStats);
    return;
  }

  addOperatorRuntimeStats(name, value, runtimeStats);
}

void OperatorStats::setRuntimeStat(
    const std::string& name,
    const RuntimeCounter& value) {
  if (UNLIKELY(abMetrics.find(name) != abMetrics.end() && !isControlGroup)) {
    std::string newName = capitalizeWithBasePrefix(name);
    setOperatorRuntimeStats(newName, value, runtimeStats);
    return;
  }

  setOperatorRuntimeStats(name, value, runtimeStats);
}

void OperatorStats::add(const OperatorStats& other) {
  numSplits += other.numSplits;
  rawInputBytes += other.rawInputBytes;
  rawInputPositions += other.rawInputPositions;

  for (int i = 0; i < 4; i++) {
    rawBytesReads[i] += other.rawBytesReads[i];
    cntReads[i] += other.cntReads[i];
    scanTimeReads[i] += other.scanTimeReads[i];
  }

  addInputTiming.add(other.addInputTiming);
  inputBytes += other.inputBytes;
  inputPositions += other.inputPositions;

  // Update the new operator-level stats for coefficient of variation
  sumSquaredInputPositions += other.inputPositions * other.inputPositions;
  // If we use numDrivers instead of nonZeroInputPositionsNumDrivers, then the
  // coefficient of variation will be overestimated.
  // nonZeroInputPositionsNumDrivers doesn't increment when OperatorStats is
  // added with inputPositions = 0
  if (other.inputPositions > 0) {
    nonZeroInputPositionsNumDrivers += other.numDrivers;
  }

  inputVectors += other.inputVectors;

  getOutputTiming.add(other.getOutputTiming);
  outputBytes += other.outputBytes;
  outputPositions += other.outputPositions;
  outputVectors += other.outputVectors;

  physicalWrittenBytes += other.physicalWrittenBytes;

  blockedWallNanos += other.blockedWallNanos;

  finishTiming.add(other.finishTiming);

  isBlockedTiming.add(other.isBlockedTiming);

  backgroundTiming.add(other.backgroundTiming);

  memoryStats.add(other.memoryStats);

  for (const auto& [name, stats] : other.runtimeStats) {
    if (UNLIKELY(runtimeStats.count(name) == 0)) {
      runtimeStats.insert(std::make_pair(name, stats));
    } else {
      runtimeStats.at(name).merge(stats);
    }
  }

  for (const auto& [name, exprStats] : other.expressionStats) {
    if (UNLIKELY(expressionStats.count(name) == 0)) {
      expressionStats.insert(std::make_pair(name, exprStats));
    } else {
      expressionStats.at(name).add(exprStats);
    }
  }

  numDrivers += other.numDrivers;
  spilledInputBytes += other.spilledInputBytes;
  spilledBytes += other.spilledBytes;
  spilledRows += other.spilledRows;
  spillTotalTime += other.spillTotalTime;
  sortOutputTime += other.sortOutputTime;
  sortColToRowTime += other.sortColToRowTime;
  sortInSortTime += other.sortInSortTime;
  windowAddInputTime += other.windowAddInputTime;
  windowComputeWindowFunctionTime += other.windowComputeWindowFunctionTime;
  windowExtractColumnTime += other.windowExtractColumnTime;
  windowOutputTime += other.windowOutputTime;
  windowHasNextPartitionTime += other.windowHasNextPartitionTime;
  windowLoadFromSpillTime += other.windowLoadFromSpillTime;
  windowSpillTime += other.windowSpillTime;
  buildPartitionTime += other.buildPartitionTime;

  spilledPartitions += other.spilledPartitions;
  spilledFiles += other.spilledFiles;

  numNullKeys += other.numNullKeys;

  dynamicFilterStats.add(other.dynamicFilterStats);
}

void OperatorStats::clear() {
  numSplits = 0;
  rawInputBytes = 0;
  rawInputPositions = 0;

  for (int i = 0; i < 4; i++) {
    rawBytesReads[i] = 0;
    cntReads[i] = 0;
    scanTimeReads[i] = 0;
  }

  addInputTiming.clear();
  inputBytes = 0;
  inputPositions = 0;

  getOutputTiming.clear();
  outputBytes = 0;
  outputPositions = 0;

  physicalWrittenBytes = 0;

  blockedWallNanos = 0;

  finishTiming.clear();

  backgroundTiming.clear();

  memoryStats.clear();

  runtimeStats.clear();
  expressionStats.clear();

  numDrivers = 0;
  spilledInputBytes = 0;
  spilledBytes = 0;
  spilledRows = 0;
  spilledPartitions = 0;
  spilledFiles = 0;
  spillTotalTime = 0;

  sortOutputTime = 0;
  sortColToRowTime = 0;
  sortInSortTime = 0;
  windowAddInputTime = 0;
  windowComputeWindowFunctionTime = 0;
  windowExtractColumnTime = 0;
  windowOutputTime = 0;
  windowHasNextPartitionTime = 0;
  windowLoadFromSpillTime = 0;
  windowSpillTime = 0;
  buildPartitionTime = 0;

  dynamicFilterStats.clear();
}

std::unique_ptr<memory::MemoryReclaimer> Operator::MemoryReclaimer::create(
    DriverCtx* driverCtx,
    Operator* op) {
  return std::unique_ptr<memory::MemoryReclaimer>(
      new Operator::MemoryReclaimer(driverCtx->driver->shared_from_this(), op));
}

void Operator::MemoryReclaimer::enterArbitration() {
  DriverThreadContext* driverThreadCtx = driverThreadContext();
  if (FOLLY_UNLIKELY(driverThreadCtx == nullptr)) {
    // Skips the driver suspension handling if this memory arbitration request
    // is not issued from a driver thread. For example, async streaming shuffle
    // and table scan prefetch execution path might initiate memory arbitration
    // request from non-driver thread.
    return;
  }

  Driver* const runningDriver = driverThreadCtx->driverCtx()->driver;
  if (!FLAGS_bolt_memory_pool_capacity_transfer_across_tasks) {
    if (auto opDriver = ensureDriver()) {
      // NOTE: the current running driver might not be the driver of the
      // operator that requests memory arbitration. The reason is that an
      // operator might extend the buffer allocated from the other operator
      // either from the same or different drivers. But they must be from the
      // same task.
      BOLT_CHECK_EQ(
          runningDriver->task()->taskId(),
          opDriver->task()->taskId(),
          "The current running driver and the request driver must be from the same task");
    }
  }
  if (runningDriver->task()->enterSuspended(runningDriver->state()) !=
      StopReason::kNone) {
    // There is no need for arbitration if the associated task has already
    // terminated.
    BOLT_FAIL("Terminate detected when entering suspension");
  }
}

void Operator::MemoryReclaimer::leaveArbitration() noexcept {
  DriverThreadContext* driverThreadCtx = driverThreadContext();
  if (FOLLY_UNLIKELY(driverThreadCtx == nullptr)) {
    // Skips the driver suspension handling if this memory arbitration request
    // is not issued from a driver thread.
    return;
  }
  Driver* const runningDriver = driverThreadCtx->driverCtx()->driver;
  if (!FLAGS_bolt_memory_pool_capacity_transfer_across_tasks) {
    if (auto opDriver = ensureDriver()) {
      BOLT_CHECK_EQ(
          runningDriver->task()->taskId(),
          opDriver->task()->taskId(),
          "The current running driver and the request driver must be from the same task");
    }
  }
  runningDriver->task()->leaveSuspended(runningDriver->state());
}

bool Operator::MemoryReclaimer::reclaimableBytes(
    const memory::MemoryPool& pool,
    uint64_t& reclaimableBytes) const {
  reclaimableBytes = 0;
  std::shared_ptr<Driver> driver = ensureDriver();
  if (FOLLY_UNLIKELY(driver == nullptr)) {
    return false;
  }
  BOLT_CHECK_EQ(pool.name(), op_->pool()->name());
  return op_->reclaimableBytes(reclaimableBytes);
}

uint64_t Operator::MemoryReclaimer::reclaim(
    memory::MemoryPool* pool,
    uint64_t targetBytes,
    uint64_t /*unused*/,
    memory::MemoryReclaimer::Stats& stats) {
  std::shared_ptr<Driver> driver = ensureDriver();
  if (FOLLY_UNLIKELY(driver == nullptr)) {
    return 0;
  }
  if (!op_->canReclaim()) {
    return 0;
  }
  BOLT_CHECK_EQ(pool->name(), op_->pool()->name());
  BOLT_CHECK(
      !driver->state().isOnThread() || driver->state().suspended() ||
          driver->state().isTerminated,
      "driverOnThread {}, driverSuspended {} driverTerminated {} {}",
      driver->state().isOnThread(),
      driver->state().suspended(),
      driver->state().isTerminated,
      pool->name());
  BOLT_CHECK(driver->task()->pauseRequested());

  TestValue::adjust(
      "bytedance::bolt::exec::Operator::MemoryReclaimer::reclaim", pool);

  // NOTE: we can't reclaim memory from an operator which is under
  // non-reclaimable section.
  if (op_->nonReclaimableSection_) {
    // TODO: reduce the log frequency if it is too verbose.
    ++stats.numNonReclaimableAttempts;
    RECORD_METRIC_VALUE(kMetricMemoryNonReclaimableCount);
    LOG(WARNING) << "Can't reclaim from memory pool " << pool->name()
                 << " which is under non-reclaimable section, memory usage: "
                 << succinctBytes(pool->currentBytes())
                 << ", reservation: " << succinctBytes(pool->reservedBytes());
    return 0;
  }

  RuntimeStatWriterScopeGuard opStatsGuard(op_);

  return memory::MemoryReclaimer::run(
      [&]() {
        int64_t reclaimedBytes{0};
        {
          memory::ScopedReclaimedBytesRecorder recoder(pool, &reclaimedBytes);
          op_->reclaim(targetBytes, stats);
        }
        return reclaimedBytes;
      },
      stats);
}

void Operator::MemoryReclaimer::abort(
    memory::MemoryPool* pool,
    const std::exception_ptr& /* error */) {
  std::shared_ptr<Driver> driver = ensureDriver();
  if (FOLLY_UNLIKELY(driver == nullptr)) {
    return;
  }
  BOLT_CHECK_EQ(pool->name(), op_->pool()->name());
  BOLT_CHECK(
      !driver->state().isOnThread() || driver->state().suspended() ||
      driver->state().isTerminated);
  BOLT_CHECK(driver->task()->isCancelled());

  // Calls operator close to free up major memory usage.
  op_->close();
}
} // namespace bytedance::bolt::exec
