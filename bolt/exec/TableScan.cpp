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
 */
/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

#include <glog/logging.h>
#include <cstdint>
#include <memory>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/common/time/Timer.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/exec/Operator.h"
#include "bolt/exec/OperatorMetric.h"
#include "bolt/exec/TableScan.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/TraceUtil.h"
#include "bolt/expression/Expr.h"

using bytedance::bolt::common::testutil::TestValue;
namespace bytedance::bolt::exec {

std::atomic<uint64_t> TableScan::ioWaitNanos_;

static constexpr double MinEstimateBatchSizeFactor = 0.15;

TableScan::TableScan(
    int32_t operatorId,
    DriverCtx* driverCtx,
    std::shared_ptr<const core::TableScanNode> tableScanNode)
    : SourceOperator(
          driverCtx,
          tableScanNode->outputType(),
          operatorId,
          tableScanNode->id(),
          "TableScan"),
      tableHandle_(tableScanNode->tableHandle()),
      columnHandles_(tableScanNode->assignments()),
      driverCtx_(driverCtx),
      connectorPool_(driverCtx_->task->addConnectorPoolLocked(
          planNodeId(),
          driverCtx_->pipelineId,
          driverCtx_->driverId,
          operatorType(),
          tableHandle_->connectorId())),
      maxSplitPreloadPerDriver_(
          driverCtx_->queryConfig().maxSplitPreloadPerDriver()),
      readBatchSize_(driverCtx_->queryConfig().preferredOutputBatchRows()),
      maxReadBatchSize_(getMaxReadBatchSize(4096, 1024, 8)),
      minReadBatchSize_(driverCtx_->queryConfig().minOutputBatchRows()),
      getOutputTimeLimitMs_(
          driverCtx_->queryConfig().tableScanGetOutputTimeLimitMs()),
      enableEstimateBytesPerRow_(
          driverCtx_->queryConfig().iskEstimateRowSizeBasedOnSampleEnabled()),
      asyncThreadCtx_(
          driverCtx_->queryConfig().preloadBytesLimit(),
          driverCtx_->queryConfig().adaptivePreloadEnabled()) {
  for (const auto& type : asRowType(outputType_)->children()) {
    if (!type->isFixedWidth()) {
      isFixedWidthOutputType_ = false;
      break;
    }
  }
  if (isFixedWidthOutputType_) {
    enableEstimateBytesPerRow_ = false;
  }
  connector_ = connector::getConnector(tableHandle_->connectorId());
  this->setRuntimeMetric(kCanUsedToEstimateHashBuildPartitionNum, "true");
  this->setRuntimeMetric(
      OperatorMetricKey::kHasBeenProcessedRowCount, folly::to<std::string>(0));
  if (tableScanNode->existRowCount()) {
    VLOG(1) << "TableScan RowCount=" << tableScanNode->getRowCount();
    this->setRuntimeMetric(
        OperatorMetricKey::kTotalRowCount,
        std::to_string(tableScanNode->getRowCount()));
  } else {
    this->setRuntimeMetric(
        OperatorMetricKey::kTotalRowCount, folly::to<std::string>(0));
  }
}

folly::dynamic TableScan::toJson() const {
  auto ret = SourceOperator::toJson();
  ret["status"] = curStatus_.load();
  return ret;
}

OperatorStats TableScan::stats(bool clear) {
  if (!noMoreSplits_) {
    VLOG(1) << "get stats when there exist more splits!" << std::endl;
    if (dataSource_) {
      curStatus_ = "getOutput: noMoreSplits_=1, updating stats_";
      auto connectorStats = dataSource_->runtimeStats();
      auto lockedStats = stats_.wlock();
      for (const auto& [name, counter] : connectorStats) {
        if (name == "ioWaitWallNanos") {
          ioWaitNanos_ += counter.value - lastIoWaitNanos_;
          lastIoWaitNanos_ = counter.value;
        }
        if (UNLIKELY(lockedStats->runtimeStats.count(name) == 0)) {
          lockedStats->runtimeStats.insert(
              std::make_pair(name, RuntimeMetric(counter.unit)));
        } else {
          BOLT_CHECK_EQ(lockedStats->runtimeStats.at(name).unit, counter.unit);
        }
        lockedStats->runtimeStats.at(name).addValue(counter.value);
      }
    }
  }
  return Operator::stats(clear);
}

bool TableScan::shouldYield(StopReason taskStopReason, size_t startTimeMs)
    const {
  // Checks task-level yield signal, driver-level yield signal and table scan
  // output processing time limit.
  //
  // NOTE: if the task is being paused, then we shall continue execution as we
  // won't yield the driver thread but simply spinning (with on-thread time
  // sleep) until the task has been resumed.
  return (taskStopReason == StopReason::kYield ||
          driverCtx_->driver->shouldYield() ||
          ((getOutputTimeLimitMs_ != 0) &&
           (getCurrentTimeMs() - startTimeMs) >= getOutputTimeLimitMs_)) &&
      !driverCtx_->task->pauseRequested();
}

bool TableScan::shouldStop(StopReason taskStopReason) const {
  return taskStopReason != StopReason::kNone &&
      taskStopReason != StopReason::kYield;
}

RowVectorPtr TableScan::getOutput() {
  SuspendedSection suspendedSection(driverCtx_->driver);
  auto exitCurStatusGuard = folly::makeGuard([this]() { curStatus_ = ""; });

  if (noMoreSplits_) {
    return nullptr;
  }

  const auto& queryConfig = operatorCtx_->task()->queryCtx()->queryConfig();

  curStatus_ = "getOutput: enter";
  const auto startTimeMs = getCurrentTimeMs();
  for (;;) {
    if (needNewSplit_) {
      // Check if our Task needs us to yield or we've been running for too long
      // w/o producing a result. In this case we return with the Yield blocking
      // reason and an already fulfilled future.
      curStatus_ = "getOutput: task->shouldStop";
      const StopReason taskStopReason = driverCtx_->task->shouldStop();
      if (shouldStop(taskStopReason) ||
          shouldYield(taskStopReason, startTimeMs)) {
        blockingReason_ = BlockingReason::kYield;
        blockingFuture_ = ContinueFuture{folly::Unit{}};
        // A point for test code injection.
        TestValue::adjust(
            "bytedance::bolt::exec::TableScan::getOutput::bail", this);
        return nullptr;
      }

      // A point for test code injection.
      TestValue::adjust("bytedance::bolt::exec::TableScan::getOutput", this);

      exec::Split split;
      curStatus_ = "getOutput: task->getSplitOrFuture";
      blockingReason_ = driverCtx_->task->getSplitOrFuture(
          driverCtx_->splitGroupId,
          planNodeId(),
          split,
          blockingFuture_,
          maxPreloadedSplits_,
          splitPreloader_);
      if (blockingReason_ != BlockingReason::kNotBlocked) {
        return nullptr;
      }

      if (!split.hasConnectorSplit()) {
        noMoreSplits_ = true;
        pendingDynamicFilters_.clear();
        if (dataSource_) {
          curStatus_ = "getOutput: noMoreSplits_=1, updating stats_";
          auto connectorStats = dataSource_->runtimeStats();
          auto lockedStats = stats_.wlock();
          if (connectorStats.count("rawBytesRead<4k") > 0) {
            LOG(INFO)
                << "IO pattern: "
                << "totalBytesRead: "
                << succinctBytes(connectorStats.at("rawBytesRead").value)
                << ", totalScanTime: "
                << succinctNanos(connectorStats.at("totalScanTime").value)
                << ", readBytes < 4K: [scantime: "
                << succinctNanos(connectorStats.at("totalTimeRead<4k").value)
                << ", readCnt: " << connectorStats.at("cntRead<4k").value
                << ", readBytes: "
                << succinctBytes(connectorStats.at("rawBytesRead<4k").value)
                << "]"
                << ", 4k <= readBytes < 32K: [scantime: "
                << succinctNanos(connectorStats.at("totalTimeRead<32k").value)
                << ", readCnt: " << connectorStats.at("cntRead<32k").value
                << ", readBytes: "
                << succinctBytes(connectorStats.at("rawBytesRead<32k").value)
                << "]"
                << ", 32k <= readBytes < 128K: [scantime: "
                << succinctNanos(connectorStats.at("totalTimeRead<128k").value)
                << ", readCnt: " << connectorStats.at("cntRead<128k").value
                << ", readBytes: "
                << succinctBytes(connectorStats.at("rawBytesRead<128k").value)
                << "]"
                << ", readBytes >= 128K: [scantime: "
                << succinctNanos(connectorStats.at("totalTimeRead>=128k").value)
                << ", readCnt: " << connectorStats.at("cntRead>=128k").value
                << ", readBytes: "
                << succinctBytes(connectorStats.at("rawBytesRead>=128k").value)
                << "]";
          }
          for (const auto& [name, counter] : connectorStats) {
            if (name == "ioWaitWallNanos") {
              ioWaitNanos_ += counter.value - lastIoWaitNanos_;
              lastIoWaitNanos_ = counter.value;
            }
            if (UNLIKELY(lockedStats->runtimeStats.count(name) == 0)) {
              lockedStats->runtimeStats.insert(
                  std::make_pair(name, RuntimeMetric(counter.unit)));
            } else {
              BOLT_CHECK_EQ(
                  lockedStats->runtimeStats.at(name).unit, counter.unit);
            }
            lockedStats->runtimeStats.at(name).addValue(counter.value);
          }
          lockedStats->addRuntimeStat(
              "dynamicConcurrency", RuntimeCounter(this->getConcurrency()));
        }
        return nullptr;
      }

      if (FOLLY_UNLIKELY(splitTracer_ != nullptr)) {
        splitTracer_->write(split);
      }
      const auto& connectorSplit = split.connectorSplit;
      needNewSplit_ = false;

      NanosecondTimer splitTimer(&prepareSplitTimeNs_);

      // update currentSplitStr_ only when new split is added
      if (auto s = std::dynamic_pointer_cast<
              const connector::hive::HiveConnectorSplit>(connectorSplit)) {
        currentSplitStr_ = s->filePath;
      } else {
        currentSplitStr_ = "empty_split_str__cast_to_HiveConnectorSplit_failed";
      }

      BOLT_CHECK_EQ(
          connector_->connectorId(),
          connectorSplit->connectorId,
          "Got splits with different connector IDs");

      if (dataSource_ == nullptr) {
        curStatus_ = "getOutput: creating dataSource_";
        connectorQueryCtx_ = operatorCtx_->createConnectorQueryCtx(
            connectorSplit->connectorId,
            planNodeId(),
            connectorPool_,
            nullptr,
            &asyncThreadCtx_);
        dataSource_ = connector_->createDataSource(
            outputType_,
            tableHandle_,
            columnHandles_,
            connectorQueryCtx_,
            driverCtx_->task->queryCtx()->queryConfig());
        for (const auto& entry : pendingDynamicFilters_) {
          dataSource_->addDynamicFilter(entry.first, entry.second);
        }
      }

      debugString_ = fmt::format(
          "Split [{}] Task {}",
          connectorSplit->toString(),
          operatorCtx_->task()->taskId());

      ExceptionContextSetter exceptionContext(
          {[](BoltException::Type /*exceptionType*/, auto* debugString) {
             return *static_cast<std::string*>(debugString);
           },
           &debugString_});

      if (connectorSplit->dataSource != nullptr) {
        curStatus_ = "getOutput: preloaded split";
        ++numPreloadedSplits_;
        // The AsyncSource returns a unique_ptr to a shared_ptr. The
        // unique_ptr will be nullptr if there was a cancellation.
        numReadyPreloadedSplits_ += connectorSplit->dataSource->hasValue();
        auto preparedDataSource = connectorSplit->dataSource->move();
        if (!preparedDataSource) {
          // There must be a cancellation.
          BOLT_CHECK(operatorCtx_->task()->isCancelled());
          return nullptr;
        }
        dataSource_->setFromDataSource(std::move(preparedDataSource));
      } else {
        curStatus_ = "getOutput: adding split";
        dataSource_->addSplit(connectorSplit);
      }
      curStatus_ = "getOutput: updating stats_.numSplits";
      ++stats_.wlock()->numSplits;
      curStatus_ = "getOutput: dataSource_->estimatedRowSize";
      bytesPerRowAverage_ = dataSource_->estimatedRowSize();
      readBatchSize_ = guessBatchSize(bytesPerRowAverage_);
      if (enableEstimateBytesPerRow_) {
        readBatchSize_ = std::min(64, readBatchSize_);
        bytesPerRowLastBatch_.reset();
        if (estimateBatchSizeFactor_ < 0.5) {
          estimateBatchSizeFactor_ = 0.5;
          bytesPerRowAverage_ = 0;
        }
      }
    }

    if (bytesPerRowLastBatch_.has_value()) {
      auto batchSize = guessBatchSize(bytesPerRowLastBatch_.value());
      readBatchSize_ =
          std::max<int32_t>(1, batchSize * estimateBatchSizeFactor_);
    }

    const auto ioTimeStartMicros = getCurrentTimeMicro();
    // Check for  cancellation since scans that filter everything out will not
    // hit the check in Driver.
    curStatus_ = "getOutput: task->isCancelled";
    if (operatorCtx_->task()->isCancelled()) {
      return nullptr;
    }
    ExceptionContextSetter exceptionContext(
        {[](BoltException::Type /*exceptionType*/, auto* debugString) {
           return *static_cast<std::string*>(debugString);
         },
         &debugString_});

    int readBatchSize = readBatchSize_;
    if (maxFilteringRatio_ > 0) {
      readBatchSize = static_cast<int>(readBatchSize / maxFilteringRatio_);
    }
    readBatchSize = std::min<int32_t>(
        maxReadBatchSize_, std::max<int32_t>(minReadBatchSize_, readBatchSize));
    curStatus_ = "getOutput: dataSource_->next";
    auto dataOptional = dataSource_->next(readBatchSize, blockingFuture_);
    curStatus_ = "getOutput: checkPreload";
    checkPreload();

    estimateBytesPerRow(dataOptional);

    {
      curStatus_ = "getOutput: updating stats_.dataSourceWallNanos";
      auto lockedStats = stats_.wlock();
      lockedStats->addRuntimeStat(
          "dataSourceWallNanos",
          RuntimeCounter(
              (getCurrentTimeMicro() - ioTimeStartMicros) * 1'000,
              RuntimeCounter::Unit::kNanos));

      if (!dataOptional.has_value()) {
        blockingReason_ = BlockingReason::kWaitForConnector;
        return nullptr;
      }

      curStatus_ = "getOutput: updating stats_.rawInput";
      lockedStats->rawInputPositions = dataSource_->getCompletedRows();
      lockedStats->rawInputBytes = dataSource_->getCompletedBytes();
      lockedStats->rawBytesReads = dataSource_->getCompletedBytesReads();
      lockedStats->cntReads = dataSource_->getCompletedCntReads();
      lockedStats->scanTimeReads = dataSource_->getCompletedScanTimeReads();

      auto data = dataOptional.value();
      if (data) {
        if (data->size() > 0) {
          lockedStats->addInputVector(data->estimateFlatSize(), data->size());
          constexpr int kMaxSelectiveBatchSizeMultiplier = 4;
          maxFilteringRatio_ = std::max(
              {maxFilteringRatio_,
               1.0 * data->size() / readBatchSize,
               1.0 / kMaxSelectiveBatchSizeMultiplier});
          // will be used in FilterProject operator
          driverCtx_->currentSplitStr = currentSplitStr_;

          outputRows_ += data->size();
          this->setRuntimeMetric(
              kHasBeenProcessedRowCount, folly::to<std::string>(outputRows_));

          return data;
        }
        continue;
      }
    }

    {
      curStatus_ = "getOutput: updating stats_.preloadedSplits";
      auto lockedStats = stats_.wlock();
      if (numPreloadedSplits_ > 0) {
        lockedStats->addRuntimeStat(
            "preloadSplits", RuntimeCounter(numPreloadedSplits_));
        numPreloadedSplits_ = 0;
      }
      if (numReadyPreloadedSplits_ > 0) {
        lockedStats->addRuntimeStat(
            "readyPreloadedSplits", RuntimeCounter(numReadyPreloadedSplits_));
        numReadyPreloadedSplits_ = 0;
      }
      if (prepareSplitTimeNs_ > 0) {
        lockedStats->addRuntimeStat(
            "prepareSplitTimeNs", RuntimeCounter(prepareSplitTimeNs_));
        prepareSplitTimeNs_ = 0;
      }
    }

    curStatus_ = "getOutput: task->splitFinished";
    driverCtx_->task->splitFinished();
    needNewSplit_ = true;
  }
}

void TableScan::preload(std::shared_ptr<connector::ConnectorSplit> split) {
  // The AsyncSource returns a unique_ptr to the shared_ptr of the
  // DataSource. The callback may outlive the Task, hence it captures
  // a shared_ptr to it. This is required to keep memory pools live
  // for the duration. The callback checks for task cancellation to
  // avoid needless work.
  split->dataSource = std::make_unique<AsyncSource<connector::DataSource>>(
      [type = outputType_,
       table = tableHandle_,
       columns = columnHandles_,
       connector = connector_,
       ctx = operatorCtx_->createConnectorQueryCtx(
           split->connectorId,
           planNodeId(),
           connectorPool_,
           nullptr,
           &asyncThreadCtx_),
       task = operatorCtx_->task(),
       pendingDynamicFilters = pendingDynamicFilters_,
       split]() -> std::unique_ptr<connector::DataSource> {
        if (task->isCancelled()) {
          return nullptr;
        }
        auto debugString =
            fmt::format("Split {} Task {}", split->toString(), task->taskId());
        ExceptionContextSetter exceptionContext(
            {[](BoltException::Type /*exceptionType*/, auto* debugString) {
               return *static_cast<std::string*>(debugString);
             },
             &debugString});
        auto ptr = connector->createDataSource(
            type, table, columns, ctx, task->queryCtx()->queryConfig());
        if (task->isCancelled()) {
          return nullptr;
        }
        for (const auto& entry : pendingDynamicFilters) {
          ptr->addDynamicFilter(entry.first, entry.second);
        }
        ptr->addSplit(split);
        return ptr;
      },
      true);
}

void TableScan::checkPreload() {
  auto executor = connector_->executor();
  if (maxSplitPreloadPerDriver_ == 0 || !executor ||
      !connector_->supportsSplitPreload() || !asyncThreadCtx_.allowPreload()) {
    return;
  }
  if (dataSource_->allPrefetchIssued()) {
    maxPreloadedSplits_ = driverCtx_->task->numDrivers(driverCtx_->driver) *
        maxSplitPreloadPerDriver_;
    if (!splitPreloader_) {
      splitPreloader_ =
          [executor, this](std::shared_ptr<connector::ConnectorSplit> split) {
            preload(split);

            executor->add([connectorSplit = split]() mutable {
              connectorSplit->dataSource->prepare();
              connectorSplit.reset();
            });
          };
    }
  }
}

bool TableScan::isFinished() {
  return noMoreSplits_;
}

void TableScan::addDynamicFilter(
    const core::PlanNodeId& producer,
    column_index_t outputChannel,
    const std::shared_ptr<common::Filter>& filter) {
  if (dataSource_) {
    dataSource_->addDynamicFilter(outputChannel, filter);
  }
  auto& currentFilter = pendingDynamicFilters_[outputChannel];
  if (currentFilter) {
    currentFilter = currentFilter->mergeWith(filter.get());
  } else {
    currentFilter = filter;
  }
  pendingDynamicFilters_.emplace(outputChannel, currentFilter);
  stats_.wlock()->dynamicFilterStats.producerNodeIds.emplace(producer);
}

void TableScan::estimateBytesPerRow(
    const std::optional<RowVectorPtr>& dataOptional) {
  if (!enableEstimateBytesPerRow_ || !dataOptional || !dataOptional.value()) {
    return;
  }
  auto size = dataOptional.value()->size();
  if (size <= 0) {
    return;
  }

  auto batchByets = dataOptional.value()->usedSize();
  auto bytesPerRow = batchByets / size;

  auto adjustBatchSizeFactor = [this]() {
    estimateBatchSizeFactor_ =
        std::max(estimateBatchSizeFactor_ / 2, MinEstimateBatchSizeFactor);
  };
  auto isLargeDiff = [this](int64_t l, int64_t r, int32_t multiples) -> bool {
    return l * multiples / estimateBatchSizeFactor_ < r ||
        l > r * multiples / estimateBatchSizeFactor_;
  };

  if (bytesPerRowAverage_ > 0 &&
      isLargeDiff(bytesPerRow, bytesPerRowAverage_, 5)) {
    adjustBatchSizeFactor();
    bytesPerRowAverage_ = 0;
  } else if (
      bytesPerRowLastBatch_.has_value() &&
      isLargeDiff(bytesPerRow, bytesPerRowLastBatch_.value(), 3)) {
    adjustBatchSizeFactor();
  }
  bytesPerRowLastBatch_ = bytesPerRow;
}

void TableScan::close() {
  if (dataSource_) {
    dataSource_->close(); // release all bufferedInputs(loads)
  }
  // wait all async threads to be finished
  uint64_t waitMs;
  {
    MicrosecondTimer timer(&waitMs);
    asyncThreadCtx_.wait();
  }

  LOG_IF(INFO, waitMs > 60000)
      << "TableScan close wait async thread ctx cost: " << waitMs << "ms, task "
      << operatorCtx_->task()->taskId()
      << " state = " << operatorCtx_->task()->state();

  SourceOperator::close();
}

} // namespace bytedance::bolt::exec
