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

#include "bolt/common/base/SpillStats.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/time/CpuWallTimer.h"
#include "bolt/expression/ExprStats.h"
namespace bytedance::bolt::exec {

struct MemoryStats {
  uint64_t userMemoryReservation{0};
  uint64_t revocableMemoryReservation{0};
  uint64_t systemMemoryReservation{0};
  uint64_t peakUserMemoryReservation{0};
  uint64_t peakSystemMemoryReservation{0};
  uint64_t peakTotalMemoryReservation{0};
  uint64_t numMemoryAllocations{0};

  void add(const MemoryStats& other) {
    userMemoryReservation += other.userMemoryReservation;
    revocableMemoryReservation += other.revocableMemoryReservation;
    systemMemoryReservation += other.systemMemoryReservation;
    peakUserMemoryReservation =
        std::max(peakUserMemoryReservation, other.peakUserMemoryReservation);
    peakSystemMemoryReservation = std::max(
        peakSystemMemoryReservation, other.peakSystemMemoryReservation);
    peakTotalMemoryReservation =
        std::max(peakTotalMemoryReservation, other.peakTotalMemoryReservation);
    numMemoryAllocations += other.numMemoryAllocations;
  }

  void clear() {
    userMemoryReservation = 0;
    revocableMemoryReservation = 0;
    systemMemoryReservation = 0;
    peakUserMemoryReservation = 0;
    peakSystemMemoryReservation = 0;
    peakTotalMemoryReservation = 0;
    numMemoryAllocations = 0;
  }

  static MemoryStats memStatsFromPool(const memory::MemoryPool* pool) {
    const auto poolStats = pool->stats();
    MemoryStats memStats;
    memStats.userMemoryReservation = poolStats.currentBytes;
    memStats.systemMemoryReservation = 0;
    memStats.peakUserMemoryReservation = poolStats.peakBytes;
    memStats.peakSystemMemoryReservation = 0;
    memStats.peakTotalMemoryReservation = poolStats.peakBytes;
    memStats.numMemoryAllocations = poolStats.numAllocs;
    return memStats;
  }
};

/// Records the dynamic filter stats of an operator.
struct DynamicFilterStats {
  /// The set of plan node ids that produce the dynamic filter added to an
  /// operator. If it is empty, then there is no dynamic filter added.
  std::unordered_set<core::PlanNodeId> producerNodeIds;

  void clear() {
    producerNodeIds.clear();
  }

  void add(const DynamicFilterStats& other) {
    producerNodeIds.insert(
        other.producerNodeIds.begin(), other.producerNodeIds.end());
  }

  bool empty() const {
    return producerNodeIds.empty();
  }
};

struct OperatorStats {
  /// Initial ordinal position in the operator's pipeline.
  int32_t operatorId = 0;
  int32_t pipelineId = 0;
  core::PlanNodeId planNodeId;

  /// Some operators perform the logic describe in multiple consecutive plan
  /// nodes. For example, FilterProject operator maps to Filter node followed by
  /// Project node. In this case, runtime stats are collected for the combined
  /// operator and attached to the "main" plan node ID chosen by the operator.
  /// (Project node ID in case of FilterProject operator.) The operator can then
  /// provide a function to split the stats among all plan nodes that are being
  /// represented. For example, FilterProject would split the stats but moving
  /// cardinality reduction to Filter and making Project cardinality neutral.
  using StatsSplitter = std::function<std::vector<OperatorStats>(
      const OperatorStats& combinedStats)>;

  std::optional<StatsSplitter> statsSplitter;

  /// Name for reporting. We use Presto compatible names set at
  /// construction of the Operator where applicable.
  std::string operatorType;

  /// Number of splits (or chunks of work). Split can be a part of data file to
  /// read.
  int64_t numSplits{0};

  CpuWallTiming isBlockedTiming;

  /// Bytes read from raw source, e.g. compressed file or network connection.
  uint64_t rawInputBytes = 0;
  uint64_t rawInputPositions = 0;

  std::vector<uint64_t> rawBytesReads{0, 0, 0, 0};
  std::vector<uint64_t> cntReads{0, 0, 0, 0};
  std::vector<uint64_t> scanTimeReads{0, 0, 0, 0};

  CpuWallTiming addInputTiming;

  /// Bytes of input in terms of retained size of input vectors.
  uint64_t inputBytes = 0;
  uint64_t inputPositions = 0;

  /// Contains the dynamic filters stats if applied.
  DynamicFilterStats dynamicFilterStats;

  /// These operator-level stats are needed if you need to calculate the driver
  /// skewness within 1 operator as an intermediate step. These sums are updated
  /// upon each driver closing.
  uint64_t sumSquaredInputPositions = 0;
  uint64_t nonZeroInputPositionsNumDrivers = 0;

  /// Number of input batches / vectors. Allows to compute an average batch
  /// size.
  uint64_t inputVectors = 0;

  CpuWallTiming getOutputTiming;

  /// Bytes of output in terms of retained size of vectors.
  uint64_t outputBytes = 0;
  uint64_t outputPositions = 0;

  /// Number of output batches / vectors. Allows to compute an average batch
  /// size.
  uint64_t outputVectors = 0;

  uint64_t physicalWrittenBytes = 0;

  uint64_t blockedWallNanos = 0;

  CpuWallTiming finishTiming;

  // CPU time spent on background activities (activities that are not
  // running on driver threads). Operators are responsible to report background
  // CPU time at a reasonable time granularity.
  CpuWallTiming backgroundTiming;

  MemoryStats memoryStats;

  // Total bytes in memory for spilling
  uint64_t spilledInputBytes{0};

  // Total bytes written to file for spilling.
  uint64_t spilledBytes{0};

  // Total rows written for spilling.
  uint64_t spilledRows{0};

  // Total spilled partitions.
  uint32_t spilledPartitions{0};

  // Total current spilled files.
  uint32_t spilledFiles{0};
  uint32_t spillTotalTime{0};

  uint64_t sortOutputTime{0};
  uint64_t sortColToRowTime{0};
  uint64_t sortInSortTime{0};

  uint64_t windowAddInputTime{0};
  uint64_t windowOutputTime{0};
  uint64_t windowComputeWindowFunctionTime{0};
  uint64_t windowExtractColumnTime{0};
  uint64_t windowHasNextPartitionTime{0};
  uint64_t windowLoadFromSpillTime{0};
  uint64_t windowSpillTime{0};
  uint64_t buildPartitionTime{0};
  uint64_t windowSpilledRows{0};
  uint64_t windowSpilledFiles{0};
  uint64_t windowSpillTotalTime{0};

  // Last recorded values for lazy loading times for loads triggered by 'this'.
  int64_t lastLazyCpuNanos{0};
  int64_t lastLazyWallNanos{0};
  int64_t lastLazyInputBytes{0};

  // Total null keys processed by the operator.
  // Currently populated only by HashJoin/HashBuild.
  // HashProbe doesn't populate numNullKeys when build side is empty.
  int64_t numNullKeys{0};

  std::unordered_map<std::string, RuntimeMetric> runtimeStats;

  // A map of expression name to its respective stats.
  // These are only populated when a copy of the stats is returned via
  // Operator::stats(bool) API.
  std::unordered_map<std::string, ExprStats> expressionStats;

  int numDrivers = 0;

  bool isControlGroup;
  std::unordered_set<std::string> abMetrics;

  OperatorStats() = default;
  OperatorStats(
      int32_t _operatorId,
      int32_t _pipelineId,
      std::string _planNodeId,
      std::string _operatorType,
      bool isControlGroup,
      std::unordered_set<std::string> abtestMetrics)
      : operatorId(_operatorId),
        pipelineId(_pipelineId),
        planNodeId(std::move(_planNodeId)),
        operatorType(std::move(_operatorType)),
        isControlGroup(isControlGroup),
        abMetrics(abtestMetrics) {}

  void setStatSplitter(StatsSplitter splitter) {
    statsSplitter = std::move(splitter);
  }

  void addInputVector(uint64_t bytes, uint64_t positions) {
    inputBytes += bytes;
    inputPositions += positions;
    inputVectors += 1;
  }

  void addOutputVector(uint64_t bytes, uint64_t positions) {
    outputBytes += bytes;
    outputPositions += positions;
    outputVectors += 1;
  }

  void addRuntimeStat(const std::string& name, const RuntimeCounter& value);
  void setRuntimeStat(const std::string& name, const RuntimeCounter& value);
  void addSpillStats(const common::SpillStats& spillStats);
  void add(const OperatorStats& other);
  void clear();
  folly::dynamic serialize() const {
    folly::dynamic obj = folly::dynamic::object;
    obj["operatorId"] = operatorId;
    obj["pipelineId"] = pipelineId;
    obj["planNodeId"] = planNodeId;
    obj["operatorType"] = operatorType;
    obj["numSplits"] = numSplits;
    obj["rawInputBytes"] = rawInputBytes;
    obj["rawInputPositions"] = rawInputPositions;
    obj["addInputTiming"] = addInputTiming.serialize();
    obj["inputBytes"] = inputBytes;
    obj["inputPositions"] = inputPositions;
    obj["inputVectors"] = inputVectors;
    obj["getOutputTiming"] = getOutputTiming.serialize();
    obj["outputBytes"] = outputBytes;
    obj["outputPositions"] = outputPositions;
    obj["outputVectors"] = outputVectors;
    obj["physicalWrittenBytes"] = physicalWrittenBytes;
    obj["blockedWallNanos"] = blockedWallNanos;
    obj["finishTiming"] = finishTiming.serialize();
    obj["backgroundTiming"] = backgroundTiming.serialize();
    obj["memoryStats"] = folly::dynamic::object;
    obj["memoryStats"]["userMemoryReservation"] =
        memoryStats.userMemoryReservation;
    obj["memoryStats"]["revocableMemoryReservation"] =
        memoryStats.revocableMemoryReservation;
    obj["memoryStats"]["systemMemoryReservation"] =
        memoryStats.systemMemoryReservation;
    obj["memoryStats"]["peakUserMemoryReservation"] =
        memoryStats.peakUserMemoryReservation;
    obj["memoryStats"]["peakSystemMemoryReservation"] =
        memoryStats.peakSystemMemoryReservation;
    obj["memoryStats"]["peakTotalMemoryReservation"] =
        memoryStats.peakTotalMemoryReservation;
    obj["memoryStats"]["numMemoryAllocations"] =
        memoryStats.numMemoryAllocations;
    obj["spilledInputBytes"] = spilledInputBytes;
    obj["spilledBytes"] = spilledBytes;
    obj["spilledRows"] = spilledRows;
    obj["spillTotalTime"] = spillTotalTime;
    obj["sortOutputTime"] = sortOutputTime;
    obj["sortColToRowTime"] = sortColToRowTime;
    obj["sortInSortTime"] = sortInSortTime;
    obj["windowAddInputTime"] = windowAddInputTime;
    obj["windowComputeWindowFunctionTime"] = windowComputeWindowFunctionTime;
    obj["windowExtractColumnTime"] = windowExtractColumnTime;
    obj["windowOutputTime"] = windowOutputTime;
    obj["windowLoadFromSpillTime"] = windowLoadFromSpillTime;
    obj["windowSpillTime"] = windowSpillTime;
    obj["windowHasNextPartitionTime"] = windowHasNextPartitionTime;
    obj["buildPartitionTime"] = buildPartitionTime;
    obj["spilledPartitions"] = spilledPartitions;
    obj["spilledFiles"] = spilledFiles;
    obj["lastLazyCpuNanos"] = lastLazyCpuNanos;
    obj["lastLazyWallNanos"] = lastLazyWallNanos;
    obj["runtimeStats"] = folly::dynamic::object;
    for (const auto& [name, value] : runtimeStats) {
      obj["runtimeStats"][name] = value.serialize();
    }
    obj["numDrivers"] = numDrivers;
    return obj;
  }
};

} // namespace bytedance::bolt::exec
