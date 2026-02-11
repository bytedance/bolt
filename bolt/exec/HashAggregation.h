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

#include "bolt/exec/GroupingSet.h"
#include "bolt/exec/Operator.h"
namespace bytedance::bolt::exec {

class HashAggregation : public Operator {
 public:
  HashAggregation(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::AggregationNode>& aggregationNode);

  void initialize() override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !noMoreInput_ && !partialFull_;
  }

  void noMoreInput() override;

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  void reclaim(uint64_t targetBytes, memory::MemoryReclaimer::Stats& stats)
      override;

  void close() override;

 private:
  void updateRuntimeStats();

  void prepareOutput(vector_size_t size, bool isCompositeOutput);

  // Invoked to reset partial aggregation state if it was full and has been
  // flushed.
  void resetPartialOutputIfNeed();

  // Invoked on partial output flush to try to bump up the partial aggregation
  // memory usage if it needs. 'aggregationPct' is the ratio between the number
  // of output rows and the number of input rows as a percentage. It is a
  // measure of the effectiveness of the partial aggregation.
  void maybeIncreasePartialAggregationMemoryUsage(double aggregationPct);

  // True if we have enough rows and not enough reduction, i.e. more than
  // 'abandonPartialAggregationMinRows_' rows and more than
  // 'abandonPartialAggregationMinPct_' % of rows are unique.
  bool abandonPartialAggregationEarly(int64_t numOutput) const;

  // True if we have enough rows and reduction
  bool preferPartialSpill(int64_t numOutput, bool ignoreMinSampleRows);

  RowVectorPtr getDistinctOutput();

  // Invoked to record the spilling stats in operator stats after processing all
  // the inputs.
  void recordSpillStats();
  void recordSpillReadStats();

  std::shared_ptr<const core::AggregationNode> aggregationNode_;

  void updateEstimatedOutputRowSize();

  // Invoked to record runtime metrics every output
  void recordRuntimeMetrics();

  const bool isPartialOutput_;
  const bool isPartialStep_;
  const bool isGlobal_;
  const bool isDistinct_;
  const int64_t maxExtendedPartialAggregationMemoryUsage_;
  // Minimum number of rows to see before deciding to give up on partial
  // aggregation.
  const int32_t abandonPartialAggregationMinRows_;
  // Min unique rows pct for partial aggregation. If more than this many rows
  // are unique, the partial aggregation is not worthwhile.
  const volatile int32_t abandonPartialAggregationMinPct_;

  // Min unique rows pct for partial aggregation when memory usage reaches
  // maximum. If more than this many rows are unique, the partial aggregation is
  // not worthwhile.
  const volatile int32_t abandonPartialAggregationMinFinalPct_;

  // Max unique rows pct for partital aggregation's spill
  // If less than this many rows are unique, do spill
  // otherwise, do flush or abandon
  const volatile int32_t partialAggregationSpillMaxPct_;

  int64_t maxPartialAggregationMemoryUsage_;
  std::unique_ptr<GroupingSet> groupingSet_;

  // Size of a single output row estimated using
  // 'groupingSet_->estimateRowSize()'. If spilling, this value is set to max
  // 'groupingSet_->estimateRowSize()' across all accumulated data set.
  std::optional<int64_t> estimatedOutputRowSize_;

  bool partialFull_ = false;
  bool newDistincts_ = false;
  bool finished_ = false;
  // True if partial aggregation has been found to be non-reducing.
  bool abandonedPartialAggregation_{false};

  bool preferPartialSpill_{false};
  bool adaptiveAdjustment_{false};
  uint64_t skippedDataSizeThreshold_{0};
  uint64_t totalInputRows_{0};
  int64_t avgRowSize_{0};

  RowContainerIterator resultIterator_;
  bool pushdownChecked_ = false;
  bool mayPushdown_ = false;

  // Count the number of input rows. It is reset on partial aggregation output
  // flush.
  int64_t numInputRows_ = 0;
  // Count the number of output rows. It is reset on partial aggregation output
  // flush.
  int64_t numOutputRows_ = 0;

  int64_t maxInputBatchCount_ = 0;

  // Possibly reusable output vector.
  RowVectorPtr output_;

  uint32_t minOutputRows_;

  // contains only one aggregate function - vid_split udaf
  bool containsVidSplit_{false};

  // input RowVector can be CompositeRowVector or not
  bool acceptCompositeVectorInput_{false};
  bool supportRowBasedOutput_{false};
  std::vector<AggregateInfo> aggregatesForExtractColumns_;
  RowVectorPtr convertedInput_{nullptr};
};

} // namespace bytedance::bolt::exec
