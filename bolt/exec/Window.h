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

#include <common/base/WindowStat.h>
#include "bolt/exec/Operator.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/WindowBuild.h"
#include "bolt/exec/WindowFunction.h"
#include "bolt/exec/WindowPartition.h"
namespace bytedance::bolt::exec {

// To test specific window build
enum class WindowBuildType {
  kUnspecified = 0, // no window build is specified
  kSortWindowBuild = 1, // sort window build
  kRowStreamingWindowBuild = 2, // row streaming window build
  kSpillableWindowBuild = 3 // spillable window build
};

class TestWindowInjection {
 public:
  explicit TestWindowInjection(WindowBuildType windowBuildType);

  ~TestWindowInjection();
};

/// This is a very simple in-Memory implementation of a Window Operator
/// to compute window functions.
///
/// This operator uses a very naive algorithm that sorts all the input
/// data with a combination of the (partition_by keys + order_by keys)
/// to obtain a full ordering of the input. We can easily identify
/// partitions while traversing this sorted data in order.
/// It is also sorted in the order required for the WindowFunction
/// to process it.
///
/// We will revise this algorithm in the future using a HashTable based
/// approach pending some profiling results.
class Window : public Operator {
 public:
  Window(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::WindowNode>& windowNode);

  /// Initialize the window functions from 'windowNode_' once by driver operator
  /// initialization. 'windowNode_' is reset after this call.
  void initialize() override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !noMoreInput_ && windowBuild_->needsInput();
  }

  void noMoreInput() override;

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_ &&
        (numRows_ == numProcessedRows_ ||
         (windowBuild_->hasOutputAll() &&
          (!currentPartition_ || !(currentPartition_->buildNextRows()))));
  }

  bool canReclaim() const override {
    // TODO Add support for spilling after noMoreInput().
    return Operator::canReclaim() && !noMoreInput_;
  }

  void reclaim(uint64_t targetBytes, memory::MemoryReclaimer::Stats& stats)
      override;

  RowVectorPtr getOutputFromSpilledPartition();

  std::optional<common::WindowStats> windowStats() const {
    common::WindowStats windowStats;
    windowStats.addInputTimeUs = addInputTimeUs_;
    windowStats.computeWindowFunctionTimeUs = computeWindowFunctionTimeUs_;
    windowStats.extractColumnTimeUs = extractColumnTimeUs_;
    windowStats.getOutputTimeUs = getOutputTimeUs_;
    windowStats.hasNextPartitionUs = hasNextPartitionUs_;
    windowStats.loadFromSpillTimeUs = loadFromSpillTimeUs_;
    windowStats.windowSpillTimeUs = windowSpillTimeUs_;
    windowStats.buildPartitionTimeUs = buildPartitionTimeUs_;

    if (windowBuild_->windowSpilledStats().has_value()) {
      auto windowSpilledStats = windowBuild_->windowSpilledStats().value();

      windowStats.windowSpillTotalTimeUs = windowSpilledStats.spillTotalTimeUs;
      windowStats.windowSpilledFiles = windowSpilledStats.spilledFiles;
      windowStats.windowSpilledFiles = windowSpilledStats.spilledRows;
    }
    return windowStats;
  }

  void recordWindowStats();

 private:
  // Used for k preceding/following frames. Index is the column index if k is a
  // column. value is used to read column values from the column index when k
  // is a column. The field constant stores constant k values.
  struct FrameChannelArg {
    column_index_t index;
    VectorPtr value;
    std::optional<int64_t> constant;
  };

  // Structure for the window frame for each function.
  struct WindowFrame {
    const core::WindowNode::WindowType type;
    const core::WindowNode::BoundType startType;
    const core::WindowNode::BoundType endType;
    // Set only when startType is BoundType::kPreceding or kFollowing.
    const std::optional<FrameChannelArg> start;
    // Set only when endType is BoundType::kPreceding or kFollowing.
    const std::optional<FrameChannelArg> end;
  };

  bool supportRowsStreaming();

  bool ignorePeer();
  // Creates WindowFunction and frame objects for this operator.
  void createWindowFunctions();

  // Creates the buffers for peer and frame row
  // indices to send in window function apply invocations.
  void createPeerAndFrameBuffers();

  // Compute the peer and frame buffers for rows between
  // startRow and endRow in the current partition.
  void computePeerAndFrameBuffers(vector_size_t startRow, vector_size_t endRow);

  // Updates all the state for the next partition.
  void callResetPartition();

  // Computes the result vector for a subset of the current
  // partition rows starting from startRow to endRow. A single partition
  // could span multiple output blocks and a single output block could
  // also have multiple partitions in it. So resultOffset is the
  // offset in the result vector corresponding to the current range of
  // partition rows.
  void callApplyForPartitionRows(
      vector_size_t startRow,
      vector_size_t endRow,
      vector_size_t resultOffset,
      const RowVectorPtr& result);

  // Gets the input columns of the current window partition
  // between startRow and endRow in result at resultOffset.
  void getInputColumns(
      vector_size_t startRow,
      vector_size_t endRow,
      vector_size_t resultOffset,
      const RowVectorPtr& result);

  // Computes the result vector for a single output block. The result
  // consists of all the input columns followed by the results of the
  // window function.
  // @return The number of rows processed in the loop.
  vector_size_t callApplyLoop(
      vector_size_t numOutputRows,
      const RowVectorPtr& result);

  // Converts WindowNode::Frame to Window::WindowFrame.
  WindowFrame createWindowFrame(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      const core::WindowNode::Frame& frame,
      const RowTypePtr& inputType);

  // Update frame bounds for kPreceding, kFollowing row frames.
  void updateKRowsFrameBounds(
      bool isKPreceding,
      const FrameChannelArg& frameArg,
      vector_size_t startRow,
      vector_size_t numRows,
      vector_size_t* rawFrameBounds);

  void updateFrameBounds(
      const WindowFrame& windowFrame,
      const bool isStartBound,
      const vector_size_t startRow,
      const vector_size_t numRows,
      const vector_size_t* rawPeerStarts,
      const vector_size_t* rawPeerEnds,
      vector_size_t* rawFrameBounds);

  void setWindowBuild(
      WindowBuildType windowBuildType,
      const common::SpillConfig* spillConfig,
      int64_t spillThreshold,
      bool ignorePeer,
      int32_t maxBatchRows,
      int64_t preferredBatchBytes);

  void setRowsStreamingWindowBuild(
      bool ignorePeer,
      const common::SpillConfig* spillConfig,
      int64_t spillThreshold);

  void setStreamingWindowBuild(
      bool ignorePeer,
      const common::SpillConfig* spillConfig,
      int64_t spillThreshold);

  void setSpillableWindowBuild(
      int32_t maxBatchRows,
      int64_t preferredBatchBytes,
      const common::SpillConfig* spillConfig,
      int64_t spillThreshold);

  // decide if (1) current window function is supported for spill
  // (2) if window function is aggregated (sum, max, min, count..) or not
  // (lead, lag)
  bool isSpillableWindowBuild();

  bool supportedNonAggWindowFuncOffsetBySpill(
      const core::WindowNode::Function& func);

  void createWindowFunctionsForSpillableWindowBuild();

  const vector_size_t numInputColumns_;

  bool enableJit;

  // HashStringAllocator required by functions that allocate out of line
  // buffers.
  HashStringAllocator stringAllocator_;

  // WindowBuild is used to store input rows and return WindowPartitions
  // for the processing.
  std::unique_ptr<WindowBuild> windowBuild_;

  std::vector<std::pair<column_index_t, core::SortOrder>> sortKeyInfo_;

  // The cached window plan node used for window function initialization. It is
  // reset after the initialization.
  std::shared_ptr<const core::WindowNode> windowNode_;

  // Used to access window partition rows and columns by the window
  // operator and functions. This structure is owned by the WindowBuild.
  std::shared_ptr<WindowPartition> currentPartition_;

  // Vector of WindowFunction objects required by this operator.
  // WindowFunction is the base API implemented by all the window functions.
  // The functions are ordered by their positions in the output columns.
  std::vector<std::unique_ptr<exec::WindowFunction>> windowFunctions_;

  // Vector of WindowFrames corresponding to each windowFunction above.
  // It represents the frame spec for the function computation.
  std::vector<WindowFrame> windowFrames_;

  // Number of input rows.
  vector_size_t numRows_ = 0;

  // Number of rows that be fit into an output block.
  vector_size_t numRowsPerOutput_;

  vector_size_t maxNumRowsPerOutput_ = 0;

  // The following 4 Buffers are used to pass peer and frame start and
  // end values to the WindowFunction::apply method. These
  // buffers can be allocated once and reused across all the getOutput
  // calls.
  // Only a single peer start and peer end buffer is needed across all
  // functions (as the peer values are based on the ORDER BY clause).
  BufferPtr peerStartBuffer_;
  BufferPtr peerEndBuffer_;
  // A separate BufferPtr is required for the frame indexes of each
  // function. Each function has its own frame clause and style. So we
  // have as many buffers as the number of functions.
  std::vector<BufferPtr> frameStartBuffers_;
  std::vector<BufferPtr> frameEndBuffers_;

  // Frame types for kPreceding or kFollowing could result in empty
  // frames if the frameStart > frameEnds, or frameEnds < firstPartitionRow
  // or frameStarts > lastPartitionRow. Such frames usually evaluate to NULL
  // in the window function.
  // This SelectivityVector captures the valid (non-empty) frames in the
  // buffer being worked on. The window function can use this to compute
  // output values.
  // There is one SelectivityVector per window function.
  std::vector<SelectivityVector> validFrames_;

  // Number of rows output from the WindowOperator so far. The rows
  // are output in the same order of the pointers in sortedRows. This
  // value is updated as the WindowFunction::apply() function is
  // called on the partition blocks.
  vector_size_t numProcessedRows_ = 0;

  // Tracks how far along the partition rows have been output.
  vector_size_t partitionOffset_ = 0;

  // When traversing input partition rows, the peers are the rows
  // with the same values for the ORDER BY clause. These rows
  // are equal in some ways and affect the results of ranking functions.
  // Since all rows between the peerStartRow_ and peerEndRow_ have the same
  // values for peerStartRow_ and peerEndRow_, we needn't compute
  // them for each row independently. Since these rows might
  // cross getOutput boundaries and be called in subsequent calls to
  // computePeerBuffers they are saved here.
  vector_size_t peerStartRow_ = 0;
  vector_size_t peerEndRow_ = 0;

  bool isSpillableWindowBuild_ = false;
  bool isAggWindowFunc_ = false;
  std::vector<TypePtr> windowResultTypes_;

  // When sort and window is merge, we need to sort the data in window operator
  bool needSort_ = true;

  bool enableJit_ = false;

  uint32_t maxOutputRows_;

  uint64_t addInputTimeUs_{0};
  uint64_t computeWindowFunctionTimeUs_{0};
  uint64_t extractColumnTimeUs_{0};
  uint64_t getOutputTimeUs_{0};
  uint64_t buildPartitionTimeUs_{0};
  uint64_t hasNextPartitionUs_{0};
  uint64_t loadFromSpillTimeUs_{0};
  uint64_t windowSpillTimeUs_{0};
  bool hasRecordSpillStats_ = false;
};

} // namespace bytedance::bolt::exec
