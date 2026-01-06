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

#pragma once

#include <exec/Spiller.h>
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/WindowPartition.h"
namespace bytedance::bolt::exec {
namespace {
std::vector<CompareFlags> makeSpillCompareFlags(
    int32_t numPartitionKeys,
    const std::vector<core::SortOrder>& sortingOrders) {
  std::vector<CompareFlags> compareFlags;
  compareFlags.reserve(numPartitionKeys + sortingOrders.size());

  for (auto i = 0; i < numPartitionKeys; ++i) {
    compareFlags.push_back({});
  }

  for (const auto& order : sortingOrders) {
    compareFlags.push_back(
        {order.isNullsFirst(), order.isAscending(), false /*equalsOnly*/});
  }

  return compareFlags;
}
} // namespace

// The Window operator needs to see all input rows, and separate them into
// partitions based on a partitioning key. There are many approaches to do
// this. e.g with a full-sort, HashTable, streaming etc. This abstraction
// is used by the Window operator to hold the input rows and provide
// partitions to it for processing. Varied implementations of the
// WindowBuild can use different algorithms.
class WindowBuild {
 public:
  WindowBuild(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      bolt::memory::MemoryPool* pool,
      const common::SpillConfig* spillConfig,
      tsan_atomic<bool>* nonReclaimableSection,
      uint64_t spillMemoryThreshold,
      bool enableJit);

  virtual ~WindowBuild() = default;

  // The Window operator invokes this function to check if the WindowBuild can
  // accept input. The Streaming Window build doesn't accept input if it has a
  // partition to output.
  virtual bool needsInput() = 0;

  virtual bool hasOutputAll() = 0;

  // Adds new input rows to the WindowBuild.
  virtual void addInput(RowVectorPtr input) = 0;

  // Can be called any time before noMoreInput().
  virtual void spill() = 0;

  /// Returns the spiller stats including total bytes and rows spilled so far.
  std::optional<common::SpillStats> spilledStats() const {
    return sortSpiller_ ? std::make_optional(sortSpiller_->stats())
                        : windowSpilledStats();
  }

  /// Returns the spiller stats including total bytes and rows spilled so far.
  virtual std::optional<common::SpillStats> windowSpilledStats() const {
    return std::nullopt;
  }

  // The Window operator invokes this function to indicate that no
  // more input rows will be passed from the Window operator to the
  // WindowBuild.
  // When using a sort based build, all input rows need to be
  // seen before any partitions are determined. So this function is
  // used to indicate to the WindowBuild that it can proceed with
  // building partitions.
  virtual void noMoreInput() = 0;

  // After all partitions being outputed, clear the memory used.
  virtual void finish() {
    return;
  }

  // Returns true if a new Window partition is available for the Window
  // operator to consume.
  virtual bool hasNextPartition() = 0;

  // The Window operator invokes this function to get the next Window partition
  // to pass along to the WindowFunction. The WindowPartition has APIs to access
  // the underlying columns of Window partition data.
  // Check hasNextPartition() before invoking this function. This function fails
  // if called when no partition is available.
  virtual std::shared_ptr<WindowPartition> nextPartition() = 0;

  virtual uint64_t* getloadFromSpillTime() {
    return nullptr;
  }

  virtual uint64_t* getWindowSpillTime() {
    return nullptr;
  }

  virtual uint64_t* getBuildPartitionTime() {
    return nullptr;
  }

  // Returns the average size of input rows in bytes stored in the
  // data container of the WindowBuild.
  std::optional<int64_t> estimateRowSize() {
    return data_->estimateRowSize();
  }

  std::optional<uint64_t> estimateOutputRowSize() const {
    return estimatedOutputRowSize_;
  }

  // Specifies the maximum number of rows to read per spill read operation.
  void setMaxOutputRows(uint32_t maxOutputRows) {
    maxOutputRows_ = maxOutputRows;
  }

  virtual void resetSpiller() {}

  void setNumRowsPerOutput(vector_size_t numRowsPerOutput) {
    numRowsPerOutput_ = numRowsPerOutput;
  }

  virtual void setIgnorePeer(bool ignorePeer) {
    BOLT_UNREACHABLE("should not call setIgnorePeer in base WindowBuild");
  }

  virtual void loadNextPartialPartitionFromSpill() {
    return;
  }

  virtual void setIfAggWindowFunc(bool ifAggWindowFunc) {
    BOLT_UNREACHABLE("should not call setIfAggWindowFunc in base WindowBuild");
  }

  virtual bool isAggWindowFunc() {
    BOLT_UNREACHABLE("should not call setIfAggWindowFunc in base WindowBuild");
  }

  void addInputCommon(RowVectorPtr input);

  void noMoreInputCommon();

  void setupSortSpiller();

  void sortSpill();

 protected:
  void ensureInputFits(const RowVectorPtr& input);

  void sortPartitions();

  vector_size_t findNextPartitionStartRow(vector_size_t start);

  bool compareRowsWithKeys(
      const char* lhs,
      const char* rhs,
      const std::vector<std::pair<column_index_t, core::SortOrder>>& keys);

  void updateEstimatedOutputRowSize();

  // The below 2 vectors represent the ChannelIndex of the partition keys
  // and the order by keys. These keyInfo are used for sorting by those
  // key combinations during the processing.
  // partitionKeyInfo_ is used to separate partitions in the rows.
  // sortKeyInfo_ is used to identify peer rows in a partition.
  std::vector<std::pair<column_index_t, core::SortOrder>> partitionKeyInfo_;
  std::vector<std::pair<column_index_t, core::SortOrder>> sortKeyInfo_;

  // Input columns in the order of: partition keys, sorting keys, the rest.
  std::vector<column_index_t> inputChannels_;

  // The mapping from original input column index to the index after column
  // reordering. This is the inversed mapping of inputChannels_.
  std::vector<column_index_t> inversedInputChannels_;

  // Input column types in 'inputChannels_' order.
  RowTypePtr inputType_;

  const common::SpillConfig* const spillConfig_;
  tsan_atomic<bool>* const nonReclaimableSection_;

  // The RowContainer holds all the input rows in WindowBuild. Columns are
  // already reordered according to inputChannels_.
  std::unique_ptr<RowContainer> data_;

  // The decodedInputVectors_ are reused across addInput() calls to decode
  // the partition and sort keys for the above RowContainer.
  std::vector<DecodedVector> decodedInputVectors_;

  // Number of input rows.
  vector_size_t numRows_ = 0;

  // Number of rows that be fit into an output block.
  vector_size_t numRowsPerOutput_;

  int followedTopNum_ = 0;

  // Spiller for input which should be sorted.
  std::unique_ptr<Spiller> sortSpiller_;

  const std::vector<CompareFlags> sortSpillCompareFlags_;

  std::unique_ptr<TreeOfLosers<SpillMergeStream>> sortMerge_;

  std::unique_ptr<TreeOfLosers<RowBasedSpillMergeStream>>
      rowBasedSpillSortMerger_;

  memory::MemoryPool* const pool_;

  std::vector<char*> sortRows_;

  std::vector<std::pair<column_index_t, core::SortOrder>> allKeysInfo_;

  std::optional<uint64_t> estimatedOutputRowSize_{};

  uint32_t maxOutputRows_ = 0;

#ifdef ENABLE_BOLT_JIT
  bolt::jit::CompiledModuleSP jitModule_;
#endif
  HybridSorter sorter_;
  RowRowCompare cmp_{nullptr};

  // The maximum size that an SortBuffer can hold in memory before spilling.
  // Zero indicates no limit.
  // NOTE: 'spillMemoryThreshold_' only applies if disk spilling is enabled.
  const uint64_t spillMemoryThreshold_;

  bool enableJit_ = false;

  // Tracks memory addresses and sizes of buffers holding spilled rows read back
  // from disk.
  std::vector<char*> allocatedStarts_;
  std::vector<size_t> allocatedSizes_;
  // Records the number of memory buffers allocated per partition for spill read
  // operations.
  std::vector<size_t> numBuffers_;

  unsigned long accumulatedLength_ = 0;
  std::vector<char*> tmpSortedRows_;
  std::vector<size_t> tmpSortedRowsSize_;
  const size_t numPartitionKeys_;
  const RowFormatInfo* info_ = nullptr;

  bool finishLoadFromSpill_ = true;
};

} // namespace bytedance::bolt::exec