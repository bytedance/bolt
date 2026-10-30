/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#pragma once

#include "bolt/exec/Spiller.h"
#include "bolt/exec/WindowBuild.h"
#include "bolt/exec/WindowFunction.h"
namespace bytedance::bolt::exec {

/// The SpillableWindowBuild is used when the input data is already sorted by
/// {partition keys + order by keys}. The logic identifies partition changes
/// when receiving input rows and splits out WindowPartitions for the Window
/// operator to process.
template <bool needSort>
class SpillableWindowBuild : public WindowBuild {
 public:
  SpillableWindowBuild(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      bolt::memory::MemoryPool* pool,
      const common::SpillConfig* spillConfig,
      tsan_atomic<bool>* nonReclaimableSection,
      uint32_t maxBatchRows,
      uint64_t preferredOutputBatchBytes,
      uint64_t spillMemoryThreshold,
      bool enableJit);

  void addInput(RowVectorPtr input) override;

  void spill() override;

  std::optional<common::SpillStats> windowSpilledStats() const override {
    return stats_;
  }

  void noMoreInput() override;

  void finish() override;

  bool hasNextPartition() override;

  std::shared_ptr<WindowPartition> nextPartition() override;

  bool needsInput() override {
    // No partitions are available or the currentPartition is the last available
    // one, so can consume input rows.
    // One exception is if currentPartition_ is an spilled partition, do no
    // consume inputs until the spilled partition is fully done
    return partitionStartRows_.size() == 1 ||
        (currentPartition_ == partitionStartRows_.size() - 2 &&
         spillers_[currentPartition_] == nullptr);
  }

  bool hasOutputAll() override {
    // No partitions are available or the currentPartition is the last available
    // one, so can consume input rows.
    return (currentPartition_ >= partitionStartRows_.size() - 2) &
        finishLoadFromSpill_;
  }

  uint64_t* getloadFromSpillTime() override {
    return &loadFromSpillTimeUs_;
  }

  uint64_t* getWindowSpillTime() override {
    return &windowSpillTimeUs_;
  }

  uint64_t* getBuildPartitionTime() override {
    return &buildPartitionTimeUs_;
  }

  void setWindowFunction(
      std::vector<std::unique_ptr<exec::WindowFunction>>&& windowFunctions) {
    windowFunctions_ = std::move(windowFunctions);
  }

  void resetSpiller() override;

  void setIfAggWindowFunc(bool ifAggWindowFunc) override {
    ifAggWindowFunc_ = ifAggWindowFunc;
  }

  bool isAggWindowFunc() override {
    return ifAggWindowFunc_;
  }
  // Aggregate Window Function (true): sum, count, min, max, avg
  // Non-aggregate Window Function (false): lead, lag, first_value ...
  bool ifAggWindowFunc_{false};

 private:
  void buildNextPartition();

  void setupSpiller();

  void ensureInputFits(const RowVectorPtr& input);

  std::unique_ptr<WindowPartition> createPartialPartition();

  void storeRows();

  void loadNextPartitionFromSpill();

  void ensureOutputFits();

  std::optional<size_t> rowLength_ = std::nullopt;

  int64_t fixedArgumentBytes_{0};
  int64_t variableArgumentCount_{0};
  int64_t fixedResultBytes_{0};
  int64_t variableResultCount_{0};

  // Vector of pointers to each input row in the data_ RowContainer.
  // Rows are erased from data_ when they are output from the
  // Window operator.
  std::vector<char*> sortedRows_;

  // Holds input rows within the current partition.
  std::vector<char*> inputRows_;

  // Indices of  the start row (in sortedRows_) of each partition in
  // the RowContainer data_. This auxiliary structure helps demarcate
  // partitions.
  std::vector<vector_size_t> partitionStartRows_;

  // Compare flags for partition and sorting keys. Compare flags for partition
  // keys are set to default values. Compare flags for sorting keys match
  // sorting order specified in the plan node.
  //
  // Used to sort 'data_' while spilling.

  // Used to compare rows based on partitionKeys.
  char* previousRow_ = nullptr;

  // Current partition being output. Used to construct WindowPartitions
  // during resetPartition.
  vector_size_t currentPartition_ = -1;

  vector_size_t storeCurrentPartition_ = -1;

  vector_size_t currentSpilledPartition_ = 0;

  bool lastRun_ = false;

  bool doInitialize_ = false;

  // temporarily store spilled rows for one partition
  uint64_t spilledNumRows_ = 0;

  std::vector<std::unique_ptr<Spiller>> spillers_;

  std::vector<std::unique_ptr<UnorderedStreamReader<BatchStream>>> merges_;

  std::vector<std::unique_ptr<exec::WindowFunction>> windowFunctions_;

  bolt::memory::MemoryPool* pool_;

  common::SpillStats stats_;

  uint64_t maxBatchRows_{0};

  uint64_t preferredOutputBatchBytes_{0};

  uint64_t lastRowSize_{0};

  const RowTypePtr outputType_;

  uint64_t loadFromSpillTimeUs_ = 0;

  uint64_t windowSpillTimeUs_ = 0;

  uint64_t buildPartitionTimeUs_ = 0;

  int64_t lastErasePartition_ = -1;
};

} // namespace bytedance::bolt::exec
