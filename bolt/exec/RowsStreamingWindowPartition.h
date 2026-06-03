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

#include "bolt/exec/RowContainer.h"
#include "bolt/exec/WindowPartition.h"
namespace bytedance::bolt::exec {

/// RowsStreamingWindowPartition is to facilitate RowsStreamingWindowBuild by
/// processing rows within WindowPartition in a streaming manner.
template <RowFormat R = RowFormat::kRowContainer>
class RowsStreamingWindowPartition : public WindowPartitionImpl<R> {
 public:
  RowsStreamingWindowPartition(
      RowContainer* data,
      const folly::Range<char**>& rows,
      const std::vector<column_index_t>& inputMapping,
      const std::vector<std::pair<column_index_t, core::SortOrder>>&
          sortKeyInfo,
      bool enableJit = false);

  // Returns the number of rows in the current partial window partition,
  // including the offset within the full partition.
  vector_size_t numRows() const override {
    return currentPartition_ == -1
        ? 0
        : this->partition_.size() + partitionStartRows_[currentPartition_];
  }

  vector_size_t offsetInPartition() const override {
    return partitionStartRows_[currentPartition_];
  }

  bool supportRowsStreaming() const override {
    return true;
  }

  void setInputRowsFinished() override {
    inputRowsFinished_ = true;
  }

  bool getInputRowsFinished() override {
    return inputRowsFinished_;
  }

  void setPartialPartitionFinished(bool patialPartitionFinished) override {
    partialPartitionFinished_ = patialPartitionFinished;
  }

  bool getPartialPartitionFinished() override {
    return partialPartitionFinished_;
  }

  char* getLastRow() override {
    BOLT_CHECK_GT(sortedRows_.size(), 0);
    return sortedRows_.back();
  }

  void addNewRows(std::vector<char*> rows) override;

  void addNewRows(
      bool patialPartitionFinished,
      std::vector<char*> rows,
      char* allocatedStart,
      unsigned long allocatedSize) override;

  bool buildNextRows() override;

  void erasePartition() override;

  bool processFinished() const override {
    return (
        inputRowsFinished_ &&
        currentPartition_ == partitionStartRows_.size() - 2);
  }

 private:
  // Indicates whether all input rows have been added to sortedRows_
  bool inputRowsFinished_ = false;
  bool partialPartitionFinished_ = true;

  // Stores new rows added to the WindowPartition.
  std::vector<char*> sortedRows_;

  // Indices of the start row (in sortedRows_) of each partial partition.
  std::vector<vector_size_t> partitionStartRows_;

  // Current partial partition being output.
  vector_size_t currentPartition_ = -1;

  std::vector<char*> allocatedStarts_;
  std::vector<size_t> allocatedSizes_;
  std::vector<size_t> bufferStarts_;
};
} // namespace bytedance::bolt::exec
