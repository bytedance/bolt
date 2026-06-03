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

#include "bolt/exec/StreamingWindowBuild.h"
namespace bytedance::bolt::exec {

StreamingWindowBuild::StreamingWindowBuild(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    bolt::memory::MemoryPool* pool,
    const common::SpillConfig* spillConfig,
    tsan_atomic<bool>* nonReclaimableSection,
    uint64_t spillMemoryThreshold,
    bool enableJit)
    : WindowBuild(
          windowNode,
          pool,
          spillConfig,
          nonReclaimableSection,
          spillMemoryThreshold,
          enableJit) {}

void StreamingWindowBuild::buildNextPartition() {
  partitionStartRows_.push_back(sortedRows_.size());
  sortedRows_.insert(sortedRows_.end(), inputRows_.begin(), inputRows_.end());
  inputRows_.clear();
}

void StreamingWindowBuild::addInput(RowVectorPtr input) {
  for (auto i = 0; i < inputChannels_.size(); ++i) {
    decodedInputVectors_[i].decode(*input->childAt(inputChannels_[i]));
  }

  for (auto row = 0; row < input->size(); ++row) {
    char* newRow = data_->newRow();
    for (auto col = 0; col < input->childrenSize(); ++col) {
      data_->store(decodedInputVectors_[col], row, newRow, col);
    }
    if (previousRow_ != nullptr &&
        compareRowsWithKeys(previousRow_, newRow, partitionKeyInfo_)) {
      buildNextPartition();
    }

    inputRows_.push_back(newRow);
    previousRow_ = newRow;
  }
}

void StreamingWindowBuild::noMoreInput() {
  buildNextPartition();

  // Help for last partition related calculations.
  partitionStartRows_.push_back(sortedRows_.size());
}

std::shared_ptr<WindowPartition> StreamingWindowBuild::nextPartition() {
  BOLT_CHECK_GT(partitionStartRows_.size(), 0, "No window partitions available")

  currentPartition_++;
  BOLT_CHECK_LE(
      currentPartition_,
      partitionStartRows_.size() - 2,
      "All window partitions consumed");

  // Erase previous partition.
  if (currentPartition_ > 0 &&
      partitionStartRows_.size() - currentPartition_ <= 3) {
    auto numPreviousPartitionRows = partitionStartRows_[currentPartition_];
    data_->eraseRows(
        folly::Range<char**>(sortedRows_.data(), numPreviousPartitionRows));
    sortedRows_.erase(
        sortedRows_.begin(), sortedRows_.begin() + numPreviousPartitionRows);
    for (int i = currentPartition_; i < partitionStartRows_.size(); i++) {
      partitionStartRows_[i] =
          partitionStartRows_[i] - numPreviousPartitionRows;
    }
  }

  auto partitionSize = partitionStartRows_[currentPartition_ + 1] -
      partitionStartRows_[currentPartition_];
  auto partition = folly::Range(
      sortedRows_.data() + partitionStartRows_[currentPartition_],
      partitionSize);

  return std::make_shared<WindowPartitionImpl<RowFormat::kRowContainer>>(
      data_.get(), partition, inversedInputChannels_, sortKeyInfo_, enableJit_);
}

bool StreamingWindowBuild::hasNextPartition() {
  return partitionStartRows_.size() > 0 &&
      currentPartition_ < int(partitionStartRows_.size() - 2);
}

} // namespace bytedance::bolt::exec
