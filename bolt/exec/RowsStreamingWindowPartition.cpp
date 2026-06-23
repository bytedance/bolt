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

#include "bolt/exec/RowsStreamingWindowPartition.h"
namespace bytedance::bolt::exec {

template <RowFormat R>
RowsStreamingWindowPartition<R>::RowsStreamingWindowPartition(
    RowContainer* data,
    const folly::Range<char**>& rows,
    const std::vector<column_index_t>& inputMapping,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo,
    bool enableJit)
    : WindowPartitionImpl<R>(data, rows, inputMapping, sortKeyInfo, enableJit) {
  partitionStartRows_.push_back(0);
  bufferStarts_.push_back(0);
}

template <RowFormat R>
void RowsStreamingWindowPartition<R>::addNewRows(std::vector<char*> rows) {
  partitionStartRows_.push_back(partitionStartRows_.back() + rows.size());

  sortedRows_.insert(sortedRows_.end(), rows.begin(), rows.end());
}

template <RowFormat R>
void RowsStreamingWindowPartition<R>::addNewRows(
    bool patialPartitionFinished,
    std::vector<char*> rows,
    char* allocatedStart,
    unsigned long allocatedSize) {
  auto size = rows.size();
  sortedRows_.insert(sortedRows_.end(), rows.begin(), rows.end());

  allocatedStarts_.push_back(allocatedStart);
  allocatedSizes_.push_back(allocatedSize);

  if (patialPartitionFinished) {
    partitionStartRows_.push_back(partitionStartRows_.back() + size);
    bufferStarts_.push_back(bufferStarts_.back() + 1);
  } else {
    partitionStartRows_[partitionStartRows_.size() - 1] += size;
    bufferStarts_.back()++;
  }
}

template <RowFormat R>
bool RowsStreamingWindowPartition<R>::buildNextRows() {
  if (currentPartition_ >= int(partitionStartRows_.size() - 2) ||
      !getPartialPartitionFinished())
    return false;

  currentPartition_++;

  // Erase previous rows in current partition.
  if (currentPartition_ > 0) {
    auto numPreviousPartitionRows = partitionStartRows_[currentPartition_] -
        partitionStartRows_[currentPartition_ - 1];
    if constexpr (R == RowFormat::kSerializedRows) {
      for (auto i = bufferStarts_[currentPartition_ - 1];
           i < bufferStarts_[currentPartition_];
           i++) {
        if (allocatedSizes_[i] > 0) {
          this->data_->pool()->free(allocatedStarts_[i], allocatedSizes_[i]);
          allocatedSizes_[i] = 0;
        }
      }
    } else {
      BOLT_CHECK_EQ(R, RowFormat::kRowContainer);
      this->data_->eraseRows(
          folly::Range<char**>(sortedRows_.data(), numPreviousPartitionRows));
    }
    sortedRows_.erase(
        sortedRows_.begin(), sortedRows_.begin() + numPreviousPartitionRows);
  }

  auto partitionSize = partitionStartRows_[currentPartition_ + 1] -
      partitionStartRows_[currentPartition_];

  this->partition_ = folly::Range(sortedRows_.data(), partitionSize);
  return true;
}

template <RowFormat R>
void RowsStreamingWindowPartition<R>::erasePartition() {
  auto rowsSize = sortedRows_.size();
  if constexpr (R == RowFormat::kSerializedRows) {
    BOLT_CHECK_EQ(allocatedStarts_.size(), allocatedSizes_.size());
    for (auto i = 0; i < allocatedStarts_.size(); i++) {
      if (allocatedSizes_[i] == 0)
        continue;
      this->data_->pool()->free(allocatedStarts_[i], allocatedSizes_[i]);
      allocatedSizes_[i] = 0;
    }
  } else {
    BOLT_CHECK_EQ(R, RowFormat::kRowContainer);
    this->data_->eraseRows(folly::Range<char**>(sortedRows_.data(), rowsSize));
  }
  sortedRows_.erase(sortedRows_.begin(), sortedRows_.begin() + rowsSize);
}

template class RowsStreamingWindowPartition<RowFormat::kRowContainer>;
template class RowsStreamingWindowPartition<RowFormat::kSerializedRows>;

} // namespace bytedance::bolt::exec
