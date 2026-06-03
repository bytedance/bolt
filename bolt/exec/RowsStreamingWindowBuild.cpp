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

#include "bolt/exec/RowsStreamingWindowBuild.h"
#include "bolt/common/base/SimdUtil.h"
#include "bolt/exec/ContainerRow2RowSerde.h"
#include "bolt/exec/RowsStreamingWindowPartition.h"
namespace bytedance::bolt::exec {
template <bool needSort>
RowsStreamingWindowBuild<needSort>::RowsStreamingWindowBuild(
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

template <bool needSort>
void RowsStreamingWindowBuild<needSort>::buildNextInputOrPartition(
    bool isFinished) {
  if (windowPartitions_.size() <= inputCurrentPartition_) {
    windowPartitions_.push_back(
        std::make_shared<
            RowsStreamingWindowPartition<RowFormat::kRowContainer>>(
            data_.get(),
            folly::Range<char**>(nullptr, nullptr),
            inversedInputChannels_,
            sortKeyInfo_,
            enableJit_));
  }

  windowPartitions_[inputCurrentPartition_]->addNewRows(inputRows_);

  if (isFinished) {
    windowPartitions_[inputCurrentPartition_]->setInputRowsFinished();
    inputCurrentPartition_++;
  }

  inputRows_.clear();
}

template <bool needSort>
void RowsStreamingWindowBuild<needSort>::buildNextInputOrPartitionFromSpill(
    bool isFinished,
    bool patialPartitionFinished,
    char* allocatedStart,
    unsigned long allocatedSize) {
  if (allocatedStart) {
    if (windowPartitions_.size() <= inputCurrentPartition_) {
      windowPartitions_.push_back(
          std::make_shared<
              RowsStreamingWindowPartition<RowFormat::kSerializedRows>>(
              data_.get(),
              folly::Range<char**>(nullptr, nullptr),
              inversedInputChannels_,
              sortKeyInfo_,
              enableJit_));
    }
    windowPartitions_[inputCurrentPartition_]->addNewRows(
        windowPartitions_[inputCurrentPartition_]
            ->getPartialPartitionFinished(),
        sortRows_,
        allocatedStart,
        allocatedSize);
  }

  windowPartitions_[inputCurrentPartition_]->setPartialPartitionFinished(
      patialPartitionFinished);

  if (isFinished) {
    windowPartitions_[inputCurrentPartition_]->setInputRowsFinished();
    inputCurrentPartition_++;
  }

  sortRows_.clear();
}

template <bool needSort>
void RowsStreamingWindowBuild<needSort>::addInput(RowVectorPtr input) {
  if constexpr (needSort) {
    return;
  }
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
      buildNextInputOrPartition(true);
    }

    // Wait for the peers to be ready in single partition; these peers are the
    // rows that have identical values in the ORDER BY clause.
    // row_number() is an exception
    if (previousRow_ != nullptr && inputRows_.size() >= numRowsPerOutput_ &&
        (ignorePeer_ ||
         compareRowsWithKeys(previousRow_, newRow, sortKeyInfo_))) {
      buildNextInputOrPartition(false);
    }

    inputRows_.push_back(newRow);
    previousRow_ = newRow;
  }
}

template <bool needSort>
void RowsStreamingWindowBuild<needSort>::noMoreInput() {
  if constexpr (!needSort) {
    buildNextInputOrPartition(true);
    return;
  }

  if (sortSpiller_ != nullptr) {
    finishLoadFromSpill_ = false;
    return;
  } else {
    buildPartition();
  }
}

template <bool needSort>
void RowsStreamingWindowBuild<needSort>::buildPartition() {
  if (sortRows_.size() == 0) {
    return;
  }
  char* newRow = nullptr;

  vector_size_t start = 0;
  while (start < sortRows_.size()) {
    auto next = findNextPartitionStartRow(start);

    inputRows_.insert(
        inputRows_.end(), sortRows_.begin() + start, sortRows_.begin() + next);
    buildNextInputOrPartition(true);

    start = next;
  }
  buildNextInputOrPartition(true);
}

template <bool needSort>
char* RowsStreamingWindowBuild<needSort>::storeRows() {
  BOLT_CHECK_GT(accumulatedLength_, 0);
  BOLT_CHECK_NOT_NULL(info_);
  char* newRows = nullptr;
  newRows =
      reinterpret_cast<char*>(data_->pool()->allocate(accumulatedLength_));
  int64_t offset = 0;
  for (auto i = 0; i < tmpSortedRows_.size(); i++) {
    auto dest = newRows + offset;
    ContainerRow2RowSerde::copyRow(
        dest, tmpSortedRows_[i], tmpSortedRowsSize_[i], *info_);
    offset += tmpSortedRowsSize_[i];
    sortRows_.push_back(dest);
  }
  tmpSortedRows_.clear();
  tmpSortedRowsSize_.clear();
  return newRows;
}

template <bool needSort>
void RowsStreamingWindowBuild<needSort>::loadNextPartitionFromSpill() {
  BOLT_CHECK_NULL(sortMerge_);
  BOLT_CHECK_NOT_NULL(rowBasedSpillSortMerger_);
  if (finishLoadFromSpill_) {
    return;
  }
  RowBasedSpillMergeStream* next = rowBasedSpillSortMerger_->next();
  if (!next) {
    auto lastPartition = windowPartitions_.size() - 1;
    BOLT_CHECK(
        (inputCurrentPartition_ >= lastPartition) && (lastPartition >= 0));
    windowPartitions_[lastPartition]->setPartialPartitionFinished(true);
    windowPartitions_[lastPartition]->setInputRowsFinished();
    finishLoadFromSpill_ = true;
    return;
  }
  info_ = &(next->getRowFormatInfo());

  int partitionOffset = -1;

  bool needCopy = false;
  int cnt = 0;

  for (;;) {
    next = rowBasedSpillSortMerger_->next();
    cnt++;
    if (next == nullptr) {
      BOLT_CHECK(accumulatedLength_ == 0);
      buildNextInputOrPartitionFromSpill(true, true, nullptr, 0);
      partitionOffset = -1;
      finishLoadFromSpill_ = true;
      break;
    }

    bool isEndOfBatch = false;
    const auto& currentBatch = next->current();
    const auto& currentBatchLengths = next->currentLengths();
    auto index = next->currentIndex(&isEndOfBatch);
    auto currentRow = currentBatch[index];
    auto currentLength = currentBatchLengths[index];

    bool newPartition = false;
    char* previousRow = nullptr;
    if (!tmpSortedRows_.empty()) {
      previousRow = tmpSortedRows_.back();
    } else if (!sortRows_.empty()) {
      previousRow = sortRows_.back();
    } else if (windowPartitions_.size() > 0) {
      auto lastWindowPartition =
          windowPartitions_[windowPartitions_.size() - 1];
      if (!(lastWindowPartition->getPartialPartitionFinished()) ||
          !(lastWindowPartition->getInputRowsFinished())) {
        previousRow = lastWindowPartition->getLastRow();
      }
    }
    if (previousRow) {
      CompareFlags compareFlags =
          CompareFlags::equality(CompareFlags::NullHandlingMode::kNullAsValue);

      for (vector_size_t i = 0; i < numPartitionKeys_; ++i) {
        if (compareRow(
                previousRow,
                currentRow,
                i,
                next->rowColumns(),
                next->rowType(),
                compareFlags)) {
          newPartition = true;
          break;
        }
      }
    }

    if (newPartition) {
      char* newRows = accumulatedLength_ > 0 ? storeRows() : nullptr;
      buildNextInputOrPartitionFromSpill(
          true, true, newRows, accumulatedLength_);
      accumulatedLength_ = 0;
      partitionOffset = -1;
    }

    BOLT_CHECK(currentLength > 0);
    accumulatedLength_ += currentLength;

    tmpSortedRows_.push_back(currentRow);
    tmpSortedRowsSize_.push_back(currentLength);
    if ((isEndOfBatch || cnt > maxOutputRows_) && accumulatedLength_ > 0) {
      BOLT_CHECK_GT(accumulatedLength_, 0);
      char* newRows = nullptr;
      newRows = storeRows();

      auto partialPartitionFinished =
          ((sortRows_.size() >= numRowsPerOutput_ || cnt > maxOutputRows_) &&
           ignorePeer_);

      buildNextInputOrPartitionFromSpill(
          false, partialPartitionFinished, newRows, accumulatedLength_);

      if (partialPartitionFinished) {
        next->popWithLengths();
        break;
      }

      partitionOffset = sortRows_.size();

      accumulatedLength_ = 0;
    }
    next->popWithLengths();
    if (cnt > maxOutputRows_) {
      break;
    }
  }
}

template <bool needSort>
std::shared_ptr<WindowPartition>
RowsStreamingWindowBuild<needSort>::nextPartition() {
  // erase previous partition
  if (outputCurrentPartition_ > 0 &&
      windowPartitions_[outputCurrentPartition_]) {
    windowPartitions_[outputCurrentPartition_]->erasePartition();
  }

  if (outputCurrentPartition_ > 0) {
    windowPartitions_[outputCurrentPartition_].reset();
  }

  return windowPartitions_[++outputCurrentPartition_];
}
template <bool needSort>
bool RowsStreamingWindowBuild<needSort>::hasNextPartition() {
  if constexpr (needSort) {
    if (windowPartitions_.size() > 0 &&
        outputCurrentPartition_ <= int(windowPartitions_.size() - 2)) {
      return true;
    }
    BOLT_CHECK_NULL(sortMerge_);
    if (rowBasedSpillSortMerger_ == nullptr) {
      return false;
    }
    loadNextPartitionFromSpill();
  }
  return windowPartitions_.size() > 0 &&
      outputCurrentPartition_ <= int(windowPartitions_.size() - 2);
}

template <bool needSort>
void RowsStreamingWindowBuild<needSort>::finish() {
  for (int i = 0; i < windowPartitions_.size(); i++) {
    if (windowPartitions_[i]) {
      windowPartitions_[i]->erasePartition();
      windowPartitions_[i].reset();
    }
  }

  sortRows_.clear();
  data_->clear();
  data_->pool()->release();
}

template class RowsStreamingWindowBuild<true>;
template class RowsStreamingWindowBuild<false>;

} // namespace bytedance::bolt::exec
