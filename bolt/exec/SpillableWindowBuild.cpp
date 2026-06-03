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

#include "bolt/exec/SpillableWindowBuild.h"
#include <exec/Spill.h>
#include "bolt/common/base/SimdUtil.h"
#include "bolt/exec/ContainerRow2RowSerde.h"
#include "bolt/exec/MemoryReclaimer.h"
#include "bolt/exec/RowToColumnVector.h"
namespace bytedance::bolt::exec {
template <bool needSort>
SpillableWindowBuild<needSort>::SpillableWindowBuild(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    bolt::memory::MemoryPool* pool,
    const common::SpillConfig* spillConfig,
    tsan_atomic<bool>* nonReclaimableSection,
    const uint32_t maxBatchRows,
    uint64_t preferredOutputBatchBytes,
    uint64_t spillMemoryThreshold,
    bool enableJit)
    : WindowBuild(
          windowNode,
          pool,
          spillConfig,
          nonReclaimableSection,
          spillMemoryThreshold,
          enableJit),
      pool_(pool),
      maxBatchRows_(maxBatchRows),
      preferredOutputBatchBytes_(preferredOutputBatchBytes),
      outputType_(windowNode->inputType()) {
  partitionStartRows_.push_back(0);
}
template <bool needSort>
void SpillableWindowBuild<needSort>::buildNextPartition() {
  if (currentSpilledPartition_ < spillers_.size() &&
      spillers_[currentSpilledPartition_] != nullptr) {
    partitionStartRows_.push_back(sortedRows_.size());
    lastRun_ = true;
    // spill remaining data in inputRows
    spill();
    // get Aggregate results
    auto spillPartition = spillers_[currentSpilledPartition_]->finishSpill();
    stats_ += spillers_[currentSpilledPartition_]->stats();
    BOLT_CHECK_EQ(
        spilledNumRows_,
        spillers_[currentSpilledPartition_]->stats().spilledRows);
    if constexpr (needSort) {
      merges_.push_back(spillPartition.createUnorderedReader(
          pool_,
          spillConfig_->spillUringEnabled,
          (spillConfig_->rowBasedSpillMode !=
           common::RowBasedSpillMode::DISABLE)));
    } else {
      merges_.push_back(spillPartition.createUnorderedReader(pool_));
    }
    // RowContainer is empty, release memory
    if (data_->numRows() == 0) {
      data_->clear();
      data_->pool()->release();
    }
  } else {
    merges_.push_back(nullptr);
    spillers_.push_back(nullptr);
    sortedRows_.insert(sortedRows_.end(), inputRows_.begin(), inputRows_.end());
    partitionStartRows_.push_back(sortedRows_.size());
    inputRows_.clear();
  }
}

template <bool needSort>
void SpillableWindowBuild<needSort>::setupSpiller() {
  auto spiller = std::make_unique<Spiller>(
      Spiller::Type::kOrderByOutput,
      data_.get(),
      inputType_,
      spillConfig_,
      maxBatchRows_);
  spiller->setSpillConfig(spillConfig_);
  spillers_.emplace_back(std::move(spiller));
  lastRun_ = false;
  doInitialize_ = true;
  spilledNumRows_ = 0;
}

template <bool needSort>
void SpillableWindowBuild<needSort>::spill() {
  if (inputRows_.size() == 0) {
    return;
  }
  if (spillers_.size() < currentSpilledPartition_ + 1) {
    setupSpiller();
  }

  bool keepLastRow =
      (inputRows_[inputRows_.size() - 1] == previousRow_ && !lastRun_);
  if (!sortSpiller_) {
    RowVectorPtr lastRowPtr = nullptr;
    if (keepLastRow) {
      if (inputRows_.size() == 1) {
        return;
      }

      lastRowPtr = BaseVector::create<RowVector>(
          inputType_, 1, memory::spillMemoryPool());
      for (auto i = 0; i < inputType_->size(); ++i) {
        data_->extractColumn(
            &previousRow_, 1, data_->columnAt(i), 0, lastRowPtr->childAt(i));
      }

      // pop_back() here only for aggregate window function
      // for non-aggregate window function (lag, lead), need to keep the
      // previousRow in inputRows_ when create partialPartiton
      // after creating partialPartiton, pop_back() from inputRows_
      if (isAggWindowFunc()) {
        inputRows_.pop_back();
      }
    }

    // calculate results before spilling
    auto currentPartition = createPartialPartition();
    if (keepLastRow && !isAggWindowFunc()) {
      inputRows_.pop_back();
    }
    for (auto i = 0; i < windowFunctions_.size(); ++i) {
      windowFunctions_[i]->resetPartition(currentPartition.get());
      windowFunctions_[i]->computeSpillableAggregate(doInitialize_);
    }
    doInitialize_ = false;

    spillers_[currentSpilledPartition_]->spill(inputRows_, lastRun_);

    spilledNumRows_ += inputRows_.size();

    if (inputRows_.size() == data_->numRows()) {
      data_->clear();
      data_->pool()->release();
    } else {
      data_->eraseRows(
          folly::Range<char**>(inputRows_.data(), inputRows_.size()));
    }
    inputRows_.clear();

    if (keepLastRow) {
      if (data_->numRows() == 0) {
        BOLT_CHECK(lastRowPtr != nullptr);
        for (auto i = 0; i < inputChannels_.size(); ++i) {
          decodedInputVectors_[i].decode(*lastRowPtr->childAt(i));
        }
        char* newRow = data_->newRow();
        for (auto col = 0; col < lastRowPtr->childrenSize(); ++col) {
          data_->store(decodedInputVectors_[col], 0, newRow, col);
        }
        previousRow_ = newRow;
      }
      inputRows_.push_back(previousRow_);
    }
  } else {
    char* lastRowPtr = nullptr;

    if (keepLastRow) {
      if (inputRows_.size() == 1) {
        return;
      }
      BOLT_CHECK_GT(lastRowSize_, 0);
      BOLT_CHECK_EQ(numBuffers_.size(), currentSpilledPartition_);
      lastRowPtr =
          reinterpret_cast<char*>(data_->pool()->allocate(lastRowSize_));
      BOLT_CHECK_NOT_NULL(info_);
      ContainerRow2RowSerde::copyRow(
          lastRowPtr, inputRows_.back(), lastRowSize_, *info_);

      // pop_back() here only for aggregate window function
      // for non-aggregate window function (lag, lead), need to keep the
      // previousRow in inputRows_ when create partialPartiton
      // after creating partialPartiton, pop_back() from inputRows_
      if (isAggWindowFunc()) {
        inputRows_.pop_back();
      }
    }

    // calculate results before spilling
    auto currentPartition = createPartialPartition();
    if (keepLastRow && !isAggWindowFunc()) {
      inputRows_.pop_back();
    }
    for (auto i = 0; i < windowFunctions_.size(); ++i) {
      windowFunctions_[i]->resetPartition(currentPartition.get());
      windowFunctions_[i]->computeSpillableAggregate(doInitialize_);
    }
    doInitialize_ = false;

    spillers_[currentSpilledPartition_]->setRowFormatInfo(true);
    spillers_[currentSpilledPartition_]->spill(inputRows_, lastRun_);

    spilledNumRows_ += inputRows_.size();

    int lastBuffer = (currentSpilledPartition_ == 0)
        ? 0
        : numBuffers_[currentSpilledPartition_ - 1];
    for (int i = allocatedStarts_.size() - 1; i >= lastBuffer; --i) {
      BOLT_CHECK_GE(i, lastBuffer);
      data_->pool()->free(allocatedStarts_[i], allocatedSizes_[i]);
      allocatedStarts_.pop_back();
      allocatedSizes_.pop_back();
    }
    inputRows_.clear();

    if (keepLastRow) {
      BOLT_CHECK(lastRowPtr != nullptr);
      allocatedStarts_.push_back(lastRowPtr);
      allocatedSizes_.push_back(lastRowSize_);

      previousRow_ = lastRowPtr;
      inputRows_.push_back(previousRow_);
    }
  }
}

template <bool needSort>
void SpillableWindowBuild<needSort>::ensureInputFits(
    const RowVectorPtr& input) {
  if (spillConfig_ == nullptr) {
    // Spilling is disabled.
    return;
  }

  if (data_->numRows() == 0) {
    // Nothing to spill.
    return;
  }

  // Test-only spill path.
  if (spillConfig_->testSpillPct > 0 && inputRows_.size() > 0) {
    spill();
    return;
  }

  auto [freeRows, outOfLineFreeBytes] = data_->freeSpace();
  const auto outOfLineBytes =
      data_->stringAllocator().retainedSize() - outOfLineFreeBytes;
  const auto outOfLineBytesPerRow = outOfLineBytes / data_->numRows();

  const auto currentUsage = data_->pool()->currentBytes();
  const auto minReservationBytes =
      currentUsage * spillConfig_->minSpillableReservationPct / 100;
  const auto availableReservationBytes = data_->pool()->availableReservation();
  const auto incrementBytes =
      data_->sizeIncrement(input->size(), outOfLineBytesPerRow * input->size());

  // First to check if we have sufficient minimal memory reservation.
  if (availableReservationBytes >= minReservationBytes) {
    if ((freeRows > input->size()) &&
        (outOfLineBytes == 0 ||
         outOfLineFreeBytes >= outOfLineBytesPerRow * input->size())) {
      // Enough free rows for input rows and enough variable length free space.
      return;
    }
  }

  // Check if we can increase reservation. The increment is the largest of twice
  // the maximum increment from this input and 'spillableReservationGrowthPct_'
  // of the current memory usage.
  const auto targetIncrementBytes = std::max<int64_t>(
      incrementBytes * 2,
      currentUsage * spillConfig_->spillableReservationGrowthPct / 100);
  {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (data_->pool()->maybeReserve(targetIncrementBytes)) {
      return;
    }
  }

  LOG(WARNING) << "Failed to reserve " << succinctBytes(targetIncrementBytes)
               << " for memory pool " << data_->pool()->name()
               << ", usage: " << succinctBytes(data_->pool()->currentBytes())
               << ", reservation: "
               << succinctBytes(data_->pool()->reservedBytes());
}

template <bool needSort>
void SpillableWindowBuild<needSort>::addInput(RowVectorPtr input) {
  if constexpr (needSort) {
    return;
  }
  for (auto i = 0; i < inputChannels_.size(); ++i) {
    decodedInputVectors_[i].decode(*input->childAt(inputChannels_[i]));
  }

  ensureInputFits(input);

  for (auto row = 0; row < input->size(); ++row) {
    char* newRow = data_->newRow();

    for (auto col = 0; col < input->childrenSize(); ++col) {
      data_->store(decodedInputVectors_[col], row, newRow, col);
    }

    if (previousRow_ != nullptr &&
        compareRowsWithKeys(previousRow_, newRow, partitionKeyInfo_)) {
      buildNextPartition();
      currentSpilledPartition_++;
    }

    inputRows_.push_back(newRow);
    previousRow_ = newRow;
  }
}

template <bool needSort>
void SpillableWindowBuild<needSort>::noMoreInput() {
  if constexpr (!needSort) {
    buildNextPartition();
    return;
  }

  if (numRows_ == 0) {
    return;
  }

  if (sortSpiller_) {
    finishLoadFromSpill_ = false;
    return;
  } else {
    char* newRow = nullptr;
    for (auto row = 0; row < sortRows_.size(); ++row) {
      newRow = sortRows_[row];

      if (previousRow_ != nullptr &&
          compareRowsWithKeys(previousRow_, newRow, partitionKeyInfo_)) {
        buildNextPartition();
        currentSpilledPartition_++;
      }

      inputRows_.push_back(newRow);
      previousRow_ = newRow;
    }
    buildNextPartition();
  }
}

template <bool needSort>
void SpillableWindowBuild<needSort>::finish() {
  for (int i = 0; i < allocatedSizes_.size(); i++) {
    if (allocatedSizes_[i] == 0) {
      continue;
    }
    data_->pool()->free(allocatedStarts_[i], allocatedSizes_[i]);
    allocatedSizes_[i] = 0;
  }
  sortRows_.clear();
  allocatedStarts_.clear();
  allocatedSizes_.clear();
  numBuffers_.clear();
  data_->clear();
  data_->pool()->release();
}

// Spill large partitions
template <bool needSort>
void SpillableWindowBuild<needSort>::ensureOutputFits() {
  if (spillConfig_->testSpillPct > 0 && inputRows_.size() > 0) {
    spill();
    return;
  }

  std::optional<size_t> rowLength = std::nullopt;
  if (rowLength_.has_value()) {
    rowLength = rowLength_;
  } else if (estimateRowSize().has_value()) {
    rowLength = estimateRowSize();
  }
  const uint64_t outputBufferSizeToReserve =
      (rowLength.has_value() ? (rowLength.value() * maxBatchRows_)
                             : preferredOutputBatchBytes_) *
      1.2;

  {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (data_->pool()->maybeReserve(outputBufferSizeToReserve)) {
      return;
    }
  }
  LOG(WARNING) << "Failed to reserve "
               << succinctBytes(outputBufferSizeToReserve)
               << " for memory pool " << data_->pool()->name()
               << ", usage: " << succinctBytes(data_->pool()->currentBytes())
               << ", reservation: "
               << succinctBytes(data_->pool()->reservedBytes());
}

template <bool needSort>
void SpillableWindowBuild<needSort>::storeRows() {
  BOLT_CHECK_GT(accumulatedLength_, 0);
  BOLT_CHECK_NOT_NULL(info_);
  char* newRows = nullptr;
  newRows =
      reinterpret_cast<char*>(data_->pool()->allocate(accumulatedLength_));
  allocatedStarts_.push_back(newRows);
  allocatedSizes_.push_back(accumulatedLength_);
  int64_t offset = 0;
  char* dest = nullptr;
  for (auto i = 0; i < tmpSortedRows_.size(); i++) {
    dest = newRows + offset;
    ContainerRow2RowSerde::copyRow(
        dest, tmpSortedRows_[i], tmpSortedRowsSize_[i], *info_);
    offset += tmpSortedRowsSize_[i];
    inputRows_.push_back(dest);
  }
  previousRow_ = dest;
  lastRowSize_ = tmpSortedRowsSize_.back();
  tmpSortedRows_.clear();
  tmpSortedRowsSize_.clear();
  accumulatedLength_ = 0;
}

template <bool needSort>
void SpillableWindowBuild<needSort>::loadNextPartitionFromSpill() {
  BOLT_CHECK_NOT_NULL(rowBasedSpillSortMerger_);
  ensureOutputFits();
  RowBasedSpillMergeStream* next = rowBasedSpillSortMerger_->next();
  if (finishLoadFromSpill_) {
    return;
  }
  if (!next) {
    buildNextPartition();
    numBuffers_.push_back(allocatedStarts_.size());
    finishLoadFromSpill_ = true;
    return;
  }
  info_ = &(next->getRowFormatInfo());

  int cnt = 0;
  size_t currentLength = 0;
  size_t totalLength = 0;

  for (;;) {
    next = rowBasedSpillSortMerger_->next();
    cnt++;
    if (next == nullptr) {
      BOLT_CHECK(accumulatedLength_ == 0);
      buildNextPartition();
      numBuffers_.push_back(allocatedStarts_.size());
      finishLoadFromSpill_ = true;
      break;
    }

    bool isEndOfBatch = false;
    const auto& currentBatch = next->current();
    const auto& currentBatchLengths = next->currentLengths();
    auto index = next->currentIndex(&isEndOfBatch);
    auto currentRow = currentBatch[index];
    currentLength = currentBatchLengths[index];
    totalLength += currentLength;

    bool newPartition = false;
    char* previousRow = nullptr;
    if (!tmpSortedRows_.empty()) {
      previousRow = tmpSortedRows_.back();
    } else if (!inputRows_.empty()) {
      previousRow = inputRows_.back();
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
      storeCurrentPartition_++;
      if (accumulatedLength_ > 0) {
        storeRows();
      }
      if (partitionStartRows_.size() > 0 && inputRows_.size() > 0) {
        buildNextPartition();
        currentSpilledPartition_++;
        numBuffers_.push_back(allocatedStarts_.size());
      }
    }

    BOLT_CHECK(currentLength > 0);
    accumulatedLength_ += currentLength;

    tmpSortedRows_.push_back(currentRow);
    tmpSortedRowsSize_.push_back(currentLength);
    if ((isEndOfBatch || cnt > maxBatchRows_) && accumulatedLength_ > 0) {
      storeRows();
    }
    next->popWithLengths();
    if (cnt > maxBatchRows_) {
      break;
    }
  }
  if (cnt > 0) {
    rowLength_ = totalLength / cnt;
  }
}

template <bool needSort>
std::shared_ptr<WindowPartition>
SpillableWindowBuild<needSort>::nextPartition() {
  BOLT_CHECK_GT(partitionStartRows_.size(), 0, "No window partitions available")
  currentPartition_++;
  BOLT_CHECK_LE(
      currentPartition_,
      partitionStartRows_.size() - 2,
      "All window partitions consumed");
  if (rowBasedSpillSortMerger_ != nullptr) {
    BOLT_CHECK_LE(
        currentPartition_,
        partitionStartRows_.size() - 2,
        "All window partitions consumed");

    if (currentPartition_ > 0 &&
        partitionStartRows_.size() - currentPartition_ <= 3) {
      auto numPreviousPartitionRows = partitionStartRows_[currentPartition_];

      auto lastEraseBuffer =
          (lastErasePartition_ == -1) ? 0 : numBuffers_[lastErasePartition_];
      int i = lastEraseBuffer;

      for (; i < numBuffers_[currentPartition_ - 1]; i++) {
        data_->pool()->free(allocatedStarts_[i], allocatedSizes_[i]);
        allocatedSizes_[i] = 0;
      }
      lastErasePartition_ = currentPartition_ - 1;

      sortedRows_.erase(
          sortedRows_.begin(), sortedRows_.begin() + numPreviousPartitionRows);
      for (int i = currentPartition_; i < partitionStartRows_.size(); i++) {
        partitionStartRows_[i] =
            partitionStartRows_[i] - numPreviousPartitionRows;
      }
    }
  } else {
    // Erase previous partition.
    if ((!needSort && currentPartition_ > 0 &&
         partitionStartRows_.size() - currentPartition_ <= 3)) {
      auto numPreviousPartitionRows = partitionStartRows_[currentPartition_];
      if (numPreviousPartitionRows > 0) {
        data_->eraseRows(
            folly::Range<char**>(sortedRows_.data(), numPreviousPartitionRows));
        sortedRows_.erase(
            sortedRows_.begin(),
            sortedRows_.begin() + numPreviousPartitionRows);
        for (int i = currentPartition_; i < partitionStartRows_.size(); i++) {
          partitionStartRows_[i] =
              partitionStartRows_[i] - numPreviousPartitionRows;
        }
      }
    }
  }

  auto partitionSize = partitionStartRows_[currentPartition_ + 1] -
      partitionStartRows_[currentPartition_];
  auto partition = folly::Range(
      sortedRows_.data() + partitionStartRows_[currentPartition_],
      partitionSize);

  if (spillers_.size() > 0 && spillers_[currentPartition_]) {
    BOLT_CHECK_NOT_NULL(merges_[currentPartition_]);
    std::vector<std::deque<VectorPtr>> aggregateResults;
    for (const auto& func : windowFunctions_) {
      aggregateResults.emplace_back(
          std::move(func->getAggregateResultVector()));
      func->cleanUp();
    }
    if (rowBasedSpillSortMerger_) {
      return std::make_shared<
          SpilledWindowPartition<RowFormat::kSerializedRows>>(
          data_.get(),
          partition,
          inversedInputChannels_,
          sortKeyInfo_,
          std::move(merges_[currentPartition_]),
          std::move(aggregateResults),
          spillers_[currentPartition_]->stats().spilledRows,
          outputType_,
          &inputChannels_,
          pool_,
          enableJit_);
    } else {
      return std::make_shared<SpilledWindowPartition<RowFormat::kRowContainer>>(
          data_.get(),
          partition,
          inversedInputChannels_,
          sortKeyInfo_,
          std::move(merges_[currentPartition_]),
          std::move(aggregateResults),
          spillers_[currentPartition_]->stats().spilledRows,
          outputType_,
          &inputChannels_,
          pool_,
          enableJit_);
    }
  } else if (rowBasedSpillSortMerger_) {
    return std::make_shared<WindowPartitionImpl<RowFormat::kSerializedRows>>(
        data_.get(),
        partition,
        inversedInputChannels_,
        sortKeyInfo_,
        enableJit_);
  }
  return std::make_shared<WindowPartitionImpl<RowFormat::kRowContainer>>(
      data_.get(), partition, inversedInputChannels_, sortKeyInfo_, enableJit_);
}

template <bool needSort>
std::unique_ptr<WindowPartition>
SpillableWindowBuild<needSort>::createPartialPartition() {
  auto partitionSize = inputRows_.size();
  BOLT_CHECK_GT(
      partitionSize, 0, "inputRows in partition should be larger than 0");
  auto partition = folly::Range(inputRows_.data(), partitionSize);
  if (rowBasedSpillSortMerger_) {
    return std::make_unique<WindowPartitionImpl<RowFormat::kSerializedRows>>(
        data_.get(),
        partition,
        inversedInputChannels_,
        sortKeyInfo_,
        enableJit_);
  }
  return std::make_unique<WindowPartitionImpl<RowFormat::kRowContainer>>(
      data_.get(), partition, inversedInputChannels_, sortKeyInfo_, enableJit_);
}

template <bool needSort>
bool SpillableWindowBuild<needSort>::hasNextPartition() {
  if (rowBasedSpillSortMerger_ != nullptr) {
    if (partitionStartRows_.size() > 0 &&
        currentPartition_ < int(partitionStartRows_.size() - 2)) {
      return true;
    }
    loadNextPartitionFromSpill();
  }
  return partitionStartRows_.size() > 1 &&
      currentPartition_ < int(partitionStartRows_.size() - 2);
}

template <bool needSort>
void SpillableWindowBuild<needSort>::resetSpiller() {
  spillers_[currentPartition_] = nullptr;
}

template class SpillableWindowBuild<true>;
template class SpillableWindowBuild<false>;

} // namespace bytedance::bolt::exec
