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

#include "bolt/exec/SortWindowBuild.h"
#include <exec/RowBasedCompare.h>
#include <exec/Spill.h>
#include <folly/BenchmarkUtil.h>
#define XXH_INLINE_ALL
#include <boost/sort/pdqsort/pdqsort.hpp>
#include <xxhash.h>
#include "bolt/common/base/SimdUtil.h"
#include "bolt/exec/ContainerRow2RowSerde.h"
#include "bolt/exec/MemoryReclaimer.h"
namespace bytedance::bolt::exec {

SortWindowBuild::SortWindowBuild(
    const std::shared_ptr<const core::WindowNode>& node,
    bolt::memory::MemoryPool* pool,
    const common::SpillConfig* spillConfig,
    tsan_atomic<bool>* nonReclaimableSection,
    uint64_t spillMemoryThreshold,
    bool enableJit)
    : WindowBuild(
          node,
          pool,
          spillConfig,
          nonReclaimableSection,
          spillMemoryThreshold,
          enableJit),
      pool_(pool),
      comparator_(sortKeyInfo_, data_.get()) {
  BOLT_CHECK_NOT_NULL(pool_);
  allKeyInfo_.reserve(partitionKeyInfo_.size() + sortKeyInfo_.size());
  allKeyInfo_.insert(
      allKeyInfo_.cend(), partitionKeyInfo_.begin(), partitionKeyInfo_.end());
  allKeyInfo_.insert(
      allKeyInfo_.cend(), sortKeyInfo_.begin(), sortKeyInfo_.end());
  partitionStartRows_.resize(0);
  followedTopNum_ = node->followedTopNum();
  if (followedTopNum_ > 0) {
    if (numPartitionKeys_ > 0) {
      Accumulator accumulator{
          true,
          sizeof(TopRows),
          false,
          1,
          nullptr,
          nullptr,
          [](auto, auto) { BOLT_UNREACHABLE(); },
          [](auto, auto) { BOLT_UNREACHABLE(); },
          [](auto) {}};

      table_ = std::make_unique<HashTable<false>>(
          createVectorHashers(node->inputType(), node->partitionKeys()),
          std::vector<Accumulator>{accumulator},
          std::vector<TypePtr>{},
          false, // allowDuplicates
          false, // isJoinBuild
          false, // hasProbedFlag
          0, // minTableSizeForParallelJoinBuild
          pool_,
          nullptr,
          false);
      partitionOffset_ =
          table_->rows()->columnAt(node->partitionKeys().size()).offset();
      lookup_ = std::make_unique<HashLookup>(table_->hashers());
    } else {
      allocator_ = std::make_unique<HashStringAllocator>(pool_);
      singlePartition_ =
          std::make_unique<TopRows>(allocator_.get(), comparator_);
    }
  }
}

// Handle top N
void SortWindowBuild::addInput(RowVectorPtr input) {
  if (followedTopNum_ <= 0) {
    return;
  }

  ensureInputFits(input);

  const auto numInput = input->size();
  vector_size_t rowCnt = 0;
  if (table_) {
    // keep rows in topN heap
    SelectivityVector rows(numInput);
    table_->prepareForGroupProbe(
        *lookup_,
        input,
        rows,
        false,
        BaseHashTable::kNoSpillInputStartPartitionBit);
    table_->groupProbe(*lookup_);

    // Initialize new partitions.
    initializeNewPartitions();

    for (auto row = 0; row < numInput; ++row) {
      // get partitionKeys and calc hashid
      auto& partition = partitionAt(lookup_->hits[row]);
      if (processInputRow(row, partition)) {
        rowCnt++;
      }
    }
  } else {
    // no partition key
    for (auto row = 0; row < numInput; ++row) {
      if (processInputRow(row, *singlePartition_)) {
        rowCnt++;
      }
    }
  }
  numRows_ += rowCnt;
}

void SortWindowBuild::ensureInputFits(const RowVectorPtr& input) {
  if (spillConfig_ == nullptr) {
    // Spilling is disabled.
    return;
  }

  if (data_->numRows() == 0) {
    // Nothing to spill.
    return;
  }

  // Test-only spill path.
  if (testingTriggerSpill()) {
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
  uint64_t tableIncrementBytes = 0;
  if (table_) {
    tableIncrementBytes = table_->hashTableSizeIncrease(input->size());
  }
  const auto incrementBytes =
      data_->sizeIncrement(
          input->size(), outOfLineBytesPerRow * input->size()) +
      tableIncrementBytes;

  // First to check if we have sufficient minimal memory reservation.
  if (availableReservationBytes >= minReservationBytes) {
    if ((tableIncrementBytes == 0) && (freeRows > input->size()) &&
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

void SortWindowBuild::computePartitionStartRows() {
  partitionStartRows_.reserve(numRows_);

  // Using a sequential traversal to find changing partitions.
  // This algorithm is inefficient and can be changed
  // i) Use a binary search kind of strategy.
  // ii) If we use a Hashtable instead of a full sort then the count
  // of rows in the partition can be directly used.
  partitionStartRows_.push_back(0);

  BOLT_CHECK_GT(sortRows_.size(), 0);

  vector_size_t start = 0;
  while (start < sortRows_.size()) {
    auto next = findNextPartitionStartRow(start);
    partitionStartRows_.push_back(next);
    start = next;
  }
}

void SortWindowBuild::noMoreInput() {
  if (numRows_ == 0) {
    return;
  }

  if (sortSpiller_ != nullptr) {
    finishLoadFromSpill_ = false;
    return;

  } else {
    // At this point we have seen all the input rows which are sorted. This will
    // compute the index for each partition.
    computePartitionStartRows();
    if (table_) {
      BaseHashTable::RowsIterator partitionIt;
      static const size_t kPartitionBatchSize = 1000;
      std::vector<char*> partitions{kPartitionBatchSize};
      while (auto numPartitions = table_->listAllRows(
                 &partitionIt,
                 partitions.size(),
                 RowContainer::kUnlimited,
                 partitions.data())) {
        for (auto i = 0; i < numPartitions; ++i) {
          std::destroy_at(
              reinterpret_cast<TopRows*>(partitions[i] + partitionOffset_));
        }
      }
    }
  }
}

void SortWindowBuild::finish() {
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

void SortWindowBuild::storeRows() {
  BOLT_CHECK_GT(accumulatedLength_, 0);
  BOLT_CHECK_NOT_NULL(info_);
  char* newRows = nullptr;
  newRows =
      reinterpret_cast<char*>(data_->pool()->allocate(accumulatedLength_));
  allocatedStarts_.push_back(newRows);
  allocatedSizes_.push_back(accumulatedLength_);
  uint64_t offset = 0;
  for (auto i = 0; i < tmpSortedRows_.size(); i++) {
    auto dest = newRows + offset;
    ContainerRow2RowSerde::copyRow(
        dest, tmpSortedRows_[i], tmpSortedRowsSize_[i], *info_);
    offset += tmpSortedRowsSize_[i];
    sortRows_.push_back(dest);
  }
  tmpSortedRows_.clear();
  tmpSortedRowsSize_.clear();
  accumulatedLength_ = 0;
}

void SortWindowBuild::loadNextPartitionFromSpill() {
  if (sortMerge_) {
    BOLT_CHECK_NOT_NULL(sortMerge_);
    sortRows_.clear();
    if (data_->numRows()) {
      data_->clear();
    }
    for (;;) {
      auto next = sortMerge_->next();
      if (next == nullptr) {
        break;
      }

      bool newPartition = false;
      if (!sortRows_.empty()) {
        CompareFlags compareFlags = CompareFlags::equality(
            CompareFlags::NullHandlingMode::kNullAsValue);

        for (auto i = 0; i < numPartitionKeys_; ++i) {
          if (data_->compare(
                  sortRows_.back(),
                  data_->columnAt(i),
                  next->decoded(i),
                  next->currentIndex(),
                  compareFlags)) {
            newPartition = true;
            break;
          }
        }
      }

      if (newPartition) {
        break;
      }

      auto* newRow = data_->newRow();
      for (auto i = 0; i < inputChannels_.size(); ++i) {
        data_->store(next->decoded(i), next->currentIndex(), newRow, i);
      }
      sortRows_.push_back(newRow);
      next->pop();
    }
  } else {
    BOLT_CHECK_NOT_NULL(rowBasedSpillSortMerger_);
    if (finishLoadFromSpill_) {
      return;
    }
    RowBasedSpillMergeStream* next = rowBasedSpillSortMerger_->next();
    if (!next) {
      BOLT_CHECK(accumulatedLength_ == 0);
      if (partitionStartRows_.size() > 0 &&
          partitionStartRows_.back() != sortRows_.size()) {
        partitionStartRows_.push_back(sortRows_.size());
        numBuffers_.push_back(allocatedStarts_.size());
      }
      finishLoadFromSpill_ = true;
      return;
    }
    const RowFormatInfo& info = next->getRowFormatInfo();
    info_ = &(next->getRowFormatInfo());

    int cnt = 0;

    if (partitionStartRows_.size() <= 0) {
      partitionStartRows_.push_back(0);
    }

    for (;;) {
      next = rowBasedSpillSortMerger_->next();
      cnt++;
      if (next == nullptr) {
        BOLT_CHECK(accumulatedLength_ == 0);
        partitionStartRows_.push_back(sortRows_.size());
        numBuffers_.push_back(allocatedStarts_.size());
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
      }
      if (previousRow) {
        CompareFlags compareFlags = CompareFlags::equality(
            CompareFlags::NullHandlingMode::kNullAsValue);

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
        if (accumulatedLength_ > 0) {
          storeRows();
        }
        if (partitionStartRows_.size() > 0 &&
            partitionStartRows_.back() != sortRows_.size()) {
          partitionStartRows_.push_back(sortRows_.size());
          numBuffers_.push_back(allocatedStarts_.size());
        }
      }

      BOLT_CHECK(currentLength > 0);
      accumulatedLength_ += currentLength;

      tmpSortedRows_.push_back(currentRow);
      tmpSortedRowsSize_.push_back(currentLength);
      if ((isEndOfBatch || cnt > maxOutputRows_) && accumulatedLength_ > 0) {
        storeRows();
      }
      next->popWithLengths();
      if (cnt > maxOutputRows_) {
        break;
      }
    }
  }
}

std::shared_ptr<WindowPartition> SortWindowBuild::nextPartition() {
  if (sortMerge_ != nullptr) {
    BOLT_CHECK(!sortRows_.empty(), "No window partitions available");
    auto partition = folly::Range(sortRows_.data(), sortRows_.size());
    return std::make_shared<WindowPartitionImpl<RowFormat::kRowContainer>>(
        data_.get(),
        partition,
        inversedInputChannels_,
        sortKeyInfo_,
        enableJit_);
  }

  if (rowBasedSpillSortMerger_ != nullptr) {
    BOLT_CHECK(!partitionStartRows_.empty(), "No window partitions available");

    currentPartition_++;
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

      MicrosecondTimer timer(&loadFromSpillTimeUs_);

      for (; i < numBuffers_[currentPartition_ - 1]; i++) {
        data_->pool()->free(allocatedStarts_[i], allocatedSizes_[i]);
        allocatedSizes_[i] = 0;
      }
      lastErasePartition_ = currentPartition_ - 1;

      sortRows_.erase(
          sortRows_.begin(), sortRows_.begin() + numPreviousPartitionRows);
      for (int i = currentPartition_; i < partitionStartRows_.size(); i++) {
        partitionStartRows_[i] =
            partitionStartRows_[i] - numPreviousPartitionRows;
      }
    }

    // There is partition data available now.
    auto partitionSize = partitionStartRows_[currentPartition_ + 1] -
        partitionStartRows_[currentPartition_];
    auto partition = folly::Range(
        sortRows_.data() + partitionStartRows_[currentPartition_],
        partitionSize);
    return std::make_shared<WindowPartitionImpl<RowFormat::kSerializedRows>>(
        data_.get(),
        partition,
        inversedInputChannels_,
        sortKeyInfo_,
        enableJit_);
  }

  BOLT_CHECK(!partitionStartRows_.empty(), "No window partitions available")

  currentPartition_++;
  BOLT_CHECK_LE(
      currentPartition_,
      partitionStartRows_.size() - 2,
      "All window partitions consumed");

  // There is partition data available now.
  auto partitionSize = partitionStartRows_[currentPartition_ + 1] -
      partitionStartRows_[currentPartition_];
  auto partition = folly::Range(
      sortRows_.data() + partitionStartRows_[currentPartition_], partitionSize);
  return std::make_shared<WindowPartitionImpl<RowFormat::kRowContainer>>(
      data_.get(), partition, inversedInputChannels_, sortKeyInfo_, enableJit_);
}

bool SortWindowBuild::hasNextPartition() {
  if (sortMerge_ != nullptr) {
    loadNextPartitionFromSpill();
    return !sortRows_.empty();
  }

  if (rowBasedSpillSortMerger_ != nullptr) {
    if (partitionStartRows_.size() > 0 &&
        currentPartition_ < int(partitionStartRows_.size() - 2)) {
      return true;
    }
    loadNextPartitionFromSpill();
  }

  return partitionStartRows_.size() > 0 &&
      currentPartition_ < int(partitionStartRows_.size() - 2);
}

void SortWindowBuild::initializeNewPartitions() {
  for (auto index : lookup_->newGroups) {
    new (lookup_->hits[index] + partitionOffset_)
        TopRows(table_->stringAllocator(), comparator_);
  }
}

bool SortWindowBuild::processInputRow(vector_size_t index, TopRows& partition) {
  auto& topRows = partition.rows;
  bool needPush = false;

  // keep top N not equal value
  if (topRows.size() >= followedTopNum_) {
    char* topRow = topRows.top();
    for (auto& key : sortKeyInfo_) {
      if (auto result = data_->compare(
              topRow,
              data_->columnAt(key.first),
              decodedInputVectors_[key.first],
              index,
              {key.second.isNullsFirst(), key.second.isAscending(), false})) {
        if (result < 0) {
          return false; // decodeVectors[index] < topRow
        } else {
          topRows.pop();
          needPush = true; // decodeVectors[index] > topRow
          break;
        }
      }
    }
  } else {
    needPush = true;
  }

  // decodeVectors[index] >= topRow
  char* newRow = data_->newRow();

  for (auto col = 0; col < decodedInputVectors_.size(); ++col) {
    data_->store(decodedInputVectors_[col], index, newRow, col);
  }

  // decodeVectors[index] > topRow
  if (needPush) {
    topRows.push(newRow);
  }
  return true;
}

} // namespace bytedance::bolt::exec
