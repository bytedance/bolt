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

#include "bolt/exec/WindowBuild.h"
#include "bolt/exec/ContainerRow2RowSerde.h"
#include "bolt/exec/MemoryReclaimer.h"
#include "bolt/exec/Operator.h"
namespace bytedance::bolt::exec {

namespace {
std::tuple<std::vector<column_index_t>, std::vector<column_index_t>, RowTypePtr>
reorderInputChannels(
    const RowTypePtr& inputType,
    const std::vector<core::FieldAccessTypedExprPtr>& partitionKeys,
    const std::vector<core::FieldAccessTypedExprPtr>& sortingKeys) {
  const auto size = inputType->size();

  std::vector<column_index_t> channels;
  std::vector<column_index_t> inversedChannels;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  channels.reserve(size);
  inversedChannels.resize(size);
  names.reserve(size);
  types.reserve(size);

  std::unordered_set<std::string> keyNames;

  auto appendChannel =
      [&inputType, &channels, &inversedChannels, &names, &types](
          column_index_t channel) {
        channels.push_back(channel);
        inversedChannels[channel] = channels.size() - 1;
        names.push_back(inputType->nameOf(channel));
        types.push_back(inputType->childAt(channel));
      };

  for (const auto& key : partitionKeys) {
    auto channel = exprToChannel(key.get(), inputType);
    appendChannel(channel);
    keyNames.insert(key->name());
  }

  for (const auto& key : sortingKeys) {
    auto channel = exprToChannel(key.get(), inputType);
    appendChannel(channel);
    keyNames.insert(key->name());
  }

  for (auto i = 0; i < size; ++i) {
    if (keyNames.count(inputType->nameOf(i)) == 0) {
      appendChannel(i);
    }
  }

  return std::make_tuple(
      channels, inversedChannels, ROW(std::move(names), std::move(types)));
}

// Returns a [start, end) slice of the 'types' vector.
std::vector<TypePtr>
slice(const std::vector<TypePtr>& types, int32_t start, int32_t end) {
  std::vector<TypePtr> result;
  result.reserve(end - start);
  for (auto i = start; i < end; ++i) {
    result.push_back(types[i]);
  }
  return result;
}
} // namespace

WindowBuild::WindowBuild(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    bolt::memory::MemoryPool* pool,
    const common::SpillConfig* spillConfig,
    tsan_atomic<bool>* nonReclaimableSection,
    uint64_t spillMemoryThreshold,
    bool enableJit)
    : spillConfig_{spillConfig},
      nonReclaimableSection_{nonReclaimableSection},
      decodedInputVectors_(windowNode->inputType()->size()),
      sortSpillCompareFlags_{makeSpillCompareFlags(
          windowNode->partitionKeys().size(),
          windowNode->sortingOrders())},
      pool_(pool),
      spillMemoryThreshold_(spillMemoryThreshold),
      enableJit_(enableJit),
      numPartitionKeys_{windowNode->partitionKeys().size()} {
  BOLT_CHECK_NOT_NULL(pool_);
  std::tie(inputChannels_, inversedInputChannels_, inputType_) =
      reorderInputChannels(
          windowNode->inputType(),
          windowNode->partitionKeys(),
          windowNode->sortingKeys());
  followedTopNum_ = windowNode->followedTopNum();
  const auto numPartitionKeys = windowNode->partitionKeys().size();
  const auto numSortingKeys = windowNode->sortingKeys().size();
  const auto numKeys = numPartitionKeys + numSortingKeys;
  data_ = std::make_unique<RowContainer>(
      slice(inputType_->children(), 0, numKeys),
      slice(inputType_->children(), numKeys, inputType_->size()),
      pool);

  for (auto i = 0; i < numPartitionKeys; ++i) {
    partitionKeyInfo_.push_back(std::make_pair(i, core::SortOrder{true, true}));
  }

  for (auto i = 0; i < numSortingKeys; ++i) {
    sortKeyInfo_.push_back(
        std::make_pair(numPartitionKeys + i, windowNode->sortingOrders()[i]));
  }
  allKeysInfo_.reserve(partitionKeyInfo_.size() + sortKeyInfo_.size());
  allKeysInfo_.insert(
      allKeysInfo_.cend(), partitionKeyInfo_.begin(), partitionKeyInfo_.end());
  allKeysInfo_.insert(
      allKeysInfo_.cend(), sortKeyInfo_.begin(), sortKeyInfo_.end());
}

void WindowBuild::updateEstimatedOutputRowSize() {
  const auto optionalRowSize = data_->estimateRowSize();
  if (!optionalRowSize.has_value() || optionalRowSize.value() == 0) {
    return;
  }

  const auto rowSize = optionalRowSize.value();
  if (!estimatedOutputRowSize_.has_value()) {
    estimatedOutputRowSize_ = rowSize;
  } else if (rowSize > estimatedOutputRowSize_.value()) {
    estimatedOutputRowSize_ = rowSize;
  }
}

// Column to row and spill
void WindowBuild::addInputCommon(RowVectorPtr input) {
  for (auto i = 0; i < inputChannels_.size(); ++i) {
    decodedInputVectors_[i].decode(*input->childAt(inputChannels_[i]));
  }

  if (followedTopNum_ > 0) {
    return;
  }

  ensureInputFits(input);
  const auto numInput = input->size();

  vector_size_t rowCnt = 0;
  // Add all the rows into the RowContainer.
  for (auto row = 0; row < numInput; ++row) {
    char* newRow = data_->newRow();

    for (auto col = 0; col < input->childrenSize(); ++col) {
      data_->store(decodedInputVectors_[col], row, newRow, col);
    }
  }
  rowCnt = numInput;
  numRows_ += rowCnt;
}

void WindowBuild::noMoreInputCommon() {
  if (numRows_ == 0) {
    return;
  }

  if (sortSpiller_ != nullptr) {
    // Spill remaining data to avoid running out of memory while sort-merging
    // spilled data.
    sortSpill();

    BOLT_CHECK_NULL(sortMerge_);
    BOLT_CHECK_NULL(rowBasedSpillSortMerger_);
    auto spillPartition = sortSpiller_->finishSpill();
    if (spillConfig_->rowBasedSpillMode == common::RowBasedSpillMode::DISABLE) {
      sortMerge_ = spillPartition.createOrderedReader(pool_);
    } else {
      rowBasedSpillSortMerger_ =
          spillPartition.createRowBasedOrderedReaderWithLength(
              pool_,
              data_.get(),
              spillConfig_->getJITenabledForSpill(),
              spillConfig_->spillUringEnabled);
    }

  } else {
    // At this point we have seen all the input rows. The operator is
    // being prepared to output rows now.
    // To prepare the rows for output in SortWindowBuild they need to
    // be separated into partitions and sort by ORDER BY keys within
    // the partition. This will order the rows for getOutput().
    updateEstimatedOutputRowSize();
    sortPartitions();
    return;
  }
}

void WindowBuild::sortPartitions() {
  sortRows_.resize(numRows_);
  RowContainerIterator iter;
  data_->listRows(&iter, numRows_, sortRows_.data());

#ifdef ENABLE_BOLT_JIT
  if (cmp_ == nullptr && enableJit_) {
    if (data_->JITable(data_->keyTypes())) {
      auto [jitMod, rowRowCmpfn] = data_->codegenCompare(
          data_->keyTypes(),
          sortSpillCompareFlags_,
          bytedance::bolt::jit::CmpType::SORT_LESS,
          true);
      jitModule_ = std::move(jitMod);
      cmp_ = (RowRowCompare)jitModule_->getFuncPtr(rowRowCmpfn);
    }
  }
  if (cmp_) {
    sorter_.sort(sortRows_.begin(), sortRows_.end(), cmp_);
  } else {
#endif

#ifdef ENABLE_META_SORT
    MetaRowsSorterWraper<BufferRows>::MetaCodegenSort(
        sortedRows_,
        data_.get(),
        sorter_,
        data_->keyIndices(),
        sortCompareFlags_);
#else
  sorter_.sort(
      sortRows_.begin(),
      sortRows_.end(),
      [this](const char* leftRow, const char* rightRow) {
        for (vector_size_t index = 0; index < sortSpillCompareFlags_.size();
             ++index) {
          if (auto result = data_->compare(
                  leftRow, rightRow, index, sortSpillCompareFlags_[index])) {
            return result < 0;
          }
        }
        return false;
      });
#endif

#ifdef ENABLE_BOLT_JIT
  }
#endif
}

void WindowBuild::ensureInputFits(const RowVectorPtr& input) {
  if (spillConfig_ == nullptr) {
    return;
  }

  const int64_t numRows = data_->numRows();
  if (numRows == 0) {
    // 'data_' is empty. Nothing to spill.
    return;
  }

  auto [freeRows, outOfLineFreeBytes] = data_->freeSpace();
  const auto outOfLineBytes =
      data_->stringAllocator().retainedSize() - outOfLineFreeBytes;
  const int64_t flatInputBytes = input->usedSize();

  // Test-only spill path.
  if (numRows > 0 && testingTriggerSpill()) {
    sortSpill();
    return;
  }

  // If current memory usage exceeds spilling threshold, trigger spilling.
  const auto currentMemoryUsage = pool_->currentBytes();
  if (spillMemoryThreshold_ != 0 &&
      currentMemoryUsage > spillMemoryThreshold_) {
    spill();
    return;
  }

  const auto minReservationBytes =
      currentMemoryUsage * spillConfig_->minSpillableReservationPct / 100;
  const auto availableReservationBytes = pool_->availableReservation();
  const int64_t estimatedIncrementalBytes =
      data_->sizeIncrement(input->size(), outOfLineBytes ? flatInputBytes : 0);

  if (availableReservationBytes > minReservationBytes) {
    // If we have enough free rows for input rows and enough variable length
    // free space for the vector's flat size, no need for spilling.
    if (freeRows > input->size() &&
        (outOfLineBytes == 0 || outOfLineFreeBytes >= flatInputBytes)) {
      return;
    }

    // If the current available reservation in memory pool is 2X the
    // estimatedIncrementalBytes, no need to spill.
    if (availableReservationBytes > 2 * estimatedIncrementalBytes) {
      return;
    }
  }

  // Try reserving targetIncrementBytes more in memory pool, if succeed, no
  // need to spill.
  const auto targetIncrementBytes = std::max<int64_t>(
      estimatedIncrementalBytes * 2,
      currentMemoryUsage * spillConfig_->spillableReservationGrowthPct / 100);
  {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (pool_->maybeReserve(targetIncrementBytes)) {
      return;
    }
  }
  LOG(WARNING) << "Failed to reserve " << succinctBytes(targetIncrementBytes)
               << " for memory pool " << data_->pool()->name()
               << ", usage: " << succinctBytes(data_->pool()->currentBytes())
               << ", reservation: "
               << succinctBytes(data_->pool()->reservedBytes());
}

void WindowBuild::setupSortSpiller() {
  BOLT_CHECK_NULL(sortSpiller_);

  const auto sortingKeys = SpillState::makeSortingKeys(
      std::vector<CompareFlags>(sortSpillCompareFlags_));
  sortSpiller_ = std::make_unique<Spiller>(
      Spiller::Type::kOrderByInput,
      data_.get(),
      inputType_,
      sortingKeys,
      spillConfig_);
  sortSpiller_->setSpillConfig(spillConfig_);
}

void WindowBuild::sortSpill() {
  updateEstimatedOutputRowSize();

  if (sortSpiller_ == nullptr) {
    setupSortSpiller();
  }

  sortSpiller_->spill();
  data_->clear();
  data_->pool()->release();
}

vector_size_t WindowBuild::findNextPartitionStartRow(vector_size_t start) {
  auto partitionCompare = [&](const char* lhs, const char* rhs) -> bool {
    return compareRowsWithKeys(lhs, rhs, partitionKeyInfo_);
  };

  auto left = start;
  auto right = left + 1;
  auto lastPosition = sortRows_.size();
  while (right < lastPosition) {
    auto distance = 1;
    for (; distance < lastPosition - left; distance *= 2) {
      right = left + distance;
      if (partitionCompare(sortRows_[left], sortRows_[right]) != 0) {
        lastPosition = right;
        break;
      }
    }
    left += distance / 2;
    right = left + 1;
  }
  return right;
}

bool WindowBuild::compareRowsWithKeys(
    const char* lhs,
    const char* rhs,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& keys) {
  if (lhs == rhs) {
    return false;
  }
  for (auto& key : keys) {
    if (auto result = data_->compare(
            lhs,
            rhs,
            key.first,
            {key.second.isNullsFirst(), key.second.isAscending(), false})) {
      return result < 0;
    }
  }
  return false;
}

} // namespace bytedance::bolt::exec
