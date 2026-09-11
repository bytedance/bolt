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

#include "bolt/exec/radixsort/RadixSortRunStorage.h"

#include <algorithm>
#include <utility>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::exec::radixsort {

constexpr uint64_t kMaxPayloadFixedBlockBytes = 64 * 1024;

char* PayloadRowBatch::heapAt(vector_size_t row) const {
  return heaps_ == nullptr ? nullptr : heaps_->as<char*>()[row];
}

RadixSortRunStorage::RadixSortRunStorage(
    memory::MemoryPool* pool,
    RadixSortKeyLayout layout,
    std::shared_ptr<const PayloadRowLayout> payloadLayout)
    : pool_(pool),
      layout_(std::move(layout)),
      keysPerBlock_(static_cast<uint32_t>(
          std::max<uint64_t>(1, kDefaultBlockBytes / layout_.width()))),
      payloadLayout_(std::move(payloadLayout)),
      allocationPool_(pool_),
      keyBlocks_(memory::StlAllocator<RadixSortKeyBlock>(pool_)),
      keyHeapGroups_(memory::StlAllocator<RadixSortKeyOverflowBlock>(pool_)),
      payloadFixedBlocks_(memory::StlAllocator<PayloadRowFixedBlock>(pool_)),
      payloadHeapGroups_(memory::StlAllocator<PayloadRowHeapBlock>(pool_)) {}

uint64_t RadixSortRunStorage::estimatedOutputBytes() const {
  uint64_t bytes = 0;
  const auto keyBytesPerRow = layout_.hasPayload()
      ? static_cast<uint64_t>(*layout_.payloadOffset())
      : static_cast<uint64_t>(layout_.width());
  for (const auto& block : keyBlocks_) {
    bytes += static_cast<uint64_t>(block.count) * keyBytesPerRow;
  }
  for (const auto& group : keyHeapGroups_) {
    bytes += group.used;
  }
  if (payloadLayout_ != nullptr) {
    for (const auto& block : payloadFixedBlocks_) {
      bytes += static_cast<uint64_t>(block.count) * payloadLayout_->rowWidth();
    }
    for (const auto& group : payloadHeapGroups_) {
      bytes += group.used;
    }
  }
  return bytes;
}

RadixSortKeyRange RadixSortRunStorage::keyRangeAt(
    uint64_t index,
    vector_size_t maxCount) const {
  if (maxCount == 0 || index == size_) {
    return {nullptr, 0};
  }
  const auto blockIndex = index / keysPerBlock_;
  const auto indexInBlock = index % keysPerBlock_;
  const auto& block = keyBlocks_[blockIndex];
  const auto available = static_cast<vector_size_t>(block.count - indexInBlock);
  return {
      block.base + indexInBlock * layout_.width(),
      std::min(maxCount, available)};
}

void RadixSortRunStorage::allocatePayloadRowBatch(
    std::span<const uint64_t> heapSizes,
    const BufferPtr& sizeStorage,
    PayloadRowBatch& batch) {
  const auto count = static_cast<vector_size_t>(heapSizes.size());
  if (count == 0) {
    if (batch.rows_ != nullptr && batch.rows_->isMutable()) {
      batch.rows_->setSize(0);
    }
    if (batch.heaps_ != nullptr && batch.heaps_->isMutable()) {
      batch.heaps_->setSize(0);
    }
    batch.heapSizes_ = sizeStorage;
    return;
  }
  auto** rows = prepareReusableBuffer<char*>(batch.rows_, count, pool_);
  auto** heaps = prepareReusableBuffer<char*>(batch.heaps_, count, pool_);
  std::fill(heaps, heaps + count, nullptr);
  batch.heapSizes_ = sizeStorage;
  if (batch.heapSizes_ == nullptr) {
    prepareReusableBuffer<uint64_t>(batch.heapSizes_, count, pool_);
    std::memcpy(
        batch.heapSizes_->asMutable<uint64_t>(),
        heapSizes.data(),
        count * sizeof(uint64_t));
  }
  allocatePayloadRowPointers(count, rows);

  vector_size_t row = 0;
  while (row < count) {
    while (row < count && heapSizes[row] == 0) {
      ++row;
    }
    if (row == count) {
      break;
    }
    const auto groupStart = row;
    auto groupEnd = row;
    uint64_t groupSize = 0;
    while (groupEnd < count) {
      if (heapSizes[groupEnd] == 0) {
        ++groupEnd;
        continue;
      }
      if (groupSize > 0 &&
          (heapSizes[groupEnd] > kDefaultBlockBytes ||
           groupSize > kDefaultBlockBytes - heapSizes[groupEnd])) {
        break;
      }
      groupSize += heapSizes[groupEnd];
      ++groupEnd;
      if (groupSize >= kDefaultBlockBytes) {
        break;
      }
    }
    auto* groupBase = allocationPool_.allocateFixed(groupSize, 1);
    uint64_t offset = 0;
    for (auto groupRow = groupStart; groupRow < groupEnd; ++groupRow) {
      if (heapSizes[groupRow] == 0) {
        continue;
      }
      heaps[groupRow] = groupBase + offset;
      offset += heapSizes[groupRow];
    }
    payloadHeapGroups_.push_back(PayloadRowHeapBlock{groupSize});
    row = groupEnd;
  }
}

void RadixSortRunStorage::allocateFixedPayloadRowBatch(
    vector_size_t count,
    PayloadRowBatch& batch) {
  batch.heaps_.reset();
  batch.heapSizes_.reset();
  if (count == 0) {
    if (batch.rows_ != nullptr && batch.rows_->isMutable()) {
      batch.rows_->setSize(0);
    }
    return;
  }
  auto** rows = prepareReusableBuffer<char*>(batch.rows_, count, pool_);
  allocatePayloadRowPointers(count, rows);
}

void RadixSortRunStorage::allocatePayloadRowPointers(
    vector_size_t count,
    char** rows) {
  vector_size_t outputRow = 0;
  while (outputRow < count) {
    ensurePayloadFixedBlock();

    auto& block = payloadFixedBlocks_.back();
    const auto blockCount = static_cast<vector_size_t>(
        std::min<uint32_t>(block.capacity - block.count, count - outputRow));
    auto* row = block.base +
        static_cast<uint64_t>(block.count) * payloadLayout_->rowWidth();
    for (vector_size_t index = 0; index < blockCount; ++index) {
      rows[outputRow + index] =
          row + static_cast<uint64_t>(index) * payloadLayout_->rowWidth();
    }
    block.count += blockCount;
    outputRow += blockCount;
  }
}

void RadixSortRunStorage::clear() {
  {
    BlockVector emptyBlocks{memory::StlAllocator<RadixSortKeyBlock>(pool_)};
    HeapGroupVector emptyGroups{
        memory::StlAllocator<RadixSortKeyOverflowBlock>(pool_)};
    PayloadFixedBlockVector emptyPayloadBlocks{
        memory::StlAllocator<PayloadRowFixedBlock>(pool_)};
    PayloadHeapGroupVector emptyPayloadGroups{
        memory::StlAllocator<PayloadRowHeapBlock>(pool_)};
    keyBlocks_.swap(emptyBlocks);
    keyHeapGroups_.swap(emptyGroups);
    payloadFixedBlocks_.swap(emptyPayloadBlocks);
    payloadHeapGroups_.swap(emptyPayloadGroups);
  }
  allocationPool_.clear();
  size_ = 0;
}

void RadixSortRunStorage::ensureKeyBlock() {
  if (!keyBlocks_.empty() &&
      keyBlocks_.back().count < keyBlocks_.back().capacity) {
    return;
  }
  const auto bytes = static_cast<uint64_t>(keysPerBlock_) * layout_.width();
  auto* base = allocationPool_.allocateFixed(bytes, alignof(uint64_t));
  checkCompactPointerRange(base, bytes);
  keyBlocks_.push_back(RadixSortKeyBlock{base, keysPerBlock_, 0});
}

void RadixSortRunStorage::ensurePayloadFixedBlock() {
  if (!payloadFixedBlocks_.empty() &&
      payloadFixedBlocks_.back().count < payloadFixedBlocks_.back().capacity) {
    return;
  }
  const auto rowWidth = payloadLayout_->rowWidth();
  const auto byteLimitedCapacity =
      std::max<uint64_t>(1, kMaxPayloadFixedBlockBytes / rowWidth);
  const auto capacity = static_cast<uint32_t>(byteLimitedCapacity);
  const auto bytes = static_cast<uint64_t>(capacity) * rowWidth;
  auto* base = allocationPool_.allocateFixed(bytes, alignof(uint64_t));
  checkCompactPointerRange(base, bytes);
  payloadFixedBlocks_.push_back(PayloadRowFixedBlock{base, capacity, 0});
}

void RadixSortRunStorage::allocateOverflow(uint64_t size, char*& data) {
  if (keyHeapGroups_.empty() ||
      keyHeapGroups_.back().capacity - keyHeapGroups_.back().used < size) {
    const auto capacity = std::max(kDefaultBlockBytes, size);
    auto* base = allocationPool_.allocateFixed(capacity, 1);
    checkCompactPointerRange(base, capacity);
    keyHeapGroups_.push_back(RadixSortKeyOverflowBlock{base, capacity, 0});
  }
  auto& group = keyHeapGroups_.back();
  data = group.base + group.used;
  group.used += size;
}

} // namespace bytedance::bolt::exec::radixsort
