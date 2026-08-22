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
#include <array>
#include <limits>
#include <utility>

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"

namespace bytedance::bolt::exec::radixsort {

namespace {

constexpr uint64_t kMaxPayloadFixedBlockBytes = 64 * 1024;

template <RadixSortKeyLayoutKind KIND>
void appendInlineVariableKeys(
    const EncodedKeyBatch& encodedKeys,
    std::span<char* const> payloads,
    RadixSortKeyBlock& block,
    vector_size_t source,
    vector_size_t count) {
  using Traits = RadixSortKeyTraits<KIND>;
  static_assert(!Traits::kVariable);
  const auto* offsets = encodedKeys.offsets()->as<uint64_t>();
  const auto* data = encodedKeys.data()->as<char>();
  auto* destination =
      block.base + static_cast<uint64_t>(block.count) * Traits::kWidth;
  for (vector_size_t row = 0; row < count; ++row) {
    const auto inputRow = source + row;
    const auto begin = offsets[inputRow];
    const auto size = offsets[inputRow + 1] - begin;
    auto* record = destination + static_cast<uint64_t>(row) * Traits::kWidth;
    for (uint32_t word = 0; word < Traits::kInlineWords; ++word) {
      uint64_t encodedWord = 0;
      const auto wordOffset = static_cast<uint64_t>(word) * sizeof(uint64_t);
      if (wordOffset < size) {
        std::memcpy(
            &encodedWord,
            data + begin + wordOffset,
            std::min<uint64_t>(sizeof(uint64_t), size - wordOffset));
      }
      if constexpr (std::endian::native == std::endian::little) {
        encodedWord = byteSwap(encodedWord);
      }
      storeUnaligned<uint64_t>(record + word * sizeof(uint64_t), encodedWord);
    }
    if constexpr (Traits::kHasPayload) {
      storeUnaligned<char*>(
          record + Traits::kPayloadOffset,
          payloads.empty() ? nullptr : payloads[inputRow]);
    }
  }
}

} // namespace

char* PayloadRowBatch::rowAt(vector_size_t row) const {
  return rows_->as<char*>()[row];
}

char* PayloadRowBatch::heapAt(vector_size_t row) const {
  return heaps_ == nullptr ? nullptr : heaps_->as<char*>()[row];
}

uint64_t PayloadRowBatch::heapSizeAt(vector_size_t row) const {
  return heapSizes_ == nullptr ? 0 : heapSizes_->as<uint64_t>()[row];
}

RadixSortRunStorage::RadixSortRunStorage(
    memory::MemoryPool* pool,
    RadixSortKeyLayout layout,
    uint32_t keysPerBlock,
    uint64_t preferredHeapGroupBytes,
    std::shared_ptr<const PayloadRowLayout> payloadLayout,
    uint32_t payloadRowsPerBlock,
    uint64_t preferredPayloadHeapGroupBytes)
    : pool_(pool),
      layout_(std::move(layout)),
      keysPerBlock_(keysPerBlock),
      preferredHeapGroupBytes_(preferredHeapGroupBytes),
      payloadLayout_(std::move(payloadLayout)),
      payloadRowsPerBlock_(payloadRowsPerBlock),
      preferredPayloadHeapGroupBytes_(preferredPayloadHeapGroupBytes),
      allocationPool_(pool_),
      keyBlocks_(memory::StlAllocator<RadixSortKeyBlock>(pool_)),
      keyHeapGroups_(memory::StlAllocator<RadixSortKeyOverflowBlock>(pool_)),
      payloadFixedBlocks_(memory::StlAllocator<PayloadRowFixedBlock>(pool_)),
      payloadHeapGroups_(memory::StlAllocator<PayloadRowHeapBlock>(pool_)) {}

RadixSortKey RadixSortRunStorage::keyAt(uint64_t index) {
  return RadixSortKey(layout_, keyDataAt(index));
}

RadixSortKey RadixSortRunStorage::keyAt(uint64_t index) const {
  return RadixSortKey(layout_, keyDataAt(index));
}

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

char* RadixSortRunStorage::keyDataAt(uint64_t index) {
  const auto blockIndex = index / keysPerBlock_;
  const auto indexInBlock = index % keysPerBlock_;
  auto& block = keyBlocks_[blockIndex];
  return block.base + indexInBlock * layout_.width();
}

const char* RadixSortRunStorage::keyDataAt(uint64_t index) const {
  const auto blockIndex = index / keysPerBlock_;
  const auto indexInBlock = index % keysPerBlock_;
  const auto& block = keyBlocks_[blockIndex];
  return block.base + indexInBlock * layout_.width();
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

void RadixSortRunStorage::append(
    std::string_view encodedKey,
    char* payload,
    uint64_t* index) {
  ensureKeyBlock();
  char* overflowData = nullptr;
  if (layout_.isVariable() && encodedKey.size() > layout_.inlineCapacity()) {
    allocateOverflow(encodedKey.size(), overflowData);
  }

  auto& block = keyBlocks_.back();
  auto key = RadixSortKey(layout_, block.base + block.count * layout_.width());
  key.construct(encodedKey, overflowData, payload);

  if (index != nullptr) {
    *index = size_;
  }
  ++block.count;
  ++size_;
}

void RadixSortRunStorage::appendBatch(
    std::span<const std::string_view> encodedKeys,
    std::span<char* const> payloads) {
  for (uint64_t index = 0; index < encodedKeys.size(); ++index) {
    append(encodedKeys[index], payloads.empty() ? nullptr : payloads[index]);
  }
}

void RadixSortRunStorage::appendBatch(
    const EncodedKeyBatch& encodedKeys,
    std::span<char* const> payloads) {
  if (encodedKeys.format() == EncodedKeyFormat::kFixed64 &&
      (layout_.kind() == RadixSortKeyLayoutKind::kKeyOnlyFixed8 ||
       layout_.kind() == RadixSortKeyLayoutKind::kKeyWithPayloadFixed16)) {
    const auto* keys = encodedKeys.fixedKeys() == nullptr
        ? nullptr
        : encodedKeys.fixedKeys()->as<uint64_t>();
    vector_size_t source = 0;
    while (source < encodedKeys.size()) {
      ensureKeyBlock();
      auto& block = keyBlocks_.back();
      const auto count = static_cast<vector_size_t>(std::min<uint32_t>(
          block.capacity - block.count, encodedKeys.size() - source));
      auto* destination =
          block.base + static_cast<uint64_t>(block.count) * layout_.width();
      if (layout_.kind() == RadixSortKeyLayoutKind::kKeyOnlyFixed8) {
        std::memcpy(
            destination,
            keys + source,
            static_cast<uint64_t>(count) * sizeof(uint64_t));
      } else {
        auto* records =
            reinterpret_cast<KeyWithPayloadFixed16Record*>(destination);
        for (vector_size_t row = 0; row < count; ++row) {
          records[row].part0 = keys[source + row];
          records[row].payload.pointer =
              payloads.empty() ? nullptr : payloads[source + row];
        }
      }
      block.count += count;
      size_ += count;
      source += count;
    }
    return;
  }

  if (encodedKeys.format() == EncodedKeyFormat::kVariableBinary &&
      !layout_.isVariable()) {
    if (encodedKeys.offsets() == nullptr || encodedKeys.data() == nullptr) {
      BOLT_FAIL("Variable encoded key buffers must not be null");
    }
    const auto appendFixed = [&]<RadixSortKeyLayoutKind KIND>() {
      vector_size_t source = 0;
      while (source < encodedKeys.size()) {
        ensureKeyBlock();
        auto& block = keyBlocks_.back();
        const auto count = static_cast<vector_size_t>(std::min<uint32_t>(
            block.capacity - block.count, encodedKeys.size() - source));
        appendInlineVariableKeys<KIND>(
            encodedKeys, payloads, block, source, count);
        block.count += count;
        size_ += count;
        source += count;
      }
    };
    switch (layout_.kind()) {
      case RadixSortKeyLayoutKind::kKeyOnlyFixed16:
        return appendFixed
            .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed16>();
      case RadixSortKeyLayoutKind::kKeyOnlyFixed24:
        return appendFixed
            .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed24>();
      case RadixSortKeyLayoutKind::kKeyOnlyFixed32:
        return appendFixed
            .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed32>();
      case RadixSortKeyLayoutKind::kKeyWithPayloadFixed24:
        return appendFixed.template
        operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed24>();
      case RadixSortKeyLayoutKind::kKeyWithPayloadFixed32:
        return appendFixed.template
        operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed32>();
      default:
        BOLT_FAIL("Unsupported fixed radix sort key layout");
    }
  }

  if (encodedKeys.format() == EncodedKeyFormat::kVariableBinary &&
      layout_.isVariable()) {
    if (encodedKeys.offsets() == nullptr || encodedKeys.data() == nullptr) {
      BOLT_FAIL("Variable encoded key buffers must not be null");
    }
    const auto* offsets = encodedKeys.offsets()->as<uint64_t>();
    const auto* data = encodedKeys.data()->as<char>();
    const auto appendVariable = [&]<RadixSortKeyLayoutKind KIND>() {
      using Traits = RadixSortKeyTraits<KIND>;
      static_assert(Traits::kVariable);
      vector_size_t source = 0;
      while (source < encodedKeys.size()) {
        ensureKeyBlock();
        auto& block = keyBlocks_.back();
        const auto count = static_cast<vector_size_t>(std::min<uint32_t>(
            block.capacity - block.count, encodedKeys.size() - source));
        auto* destination =
            block.base + static_cast<uint64_t>(block.count) * Traits::kWidth;
        for (vector_size_t row = 0; row < count; ++row) {
          const auto inputRow = source + row;
          const auto begin = offsets[inputRow];
          const auto size = offsets[inputRow + 1] - begin;
          auto* record =
              destination + static_cast<uint64_t>(row) * Traits::kWidth;
          std::array<uint64_t, Traits::kInlineWords> inlineWords{};
          std::memcpy(
              inlineWords.data(),
              data + begin,
              std::min<uint64_t>(size, Traits::kInlineCapacity));
          for (uint32_t word = 0; word < Traits::kInlineWords; ++word) {
            auto encodedWord = inlineWords[word];
            if constexpr (std::endian::native == std::endian::little) {
              encodedWord = byteSwap(encodedWord);
            }
            storeUnaligned<uint64_t>(
                record + word * sizeof(uint64_t), encodedWord);
          }
          storeUnaligned<uint64_t>(record + Traits::kSizeOffset, size);
          if (size > Traits::kInlineCapacity) {
            char* overflow;
            allocateOverflow(size, overflow);
            std::memcpy(overflow, data + begin, size);
            storeUnaligned<char*>(record + Traits::kDataOffset, overflow);
          } else {
            storeUnaligned<char*>(record + Traits::kDataOffset, nullptr);
          }
          if constexpr (Traits::kHasPayload) {
            storeUnaligned<char*>(
                record + Traits::kPayloadOffset,
                payloads.empty() ? nullptr : payloads[inputRow]);
          }
        }
        block.count += count;
        size_ += count;
        source += count;
      }
    };

    switch (layout_.kind()) {
      case RadixSortKeyLayoutKind::kKeyOnlyVariable32:
        return appendVariable
            .template operator()<RadixSortKeyLayoutKind::kKeyOnlyVariable32>();
      case RadixSortKeyLayoutKind::kKeyWithPayloadVariable32:
        return appendVariable.template
        operator()<RadixSortKeyLayoutKind::kKeyWithPayloadVariable32>();
      default:
        BOLT_FAIL("Unsupported variable radix sort key layout");
    }
  }

  for (vector_size_t row = 0; row < encodedKeys.size(); ++row) {
    std::array<char, sizeof(uint64_t)> fixedBytes{};
    std::string_view key;
    if (encodedKeys.format() == EncodedKeyFormat::kFixed64) {
      auto word = encodedKeys.fixedKeyAt(row);
      if constexpr (std::endian::native == std::endian::little) {
        word = byteSwap(word);
      }
      storeUnaligned(fixedBytes.data(), word);
      key = std::string_view(fixedBytes.data(), fixedBytes.size());
    } else {
      key = encodedKeys.variableKeyAt(row);
    }
    append(key, payloads.empty() ? nullptr : payloads[row]);
  }
}

void RadixSortRunStorage::allocatePayloadRowBatch(
    std::span<const uint64_t> heapSizes,
    PayloadRowBatch& batch) {
  allocatePayloadRowBatch(heapSizes, nullptr, batch);
}

void RadixSortRunStorage::allocatePayloadRowBatch(
    std::span<const uint64_t> heapSizes,
    const BufferPtr& sizeStorage,
    PayloadRowBatch& batch) {
  const auto count = static_cast<vector_size_t>(heapSizes.size());
  batch.size_ = count;
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
          (heapSizes[groupEnd] > preferredPayloadHeapGroupBytes_ ||
           groupSize > preferredPayloadHeapGroupBytes_ - heapSizes[groupEnd])) {
        break;
      }
      groupSize += heapSizes[groupEnd];
      ++groupEnd;
      if (groupSize >= preferredPayloadHeapGroupBytes_) {
        break;
      }
    }
    auto* groupBase = allocationPool_.allocateFixed(groupSize, 1);
    uint64_t offset = 0;
    uint64_t heapRowCount = 0;
    for (auto groupRow = groupStart; groupRow < groupEnd; ++groupRow) {
      if (heapSizes[groupRow] == 0) {
        continue;
      }
      heaps[groupRow] = groupBase + offset;
      offset += heapSizes[groupRow];
      ++heapRowCount;
    }
    payloadHeapGroups_.push_back(
        PayloadRowHeapBlock{groupBase, groupSize, groupSize, heapRowCount});
    row = groupEnd;
  }
}

void RadixSortRunStorage::allocateFixedPayloadRowBatch(
    vector_size_t count,
    PayloadRowBatch& batch) {
  batch.size_ = count;
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
    payloadSize_ += blockCount;
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
  payloadSize_ = 0;
}

void RadixSortRunStorage::ensureKeyBlock() {
  if (!keyBlocks_.empty() &&
      keyBlocks_.back().count < keyBlocks_.back().capacity) {
    return;
  }
  const auto bytes = static_cast<uint64_t>(keysPerBlock_) * layout_.width();
  auto* base = allocationPool_.allocateFixed(bytes, alignof(uint64_t));
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
  const auto capacity = static_cast<uint32_t>(
      std::min<uint64_t>(payloadRowsPerBlock_, byteLimitedCapacity));
  const auto bytes = static_cast<uint64_t>(capacity) * rowWidth;
  auto* base = allocationPool_.allocateFixed(bytes, alignof(uint64_t));
  payloadFixedBlocks_.push_back(PayloadRowFixedBlock{base, capacity, 0});
}

void RadixSortRunStorage::allocateOverflow(uint64_t size, char*& data) {
  if (keyHeapGroups_.empty() ||
      keyHeapGroups_.back().capacity - keyHeapGroups_.back().used < size) {
    const auto capacity = std::max(preferredHeapGroupBytes_, size);
    auto* base = allocationPool_.allocateFixed(capacity, 1);
    keyHeapGroups_.push_back(RadixSortKeyOverflowBlock{base, capacity, 0, 0});
  }
  auto& group = keyHeapGroups_.back();
  data = group.base + group.used;
  group.used += size;
  ++group.keyCount;
}

} // namespace bytedance::bolt::exec::radixsort
