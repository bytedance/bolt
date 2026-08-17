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

#include <algorithm>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

#include "bolt/common/memory/AllocationPool.h"
#include "bolt/exec/radixsort/PayloadRowLayout.h"
#include "bolt/exec/radixsort/RadixSortKey.h"
#include "bolt/exec/radixsort/RadixSortKeyCodec.h"

namespace bytedance::bolt::exec::radixsort {

struct RadixSortKeyBlock {
  char* base;
  uint32_t capacity;
  uint32_t count;
};

struct RadixSortKeyOverflowBlock {
  char* base;
  uint64_t capacity;
  uint64_t used;
  uint64_t keyCount;
};

struct PayloadRowFixedBlock {
  char* base;
  uint32_t capacity;
  uint32_t count;
};

struct PayloadRowHeapBlock {
  char* base;
  uint64_t capacity;
  uint64_t used;
  uint64_t rowCount;
};

struct RadixSortKeyRange {
  const char* data;
  vector_size_t count;
};

class PayloadRowBatch {
 public:
  vector_size_t size() const {
    return size_;
  }

  char* rowAt(vector_size_t row) const;

  char* heapAt(vector_size_t row) const;

  uint64_t heapSizeAt(vector_size_t row) const;

  const BufferPtr& rows() const {
    return rows_;
  }

  const BufferPtr& heaps() const {
    return heaps_;
  }

  const BufferPtr& heapSizes() const {
    return heapSizes_;
  }

 private:
  friend class RadixSortRunStorage;
  friend class PayloadRowWriter;

  vector_size_t size_{0};
  BufferPtr rows_;
  BufferPtr heaps_;
  BufferPtr heapSizes_;
};

class RadixSortRunStorage {
 public:
  static constexpr uint32_t kDefaultKeysPerBlock = 2048;

  RadixSortRunStorage(
      memory::MemoryPool* pool,
      RadixSortKeyLayout layout,
      uint32_t keysPerBlock = kDefaultKeysPerBlock,
      uint64_t preferredHeapGroupBytes = 64 * 1024,
      std::shared_ptr<const PayloadRowLayout> payloadLayout = nullptr,
      uint32_t payloadRowsPerBlock = kDefaultKeysPerBlock,
      uint64_t preferredPayloadHeapGroupBytes = 64 * 1024);

  const RadixSortKeyLayout& layout() const {
    return layout_;
  }

  memory::MemoryPool* pool() const {
    return pool_;
  }

  uint64_t size() const {
    return size_;
  }

  uint32_t keysPerBlock() const {
    return keysPerBlock_;
  }

  const auto& keyBlocks() const {
    return keyBlocks_;
  }

  const auto& keyHeapGroups() const {
    return keyHeapGroups_;
  }

  const std::shared_ptr<const PayloadRowLayout>& payloadLayout() const {
    return payloadLayout_;
  }

  uint64_t payloadSize() const {
    return payloadSize_;
  }

  const auto& payloadFixedBlocks() const {
    return payloadFixedBlocks_;
  }

  const auto& payloadHeapGroups() const {
    return payloadHeapGroups_;
  }

  int64_t allocatedBytes() const {
    return allocationPool_.allocatedBytes();
  }

  uint64_t estimatedOutputBytes() const;

  int32_t numRanges() const {
    return allocationPool_.numRanges();
  }

  RadixSortKey keyAt(uint64_t index);

  RadixSortKey keyAt(uint64_t index) const;

  char* keyDataAt(uint64_t index);

  const char* keyDataAt(uint64_t index) const;

  RadixSortKeyRange keyRangeAt(uint64_t index, vector_size_t maxCount) const;

  void append(
      std::string_view encodedKey,
      char* payload = nullptr,
      uint64_t* index = nullptr);

  void appendBatch(
      std::span<const std::string_view> encodedKeys,
      std::span<char* const> payloads = {});

  void appendBatch(
      const EncodedKeyBatch& encodedKeys,
      std::span<char* const> payloads = {});

  template <typename Append>
  void appendKeyBlocks(vector_size_t count, Append append) {
    vector_size_t source = 0;
    while (source < count) {
      ensureKeyBlock();
      auto& block = keyBlocks_.back();
      const auto blockCount = static_cast<vector_size_t>(
          std::min<uint32_t>(block.capacity - block.count, count - source));
      auto* destination =
          block.base + static_cast<uint64_t>(block.count) * layout_.width();
      append(source, blockCount, destination);
      block.count += blockCount;
      size_ += blockCount;
      source += blockCount;
    }
  }

  void allocatePayloadRowBatch(
      std::span<const uint64_t> heapSizes,
      PayloadRowBatch& batch);

  void allocateFixedPayloadRowBatch(
      vector_size_t count,
      PayloadRowBatch& batch);

  void clear();

 private:
  using BlockVector =
      std::vector<RadixSortKeyBlock, memory::StlAllocator<RadixSortKeyBlock>>;
  using HeapGroupVector = std::vector<
      RadixSortKeyOverflowBlock,
      memory::StlAllocator<RadixSortKeyOverflowBlock>>;
  using PayloadFixedBlockVector = std::
      vector<PayloadRowFixedBlock, memory::StlAllocator<PayloadRowFixedBlock>>;
  using PayloadHeapGroupVector = std::
      vector<PayloadRowHeapBlock, memory::StlAllocator<PayloadRowHeapBlock>>;

  void ensureKeyBlock();

  void ensurePayloadFixedBlock();

  void allocatePayloadRowPointers(vector_size_t count, char** rows);

  void allocateOverflow(uint64_t size, char*& data);

  memory::MemoryPool* pool_;
  RadixSortKeyLayout layout_;
  uint32_t keysPerBlock_;
  uint64_t preferredHeapGroupBytes_;
  std::shared_ptr<const PayloadRowLayout> payloadLayout_;
  uint32_t payloadRowsPerBlock_;
  uint64_t preferredPayloadHeapGroupBytes_;
  memory::AllocationPool allocationPool_;
  BlockVector keyBlocks_;
  HeapGroupVector keyHeapGroups_;
  PayloadFixedBlockVector payloadFixedBlocks_;
  PayloadHeapGroupVector payloadHeapGroups_;
  uint64_t size_{0};
  uint64_t payloadSize_{0};
};

} // namespace bytedance::bolt::exec::radixsort
