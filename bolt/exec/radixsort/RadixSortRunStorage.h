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
#include <cstring>
#include <span>
#include <string_view>
#include <vector>

#include "bolt/common/memory/AllocationPool.h"
#include "bolt/exec/radixsort/PayloadRow.h"
#include "bolt/exec/radixsort/RadixSortKey.h"

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
};

struct PayloadRowFixedBlock {
  char* base;
  uint32_t capacity;
  uint32_t count;
};

struct PayloadRowHeapBlock {
  uint64_t used;
};

struct RadixSortKeyRange {
  const char* data;
  vector_size_t count;
};

class PayloadRowBatch {
 public:
  char* heapAt(vector_size_t row) const;

  const BufferPtr& rows() const {
    return rows_;
  }

 private:
  friend class RadixSortRunStorage;
  friend class PayloadRowWriter;

  BufferPtr rows_;
  BufferPtr heaps_;
  BufferPtr heapSizes_;
};

class RadixSortRunStorage {
 public:
  RadixSortRunStorage(
      memory::MemoryPool* pool,
      RadixSortKeyLayout layout,
      std::shared_ptr<const PayloadRowLayout> payloadLayout = nullptr);

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

  const std::shared_ptr<const PayloadRowLayout>& payloadLayout() const {
    return payloadLayout_;
  }

  int64_t allocatedBytes() const {
    return allocationPool_.allocatedBytes();
  }

  uint64_t estimatedOutputBytes() const;

  RadixSortKeyRange keyRangeAt(uint64_t index, vector_size_t maxCount) const;

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
      const BufferPtr& sizeStorage,
      PayloadRowBatch& batch);

  void allocateFixedPayloadRowBatch(
      vector_size_t count,
      PayloadRowBatch& batch);

  void clear();

 private:
  friend class RadixSortKeyCodec;

  static constexpr uint64_t kDefaultBlockBytes = 64 * 1024;

  template <typename Encode>
  uint64_t appendVariableKeyBatch(
      std::span<const uint64_t> heapSizes,
      std::span<char* const> payloads,
      Encode encode) {
    BOLT_DCHECK(layout_.isVariable());
    BOLT_DCHECK(payloads.empty() || payloads.size() == heapSizes.size());

    uint64_t maximumHeapSize = 0;
    appendKeyBlocks(
        heapSizes.size(),
        [&](vector_size_t source, vector_size_t count, char* destination) {
          for (vector_size_t row = 0; row < count; ++row) {
            const auto inputRow = source + row;
            maximumHeapSize = std::max(maximumHeapSize, heapSizes[inputRow]);
            auto* record =
                destination + static_cast<uint64_t>(row) * layout_.width();
            std::memset(
                record + layout_.heapKeyOffset(),
                0,
                layout_.inlineCapacity() - layout_.heapKeyOffset());

            char* heap;
            BOLT_DCHECK_GT(heapSizes[inputRow], 0);
            allocateOverflow(heapSizes[inputRow], heap);
            storeUnaligned<uint64_t>(
                record + *layout_.sizeOffset(),
                layout_.heapKeyOffset() + heapSizes[inputRow]);
            storeCompactPointer(record + *layout_.dataOffset(), heap);
            if (layout_.hasPayload()) {
              storeCompactPointer(
                  record + *layout_.payloadOffset(),
                  payloads.empty() ? nullptr : payloads[inputRow]);
            }
          }
          encode(source, count, destination);
        });
    return layout_.heapKeyOffset() + maximumHeapSize;
  }

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
  std::shared_ptr<const PayloadRowLayout> payloadLayout_;
  memory::AllocationPool allocationPool_;
  BlockVector keyBlocks_;
  HeapGroupVector keyHeapGroups_;
  PayloadFixedBlockVector payloadFixedBlocks_;
  PayloadHeapGroupVector payloadHeapGroups_;
  uint64_t size_{0};
};

} // namespace bytedance::bolt::exec::radixsort
