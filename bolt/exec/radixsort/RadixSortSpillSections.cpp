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

#include "bolt/exec/radixsort/RadixSortSpillSections.h"

#include <folly/Portability.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <optional>
#include <utility>

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"
#include "bolt/type/StringView.h"

namespace bytedance::bolt::exec::radixsort {
namespace {

RadixSortSpillPayloadVariableKind variableKind(
    const PayloadRowColumnLayout& column) {
  return column.complex ? RadixSortSpillPayloadVariableKind::kComplex
                        : RadixSortSpillPayloadVariableKind::kString;
}

FOLLY_ALWAYS_INLINE bool isNull(
    const char* row,
    const RadixSortSpillPayloadVariableOp& op) {
  return (static_cast<uint8_t>(row[op.nullByte]) & op.nullMask) == 0;
}

FOLLY_ALWAYS_INLINE void storeStringPointer(void* slot, const char* data) {
  storeUnaligned<const char*>(
      static_cast<char*>(slot) + sizeof(uint64_t), data);
}

FOLLY_ALWAYS_INLINE void clearCompactPointer(void* slot) {
  storeCompactPointer(slot, nullptr);
}

FOLLY_ALWAYS_INLINE
std::optional<uint64_t> restorePayloadPointersAndGetHeapSize(
    const RadixSortSpillSectionMeta& meta,
    char* payloadFixed,
    char* payloadHeap,
    uint64_t availablePayloadHeapBytes);

template <bool HasKeyHeap>
FOLLY_ALWAYS_INLINE uint64_t keyHeapSizeFromRecordForLayout(
    const RadixSortSpillSectionMeta& meta,
    const char* key) {
  if constexpr (HasKeyHeap) {
    const auto encodedSize = loadUnaligned<uint64_t>(key + meta.keySizeOffset);
    return encodedSize < meta.keyHeapOffset
        ? std::numeric_limits<uint64_t>::max()
        : encodedSize - meta.keyHeapOffset;
  }
  return 0;
}

template <bool HasVariablePayload>
FOLLY_ALWAYS_INLINE uint64_t payloadHeapSizeFromFixedForLayout(
    const RadixSortSpillSectionMeta& meta,
    const char* payloadFixed) {
  if constexpr (HasVariablePayload) {
    BOLT_DCHECK_NOT_NULL(payloadFixed);
    uint64_t heapSize = 0;
    for (const auto& op : meta.payloadVariableOps) {
      if (isNull(payloadFixed, op)) {
        continue;
      }
      const auto* slot = payloadFixed + op.offset;
      uint64_t fieldSize = 0;
      if (op.kind == RadixSortSpillPayloadVariableKind::kString) {
        const auto value = loadUnaligned<StringView>(slot);
        fieldSize = value.isInline() ? 0 : value.size();
      } else {
        fieldSize = loadUnaligned<PayloadVarlenRef>(slot).size;
      }
      heapSize += fieldSize;
    }
    return heapSize;
  }
  return 0;
}

template <bool HasKeyHeap, bool HasPayload, bool HasVariablePayload>
FOLLY_ALWAYS_INLINE RadixSortSpillSectionBatchSize
sizeForSerializeRowsForLayout(
    const RadixSortSpillSectionMeta& meta,
    const char* keyBase,
    uint64_t maxRowCount,
    uint64_t maxBytes,
    std::vector<RadixSortSpillSectionSize>& rowSizes) {
  static_assert(!HasVariablePayload || HasPayload);
  rowSizes.clear();

  RadixSortSpillSectionBatchSize result;
  const auto fixedRowSize = meta.runtimeKeyRecordSize + meta.payloadFixedSize;
  if constexpr (!HasKeyHeap && !HasVariablePayload) {
    result.rowCount = std::min(maxRowCount, maxBytes / fixedRowSize);
    result.keyRecordBytes = result.rowCount * meta.runtimeKeyRecordSize;
    result.payloadFixedBytes = result.rowCount * meta.payloadFixedSize;
    return result;
  }

  rowSizes.reserve(maxRowCount);
  uint64_t totalBytes = 0;
  uint64_t keyHeapBytes = 0;
  uint64_t payloadHeapBytes = 0;
  for (uint64_t row = 0; row < maxRowCount; ++row) {
    const auto* key = keyBase + row * meta.runtimeKeyRecordSize;
    const auto keyHeapSize =
        keyHeapSizeFromRecordForLayout<HasKeyHeap>(meta, key);
    uint64_t payloadHeapSize = 0;
    if constexpr (HasVariablePayload) {
      payloadHeapSize = payloadHeapSizeFromFixedForLayout<true>(
          meta, loadCompactPointer(key + meta.keyPayloadOffset));
    }
    const auto rowBytes = fixedRowSize + keyHeapSize + payloadHeapSize;
    if (rowBytes > maxBytes - totalBytes) {
      break;
    }
    totalBytes += rowBytes;
    keyHeapBytes += keyHeapSize;
    payloadHeapBytes += payloadHeapSize;
    rowSizes.push_back(
        RadixSortSpillSectionSize{rowBytes, keyHeapSize, payloadHeapSize});
  }
  result.rowCount = rowSizes.size();
  result.keyRecordBytes = result.rowCount * meta.runtimeKeyRecordSize;
  result.keyHeapBytes = keyHeapBytes;
  result.payloadFixedBytes = result.rowCount * meta.payloadFixedSize;
  result.payloadHeapBytes = payloadHeapBytes;
  return result;
}

FOLLY_ALWAYS_INLINE void copyPayloadVariableFieldsToHeapAndClear(
    const RadixSortSpillSectionMeta& meta,
    char* payloadFixed,
    uint64_t payloadHeapSize,
    char*& payloadHeap) {
  auto* const start = payloadHeap;
  for (const auto& op : meta.payloadVariableOps) {
    auto* const slot = payloadFixed + op.offset;
    if (isNull(payloadFixed, op)) {
      std::memset(slot, 0, op.width);
      continue;
    }
    if (op.kind == RadixSortSpillPayloadVariableKind::kString) {
      const auto value = loadUnaligned<StringView>(slot);
      if (value.isInline()) {
        continue;
      }
      std::memcpy(payloadHeap, value.data(), value.size());
      storeStringPointer(slot, nullptr);
      payloadHeap += value.size();
      continue;
    }
    const auto value = loadUnaligned<PayloadVarlenRef>(slot);
    if (value.size > 0) {
      std::memcpy(payloadHeap, value.data, value.size);
      payloadHeap += value.size;
    }
    storeUnaligned<PayloadVarlenRef>(
        slot, PayloadVarlenRef{value.size, nullptr});
  }
  BOLT_DCHECK_EQ(payloadHeap, start + payloadHeapSize);
}

template <
    bool HasKeyHeap,
    bool HasPayload,
    bool HasVariablePayload,
    bool HasPayloadHeap>
void copyRowsToSectionsForLayout(
    const RadixSortSpillSectionMeta& meta,
    const char* keyBase,
    const RadixSortSpillSectionSize* rowSizes,
    uint64_t rowCount,
    uint64_t keyHeapBytes,
    uint64_t payloadHeapBytes,
    char* keyRecords,
    char*& keyHeap,
    char* payloadFixedRows,
    char*& payloadHeap) {
  static_assert(!HasVariablePayload || HasPayload);
  static_assert(!HasPayloadHeap || HasVariablePayload);

  std::memcpy(
      keyRecords,
      keyBase,
      rowCount * static_cast<uint64_t>(meta.runtimeKeyRecordSize));

  auto* const keyHeapStart = keyHeap;
  auto* const payloadHeapStart = payloadHeap;
  if constexpr (!HasKeyHeap && !HasPayload) {
    return;
  }
  for (uint64_t row = 0; row < rowCount; ++row) {
    const auto* sourceKey = keyBase + row * meta.runtimeKeyRecordSize;
    auto* key = keyRecords + row * meta.runtimeKeyRecordSize;
    if constexpr (HasKeyHeap) {
      const auto keyHeapSize = rowSizes[row].keyHeapSize;
      std::memcpy(
          keyHeap,
          loadCompactPointer(sourceKey + meta.keyDataOffset),
          keyHeapSize);
      keyHeap += keyHeapSize;
      clearCompactPointer(key + meta.keyDataOffset);
    }
    if constexpr (HasPayload) {
      auto* payloadFixed =
          payloadFixedRows + row * static_cast<uint64_t>(meta.payloadFixedSize);
      const auto* sourcePayload =
          loadCompactPointer(sourceKey + meta.keyPayloadOffset);
      std::memcpy(payloadFixed, sourcePayload, meta.payloadFixedSize);
      if constexpr (HasPayloadHeap) {
        copyPayloadVariableFieldsToHeapAndClear(
            meta, payloadFixed, rowSizes[row].payloadHeapSize, payloadHeap);
      }
      clearCompactPointer(key + meta.keyPayloadOffset);
    }
  }
  if constexpr (HasKeyHeap) {
    BOLT_DCHECK_EQ(keyHeap, keyHeapStart + keyHeapBytes);
  }
  if constexpr (HasPayloadHeap) {
    BOLT_DCHECK_EQ(payloadHeap, payloadHeapStart + payloadHeapBytes);
  }
}

template <bool HasKeyHeap>
FOLLY_ALWAYS_INLINE bool restoreKeyHeapPointer(
    const RadixSortSpillSectionMeta& meta,
    char* key,
    char*& keyHeap,
    const char* keyHeapEnd) {
  if constexpr (HasKeyHeap) {
    const auto keyHeapSize = keyHeapSizeFromRecordForLayout<true>(meta, key);
    if (keyHeapSize == std::numeric_limits<uint64_t>::max() ||
        keyHeapSize > static_cast<uint64_t>(keyHeapEnd - keyHeap)) {
      return false;
    }
    storeCompactPointer(key + meta.keyDataOffset, keyHeap);
    keyHeap += keyHeapSize;
  }
  return true;
}

template <
    bool HasKeyHeap,
    bool HasPayload,
    bool HasVariablePayload,
    bool HasPayloadHeap>
bool restorePointersInSectionRowsForLayout(
    const RadixSortSpillSectionMeta& meta,
    char* keyRecords,
    uint64_t rowCount,
    char*& keyHeap,
    const char* keyHeapEnd,
    char* payloadFixedRows,
    char*& payloadHeap,
    const char* payloadHeapEnd,
    std::vector<const char*>& keys) {
  static_assert(!HasVariablePayload || HasPayload);
  static_assert(!HasPayloadHeap || HasVariablePayload);
  keys.reserve(keys.size() + rowCount);
  for (uint64_t row = 0; row < rowCount; ++row) {
    auto* key = keyRecords + row * meta.runtimeKeyRecordSize;
    if (!restoreKeyHeapPointer<HasKeyHeap>(meta, key, keyHeap, keyHeapEnd)) {
      return false;
    }
    if constexpr (HasPayload) {
      auto* payloadFixed =
          payloadFixedRows + row * static_cast<uint64_t>(meta.payloadFixedSize);
      storeCompactPointer(key + meta.keyPayloadOffset, payloadFixed);
      if constexpr (HasPayloadHeap) {
        const auto availablePayloadHeapBytes =
            static_cast<uint64_t>(payloadHeapEnd - payloadHeap);
        const auto payloadHeapSize = restorePayloadPointersAndGetHeapSize(
            meta, payloadFixed, payloadHeap, availablePayloadHeapBytes);
        if (!payloadHeapSize.has_value()) {
          return false;
        }
        payloadHeap += *payloadHeapSize;
      }
    }
    keys.push_back(key);
  }
  return true;
}

FOLLY_ALWAYS_INLINE
std::optional<uint64_t> restorePayloadPointersAndGetHeapSize(
    const RadixSortSpillSectionMeta& meta,
    char* payloadFixed,
    char* payloadHeap,
    uint64_t availablePayloadHeapBytes) {
  uint64_t heapSize = 0;
  for (const auto& op : meta.payloadVariableOps) {
    auto* slot = payloadFixed + op.offset;
    if (isNull(payloadFixed, op)) {
      continue;
    }
    if (op.kind == RadixSortSpillPayloadVariableKind::kString) {
      const auto value = loadUnaligned<StringView>(slot);
      if (value.isInline()) {
        continue;
      }
      if (value.size() > availablePayloadHeapBytes - heapSize) {
        return std::nullopt;
      }
      storeStringPointer(slot, payloadHeap + heapSize);
      heapSize += value.size();
      continue;
    }
    const auto value = loadUnaligned<PayloadVarlenRef>(slot);
    if (value.size == 0) {
      continue;
    }
    if (value.size > availablePayloadHeapBytes - heapSize) {
      return std::nullopt;
    }
    storeUnaligned<PayloadVarlenRef>(
        slot, PayloadVarlenRef{value.size, payloadHeap + heapSize});
    heapSize += value.size;
  }
  return heapSize;
}

} // namespace

RadixSortSpillSectionMeta RadixSortSpillSectionMeta::create(
    RadixSortKeyLayout keyLayout,
    const PayloadRowLayout* payloadLayout) {
  RadixSortSpillSectionMeta meta;
  meta.keyLayout = std::move(keyLayout);
  meta.initialize(payloadLayout);
  return meta;
}

void RadixSortSpillSectionMeta::initialize(
    const PayloadRowLayout* payloadLayout) {
  runtimeKeyRecordSize = keyLayout.width();
  keyHeapOffset = 0;
  keySizeOffset = 0;
  keyDataOffset = 0;
  keyPayloadOffset = 0;
  hasKeyHeap = false;
  if (keyLayout.isVariable()) {
    keyHeapOffset = keyLayout.heapKeyOffset();
    keySizeOffset = *keyLayout.sizeOffset();
    keyDataOffset = *keyLayout.dataOffset();
    hasKeyHeap = true;
  }
  if (keyLayout.hasPayload()) {
    keyPayloadOffset = *keyLayout.payloadOffset();
  }
  hasPayload = keyLayout.hasPayload();
  if (hasPayload) {
    BOLT_CHECK_NOT_NULL(payloadLayout);
  }
  payloadFixedSize = hasPayload ? payloadLayout->rowWidth() : 0;
  payloadVariableOps.clear();
  if (hasPayload) {
    payloadVariableOps.reserve(payloadLayout->variableColumns().size());
    for (const auto& column : payloadLayout->variableColumns()) {
      payloadVariableOps.push_back(RadixSortSpillPayloadVariableOp{
          column.offset,
          column.width,
          column.nullByte,
          column.nullMask,
          variableKind(column)});
    }
  }
}

RadixSortSpillSectionBatchSize RadixSortSpillSections::sizeForSerializeRows(
    const RadixSortSpillSectionMeta& meta,
    const char* keyBase,
    uint64_t maxRowCount,
    uint64_t maxBytes,
    std::vector<RadixSortSpillSectionSize>& rowSizes) {
  const auto hasVariablePayload = meta.hasVariablePayload();
  if (meta.hasKeyHeap) {
    if (meta.hasPayload) {
      if (hasVariablePayload) {
        return sizeForSerializeRowsForLayout<true, true, true>(
            meta, keyBase, maxRowCount, maxBytes, rowSizes);
      }
      return sizeForSerializeRowsForLayout<true, true, false>(
          meta, keyBase, maxRowCount, maxBytes, rowSizes);
    }
    return sizeForSerializeRowsForLayout<true, false, false>(
        meta, keyBase, maxRowCount, maxBytes, rowSizes);
  }
  if (meta.hasPayload) {
    if (hasVariablePayload) {
      return sizeForSerializeRowsForLayout<false, true, true>(
          meta, keyBase, maxRowCount, maxBytes, rowSizes);
    }
    return sizeForSerializeRowsForLayout<false, true, false>(
        meta, keyBase, maxRowCount, maxBytes, rowSizes);
  }
  return sizeForSerializeRowsForLayout<false, false, false>(
      meta, keyBase, maxRowCount, maxBytes, rowSizes);
}

void RadixSortSpillSections::copyRowsToSections(
    const RadixSortSpillSectionMeta& meta,
    const char* keyBase,
    const RadixSortSpillSectionSize* rowSizes,
    uint64_t rowCount,
    uint64_t keyHeapBytes,
    uint64_t payloadHeapBytes,
    char* keyRecords,
    char*& keyHeap,
    char* payloadFixedRows,
    char*& payloadHeap) {
  const auto hasVariablePayload = meta.hasVariablePayload();
  const auto hasPayloadHeap = hasVariablePayload && payloadHeapBytes > 0;
  if (meta.hasKeyHeap) {
    if (meta.hasPayload) {
      if (hasPayloadHeap) {
        copyRowsToSectionsForLayout<true, true, true, true>(
            meta,
            keyBase,
            rowSizes,
            rowCount,
            keyHeapBytes,
            payloadHeapBytes,
            keyRecords,
            keyHeap,
            payloadFixedRows,
            payloadHeap);
      } else if (hasVariablePayload) {
        copyRowsToSectionsForLayout<true, true, true, false>(
            meta,
            keyBase,
            rowSizes,
            rowCount,
            keyHeapBytes,
            payloadHeapBytes,
            keyRecords,
            keyHeap,
            payloadFixedRows,
            payloadHeap);
      } else {
        copyRowsToSectionsForLayout<true, true, false, false>(
            meta,
            keyBase,
            rowSizes,
            rowCount,
            keyHeapBytes,
            payloadHeapBytes,
            keyRecords,
            keyHeap,
            payloadFixedRows,
            payloadHeap);
      }
      return;
    }
    copyRowsToSectionsForLayout<true, false, false, false>(
        meta,
        keyBase,
        rowSizes,
        rowCount,
        keyHeapBytes,
        payloadHeapBytes,
        keyRecords,
        keyHeap,
        payloadFixedRows,
        payloadHeap);
    return;
  }
  if (meta.hasPayload) {
    if (hasPayloadHeap) {
      copyRowsToSectionsForLayout<false, true, true, true>(
          meta,
          keyBase,
          rowSizes,
          rowCount,
          keyHeapBytes,
          payloadHeapBytes,
          keyRecords,
          keyHeap,
          payloadFixedRows,
          payloadHeap);
    } else if (hasVariablePayload) {
      copyRowsToSectionsForLayout<false, true, true, false>(
          meta,
          keyBase,
          rowSizes,
          rowCount,
          keyHeapBytes,
          payloadHeapBytes,
          keyRecords,
          keyHeap,
          payloadFixedRows,
          payloadHeap);
    } else {
      copyRowsToSectionsForLayout<false, true, false, false>(
          meta,
          keyBase,
          rowSizes,
          rowCount,
          keyHeapBytes,
          payloadHeapBytes,
          keyRecords,
          keyHeap,
          payloadFixedRows,
          payloadHeap);
    }
    return;
  }
  copyRowsToSectionsForLayout<false, false, false, false>(
      meta,
      keyBase,
      rowSizes,
      rowCount,
      keyHeapBytes,
      payloadHeapBytes,
      keyRecords,
      keyHeap,
      payloadFixedRows,
      payloadHeap);
}

bool RadixSortSpillSections::restorePointersInSectionRows(
    const RadixSortSpillSectionMeta& meta,
    char* keyRecords,
    uint64_t rowCount,
    char*& keyHeap,
    const char* keyHeapEnd,
    char* payloadFixedRows,
    char*& payloadHeap,
    const char* payloadHeapEnd,
    std::vector<const char*>& keys) {
  const auto hasVariablePayload = meta.hasVariablePayload();
  const auto hasPayloadHeap =
      hasVariablePayload && payloadHeapEnd > payloadHeap;
  if (!meta.hasKeyHeap && !meta.hasPayload) {
    keys.reserve(keys.size() + rowCount);
    for (uint64_t row = 0; row < rowCount; ++row) {
      keys.push_back(keyRecords + row * meta.runtimeKeyRecordSize);
    }
    return true;
  }
  if (meta.hasKeyHeap) {
    if (meta.hasPayload) {
      if (hasPayloadHeap) {
        return restorePointersInSectionRowsForLayout<true, true, true, true>(
            meta,
            keyRecords,
            rowCount,
            keyHeap,
            keyHeapEnd,
            payloadFixedRows,
            payloadHeap,
            payloadHeapEnd,
            keys);
      }
      if (hasVariablePayload) {
        return restorePointersInSectionRowsForLayout<true, true, true, false>(
            meta,
            keyRecords,
            rowCount,
            keyHeap,
            keyHeapEnd,
            payloadFixedRows,
            payloadHeap,
            payloadHeapEnd,
            keys);
      }
      return restorePointersInSectionRowsForLayout<true, true, false, false>(
          meta,
          keyRecords,
          rowCount,
          keyHeap,
          keyHeapEnd,
          payloadFixedRows,
          payloadHeap,
          payloadHeapEnd,
          keys);
    }
    return restorePointersInSectionRowsForLayout<true, false, false, false>(
        meta,
        keyRecords,
        rowCount,
        keyHeap,
        keyHeapEnd,
        payloadFixedRows,
        payloadHeap,
        payloadHeapEnd,
        keys);
  }
  if (meta.hasPayload) {
    if (hasPayloadHeap) {
      return restorePointersInSectionRowsForLayout<false, true, true, true>(
          meta,
          keyRecords,
          rowCount,
          keyHeap,
          keyHeapEnd,
          payloadFixedRows,
          payloadHeap,
          payloadHeapEnd,
          keys);
    }
    if (hasVariablePayload) {
      return restorePointersInSectionRowsForLayout<false, true, true, false>(
          meta,
          keyRecords,
          rowCount,
          keyHeap,
          keyHeapEnd,
          payloadFixedRows,
          payloadHeap,
          payloadHeapEnd,
          keys);
    }
    return restorePointersInSectionRowsForLayout<false, true, false, false>(
        meta,
        keyRecords,
        rowCount,
        keyHeap,
        keyHeapEnd,
        payloadFixedRows,
        payloadHeap,
        payloadHeapEnd,
        keys);
  }
  return true;
}

} // namespace bytedance::bolt::exec::radixsort
