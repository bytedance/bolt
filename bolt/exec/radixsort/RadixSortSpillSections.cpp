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

FOLLY_ALWAYS_INLINE
std::optional<uint64_t> restorePayloadPointersAndGetHeapSize(
    const RadixSortSpillSectionMeta& meta,
    char* payloadFixed,
    char* payloadHeap,
    uint64_t availablePayloadHeapBytes);

template <RadixSortKeyLayoutKind KIND>
constexpr uint32_t wireKeyRecordSizeForLayout() {
  using Traits = RadixSortKeyTraits<KIND>;
  if constexpr (Traits::kVariable) {
    return Traits::kDataOffset;
  } else if constexpr (Traits::kHasPayload) {
    return Traits::kPayloadOffset;
  } else {
    return Traits::kWidth;
  }
}

uint32_t wireKeyRecordSizeForLayout(RadixSortKeyLayoutKind kind) {
  switch (kind) {
    case RadixSortKeyLayoutKind::kInvalid:
      BOLT_FAIL("Invalid radix sort key layout");
    case RadixSortKeyLayoutKind::kKeyOnlyFixed8:
      return wireKeyRecordSizeForLayout<
          RadixSortKeyLayoutKind::kKeyOnlyFixed8>();
    case RadixSortKeyLayoutKind::kKeyOnlyFixed16:
      return wireKeyRecordSizeForLayout<
          RadixSortKeyLayoutKind::kKeyOnlyFixed16>();
    case RadixSortKeyLayoutKind::kKeyOnlyFixed24:
      return wireKeyRecordSizeForLayout<
          RadixSortKeyLayoutKind::kKeyOnlyFixed24>();
    case RadixSortKeyLayoutKind::kKeyOnlyFixed32:
      return wireKeyRecordSizeForLayout<
          RadixSortKeyLayoutKind::kKeyOnlyFixed32>();
    case RadixSortKeyLayoutKind::kKeyOnlyVariable32:
      return wireKeyRecordSizeForLayout<
          RadixSortKeyLayoutKind::kKeyOnlyVariable32>();
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed16:
      return wireKeyRecordSizeForLayout<
          RadixSortKeyLayoutKind::kKeyWithPayloadFixed16>();
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed24:
      return wireKeyRecordSizeForLayout<
          RadixSortKeyLayoutKind::kKeyWithPayloadFixed24>();
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed32:
      return wireKeyRecordSizeForLayout<
          RadixSortKeyLayoutKind::kKeyWithPayloadFixed32>();
    case RadixSortKeyLayoutKind::kKeyWithPayloadVariable32:
      return wireKeyRecordSizeForLayout<
          RadixSortKeyLayoutKind::kKeyWithPayloadVariable32>();
  }
  BOLT_FAIL("Unknown radix sort key layout");
}

static_assert(
    wireKeyRecordSizeForLayout<RadixSortKeyLayoutKind::kKeyOnlyFixed8>() == 8);
static_assert(
    wireKeyRecordSizeForLayout<RadixSortKeyLayoutKind::kKeyOnlyFixed16>() ==
    16);
static_assert(
    wireKeyRecordSizeForLayout<RadixSortKeyLayoutKind::kKeyOnlyFixed24>() ==
    24);
static_assert(
    wireKeyRecordSizeForLayout<RadixSortKeyLayoutKind::kKeyOnlyFixed32>() ==
    32);
static_assert(
    wireKeyRecordSizeForLayout<RadixSortKeyLayoutKind::kKeyOnlyVariable32>() ==
    26);
static_assert(
    wireKeyRecordSizeForLayout<
        RadixSortKeyLayoutKind::kKeyWithPayloadFixed16>() == 10);
static_assert(
    wireKeyRecordSizeForLayout<
        RadixSortKeyLayoutKind::kKeyWithPayloadFixed24>() == 18);
static_assert(
    wireKeyRecordSizeForLayout<
        RadixSortKeyLayoutKind::kKeyWithPayloadFixed32>() == 26);
static_assert(
    wireKeyRecordSizeForLayout<
        RadixSortKeyLayoutKind::kKeyWithPayloadVariable32>() == 20);

template <bool HasKeyHeap>
FOLLY_ALWAYS_INLINE uint64_t keyHeapSizeFromRecordForLayout(
    const RadixSortSpillSectionMeta& meta,
    const char* key) {
  if constexpr (HasKeyHeap) {
    const auto encodedSize = loadUnaligned<uint64_t>(key + meta.keySizeOffset);
    return encodedSize <= meta.keyHeapOffset
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
      if (fieldSize > std::numeric_limits<uint64_t>::max() - heapSize) {
        return std::numeric_limits<uint64_t>::max();
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
    uint64_t maxBytes) {
  static_assert(!HasVariablePayload || HasPayload);

  RadixSortSpillSectionBatchSize result;
  const auto fixedRowSize = meta.fixedWireBytesPerRow();
  if constexpr (!HasKeyHeap && !HasVariablePayload) {
    result.rowCount = std::min(maxRowCount, maxBytes / fixedRowSize);
    return result;
  }

  const auto maxFittingRows = std::min(maxRowCount, maxBytes / fixedRowSize);
  uint64_t rowCount = 0;
  uint64_t totalBytes = 0;
  uint64_t keyHeapBytes = 0;
  uint64_t payloadHeapBytes = 0;
  for (; rowCount < maxFittingRows; ++rowCount) {
    auto remainingBytes = maxBytes - totalBytes;
    if (fixedRowSize > remainingBytes) {
      break;
    }
    remainingBytes -= fixedRowSize;
    const auto* key = keyBase + rowCount * meta.runtimeKeyRecordSize;
    const auto keyHeapSize =
        keyHeapSizeFromRecordForLayout<HasKeyHeap>(meta, key);
    if (keyHeapSize > remainingBytes) {
      break;
    }
    remainingBytes -= keyHeapSize;
    uint64_t payloadHeapSize = 0;
    if constexpr (HasVariablePayload) {
      payloadHeapSize = payloadHeapSizeFromFixedForLayout<true>(
          meta, loadCompactPointer(key + meta.keyPayloadOffset));
    }
    if (payloadHeapSize > remainingBytes) {
      break;
    }
    totalBytes += fixedRowSize + keyHeapSize + payloadHeapSize;
    keyHeapBytes += keyHeapSize;
    payloadHeapBytes += payloadHeapSize;
  }
  result.rowCount = rowCount;
  result.keyHeapBytes = keyHeapBytes;
  result.payloadHeapBytes = payloadHeapBytes;
  return result;
}

FOLLY_ALWAYS_INLINE void copyPayloadVariableFieldsToHeapAndClear(
    const RadixSortSpillSectionMeta& meta,
    char* payloadFixed,
    char*& payloadHeap) {
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
}

template <RadixSortKeyLayoutKind KIND, bool HasPayloadHeap>
void copyRowsToSectionsForLayout(
    const RadixSortSpillSectionMeta& meta,
    const char* keyBase,
    uint64_t rowCount,
    uint64_t keyHeapBytes,
    uint64_t payloadHeapBytes,
    char* keyRecords,
    char*& keyHeap,
    char* payloadFixedRows,
    char*& payloadHeap) {
  using Traits = RadixSortKeyTraits<KIND>;
  constexpr bool kHasKeyHeap = Traits::kVariable;
  constexpr bool kHasPayload = Traits::kHasPayload;
  constexpr auto kWireKeyRecordSize = wireKeyRecordSizeForLayout<KIND>();
  static_assert(!HasPayloadHeap || kHasPayload);
  static_assert(kWireKeyRecordSize <= Traits::kWidth);
  BOLT_DCHECK_EQ(meta.runtimeKeyRecordSize, Traits::kWidth);
  BOLT_DCHECK_EQ(meta.wireKeyRecordSize, kWireKeyRecordSize);

  auto* const keyHeapStart = keyHeap;
  auto* const keyHeapEnd = keyHeapStart + keyHeapBytes;
  auto* const payloadHeapStart = payloadHeap;
  if constexpr (!kHasKeyHeap && !kHasPayload) {
    static_assert(!HasPayloadHeap);
    std::memcpy(
        keyRecords, keyBase, rowCount * static_cast<uint64_t>(Traits::kWidth));
    return;
  }

  const auto* sourceKey = keyBase;
  auto* destinationKey = keyRecords;
  auto* payloadFixed = payloadFixedRows;
  for (uint64_t row = 0; row < rowCount; ++row) {
    std::memcpy(destinationKey, sourceKey, kWireKeyRecordSize);
    if constexpr (kHasKeyHeap) {
      const auto encodedSize =
          loadUnaligned<uint64_t>(sourceKey + Traits::kSizeOffset);
      BOLT_DCHECK_GT(encodedSize, meta.keyHeapOffset);
      const auto keyHeapSize = encodedSize - meta.keyHeapOffset;
      // keyHeapBytes is the sizing snapshot for this range. Source records
      // must remain immutable until this delayed copy completes.
      BOLT_DCHECK_LE(keyHeap, keyHeapEnd);
      BOLT_DCHECK_LE(keyHeapSize, static_cast<uint64_t>(keyHeapEnd - keyHeap));
      std::memcpy(
          keyHeap,
          loadCompactPointer(sourceKey + Traits::kDataOffset),
          keyHeapSize);
      keyHeap += keyHeapSize;
    }
    if constexpr (kHasPayload) {
      const auto* sourcePayload =
          loadCompactPointer(sourceKey + Traits::kPayloadOffset);
      std::memcpy(payloadFixed, sourcePayload, meta.payloadFixedSize);
      if constexpr (HasPayloadHeap) {
        copyPayloadVariableFieldsToHeapAndClear(
            meta, payloadFixed, payloadHeap);
      }
      payloadFixed += meta.payloadFixedSize;
    }
    sourceKey += Traits::kWidth;
    destinationKey += kWireKeyRecordSize;
  }
  if constexpr (kHasKeyHeap) {
    BOLT_DCHECK_EQ(keyHeap, keyHeapEnd);
  }
  if constexpr (HasPayloadHeap) {
    BOLT_DCHECK_EQ(payloadHeap, payloadHeapStart + payloadHeapBytes);
  }
}

template <RadixSortKeyLayoutKind KIND>
void copyRowsToSectionsForLayout(
    const RadixSortSpillSectionMeta& meta,
    const char* keyBase,
    uint64_t rowCount,
    uint64_t keyHeapBytes,
    uint64_t payloadHeapBytes,
    char* keyRecords,
    char*& keyHeap,
    char* payloadFixedRows,
    char*& payloadHeap) {
  using Traits = RadixSortKeyTraits<KIND>;
  if constexpr (Traits::kHasPayload) {
    // Variable slots must be canonicalized even when this batch contributes
    // no heap bytes. In particular, null and empty complex values must never
    // leak process-local pointers into the wire image.
    if (!meta.payloadVariableOps.empty()) {
      copyRowsToSectionsForLayout<KIND, true>(
          meta,
          keyBase,
          rowCount,
          keyHeapBytes,
          payloadHeapBytes,
          keyRecords,
          keyHeap,
          payloadFixedRows,
          payloadHeap);
      return;
    }
  }
  copyRowsToSectionsForLayout<KIND, false>(
      meta,
      keyBase,
      rowCount,
      keyHeapBytes,
      payloadHeapBytes,
      keyRecords,
      keyHeap,
      payloadFixedRows,
      payloadHeap);
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
  wireKeyRecordSize = wireKeyRecordSizeForLayout(keyLayout.kind());
  BOLT_CHECK_LE(wireKeyRecordSize, runtimeKeyRecordSize);
  keyHeapOffset = 0;
  keySizeOffset = 0;
  keyPayloadOffset = 0;
  hasKeyHeap = false;
  hasPayload = keyLayout.hasPayload();
  if (keyLayout.isVariable()) {
    keyHeapOffset = keyLayout.heapKeyOffset();
    keySizeOffset = *keyLayout.sizeOffset();
    hasKeyHeap = true;
  }
  if (hasPayload) {
    keyPayloadOffset = *keyLayout.payloadOffset();
  }
  if (hasPayload) {
    BOLT_CHECK_NOT_NULL(payloadLayout);
  }
  payloadFixedSize = hasPayload ? payloadLayout->rowWidth() : 0;
  BOLT_CHECK(
      checkedAdd<uint64_t>(wireKeyRecordSize, payloadFixedSize).has_value(),
      "Radix sort spill fixed wire row size overflows");
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
    uint64_t maxBytes) {
  const auto hasVariablePayload = meta.hasVariablePayload();
  if (meta.hasKeyHeap) {
    if (meta.hasPayload) {
      if (hasVariablePayload) {
        return sizeForSerializeRowsForLayout<true, true, true>(
            meta, keyBase, maxRowCount, maxBytes);
      }
      return sizeForSerializeRowsForLayout<true, true, false>(
          meta, keyBase, maxRowCount, maxBytes);
    }
    return sizeForSerializeRowsForLayout<true, false, false>(
        meta, keyBase, maxRowCount, maxBytes);
  }
  if (meta.hasPayload) {
    if (hasVariablePayload) {
      return sizeForSerializeRowsForLayout<false, true, true>(
          meta, keyBase, maxRowCount, maxBytes);
    }
    return sizeForSerializeRowsForLayout<false, true, false>(
        meta, keyBase, maxRowCount, maxBytes);
  }
  return sizeForSerializeRowsForLayout<false, false, false>(
      meta, keyBase, maxRowCount, maxBytes);
}

void RadixSortSpillSections::copyRowsToSections(
    const RadixSortSpillSectionMeta& meta,
    const char* keyBase,
    uint64_t rowCount,
    uint64_t keyHeapBytes,
    uint64_t payloadHeapBytes,
    char* keyRecords,
    char*& keyHeap,
    char* payloadFixedRows,
    char*& payloadHeap) {
  const auto copy = [&]<RadixSortKeyLayoutKind KIND>() {
    copyRowsToSectionsForLayout<KIND>(
        meta,
        keyBase,
        rowCount,
        keyHeapBytes,
        payloadHeapBytes,
        keyRecords,
        keyHeap,
        payloadFixedRows,
        payloadHeap);
  };
  switch (meta.keyLayout.kind()) {
    case RadixSortKeyLayoutKind::kInvalid:
      BOLT_FAIL("Invalid radix sort key layout");
    case RadixSortKeyLayoutKind::kKeyOnlyFixed8:
      return copy.template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed8>();
    case RadixSortKeyLayoutKind::kKeyOnlyFixed16:
      return copy
          .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed16>();
    case RadixSortKeyLayoutKind::kKeyOnlyFixed24:
      return copy
          .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed24>();
    case RadixSortKeyLayoutKind::kKeyOnlyFixed32:
      return copy
          .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed32>();
    case RadixSortKeyLayoutKind::kKeyOnlyVariable32:
      return copy
          .template operator()<RadixSortKeyLayoutKind::kKeyOnlyVariable32>();
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed16:
      return copy.template
      operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed16>();
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed24:
      return copy.template
      operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed24>();
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed32:
      return copy.template
      operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed32>();
    case RadixSortKeyLayoutKind::kKeyWithPayloadVariable32:
      return copy.template
      operator()<RadixSortKeyLayoutKind::kKeyWithPayloadVariable32>();
  }
  BOLT_FAIL("Unknown radix sort key layout");
}

bool RadixSortSpillSections::restorePayloadPointersInSectionRows(
    const RadixSortSpillSectionMeta& meta,
    uint64_t rowCount,
    char* payloadFixedRows,
    char*& payloadHeap,
    const char* payloadHeapEnd) {
  if (payloadHeap > payloadHeapEnd) {
    return false;
  }
  if (!meta.hasPayload || !meta.hasVariablePayload()) {
    return payloadHeap == payloadHeapEnd;
  }

  auto* payloadFixed = payloadFixedRows;
  for (uint64_t row = 0; row < rowCount; ++row) {
    const auto availablePayloadHeapBytes =
        static_cast<uint64_t>(payloadHeapEnd - payloadHeap);
    const auto payloadHeapSize = restorePayloadPointersAndGetHeapSize(
        meta, payloadFixed, payloadHeap, availablePayloadHeapBytes);
    if (!payloadHeapSize.has_value()) {
      return false;
    }
    payloadHeap += *payloadHeapSize;
    payloadFixed += meta.payloadFixedSize;
  }
  return payloadHeap == payloadHeapEnd;
}

} // namespace bytedance::bolt::exec::radixsort
