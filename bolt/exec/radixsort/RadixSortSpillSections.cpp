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

#include <cstring>
#include <limits>
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

bool isNull(const char* row, const RadixSortSpillPayloadVariableOp& op) {
  return (static_cast<uint8_t>(row[op.nullByte]) & op.nullMask) == 0;
}

uint64_t rawKeyHeapSizeFromRecord(
    const RadixSortSpillSectionMeta& meta,
    const char* key) {
  if (!meta.hasKeyHeap) {
    return 0;
  }
  const auto encodedSize = loadUnaligned<uint64_t>(key + meta.keySizeOffset);
  return encodedSize < meta.keyHeapOffset ? std::numeric_limits<uint64_t>::max()
                                          : encodedSize - meta.keyHeapOffset;
}

uint64_t payloadHeapSizeFromFixed(
    const RadixSortSpillSectionMeta& meta,
    const char* payloadFixed) {
  if (payloadFixed == nullptr || !meta.hasVariablePayload()) {
    return 0;
  }

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

void storeStringPointer(void* slot, const char* data) {
  storeUnaligned<const char*>(
      static_cast<char*>(slot) + sizeof(uint64_t), data);
}

void clearCompactPointer(void* slot) {
  storeCompactPointer(slot, nullptr);
}

void clearPayloadPointers(
    const RadixSortSpillSectionMeta& meta,
    const char* sourceFixed,
    char* destinationFixed) {
  for (const auto& op : meta.payloadVariableOps) {
    auto* destinationSlot = destinationFixed + op.offset;
    if (isNull(sourceFixed, op)) {
      std::memset(destinationSlot, 0, op.width);
      continue;
    }
    const auto* sourceSlot = sourceFixed + op.offset;
    if (op.kind == RadixSortSpillPayloadVariableKind::kString) {
      const auto value = loadUnaligned<StringView>(sourceSlot);
      if (!value.isInline()) {
        storeStringPointer(destinationSlot, nullptr);
      }
      continue;
    }
    const auto value = loadUnaligned<PayloadVarlenRef>(sourceSlot);
    storeUnaligned<PayloadVarlenRef>(
        destinationSlot, PayloadVarlenRef{value.size, nullptr});
  }
}

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

RadixSortSpillSectionSize RadixSortSpillSections::sizeForSerialize(
    const RadixSortSpillSectionMeta& meta,
    const char* key) {
  RadixSortSpillSectionSize result;
  result.keyHeapSize = rawKeyHeapSizeFromRecord(meta, key);
  const auto* payload =
      meta.hasPayload ? RadixSortKey(meta.keyLayout, key).payload() : nullptr;
  result.payloadHeapSize = payloadHeapSizeFromFixed(meta, payload);
  uint64_t total = meta.runtimeKeyRecordSize;
  total += result.keyHeapSize;
  total += meta.payloadFixedSize;
  total += result.payloadHeapSize;
  result.totalSize = total;
  return result;
}

void RadixSortSpillSections::copyKeyHeapToSection(
    const RadixSortSpillSectionMeta& meta,
    const char* sourceKey,
    uint64_t keyHeapSize,
    char*& keyHeap) {
  if (keyHeapSize == 0) {
    return;
  }
  std::memcpy(
      keyHeap, loadCompactPointer(sourceKey + meta.keyDataOffset), keyHeapSize);
  keyHeap += keyHeapSize;
}

void RadixSortSpillSections::copyPayloadFixedToSection(
    const RadixSortSpillSectionMeta& meta,
    const char* sourceKey,
    char* payloadFixed) {
  if (!meta.hasPayload) {
    return;
  }
  const auto* sourcePayload =
      loadCompactPointer(sourceKey + meta.keyPayloadOffset);
  std::memcpy(payloadFixed, sourcePayload, meta.payloadFixedSize);
}

void RadixSortSpillSections::copyPayloadHeapFromFixedToSection(
    const RadixSortSpillSectionMeta& meta,
    const char* payloadFixed,
    uint64_t payloadHeapSize,
    char*& payloadHeap) {
  if (payloadHeapSize == 0) {
    return;
  }
  auto* const start = payloadHeap;
  for (const auto& op : meta.payloadVariableOps) {
    if (isNull(payloadFixed, op)) {
      continue;
    }
    const auto* slot = payloadFixed + op.offset;
    if (op.kind == RadixSortSpillPayloadVariableKind::kString) {
      const auto value = loadUnaligned<StringView>(slot);
      if (value.isInline()) {
        continue;
      }
      std::memcpy(payloadHeap, value.data(), value.size());
      payloadHeap += value.size();
      continue;
    }
    const auto value = loadUnaligned<PayloadVarlenRef>(slot);
    if (value.size == 0) {
      continue;
    }
    std::memcpy(payloadHeap, value.data, value.size);
    payloadHeap += value.size;
  }
  BOLT_DCHECK_EQ(payloadHeap, start + payloadHeapSize);
}

void RadixSortSpillSections::clearKeyPointers(
    const RadixSortSpillSectionMeta& meta,
    char* keyRecords,
    uint64_t rowCount) {
  for (uint64_t row = 0; row < rowCount; ++row) {
    auto* key = keyRecords + row * meta.runtimeKeyRecordSize;
    if (meta.hasKeyHeap) {
      clearCompactPointer(key + meta.keyDataOffset);
    }
    if (meta.hasPayload) {
      clearCompactPointer(key + meta.keyPayloadOffset);
    }
  }
}

void RadixSortSpillSections::clearPayloadPointers(
    const RadixSortSpillSectionMeta& meta,
    char* payloadFixedRows,
    uint64_t rowCount) {
  if (!meta.hasVariablePayload()) {
    return;
  }
  for (uint64_t row = 0; row < rowCount; ++row) {
    auto* payloadFixed = payloadFixedRows + row * meta.payloadFixedSize;
    bytedance::bolt::exec::radixsort::clearPayloadPointers(
        meta, payloadFixed, payloadFixed);
  }
}

std::optional<uint64_t> RadixSortSpillSections::restorePointersInSections(
    const RadixSortSpillSectionMeta& meta,
    char* key,
    char*& keyHeap,
    const char* keyHeapEnd,
    char* payloadFixed,
    char*& payloadHeap,
    const char* payloadHeapEnd) {
  const auto keyHeapSize = rawKeyHeapSizeFromRecord(meta, key);
  if (keyHeapSize == std::numeric_limits<uint64_t>::max() ||
      keyHeapSize > static_cast<uint64_t>(keyHeapEnd - keyHeap)) {
    return std::nullopt;
  }
  if (meta.hasKeyHeap) {
    storeCompactPointer(key + meta.keyDataOffset, keyHeap);
    keyHeap += keyHeapSize;
  }

  if (meta.hasPayload) {
    storeCompactPointer(key + meta.keyPayloadOffset, payloadFixed);
  }

  if (meta.hasVariablePayload()) {
    const auto availablePayloadHeapBytes =
        static_cast<uint64_t>(payloadHeapEnd - payloadHeap);
    const auto payloadHeapSize = restorePayloadPointersAndGetHeapSize(
        meta, payloadFixed, payloadHeap, availablePayloadHeapBytes);
    if (!payloadHeapSize.has_value()) {
      return std::nullopt;
    }
    payloadHeap += *payloadHeapSize;
    return *payloadHeapSize;
  }

  return 0;
}

} // namespace bytedance::bolt::exec::radixsort
