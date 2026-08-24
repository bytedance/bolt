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

#include "bolt/exec/radixsort/RadixSortSpillRow.h"

#include <cstring>
#include <limits>

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"
#include "bolt/type/StringView.h"

namespace bytedance::bolt::exec::radixsort {
namespace {

uint64_t payloadHeapSizeFromFixed(
    const PayloadRowLayout* layout,
    const char* payloadFixed) {
  if (layout == nullptr || !layout->variableSizeOffset().has_value()) {
    return 0;
  }
  return loadUnaligned<uint64_t>(payloadFixed + *layout->variableSizeOffset());
}

uint64_t checkedTotalSize(
    uint64_t keySize,
    uint64_t payloadFixedSize,
    uint64_t payloadHeapSize) {
  auto total = checkedAdd<uint64_t>(RadixSortSpillRow::kHeaderSize, keySize);
  total = checkedAdd<uint64_t>(*total, payloadFixedSize);
  total = checkedAdd<uint64_t>(*total, payloadHeapSize);
  return *total;
}

uint32_t spilledKeyFixedSize(const RadixSortKeyLayout& layout) {
  if (layout.isVariable()) {
    return *layout.dataOffset() + kCompactPointerBytes;
  }
  return layout.hasPayload() ? *layout.payloadOffset() : layout.width();
}

bool isStringType(const Type& type) {
  return type.kind() == TypeKind::VARCHAR || type.kind() == TypeKind::VARBINARY;
}

const char* stringPointer(const StringView& value) {
  return loadUnaligned<const char*>(
      reinterpret_cast<const char*>(&value) + sizeof(uint64_t));
}

void storeStringPointer(void* slot, const char* data) {
  storeUnaligned<const char*>(
      static_cast<char*>(slot) + sizeof(uint64_t), data);
}

void swizzlePayloadPointerFields(
    const PayloadRowLayout& layout,
    char* row,
    char* fixed,
    char* heapCursor,
    uint64_t totalSize,
    bool toOffset,
    bool copyHeap,
    bool checkRanges) {
  bool validRanges = true;
  for (const auto& column : layout.columns()) {
    if (!column.variable) {
      continue;
    }
    auto* slot = fixed + column.offset;
    if (isStringType(*column.type)) {
      auto value = loadUnaligned<StringView>(slot);
      if (value.isInline() || value.size() == 0) {
        continue;
      }
      if (toOffset) {
        const auto* source = stringPointer(value);
        if (copyHeap) {
          std::memcpy(heapCursor, source, value.size());
        }
        const auto offset = reinterpret_cast<uintptr_t>(heapCursor) -
            reinterpret_cast<uintptr_t>(row);
        validRanges &= !checkRanges ||
            isValidRecordRelativeRange(totalSize, offset, value.size());
        storeStringPointer(slot, reinterpret_cast<const char*>(offset));
        heapCursor += value.size();
      } else {
        const auto offset = reinterpret_cast<uint64_t>(stringPointer(value));
        validRanges &= !checkRanges ||
            isValidRecordRelativeRange(totalSize, offset, value.size());
        storeStringPointer(slot, row + offset);
      }
      continue;
    }
    auto value = loadUnaligned<PayloadVarlenRef>(slot);
    if (value.size == 0) {
      continue;
    }
    if (toOffset) {
      const auto offset = reinterpret_cast<uintptr_t>(heapCursor) -
          reinterpret_cast<uintptr_t>(row);
      if (copyHeap) {
        std::memcpy(heapCursor, value.data, value.size);
      }
      validRanges &= !checkRanges ||
          isValidRecordRelativeRange(totalSize, offset, value.size);
      storeUnaligned<PayloadVarlenRef>(
          slot, PayloadVarlenRef{value.size, reinterpret_cast<char*>(offset)});
      heapCursor += value.size;
    } else {
      const auto offset = reinterpret_cast<uint64_t>(value.data);
      validRanges &= !checkRanges ||
          isValidRecordRelativeRange(totalSize, offset, value.size);
      storeUnaligned<PayloadVarlenRef>(
          slot, PayloadVarlenRef{value.size, row + offset});
    }
  }
  BOLT_CHECK(validRanges, "Invalid radix sort spill row variable data range");
  if (toOffset) {
    BOLT_CHECK_EQ(heapCursor, row + totalSize);
  }
}

void trustedRestoreKeyDataPointerInRow(
    char* row,
    const RadixSortKeyLayout& layout);

void trustedRestoreKeyDataPointerInRow(
    char* row,
    const RadixSortKeyLayout& layout) {
  if (!layout.isVariable()) {
    return;
  }
  auto* key = row + RadixSortSpillRow::kHeaderSize;
  const auto size = loadUnaligned<uint64_t>(key + *layout.sizeOffset());
  if (layout.heapSize(size) > 0) {
    const auto offset = loadCompactUInt48(key + *layout.dataOffset());
    storeCompactPointer(key + *layout.dataOffset(), row + offset);
  }
}

} // namespace

uint32_t RadixSortSpillRow::keyFixedSize(const RadixSortKeyLayout& keyLayout) {
  return spilledKeyFixedSize(keyLayout);
}

RadixSortSpillRowSize RadixSortSpillRow::sizeForSerialize(
    const RadixSortKeyLayout& keyLayout,
    const PayloadRowLayout* payloadLayout,
    const char* key) {
  const auto radixKey = RadixSortKey(keyLayout, key);
  return sizeForSerialize(keyLayout, payloadLayout, key, radixKey.payload());
}

RadixSortSpillRowSize RadixSortSpillRow::sizeForSerialize(
    const RadixSortKeyLayout& keyLayout,
    const PayloadRowLayout* payloadLayout,
    const char* key,
    char* payload) {
  const auto radixKey = RadixSortKey(keyLayout, key);
  RadixSortSpillRowSize result;
  result.keyHeapSize = radixKey.heapSize();
  const auto keySize =
      checkedAdd<uint64_t>(spilledKeyFixedSize(keyLayout), result.keyHeapSize);
  result.keySize = static_cast<uint32_t>(*keySize);
  result.payload = payload;
  const auto payloadFixedSize =
      payloadLayout == nullptr ? 0 : payloadLayout->rowWidth();
  result.payloadHeapSize =
      payloadHeapSizeFromFixed(payloadLayout, result.payload);
  result.totalSize = static_cast<uint32_t>(checkedTotalSize(
      result.keySize, payloadFixedSize, result.payloadHeapSize));
  result.payloadFixedSize = static_cast<uint32_t>(payloadFixedSize);
  return result;
}

uint32_t RadixSortSpillRow::trustedKeySize(
    const RadixSortSpillRunMeta& meta) const {
  if (!meta.keyLayout.isVariable()) {
    return spilledKeyFixedSize(meta.keyLayout);
  }
  const auto* key = row_ + kHeaderSize;
  const auto storedSize =
      loadUnaligned<uint64_t>(key + *meta.keyLayout.sizeOffset());
  const auto heapSize = meta.keyLayout.heapSize(storedSize);
  return static_cast<uint32_t>(spilledKeyFixedSize(meta.keyLayout) + heapSize);
}

uint32_t RadixSortSpillRow::trustedPayloadHeapSize(
    const RadixSortSpillRunMeta& meta) const {
  return static_cast<uint32_t>(
      header().totalSize - kHeaderSize - trustedKeySize(meta) -
      meta.payloadFixedSize);
}

void RadixSortSpillRow::serialize(
    const RadixSortKeyLayout& keyLayout,
    const PayloadRowLayout* payloadLayout,
    const char* key,
    char* destination) {
  serialize(
      keyLayout,
      payloadLayout,
      key,
      sizeForSerialize(keyLayout, payloadLayout, key),
      destination);
}

void RadixSortSpillRow::serialize(
    const RadixSortKeyLayout& keyLayout,
    const PayloadRowLayout* payloadLayout,
    const char* key,
    const RadixSortSpillRowSize& size,
    char* destination) {
  const auto radixKey = RadixSortKey(keyLayout, key);
  RadixSortSpillRowHeader header{size.totalSize};
  storeUnaligned<RadixSortSpillRowHeader>(destination, header);

  auto* current = destination + kHeaderSize;
  const auto keyFixedSize = spilledKeyFixedSize(keyLayout);
  std::memcpy(current, key, keyFixedSize);
  if (size.keyHeapSize > 0) {
    auto* keyHeap = current + keyFixedSize;
    std::memcpy(keyHeap, radixKey.heapKeyData(), size.keyHeapSize);
    storeCompactUInt48(
        current + *keyLayout.dataOffset(),
        static_cast<uint64_t>(keyHeap - destination));
  }
  current += size.keySize;

  if (size.payloadFixedSize == 0) {
    return;
  }
  std::memcpy(current, size.payload, size.payloadFixedSize);
  auto* fixed = current;
  current += size.payloadFixedSize;
  if (!payloadLayout->hasVariableFields()) {
    return;
  }
  swizzlePayloadPointerFields(
      *payloadLayout,
      destination,
      fixed,
      current,
      size.totalSize,
      true,
      true,
      false);
}

RadixSortSpillRowHeader RadixSortSpillRow::header() const {
  return loadUnaligned<RadixSortSpillRowHeader>(row_);
}

void RadixSortSpillRow::validate(const RadixSortSpillRunMeta& meta) const {
  const auto h = header();
  BOLT_CHECK_GE(h.totalSize, kHeaderSize);
  const auto keyBytes = trustedKeySize(meta);
  const auto heapBytes = trustedPayloadHeapSize(meta);
  if (meta.payloadFixedSize == 0) {
    BOLT_CHECK_EQ(heapBytes, 0);
  }
  const auto total =
      checkedTotalSize(keyBytes, meta.payloadFixedSize, heapBytes);
  BOLT_CHECK_EQ(h.totalSize, total);
}

std::string_view RadixSortSpillRow::trustedKeyBytes(
    const RadixSortSpillRunMeta& meta) const {
  return std::string_view(row_ + kHeaderSize, trustedKeySize(meta));
}

char* RadixSortSpillRow::trustedPayloadFixedOrEnd(
    const RadixSortSpillRunMeta& meta) const {
  return row_ + kHeaderSize + trustedKeySize(meta);
}

char* RadixSortSpillRow::trustedPayloadFixed(
    const RadixSortSpillRunMeta& meta) const {
  return meta.payloadFixedSize == 0 ? nullptr : trustedPayloadFixedOrEnd(meta);
}

char* RadixSortSpillRow::trustedPayloadHeap(
    const RadixSortSpillRunMeta& meta) const {
  return trustedPayloadHeapSize(meta) == 0
      ? nullptr
      : trustedPayloadFixedOrEnd(meta) + meta.payloadFixedSize;
}

void RadixSortSpillRow::trustedRestoreKeyDataPointer(
    const RadixSortSpillRunMeta& meta) const {
  trustedRestoreKeyDataPointerInRow(row_, meta.keyLayout);
}

void RadixSortSpillRow::trustedRestorePayloadPointers(
    const RadixSortSpillRunMeta& meta,
    const PayloadRowLayout& payloadLayout) const {
  if (meta.payloadFixedSize == 0 || !payloadLayout.hasVariableFields()) {
    return;
  }
  auto* fixed = trustedPayloadFixed(meta);
  swizzlePayloadPointerFields(
      payloadLayout,
      row_,
      fixed,
      trustedPayloadHeap(meta),
      header().totalSize,
      false,
      false,
      false);
}

} // namespace bytedance::bolt::exec::radixsort
