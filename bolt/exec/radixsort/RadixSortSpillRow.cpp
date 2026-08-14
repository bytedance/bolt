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
  BOLT_CHECK_NOT_NULL(payloadFixed);
  return loadUnaligned<uint64_t>(payloadFixed + *layout->variableSizeOffset());
}

uint64_t checkedTotalSize(
    uint64_t keySize,
    uint64_t payloadFixedSize,
    uint64_t payloadHeapSize) {
  auto total = checkedAdd<uint64_t>(RadixSortSpillRow::kHeaderSize, keySize);
  BOLT_CHECK(total.has_value(), "Radix sort spill row size overflows");
  total = checkedAdd<uint64_t>(*total, payloadFixedSize);
  BOLT_CHECK(total.has_value(), "Radix sort spill row size overflows");
  total = checkedAdd<uint64_t>(*total, payloadHeapSize);
  BOLT_CHECK(total.has_value(), "Radix sort spill row size overflows");
  BOLT_CHECK_LE(*total, std::numeric_limits<uint32_t>::max());
  return *total;
}

uint32_t spilledKeyFixedSize(
    const RadixSortKeyLayout& layout,
    uint64_t heapSize = 0) {
  if (layout.isVariable()) {
    return *layout.dataOffset() + (heapSize == 0 ? 0 : sizeof(uint64_t));
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

void restoreKeyPointerInRow(
    char* row,
    RadixSortSpillRowHeader header,
    const RadixSortKeyLayout& layout) {
  auto* key = row + RadixSortSpillRow::kHeaderSize;
  if (!layout.isVariable()) {
    return;
  }
  const auto size = loadUnaligned<uint64_t>(key + *layout.sizeOffset());
  if (size <= layout.inlineCapacity()) {
    return;
  }
  const auto offset = loadUnaligned<uint64_t>(key + *layout.dataOffset());
  BOLT_CHECK(isValidRecordRelativeRange(header.totalSize, offset, size));
  storeUnaligned<char*>(key + *layout.dataOffset(), row + offset);
}

void trustedRestoreKeyDataPointerInRow(
    char* row,
    const RadixSortKeyLayout& layout);

void trustedRestoreKeyPointerInRow(
    char* row,
    const RadixSortKeyLayout& layout) {
  trustedRestoreKeyDataPointerInRow(row, layout);
}

void trustedRestoreKeyDataPointerInRow(
    char* row,
    const RadixSortKeyLayout& layout) {
  if (!layout.isVariable()) {
    return;
  }
  auto* key = row + RadixSortSpillRow::kHeaderSize;
  const auto size = loadUnaligned<uint64_t>(key + *layout.sizeOffset());
  if (size > layout.inlineCapacity()) {
    const auto offset = loadUnaligned<uint64_t>(key + *layout.dataOffset());
    storeUnaligned<char*>(key + *layout.dataOffset(), row + offset);
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
  BOLT_CHECK_NOT_NULL(key);
  const auto radixKey = RadixSortKey(keyLayout, key);
  return sizeForSerialize(keyLayout, payloadLayout, key, radixKey.payload());
}

RadixSortSpillRowSize RadixSortSpillRow::sizeForSerialize(
    const RadixSortKeyLayout& keyLayout,
    const PayloadRowLayout* payloadLayout,
    const char* key,
    char* payload) {
  BOLT_CHECK_NOT_NULL(key);
  const auto radixKey = RadixSortKey(keyLayout, key);
  RadixSortSpillRowSize result;
  result.keyHeapSize = radixKey.heapSize();
  const auto keySize = checkedAdd<uint64_t>(
      spilledKeyFixedSize(keyLayout, result.keyHeapSize), result.keyHeapSize);
  BOLT_CHECK(keySize.has_value(), "Radix sort spill row key size overflows");
  BOLT_CHECK_LE(*keySize, std::numeric_limits<uint32_t>::max());
  result.keySize = static_cast<uint32_t>(*keySize);
  result.payload = payload;
  const auto payloadFixedSize =
      payloadLayout == nullptr ? 0 : payloadLayout->rowWidth();
  result.payloadHeapSize =
      payloadHeapSizeFromFixed(payloadLayout, result.payload);
  result.totalSize = static_cast<uint32_t>(checkedTotalSize(
      result.keySize, payloadFixedSize, result.payloadHeapSize));
  BOLT_CHECK_LE(payloadFixedSize, std::numeric_limits<uint32_t>::max());
  result.payloadFixedSize = static_cast<uint32_t>(payloadFixedSize);
  return result;
}

uint32_t RadixSortSpillRow::keySize(const RadixSortSpillRunMeta& meta) const {
  BOLT_CHECK_NOT_NULL(row_);
  return trustedKeySize(meta);
}

uint32_t RadixSortSpillRow::trustedKeySize(
    const RadixSortSpillRunMeta& meta) const {
  if (!meta.keyLayout.isVariable()) {
    return spilledKeyFixedSize(meta.keyLayout);
  }
  const auto* key = row_ + kHeaderSize;
  const auto storedSize =
      loadUnaligned<uint64_t>(key + *meta.keyLayout.sizeOffset());
  const auto heapSize =
      storedSize > meta.keyLayout.inlineCapacity() ? storedSize : 0;
  return static_cast<uint32_t>(
      spilledKeyFixedSize(meta.keyLayout, heapSize) + heapSize);
}

uint32_t RadixSortSpillRow::payloadHeapSize(
    const RadixSortSpillRunMeta& meta) const {
  const auto h = header();
  const auto keyBytes = keySize(meta);
  auto prefix = checkedAdd<uint64_t>(kHeaderSize, keyBytes);
  BOLT_CHECK(prefix.has_value(), "Radix sort spill row size overflows");
  prefix = checkedAdd<uint64_t>(*prefix, meta.payloadFixedSize);
  BOLT_CHECK(prefix.has_value(), "Radix sort spill row size overflows");
  BOLT_CHECK_GE(h.totalSize, *prefix);
  return static_cast<uint32_t>(h.totalSize - *prefix);
}

uint32_t RadixSortSpillRow::trustedPayloadHeapSize(
    const RadixSortSpillRunMeta& meta) const {
  return static_cast<uint32_t>(
      header().totalSize - kHeaderSize - trustedKeySize(meta) -
      meta.payloadFixedSize);
}

uint64_t RadixSortSpillRow::serializedSize(
    const RadixSortKeyLayout& keyLayout,
    const PayloadRowLayout* payloadLayout,
    const char* key) {
  return sizeForSerialize(keyLayout, payloadLayout, key).totalSize;
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
  BOLT_CHECK_NOT_NULL(destination);
  const auto radixKey = RadixSortKey(keyLayout, key);
  RadixSortSpillRowHeader header{size.totalSize};
  storeUnaligned<RadixSortSpillRowHeader>(destination, header);

  auto* current = destination + kHeaderSize;
  const auto keyFixedSize = spilledKeyFixedSize(keyLayout, size.keyHeapSize);
  std::memcpy(current, key, keyFixedSize);
  if (size.keyHeapSize > 0) {
    auto* keyHeap = current + keyFixedSize;
    std::memcpy(keyHeap, radixKey.fullKeyData(), size.keyHeapSize);
    storeUnaligned<uint64_t>(
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
  BOLT_CHECK_NOT_NULL(row_);
  return loadUnaligned<RadixSortSpillRowHeader>(row_);
}

void RadixSortSpillRow::validate(const RadixSortSpillRunMeta& meta) const {
  const auto h = header();
  BOLT_CHECK_GE(h.totalSize, kHeaderSize);
  const auto keyBytes = keySize(meta);
  const auto heapBytes = payloadHeapSize(meta);
  if (meta.payloadFixedSize == 0) {
    BOLT_CHECK_EQ(heapBytes, 0);
  }
  const auto total =
      checkedTotalSize(keyBytes, meta.payloadFixedSize, heapBytes);
  BOLT_CHECK_EQ(h.totalSize, total);
}

std::string_view RadixSortSpillRow::keyBytes(
    const RadixSortSpillRunMeta& meta) const {
  validate(meta);
  return trustedKeyBytes(meta);
}

std::string_view RadixSortSpillRow::trustedKeyBytes(
    const RadixSortSpillRunMeta& meta) const {
  return std::string_view(row_ + kHeaderSize, trustedKeySize(meta));
}

char* RadixSortSpillRow::payloadFixed(const RadixSortSpillRunMeta& meta) const {
  validate(meta);
  return trustedPayloadFixed(meta);
}

char* RadixSortSpillRow::trustedPayloadFixed(
    const RadixSortSpillRunMeta& meta) const {
  return meta.payloadFixedSize == 0 ? nullptr
                                    : row_ + kHeaderSize + trustedKeySize(meta);
}

char* RadixSortSpillRow::payloadHeap(const RadixSortSpillRunMeta& meta) const {
  validate(meta);
  return trustedPayloadHeap(meta);
}

char* RadixSortSpillRow::trustedPayloadHeap(
    const RadixSortSpillRunMeta& meta) const {
  return trustedPayloadHeapSize(meta) == 0
      ? nullptr
      : row_ + kHeaderSize + trustedKeySize(meta) + meta.payloadFixedSize;
}

void RadixSortSpillRow::restoreKeyPointer(
    const RadixSortSpillRunMeta& meta) const {
  validate(meta);
  trustedRestoreKeyPointer(meta);
}

void RadixSortSpillRow::trustedRestoreKeyPointer(
    const RadixSortSpillRunMeta& meta) const {
  trustedRestoreKeyPointerInRow(row_, meta.keyLayout);
}

void RadixSortSpillRow::trustedRestoreKeyDataPointer(
    const RadixSortSpillRunMeta& meta) const {
  trustedRestoreKeyDataPointerInRow(row_, meta.keyLayout);
}

void RadixSortSpillRow::restorePayloadPointers(
    const RadixSortSpillRunMeta& meta,
    const PayloadRowLayout& payloadLayout) const {
  validate(meta);
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
      true);
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
