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

uint64_t keyHeapSizeFromRecord(
    const RadixRow2RowSerdeMeta& meta,
    const char* key) {
  switch (meta.keyHeapMode) {
    case RadixSortSpillKeyHeapMode::kNone:
      return 0;
    case RadixSortSpillKeyHeapMode::kVariableFixedSize:
      return meta.fixedKeyHeapSize;
    case RadixSortSpillKeyHeapMode::kVariableRowSize: {
      const auto encodedSize =
          loadUnaligned<uint64_t>(key + meta.keySizeOffset);
      return encodedSize < meta.keyHeapOffset
          ? std::numeric_limits<uint64_t>::max()
          : encodedSize - meta.keyHeapOffset;
    }
  }
  return 0;
}

uint64_t payloadHeapSizeFromFixed(
    const RadixRow2RowSerdeMeta& meta,
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

const char* stringPointer(const StringView& value) {
  return loadUnaligned<const char*>(
      reinterpret_cast<const char*>(&value) + sizeof(uint64_t));
}

void storeStringPointer(void* slot, const char* data) {
  storeUnaligned<const char*>(
      static_cast<char*>(slot) + sizeof(uint64_t), data);
}

struct PayloadHeapRange {
  const char* begin{nullptr};
  uint64_t size{0};
};

void addPayloadHeapRange(
    PayloadHeapRange& range,
    const char* data,
    uint64_t size) {
  if (size == 0) {
    return;
  }
  BOLT_DCHECK_NOT_NULL(data);
  if (range.begin == nullptr) {
    range.begin = data;
  } else {
    BOLT_DCHECK_EQ(data, range.begin + range.size);
  }
  range.size += size;
}

PayloadHeapRange payloadHeapRangeFromFixed(
    const RadixRow2RowSerdeMeta& meta,
    const char* sourceFixed) {
  if (sourceFixed == nullptr || !meta.hasVariablePayload()) {
    return {};
  }

  PayloadHeapRange range;
  for (const auto& op : meta.payloadVariableOps) {
    if (isNull(sourceFixed, op)) {
      continue;
    }
    const auto* sourceSlot = sourceFixed + op.offset;
    if (op.kind == RadixSortSpillPayloadVariableKind::kString) {
      const auto value = loadUnaligned<StringView>(sourceSlot);
      if (value.isInline()) {
        continue;
      }
      addPayloadHeapRange(range, stringPointer(value), value.size());
      continue;
    }
    const auto value = loadUnaligned<PayloadVarlenRef>(sourceSlot);
    if (value.size > 0) {
      addPayloadHeapRange(range, value.data, value.size);
    }
  }
  return range;
}

void clearPayloadPointers(
    const RadixRow2RowSerdeMeta& meta,
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

void restorePayloadPointers(
    const RadixRow2RowSerdeMeta& meta,
    char* payloadFixed,
    char* payloadHeap) {
  auto* heapCursor = payloadHeap;
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
      storeStringPointer(slot, heapCursor);
      heapCursor += value.size();
      continue;
    }
    const auto value = loadUnaligned<PayloadVarlenRef>(slot);
    if (value.size == 0) {
      continue;
    }
    storeUnaligned<PayloadVarlenRef>(
        slot, PayloadVarlenRef{value.size, heapCursor});
    heapCursor += value.size;
  }
}

} // namespace

RadixRow2RowSerdeMeta RadixRow2RowSerdeMeta::create(
    RadixSortKeyLayout keyLayout,
    const PayloadRowLayout* payloadLayout) {
  RadixRow2RowSerdeMeta meta;
  meta.keyLayout = std::move(keyLayout);
  meta.initialize(payloadLayout);
  return meta;
}

void RadixRow2RowSerdeMeta::initialize(const PayloadRowLayout* payloadLayout) {
  runtimeKeyRecordSize = keyLayout.width();
  spilledKeyRecordSize = RadixSortSpillRow::keyRecordCopySize(keyLayout);
  keyHeapOffset = 0;
  keySizeOffset = 0;
  keyDataOffset = 0;
  keyPayloadOffset = 0;
  fixedKeyHeapSize = 0;
  if (keyLayout.isVariable()) {
    keyHeapOffset = keyLayout.heapKeyOffset();
    keySizeOffset = *keyLayout.sizeOffset();
    keyDataOffset = *keyLayout.dataOffset();
    keyHeapMode = RadixSortSpillKeyHeapMode::kVariableRowSize;
  } else {
    keyHeapMode = RadixSortSpillKeyHeapMode::kNone;
  }
  if (keyLayout.hasPayload()) {
    keyPayloadOffset = *keyLayout.payloadOffset();
  }
  payloadFixedSize = payloadLayout == nullptr ? 0 : payloadLayout->rowWidth();
  hasPayload = payloadFixedSize != 0;
  payloadVariableOps.clear();
  if (payloadLayout != nullptr) {
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

uint32_t RadixSortSpillRow::keyRecordCopySize(
    const RadixSortKeyLayout& keyLayout) {
  if (keyLayout.isVariable()) {
    return *keyLayout.dataOffset();
  }
  return keyLayout.hasPayload() ? *keyLayout.payloadOffset()
                                : keyLayout.width();
}

uint64_t RadixSortSpillRow::fixedSerializedRowSize(
    const RadixRow2RowSerdeMeta& meta) {
  uint64_t total = meta.spilledKeyRecordSize;
  if (meta.keyHeapMode == RadixSortSpillKeyHeapMode::kVariableFixedSize) {
    total += meta.fixedKeyHeapSize;
  }
  total += meta.payloadFixedSize;
  return total;
}

uint64_t RadixSortSpillRow::fixedRuntimeRowSize(
    const RadixRow2RowSerdeMeta& meta) {
  uint64_t total = meta.runtimeKeyRecordSize;
  if (meta.keyHeapMode == RadixSortSpillKeyHeapMode::kVariableFixedSize) {
    total += meta.fixedKeyHeapSize;
  }
  total += meta.payloadFixedSize;
  return total;
}

RadixSortSpillRowSize RadixSortSpillRow::sizeForSerialize(
    const RadixRow2RowSerdeMeta& meta,
    const char* key) {
  return sizeForSerialize(
      meta,
      key,
      meta.keyLayout.hasPayload() ? RadixSortKey(meta.keyLayout, key).payload()
                                  : nullptr);
}

RadixSortSpillRowSize RadixSortSpillRow::sizeForSerialize(
    const RadixRow2RowSerdeMeta& meta,
    const char* key,
    const char* payload) {
  RadixSortSpillRowSize result;
  result.keyHeapSize = keyHeapSizeFromRecord(meta, key);
  uint64_t keySize = meta.spilledKeyRecordSize;
  keySize += result.keyHeapSize;
  result.keySize = keySize;
  result.payload = payload;
  result.payloadFixedSize = meta.payloadFixedSize;
  const auto payloadHeap = payloadHeapRangeFromFixed(meta, result.payload);
  result.payloadHeapSize = payloadHeap.size;
  result.payloadHeap = payloadHeap.begin;
  uint64_t total = result.keySize;
  total += result.payloadFixedSize;
  total += result.payloadHeapSize;
  result.totalSize = total;
  uint64_t runtimeSize = meta.runtimeKeyRecordSize;
  runtimeSize += result.keyHeapSize;
  runtimeSize += result.payloadFixedSize;
  runtimeSize += result.payloadHeapSize;
  result.runtimeSize = runtimeSize;
  return result;
}

std::optional<RadixSortSpillRowSize> RadixSortSpillRow::sizeFromDisk(
    const RadixRow2RowSerdeMeta& meta,
    const char* row,
    uint64_t available) {
  if (available < meta.spilledKeyRecordSize) {
    return std::nullopt;
  }
  const auto keyHeapSize = keyHeapSizeFromRecord(meta, row);
  if (keyHeapSize == std::numeric_limits<uint64_t>::max()) {
    return std::nullopt;
  }
  uint64_t cursor = meta.spilledKeyRecordSize;
  auto next = checkedAdd<uint64_t>(cursor, keyHeapSize);
  if (!next.has_value() || *next > available) {
    return std::nullopt;
  }
  cursor = *next;
  next = checkedAdd<uint64_t>(cursor, meta.payloadFixedSize);
  if (!next.has_value() || *next > available) {
    return std::nullopt;
  }
  cursor = *next;
  const auto* payloadFixed =
      meta.hasPayload ? row + cursor - meta.payloadFixedSize : nullptr;
  const auto payloadHeapSize = payloadHeapSizeFromFixed(meta, payloadFixed);
  next = checkedAdd<uint64_t>(cursor, payloadHeapSize);
  if (!next.has_value() || *next > available) {
    return std::nullopt;
  }
  RadixSortSpillRowSize size;
  size.keyHeapSize = keyHeapSize;
  uint64_t keySize = meta.spilledKeyRecordSize;
  keySize += keyHeapSize;
  size.keySize = keySize;
  size.payloadFixedSize = meta.payloadFixedSize;
  size.payloadHeapSize = payloadHeapSize;
  size.payload = payloadFixed;
  size.payloadHeap = payloadHeapSize == 0 ? nullptr : row + cursor;
  size.totalSize = *next;
  uint64_t runtimeSize = meta.runtimeKeyRecordSize;
  runtimeSize += keyHeapSize;
  runtimeSize += meta.payloadFixedSize;
  runtimeSize += payloadHeapSize;
  size.runtimeSize = runtimeSize;
  return size;
}

char* RadixSortSpillRow::serializeRow(
    const RadixRow2RowSerdeMeta& meta,
    const char* key,
    const char* payload,
    char* out) {
  const auto size = sizeForSerialize(meta, key, payload);
  serialize(meta, key, size, out);
  return out + size.totalSize;
}

void RadixSortSpillRow::serialize(
    const RadixRow2RowSerdeMeta& meta,
    const char* key,
    const RadixSortSpillRowSize& size,
    char* destination) {
  auto* current = destination;
  std::memcpy(current, key, meta.spilledKeyRecordSize);
  current += meta.spilledKeyRecordSize;
  if (size.keyHeapSize > 0) {
    std::memcpy(
        current,
        loadCompactPointer(key + meta.keyDataOffset),
        size.keyHeapSize);
    current += size.keyHeapSize;
  }

  if (size.payloadFixedSize == 0) {
    return;
  }
  std::memcpy(current, size.payload, size.payloadFixedSize);
  auto* fixed = current;
  current += size.payloadFixedSize;
  if (!meta.hasVariablePayload()) {
    return;
  }
  clearPayloadPointers(meta, size.payload, fixed);
  if (size.payloadHeapSize > 0) {
    std::memcpy(current, size.payloadHeap, size.payloadHeapSize);
  }
}

RadixSortSpillDeserializedRow RadixSortSpillRow::deserializeRow(
    const RadixRow2RowSerdeMeta& meta,
    const char* in,
    const RadixSortSpillRowSize& size,
    char* out) {
  auto* const runtimeKey = out;
  std::memcpy(runtimeKey, in, meta.spilledKeyRecordSize);
  in += meta.spilledKeyRecordSize;
  out += meta.runtimeKeyRecordSize;

  if (meta.hasKeyHeap()) {
    std::memcpy(out, in, size.keyHeapSize);
    storeCompactPointer(runtimeKey + meta.keyDataOffset, out);
    in += size.keyHeapSize;
    out += size.keyHeapSize;
  }

  char* runtimePayload = nullptr;
  if (meta.payloadFixedSize > 0) {
    runtimePayload = out;
    std::memcpy(runtimePayload, in, meta.payloadFixedSize);
    in += meta.payloadFixedSize;
    out += meta.payloadFixedSize;
  }
  if (meta.keyLayout.hasPayload()) {
    storeCompactPointer(runtimeKey + meta.keyPayloadOffset, runtimePayload);
  }

  if (size.payloadHeapSize > 0) {
    std::memcpy(out, in, size.payloadHeapSize);
    restorePayloadPointers(meta, runtimePayload, out);
    in += size.payloadHeapSize;
    out += size.payloadHeapSize;
  }
  return {in, out, runtimeKey, runtimePayload};
}

uint64_t RadixSortSpillRow::maxRuntimeSizeForBlock(
    const RadixRow2RowSerdeMeta& meta,
    uint64_t serializedBlockSize) {
  const auto fixedSerializedRowSize =
      RadixSortSpillRow::fixedSerializedRowSize(meta);
  if (fixedSerializedRowSize == 0) {
    return serializedBlockSize;
  }
  const auto rowsUpperBound = serializedBlockSize / fixedSerializedRowSize;
  const auto extraPerRow =
      meta.runtimeKeyRecordSize - meta.spilledKeyRecordSize;
  return serializedBlockSize + rowsUpperBound * extraPerRow;
}

} // namespace bytedance::bolt::exec::radixsort
