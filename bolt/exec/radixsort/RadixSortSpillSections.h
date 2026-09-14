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

#include <cstdint>
#include <vector>

#include "bolt/exec/radixsort/PayloadRow.h"
#include "bolt/exec/radixsort/RadixSortKey.h"

namespace bytedance::bolt::exec::radixsort {

enum class RadixSortSpillPayloadVariableKind : uint8_t {
  kString,
  kComplex,
};

struct RadixSortSpillPayloadVariableOp {
  uint64_t offset{0};
  uint32_t width{0};
  uint32_t nullByte{0};
  uint8_t nullMask{0};
  RadixSortSpillPayloadVariableKind kind{
      RadixSortSpillPayloadVariableKind::kString};
};

struct RadixSortSpillSectionMeta {
  RadixSortKeyLayout keyLayout;
  uint64_t payloadFixedSize{0};

  uint32_t runtimeKeyRecordSize{0};
  uint32_t wireKeyRecordSize{0};
  uint32_t keyHeapOffset{0};
  uint32_t keySizeOffset{0};
  uint32_t keyPayloadOffset{0};

  bool hasKeyHeap{false};
  bool hasPayload{false};
  std::vector<RadixSortSpillPayloadVariableOp> payloadVariableOps;

  static RadixSortSpillSectionMeta create(
      RadixSortKeyLayout keyLayout,
      const PayloadRowLayout* payloadLayout);

  void initialize(const PayloadRowLayout* payloadLayout);

  bool hasVariablePayload() const {
    return !payloadVariableOps.empty();
  }

  uint64_t fixedWireBytesPerRow() const {
    return wireKeyRecordSize + payloadFixedSize;
  }
};

struct RadixSortSpillSectionBatchSize {
  uint64_t rowCount{0};
  uint64_t keyHeapBytes{0};
  uint64_t payloadHeapBytes{0};

  uint64_t totalBytes(uint64_t fixedWireBytesPerRow) const {
    return rowCount * fixedWireBytesPerRow + keyHeapBytes + payloadHeapBytes;
  }
};

class RadixSortSpillSections {
 public:
  static RadixSortSpillSectionBatchSize sizeForSerializeRows(
      const RadixSortSpillSectionMeta& meta,
      const char* keyBase,
      uint64_t maxRowCount,
      uint64_t maxBytes);

  static void copyRowsToSections(
      const RadixSortSpillSectionMeta& meta,
      const char* keyBase,
      uint64_t rowCount,
      uint64_t keyHeapBytes,
      uint64_t payloadHeapBytes,
      char* keyRecords,
      char*& keyHeap,
      char* payloadFixedRows,
      char*& payloadHeap);

  static bool restorePayloadPointersInSectionRows(
      const RadixSortSpillSectionMeta& meta,
      uint64_t rowCount,
      char* payloadFixedRows,
      char*& payloadHeap,
      const char* payloadHeapEnd);
};

} // namespace bytedance::bolt::exec::radixsort
