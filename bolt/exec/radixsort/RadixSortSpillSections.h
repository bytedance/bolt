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
#include <optional>
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
  uint32_t keyHeapOffset{0};
  uint32_t keySizeOffset{0};
  uint32_t keyDataOffset{0};
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
};

struct RadixSortSpillSectionSize {
  uint64_t totalSize{0};
  uint64_t keyHeapSize{0};
  uint64_t payloadHeapSize{0};
};

class RadixSortSpillSections {
 public:
  static RadixSortSpillSectionSize sizeForSerialize(
      const RadixSortSpillSectionMeta& meta,
      const char* key);

  static void copyKeyHeapToSection(
      const RadixSortSpillSectionMeta& meta,
      const char* sourceKey,
      uint64_t keyHeapSize,
      char*& keyHeap);

  static void copyPayloadFixedToSection(
      const RadixSortSpillSectionMeta& meta,
      const char* sourceKey,
      char* payloadFixed);

  static void copyPayloadHeapFromFixedToSection(
      const RadixSortSpillSectionMeta& meta,
      const char* payloadFixed,
      uint64_t payloadHeapSize,
      char*& payloadHeap);

  static void clearKeyPointers(
      const RadixSortSpillSectionMeta& meta,
      char* keyRecords,
      uint64_t rowCount);

  static void clearPayloadPointers(
      const RadixSortSpillSectionMeta& meta,
      char* payloadFixedRows,
      uint64_t rowCount);

  static std::optional<uint64_t> restorePointersInSections(
      const RadixSortSpillSectionMeta& meta,
      char* key,
      char*& keyHeap,
      const char* keyHeapEnd,
      char* payloadFixed,
      char*& payloadHeap,
      const char* payloadHeapEnd);
};

} // namespace bytedance::bolt::exec::radixsort
