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

enum class RadixSortSpillKeyHeapMode : uint8_t {
  kNone,
  kVariableFixedSize,
  kVariableRowSize,
};

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

struct RadixRow2RowSerdeMeta {
  RadixSortKeyLayout keyLayout;
  uint64_t payloadFixedSize{0};

  uint32_t spilledKeyRecordSize{0};
  uint32_t runtimeKeyRecordSize{0};
  uint32_t keyHeapOffset{0};
  uint32_t keySizeOffset{0};
  uint32_t keyDataOffset{0};
  uint32_t keyPayloadOffset{0};
  RadixSortSpillKeyHeapMode keyHeapMode{RadixSortSpillKeyHeapMode::kNone};
  uint64_t fixedKeyHeapSize{0};

  bool hasPayload{false};
  std::vector<RadixSortSpillPayloadVariableOp> payloadVariableOps;

  static RadixRow2RowSerdeMeta create(
      RadixSortKeyLayout keyLayout,
      const PayloadRowLayout* payloadLayout);

  void initialize(const PayloadRowLayout* payloadLayout);

  bool hasKeyHeap() const {
    return keyHeapMode != RadixSortSpillKeyHeapMode::kNone;
  }

  bool hasVariablePayload() const {
    return !payloadVariableOps.empty();
  }
};

using RadixSortSpillRunMeta = RadixRow2RowSerdeMeta;

struct RadixSortSpillRowSize {
  uint64_t totalSize{0};
  uint64_t keySize{0};
  uint64_t payloadFixedSize{0};
  uint64_t payloadHeapSize{0};
  uint64_t keyHeapSize{0};
  uint64_t runtimeSize{0};
  const char* payload{nullptr};
  const char* payloadHeap{nullptr};
};

struct RadixSortSpillDeserializedRow {
  const char* nextInput{nullptr};
  char* nextOutput{nullptr};
  char* key{nullptr};
  char* payload{nullptr};
};

struct RadixSortSpillSectionDeserializedRow {
  char* nextOutput{nullptr};
  char* key{nullptr};
  char* payload{nullptr};
};

class RadixSortSpillRow {
 public:
  static constexpr uint16_t kVersion = 1;

  static uint32_t keyRecordCopySize(const RadixSortKeyLayout& keyLayout);

  static uint64_t fixedSerializedRowSize(const RadixRow2RowSerdeMeta& meta);

  static uint64_t fixedRuntimeRowSize(const RadixRow2RowSerdeMeta& meta);

  static RadixSortSpillRowSize sizeForSerialize(
      const RadixRow2RowSerdeMeta& meta,
      const char* key);

  static RadixSortSpillRowSize sizeForSerialize(
      const RadixRow2RowSerdeMeta& meta,
      const char* key,
      const char* payload);

  static std::optional<RadixSortSpillRowSize> sizeFromDisk(
      const RadixRow2RowSerdeMeta& meta,
      const char* row,
      uint64_t available);

  static char* serializeRow(
      const RadixRow2RowSerdeMeta& meta,
      const char* key,
      const char* payload,
      char* out);

  static RadixSortSpillDeserializedRow deserializeRow(
      const RadixRow2RowSerdeMeta& meta,
      const char* in,
      const RadixSortSpillRowSize& size,
      char* out);

  static void serializeRowToSections(
      const RadixRow2RowSerdeMeta& meta,
      const char* key,
      const RadixSortSpillRowSize& size,
      char* keyRecord,
      char*& keyHeap,
      char* payloadFixed,
      char*& payloadHeap);

  static std::optional<RadixSortSpillSectionDeserializedRow>
  deserializeRowFromSections(
      const RadixRow2RowSerdeMeta& meta,
      char* keyRecord,
      char*& keyHeap,
      const char* keyHeapEnd,
      char* payloadFixed,
      char*& payloadHeap,
      const char* payloadHeapEnd,
      char* out);

  static void serialize(
      const RadixRow2RowSerdeMeta& meta,
      const char* key,
      const RadixSortSpillRowSize& size,
      char* destination);
};

} // namespace bytedance::bolt::exec::radixsort
