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

#include <span>
#include <string_view>

#include "bolt/exec/radixsort/PayloadRowLayout.h"
#include "bolt/exec/radixsort/RadixSortKey.h"

namespace bytedance::bolt::exec::radixsort {

struct RadixSortSpillRowHeader {
  uint32_t totalSize;
};

static_assert(sizeof(RadixSortSpillRowHeader) == 4);
static_assert(std::is_trivially_copyable_v<RadixSortSpillRowHeader>);

struct RadixSortSpillRunMeta {
  RadixSortKeyLayout keyLayout;
  uint32_t payloadFixedSize{0};
};

struct RadixSortSpillRowSize {
  uint32_t totalSize{0};
  uint32_t keySize{0};
  uint32_t payloadFixedSize{0};
  uint64_t payloadHeapSize{0};
  uint64_t keyHeapSize{0};
  char* payload{nullptr};
};

class RadixSortSpillRow {
 public:
  static constexpr uint16_t kVersion = 1;
  static constexpr uint64_t kHeaderSize = sizeof(RadixSortSpillRowHeader);

  static uint32_t keyFixedSize(const RadixSortKeyLayout& keyLayout);

  static RadixSortSpillRowSize sizeForSerialize(
      const RadixSortKeyLayout& keyLayout,
      const PayloadRowLayout* payloadLayout,
      const char* key);

  static RadixSortSpillRowSize sizeForSerialize(
      const RadixSortKeyLayout& keyLayout,
      const PayloadRowLayout* payloadLayout,
      const char* key,
      char* payload);

  static void serialize(
      const RadixSortKeyLayout& keyLayout,
      const PayloadRowLayout* payloadLayout,
      const char* key,
      char* destination);

  static void serialize(
      const RadixSortKeyLayout& keyLayout,
      const PayloadRowLayout* payloadLayout,
      const char* key,
      const RadixSortSpillRowSize& size,
      char* destination);

  explicit RadixSortSpillRow(char* row) : row_(row) {}

  explicit RadixSortSpillRow(const char* row) : row_(const_cast<char*>(row)) {}

  void validate(const RadixSortSpillRunMeta& meta) const;

  RadixSortSpillRowHeader header() const;

  uint32_t totalSize() const {
    return header().totalSize;
  }

  uint32_t trustedKeySize(const RadixSortSpillRunMeta& meta) const;

  uint32_t trustedPayloadHeapSize(const RadixSortSpillRunMeta& meta) const;

  std::string_view trustedKeyBytes(const RadixSortSpillRunMeta& meta) const;

  char* trustedPayloadFixed(const RadixSortSpillRunMeta& meta) const;

  char* trustedPayloadHeap(const RadixSortSpillRunMeta& meta) const;

  void trustedRestoreKeyDataPointer(const RadixSortSpillRunMeta& meta) const;

  void trustedRestorePayloadPointers(
      const RadixSortSpillRunMeta& meta,
      const PayloadRowLayout& payloadLayout) const;

 private:
  char* trustedPayloadFixedOrEnd(const RadixSortSpillRunMeta& meta) const;

  char* row_;
};

} // namespace bytedance::bolt::exec::radixsort
