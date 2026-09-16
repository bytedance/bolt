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

#include <array>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "bolt/buffer/Buffer.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/exec/radixsort/RadixSortKey.h"
#include "bolt/type/Type.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::exec::radixsort {
class RadixSortRun;
class RadixSortRunStorage;
struct EncodedKeyView;
enum class RadixSortKeyLayoutKind : uint8_t;
class EncodedKeyReader;
struct RadixSortKeyColumn;

using RadixSortPhysicalComparator = int32_t (*)(
    const RadixSortKeyColumn&,
    const char*,
    const char*,
    uint32_t,
    uint32_t);
using RadixSortEncodedComparator = int32_t (*)(
    const RadixSortKeyColumn&,
    EncodedKeyReader&,
    EncodedKeyReader&);

struct RadixSortKeyColumn {
  TypePtr type;
  CompareFlags flags;
  std::optional<uint64_t> maximumEncodedSize;
  std::optional<uint32_t> fixedPrefixOffset;
  bool containsFloatingPoint{false};
  mutable bool hasSpecialValues{false};
  mutable RadixSortPhysicalComparator fixedComparator{nullptr};
  mutable RadixSortPhysicalComparator variableComparator{nullptr};
  mutable RadixSortEncodedComparator encodedComparator{nullptr};
  std::vector<RadixSortKeyColumn> children;
};

struct RadixSortFloatingPointDigit {
  uint32_t offset{0};
  uint8_t width{0};
  bool descending{false};

  template <bool HostOrderWords>
  uint8_t extract(const char* key, uint32_t byte, uint32_t wordBytes) const {
    if (width == sizeof(float)) {
      auto value =
          loadEncodedUnsigned<HostOrderWords, uint32_t>(key, offset, wordBytes);
      value = normalizeFloatingPointKey(value, descending);
      return static_cast<uint8_t>(value >> ((width - 1 - (byte - offset)) * 8));
    }
    auto value =
        loadEncodedUnsigned<HostOrderWords, uint64_t>(key, offset, wordBytes);
    value = normalizeFloatingPointKey(value, descending);
    return static_cast<uint8_t>(value >> ((width - 1 - (byte - offset)) * 8));
  }
};

struct RadixSortFloatingPointPlan {
  uint32_t radixWidth{0};
  bool complete{false};
  std::array<RadixSortFloatingPointDigit, 32> digits{};
};

class RadixSortKeyCodec {
  friend class RadixSortRun;

 public:
  static bool supportsEncodeDecode(const Type& type);

  static void bind(
      const std::vector<TypePtr>& types,
      const std::vector<CompareFlags>& flags,
      std::unique_ptr<RadixSortKeyCodec>& codec);

  std::optional<uint64_t> maximumEncodedSize() const {
    return maximumEncodedSize_;
  }

  std::vector<uint32_t> leadingSkippableValidityOffsets(
      std::span<const uint8_t> keyMayHaveNulls,
      uint32_t radixWidth) const;

  uint32_t heapKeyOffsetForVariableLayout(uint32_t inlineCapacity) const;

  uint32_t fixedPrefixColumnCount(uint32_t heapKeyOffset) const;

  const RadixSortKeyColumn* singleFloatingPointColumn() const {
    return columns_.size() == 1 &&
            (columns_[0].type->kind() == TypeKind::REAL ||
             columns_[0].type->kind() == TypeKind::DOUBLE)
        ? &columns_[0]
        : nullptr;
  }

  bool hasSpecialValues() const;

  // Selects comparators once from the per-column special-value metadata.
  void prepareSpecialComparators() const;

  std::vector<uint8_t> specialValueFlags() const;

  void mergeSpecialValueFlags(std::span<const uint8_t> flags);

  RadixSortFloatingPointPlan floatingPointPlan(
      const RadixSortKeyLayout& layout,
      std::span<const uint8_t> mayHaveNulls) const;

  int32_t compareEncoded(
      std::string_view left,
      std::string_view right,
      uint32_t firstColumn = 0) const;

  int32_t comparePhysical(
      const RadixSortKeyLayout& layout,
      const char* left,
      const char* right,
      std::string_view leftSuffix = {},
      std::string_view rightSuffix = {}) const;

  void decode(
      std::span<const EncodedKeyView> keys,
      std::span<const uint8_t> decodedColumns,
      std::span<const uint8_t> mayHaveNulls,
      memory::MemoryPool* pool,
      BufferPtr& cursorScratch,
      RowVectorPtr& result,
      uint32_t firstColumn = 0) const;

 private:
  RadixSortKeyCodec(
      std::vector<RadixSortKeyColumn> columns,
      std::optional<uint64_t> maximumEncodedSize);

  bool canAppendSingleFixedFlat(
      const BaseVector& input,
      const RadixSortRunStorage& arena) const;

  bool tryAppendSingleFixedFlat(
      const BaseVector& input,
      vector_size_t size,
      RadixSortRunStorage& arena,
      std::span<char* const> payloads) const;

  uint64_t append(
      const RowVector& input,
      RadixSortRunStorage& storage,
      std::span<char* const> payloads,
      BufferPtr& sizeScratch) const;

  bool canDecodeSingleFixedColumn() const;

  bool tryDecodeSingleFixedColumn(
      const RadixSortRunStorage& arena,
      uint64_t begin,
      vector_size_t count,
      bool mayHaveNulls,
      memory::MemoryPool* pool,
      RowVectorPtr& result) const;

  std::optional<uint32_t> singleFixedWordBytes(
      RadixSortKeyLayoutKind layoutKind) const;

  void decodeSingleFixedAt(
      std::span<const char* const> keys,
      bool mayHaveNulls,
      vector_size_t outputOffset,
      uint32_t inlineWordBytes,
      const VectorPtr& destination) const;

  uint64_t decodeScratchWordsPerRowWithMask(
      std::span<const uint8_t> decodedColumns,
      std::span<const uint8_t> mayHaveNulls,
      uint32_t firstColumn,
      uint32_t endColumn) const;

  void decodeSuffixAtWithPreparedScratch(
      std::span<const EncodedKeyView> keys,
      std::span<const uint8_t> decodedColumns,
      std::span<const uint8_t> mayHaveNulls,
      vector_size_t outputOffset,
      memory::MemoryPool* scratchPool,
      const BufferPtr& cursorScratch,
      RowVector& output,
      std::span<const column_index_t> directKeyChannels,
      uint64_t scratchWordsPerRow,
      uint32_t firstColumn,
      uint32_t endColumn) const;

  void decodePrefixAt(
      std::span<const char* const> keys,
      std::span<const uint8_t> decodedColumns,
      std::span<const uint8_t> mayHaveNulls,
      vector_size_t outputOffset,
      RowVector& output,
      std::span<const column_index_t> directKeyChannels,
      uint32_t prefixColumnCount) const;

  void finishDecode(
      std::span<const uint8_t> decodedColumns,
      RowVector& output,
      std::span<const column_index_t> directKeyChannels) const;

  uint64_t appendVariable(
      const RowVector& input,
      RadixSortRunStorage& arena,
      std::span<char* const> payloads,
      uint32_t firstSuffixColumn,
      BufferPtr& sizeScratch) const;

  void decodeFixedPrefix(
      const RadixSortRunStorage& arena,
      uint64_t begin,
      vector_size_t count,
      std::span<const uint8_t> decodedColumns,
      std::span<const uint8_t> mayHaveNulls,
      RowVectorPtr& result,
      uint32_t prefixColumnCount) const;

  std::vector<RadixSortKeyColumn> columns_;
  RowTypePtr rowType_;
  std::optional<uint64_t> maximumEncodedSize_;
  bool allFixedScalarColumns_;
  uint32_t floatingPointEnd_{0};
  mutable uint32_t specialComparisonEnd_{0};
  mutable std::vector<uint64_t> encodeCursorScratch_;
};

} // namespace bytedance::bolt::exec::radixsort
