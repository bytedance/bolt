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

#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "bolt/buffer/Buffer.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/type/Type.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::exec::radixsort {

class RadixSortRun;
class RadixSortRunStorage;
struct EncodedKeyView;
enum class RadixSortKeyLayoutKind : uint8_t;

enum class EncodedKeyFormat : uint8_t {
  kFixed64,
  kVariableBinary,
};

struct RadixSortKeyColumn {
  TypePtr type;
  CompareFlags flags;
  std::optional<uint64_t> maximumEncodedSize;
  std::optional<uint32_t> fixedPrefixOffset;
  bool encodeDecodeSupported;
  std::vector<RadixSortKeyColumn> children;
};

class EncodedKeyBatch {
 public:
  EncodedKeyFormat format() const {
    return format_;
  }

  vector_size_t size() const {
    return size_;
  }

  uint64_t fixedKeyAt(vector_size_t row) const;

  std::string_view variableKeyAt(vector_size_t row) const;

  const BufferPtr& fixedKeys() const {
    return fixedKeys_;
  }

  const BufferPtr& offsets() const {
    return offsets_;
  }

  const BufferPtr& data() const {
    return data_;
  }

 private:
  friend class RadixSortKeyCodec;
  friend class RadixSortRun;

  void resetData() {
    data_.reset();
  }

  EncodedKeyFormat format_{EncodedKeyFormat::kVariableBinary};
  vector_size_t size_{0};
  BufferPtr fixedKeys_;
  BufferPtr offsets_;
  BufferPtr data_;
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

  bool canEncodeDecode() const {
    return canEncodeDecode_;
  }

  std::vector<uint32_t> leadingSkippableValidityOffsets(
      std::span<const uint8_t> keyMayHaveNulls,
      uint32_t radixWidth) const;

  uint32_t heapKeyOffsetForVariableLayout(uint32_t inlineCapacity) const;

  uint32_t fixedPrefixColumnCount(uint32_t heapKeyOffset) const;

  void encode(
      const RowVector& input,
      memory::MemoryPool* pool,
      EncodedKeyBatch& result) const;

  void decode(
      std::span<const EncodedKeyView> keys,
      std::span<const uint8_t> decodedColumns,
      std::span<const uint8_t> mayHaveNulls,
      memory::MemoryPool* pool,
      BufferPtr& cursorScratch,
      RowVectorPtr& result,
      uint32_t firstColumn = 0) const;

 private:
  void encodeSingleFixedFlat(
      const RowVector& input,
      memory::MemoryPool* pool,
      EncodedKeyBatch& result) const;

  RadixSortKeyCodec(
      std::vector<RadixSortKeyColumn> columns,
      EncodedKeyFormat format,
      std::optional<uint64_t> maximumEncodedSize,
      bool canEncodeDecode);

  bool canAppendSingleFixedFlat(
      const BaseVector& input,
      const RadixSortRunStorage& arena) const;

  bool tryAppendSingleFixedFlat(
      const BaseVector& input,
      vector_size_t size,
      RadixSortRunStorage& arena,
      std::span<char* const> payloads) const;

  bool canDecodeSingleFixedColumn() const;

  bool tryDecodeSingleFixedColumn(
      const RadixSortRunStorage& arena,
      uint64_t begin,
      vector_size_t count,
      bool mayHaveNulls,
      memory::MemoryPool* pool,
      RowVectorPtr& result) const;

  bool tryDecodeSingleFixedColumn(
      std::span<const char* const> keys,
      RadixSortKeyLayoutKind layoutKind,
      bool mayHaveNulls,
      memory::MemoryPool* pool,
      RowVectorPtr& result) const;

  bool encodeAndAppendVariable(
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

  void decodeFixedPrefix(
      std::span<const char* const> keys,
      std::span<const uint8_t> decodedColumns,
      std::span<const uint8_t> mayHaveNulls,
      RowVectorPtr& result,
      uint32_t prefixColumnCount) const;

  std::vector<RadixSortKeyColumn> columns_;
  RowTypePtr rowType_;
  EncodedKeyFormat format_;
  std::optional<uint64_t> maximumEncodedSize_;
  bool canEncodeDecode_;
  mutable std::vector<uint64_t> encodeCursorScratch_;
};

} // namespace bytedance::bolt::exec::radixsort
