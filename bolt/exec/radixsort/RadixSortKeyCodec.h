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

  EncodedKeyFormat format_{EncodedKeyFormat::kVariableBinary};
  vector_size_t size_{0};
  BufferPtr fixedKeys_;
  BufferPtr offsets_;
  BufferPtr data_;
};

class RadixSortKeyCodec {
 public:
  static bool supportsEncodeDecode(const Type& type);

  static void bind(
      const std::vector<TypePtr>& types,
      const std::vector<CompareFlags>& flags,
      const std::vector<bool>& knownNonNull,
      std::unique_ptr<RadixSortKeyCodec>& codec);

  static void bind(
      const std::vector<TypePtr>& types,
      const std::vector<CompareFlags>& flags,
      std::unique_ptr<RadixSortKeyCodec>& codec) {
    bind(types, flags, {}, codec);
  }

  const std::vector<RadixSortKeyColumn>& columns() const {
    return columns_;
  }

  EncodedKeyFormat format() const {
    return format_;
  }

  std::optional<uint64_t> maximumEncodedSize() const {
    return maximumEncodedSize_;
  }

  bool canEncodeDecode() const {
    return canEncodeDecode_;
  }

  const std::vector<uint32_t>& leadingSkippableValidityOffsets() const {
    return leadingSkippableValidityOffsets_;
  }

  std::vector<uint32_t> leadingSkippableValidityOffsets(
      std::span<const uint8_t> keyMayHaveNulls) const;

  void encode(
      const RowVector& input,
      memory::MemoryPool* pool,
      EncodedKeyBatch& result) const;

  bool canEncodeSingleFixedFlat(
      const BaseVector& input,
      const RadixSortRunStorage& arena) const;

  void appendSingleFixedFlat(
      const BaseVector& input,
      vector_size_t size,
      RadixSortRunStorage& arena,
      std::span<char* const> payloads) const;

  void decode(
      const EncodedKeyBatch& keys,
      memory::MemoryPool* pool,
      RowVectorPtr& result) const;

  void decode(
      std::span<const EncodedKeyView> keys,
      memory::MemoryPool* pool,
      RowVectorPtr& result) const;

  void decode(
      std::span<const EncodedKeyView> keys,
      memory::MemoryPool* pool,
      BufferPtr& cursorScratch,
      RowVectorPtr& result) const;

  void decode(
      std::span<const EncodedKeyView> keys,
      std::span<const uint8_t> decodedColumns,
      std::span<const uint8_t> mayHaveNulls,
      memory::MemoryPool* pool,
      BufferPtr& cursorScratch,
      RowVectorPtr& result) const;

  bool canDecodeSingleFixedColumn() const;

  void decodeSingleFixedColumn(
      const RadixSortRunStorage& arena,
      uint64_t begin,
      vector_size_t count,
      bool mayHaveNulls,
      memory::MemoryPool* pool,
      RowVectorPtr& result) const;

  void decodeSingleFixedColumn(
      std::span<const char* const> keys,
      RadixSortKeyLayoutKind layoutKind,
      bool mayHaveNulls,
      memory::MemoryPool* pool,
      RowVectorPtr& result) const;

  int32_t compare(
      const EncodedKeyBatch& keys,
      vector_size_t left,
      vector_size_t right) const;

 private:
  void encodeSingleFixedFlat(
      const RowVector& input,
      memory::MemoryPool* pool,
      EncodedKeyBatch& result) const;

  RadixSortKeyCodec(
      std::vector<RadixSortKeyColumn> columns,
      EncodedKeyFormat format,
      std::optional<uint64_t> maximumEncodedSize,
      bool canEncodeDecode,
      std::vector<uint32_t> leadingSkippableValidityOffsets);

  std::vector<RadixSortKeyColumn> columns_;
  RowTypePtr rowType_;
  EncodedKeyFormat format_;
  std::optional<uint64_t> maximumEncodedSize_;
  bool canEncodeDecode_;
  std::vector<uint32_t> leadingSkippableValidityOffsets_;
  mutable std::vector<uint64_t> encodeCursorScratch_;
};

} // namespace bytedance::bolt::exec::radixsort
