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

#include "bolt/exec/radixsort/RadixSortKeyCodec.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <limits>
#include <type_traits>

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/radixsort/PayloadRow.h"
#include "bolt/exec/radixsort/RadixSortRunStorage.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/ConstantVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/SimpleVector.h"

namespace bytedance::bolt::exec::radixsort {
namespace {

constexpr uint8_t kNullFirstMarker = 1;
constexpr uint8_t kNullLastMarker = 2;
constexpr uint8_t kStringDelimiter = 0;
constexpr uint8_t kBlobEscape = 1;

bool validFlags(const CompareFlags& flags) {
  return !flags.equalsOnly && !flags.compareSizeFirst &&
      flags.nullHandlingMode == CompareFlags::NullHandlingMode::kNullAsValue;
}

std::optional<uint64_t> fixedBodySize(const Type& type) {
  if (type.isShortDecimal()) {
    return sizeof(int64_t);
  }
  if (type.isLongDecimal()) {
    return sizeof(int128_t);
  }
  switch (type.kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
      return 1;
    case TypeKind::SMALLINT:
      return 2;
    case TypeKind::INTEGER:
    case TypeKind::REAL:
      return 4;
    case TypeKind::BIGINT:
    case TypeKind::DOUBLE:
      return 8;
    case TypeKind::HUGEINT:
      return 16;
    case TypeKind::TIMESTAMP:
      return sizeof(int64_t) + sizeof(uint64_t);
    case TypeKind::UNKNOWN:
      return 0;
    default:
      return std::nullopt;
  }
}

bool supportsType(const Type& type) {
  if (type.kind() == TypeKind::UNKNOWN) {
    return true;
  }
  if (type.isDecimal()) {
    return true;
  }
  switch (type.kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::HUGEINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::TIMESTAMP:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return true;
    case TypeKind::ARRAY:
      return supportsType(*type.childAt(0));
    case TypeKind::ROW: {
      for (uint32_t child = 0; child < type.size(); ++child) {
        if (!supportsType(*type.childAt(child))) {
          return false;
        }
      }
      return true;
    }
    case TypeKind::MAP:
      return supportsType(*type.childAt(0)) && supportsType(*type.childAt(1));
    default:
      return false;
  }
}

void buildMetadata(
    const TypePtr& type,
    const CompareFlags& flags,
    RadixSortKeyColumn& metadata) {
  BOLT_CHECK(
      validFlags(flags),
      "Radix sort key comparison flags are not order-by flags");

  metadata.type = type;
  metadata.flags = flags;
  metadata.encodeDecodeSupported = supportsType(*type);
  auto bodySize = fixedBodySize(*type);
  if (bodySize.has_value()) {
    metadata.maximumEncodedSize = *bodySize + 1;
  }

  if (type->kind() == TypeKind::ARRAY || type->kind() == TypeKind::ROW ||
      type->kind() == TypeKind::MAP) {
    metadata.children.reserve(type->size());
    for (uint32_t child = 0; child < type->size(); ++child) {
      RadixSortKeyColumn childMetadata;
      buildMetadata(type->childAt(child), flags, childMetadata);
      metadata.children.push_back(std::move(childMetadata));
    }
    if (type->kind() == TypeKind::ROW) {
      std::optional<uint64_t> maximumSize = 1;
      for (const auto& child : metadata.children) {
        if (!maximumSize.has_value() || !child.maximumEncodedSize.has_value()) {
          maximumSize = std::nullopt;
          break;
        }
        maximumSize = checkedAdd(*maximumSize, *child.maximumEncodedSize);
      }
      metadata.maximumEncodedSize = maximumSize;
    }
  }
}

uint8_t nullMarker(const CompareFlags& flags) {
  return flags.nullsFirst ? kNullFirstMarker : kNullLastMarker;
}

uint8_t validMarker(const CompareFlags& flags) {
  return flags.nullsFirst ? kNullLastMarker : kNullFirstMarker;
}

template <typename T>
FOLLY_ALWAYS_INLINE void
encodeUnsigned(T value, char* output, bool descending) {
  static_assert(std::is_unsigned_v<T>);
  for (uint32_t byte = 0; byte < sizeof(T); ++byte) {
    auto encoded = static_cast<uint8_t>(
        value >>
        ((sizeof(T) - byte - 1) * std::numeric_limits<uint8_t>::digits));
    output[byte] = static_cast<char>(
        descending ? static_cast<uint8_t>(~encoded) : encoded);
  }
}

template <typename T>
FOLLY_ALWAYS_INLINE void encodeSigned(T value, char* output, bool descending) {
  static_assert(std::is_signed_v<T>);
  using Unsigned = std::make_unsigned_t<T>;
  Unsigned bits;
  std::memcpy(&bits, &value, sizeof(bits));
  bits ^= Unsigned{1} << (sizeof(T) * std::numeric_limits<uint8_t>::digits - 1);
  encodeUnsigned(bits, output, descending);
}

template <typename T>
FOLLY_ALWAYS_INLINE void
encodeUnsignedWord(T value, char* output, bool descending) {
  static_assert(std::is_unsigned_v<T>);
  static_assert(sizeof(T) <= sizeof(uint64_t));
  auto encoded = toBigEndian(value);
  if (descending) {
    encoded = static_cast<T>(~encoded);
  }
  storeUnaligned<T>(output, encoded);
}

template <typename T>
FOLLY_ALWAYS_INLINE void
encodeSignedWord(T value, char* output, bool descending) {
  static_assert(std::is_signed_v<T>);
  using Unsigned = std::make_unsigned_t<T>;
  Unsigned bits;
  std::memcpy(&bits, &value, sizeof(bits));
  bits ^= Unsigned{1} << (sizeof(T) * std::numeric_limits<uint8_t>::digits - 1);
  encodeUnsignedWord(bits, output, descending);
}

uint32_t encodeFloat(float value) {
  if (value == 0) {
    return uint32_t{1} << 31;
  }
  if (std::isnan(value)) {
    return std::numeric_limits<uint32_t>::max();
  }
  if (std::isinf(value)) {
    return value > 0 ? std::numeric_limits<uint32_t>::max() - 1 : 0;
  }
  uint32_t bits;
  std::memcpy(&bits, &value, sizeof(bits));
  return (bits & (uint32_t{1} << 31)) == 0 ? bits | (uint32_t{1} << 31) : ~bits;
}

uint64_t encodeDouble(double value) {
  if (value == 0) {
    return uint64_t{1} << 63;
  }
  if (std::isnan(value)) {
    return std::numeric_limits<uint64_t>::max();
  }
  if (std::isinf(value)) {
    return value > 0 ? std::numeric_limits<uint64_t>::max() - 1 : 0;
  }
  uint64_t bits;
  std::memcpy(&bits, &value, sizeof(bits));
  return (bits & (uint64_t{1} << 63)) == 0 ? bits | (uint64_t{1} << 63) : ~bits;
}

template <typename T>
T valueAt(const BaseVector& vector, vector_size_t row) {
  const auto* base = vector.wrappedVector();
  return base->asUnchecked<SimpleVector<T>>()->valueAt(
      vector.wrappedIndex(row));
}

bool isFixedScalarColumn(const RadixSortKeyColumn& column) {
  return column.type->kind() != TypeKind::UNKNOWN &&
      fixedBodySize(*column.type).has_value();
}

template <typename T, typename EncodeBody>
void encodeSingleFixedFlat(
    const RadixSortKeyColumn& column,
    const FlatVector<T>& input,
    vector_size_t size,
    EncodedKeyFormat format,
    uint64_t* words,
    const uint64_t* offsets,
    char* data,
    EncodeBody encodeBody) {
  const auto* nulls = input.rawNulls();
  const bool descending = !column.flags.ascending;

  if (format == EncodedKeyFormat::kFixed64) {
    for (vector_size_t row = 0; row < size; ++row) {
      std::array<char, sizeof(uint64_t)> bytes{};
      if (nulls != nullptr && bits::isBitNull(nulls, row)) {
        bytes[0] = static_cast<char>(nullMarker(column.flags));
      } else {
        bytes[0] = static_cast<char>(validMarker(column.flags));
        encodeBody(input, row, bytes.data() + 1, descending);
      }
      auto word = loadUnaligned<uint64_t>(bytes.data());
      if constexpr (std::endian::native == std::endian::little) {
        word = byteSwap(word);
      }
      words[row] = word;
    }
    return;
  }

  for (vector_size_t row = 0; row < size; ++row) {
    auto* output = data + offsets[row];
    if (nulls != nullptr && bits::isBitNull(nulls, row)) {
      output[0] = static_cast<char>(nullMarker(column.flags));
    } else {
      output[0] = static_cast<char>(validMarker(column.flags));
      encodeBody(input, row, output + 1, descending);
    }
  }
}

template <typename T, typename EncodeBody>
void encodeSingleVariableFixedFlat(
    const RadixSortKeyColumn& column,
    const FlatVector<T>& input,
    vector_size_t size,
    const uint64_t* offsets,
    char* data,
    EncodeBody encodeBody) {
  const auto* nulls = input.rawNulls();
  const bool descending = !column.flags.ascending;
  for (vector_size_t row = 0; row < size; ++row) {
    auto* output = data + offsets[row];
    if (nulls != nullptr && bits::isBitNull(nulls, row)) {
      output[0] = static_cast<char>(nullMarker(column.flags));
    } else {
      output[0] = static_cast<char>(validMarker(column.flags));
      encodeBody(input, row, output + 1, descending);
    }
  }
}

void encodeSingleFixedFlat(
    const RadixSortKeyColumn& column,
    const BaseVector& input,
    vector_size_t size,
    EncodedKeyFormat format,
    uint64_t* words,
    const uint64_t* offsets,
    char* data) {
  if (column.type->isShortDecimal()) {
    const auto* flat = input.asUnchecked<FlatVector<int64_t>>();
    if (format == EncodedKeyFormat::kVariableBinary) {
      encodeSingleVariableFixedFlat(
          column,
          *flat,
          size,
          offsets,
          data,
          [](const auto& values, auto row, auto* output, bool descending) {
            encodeSignedWord<int64_t>(
                values.rawValues()[row], output, descending);
          });
      return;
    }
    encodeSingleFixedFlat(
        column,
        *flat,
        size,
        format,
        words,
        offsets,
        data,
        [](const auto& values, auto row, auto* output, bool descending) {
          encodeSigned<int64_t>(values.rawValues()[row], output, descending);
        });
    return;
  }
  if (column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    const auto* flat = input.asUnchecked<FlatVector<int128_t>>();
    encodeSingleFixedFlat(
        column,
        *flat,
        size,
        format,
        words,
        offsets,
        data,
        [](const auto& values, auto row, auto* output, bool descending) {
          const auto value = values.rawValues()[row];
          encodeSigned<int64_t>(
              static_cast<int64_t>(HugeInt::upper(value)), output, descending);
          encodeUnsigned<uint64_t>(
              HugeInt::lower(value), output + sizeof(int64_t), descending);
        });
    return;
  }
  if (format == EncodedKeyFormat::kVariableBinary &&
      column.type->kind() == TypeKind::BIGINT) {
    const auto* flat = input.asUnchecked<FlatVector<int64_t>>();
    encodeSingleVariableFixedFlat(
        column,
        *flat,
        size,
        offsets,
        data,
        [](const auto& values, auto row, auto* output, bool descending) {
          encodeSignedWord<int64_t>(
              values.rawValues()[row], output, descending);
        });
    return;
  }
  if (format == EncodedKeyFormat::kVariableBinary &&
      column.type->kind() == TypeKind::DOUBLE) {
    const auto* flat = input.asUnchecked<FlatVector<double>>();
    encodeSingleVariableFixedFlat(
        column,
        *flat,
        size,
        offsets,
        data,
        [](const auto& values, auto row, auto* output, bool descending) {
          encodeUnsignedWord<uint64_t>(
              encodeDouble(values.rawValues()[row]), output, descending);
        });
    return;
  }

#define BOLT_ENCODE_FIXED_FLAT(kind, cppType, expression)                 \
  case TypeKind::kind: {                                                  \
    const auto* flat = input.asUnchecked<FlatVector<cppType>>();          \
    encodeSingleFixedFlat(                                                \
        column,                                                           \
        *flat,                                                            \
        size,                                                             \
        format,                                                           \
        words,                                                            \
        offsets,                                                          \
        data,                                                             \
        [](const auto& values, auto row, auto* output, bool descending) { \
          expression;                                                     \
        });                                                               \
    return;                                                               \
  }

  switch (column.type->kind()) {
    BOLT_ENCODE_FIXED_FLAT(
        BOOLEAN,
        bool,
        const auto value = static_cast<uint8_t>(values.valueAtFast(row));
        output[0] = static_cast<char>(
            descending ? static_cast<uint8_t>(~value) : value))
    BOLT_ENCODE_FIXED_FLAT(
        TINYINT,
        int8_t,
        encodeSignedWord<int8_t>(values.rawValues()[row], output, descending))
    BOLT_ENCODE_FIXED_FLAT(
        SMALLINT,
        int16_t,
        encodeSignedWord<int16_t>(values.rawValues()[row], output, descending))
    BOLT_ENCODE_FIXED_FLAT(
        INTEGER,
        int32_t,
        encodeSignedWord<int32_t>(values.rawValues()[row], output, descending))
    BOLT_ENCODE_FIXED_FLAT(
        BIGINT,
        int64_t,
        encodeSigned<int64_t>(values.rawValues()[row], output, descending))
    BOLT_ENCODE_FIXED_FLAT(
        REAL,
        float,
        encodeUnsignedWord<uint32_t>(
            encodeFloat(values.rawValues()[row]), output, descending))
    BOLT_ENCODE_FIXED_FLAT(
        DOUBLE,
        double,
        encodeUnsigned<uint64_t>(
            encodeDouble(values.rawValues()[row]), output, descending))
    BOLT_ENCODE_FIXED_FLAT(
        TIMESTAMP, Timestamp, const auto value = values.rawValues()[row];
        encodeSigned<int64_t>(value.getSeconds(), output, descending);
        encodeUnsigned<uint64_t>(
            value.getNanos(), output + sizeof(int64_t), descending))
    default:
      BOLT_FAIL(
          "Single fixed sort key fast path is not implemented for {}",
          column.type->toString());
  }
#undef BOLT_ENCODE_FIXED_FLAT
}

bool canEncodeSingleFixedFlatVector(
    const std::vector<RadixSortKeyColumn>& columns,
    const RowVector& input) {
  return columns.size() == 1 && input.childAt(0) != nullptr &&
      input.childAt(0)->encoding() == VectorEncoding::Simple::FLAT &&
      input.childAt(0)->type()->kind() != TypeKind::UNKNOWN &&
      fixedBodySize(*input.childAt(0)->type()).has_value();
}

template <
    RadixSortKeyLayoutKind KIND,
    bool HasNulls,
    typename T,
    typename EncodeBody>
void appendSingleFixedFlatKernel(
    const RadixSortKeyColumn& column,
    const FlatVector<T>& input,
    vector_size_t size,
    RadixSortRunStorage& arena,
    std::span<char* const> payloads,
    EncodeBody encodeBody) {
  using Traits = RadixSortKeyTraits<KIND>;
  static_assert(!Traits::kVariable);
  static_assert(Traits::kInlineWords > 0);
  static_assert(Traits::kInlineWords <= 3);
  const auto* nulls = input.rawNulls();
  const bool descending = !column.flags.ascending;
  arena.appendKeyBlocks(
      size, [&](vector_size_t source, vector_size_t count, char* destination) {
        auto* record = destination;
        for (vector_size_t row = 0; row < count; ++row) {
          const auto inputRow = source + row;
          std::array<uint64_t, 3> encodedWords{};
          auto* bytes = reinterpret_cast<char*>(encodedWords.data());
          if constexpr (HasNulls) {
            if (bits::isBitNull(nulls, inputRow)) {
              bytes[0] = static_cast<char>(nullMarker(column.flags));
            } else {
              bytes[0] = static_cast<char>(validMarker(column.flags));
              encodeBody(input, inputRow, bytes + 1, descending);
            }
          } else {
            bytes[0] = static_cast<char>(validMarker(column.flags));
            encodeBody(input, inputRow, bytes + 1, descending);
          }
          for (uint32_t word = 0; word < Traits::kInlineWords; ++word) {
            auto value = encodedWords[word];
            if constexpr (std::endian::native == std::endian::little) {
              value = byteSwap(value);
            }
            storeUnaligned<uint64_t>(record + word * sizeof(uint64_t), value);
          }
          if constexpr (Traits::kHasPayload) {
            storeUnaligned<char*>(
                record + Traits::kPayloadOffset, payloads[inputRow]);
          }
          record += Traits::kWidth;
        }
      });
}

template <RadixSortKeyLayoutKind KIND, typename T, typename EncodeBody>
void appendSingleFixedFlatLayout(
    const RadixSortKeyColumn& column,
    const FlatVector<T>& input,
    vector_size_t size,
    RadixSortRunStorage& arena,
    std::span<char* const> payloads,
    EncodeBody encodeBody) {
  using Traits = RadixSortKeyTraits<KIND>;
  if (input.rawNulls() != nullptr) {
    appendSingleFixedFlatKernel<KIND, true>(
        column, input, size, arena, payloads, encodeBody);
    return;
  }
  appendSingleFixedFlatKernel<KIND, false>(
      column, input, size, arena, payloads, encodeBody);
}

template <
    RadixSortKeyLayoutKind NoPayloadKind,
    RadixSortKeyLayoutKind PayloadKind,
    typename T,
    typename EncodeBody>
void appendSingleFixedFlat(
    const RadixSortKeyColumn& column,
    const FlatVector<T>& input,
    vector_size_t size,
    RadixSortRunStorage& arena,
    std::span<char* const> payloads,
    EncodeBody encodeBody) {
  switch (arena.layout().kind()) {
    case NoPayloadKind:
      appendSingleFixedFlatLayout<NoPayloadKind>(
          column, input, size, arena, payloads, encodeBody);
      return;
    case PayloadKind:
      appendSingleFixedFlatLayout<PayloadKind>(
          column, input, size, arena, payloads, encodeBody);
      return;
    default:
      BOLT_FAIL("Direct fixed sort key layout does not match type");
  }
}

template <
    RadixSortKeyLayoutKind KIND,
    bool HasNulls,
    typename T,
    typename EncodeBody>
void appendSingleFixed64FlatKernel(
    const RadixSortKeyColumn& column,
    const FlatVector<T>& input,
    vector_size_t size,
    RadixSortRunStorage& arena,
    std::span<char* const> payloads,
    EncodeBody encodeBody) {
  using Traits = RadixSortKeyTraits<KIND>;
  static_assert(Traits::kInlineWords == 2);
  const auto* values = input.rawValues();
  const auto* nulls = input.rawNulls();
  const auto nullWord = static_cast<uint64_t>(nullMarker(column.flags)) << 56;
  const auto validWord = static_cast<uint64_t>(validMarker(column.flags)) << 56;
  const auto descendingMask = column.flags.ascending
      ? uint64_t{0}
      : std::numeric_limits<uint64_t>::max();
  arena.appendKeyBlocks(
      size, [&](vector_size_t source, vector_size_t count, char* destination) {
        auto* record = destination;
        for (vector_size_t row = 0; row < count; ++row) {
          const auto inputRow = source + row;
          if constexpr (HasNulls) {
            if (bits::isBitNull(nulls, inputRow)) {
              storeUnaligned<uint64_t>(record, nullWord);
              storeUnaligned<uint64_t>(record + sizeof(uint64_t), 0);
            } else {
              const auto encoded =
                  encodeBody(values[inputRow]) ^ descendingMask;
              storeUnaligned<uint64_t>(record, validWord | (encoded >> 8));
              storeUnaligned<uint64_t>(
                  record + sizeof(uint64_t), encoded << 56);
            }
          } else {
            const auto encoded = encodeBody(values[inputRow]) ^ descendingMask;
            storeUnaligned<uint64_t>(record, validWord | (encoded >> 8));
            storeUnaligned<uint64_t>(record + sizeof(uint64_t), encoded << 56);
          }
          if constexpr (Traits::kHasPayload) {
            storeUnaligned<char*>(
                record + Traits::kPayloadOffset, payloads[inputRow]);
          }
          record += Traits::kWidth;
        }
      });
}

template <RadixSortKeyLayoutKind KIND, typename T, typename EncodeBody>
void appendSingleFixed64FlatLayout(
    const RadixSortKeyColumn& column,
    const FlatVector<T>& input,
    vector_size_t size,
    RadixSortRunStorage& arena,
    std::span<char* const> payloads,
    EncodeBody encodeBody) {
  using Traits = RadixSortKeyTraits<KIND>;
  if (input.rawNulls() != nullptr) {
    appendSingleFixed64FlatKernel<KIND, true>(
        column, input, size, arena, payloads, encodeBody);
    return;
  }
  appendSingleFixed64FlatKernel<KIND, false>(
      column, input, size, arena, payloads, encodeBody);
}

template <typename T, typename EncodeBody>
void appendSingleFixed64Flat(
    const RadixSortKeyColumn& column,
    const FlatVector<T>& input,
    vector_size_t size,
    RadixSortRunStorage& arena,
    std::span<char* const> payloads,
    EncodeBody encodeBody) {
  switch (arena.layout().kind()) {
    case RadixSortKeyLayoutKind::kKeyOnlyFixed16:
      appendSingleFixed64FlatLayout<RadixSortKeyLayoutKind::kKeyOnlyFixed16>(
          column, input, size, arena, payloads, encodeBody);
      return;
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed24:
      appendSingleFixed64FlatLayout<
          RadixSortKeyLayoutKind::kKeyWithPayloadFixed24>(
          column, input, size, arena, payloads, encodeBody);
      return;
    default:
      BOLT_FAIL("Direct 64-bit sort key layout does not match type");
  }
}

void appendSingleFixedFlat(
    const RadixSortKeyColumn& column,
    const BaseVector& input,
    vector_size_t size,
    RadixSortRunStorage& arena,
    std::span<char* const> payloads) {
  if (column.type->isShortDecimal()) {
    appendSingleFixed64Flat(
        column,
        *input.asUnchecked<FlatVector<int64_t>>(),
        size,
        arena,
        payloads,
        [](int64_t value) {
          return static_cast<uint64_t>(value) ^ (uint64_t{1} << 63);
        });
    return;
  }
  if (column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    appendSingleFixedFlat<
        RadixSortKeyLayoutKind::kKeyOnlyFixed24,
        RadixSortKeyLayoutKind::kKeyWithPayloadFixed32>(
        column,
        *input.asUnchecked<FlatVector<int128_t>>(),
        size,
        arena,
        payloads,
        [](const auto& values, auto row, auto* output, bool descending) {
          const auto value = values.rawValues()[row];
          encodeSignedWord<int64_t>(
              static_cast<int64_t>(HugeInt::upper(value)), output, descending);
          encodeUnsignedWord<uint64_t>(
              HugeInt::lower(value), output + sizeof(int64_t), descending);
        });
    return;
  }

#define BOLT_APPEND_FIXED_FLAT(                                           \
    kind, cppType, noPayloadKind, payloadKind, expression)                \
  case TypeKind::kind:                                                    \
    appendSingleFixedFlat<                                                \
        RadixSortKeyLayoutKind::noPayloadKind,                            \
        RadixSortKeyLayoutKind::payloadKind>(                             \
        column,                                                           \
        *input.asUnchecked<FlatVector<cppType>>(),                        \
        size,                                                             \
        arena,                                                            \
        payloads,                                                         \
        [](const auto& values, auto row, auto* output, bool descending) { \
          expression;                                                     \
        });                                                               \
    return;

  switch (column.type->kind()) {
    BOLT_APPEND_FIXED_FLAT(
        BOOLEAN,
        bool,
        kKeyOnlyFixed8,
        kKeyWithPayloadFixed16,
        const auto value = static_cast<uint8_t>(values.valueAtFast(row));
        output[0] = static_cast<char>(
            descending ? static_cast<uint8_t>(~value) : value))
    BOLT_APPEND_FIXED_FLAT(
        TINYINT,
        int8_t,
        kKeyOnlyFixed8,
        kKeyWithPayloadFixed16,
        encodeSignedWord<int8_t>(values.rawValues()[row], output, descending))
    BOLT_APPEND_FIXED_FLAT(
        SMALLINT,
        int16_t,
        kKeyOnlyFixed8,
        kKeyWithPayloadFixed16,
        encodeSignedWord<int16_t>(values.rawValues()[row], output, descending))
    BOLT_APPEND_FIXED_FLAT(
        INTEGER,
        int32_t,
        kKeyOnlyFixed8,
        kKeyWithPayloadFixed16,
        encodeSignedWord<int32_t>(values.rawValues()[row], output, descending))
    case TypeKind::BIGINT:
      appendSingleFixed64Flat(
          column,
          *input.asUnchecked<FlatVector<int64_t>>(),
          size,
          arena,
          payloads,
          [](int64_t value) {
            return static_cast<uint64_t>(value) ^ (uint64_t{1} << 63);
          });
      return;
      BOLT_APPEND_FIXED_FLAT(
          REAL,
          float,
          kKeyOnlyFixed8,
          kKeyWithPayloadFixed16,
          encodeUnsignedWord<uint32_t>(
              encodeFloat(values.rawValues()[row]), output, descending))
    case TypeKind::DOUBLE:
      appendSingleFixed64Flat(
          column,
          *input.asUnchecked<FlatVector<double>>(),
          size,
          arena,
          payloads,
          [](double value) { return encodeDouble(value); });
      return;
      BOLT_APPEND_FIXED_FLAT(
          TIMESTAMP,
          Timestamp,
          kKeyOnlyFixed24,
          kKeyWithPayloadFixed32,
          const auto value = values.rawValues()[row];
          encodeSignedWord<int64_t>(value.getSeconds(), output, descending);
          encodeUnsignedWord<uint64_t>(
              value.getNanos(), output + sizeof(int64_t), descending))
    default:
      BOLT_FAIL(
          "Direct fixed sort key encoder is not implemented for {}",
          column.type->toString());
  }
#undef BOLT_APPEND_FIXED_FLAT
}

void stringEncodedSize(
    const BaseVector& vector,
    vector_size_t row,
    uint64_t& size) {
  const auto value = valueAt<StringView>(vector, row);
  uint64_t escaped = 0;
  for (uint32_t index = 0; index < value.size(); ++index) {
    if (static_cast<uint8_t>(value.data()[index]) <= kBlobEscape) {
      ++escaped;
    }
  }
  size = static_cast<uint64_t>(value.size()) + escaped + 1;
}

uint64_t encodedStringBodySize(StringView value);

struct RangeSizeMetadata {
  std::optional<uint64_t> fixedElementSize;
  bool stringElement{false};
  bool mayHaveNulls{true};
};

RangeSizeMetadata rangeSizeMetadata(
    const RadixSortKeyColumn& column,
    const BaseVector& elements) {
  return RangeSizeMetadata{
      isFixedScalarColumn(column) ? fixedBodySize(*column.type) : std::nullopt,
      column.type->kind() == TypeKind::VARCHAR ||
          column.type->kind() == TypeKind::VARBINARY,
      elements.mayHaveNulls()};
}

void encodedSize(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t row,
    uint64_t& size);

void addVariableColumnSizes(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t size,
    uint64_t* rowSizes);

uint64_t encodedRangeSize(
    const RadixSortKeyColumn& column,
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t count,
    const RangeSizeMetadata& metadata) {
  uint64_t size = 0;
  if (metadata.fixedElementSize.has_value()) {
    const auto validSize = *metadata.fixedElementSize + 1;
    if (!metadata.mayHaveNulls) {
      return static_cast<uint64_t>(count) * validSize;
    }
    if (elements.encoding() == VectorEncoding::Simple::FLAT) {
      const auto* nulls = elements.rawNulls();
      if (nulls == nullptr) {
        return static_cast<uint64_t>(count) * validSize;
      }
      for (vector_size_t index = offset; index < offset + count; ++index) {
        size += bits::isBitNull(nulls, index) ? uint64_t{1} : validSize;
      }
      return size;
    }
    DecodedVector decoded(elements);
    if (decoded.isConstantMapping()) {
      const auto elementSize = decoded.isNullAt(0) ? uint64_t{1} : validSize;
      return static_cast<uint64_t>(count) * elementSize;
    }
    for (vector_size_t index = offset; index < offset + count; ++index) {
      size += decoded.isNullAt(index) ? uint64_t{1} : validSize;
    }
    return size;
  }

  if (metadata.stringElement) {
    if (elements.encoding() == VectorEncoding::Simple::FLAT) {
      const auto* flat = elements.asUnchecked<FlatVector<StringView>>();
      const auto* values = flat->rawValues();
      const auto* nulls = flat->rawNulls();
      for (vector_size_t index = offset; index < offset + count; ++index) {
        size += (nulls != nullptr && bits::isBitNull(nulls, index))
            ? uint64_t{1}
            : 1 + encodedStringBodySize(values[index]);
      }
      return size;
    }
    if (elements.encoding() == VectorEncoding::Simple::CONSTANT) {
      const auto* constant = elements.asUnchecked<ConstantVector<StringView>>();
      const auto elementSize = constant->isNullAt(0)
          ? uint64_t{1}
          : 1 + encodedStringBodySize(constant->valueAtFast(0));
      return static_cast<uint64_t>(count) * elementSize;
    }

    DecodedVector decoded(elements);
    if (decoded.isConstantMapping()) {
      const auto elementSize = decoded.isNullAt(0)
          ? uint64_t{1}
          : 1 + encodedStringBodySize(decoded.valueAt<StringView>(0));
      return static_cast<uint64_t>(count) * elementSize;
    }
    for (vector_size_t index = offset; index < offset + count; ++index) {
      size += decoded.isNullAt(index)
          ? uint64_t{1}
          : 1 + encodedStringBodySize(decoded.valueAt<StringView>(index));
    }
    return size;
  }

  for (vector_size_t index = 0; index < count; ++index) {
    uint64_t childSize;
    encodedSize(column, elements, offset + index, childSize);
    size += childSize;
  }
  return size;
}

uint64_t encodedArraySize(
    const RadixSortKeyColumn& column,
    const ArrayVector& array,
    vector_size_t row,
    const RangeSizeMetadata& elementMetadata) {
  return 1 +
      encodedRangeSize(
             column.children[0],
             *array.elements(),
             array.offsetAt(row),
             array.sizeAt(row),
             elementMetadata) +
      1;
}

uint64_t encodedMapSize(
    const RadixSortKeyColumn& column,
    const MapVector& map,
    vector_size_t row,
    const RangeSizeMetadata& keyMetadata,
    const RangeSizeMetadata& valueMetadata) {
  const auto offset = map.offsetAt(row);
  const auto count = map.sizeAt(row);
  const auto keysSize = encodedRangeSize(
      column.children[0], *map.mapKeys(), offset, count, keyMetadata);
  return 1 + keysSize + 1 +
      encodedRangeSize(
             column.children[1],
             *map.mapValues(),
             offset,
             count,
             valueMetadata) +
      1;
}

uint64_t encodedRowSize(
    const RadixSortKeyColumn& column,
    const RowVector& rowVector,
    vector_size_t row) {
  uint64_t size = 1;
  for (uint32_t child = 0; child < column.children.size(); ++child) {
    uint64_t childSize;
    encodedSize(
        column.children[child], *rowVector.childAt(child), row, childSize);
    size += childSize;
  }
  return size;
}

void addRowColumnSizes(
    const RadixSortKeyColumn& column,
    const DecodedVector& decoded,
    vector_size_t size,
    uint64_t* rowSizes) {
  const auto* rowVector = decoded.base()->asUnchecked<RowVector>();
  if (decoded.isIdentityMapping() && !decoded.mayHaveNulls()) {
    for (uint32_t child = 0; child < column.children.size(); ++child) {
      addVariableColumnSizes(
          column.children[child], *rowVector->childAt(child), size, rowSizes);
    }
    return;
  }

  for (uint32_t child = 0; child < column.children.size(); ++child) {
    const auto& childVector = rowVector->childAt(child);
    for (vector_size_t row = 0; row < size; ++row) {
      if (decoded.isNullAt(row)) {
        continue;
      }
      uint64_t childSize;
      encodedSize(
          column.children[child], *childVector, decoded.index(row), childSize);
      rowSizes[row] += childSize;
    }
  }
}

void addArrayColumnSizes(
    const RadixSortKeyColumn& column,
    const DecodedVector& decoded,
    vector_size_t size,
    uint64_t* rowSizes) {
  const auto* array = decoded.base()->asUnchecked<ArrayVector>();
  const auto elementMetadata =
      rangeSizeMetadata(column.children[0], *array->elements());
  for (vector_size_t row = 0; row < size; ++row) {
    if (decoded.isNullAt(row)) {
      continue;
    }
    rowSizes[row] +=
        encodedArraySize(column, *array, decoded.index(row), elementMetadata) -
        1;
  }
}

void addMapColumnSizes(
    const RadixSortKeyColumn& column,
    const DecodedVector& decoded,
    vector_size_t size,
    uint64_t* rowSizes) {
  const auto* map = decoded.base()->asUnchecked<MapVector>();
  const auto keyMetadata =
      rangeSizeMetadata(column.children[0], *map->mapKeys());
  const auto valueMetadata =
      rangeSizeMetadata(column.children[1], *map->mapValues());
  for (vector_size_t row = 0; row < size; ++row) {
    if (decoded.isNullAt(row)) {
      continue;
    }
    rowSizes[row] +=
        encodedMapSize(
            column, *map, decoded.index(row), keyMetadata, valueMetadata) -
        1;
  }
}

void addComplexColumnSizes(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t size,
    uint64_t* rowSizes) {
  DecodedVector decoded(vector);

  for (vector_size_t row = 0; row < size; ++row) {
    rowSizes[row] += 1;
  }
  if (decoded.isConstantMapping() && decoded.isNullAt(0)) {
    return;
  }

  switch (column.type->kind()) {
    case TypeKind::ROW:
      addRowColumnSizes(column, decoded, size, rowSizes);
      return;
    case TypeKind::ARRAY:
      addArrayColumnSizes(column, decoded, size, rowSizes);
      return;
    case TypeKind::MAP:
      addMapColumnSizes(column, decoded, size, rowSizes);
      return;
    default:
      BOLT_UNREACHABLE();
  }
}

void encodedSize(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t row,
    uint64_t& size) {
  if (vector.isNullAt(row)) {
    size = 1;
    return;
  }
  if (column.type->kind() == TypeKind::UNKNOWN) {
    BOLT_FAIL("UNKNOWN sort key values must be null");
  }
  if (column.type->kind() == TypeKind::ROW) {
    const auto* rowVector = vector.wrappedVector()->as<RowVector>();
    size = encodedRowSize(column, *rowVector, vector.wrappedIndex(row));
    return;
  }
  if (column.type->kind() == TypeKind::ARRAY) {
    const auto* arrayVector = vector.wrappedVector()->as<ArrayVector>();
    const auto elementMetadata =
        rangeSizeMetadata(column.children[0], *arrayVector->elements());
    size = encodedArraySize(
        column, *arrayVector, vector.wrappedIndex(row), elementMetadata);
    return;
  }
  if (column.type->kind() == TypeKind::MAP) {
    const auto* mapVector = vector.wrappedVector()->as<MapVector>();
    const auto keyMetadata =
        rangeSizeMetadata(column.children[0], *mapVector->mapKeys());
    const auto valueMetadata =
        rangeSizeMetadata(column.children[1], *mapVector->mapValues());
    size = encodedMapSize(
        column,
        *mapVector,
        vector.wrappedIndex(row),
        keyMetadata,
        valueMetadata);
    return;
  }
  if (column.type->kind() == TypeKind::VARCHAR ||
      column.type->kind() == TypeKind::VARBINARY) {
    uint64_t bodySize;
    stringEncodedSize(vector, row, bodySize);
    size = 1 + bodySize;
    return;
  }
  auto bodySize = fixedBodySize(*column.type);
  size = 1 + *bodySize;
}

uint64_t encodeStringValue(StringView value, char* output, bool descending) {
  if (!descending &&
      std::memchr(value.data(), kStringDelimiter, value.size()) == nullptr &&
      std::memchr(value.data(), kBlobEscape, value.size()) == nullptr) {
    std::memcpy(output, value.data(), value.size());
    output[value.size()] = static_cast<char>(kStringDelimiter);
    return value.size() + 1;
  }

  uint64_t offset = 0;
  for (uint32_t index = 0; index < value.size(); ++index) {
    auto byte = static_cast<uint8_t>(value.data()[index]);
    if (byte <= kBlobEscape) {
      output[offset++] = static_cast<char>(
          descending ? static_cast<uint8_t>(~kBlobEscape) : kBlobEscape);
    }
    output[offset++] =
        static_cast<char>(descending ? static_cast<uint8_t>(~byte) : byte);
  }
  output[offset] = static_cast<char>(
      descending ? static_cast<uint8_t>(~kStringDelimiter) : kStringDelimiter);
  return offset + 1;
}

uint64_t encodedStringBodySize(StringView value) {
  if (std::memchr(value.data(), kStringDelimiter, value.size()) == nullptr &&
      std::memchr(value.data(), kBlobEscape, value.size()) == nullptr) {
    return value.size() + 1;
  }
  uint64_t size = value.size() + 1;
  for (uint32_t index = 0; index < value.size(); ++index) {
    size += static_cast<uint8_t>(value.data()[index]) <= kBlobEscape;
  }
  return size;
}

template <typename T>
FOLLY_ALWAYS_INLINE T
scalarValueAt(const BaseVector& vector, vector_size_t row) {
  if (vector.encoding() == VectorEncoding::Simple::FLAT) {
    const auto* flat = vector.asUnchecked<FlatVector<T>>();
    if constexpr (std::is_same_v<T, bool>) {
      return flat->valueAtFast(row);
    } else {
      return flat->rawValues()[row];
    }
  }
  return valueAt<T>(vector, row);
}

template <typename T, typename EncodeBody>
uint64_t encodeFixedScalarArrayElements(
    const RadixSortKeyColumn& column,
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t count,
    char* output,
    uint64_t /*outputSize*/,
    EncodeBody encodeBody) {
  const auto bodySize = *fixedBodySize(*column.type);
  const bool descending = !column.flags.ascending;
  const auto null = static_cast<char>(nullMarker(column.flags));
  const auto valid = static_cast<char>(validMarker(column.flags));
  uint64_t written = 0;
  const auto writeElement =
      [&](vector_size_t index, auto valueAt, auto isNullAt) {
        if (isNullAt(index)) {
          output[written++] = null;
          return;
        }
        output[written++] = valid;
        encodeBody(valueAt(index), output + written, descending);
        written += bodySize;
      };

  if (elements.encoding() == VectorEncoding::Simple::FLAT) {
    const auto* flat = elements.asUnchecked<FlatVector<T>>();
    const auto* nulls = flat->rawNulls();
    if constexpr (std::is_same_v<T, bool>) {
      for (vector_size_t index = offset; index < offset + count; ++index) {
        writeElement(
            index,
            [&](vector_size_t row) { return flat->valueAtFast(row); },
            [&](vector_size_t row) {
              return nulls != nullptr && bits::isBitNull(nulls, row);
            });
      }
    } else {
      const auto* values = flat->rawValues();
      for (vector_size_t index = offset; index < offset + count; ++index) {
        writeElement(
            index,
            [&](vector_size_t row) { return values[row]; },
            [&](vector_size_t row) {
              return nulls != nullptr && bits::isBitNull(nulls, row);
            });
      }
    }
    return written;
  }

  DecodedVector decoded(elements);
  for (vector_size_t index = offset; index < offset + count; ++index) {
    writeElement(
        index,
        [&](vector_size_t row) { return decoded.valueAt<T>(row); },
        [&](vector_size_t row) { return decoded.isNullAt(row); });
  }
  return written;
}

uint64_t encodeFixedScalarArrayElements(
    const RadixSortKeyColumn& column,
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t count,
    char* output,
    uint64_t outputSize) {
  if (column.type->isShortDecimal()) {
    return encodeFixedScalarArrayElements<int64_t>(
        column,
        elements,
        offset,
        count,
        output,
        outputSize,
        [](auto value, auto* out, bool descending) {
          encodeSigned<int64_t>(value, out, descending);
        });
  }
  if (column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    return encodeFixedScalarArrayElements<int128_t>(
        column,
        elements,
        offset,
        count,
        output,
        outputSize,
        [](auto value, auto* out, bool descending) {
          encodeSigned<int64_t>(
              static_cast<int64_t>(HugeInt::upper(value)), out, descending);
          encodeUnsigned<uint64_t>(
              HugeInt::lower(value), out + sizeof(int64_t), descending);
        });
  }

#define BOLT_ENCODE_ARRAY_FIXED_SCALAR(kind, cppType, expression) \
  case TypeKind::kind:                                            \
    return encodeFixedScalarArrayElements<cppType>(               \
        column,                                                   \
        elements,                                                 \
        offset,                                                   \
        count,                                                    \
        output,                                                   \
        outputSize,                                               \
        [](cppType value, char* out, bool descending) { expression; })

  switch (column.type->kind()) {
    BOLT_ENCODE_ARRAY_FIXED_SCALAR(
        BOOLEAN, bool, const auto byte = static_cast<uint8_t>(value);
        out[0] =
            static_cast<char>(descending ? static_cast<uint8_t>(~byte) : byte));
    BOLT_ENCODE_ARRAY_FIXED_SCALAR(
        TINYINT, int8_t, encodeSigned<int8_t>(value, out, descending));
    BOLT_ENCODE_ARRAY_FIXED_SCALAR(
        SMALLINT, int16_t, encodeSigned<int16_t>(value, out, descending));
    BOLT_ENCODE_ARRAY_FIXED_SCALAR(
        INTEGER, int32_t, encodeSigned<int32_t>(value, out, descending));
    BOLT_ENCODE_ARRAY_FIXED_SCALAR(
        BIGINT, int64_t, encodeSigned<int64_t>(value, out, descending));
    BOLT_ENCODE_ARRAY_FIXED_SCALAR(
        REAL,
        float,
        encodeUnsigned<uint32_t>(encodeFloat(value), out, descending));
    BOLT_ENCODE_ARRAY_FIXED_SCALAR(
        DOUBLE,
        double,
        encodeUnsigned<uint64_t>(encodeDouble(value), out, descending));
    BOLT_ENCODE_ARRAY_FIXED_SCALAR(
        TIMESTAMP,
        Timestamp,
        encodeSigned<int64_t>(value.getSeconds(), out, descending);
        encodeUnsigned<uint64_t>(
            value.getNanos(), out + sizeof(int64_t), descending));
    default:
      BOLT_FAIL(
          "Radix sort fixed array element encoding is not implemented for {}",
          column.type->toString());
  }
#undef BOLT_ENCODE_ARRAY_FIXED_SCALAR
}

void encodeValue(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t row,
    char* output,
    uint64_t /*outputSize*/,
    uint64_t& written) {
  const bool isNull = vector.isNullAt(row);
  output[0] = static_cast<char>(
      isNull ? nullMarker(column.flags) : validMarker(column.flags));
  if (isNull) {
    written = 1;
    return;
  }

  const bool descending = !column.flags.ascending;
  auto* body = output + 1;
  const auto addWritten = [&](uint64_t& offset, uint64_t childWritten) {
    offset += childWritten;
  };
  if (column.type->kind() == TypeKind::ROW) {
    const auto* rowVector = vector.wrappedVector()->as<RowVector>();
    const auto wrappedRow = vector.wrappedIndex(row);
    uint64_t offset = 1;
    for (uint32_t child = 0; child < column.children.size(); ++child) {
      uint64_t childWritten;
      encodeValue(
          column.children[child],
          *rowVector->childAt(child),
          wrappedRow,
          output + offset,
          0,
          childWritten);
      addWritten(offset, childWritten);
    }
    written = offset;
    return;
  } else if (column.type->kind() == TypeKind::ARRAY) {
    const auto* arrayVector = vector.wrappedVector()->as<ArrayVector>();
    const auto wrappedRow = vector.wrappedIndex(row);
    const auto arrayOffset = arrayVector->offsetAt(wrappedRow);
    const auto count = arrayVector->sizeAt(wrappedRow);
    uint64_t offset = 1;
    const auto& child = column.children[0];
    const auto& elements = *arrayVector->elements();
    if (isFixedScalarColumn(child)) {
      const auto childWritten = encodeFixedScalarArrayElements(
          child, elements, arrayOffset, count, output + offset, 0);
      addWritten(offset, childWritten);
    } else {
      for (vector_size_t index = 0; index < count; ++index) {
        uint64_t childWritten;
        encodeValue(
            child,
            elements,
            arrayOffset + index,
            output + offset,
            0,
            childWritten);
        addWritten(offset, childWritten);
      }
    }
    output[offset++] = static_cast<char>(
        descending ? static_cast<uint8_t>(~kStringDelimiter)
                   : kStringDelimiter);
    written = offset;
    return;
  } else if (column.type->kind() == TypeKind::MAP) {
    const auto* mapVector = vector.wrappedVector()->as<MapVector>();
    const auto wrappedRow = vector.wrappedIndex(row);
    const auto indices = mapVector->sortedKeyIndices(wrappedRow);
    uint64_t offset = 1;
    for (const auto index : indices) {
      uint64_t childWritten;
      encodeValue(
          column.children[0],
          *mapVector->mapKeys(),
          index,
          output + offset,
          0,
          childWritten);
      addWritten(offset, childWritten);
    }
    const auto delimiter = static_cast<char>(
        descending ? static_cast<uint8_t>(~kStringDelimiter)
                   : kStringDelimiter);
    output[offset++] = delimiter;
    for (const auto index : indices) {
      uint64_t childWritten;
      encodeValue(
          column.children[1],
          *mapVector->mapValues(),
          index,
          output + offset,
          0,
          childWritten);
      addWritten(offset, childWritten);
    }
    output[offset++] = delimiter;
    written = offset;
    return;
  } else if (column.type->isShortDecimal()) {
    encodeSigned<int64_t>(
        scalarValueAt<int64_t>(vector, row), body, descending);
    written = 1 + sizeof(int64_t);
    return;
  } else if (
      column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    const auto value = scalarValueAt<int128_t>(vector, row);
    encodeSigned<int64_t>(
        static_cast<int64_t>(HugeInt::upper(value)), body, descending);
    encodeUnsigned<uint64_t>(
        HugeInt::lower(value), body + sizeof(int64_t), descending);
    written = 1 + sizeof(int128_t);
    return;
  } else {
    switch (column.type->kind()) {
      case TypeKind::BOOLEAN: {
        auto value = static_cast<uint8_t>(scalarValueAt<bool>(vector, row));
        body[0] = static_cast<char>(
            descending ? static_cast<uint8_t>(~value) : value);
        written = 2;
        return;
      }
      case TypeKind::TINYINT:
        encodeSigned<int8_t>(
            scalarValueAt<int8_t>(vector, row), body, descending);
        written = 2;
        return;
      case TypeKind::SMALLINT:
        encodeSigned<int16_t>(
            scalarValueAt<int16_t>(vector, row), body, descending);
        written = 1 + sizeof(int16_t);
        return;
      case TypeKind::INTEGER:
        encodeSigned<int32_t>(
            scalarValueAt<int32_t>(vector, row), body, descending);
        written = 1 + sizeof(int32_t);
        return;
      case TypeKind::BIGINT:
        encodeSigned<int64_t>(
            scalarValueAt<int64_t>(vector, row), body, descending);
        written = 1 + sizeof(int64_t);
        return;
      case TypeKind::REAL:
        encodeUnsigned<uint32_t>(
            encodeFloat(scalarValueAt<float>(vector, row)), body, descending);
        written = 1 + sizeof(uint32_t);
        return;
      case TypeKind::DOUBLE:
        encodeUnsigned<uint64_t>(
            encodeDouble(scalarValueAt<double>(vector, row)), body, descending);
        written = 1 + sizeof(uint64_t);
        return;
      case TypeKind::TIMESTAMP: {
        const auto value = scalarValueAt<Timestamp>(vector, row);
        encodeSigned<int64_t>(value.getSeconds(), body, descending);
        encodeUnsigned<uint64_t>(
            value.getNanos(), body + sizeof(int64_t), descending);
        written = 1 + sizeof(int64_t) + sizeof(uint64_t);
        return;
      }
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY: {
        const auto value = valueAt<StringView>(vector, row);
        const auto bodySize = encodedStringBodySize(value);
        written = 1 + encodeStringValue(value, body, descending);
        return;
      }
      default:
        BOLT_FAIL(
            "Radix sort key encoding is not implemented for {}",
            column.type->toString());
    }
  }
}

template <typename ValueAt, typename IsNullAt>
void addStringColumnSizes(
    vector_size_t size,
    uint64_t* rowSizes,
    ValueAt valueAt,
    IsNullAt isNullAt) {
  for (vector_size_t row = 0; row < size; ++row) {
    uint64_t columnSize = 1;
    if (!isNullAt(row)) {
      const auto value = valueAt(row);
      columnSize += encodedStringBodySize(value);
    }
    rowSizes[row] += columnSize;
  }
}

void addStringColumnSizes(
    const BaseVector& vector,
    vector_size_t size,
    uint64_t* rowSizes) {
  if (vector.encoding() == VectorEncoding::Simple::FLAT) {
    const auto* flat = vector.asUnchecked<FlatVector<StringView>>();
    const auto* values = flat->rawValues();
    const auto* nulls = flat->rawNulls();
    addStringColumnSizes(
        size,
        rowSizes,
        [&](vector_size_t row) { return values[row]; },
        [&](vector_size_t row) {
          return nulls != nullptr && bits::isBitNull(nulls, row);
        });
    return;
  }
  if (vector.encoding() == VectorEncoding::Simple::CONSTANT) {
    const auto* constant = vector.asUnchecked<ConstantVector<StringView>>();
    const bool isNull = constant->isNullAt(0);
    const auto value = isNull ? StringView() : constant->valueAtFast(0);
    addStringColumnSizes(
        size,
        rowSizes,
        [&](vector_size_t) { return value; },
        [&](vector_size_t) { return isNull; });
    return;
  }

  DecodedVector decoded(vector);
  addStringColumnSizes(
      size,
      rowSizes,
      [&](vector_size_t row) { return decoded.valueAt<StringView>(row); },
      [&](vector_size_t row) { return decoded.isNullAt(row); });
}

void addVariableColumnSizes(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t size,
    uint64_t* rowSizes) {
  if (column.type->kind() == TypeKind::VARCHAR ||
      column.type->kind() == TypeKind::VARBINARY) {
    addStringColumnSizes(vector, size, rowSizes);
    return;
  }
  const auto bodySize = fixedBodySize(*column.type);
  if (bodySize.has_value()) {
    if (column.type->kind() == TypeKind::UNKNOWN) {
      for (vector_size_t row = 0; row < size; ++row) {
        rowSizes[row] += 1;
      }
      return;
    }

    const auto validSize = *bodySize + 1;
    if (vector.encoding() == VectorEncoding::Simple::FLAT) {
      const auto* nulls = vector.rawNulls();
      if (nulls == nullptr) {
        for (vector_size_t row = 0; row < size; ++row) {
          rowSizes[row] += validSize;
        }
        return;
      }
      for (vector_size_t row = 0; row < size; ++row) {
        const auto columnSize =
            bits::isBitNull(nulls, row) ? uint64_t{1} : validSize;
        rowSizes[row] += columnSize;
      }
      return;
    }

    DecodedVector decoded(vector);
    if (decoded.isConstantMapping()) {
      const auto columnSize = decoded.isNullAt(0) ? uint64_t{1} : validSize;
      for (vector_size_t row = 0; row < size; ++row) {
        rowSizes[row] += columnSize;
      }
      return;
    }
    for (vector_size_t row = 0; row < size; ++row) {
      const auto columnSize = decoded.isNullAt(row) ? uint64_t{1} : validSize;
      rowSizes[row] += columnSize;
    }
    return;
  }

  if (column.type->kind() == TypeKind::ROW ||
      column.type->kind() == TypeKind::ARRAY ||
      column.type->kind() == TypeKind::MAP) {
    addComplexColumnSizes(column, vector, size, rowSizes);
    return;
  }

  DecodedVector decoded(vector);
  if (decoded.isConstantMapping()) {
    uint64_t columnSize;
    if (decoded.isNullAt(0)) {
      columnSize = 1;
    } else {
      encodedSize(column, *decoded.base(), decoded.index(0), columnSize);
    }
    for (vector_size_t row = 0; row < size; ++row) {
      rowSizes[row] += columnSize;
    }
    return;
  }
  BOLT_FAIL(
      "Radix sort key size estimation is not implemented for {}",
      column.type->toString());
}

template <
    bool MayHaveNulls,
    typename T,
    typename ValueAt,
    typename IsNullAt,
    typename EncodeBody>
void encodeFixedColumn(
    const RadixSortKeyColumn& column,
    vector_size_t size,
    char* data,
    uint64_t* cursors,
    ValueAt valueAt,
    IsNullAt isNullAt,
    EncodeBody encodeBody) {
  const auto bodySize = *fixedBodySize(*column.type);
  const bool descending = !column.flags.ascending;
  for (vector_size_t row = 0; row < size; ++row) {
    auto* output = data + cursors[row];
    if constexpr (MayHaveNulls) {
      if (isNullAt(row)) {
        output[0] = static_cast<char>(nullMarker(column.flags));
        ++cursors[row];
        continue;
      }
    }
    output[0] = static_cast<char>(validMarker(column.flags));
    encodeBody(valueAt(row), output + 1, descending);
    cursors[row] += bodySize + 1;
  }
}

void encodeFixedColumnAllNulls(
    const RadixSortKeyColumn& column,
    vector_size_t size,
    char* data,
    uint64_t* cursors) {
  const auto null = static_cast<char>(nullMarker(column.flags));
  for (vector_size_t row = 0; row < size; ++row) {
    data[cursors[row]++] = null;
  }
}

template <typename T, typename EncodeBody>
void encodeFixedColumn(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t size,
    char* data,
    uint64_t* cursors,
    EncodeBody encodeBody) {
  if (vector.encoding() == VectorEncoding::Simple::FLAT) {
    const auto* flat = vector.asUnchecked<FlatVector<T>>();
    const auto* nulls = flat->rawNulls();
    if constexpr (std::is_same_v<T, bool>) {
      if (nulls == nullptr) {
        encodeFixedColumn<false, T>(
            column,
            size,
            data,
            cursors,
            [&](vector_size_t row) { return flat->valueAtFast(row); },
            [](vector_size_t) { return false; },
            encodeBody);
        return;
      }
      encodeFixedColumn<true, T>(
          column,
          size,
          data,
          cursors,
          [&](vector_size_t row) { return flat->valueAtFast(row); },
          [&](vector_size_t row) {
            return nulls != nullptr && bits::isBitNull(nulls, row);
          },
          encodeBody);
    } else {
      const auto* values = flat->rawValues();
      if (nulls == nullptr) {
        encodeFixedColumn<false, T>(
            column,
            size,
            data,
            cursors,
            [&](vector_size_t row) { return values[row]; },
            [](vector_size_t) { return false; },
            encodeBody);
        return;
      }
      encodeFixedColumn<true, T>(
          column,
          size,
          data,
          cursors,
          [&](vector_size_t row) { return values[row]; },
          [&](vector_size_t row) {
            return nulls != nullptr && bits::isBitNull(nulls, row);
          },
          encodeBody);
    }
    return;
  }
  if (vector.encoding() == VectorEncoding::Simple::CONSTANT) {
    const auto* constant = vector.asUnchecked<ConstantVector<T>>();
    const bool isNull = constant->isNullAt(0);
    if (isNull) {
      encodeFixedColumnAllNulls(column, size, data, cursors);
    } else {
      const auto value = constant->valueAtFast(0);
      encodeFixedColumn<false, T>(
          column,
          size,
          data,
          cursors,
          [&](vector_size_t) { return value; },
          [](vector_size_t) { return false; },
          encodeBody);
    }
    return;
  }

  DecodedVector decoded(vector);
  encodeFixedColumn<true, T>(
      column,
      size,
      data,
      cursors,
      [&](vector_size_t row) { return decoded.valueAt<T>(row); },
      [&](vector_size_t row) { return decoded.isNullAt(row); },
      encodeBody);
}

void encodeFixedColumn(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t size,
    char* data,
    uint64_t* cursors) {
  if (column.type->kind() == TypeKind::UNKNOWN) {
    DecodedVector decoded(vector);
    for (vector_size_t row = 0; row < size; ++row) {
      data[cursors[row]++] = static_cast<char>(nullMarker(column.flags));
    }
    return;
  }
  if (column.type->isShortDecimal()) {
    encodeFixedColumn<int64_t>(
        column,
        vector,
        size,
        data,
        cursors,
        [](int64_t value, char* output, bool descending) {
          encodeSignedWord<int64_t>(value, output, descending);
        });
    return;
  }
  if (column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    encodeFixedColumn<int128_t>(
        column,
        vector,
        size,
        data,
        cursors,
        [](int128_t value, char* output, bool descending) {
          encodeSignedWord<int64_t>(
              static_cast<int64_t>(HugeInt::upper(value)), output, descending);
          encodeUnsignedWord<uint64_t>(
              HugeInt::lower(value), output + sizeof(int64_t), descending);
        });
    return;
  }

#define BOLT_ENCODE_COLUMN(kind, cppType, expression)                      \
  case TypeKind::kind:                                                     \
    encodeFixedColumn<cppType>(                                            \
        column,                                                            \
        vector,                                                            \
        size,                                                              \
        data,                                                              \
        cursors,                                                           \
        [](cppType value, char* output, bool descending) { expression; }); \
    return;

  switch (column.type->kind()) {
    BOLT_ENCODE_COLUMN(
        BOOLEAN, bool, const auto byte = static_cast<uint8_t>(value);
        output[0] =
            static_cast<char>(descending ? static_cast<uint8_t>(~byte) : byte))
    BOLT_ENCODE_COLUMN(
        TINYINT, int8_t, encodeSignedWord<int8_t>(value, output, descending))
    BOLT_ENCODE_COLUMN(
        SMALLINT, int16_t, encodeSignedWord<int16_t>(value, output, descending))
    BOLT_ENCODE_COLUMN(
        INTEGER, int32_t, encodeSignedWord<int32_t>(value, output, descending))
    BOLT_ENCODE_COLUMN(
        BIGINT, int64_t, encodeSignedWord<int64_t>(value, output, descending))
    BOLT_ENCODE_COLUMN(
        REAL,
        float,
        encodeUnsignedWord<uint32_t>(encodeFloat(value), output, descending))
    BOLT_ENCODE_COLUMN(
        DOUBLE,
        double,
        encodeUnsignedWord<uint64_t>(encodeDouble(value), output, descending))
    BOLT_ENCODE_COLUMN(
        TIMESTAMP,
        Timestamp,
        encodeSignedWord<int64_t>(value.getSeconds(), output, descending);
        encodeUnsignedWord<uint64_t>(
            value.getNanos(), output + sizeof(int64_t), descending))
    default:
      BOLT_FAIL(
          "Sort fixed key column is not implemented for {}",
          column.type->toString());
  }
#undef BOLT_ENCODE_COLUMN
}

template <typename ValueAt, typename IsNullAt>
void encodeStringColumn(
    const RadixSortKeyColumn& column,
    vector_size_t size,
    char* data,
    uint64_t* cursors,
    ValueAt valueAt,
    IsNullAt isNullAt) {
  const bool descending = !column.flags.ascending;
  for (vector_size_t row = 0; row < size; ++row) {
    auto* output = data + cursors[row];
    if (isNullAt(row)) {
      output[0] = static_cast<char>(nullMarker(column.flags));
      ++cursors[row];
      continue;
    }
    output[0] = static_cast<char>(validMarker(column.flags));
    const auto value = valueAt(row);
    cursors[row] += 1 + encodeStringValue(value, output + 1, descending);
  }
}

void encodeStringColumn(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t size,
    char* data,
    uint64_t* cursors) {
  if (vector.encoding() == VectorEncoding::Simple::FLAT) {
    const auto* flat = vector.asUnchecked<FlatVector<StringView>>();
    const auto* values = flat->rawValues();
    const auto* nulls = flat->rawNulls();
    encodeStringColumn(
        column,
        size,
        data,
        cursors,
        [&](vector_size_t row) { return values[row]; },
        [&](vector_size_t row) {
          return nulls != nullptr && bits::isBitNull(nulls, row);
        });
    return;
  }
  if (vector.encoding() == VectorEncoding::Simple::CONSTANT) {
    const auto* constant = vector.asUnchecked<ConstantVector<StringView>>();
    const bool isNull = constant->isNullAt(0);
    const auto value = isNull ? StringView() : constant->valueAtFast(0);
    encodeStringColumn(
        column,
        size,
        data,
        cursors,
        [&](vector_size_t) { return value; },
        [&](vector_size_t) { return isNull; });
    return;
  }

  DecodedVector decoded(vector);
  encodeStringColumn(
      column,
      size,
      data,
      cursors,
      [&](vector_size_t row) { return decoded.valueAt<StringView>(row); },
      [&](vector_size_t row) { return decoded.isNullAt(row); });
}

void encodeVariableColumn(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t size,
    char* data,
    uint64_t dataSize,
    uint64_t* cursors) {
  if (column.type->kind() == TypeKind::VARCHAR ||
      column.type->kind() == TypeKind::VARBINARY) {
    encodeStringColumn(column, vector, size, data, cursors);
    return;
  }
  if (fixedBodySize(*column.type).has_value()) {
    encodeFixedColumn(column, vector, size, data, cursors);
    return;
  }

  DecodedVector decoded(vector);
  for (vector_size_t row = 0; row < size; ++row) {
    if (decoded.isNullAt(row)) {
      data[cursors[row]++] = static_cast<char>(nullMarker(column.flags));
      continue;
    }
    uint64_t written;
    encodeValue(
        column,
        *decoded.base(),
        decoded.index(row),
        data + cursors[row],
        dataSize - cursors[row],
        written);
    cursors[row] += written;
  }
}

class EncodedKeyReader {
 public:
  EncodedKeyReader(const char* data, uint64_t size)
      : data_(data), size_(size) {}

  void readByte(uint8_t& value) {
    value = static_cast<uint8_t>(data_[position_++]);
  }

  void peekByte(uint8_t& value) const {
    value = static_cast<uint8_t>(data_[position_]);
  }

  void readBodyByte(bool descending, uint8_t& value) {
    readByte(value);
    if (descending) {
      value = static_cast<uint8_t>(~value);
    }
  }

  void skip(uint64_t bytes) {
    position_ += bytes;
  }

  uint64_t position() const {
    return position_;
  }

  uint64_t remaining() const {
    return size_ - position_;
  }

  const char* current() const {
    return data_ + position_;
  }

 private:
  const char* data_;
  uint64_t size_;
  uint64_t position_{0};
};

template <typename T>
void decodeUnsigned(EncodedKeyReader& reader, bool descending, T& value) {
  static_assert(std::is_unsigned_v<T>);
  value = 0;
  for (uint32_t byte = 0; byte < sizeof(T); ++byte) {
    uint8_t input;
    reader.readBodyByte(descending, input);
    value = static_cast<T>((value << 8) | input);
  }
}

template <typename T>
void decodeSigned(EncodedKeyReader& reader, bool descending, T& value) {
  static_assert(std::is_signed_v<T>);
  using Unsigned = std::make_unsigned_t<T>;
  Unsigned bits;
  decodeUnsigned(reader, descending, bits);
  bits ^= Unsigned{1} << (sizeof(T) * std::numeric_limits<uint8_t>::digits - 1);
  std::memcpy(&value, &bits, sizeof(value));
}

template <RadixSortKeyLayoutKind KIND, typename T>
T decodePhysicalUnsigned(const char* key, bool descending, uint32_t offset) {
  static_assert(std::is_unsigned_v<T>);
  using Traits = RadixSortKeyTraits<KIND>;
  static_assert(!Traits::kVariable);
  const auto wordIndex = offset / sizeof(uint64_t);
  const auto byteOffset = offset % sizeof(uint64_t);
  const auto first =
      loadUnaligned<uint64_t>(key + wordIndex * sizeof(uint64_t));
  uint64_t encoded;
  if (byteOffset + sizeof(T) <= sizeof(uint64_t)) {
    encoded = first >> ((sizeof(uint64_t) - byteOffset - sizeof(T)) * 8);
  } else {
    const auto second =
        loadUnaligned<uint64_t>(key + (wordIndex + 1) * sizeof(uint64_t));
    const unsigned __int128 words =
        (static_cast<unsigned __int128>(first) << 64) | second;
    encoded = static_cast<uint64_t>(
        words >> ((2 * sizeof(uint64_t) - byteOffset - sizeof(T)) * 8));
  }
  auto value = static_cast<T>(encoded);
  return descending ? static_cast<T>(~value) : value;
}

template <RadixSortKeyLayoutKind KIND, typename T>
T decodePhysicalSigned(const char* key, bool descending, uint32_t offset) {
  static_assert(std::is_signed_v<T>);
  using Unsigned = std::make_unsigned_t<T>;
  auto bits = decodePhysicalUnsigned<KIND, Unsigned>(key, descending, offset);
  bits ^= Unsigned{1} << (sizeof(T) * 8 - 1);
  T value;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

float decodeFloat(uint32_t input) {
  if (input == std::numeric_limits<uint32_t>::max()) {
    return std::numeric_limits<float>::quiet_NaN();
  }
  if (input == std::numeric_limits<uint32_t>::max() - 1) {
    return std::numeric_limits<float>::infinity();
  }
  if (input == 0) {
    return -std::numeric_limits<float>::infinity();
  }
  input =
      (input & (uint32_t{1} << 31)) != 0 ? input ^ (uint32_t{1} << 31) : ~input;
  float result;
  std::memcpy(&result, &input, sizeof(result));
  return result;
}

double decodeDouble(uint64_t input) {
  if (input == std::numeric_limits<uint64_t>::max()) {
    return std::numeric_limits<double>::quiet_NaN();
  }
  if (input == std::numeric_limits<uint64_t>::max() - 1) {
    return std::numeric_limits<double>::infinity();
  }
  if (input == 0) {
    return -std::numeric_limits<double>::infinity();
  }
  input =
      (input & (uint64_t{1} << 63)) != 0 ? input ^ (uint64_t{1} << 63) : ~input;
  double result;
  std::memcpy(&result, &input, sizeof(result));
  return result;
}

template <RadixSortKeyLayoutKind KIND, typename T, typename Decode>
void decodeSinglePhysicalColumn(
    const RadixSortKeyColumn& column,
    const RadixSortRunStorage& arena,
    uint64_t begin,
    vector_size_t count,
    bool mayHaveNulls,
    const VectorPtr& result,
    Decode decode) {
  auto* flat = result->asUnchecked<FlatVector<T>>();
  auto* values = flat->mutableRawValues();
  if (!mayHaveNulls) {
    result->resetNulls();
    vector_size_t outputRow = 0;
    while (outputRow < count) {
      const auto range = arena.keyRangeAt(begin + outputRow, count - outputRow);
      for (vector_size_t row = 0; row < range.count; ++row) {
        const auto* key = range.data +
            static_cast<uint64_t>(row) * RadixSortKeyTraits<KIND>::kWidth;
        values[outputRow + row] = decode(key, !column.flags.ascending);
      }
      outputRow += range.count;
    }
    return;
  }
  const auto null = nullMarker(column.flags);
  vector_size_t outputRow = 0;
  while (outputRow < count) {
    const auto range = arena.keyRangeAt(begin + outputRow, count - outputRow);
    for (vector_size_t row = 0; row < range.count; ++row) {
      const auto* key = range.data +
          static_cast<uint64_t>(row) * RadixSortKeyTraits<KIND>::kWidth;
      const auto marker = RadixSortKeyOps<KIND>::encodedByte(key, 0);
      if (marker == null) {
        flat->setNull(outputRow + row, true);
        continue;
      }
      flat->setNull(outputRow + row, false);
      values[outputRow + row] = decode(key, !column.flags.ascending);
    }
    outputRow += range.count;
  }
}

template <RadixSortKeyLayoutKind KIND, typename T, typename Decode>
void decodeSinglePhysicalColumn(
    const RadixSortKeyColumn& column,
    std::span<const char* const> keys,
    bool mayHaveNulls,
    const VectorPtr& result,
    Decode decode) {
  auto* flat = result->asUnchecked<FlatVector<T>>();
  auto* values = flat->mutableRawValues();
  if (!mayHaveNulls) {
    result->resetNulls();
    for (vector_size_t row = 0; row < keys.size(); ++row) {
      values[row] = decode(keys[row], !column.flags.ascending);
    }
    return;
  }
  const auto null = nullMarker(column.flags);
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    const auto* key = keys[row];
    const auto marker = RadixSortKeyOps<KIND>::encodedByte(key, 0);
    if (marker == null) {
      flat->setNull(row, true);
      continue;
    }
    flat->setNull(row, false);
    values[row] = decode(key, !column.flags.ascending);
  }
}

template <RadixSortKeyLayoutKind KIND>
void decodeSinglePhysicalBooleanColumn(
    const RadixSortKeyColumn& column,
    std::span<const char* const> keys,
    bool mayHaveNulls,
    const VectorPtr& result) {
  auto* flat = result->asUnchecked<FlatVector<bool>>();
  auto* values = flat->template mutableRawValues<uint64_t>();
  if (!mayHaveNulls) {
    result->resetNulls();
    for (vector_size_t row = 0; row < keys.size(); ++row) {
      auto value = RadixSortKeyOps<KIND>::encodedByte(keys[row], 1);
      if (!column.flags.ascending) {
        value = static_cast<uint8_t>(~value);
      }
      bits::setBit(values, row, value != 0);
    }
    return;
  }
  const auto null = nullMarker(column.flags);
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    const auto* key = keys[row];
    const auto marker = RadixSortKeyOps<KIND>::encodedByte(key, 0);
    if (marker == null) {
      flat->setNull(row, true);
      continue;
    }
    flat->setNull(row, false);
    auto value = RadixSortKeyOps<KIND>::encodedByte(key, 1);
    if (!column.flags.ascending) {
      value = static_cast<uint8_t>(~value);
    }
    bits::setBit(values, row, value != 0);
  }
}

template <RadixSortKeyLayoutKind KIND>
void decodeSinglePhysicalBooleanColumn(
    const RadixSortKeyColumn& column,
    const RadixSortRunStorage& arena,
    uint64_t begin,
    vector_size_t count,
    bool mayHaveNulls,
    const VectorPtr& result) {
  auto* flat = result->asUnchecked<FlatVector<bool>>();
  auto* values = flat->template mutableRawValues<uint64_t>();
  if (!mayHaveNulls) {
    result->resetNulls();
    vector_size_t outputRow = 0;
    while (outputRow < count) {
      const auto range = arena.keyRangeAt(begin + outputRow, count - outputRow);
      for (vector_size_t row = 0; row < range.count; ++row) {
        const auto* key = range.data +
            static_cast<uint64_t>(row) * RadixSortKeyTraits<KIND>::kWidth;
        auto value = RadixSortKeyOps<KIND>::encodedByte(key, 1);
        if (!column.flags.ascending) {
          value = static_cast<uint8_t>(~value);
        }
        bits::setBit(values, outputRow + row, value != 0);
      }
      outputRow += range.count;
    }
    return;
  }
  const auto null = nullMarker(column.flags);
  vector_size_t outputRow = 0;
  while (outputRow < count) {
    const auto range = arena.keyRangeAt(begin + outputRow, count - outputRow);
    for (vector_size_t row = 0; row < range.count; ++row) {
      const auto* key = range.data +
          static_cast<uint64_t>(row) * RadixSortKeyTraits<KIND>::kWidth;
      const auto marker = RadixSortKeyOps<KIND>::encodedByte(key, 0);
      if (marker == null) {
        flat->setNull(outputRow + row, true);
        continue;
      }
      flat->setNull(outputRow + row, false);
      auto value = RadixSortKeyOps<KIND>::encodedByte(key, 1);
      if (!column.flags.ascending) {
        value = static_cast<uint8_t>(~value);
      }
      bits::setBit(values, outputRow + row, value != 0);
    }
    outputRow += range.count;
  }
}

template <RadixSortKeyLayoutKind KIND>
void decodeSinglePhysicalColumn(
    const RadixSortKeyColumn& column,
    std::span<const char* const> keys,
    bool mayHaveNulls,
    const VectorPtr& result) {
  if (column.type->isShortDecimal()) {
    decodeSinglePhysicalColumn<KIND, int64_t>(
        column,
        keys,
        mayHaveNulls,
        result,
        [](const char* key, bool descending) {
          return decodePhysicalSigned<KIND, int64_t>(key, descending, 1);
        });
    return;
  }
  if (column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    decodeSinglePhysicalColumn<KIND, int128_t>(
        column,
        keys,
        mayHaveNulls,
        result,
        [](const char* key, bool descending) {
          const auto upper =
              decodePhysicalSigned<KIND, int64_t>(key, descending, 1);
          const auto lower = decodePhysicalUnsigned<KIND, uint64_t>(
              key, descending, 1 + sizeof(int64_t));
          return HugeInt::build(static_cast<uint64_t>(upper), lower);
        });
    return;
  }

  switch (column.type->kind()) {
    case TypeKind::BOOLEAN:
      decodeSinglePhysicalBooleanColumn<KIND>(
          column, keys, mayHaveNulls, result);
      return;
    case TypeKind::TINYINT:
      decodeSinglePhysicalColumn<KIND, int8_t>(
          column, keys, mayHaveNulls, result, [](const char* key, bool desc) {
            return decodePhysicalSigned<KIND, int8_t>(key, desc, 1);
          });
      return;
    case TypeKind::SMALLINT:
      decodeSinglePhysicalColumn<KIND, int16_t>(
          column, keys, mayHaveNulls, result, [](const char* key, bool desc) {
            return decodePhysicalSigned<KIND, int16_t>(key, desc, 1);
          });
      return;
    case TypeKind::INTEGER:
      decodeSinglePhysicalColumn<KIND, int32_t>(
          column, keys, mayHaveNulls, result, [](const char* key, bool desc) {
            return decodePhysicalSigned<KIND, int32_t>(key, desc, 1);
          });
      return;
    case TypeKind::BIGINT:
      decodeSinglePhysicalColumn<KIND, int64_t>(
          column, keys, mayHaveNulls, result, [](const char* key, bool desc) {
            return decodePhysicalSigned<KIND, int64_t>(key, desc, 1);
          });
      return;
    case TypeKind::REAL:
      decodeSinglePhysicalColumn<KIND, float>(
          column, keys, mayHaveNulls, result, [](const char* key, bool desc) {
            return decodeFloat(
                decodePhysicalUnsigned<KIND, uint32_t>(key, desc, 1));
          });
      return;
    case TypeKind::DOUBLE:
      decodeSinglePhysicalColumn<KIND, double>(
          column, keys, mayHaveNulls, result, [](const char* key, bool desc) {
            return decodeDouble(
                decodePhysicalUnsigned<KIND, uint64_t>(key, desc, 1));
          });
      return;
    case TypeKind::TIMESTAMP:
      decodeSinglePhysicalColumn<KIND, Timestamp>(
          column, keys, mayHaveNulls, result, [](const char* key, bool desc) {
            return Timestamp(
                decodePhysicalSigned<KIND, int64_t>(key, desc, 1),
                decodePhysicalUnsigned<KIND, uint64_t>(
                    key, desc, 1 + sizeof(int64_t)));
          });
      return;
    default:
      BOLT_FAIL(
          "Single fixed radix sort key decode is not implemented for {}",
          column.type->toString());
  }
}

template <RadixSortKeyLayoutKind KIND>
void decodeSinglePhysicalColumn(
    const RadixSortKeyColumn& column,
    const RadixSortRunStorage& arena,
    uint64_t begin,
    vector_size_t count,
    bool mayHaveNulls,
    const VectorPtr& result) {
  if (column.type->isShortDecimal()) {
    decodeSinglePhysicalColumn<KIND, int64_t>(
        column,
        arena,
        begin,
        count,
        mayHaveNulls,
        result,
        [](const char* key, bool descending) {
          return decodePhysicalSigned<KIND, int64_t>(key, descending, 1);
        });
    return;
  }
  if (column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    decodeSinglePhysicalColumn<KIND, int128_t>(
        column,
        arena,
        begin,
        count,
        mayHaveNulls,
        result,
        [](const char* key, bool descending) {
          const auto upper =
              decodePhysicalSigned<KIND, int64_t>(key, descending, 1);
          const auto lower = decodePhysicalUnsigned<KIND, uint64_t>(
              key, descending, 1 + sizeof(int64_t));
          return HugeInt::build(static_cast<uint64_t>(upper), lower);
        });
    return;
  }

  switch (column.type->kind()) {
    case TypeKind::BOOLEAN:
      decodeSinglePhysicalBooleanColumn<KIND>(
          column, arena, begin, count, mayHaveNulls, result);
      return;
    case TypeKind::TINYINT:
      decodeSinglePhysicalColumn<KIND, int8_t>(
          column,
          arena,
          begin,
          count,
          mayHaveNulls,
          result,
          [](const char* key, bool desc) {
            return decodePhysicalSigned<KIND, int8_t>(key, desc, 1);
          });
      return;
    case TypeKind::SMALLINT:
      decodeSinglePhysicalColumn<KIND, int16_t>(
          column,
          arena,
          begin,
          count,
          mayHaveNulls,
          result,
          [](const char* key, bool desc) {
            return decodePhysicalSigned<KIND, int16_t>(key, desc, 1);
          });
      return;
    case TypeKind::INTEGER:
      decodeSinglePhysicalColumn<KIND, int32_t>(
          column,
          arena,
          begin,
          count,
          mayHaveNulls,
          result,
          [](const char* key, bool desc) {
            return decodePhysicalSigned<KIND, int32_t>(key, desc, 1);
          });
      return;
    case TypeKind::BIGINT:
      decodeSinglePhysicalColumn<KIND, int64_t>(
          column,
          arena,
          begin,
          count,
          mayHaveNulls,
          result,
          [](const char* key, bool desc) {
            return decodePhysicalSigned<KIND, int64_t>(key, desc, 1);
          });
      return;
    case TypeKind::REAL:
      decodeSinglePhysicalColumn<KIND, float>(
          column,
          arena,
          begin,
          count,
          mayHaveNulls,
          result,
          [](const char* key, bool desc) {
            return decodeFloat(
                decodePhysicalUnsigned<KIND, uint32_t>(key, desc, 1));
          });
      return;
    case TypeKind::DOUBLE:
      decodeSinglePhysicalColumn<KIND, double>(
          column,
          arena,
          begin,
          count,
          mayHaveNulls,
          result,
          [](const char* key, bool desc) {
            return decodeDouble(
                decodePhysicalUnsigned<KIND, uint64_t>(key, desc, 1));
          });
      return;
    case TypeKind::TIMESTAMP:
      decodeSinglePhysicalColumn<KIND, Timestamp>(
          column,
          arena,
          begin,
          count,
          mayHaveNulls,
          result,
          [](const char* key, bool desc) {
            return Timestamp(
                decodePhysicalSigned<KIND, int64_t>(key, desc, 1),
                decodePhysicalUnsigned<KIND, uint64_t>(
                    key, desc, 1 + sizeof(int64_t)));
          });
      return;
    default:
      BOLT_FAIL(
          "Single fixed radix sort key decode is not implemented for {}",
          column.type->toString());
  }
}

template <typename Decode>
void dispatchFixedPhysicalLayout(RadixSortKeyLayoutKind kind, Decode decode) {
  switch (kind) {
    case RadixSortKeyLayoutKind::kKeyOnlyFixed8:
      decode.template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed8>();
      return;
    case RadixSortKeyLayoutKind::kKeyOnlyFixed16:
      decode.template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed16>();
      return;
    case RadixSortKeyLayoutKind::kKeyOnlyFixed24:
      decode.template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed24>();
      return;
    case RadixSortKeyLayoutKind::kKeyOnlyFixed32:
      decode.template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed32>();
      return;
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed16:
      decode.template
      operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed16>();
      return;
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed24:
      decode.template
      operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed24>();
      return;
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed32:
      decode.template
      operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed32>();
      return;
    case RadixSortKeyLayoutKind::kKeyOnlyVariable32:
    case RadixSortKeyLayoutKind::kKeyWithPayloadVariable32:
    case RadixSortKeyLayoutKind::kInvalid:
      BOLT_FAIL("Single fixed sort key decode requires fixed layout");
  }
  BOLT_FAIL("Unknown radix sort key layout");
}

template <typename T>
void setValue(const VectorPtr& vector, vector_size_t row, T value) {
  vector->asUnchecked<FlatVector<T>>()->set(row, value);
}

template <typename T>
void setScalarValue(const VectorPtr& vector, vector_size_t row, T value) {
  if constexpr (std::is_same_v<T, bool>) {
    setValue<bool>(vector, row, value);
  } else {
    auto* flat = vector->asUnchecked<FlatVector<T>>();
    auto* values = flat->mutableRawValues();
    values[row] = value;
    if (flat->rawNulls() != nullptr) {
      flat->setNull(row, false);
    }
  }
}

void decodeString(
    EncodedKeyReader& reader,
    bool descending,
    const VectorPtr& result,
    vector_size_t row) {
  auto scan = reader;
  uint64_t decodedSize = 0;
  while (true) {
    uint8_t byte;
    scan.readBodyByte(descending, byte);
    if (byte == kStringDelimiter) {
      break;
    }
    if (byte == kBlobEscape) {
      scan.readBodyByte(descending, byte);
    }
    ++decodedSize;
  }
  auto* flatResult = result->asUnchecked<FlatVector<StringView>>();
  std::array<char, StringView::kInlineSize> inlineData{};
  char* output = decodedSize <= inlineData.size()
      ? inlineData.data()
      : flatResult->getRawStringBufferWithSpace(decodedSize, true);
  for (uint64_t index = 0; index < decodedSize; ++index) {
    uint8_t byte;
    reader.readBodyByte(descending, byte);
    if (byte == kBlobEscape) {
      reader.readBodyByte(descending, byte);
    }
    output[index] = static_cast<char>(byte);
  }
  uint8_t delimiter;
  reader.readBodyByte(descending, delimiter);
  flatResult->setNoCopy(
      row, StringView(output, static_cast<int32_t>(decodedSize)));
}

template <typename T, typename Decode>
void decodeFixedScalarArrayElements(
    const RadixSortKeyColumn& column,
    EncodedKeyReader& reader,
    const VectorPtr& result,
    vector_size_t start,
    Decode decode) {
  const bool descending = !column.flags.ascending;
  const auto null = nullMarker(column.flags);
  const auto encodedDelimiter =
      descending ? static_cast<uint8_t>(~kStringDelimiter) : kStringDelimiter;
  auto scan = reader;
  vector_size_t count = 0;
  while (true) {
    uint8_t next;
    scan.peekByte(next);
    if (next == encodedDelimiter) {
      break;
    }
    scan.readByte(next);
    if (next != null) {
      scan.skip(*fixedBodySize(*column.type));
    }
    ++count;
  }
  result->resize(start + count);
  for (vector_size_t index = 0; index < count; ++index) {
    const auto row = start + index;
    uint8_t marker;
    reader.readByte(marker);
    if (marker == null) {
      result->setNull(row, true);
      continue;
    }
    setScalarValue<T>(result, row, decode(reader, descending));
  }
  uint8_t delimiter;
  reader.readByte(delimiter);
}

void decodeFixedScalarArrayElements(
    const RadixSortKeyColumn& column,
    EncodedKeyReader& reader,
    const VectorPtr& result,
    vector_size_t start) {
  if (column.type->isShortDecimal()) {
    decodeFixedScalarArrayElements<int64_t>(
        column, reader, result, start, [](auto& input, bool descending) {
          int64_t value;
          decodeSigned(input, descending, value);
          return value;
        });
    return;
  }
  if (column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    decodeFixedScalarArrayElements<int128_t>(
        column, reader, result, start, [](auto& input, bool descending) {
          int64_t upper;
          uint64_t lower;
          decodeSigned(input, descending, upper);
          decodeUnsigned(input, descending, lower);
          return HugeInt::build(static_cast<uint64_t>(upper), lower);
        });
    return;
  }

  switch (column.type->kind()) {
    case TypeKind::BOOLEAN:
      decodeFixedScalarArrayElements<bool>(
          column, reader, result, start, [](auto& input, bool descending) {
            uint8_t value;
            input.readBodyByte(descending, value);
            return value != 0;
          });
      return;
    case TypeKind::TINYINT:
      decodeFixedScalarArrayElements<int8_t>(
          column, reader, result, start, [](auto& input, bool descending) {
            int8_t value;
            decodeSigned(input, descending, value);
            return value;
          });
      return;
    case TypeKind::SMALLINT:
      decodeFixedScalarArrayElements<int16_t>(
          column, reader, result, start, [](auto& input, bool descending) {
            int16_t value;
            decodeSigned(input, descending, value);
            return value;
          });
      return;
    case TypeKind::INTEGER:
      decodeFixedScalarArrayElements<int32_t>(
          column, reader, result, start, [](auto& input, bool descending) {
            int32_t value;
            decodeSigned(input, descending, value);
            return value;
          });
      return;
    case TypeKind::BIGINT:
      decodeFixedScalarArrayElements<int64_t>(
          column, reader, result, start, [](auto& input, bool descending) {
            int64_t value;
            decodeSigned(input, descending, value);
            return value;
          });
      return;
    case TypeKind::REAL:
      decodeFixedScalarArrayElements<float>(
          column, reader, result, start, [](auto& input, bool descending) {
            uint32_t value;
            decodeUnsigned(input, descending, value);
            const auto decoded = decodeFloat(value);
            return decoded;
          });
      return;
    case TypeKind::DOUBLE:
      decodeFixedScalarArrayElements<double>(
          column, reader, result, start, [](auto& input, bool descending) {
            uint64_t value;
            decodeUnsigned(input, descending, value);
            const auto decoded = decodeDouble(value);
            return decoded;
          });
      return;
    case TypeKind::TIMESTAMP:
      decodeFixedScalarArrayElements<Timestamp>(
          column, reader, result, start, [](auto& input, bool descending) {
            int64_t seconds;
            uint64_t nanos;
            decodeSigned(input, descending, seconds);
            decodeUnsigned(input, descending, nanos);
            return Timestamp(seconds, nanos);
          });
      return;
    default:
      BOLT_FAIL(
          "Radix sort fixed array element decoding is not implemented for {}",
          column.type->toString());
  }
}

void decodeValue(
    const RadixSortKeyColumn& column,
    EncodedKeyReader& reader,
    const VectorPtr& result,
    vector_size_t row) {
  uint8_t marker;
  reader.readByte(marker);
  if (marker == nullMarker(column.flags)) {
    result->setNull(row, true);
    if (column.type->kind() == TypeKind::ARRAY) {
      auto* arrayResult = result->as<ArrayVector>();
      arrayResult->setOffsetAndSize(row, arrayResult->elements()->size(), 0);
    } else if (column.type->kind() == TypeKind::MAP) {
      auto* mapResult = result->as<MapVector>();
      mapResult->setOffsetAndSize(row, mapResult->mapKeys()->size(), 0);
    }
    return;
  }

  const bool descending = !column.flags.ascending;
  if (column.type->kind() == TypeKind::ROW) {
    auto* rowResult = result->as<RowVector>();
    result->setNull(row, false);
    for (uint32_t child = 0; child < column.children.size(); ++child) {
      decodeValue(
          column.children[child], reader, rowResult->childAt(child), row);
    }
    return;
  }
  if (column.type->kind() == TypeKind::ARRAY) {
    auto* arrayResult = result->as<ArrayVector>();
    result->setNull(row, false);
    const auto start = arrayResult->elements()->size();
    vector_size_t count = 0;
    const auto encodedDelimiter =
        descending ? static_cast<uint8_t>(~kStringDelimiter) : kStringDelimiter;
    while (true) {
      uint8_t next;
      reader.peekByte(next);
      if (next == encodedDelimiter) {
        reader.readByte(next);
        break;
      }
      if (isFixedScalarColumn(column.children[0])) {
        decodeFixedScalarArrayElements(
            column.children[0], reader, arrayResult->elements(), start);
        count = arrayResult->elements()->size() - start;
        break;
      } else {
        arrayResult->elements()->resize(start + count + 1);
        decodeValue(
            column.children[0], reader, arrayResult->elements(), start + count);
        ++count;
      }
    }
    arrayResult->setOffsetAndSize(row, start, count);
    return;
  }
  if (column.type->kind() == TypeKind::MAP) {
    auto* mapResult = result->as<MapVector>();
    result->setNull(row, false);
    const auto start = mapResult->mapKeys()->size();
    vector_size_t count = 0;
    const auto encodedDelimiter =
        descending ? static_cast<uint8_t>(~kStringDelimiter) : kStringDelimiter;
    while (true) {
      uint8_t next;
      reader.peekByte(next);
      if (next == encodedDelimiter) {
        reader.readByte(next);
        break;
      }
      mapResult->mapKeys()->resize(start + count + 1);
      decodeValue(
          column.children[0], reader, mapResult->mapKeys(), start + count);
      ++count;
    }
    mapResult->mapValues()->resize(start + count);
    for (vector_size_t index = 0; index < count; ++index) {
      decodeValue(
          column.children[1], reader, mapResult->mapValues(), start + index);
    }
    uint8_t delimiter;
    reader.readByte(delimiter);
    mapResult->setOffsetAndSize(row, start, count);
    return;
  }
  if (column.type->isShortDecimal()) {
    int64_t value;
    decodeSigned(reader, descending, value);
    setValue<int64_t>(result, row, value);
    return;
  }
  if (column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    int64_t upper;
    uint64_t lower;
    decodeSigned(reader, descending, upper);
    decodeUnsigned(reader, descending, lower);
    setValue<int128_t>(
        result, row, HugeInt::build(static_cast<uint64_t>(upper), lower));
    return;
  }

  switch (column.type->kind()) {
    case TypeKind::BOOLEAN: {
      uint8_t value;
      reader.readBodyByte(descending, value);
      setValue<bool>(result, row, value != 0);
      return;
    }
    case TypeKind::TINYINT: {
      int8_t value;
      decodeSigned(reader, descending, value);
      setValue<int8_t>(result, row, value);
      return;
    }
    case TypeKind::SMALLINT: {
      int16_t value;
      decodeSigned(reader, descending, value);
      setValue<int16_t>(result, row, value);
      return;
    }
    case TypeKind::INTEGER: {
      int32_t value;
      decodeSigned(reader, descending, value);
      setValue<int32_t>(result, row, value);
      return;
    }
    case TypeKind::BIGINT: {
      int64_t value;
      decodeSigned(reader, descending, value);
      setValue<int64_t>(result, row, value);
      return;
    }
    case TypeKind::REAL: {
      uint32_t value;
      decodeUnsigned(reader, descending, value);
      const auto decoded = decodeFloat(value);
      setValue<float>(result, row, decoded);
      return;
    }
    case TypeKind::DOUBLE: {
      uint64_t value;
      decodeUnsigned(reader, descending, value);
      const auto decoded = decodeDouble(value);
      setValue<double>(result, row, decoded);
      return;
    }
    case TypeKind::TIMESTAMP: {
      int64_t seconds;
      uint64_t nanos;
      decodeSigned(reader, descending, seconds);
      decodeUnsigned(reader, descending, nanos);
      setValue<Timestamp>(result, row, Timestamp(seconds, nanos));
      return;
    }
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      decodeString(reader, descending, result, row);
      return;
    default:
      BOLT_FAIL(
          "Radix sort key decoding is not implemented for {}",
          column.type->toString());
  }
}

void prepareDecodedResult(
    const std::vector<RadixSortKeyColumn>& columns,
    const RowTypePtr& rowType,
    vector_size_t size,
    memory::MemoryPool* pool,
    std::span<const uint8_t> decodedColumns,
    std::span<const uint8_t> mayHaveNulls,
    RowVectorPtr& result) {
  const auto shouldDecode = [&](uint32_t column) {
    return decodedColumns.empty() || decodedColumns[column] != 0 ||
        !fixedBodySize(*columns[column].type).has_value();
  };
  if (result != nullptr && result->pool() == pool &&
      result->type()->equivalent(*rowType)) {
    VectorPtr reusable(std::move(result));
    BaseVector::prepareForReuse(reusable, size);
    result = std::static_pointer_cast<RowVector>(std::move(reusable));
    for (uint32_t column = 0; column < columns.size(); ++column) {
      auto& child = result->children()[column];
      if (!shouldDecode(column)) {
        child.reset();
      } else if (child == nullptr) {
        child = BaseVector::create(columns[column].type, size, pool);
      } else {
        child->resize(size);
      }
      if (shouldDecode(column) && !mayHaveNulls.empty() &&
          mayHaveNulls[column] == 0) {
        child->resetNulls();
      }
    }
    return;
  }

  std::vector<VectorPtr> children(columns.size());
  for (uint32_t column = 0; column < columns.size(); ++column) {
    if (shouldDecode(column)) {
      children[column] = BaseVector::create(columns[column].type, size, pool);
    }
  }
  result = std::make_shared<RowVector>(
      pool, rowType, nullptr, size, std::move(children));
}

struct DecodeScratch {
  vector_size_t size;
  uint64_t* words;
  uint64_t* cursors;

  uint64_t* block(uint32_t index) const {
    return words + static_cast<uint64_t>(size) * (index + 1);
  }
};

void prepareDecodeScratch(
    vector_size_t size,
    memory::MemoryPool* pool,
    uint64_t wordsPerRow,
    BufferPtr& cursorScratch,
    DecodeScratch& scratch) {
  auto wordCount = checkedMultiply<uint64_t>(size, wordsPerRow);
  auto bytes = wordCount.has_value()
      ? checkedMultiply<uint64_t>(*wordCount, sizeof(uint64_t))
      : std::nullopt;
  if (cursorScratch == nullptr || cursorScratch->pool() != pool) {
    cursorScratch = AlignedBuffer::allocate<uint64_t>(*wordCount, pool);
  } else if (cursorScratch->capacity() < *bytes) {
    AlignedBuffer::reallocate<uint64_t>(&cursorScratch, *wordCount);
  } else {
    cursorScratch->setSize(*bytes);
  }
  scratch.size = size;
  scratch.words = cursorScratch->asMutable<uint64_t>();
  scratch.cursors = scratch.words;
}

bool isLayeredFixedColumn(const RadixSortKeyColumn& column) {
  return column.type->kind() != TypeKind::UNKNOWN &&
      fixedBodySize(*column.type).has_value();
}

bool isStringColumn(const RadixSortKeyColumn& column) {
  return column.type->kind() == TypeKind::VARCHAR ||
      column.type->kind() == TypeKind::VARBINARY;
}

bool shouldDecodeColumn(
    const std::vector<RadixSortKeyColumn>& columns,
    std::span<const uint8_t> decodedColumns,
    uint32_t column) {
  return decodedColumns.empty() || decodedColumns[column] != 0 ||
      !fixedBodySize(*columns[column].type).has_value();
}

uint64_t decodeScratchWordsPerRow(
    const std::vector<RadixSortKeyColumn>& columns,
    std::span<const uint8_t> decodedColumns,
    std::span<const uint8_t> mayHaveNulls) {
  uint64_t extraBlocks = 0;
  for (uint32_t column = 0; column < columns.size(); ++column) {
    if (!shouldDecodeColumn(columns, decodedColumns, column)) {
      continue;
    }
    const bool columnMayHaveNulls =
        mayHaveNulls.empty() || mayHaveNulls[column] != 0;
    if (isLayeredFixedColumn(columns[column])) {
      const auto bodyWords = (*fixedBodySize(*columns[column].type) + 7) / 8;
      extraBlocks =
          std::max<uint64_t>(extraBlocks, bodyWords + columnMayHaveNulls);
    } else if (isStringColumn(columns[column])) {
      extraBlocks =
          std::max<uint64_t>(extraBlocks, uint64_t{3} + columnMayHaveNulls);
    }
  }
  return 1 + extraBlocks;
}

FOLLY_ALWAYS_INLINE void readMarker(
    const EncodedKeyView& key,
    uint64_t& cursor,
    uint8_t null,
    bool& isNull) {
  const auto marker = static_cast<uint8_t>(key.bytes[cursor++]);
  isNull = marker == null;
}

FOLLY_ALWAYS_INLINE void compilerMemoryBarrier() {
#if defined(__GNUC__) || defined(__clang__)
  asm volatile("" ::: "memory");
#endif
}

template <bool Descending, typename T>
FOLLY_ALWAYS_INLINE T decodeUnsignedEncoded(T encoded) {
  static_assert(std::is_unsigned_v<T>);
  if constexpr (Descending) {
    encoded = static_cast<T>(~encoded);
  }
  return fromBigEndian(encoded);
}

template <bool Descending, typename T>
FOLLY_ALWAYS_INLINE T decodeSignedEncoded(std::make_unsigned_t<T> encoded) {
  static_assert(std::is_signed_v<T>);
  using Unsigned = std::make_unsigned_t<T>;
  auto bits = decodeUnsignedEncoded<Descending, Unsigned>(encoded);
  bits ^= static_cast<Unsigned>(Unsigned{1} << (sizeof(T) * 8 - 1));
  T value;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

template <typename T>
struct SignedFixedDecoder {
  using Encoded = std::make_unsigned_t<T>;

  template <bool Descending>
  FOLLY_ALWAYS_INLINE static T decode(Encoded encoded) {
    return decodeSignedEncoded<Descending, T>(encoded);
  }
};

struct FloatFixedDecoder {
  using Encoded = uint32_t;

  template <bool Descending>
  FOLLY_ALWAYS_INLINE static float decode(Encoded encoded) {
    return decodeFloat(decodeUnsignedEncoded<Descending, Encoded>(encoded));
  }
};

struct DoubleFixedDecoder {
  using Encoded = uint64_t;

  template <bool Descending>
  FOLLY_ALWAYS_INLINE static double decode(Encoded encoded) {
    return decodeDouble(decodeUnsignedEncoded<Descending, Encoded>(encoded));
  }
};

template <bool FirstColumn, bool MayHaveNulls>
uint64_t prepareFixedBodyPointers(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    uint64_t bodySize,
    const VectorPtr& result,
    DecodeScratch& scratch,
    uint64_t*& validRows,
    uint64_t*& bodyPointers) {
  const auto size = static_cast<vector_size_t>(keys.size());
  auto* cursors = scratch.cursors;
  validRows = MayHaveNulls ? scratch.block(0) : nullptr;
  bodyPointers = scratch.block(MayHaveNulls ? 1 : 0);

  uint64_t validCount = 0;
  if constexpr (!MayHaveNulls) {
    result->resetNulls();
  } else {
    auto* nulls =
        result->rawNulls() == nullptr ? nullptr : result->mutableRawNulls();
    if (nulls != nullptr) {
      bits::clearAllNull(nulls, size);
    }
    const auto null = nullMarker(column.flags);
    for (vector_size_t row = 0; row < size; ++row) {
      auto cursor = FirstColumn ? uint64_t{0} : cursors[row];
      bool isNull;
      readMarker(keys[row], cursor, null, isNull);
      if (isNull) {
        if (nulls == nullptr) {
          nulls = result->mutableRawNulls();
        }
        bits::setNull(nulls, row, true);
        cursors[row] = cursor;
        continue;
      }
      validRows[validCount] = row;
      bodyPointers[validCount] =
          reinterpret_cast<uintptr_t>(keys[row].bytes.data() + cursor);
      cursors[row] = cursor + bodySize;
      ++validCount;
    }
    return validCount;
  }

  for (vector_size_t row = 0; row < size; ++row) {
    auto cursor = (FirstColumn ? uint64_t{0} : cursors[row]) + 1;
    bodyPointers[row] =
        reinterpret_cast<uintptr_t>(keys[row].bytes.data() + cursor);
    cursors[row] = cursor + bodySize;
  }
  return size;
}

template <typename T>
void loadFixedBodyWord(uint64_t count, uint64_t* bodyPointers) {
  static_assert(std::is_unsigned_v<T>);
  for (uint64_t index = 0; index < count; ++index) {
    bodyPointers[index] = static_cast<uint64_t>(
        loadUnaligned<T>(reinterpret_cast<const char*>(bodyPointers[index])));
  }
}

void loadFixedBodyWords(uint64_t count, uint64_t* bodyPointers, uint64_t* low) {
  for (uint64_t index = 0; index < count; ++index) {
    const auto* body = reinterpret_cast<const char*>(bodyPointers[index]);
    bodyPointers[index] = loadUnaligned<uint64_t>(body);
    low[index] = loadUnaligned<uint64_t>(body + sizeof(uint64_t));
  }
}

template <bool MayHaveNulls>
FOLLY_ALWAYS_INLINE vector_size_t
outputRow(uint64_t index, uint64_t* validRows) {
  if constexpr (MayHaveNulls) {
    return static_cast<vector_size_t>(validRows[index]);
  }
  return static_cast<vector_size_t>(index);
}

template <typename Decode>
FOLLY_ALWAYS_INLINE void dispatchDescending(
    const RadixSortKeyColumn& column,
    Decode decode) {
  if (column.flags.ascending) {
    decode.template operator()<false>();
  } else {
    decode.template operator()<true>();
  }
}

template <
    bool FirstColumn,
    bool MayHaveNulls,
    bool Descending,
    typename T,
    typename Decoder>
__attribute__((noinline)) void decodeWordLayered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result) {
  using Encoded = typename Decoder::Encoded;
  auto* flat = result->asUnchecked<FlatVector<T>>();
  auto* values = flat->mutableRawValues();
  uint64_t* validRows;
  uint64_t* encoded;
  const auto count = prepareFixedBodyPointers<FirstColumn, MayHaveNulls>(
      column, keys, sizeof(Encoded), result, scratch, validRows, encoded);
  compilerMemoryBarrier();
  loadFixedBodyWord<Encoded>(count, encoded);
  compilerMemoryBarrier();
  for (uint64_t index = 0; index < count; ++index) {
    values[outputRow<MayHaveNulls>(index, validRows)] =
        Decoder::template decode<Descending>(
            static_cast<Encoded>(encoded[index]));
  }
}

template <bool FirstColumn, bool MayHaveNulls, bool Descending>
__attribute__((noinline)) void decodeBooleanLayered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result) {
  auto* flat = result->asUnchecked<FlatVector<bool>>();
  auto* values = flat->template mutableRawValues<uint64_t>();
  uint64_t* validRows;
  uint64_t* encoded;
  const auto count = prepareFixedBodyPointers<FirstColumn, MayHaveNulls>(
      column, keys, 1, result, scratch, validRows, encoded);
  compilerMemoryBarrier();
  loadFixedBodyWord<uint8_t>(count, encoded);
  compilerMemoryBarrier();
  for (uint64_t index = 0; index < count; ++index) {
    const auto value = decodeUnsignedEncoded<Descending, uint8_t>(
        static_cast<uint8_t>(encoded[index]));
    bits::setBit(values, outputRow<MayHaveNulls>(index, validRows), value != 0);
  }
}

template <bool FirstColumn, bool MayHaveNulls, bool Descending>
__attribute__((noinline)) void decodeInt128Layered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result) {
  auto* flat = result->asUnchecked<FlatVector<int128_t>>();
  auto* values = flat->mutableRawValues();
  uint64_t* validRows;
  uint64_t* upperEncoded;
  const auto count = prepareFixedBodyPointers<FirstColumn, MayHaveNulls>(
      column, keys, sizeof(int128_t), result, scratch, validRows, upperEncoded);
  auto* lowerEncoded = scratch.block(MayHaveNulls ? 2 : 1);
  compilerMemoryBarrier();
  loadFixedBodyWords(count, upperEncoded, lowerEncoded);
  compilerMemoryBarrier();
  for (uint64_t index = 0; index < count; ++index) {
    const auto upper = decodeSignedEncoded<Descending, int64_t>(
        static_cast<uint64_t>(upperEncoded[index]));
    const auto lower = decodeUnsignedEncoded<Descending, uint64_t>(
        static_cast<uint64_t>(lowerEncoded[index]));
    values[outputRow<MayHaveNulls>(index, validRows)] =
        HugeInt::build(static_cast<uint64_t>(upper), lower);
  }
}

template <bool FirstColumn, bool MayHaveNulls, bool Descending>
__attribute__((noinline)) void decodeTimestampLayered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result) {
  auto* flat = result->asUnchecked<FlatVector<Timestamp>>();
  auto* values = flat->mutableRawValues();
  uint64_t* validRows;
  uint64_t* secondsEncoded;
  const auto count = prepareFixedBodyPointers<FirstColumn, MayHaveNulls>(
      column,
      keys,
      sizeof(int64_t) + sizeof(uint64_t),
      result,
      scratch,
      validRows,
      secondsEncoded);
  auto* nanosEncoded = scratch.block(MayHaveNulls ? 2 : 1);
  compilerMemoryBarrier();
  loadFixedBodyWords(count, secondsEncoded, nanosEncoded);
  compilerMemoryBarrier();
  for (uint64_t index = 0; index < count; ++index) {
    values[outputRow<MayHaveNulls>(index, validRows)] = Timestamp(
        decodeSignedEncoded<Descending, int64_t>(
            static_cast<uint64_t>(secondsEncoded[index])),
        decodeUnsignedEncoded<Descending, uint64_t>(
            static_cast<uint64_t>(nanosEncoded[index])));
  }
}

template <bool FirstColumn, bool MayHaveNulls, typename T>
void decodeSignedFixedLayered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result) {
  dispatchDescending(column, [&]<bool Descending>() {
    decodeWordLayered<
        FirstColumn,
        MayHaveNulls,
        Descending,
        T,
        SignedFixedDecoder<T>>(column, keys, scratch, result);
  });
}

template <bool FirstColumn, bool MayHaveNulls>
void decodeBooleanLayered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result) {
  dispatchDescending(column, [&]<bool Descending>() {
    decodeBooleanLayered<FirstColumn, MayHaveNulls, Descending>(
        column, keys, scratch, result);
  });
}

template <bool FirstColumn, bool MayHaveNulls>
void decodeFloatLayered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result) {
  dispatchDescending(column, [&]<bool Descending>() {
    decodeWordLayered<
        FirstColumn,
        MayHaveNulls,
        Descending,
        float,
        FloatFixedDecoder>(column, keys, scratch, result);
  });
}

template <bool FirstColumn, bool MayHaveNulls>
void decodeDoubleLayered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result) {
  dispatchDescending(column, [&]<bool Descending>() {
    decodeWordLayered<
        FirstColumn,
        MayHaveNulls,
        Descending,
        double,
        DoubleFixedDecoder>(column, keys, scratch, result);
  });
}

template <bool FirstColumn, bool MayHaveNulls>
void decodeInt128Layered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result) {
  dispatchDescending(column, [&]<bool Descending>() {
    decodeInt128Layered<FirstColumn, MayHaveNulls, Descending>(
        column, keys, scratch, result);
  });
}

template <bool FirstColumn, bool MayHaveNulls>
void decodeTimestampLayered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result) {
  dispatchDescending(column, [&]<bool Descending>() {
    decodeTimestampLayered<FirstColumn, MayHaveNulls, Descending>(
        column, keys, scratch, result);
  });
}

template <bool FirstColumn>
void skipFixedColumn(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    uint64_t* cursors) {
  const auto bodySize = *fixedBodySize(*column.type);
  const auto null = nullMarker(column.flags);
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    auto cursor = FirstColumn ? uint64_t{0} : cursors[row];
    const auto marker = static_cast<uint8_t>(keys[row].bytes[cursor++]);
    cursors[row] = cursor + static_cast<uint64_t>(marker != null) * bodySize;
  }
}

void scanStringBody(
    const char* body,
    uint64_t remaining,
    bool descending,
    uint64_t& decodedSize,
    uint64_t& encodedSize) {
  decodedSize = 0;
  uint64_t cursor = 0;
  const auto encodedDelimiter =
      descending ? static_cast<uint8_t>(~kStringDelimiter) : kStringDelimiter;
  const auto encodedEscape =
      descending ? static_cast<uint8_t>(~kBlobEscape) : kBlobEscape;
  const auto* data = reinterpret_cast<const uint8_t*>(body);
  constexpr uint64_t kMemchrThreshold = 32;
  if (remaining >= kMemchrThreshold) {
    const auto* delimiter = static_cast<const uint8_t*>(
        std::memchr(data, encodedDelimiter, remaining));
    if (delimiter != nullptr) {
      const auto bytesBeforeDelimiter = static_cast<uint64_t>(delimiter - data);
      if (std::memchr(data, encodedEscape, bytesBeforeDelimiter) == nullptr) {
        decodedSize = bytesBeforeDelimiter;
        encodedSize = bytesBeforeDelimiter + 1;
        return;
      }
    }
  }

  while (cursor < remaining) {
    auto byte = static_cast<uint8_t>(body[cursor++]);
    if (descending) {
      byte = static_cast<uint8_t>(~byte);
    }
    if (byte == kStringDelimiter) {
      encodedSize = cursor;
      return;
    }
    if (byte == kBlobEscape) {
      byte = static_cast<uint8_t>(body[cursor++]);
      if (descending) {
        byte = static_cast<uint8_t>(~byte);
      }
    }
    ++decodedSize;
  }
  BOLT_FAIL("Radix sort key input is truncated");
}

template <bool Descending>
void writeDecodedString(
    const char* body,
    uint64_t encodedSize,
    uint64_t decodedSize,
    char* destination) {
  if (encodedSize == decodedSize + 1) {
    if constexpr (!Descending) {
      std::memcpy(destination, body, decodedSize);
      return;
    }
  }
  uint64_t cursor = 0;
  uint64_t written = 0;
  while (written < decodedSize) {
    auto byte = static_cast<uint8_t>(body[cursor++]);
    if constexpr (Descending) {
      byte = static_cast<uint8_t>(~byte);
    }
    if (byte == kBlobEscape) {
      byte = static_cast<uint8_t>(body[cursor++]);
      if constexpr (Descending) {
        byte = static_cast<uint8_t>(~byte);
      }
    }
    destination[written++] = static_cast<char>(byte);
  }
}

template <bool FirstColumn, bool MayHaveNulls>
uint64_t prepareStringBodies(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    const VectorPtr& result,
    DecodeScratch& scratch,
    uint64_t*& validRows,
    uint64_t*& bodyPointers,
    uint64_t*& remainingSizes) {
  const auto size = static_cast<vector_size_t>(keys.size());
  auto* cursors = scratch.cursors;
  validRows = MayHaveNulls ? scratch.block(0) : nullptr;
  bodyPointers = scratch.block(MayHaveNulls ? 1 : 0);
  remainingSizes = scratch.block(MayHaveNulls ? 2 : 1);
  uint64_t validCount = 0;
  if constexpr (!MayHaveNulls) {
    result->resetNulls();
  } else {
    auto* nulls =
        result->rawNulls() == nullptr ? nullptr : result->mutableRawNulls();
    if (nulls != nullptr) {
      bits::clearAllNull(nulls, size);
    }
    const auto null = nullMarker(column.flags);
    for (vector_size_t row = 0; row < size; ++row) {
      auto cursor = FirstColumn ? uint64_t{0} : cursors[row];
      bool isNull;
      readMarker(keys[row], cursor, null, isNull);
      if (isNull) {
        if (nulls == nullptr) {
          nulls = result->mutableRawNulls();
        }
        bits::setNull(nulls, row, true);
        cursors[row] = cursor;
        continue;
      }
      validRows[validCount] = row;
      bodyPointers[validCount] =
          reinterpret_cast<uintptr_t>(keys[row].bytes.data() + cursor);
      remainingSizes[validCount] = keys[row].bytes.size() - cursor;
      ++validCount;
    }
    return validCount;
  }

  for (vector_size_t row = 0; row < size; ++row) {
    auto cursor = (FirstColumn ? uint64_t{0} : cursors[row]) + 1;
    bodyPointers[row] =
        reinterpret_cast<uintptr_t>(keys[row].bytes.data() + cursor);
    remainingSizes[row] = keys[row].bytes.size() - cursor;
  }
  return size;
}

template <bool FirstColumn, bool MayHaveNulls, bool Descending>
__attribute__((noinline)) void decodeStringColumnLayered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result) {
  auto* flat = result->asUnchecked<FlatVector<StringView>>();
  auto* values = flat->mutableRawValues();
  uint64_t* validRows;
  uint64_t* bodyPointers;
  uint64_t* decodedSizes;
  const auto count = prepareStringBodies<FirstColumn, MayHaveNulls>(
      column, keys, result, scratch, validRows, bodyPointers, decodedSizes);
  auto* encodedSizes = scratch.block(MayHaveNulls ? 3 : 2);
  compilerMemoryBarrier();

  uint64_t stringBytes = 0;
  for (uint64_t index = 0; index < count; ++index) {
    uint64_t decodedSize;
    uint64_t encodedSize;
    scanStringBody(
        reinterpret_cast<const char*>(bodyPointers[index]),
        decodedSizes[index],
        Descending,
        decodedSize,
        encodedSize);
    decodedSizes[index] = decodedSize;
    encodedSizes[index] = encodedSize;
    if (!StringView::isInline(decodedSize)) {
      stringBytes += decodedSize;
    }
  }
  compilerMemoryBarrier();

  char* output = stringBytes == 0
      ? nullptr
      : flat->getRawStringBufferWithSpace(stringBytes, true);
  for (uint64_t index = 0; index < count; ++index) {
    const auto row = outputRow<MayHaveNulls>(index, validRows);
    const auto* body = reinterpret_cast<const char*>(bodyPointers[index]);
    const auto decodedSize = decodedSizes[index];
    const auto encodedSize = encodedSizes[index];
    std::array<char, StringView::kInlineSize> inlineData;
    auto* destination =
        StringView::isInline(decodedSize) ? inlineData.data() : output;
    writeDecodedString<Descending>(body, encodedSize, decodedSize, destination);
    values[row] = StringView(destination, static_cast<int32_t>(decodedSize));
    scratch.cursors[row] =
        static_cast<uint64_t>(body - keys[row].bytes.data()) + encodedSize;
    if (!StringView::isInline(decodedSize)) {
      output += decodedSize;
    }
  }
}

template <bool FirstColumn, bool MayHaveNulls>
void decodeStringColumnLayered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result) {
  dispatchDescending(column, [&]<bool Descending>() {
    decodeStringColumnLayered<FirstColumn, MayHaveNulls, Descending>(
        column, keys, scratch, result);
  });
}

template <bool FirstColumn, bool MayHaveNulls>
void decodeColumn(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    uint64_t* cursors,
    const VectorPtr& result,
    DecodeScratch& scratch) {
  if (column.type->isShortDecimal()) {
    decodeSignedFixedLayered<FirstColumn, MayHaveNulls, int64_t>(
        column, keys, scratch, result);
    return;
  }
  if (column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    decodeInt128Layered<FirstColumn, MayHaveNulls>(
        column, keys, scratch, result);
    return;
  }

  switch (column.type->kind()) {
    case TypeKind::BOOLEAN:
      decodeBooleanLayered<FirstColumn, MayHaveNulls>(
          column, keys, scratch, result);
      return;
#define BOLT_DECODE_SIGNED_COLUMN(kind, cppType)                  \
  case TypeKind::kind:                                            \
    decodeSignedFixedLayered<FirstColumn, MayHaveNulls, cppType>( \
        column, keys, scratch, result);                           \
    return
      BOLT_DECODE_SIGNED_COLUMN(TINYINT, int8_t);
      BOLT_DECODE_SIGNED_COLUMN(SMALLINT, int16_t);
      BOLT_DECODE_SIGNED_COLUMN(INTEGER, int32_t);
    case TypeKind::BIGINT:
      decodeSignedFixedLayered<FirstColumn, MayHaveNulls, int64_t>(
          column, keys, scratch, result);
      return;
#undef BOLT_DECODE_SIGNED_COLUMN
    case TypeKind::REAL:
      decodeFloatLayered<FirstColumn, MayHaveNulls>(
          column, keys, scratch, result);
      return;
    case TypeKind::DOUBLE:
      decodeDoubleLayered<FirstColumn, MayHaveNulls>(
          column, keys, scratch, result);
      return;
    case TypeKind::TIMESTAMP:
      decodeTimestampLayered<FirstColumn, MayHaveNulls>(
          column, keys, scratch, result);
      return;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      decodeStringColumnLayered<FirstColumn, MayHaveNulls>(
          column, keys, scratch, result);
      return;
    case TypeKind::UNKNOWN: {
      const auto null = nullMarker(column.flags);
      for (vector_size_t row = 0; row < keys.size(); ++row) {
        auto cursor = FirstColumn ? uint64_t{0} : cursors[row];
        bool isNull;
        readMarker(keys[row], cursor, null, isNull);
        cursors[row] = cursor;
        result->setNull(row, true);
      }
      return;
    }
    default:
      for (vector_size_t row = 0; row < keys.size(); ++row) {
        auto cursor = FirstColumn ? uint64_t{0} : cursors[row];
        EncodedKeyReader reader(
            keys[row].bytes.data() + cursor, keys[row].bytes.size() - cursor);
        decodeValue(column, reader, result, row);
        cursors[row] = cursor + reader.position();
      }
      return;
  }
}

void decodeColumns(
    const std::vector<RadixSortKeyColumn>& columns,
    const RowTypePtr& rowType,
    std::span<const EncodedKeyView> keys,
    std::span<const uint8_t> decodedColumns,
    std::span<const uint8_t> mayHaveNulls,
    memory::MemoryPool* pool,
    BufferPtr& cursorScratch,
    RowVectorPtr& result) {
  prepareDecodedResult(
      columns,
      rowType,
      static_cast<vector_size_t>(keys.size()),
      pool,
      decodedColumns,
      mayHaveNulls,
      result);
  DecodeScratch scratch;
  prepareDecodeScratch(
      static_cast<vector_size_t>(keys.size()),
      pool,
      decodeScratchWordsPerRow(columns, decodedColumns, mayHaveNulls),
      cursorScratch,
      scratch);
  auto* cursors = scratch.cursors;
  const auto decodeFirstColumn = [&](uint32_t column) {
    if (mayHaveNulls.empty() || mayHaveNulls[column] != 0) {
      decodeColumn<true, true>(
          columns[column], keys, cursors, result->childAt(column), scratch);
    } else {
      decodeColumn<true, false>(
          columns[column], keys, cursors, result->childAt(column), scratch);
    }
  };
  const auto decodeNextColumn = [&](uint32_t column) {
    if (mayHaveNulls.empty() || mayHaveNulls[column] != 0) {
      decodeColumn<false, true>(
          columns[column], keys, cursors, result->childAt(column), scratch);
    } else {
      decodeColumn<false, false>(
          columns[column], keys, cursors, result->childAt(column), scratch);
    }
  };

  if (!decodedColumns.empty() && decodedColumns[0] == 0 &&
      fixedBodySize(*columns[0].type).has_value()) {
    skipFixedColumn<true>(columns[0], keys, cursors);
  } else {
    decodeFirstColumn(0);
  }
  for (uint32_t column = 1; column < columns.size(); ++column) {
    if (!decodedColumns.empty() && decodedColumns[column] == 0 &&
        fixedBodySize(*columns[column].type).has_value()) {
      skipFixedColumn<false>(columns[column], keys, cursors);
    } else {
      decodeNextColumn(column);
    }
  }
}

template <typename CanSkipColumn>
std::vector<uint32_t> makeLeadingSkippableValidityOffsets(
    const std::vector<RadixSortKeyColumn>& columns,
    CanSkipColumn canSkipColumn) {
  std::vector<uint32_t> offsets;
  uint64_t encodedOffset = 0;
  for (uint32_t column = 0; column < columns.size(); ++column) {
    const auto& metadata = columns[column];
    if (!canSkipColumn(column) || !metadata.maximumEncodedSize.has_value() ||
        encodedOffset >= sizeof(uint64_t) - 1) {
      break;
    }
    offsets.push_back(static_cast<uint32_t>(encodedOffset));
    encodedOffset += *metadata.maximumEncodedSize;
  }
  return offsets;
}

} // namespace

uint64_t EncodedKeyBatch::fixedKeyAt(vector_size_t row) const {
  return fixedKeys_->as<uint64_t>()[row];
}

std::string_view EncodedKeyBatch::variableKeyAt(vector_size_t row) const {
  const auto* offsets = offsets_->as<uint64_t>();
  const auto begin = offsets[row];
  const auto end = offsets[row + 1];
  return std::string_view(
      data_ == nullptr ? nullptr : data_->as<char>() + begin, end - begin);
}

bool RadixSortKeyCodec::supportsEncodeDecode(const Type& type) {
  return supportsType(type);
}

void RadixSortKeyCodec::bind(
    const std::vector<TypePtr>& types,
    const std::vector<CompareFlags>& flags,
    std::unique_ptr<RadixSortKeyCodec>& codec) {
  codec.reset();
  BOLT_CHECK(
      !types.empty(), "Radix sort key codec requires at least one column");
  BOLT_CHECK_EQ(
      types.size(),
      flags.size(),
      "Radix sort key type and flag counts do not match");

  std::vector<RadixSortKeyColumn> columns;
  columns.reserve(types.size());
  bool canEncodeDecode = true;
  std::optional<uint64_t> maximumEncodedSize = 0;
  bool maximumEncodedSizeValid = true;
  for (uint32_t column = 0; column < types.size(); ++column) {
    RadixSortKeyColumn metadata;
    buildMetadata(types[column], flags[column], metadata);
    canEncodeDecode &= metadata.encodeDecodeSupported;
    if (maximumEncodedSize.has_value() &&
        metadata.maximumEncodedSize.has_value()) {
      auto total =
          checkedAdd(*maximumEncodedSize, *metadata.maximumEncodedSize);
      maximumEncodedSizeValid &= total.has_value();
      maximumEncodedSize = total;
    } else {
      maximumEncodedSize = std::nullopt;
    }
    columns.push_back(std::move(metadata));
  }
  BOLT_CHECK(
      maximumEncodedSizeValid, "Radix sort key maximum encoded size overflows");

  const auto format =
      maximumEncodedSize.has_value() && *maximumEncodedSize <= sizeof(uint64_t)
      ? EncodedKeyFormat::kFixed64
      : EncodedKeyFormat::kVariableBinary;
  codec = std::unique_ptr<RadixSortKeyCodec>(new RadixSortKeyCodec(
      std::move(columns), format, maximumEncodedSize, canEncodeDecode));
}

RadixSortKeyCodec::RadixSortKeyCodec(
    std::vector<RadixSortKeyColumn> columns,
    EncodedKeyFormat format,
    std::optional<uint64_t> maximumEncodedSize,
    bool canEncodeDecode)
    : columns_(std::move(columns)),
      rowType_([&]() {
        std::vector<TypePtr> types;
        types.reserve(columns_.size());
        for (const auto& column : columns_) {
          types.push_back(column.type);
        }
        return ROW(std::move(types));
      }()),
      format_(format),
      maximumEncodedSize_(maximumEncodedSize),
      canEncodeDecode_(canEncodeDecode) {}

std::vector<uint32_t> RadixSortKeyCodec::leadingSkippableValidityOffsets(
    std::span<const uint8_t> keyMayHaveNulls) const {
  return makeLeadingSkippableValidityOffsets(
      columns_, [&](uint32_t column) { return keyMayHaveNulls[column] == 0; });
}

void RadixSortKeyCodec::encodeSingleFixedFlat(
    const RowVector& input,
    memory::MemoryPool* pool,
    EncodedKeyBatch& result) const {
  result.format_ = format_;
  result.size_ = input.size();

  uint64_t* words = nullptr;
  const uint64_t* offsets = nullptr;
  char* data = nullptr;
  if (format_ == EncodedKeyFormat::kFixed64) {
    words =
        prepareReusableBuffer<uint64_t>(result.fixedKeys_, input.size(), pool);
    std::fill(words, words + input.size(), uint64_t{0});
  } else {
    auto* mutableOffsets = prepareReusableBuffer<uint64_t>(
        result.offsets_, input.size() + 1, pool);
    mutableOffsets[0] = 0;
    const auto* nulls = input.childAt(0)->rawNulls();
    const auto bodySize = *fixedBodySize(*columns_[0].type);
    for (vector_size_t row = 0; row < input.size(); ++row) {
      mutableOffsets[row + 1] = mutableOffsets[row] +
          ((nulls != nullptr && bits::isBitNull(nulls, row)) ? 1
                                                             : bodySize + 1);
    }
    offsets = mutableOffsets;
    if (mutableOffsets[input.size()] > 0) {
      result.data_ =
          AlignedBuffer::allocate<char>(mutableOffsets[input.size()], pool);
      data = result.data_->asMutable<char>();
    }
  }

  radixsort::encodeSingleFixedFlat(
      columns_[0],
      *input.childAt(0),
      input.size(),
      format_,
      words,
      offsets,
      data);
}

void RadixSortKeyCodec::encode(
    const RowVector& input,
    memory::MemoryPool* pool,
    EncodedKeyBatch& result) const {
  BOLT_CHECK_NOT_NULL(pool, "Radix sort key memory pool must not be null");

  result.format_ = format_;
  result.size_ = input.size();
  result.data_.reset();
  if (format_ == EncodedKeyFormat::kFixed64) {
    result.offsets_.reset();
  } else {
    result.fixedKeys_.reset();
  }

  if (canEncodeSingleFixedFlatVector(columns_, input)) {
    encodeSingleFixedFlat(input, pool, result);
    return;
  }

  if (format_ == EncodedKeyFormat::kFixed64) {
    auto* words =
        prepareReusableBuffer<uint64_t>(result.fixedKeys_, input.size(), pool);
    std::fill(words, words + input.size(), uint64_t{0});
    encodeCursorScratch_.resize(input.size());
    auto* cursors = encodeCursorScratch_.data();
    for (vector_size_t row = 0; row < input.size(); ++row) {
      cursors[row] = static_cast<uint64_t>(row) * sizeof(uint64_t);
    }
    auto* data = reinterpret_cast<char*>(words);
    const auto dataSize =
        static_cast<uint64_t>(input.size()) * sizeof(uint64_t);
    for (uint32_t column = 0; column < columns_.size(); ++column) {
      encodeVariableColumn(
          columns_[column],
          *input.childAt(column),
          input.size(),
          data,
          dataSize,
          cursors);
    }
    for (vector_size_t row = 0; row < input.size(); ++row) {
      const auto rowEnd = static_cast<uint64_t>(row + 1) * sizeof(uint64_t);
      auto word = words[row];
      if constexpr (std::endian::native == std::endian::little) {
        word = byteSwap(word);
      }
      words[row] = word;
    }
    return;
  }

  auto offsetCount = static_cast<uint64_t>(input.size()) + 1;
  auto* offsets = prepareReusableBuffer<uint64_t>(
      result.offsets_, static_cast<size_t>(offsetCount), pool);
  uint64_t flatFixedSize = 0;
  for (uint32_t column = 0; column < columns_.size(); ++column) {
    const auto bodySize = fixedBodySize(*columns_[column].type);
    if (bodySize.has_value() &&
        columns_[column].type->kind() != TypeKind::UNKNOWN &&
        input.childAt(column)->encoding() == VectorEncoding::Simple::FLAT) {
      flatFixedSize += *bodySize + 1;
    }
  }
  std::fill(offsets, offsets + input.size(), flatFixedSize);
  for (uint32_t column = 0; column < columns_.size(); ++column) {
    const auto& vector = *input.childAt(column);
    const auto bodySize = fixedBodySize(*columns_[column].type);
    if (bodySize.has_value() &&
        columns_[column].type->kind() != TypeKind::UNKNOWN &&
        vector.encoding() == VectorEncoding::Simple::FLAT) {
      const auto* nulls = vector.rawNulls();
      if (nulls != nullptr) {
        bits::forEachUnsetBit(nulls, 0, input.size(), [&](vector_size_t row) {
          offsets[row] -= *bodySize;
        });
      }
      continue;
    }
    addVariableColumnSizes(columns_[column], vector, input.size(), offsets);
  }

  uint64_t totalBytes = 0;
  for (vector_size_t row = 0; row < input.size(); ++row) {
    const auto rowSize = offsets[row];
    offsets[row] = totalBytes;
    totalBytes += rowSize;
  }
  offsets[input.size()] = totalBytes;

  if (totalBytes > 0) {
    result.data_ = AlignedBuffer::allocate<char>(totalBytes, pool);
  }
  auto* data =
      result.data_ == nullptr ? nullptr : result.data_->asMutable<char>();
  for (uint32_t column = 0; column < columns_.size(); ++column) {
    encodeVariableColumn(
        columns_[column],
        *input.childAt(column),
        input.size(),
        data,
        totalBytes,
        offsets);
  }

  for (vector_size_t row = input.size(); row > 0; --row) {
    offsets[row] = offsets[row - 1];
  }
  offsets[0] = 0;
}

bool RadixSortKeyCodec::canAppendSingleFixedFlat(
    const BaseVector& input,
    const RadixSortRunStorage& arena) const {
  return columns_.size() == 1 &&
      input.encoding() == VectorEncoding::Simple::FLAT &&
      input.type()->equivalent(*columns_[0].type) &&
      input.type()->kind() != TypeKind::UNKNOWN &&
      fixedBodySize(*input.type()).has_value() &&
      maximumEncodedSize_.has_value() &&
      *maximumEncodedSize_ <= arena.layout().inlineCapacity() &&
      !arena.layout().isVariable();
}

bool RadixSortKeyCodec::tryAppendSingleFixedFlat(
    const BaseVector& input,
    vector_size_t size,
    RadixSortRunStorage& arena,
    std::span<char* const> payloads) const {
  if (!canAppendSingleFixedFlat(input, arena)) {
    return false;
  }
  radixsort::appendSingleFixedFlat(columns_[0], input, size, arena, payloads);
  return true;
}

void RadixSortKeyCodec::decode(
    std::span<const EncodedKeyView> keys,
    std::span<const uint8_t> decodedColumns,
    std::span<const uint8_t> mayHaveNulls,
    memory::MemoryPool* pool,
    BufferPtr& cursorScratch,
    RowVectorPtr& result) const {
  BOLT_CHECK_NOT_NULL(pool, "Radix sort key memory pool must not be null");
  decodeColumns(
      columns_,
      rowType_,
      keys,
      decodedColumns,
      mayHaveNulls,
      pool,
      cursorScratch,
      result);
}

bool RadixSortKeyCodec::canDecodeSingleFixedColumn() const {
  return columns_.size() == 1 &&
      columns_[0].type->kind() != TypeKind::UNKNOWN &&
      fixedBodySize(*columns_[0].type).has_value();
}

bool RadixSortKeyCodec::tryDecodeSingleFixedColumn(
    const RadixSortRunStorage& arena,
    uint64_t begin,
    vector_size_t count,
    bool mayHaveNulls,
    memory::MemoryPool* pool,
    RowVectorPtr& result) const {
  if (!canDecodeSingleFixedColumn() || arena.layout().isVariable()) {
    return false;
  }
  BOLT_CHECK(
      begin <= arena.size() && count <= arena.size() - begin,
      "Radix sort key output range is out of bounds");

  VectorPtr child;
  if (result != nullptr && result->pool() == pool &&
      result->type()->equivalent(*rowType_) && result->childrenSize() == 1) {
    child = std::move(result->children()[0]);
    result.reset();
    BaseVector::prepareForReuse(child, count);
  } else {
    result.reset();
    child = BaseVector::create(columns_[0].type, count, pool);
  }
  dispatchFixedPhysicalLayout(
      arena.layout().kind(), [&]<RadixSortKeyLayoutKind KIND>() {
        decodeSinglePhysicalColumn<KIND>(
            columns_[0], arena, begin, count, mayHaveNulls, child);
      });
  result = std::make_shared<RowVector>(
      pool, rowType_, nullptr, count, std::vector<VectorPtr>{std::move(child)});
  return true;
}

bool RadixSortKeyCodec::tryDecodeSingleFixedColumn(
    std::span<const char* const> keys,
    RadixSortKeyLayoutKind layoutKind,
    bool mayHaveNulls,
    memory::MemoryPool* pool,
    RowVectorPtr& result) const {
  if (!canDecodeSingleFixedColumn() ||
      RadixSortKeyLayout::fromKind(layoutKind).isVariable()) {
    return false;
  }
  const auto count = static_cast<vector_size_t>(keys.size());

  VectorPtr child;
  if (result != nullptr && result->pool() == pool &&
      result->type()->equivalent(*rowType_) && result->childrenSize() == 1) {
    child = std::move(result->children()[0]);
    result.reset();
    BaseVector::prepareForReuse(child, count);
  } else {
    result.reset();
    child = BaseVector::create(columns_[0].type, count, pool);
  }
  dispatchFixedPhysicalLayout(layoutKind, [&]<RadixSortKeyLayoutKind KIND>() {
    decodeSinglePhysicalColumn<KIND>(columns_[0], keys, mayHaveNulls, child);
  });
  result = std::make_shared<RowVector>(
      pool, rowType_, nullptr, count, std::vector<VectorPtr>{std::move(child)});
  return true;
}

} // namespace bytedance::bolt::exec::radixsort
