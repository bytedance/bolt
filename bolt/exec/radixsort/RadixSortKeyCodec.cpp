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
#include <bit>
#include <cstring>
#include <limits>
#include <numeric>
#include <type_traits>
#include <unordered_map>

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
  // Decimal types use BIGINT/HUGEINT's physical representation.
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
  metadata.containsFloatingPoint =
      type->kind() == TypeKind::REAL || type->kind() == TypeKind::DOUBLE;
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
      metadata.containsFloatingPoint |= childMetadata.containsFloatingPoint;
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

uint32_t encodeFloat(float value, const RadixSortKeyColumn& column) {
  const auto bits = std::bit_cast<uint32_t>(value);
  const auto magnitude = bits & 0x7fffffffU;
  if (FOLLY_UNLIKELY(bits == 0x80000000U || magnitude > 0x7f800000U)) {
    column.hasSpecialValues = true;
  }
  return (bits & (uint32_t{1} << 31)) == 0 ? bits | (uint32_t{1} << 31) : ~bits;
}

uint64_t encodeDouble(double value, const RadixSortKeyColumn& column) {
  const auto bits = std::bit_cast<uint64_t>(value);
  const auto magnitude = bits & 0x7fffffffffffffffULL;
  if (FOLLY_UNLIKELY(
          bits == 0x8000000000000000ULL || magnitude > 0x7ff0000000000000ULL)) {
    column.hasSpecialValues = true;
  }
  return (bits & (uint64_t{1} << 63)) == 0 ? bits | (uint64_t{1} << 63) : ~bits;
}

template <TypeKind KIND>
FOLLY_ALWAYS_INLINE void encodeFixedScalarValue(
    typename TypeTraits<KIND>::NativeType value,
    char* output,
    bool descending,
    const RadixSortKeyColumn& column) {
  using T = typename TypeTraits<KIND>::NativeType;
  if constexpr (KIND == TypeKind::BOOLEAN) {
    const auto byte = static_cast<uint8_t>(value);
    output[0] =
        static_cast<char>(descending ? static_cast<uint8_t>(~byte) : byte);
  } else if constexpr (
      KIND == TypeKind::TINYINT || KIND == TypeKind::SMALLINT ||
      KIND == TypeKind::INTEGER || KIND == TypeKind::BIGINT) {
    encodeSignedWord<T>(value, output, descending);
  } else if constexpr (KIND == TypeKind::HUGEINT) {
    encodeSignedWord<int64_t>(
        static_cast<int64_t>(HugeInt::upper(value)), output, descending);
    encodeUnsignedWord<uint64_t>(
        HugeInt::lower(value), output + sizeof(int64_t), descending);
  } else if constexpr (KIND == TypeKind::REAL) {
    encodeUnsignedWord<uint32_t>(
        encodeFloat(value, column), output, descending);
  } else if constexpr (KIND == TypeKind::DOUBLE) {
    encodeUnsignedWord<uint64_t>(
        encodeDouble(value, column), output, descending);
  } else if constexpr (KIND == TypeKind::TIMESTAMP) {
    encodeSignedWord<int64_t>(value.getSeconds(), output, descending);
    encodeUnsignedWord<uint64_t>(
        value.getNanos(), output + sizeof(int64_t), descending);
  } else {
    BOLT_FAIL(
        "Fixed radix sort key encoding is not implemented for {}",
        TypeTraits<KIND>::name);
  }
}

template <TypeKind KIND>
FOLLY_ALWAYS_INLINE uint64_t encodeFixed64Value(
    typename TypeTraits<KIND>::NativeType value,
    const RadixSortKeyColumn& column) {
  if constexpr (KIND == TypeKind::BIGINT) {
    return static_cast<uint64_t>(value) ^ (uint64_t{1} << 63);
  } else if constexpr (KIND == TypeKind::DOUBLE) {
    return encodeDouble(value, column);
  } else {
    BOLT_FAIL(
        "64-bit radix sort key encoding is not implemented for {}",
        TypeTraits<KIND>::name);
  }
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
          std::array<char, Traits::kInlineCapacity> encodedBytes{};
          auto* bytes = encodedBytes.data();
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
          storeFixedKeyPrefix<Traits>(bytes, record);
          if constexpr (Traits::kHasPayload) {
            storeCompactPointer(
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
  static_assert(Traits::kInlineCapacity >= 1 + sizeof(T));
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
          uint64_t encoded = 0;
          auto firstWord = nullWord;
          if constexpr (HasNulls) {
            if (!bits::isBitNull(nulls, inputRow)) {
              encoded = encodeBody(values[inputRow]) ^ descendingMask;
              firstWord = validWord | (encoded >> 8);
            }
          } else {
            encoded = encodeBody(values[inputRow]) ^ descendingMask;
            firstWord = validWord | (encoded >> 8);
          }
          storeUnaligned<uint64_t>(record, firstWord);
          if constexpr (Traits::kInlineWords > 1) {
            storeUnaligned<uint64_t>(record + sizeof(uint64_t), encoded << 56);
          } else {
            static_assert(Traits::kInlineTailBytes > 0);
            record[sizeof(uint64_t)] = static_cast<char>(encoded);
            std::memset(
                record + sizeof(uint64_t) + 1, 0, Traits::kInlineTailBytes - 1);
          }
          if constexpr (Traits::kHasPayload) {
            storeCompactPointer(
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
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed16:
      appendSingleFixed64FlatLayout<
          RadixSortKeyLayoutKind::kKeyWithPayloadFixed16>(
          column, input, size, arena, payloads, encodeBody);
      return;
    default:
      BOLT_FAIL("Direct 64-bit sort key layout does not match type");
  }
}

template <TypeKind KIND>
void appendSingleFixedFlatByKind(
    const RadixSortKeyColumn& column,
    const BaseVector& input,
    vector_size_t size,
    RadixSortRunStorage& arena,
    std::span<char* const> payloads) {
  using T = typename TypeTraits<KIND>::NativeType;
  if constexpr (KIND == TypeKind::BIGINT || KIND == TypeKind::DOUBLE) {
    appendSingleFixed64Flat(
        column,
        *input.asUnchecked<FlatVector<T>>(),
        size,
        arena,
        payloads,
        [&column](T value) { return encodeFixed64Value<KIND>(value, column); });
  } else if constexpr (
      KIND == TypeKind::HUGEINT || KIND == TypeKind::TIMESTAMP) {
    appendSingleFixedFlat<
        RadixSortKeyLayoutKind::kKeyOnlyFixed24,
        RadixSortKeyLayoutKind::kKeyWithPayloadFixed24>(
        column,
        *input.asUnchecked<FlatVector<T>>(),
        size,
        arena,
        payloads,
        [&column](const auto& values, auto row, auto* output, bool descending) {
          encodeFixedScalarValue<KIND>(
              values.rawValues()[row], output, descending, column);
        });
  } else if constexpr (
      KIND == TypeKind::BOOLEAN || KIND == TypeKind::TINYINT ||
      KIND == TypeKind::SMALLINT || KIND == TypeKind::INTEGER ||
      KIND == TypeKind::REAL) {
    appendSingleFixedFlat<
        RadixSortKeyLayoutKind::kKeyOnlyFixed8,
        RadixSortKeyLayoutKind::kKeyWithPayloadFixed16>(
        column,
        *input.asUnchecked<FlatVector<T>>(),
        size,
        arena,
        payloads,
        [&column](const auto& values, auto row, auto* output, bool descending) {
          if constexpr (KIND == TypeKind::BOOLEAN) {
            encodeFixedScalarValue<KIND>(
                values.valueAtFast(row), output, descending, column);
          } else {
            encodeFixedScalarValue<KIND>(
                values.rawValues()[row], output, descending, column);
          }
        });
  } else {
    BOLT_FAIL(
        "Direct fixed sort key encoder is not implemented for {}",
        column.type->toString());
  }
}

void appendSingleFixedFlat(
    const RadixSortKeyColumn& column,
    const BaseVector& input,
    vector_size_t size,
    RadixSortRunStorage& arena,
    std::span<char* const> payloads) {
  BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      appendSingleFixedFlatByKind,
      column.type->kind(),
      column,
      input,
      size,
      arena,
      payloads);
}

uint64_t encodedStringBodySize(StringView value);

// A complex column can visit the same child vector once for every parent row.
// Keep wrapper decoding scoped to the column batch, not to each child range.
class DecodedKeyVectors {
 public:
  DecodedVector& get(const BaseVector& vector) {
    auto& decoded = vectors_[&vector];
    if (decoded == nullptr) {
      decoded = std::make_unique<DecodedVector>(vector);
    }
    return *decoded;
  }

 private:
  std::unordered_map<const BaseVector*, std::unique_ptr<DecodedVector>>
      vectors_;
};

DecodedVector& decodeInput(
    const BaseVector& vector,
    DecodedVector& local,
    DecodedKeyVectors* cache) {
  if (cache != nullptr) {
    return cache->get(vector);
  }
  local.decode(vector);
  return local;
}

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
    uint64_t& size,
    DecodedKeyVectors& decodedVectors);

void addVariableColumnSizes(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t size,
    uint64_t* rowSizes,
    DecodedKeyVectors& decodedVectors);

uint64_t encodedRangeSize(
    const RadixSortKeyColumn& column,
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t count,
    const RangeSizeMetadata& metadata,
    DecodedKeyVectors& decodedVectors) {
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
    auto& decoded = decodedVectors.get(elements);
    if (decoded.isConstantMapping()) {
      const auto elementSize = decoded.isNullAt(0) ? uint64_t{1} : validSize;
      return static_cast<uint64_t>(count) * elementSize;
    }
    const auto* nulls = decoded.nulls();
    if (nulls == nullptr) {
      return static_cast<uint64_t>(count) * validSize;
    }
    for (vector_size_t index = offset; index < offset + count; ++index) {
      size += bits::isBitNull(nulls, index) ? uint64_t{1} : validSize;
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

    auto& decoded = decodedVectors.get(elements);
    if (decoded.isConstantMapping()) {
      const auto elementSize = decoded.isNullAt(0)
          ? uint64_t{1}
          : 1 + encodedStringBodySize(decoded.valueAt<StringView>(0));
      return static_cast<uint64_t>(count) * elementSize;
    }
    const auto* values = decoded.data<StringView>();
    const auto* indices = decoded.indices();
    const auto* nulls = decoded.nulls();
    for (vector_size_t index = offset; index < offset + count; ++index) {
      size += nulls != nullptr && bits::isBitNull(nulls, index)
          ? uint64_t{1}
          : 1 + encodedStringBodySize(values[indices[index]]);
    }
    return size;
  }

  for (vector_size_t index = 0; index < count; ++index) {
    uint64_t childSize;
    encodedSize(column, elements, offset + index, childSize, decodedVectors);
    size += childSize;
  }
  return size;
}

uint64_t encodedArraySize(
    const RadixSortKeyColumn& column,
    const ArrayVector& array,
    vector_size_t row,
    const RangeSizeMetadata& elementMetadata,
    DecodedKeyVectors& decodedVectors) {
  return 1 +
      encodedRangeSize(
             column.children[0],
             *array.elements(),
             array.offsetAt(row),
             array.sizeAt(row),
             elementMetadata,
             decodedVectors) +
      1;
}

uint64_t encodedMapSize(
    const RadixSortKeyColumn& column,
    const MapVector& map,
    vector_size_t row,
    const RangeSizeMetadata& keyMetadata,
    const RangeSizeMetadata& valueMetadata,
    DecodedKeyVectors& decodedVectors) {
  const auto offset = map.offsetAt(row);
  const auto count = map.sizeAt(row);
  const auto keysSize = encodedRangeSize(
      column.children[0],
      *map.mapKeys(),
      offset,
      count,
      keyMetadata,
      decodedVectors);
  return 1 + keysSize + 1 +
      encodedRangeSize(
             column.children[1],
             *map.mapValues(),
             offset,
             count,
             valueMetadata,
             decodedVectors) +
      1;
}

uint64_t encodedRowSize(
    const RadixSortKeyColumn& column,
    const RowVector& rowVector,
    vector_size_t row,
    DecodedKeyVectors& decodedVectors) {
  uint64_t size = 1;
  for (uint32_t child = 0; child < column.children.size(); ++child) {
    uint64_t childSize;
    encodedSize(
        column.children[child],
        *rowVector.childAt(child),
        row,
        childSize,
        decodedVectors);
    size += childSize;
  }
  return size;
}

void addRowColumnSizes(
    const RadixSortKeyColumn& column,
    const DecodedVector& decoded,
    vector_size_t size,
    uint64_t* rowSizes,
    DecodedKeyVectors& decodedVectors) {
  const auto* rowVector = decoded.base()->asUnchecked<RowVector>();
  if (decoded.isIdentityMapping() && !decoded.mayHaveNulls()) {
    for (uint32_t child = 0; child < column.children.size(); ++child) {
      addVariableColumnSizes(
          column.children[child],
          *rowVector->childAt(child),
          size,
          rowSizes,
          decodedVectors);
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
          column.children[child],
          *childVector,
          decoded.index(row),
          childSize,
          decodedVectors);
      rowSizes[row] += childSize;
    }
  }
}

void addArrayColumnSizes(
    const RadixSortKeyColumn& column,
    const DecodedVector& decoded,
    vector_size_t size,
    uint64_t* rowSizes,
    DecodedKeyVectors& decodedVectors) {
  const auto* array = decoded.base()->asUnchecked<ArrayVector>();
  const auto elementMetadata =
      rangeSizeMetadata(column.children[0], *array->elements());
  for (vector_size_t row = 0; row < size; ++row) {
    if (decoded.isNullAt(row)) {
      continue;
    }
    rowSizes[row] += encodedArraySize(
                         column,
                         *array,
                         decoded.index(row),
                         elementMetadata,
                         decodedVectors) -
        1;
  }
}

void addMapColumnSizes(
    const RadixSortKeyColumn& column,
    const DecodedVector& decoded,
    vector_size_t size,
    uint64_t* rowSizes,
    DecodedKeyVectors& decodedVectors) {
  const auto* map = decoded.base()->asUnchecked<MapVector>();
  const auto keyMetadata =
      rangeSizeMetadata(column.children[0], *map->mapKeys());
  const auto valueMetadata =
      rangeSizeMetadata(column.children[1], *map->mapValues());
  for (vector_size_t row = 0; row < size; ++row) {
    if (decoded.isNullAt(row)) {
      continue;
    }
    rowSizes[row] += encodedMapSize(
                         column,
                         *map,
                         decoded.index(row),
                         keyMetadata,
                         valueMetadata,
                         decodedVectors) -
        1;
  }
}

void addComplexColumnSizes(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t size,
    uint64_t* rowSizes,
    DecodedKeyVectors& decodedVectors) {
  DecodedVector decoded(vector);

  for (vector_size_t row = 0; row < size; ++row) {
    rowSizes[row] += 1;
  }
  if (decoded.isConstantMapping() && decoded.isNullAt(0)) {
    return;
  }

  switch (column.type->kind()) {
    case TypeKind::ROW:
      addRowColumnSizes(column, decoded, size, rowSizes, decodedVectors);
      return;
    case TypeKind::ARRAY:
      addArrayColumnSizes(column, decoded, size, rowSizes, decodedVectors);
      return;
    case TypeKind::MAP:
      addMapColumnSizes(column, decoded, size, rowSizes, decodedVectors);
      return;
    default:
      BOLT_UNREACHABLE();
  }
}

void encodedSize(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t row,
    uint64_t& size,
    DecodedKeyVectors& decodedVectors) {
  if (vector.isNullAt(row)) {
    size = 1;
    return;
  }
  if (column.type->kind() == TypeKind::UNKNOWN) {
    BOLT_FAIL("UNKNOWN sort key values must be null");
  }
  if (column.type->kind() == TypeKind::ROW) {
    const auto* rowVector = vector.wrappedVector()->as<RowVector>();
    size = encodedRowSize(
        column, *rowVector, vector.wrappedIndex(row), decodedVectors);
    return;
  }
  if (column.type->kind() == TypeKind::ARRAY) {
    const auto* arrayVector = vector.wrappedVector()->as<ArrayVector>();
    const auto elementMetadata =
        rangeSizeMetadata(column.children[0], *arrayVector->elements());
    size = encodedArraySize(
        column,
        *arrayVector,
        vector.wrappedIndex(row),
        elementMetadata,
        decodedVectors);
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
        valueMetadata,
        decodedVectors);
    return;
  }
  if (column.type->kind() == TypeKind::VARCHAR ||
      column.type->kind() == TypeKind::VARBINARY) {
    size = 1 + encodedStringBodySize(valueAt<StringView>(vector, row));
    return;
  }
  auto bodySize = fixedBodySize(*column.type);
  size = 1 + *bodySize;
}

uint64_t encodeStringValue(StringView value, char* output, bool descending) {
  if (std::memchr(value.data(), kStringDelimiter, value.size()) == nullptr &&
      std::memchr(value.data(), kBlobEscape, value.size()) == nullptr) {
    if (!descending) {
      std::memcpy(output, value.data(), value.size());
    } else {
      uint32_t index = 0;
      for (; index + sizeof(uint64_t) <= value.size();
           index += sizeof(uint64_t)) {
        storeUnaligned<uint64_t>(
            output + index, ~loadUnaligned<uint64_t>(value.data() + index));
      }
      for (; index < value.size(); ++index) {
        output[index] =
            static_cast<char>(~static_cast<uint8_t>(value.data()[index]));
      }
    }
    output[value.size()] =
        static_cast<char>(descending ? ~kStringDelimiter : kStringDelimiter);
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

template <TypeKind KIND>
FOLLY_ALWAYS_INLINE void encodeBytesFixedScalarValue(
    typename TypeTraits<KIND>::NativeType value,
    char* output,
    bool descending,
    const RadixSortKeyColumn& column) {
  using T = typename TypeTraits<KIND>::NativeType;
  if constexpr (KIND == TypeKind::BOOLEAN) {
    const auto byte = static_cast<uint8_t>(value);
    output[0] =
        static_cast<char>(descending ? static_cast<uint8_t>(~byte) : byte);
  } else if constexpr (
      KIND == TypeKind::TINYINT || KIND == TypeKind::SMALLINT ||
      KIND == TypeKind::INTEGER || KIND == TypeKind::BIGINT) {
    encodeSigned<T>(value, output, descending);
  } else if constexpr (KIND == TypeKind::HUGEINT) {
    encodeSigned<int64_t>(
        static_cast<int64_t>(HugeInt::upper(value)), output, descending);
    encodeUnsigned<uint64_t>(
        HugeInt::lower(value), output + sizeof(int64_t), descending);
  } else if constexpr (KIND == TypeKind::REAL) {
    encodeUnsigned<uint32_t>(encodeFloat(value, column), output, descending);
  } else if constexpr (KIND == TypeKind::DOUBLE) {
    encodeUnsigned<uint64_t>(encodeDouble(value, column), output, descending);
  } else if constexpr (KIND == TypeKind::TIMESTAMP) {
    encodeSigned<int64_t>(value.getSeconds(), output, descending);
    encodeUnsigned<uint64_t>(
        value.getNanos(), output + sizeof(int64_t), descending);
  } else {
    BOLT_FAIL(
        "Fixed radix sort key byte encoding is not implemented for {}",
        TypeTraits<KIND>::name);
  }
}

template <typename T, typename EncodeBody>
uint64_t encodeFixedScalarArrayElements(
    const RadixSortKeyColumn& column,
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t count,
    char* output,
    EncodeBody encodeBody,
    DecodedKeyVectors& decodedVectors) {
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

  auto& decoded = decodedVectors.get(elements);
  const auto* values = decoded.data<T>();
  const auto* indices = decoded.indices();
  const auto* nulls = decoded.nulls();
  const auto valueAt = [&](vector_size_t row) {
    const auto index = indices[row];
    if constexpr (std::is_same_v<T, bool>) {
      return bits::isBitSet(reinterpret_cast<const uint64_t*>(values), index);
    } else if constexpr (std::is_same_v<T, int128_t>) {
      return HugeInt::deserialize(
          reinterpret_cast<const char*>(values) + sizeof(T) * index);
    } else {
      return values[index];
    }
  };
  for (vector_size_t index = offset; index < offset + count; ++index) {
    writeElement(index, valueAt, [&](vector_size_t row) {
      return nulls != nullptr && bits::isBitNull(nulls, row);
    });
  }
  return written;
}

template <TypeKind KIND>
uint64_t encodeFixedScalarArrayElementsByKind(
    const RadixSortKeyColumn& column,
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t count,
    char* output,
    DecodedKeyVectors& decodedVectors) {
  if constexpr (
      KIND == TypeKind::BOOLEAN || KIND == TypeKind::TINYINT ||
      KIND == TypeKind::SMALLINT || KIND == TypeKind::INTEGER ||
      KIND == TypeKind::BIGINT || KIND == TypeKind::HUGEINT ||
      KIND == TypeKind::REAL || KIND == TypeKind::DOUBLE ||
      KIND == TypeKind::TIMESTAMP) {
    using T = typename TypeTraits<KIND>::NativeType;
    return encodeFixedScalarArrayElements<T>(
        column,
        elements,
        offset,
        count,
        output,
        [&column](T value, char* out, bool descending) {
          encodeBytesFixedScalarValue<KIND>(value, out, descending, column);
        },
        decodedVectors);
  } else {
    BOLT_FAIL(
        "Radix sort fixed array element encoding is not implemented for {}",
        column.type->toString());
  }
}

uint64_t encodeFixedScalarArrayElements(
    const RadixSortKeyColumn& column,
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t count,
    char* output,
    DecodedKeyVectors& decodedVectors) {
  return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      encodeFixedScalarArrayElementsByKind,
      column.type->kind(),
      column,
      elements,
      offset,
      count,
      output,
      decodedVectors);
}

class MapKeyIndexScratch {
 public:
  template <typename Func>
  void withSortedIndices(const MapVector& map, vector_size_t row, Func&& func) {
    const auto offset = map.offsetAt(row);
    const auto count = map.sizeAt(row);
    if (count == 0 || count == 1 || map.hasSortedKeys()) {
      func(offset, count, nullptr);
      return;
    }

    auto& entry = entries_[entryIndex(map, row)];
    if (entry.map == &map && entry.row == row) {
      if (entry.sorted) {
        func(offset, count, nullptr);
        return;
      }
      if (!entry.indices.empty()) {
        func(0, count, entry.indices.data());
        return;
      }
      if (canCache(count)) {
        cacheSortedIndices(map, row, entry.indices);
        func(0, count, entry.indices.data());
        return;
      }
    } else if (entry.map == nullptr) {
      entry.map = &map;
      entry.row = row;
      entry.sorted = map.isSorted(row);
      if (entry.sorted) {
        func(offset, count, nullptr);
        return;
      }
    } else if (map.isSorted(row)) {
      func(offset, count, nullptr);
      return;
    }

    auto& indices = acquireScratch();
    auto guard = ScratchGuard(*this);
    indices.resize(count);
    std::iota(indices.begin(), indices.end(), offset);
    map.mapKeys()->sortIndices(indices, CompareFlags());
    func(0, count, indices.data());
  }

 private:
  struct Entry {
    const MapVector* map{nullptr};
    vector_size_t row{0};
    bool sorted{false};
    std::vector<vector_size_t> indices;
  };

  class ScratchGuard {
   public:
    explicit ScratchGuard(MapKeyIndexScratch& scratch) : scratch_(scratch) {}

    ~ScratchGuard() {
      --scratch_.depth_;
    }

   private:
    MapKeyIndexScratch& scratch_;
  };

  std::vector<vector_size_t>& acquireScratch() {
    if (depth_ == scratch_.size()) {
      scratch_.emplace_back();
    }
    return scratch_[depth_++];
  }

  bool canCache(size_t count) const {
    return cachedIndexCount_ + count <= kMaxCachedIndices;
  }

  void cacheSortedIndices(
      const MapVector& map,
      vector_size_t row,
      std::vector<vector_size_t>& indices) {
    const auto count = map.sizeAt(row);
    cachedIndexCount_ += count;
    indices.resize(count);
    const auto offset = map.offsetAt(row);
    std::iota(indices.begin(), indices.end(), offset);
    map.mapKeys()->sortIndices(indices, CompareFlags());
  }

  static size_t entryIndex(const MapVector& map, vector_size_t row) {
    const auto address = reinterpret_cast<uintptr_t>(&map);
    return ((address >> 4) ^ static_cast<uint32_t>(row)) & (kCacheSlots - 1);
  }

  static constexpr size_t kCacheSlots = 32;
  static constexpr size_t kMaxCachedIndices = 1 << 20;
  static_assert((kCacheSlots & (kCacheSlots - 1)) == 0);

  std::vector<std::vector<vector_size_t>> scratch_;
  std::array<Entry, kCacheSlots> entries_;
  size_t cachedIndexCount_{0};
  size_t depth_{0};
};

template <TypeKind KIND>
void encodeScalarValue(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t row,
    char* output,
    bool descending,
    uint64_t& written) {
  using T = typename TypeTraits<KIND>::NativeType;
  if constexpr (
      KIND == TypeKind::BOOLEAN || KIND == TypeKind::TINYINT ||
      KIND == TypeKind::SMALLINT || KIND == TypeKind::INTEGER ||
      KIND == TypeKind::BIGINT || KIND == TypeKind::HUGEINT ||
      KIND == TypeKind::REAL || KIND == TypeKind::DOUBLE ||
      KIND == TypeKind::TIMESTAMP) {
    encodeBytesFixedScalarValue<KIND>(
        scalarValueAt<T>(vector, row), output, descending, column);
    written = 1 + sizeof(T);
  } else if constexpr (
      KIND == TypeKind::VARCHAR || KIND == TypeKind::VARBINARY) {
    written = 1 +
        encodeStringValue(valueAt<StringView>(vector, row), output, descending);
  } else {
    BOLT_FAIL(
        "Radix sort key encoding is not implemented for {}",
        TypeTraits<KIND>::name);
  }
}

void encodeValue(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t row,
    char* output,
    uint64_t& written,
    MapKeyIndexScratch& mapIndices,
    DecodedKeyVectors& decodedVectors) {
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
          childWritten,
          mapIndices,
          decodedVectors);
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
          child, elements, arrayOffset, count, output + offset, decodedVectors);
      addWritten(offset, childWritten);
    } else {
      for (vector_size_t index = 0; index < count; ++index) {
        uint64_t childWritten;
        encodeValue(
            child,
            elements,
            arrayOffset + index,
            output + offset,
            childWritten,
            mapIndices,
            decodedVectors);
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
    uint64_t offset = 1;
    const auto delimiter = static_cast<char>(
        descending ? static_cast<uint8_t>(~kStringDelimiter)
                   : kStringDelimiter);
    mapIndices.withSortedIndices(
        *mapVector,
        wrappedRow,
        [&](vector_size_t first,
            vector_size_t count,
            const vector_size_t* indices) {
          const auto indexAt = [&](vector_size_t entry) {
            return indices == nullptr ? first + entry : indices[entry];
          };
          for (vector_size_t entry = 0; entry < count; ++entry) {
            uint64_t childWritten;
            encodeValue(
                column.children[0],
                *mapVector->mapKeys(),
                indexAt(entry),
                output + offset,
                childWritten,
                mapIndices,
                decodedVectors);
            addWritten(offset, childWritten);
          }
          output[offset++] = delimiter;
          for (vector_size_t entry = 0; entry < count; ++entry) {
            uint64_t childWritten;
            encodeValue(
                column.children[1],
                *mapVector->mapValues(),
                indexAt(entry),
                output + offset,
                childWritten,
                mapIndices,
                decodedVectors);
            addWritten(offset, childWritten);
          }
          output[offset++] = delimiter;
        });
    written = offset;
    return;
  } else if (
      column.type->isPrimitiveType() &&
      column.type->kind() != TypeKind::UNKNOWN) {
    BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
        encodeScalarValue,
        column.type->kind(),
        column,
        vector,
        row,
        body,
        descending,
        written);
    return;
  }
  BOLT_FAIL(
      "Radix sort key encoding is not implemented for {}",
      column.type->toString());
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
  const auto* values = decoded.data<StringView>();
  const auto* indices = decoded.indices();
  const auto* nulls = decoded.nulls();
  addStringColumnSizes(
      size,
      rowSizes,
      [&](vector_size_t row) { return values[indices[row]]; },
      [&](vector_size_t row) {
        return nulls != nullptr && bits::isBitNull(nulls, row);
      });
}

void addVariableColumnSizes(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t size,
    uint64_t* rowSizes,
    DecodedKeyVectors& decodedVectors) {
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
    const auto* nulls = decoded.nulls();
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

  if (column.type->kind() == TypeKind::ROW ||
      column.type->kind() == TypeKind::ARRAY ||
      column.type->kind() == TypeKind::MAP) {
    addComplexColumnSizes(column, vector, size, rowSizes, decodedVectors);
    return;
  }

  DecodedVector decoded(vector);
  if (decoded.isConstantMapping()) {
    uint64_t columnSize;
    if (decoded.isNullAt(0)) {
      columnSize = 1;
    } else {
      encodedSize(
          column,
          *decoded.base(),
          decoded.index(0),
          columnSize,
          decodedVectors);
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

void initializeVariableKeySizes(
    const std::vector<RadixSortKeyColumn>& columns,
    const RowVector& input,
    uint32_t firstColumn,
    uint64_t* rowSizes) {
  uint64_t flatFixedSize = 0;
  for (uint32_t column = firstColumn; column < columns.size(); ++column) {
    const auto bodySize = fixedBodySize(*columns[column].type);
    if (bodySize.has_value() &&
        columns[column].type->kind() != TypeKind::UNKNOWN &&
        input.childAt(column)->encoding() == VectorEncoding::Simple::FLAT) {
      flatFixedSize += *bodySize + 1;
    }
  }
  std::fill(rowSizes, rowSizes + input.size(), flatFixedSize);
  for (uint32_t column = firstColumn; column < columns.size(); ++column) {
    const auto& vector = *input.childAt(column);
    const auto bodySize = fixedBodySize(*columns[column].type);
    if (bodySize.has_value() &&
        columns[column].type->kind() != TypeKind::UNKNOWN &&
        vector.encoding() == VectorEncoding::Simple::FLAT) {
      const auto* nulls = vector.rawNulls();
      if (nulls != nullptr) {
        bits::forEachUnsetBit(nulls, 0, input.size(), [&](vector_size_t row) {
          rowSizes[row] -= *bodySize;
        });
      }
      continue;
    }
    DecodedKeyVectors decodedVectors;
    addVariableColumnSizes(
        columns[column], vector, input.size(), rowSizes, decodedVectors);
  }
}

class StridedEncodeOutput {
 public:
  StridedEncodeOutput(char* data, uint32_t stride, uint32_t offset)
      : data_(data), stride_(stride), offset_(offset) {}

  char* current(vector_size_t row) const {
    return data_ + static_cast<uint64_t>(row) * stride_ + offset_;
  }

  void advance(vector_size_t /*row*/, uint64_t /*bytes*/) const {}

 private:
  char* data_;
  uint32_t stride_;
  uint32_t offset_;
};

class StridedCursorEncodeOutput {
 public:
  StridedCursorEncodeOutput(char* data, uint32_t stride, uint64_t* cursors)
      : data_(data), stride_(stride), cursors_(cursors) {}

  char* current(vector_size_t row) const {
    return data_ + static_cast<uint64_t>(row) * stride_ + cursors_[row];
  }

  void advance(vector_size_t row, uint64_t bytes) const {
    cursors_[row] += bytes;
  }

 private:
  char* data_;
  uint32_t stride_;
  uint64_t* cursors_;
};

class IndirectEncodeOutput {
 public:
  IndirectEncodeOutput(
      char* records,
      uint32_t stride,
      uint32_t dataOffset,
      uint64_t* cursors)
      : records_(records),
        stride_(stride),
        dataOffset_(dataOffset),
        cursors_(cursors) {}

  char* current(vector_size_t row) const {
    const auto* record = records_ + static_cast<uint64_t>(row) * stride_;
    return loadCompactPointer(record + dataOffset_) + cursors_[row];
  }

  void advance(vector_size_t row, uint64_t bytes) const {
    cursors_[row] += bytes;
  }

 private:
  char* records_;
  uint32_t stride_;
  uint32_t dataOffset_;
  uint64_t* cursors_;
};

template <
    bool MayHaveNulls,
    typename T,
    typename Output,
    typename ValueAt,
    typename IsNullAt,
    typename EncodeBody>
void encodeFixedColumn(
    const RadixSortKeyColumn& column,
    vector_size_t source,
    vector_size_t size,
    const Output& output,
    ValueAt valueAt,
    IsNullAt isNullAt,
    EncodeBody encodeBody,
    bool fixedWidthNulls) {
  const auto bodySize = *fixedBodySize(*column.type);
  const bool descending = !column.flags.ascending;
  for (vector_size_t row = 0; row < size; ++row) {
    const auto inputRow = source + row;
    auto* destination = output.current(row);
    if constexpr (MayHaveNulls) {
      if (isNullAt(inputRow)) {
        destination[0] = static_cast<char>(nullMarker(column.flags));
        if (fixedWidthNulls) {
          std::memset(destination + 1, 0, bodySize);
          output.advance(row, bodySize + 1);
        } else {
          output.advance(row, 1);
        }
        continue;
      }
    }
    destination[0] = static_cast<char>(validMarker(column.flags));
    encodeBody(valueAt(inputRow), destination + 1, descending);
    output.advance(row, bodySize + 1);
  }
}

template <typename Output>
void encodeFixedColumnAllNulls(
    const RadixSortKeyColumn& column,
    vector_size_t size,
    const Output& output,
    bool fixedWidthNulls) {
  const auto null = static_cast<char>(nullMarker(column.flags));
  const auto bodySize = *fixedBodySize(*column.type);
  for (vector_size_t row = 0; row < size; ++row) {
    auto* destination = output.current(row);
    destination[0] = null;
    if (fixedWidthNulls) {
      std::memset(destination + 1, 0, bodySize);
      output.advance(row, bodySize + 1);
    } else {
      output.advance(row, 1);
    }
  }
}

template <typename T, typename Output, typename EncodeBody>
void encodeFixedColumn(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t source,
    vector_size_t size,
    const Output& output,
    EncodeBody encodeBody,
    bool fixedWidthNulls,
    DecodedKeyVectors* decodedInputs) {
  if (vector.encoding() == VectorEncoding::Simple::FLAT) {
    const auto* flat = vector.asUnchecked<FlatVector<T>>();
    const auto* nulls = flat->rawNulls();
    if constexpr (std::is_same_v<T, bool>) {
      if (nulls == nullptr) {
        encodeFixedColumn<false, T>(
            column,
            source,
            size,
            output,
            [&](vector_size_t row) { return flat->valueAtFast(row); },
            [](vector_size_t) { return false; },
            encodeBody,
            fixedWidthNulls);
        return;
      }
      encodeFixedColumn<true, T>(
          column,
          source,
          size,
          output,
          [&](vector_size_t row) { return flat->valueAtFast(row); },
          [&](vector_size_t row) {
            return nulls != nullptr && bits::isBitNull(nulls, row);
          },
          encodeBody,
          fixedWidthNulls);
    } else {
      const auto* values = flat->rawValues();
      if (nulls == nullptr) {
        encodeFixedColumn<false, T>(
            column,
            source,
            size,
            output,
            [&](vector_size_t row) { return values[row]; },
            [](vector_size_t) { return false; },
            encodeBody,
            fixedWidthNulls);
        return;
      }
      encodeFixedColumn<true, T>(
          column,
          source,
          size,
          output,
          [&](vector_size_t row) { return values[row]; },
          [&](vector_size_t row) {
            return nulls != nullptr && bits::isBitNull(nulls, row);
          },
          encodeBody,
          fixedWidthNulls);
    }
    return;
  }
  if (vector.encoding() == VectorEncoding::Simple::CONSTANT) {
    const auto* constant = vector.asUnchecked<ConstantVector<T>>();
    const bool isNull = constant->isNullAt(0);
    if (isNull) {
      encodeFixedColumnAllNulls(column, size, output, fixedWidthNulls);
    } else {
      const auto value = constant->valueAtFast(0);
      encodeFixedColumn<false, T>(
          column,
          source,
          size,
          output,
          [&](vector_size_t) { return value; },
          [](vector_size_t) { return false; },
          encodeBody,
          fixedWidthNulls);
    }
    return;
  }

  DecodedVector local;
  auto& decoded = decodeInput(vector, local, decodedInputs);
  const auto* values = decoded.data<T>();
  const auto* indices = decoded.indices();
  const auto* nulls = decoded.nulls();
  const auto valueAt = [&](vector_size_t row) {
    const auto index = indices[row];
    if constexpr (std::is_same_v<T, bool>) {
      return bits::isBitSet(reinterpret_cast<const uint64_t*>(values), index);
    } else if constexpr (std::is_same_v<T, int128_t>) {
      return HugeInt::deserialize(
          reinterpret_cast<const char*>(values) + sizeof(T) * index);
    } else {
      return values[index];
    }
  };
  if (nulls == nullptr) {
    encodeFixedColumn<false, T>(
        column,
        source,
        size,
        output,
        valueAt,
        [](vector_size_t) { return false; },
        encodeBody,
        fixedWidthNulls);
    return;
  }
  encodeFixedColumn<true, T>(
      column,
      source,
      size,
      output,
      valueAt,
      [&](vector_size_t row) { return bits::isBitNull(nulls, row); },
      encodeBody,
      fixedWidthNulls);
}

template <TypeKind KIND, typename Output>
void encodeFixedColumnByKind(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t source,
    vector_size_t size,
    const Output& output,
    DecodedKeyVectors* decodedInputs = nullptr,
    bool fixedWidthNulls = false) {
  if constexpr (KIND == TypeKind::UNKNOWN) {
    for (vector_size_t row = 0; row < size; ++row) {
      output.current(row)[0] = static_cast<char>(nullMarker(column.flags));
      output.advance(row, 1);
    }
  } else if constexpr (
      KIND == TypeKind::BOOLEAN || KIND == TypeKind::TINYINT ||
      KIND == TypeKind::SMALLINT || KIND == TypeKind::INTEGER ||
      KIND == TypeKind::BIGINT || KIND == TypeKind::HUGEINT ||
      KIND == TypeKind::REAL || KIND == TypeKind::DOUBLE ||
      KIND == TypeKind::TIMESTAMP) {
    using T = typename TypeTraits<KIND>::NativeType;
    encodeFixedColumn<T>(
        column,
        vector,
        source,
        size,
        output,
        [&column](T value, char* output, bool descending) {
          encodeFixedScalarValue<KIND>(value, output, descending, column);
        },
        fixedWidthNulls,
        decodedInputs);
  } else {
    BOLT_FAIL(
        "Sort fixed key column is not implemented for {}",
        column.type->toString());
  }
}

template <typename Output>
void encodeFixedColumn(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t source,
    vector_size_t size,
    const Output& output,
    DecodedKeyVectors* decodedInputs = nullptr,
    bool fixedWidthNulls = false) {
  const auto dispatch = [&]<TypeKind KIND>() {
    encodeFixedColumnByKind<KIND>(
        column, vector, source, size, output, decodedInputs, fixedWidthNulls);
  };
  BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      dispatch.template operator(), column.type->kind());
}

template <typename Output, typename ValueAt, typename IsNullAt>
void encodeStringColumn(
    const RadixSortKeyColumn& column,
    vector_size_t source,
    vector_size_t size,
    const Output& output,
    ValueAt valueAt,
    IsNullAt isNullAt) {
  const bool descending = !column.flags.ascending;
  for (vector_size_t row = 0; row < size; ++row) {
    const auto inputRow = source + row;
    auto* destination = output.current(row);
    if (isNullAt(inputRow)) {
      destination[0] = static_cast<char>(nullMarker(column.flags));
      output.advance(row, 1);
      continue;
    }
    destination[0] = static_cast<char>(validMarker(column.flags));
    const auto value = valueAt(inputRow);
    output.advance(
        row, 1 + encodeStringValue(value, destination + 1, descending));
  }
}

template <typename Output>
void encodeStringColumn(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t source,
    vector_size_t size,
    const Output& output,
    DecodedKeyVectors* decodedInputs) {
  if (vector.encoding() == VectorEncoding::Simple::FLAT) {
    const auto* flat = vector.asUnchecked<FlatVector<StringView>>();
    const auto* values = flat->rawValues();
    const auto* nulls = flat->rawNulls();
    encodeStringColumn(
        column,
        source,
        size,
        output,
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
        source,
        size,
        output,
        [&](vector_size_t) { return value; },
        [&](vector_size_t) { return isNull; });
    return;
  }

  DecodedVector local;
  auto& decoded = decodeInput(vector, local, decodedInputs);
  const auto* values = decoded.data<StringView>();
  const auto* indices = decoded.indices();
  const auto* nulls = decoded.nulls();
  encodeStringColumn(
      column,
      source,
      size,
      output,
      [&](vector_size_t row) { return values[indices[row]]; },
      [&](vector_size_t row) {
        return nulls != nullptr && bits::isBitNull(nulls, row);
      });
}

template <typename Output>
void encodeVariableColumn(
    const RadixSortKeyColumn& column,
    const BaseVector& vector,
    vector_size_t source,
    vector_size_t size,
    const Output& output,
    DecodedKeyVectors* decodedInputs = nullptr,
    bool fixedWidthNulls = false) {
  if (column.type->kind() == TypeKind::VARCHAR ||
      column.type->kind() == TypeKind::VARBINARY) {
    encodeStringColumn(column, vector, source, size, output, decodedInputs);
    return;
  }
  if (fixedBodySize(*column.type).has_value()) {
    encodeFixedColumn(
        column, vector, source, size, output, decodedInputs, fixedWidthNulls);
    return;
  }

  DecodedVector local;
  auto& decoded = decodeInput(vector, local, decodedInputs);
  MapKeyIndexScratch mapIndices;
  DecodedKeyVectors localDecodedInputs;
  auto& decodedVectors =
      decodedInputs == nullptr ? localDecodedInputs : *decodedInputs;
  for (vector_size_t row = 0; row < size; ++row) {
    const auto inputRow = source + row;
    auto* destination = output.current(row);
    if (decoded.isNullAt(inputRow)) {
      destination[0] = static_cast<char>(nullMarker(column.flags));
      output.advance(row, 1);
      continue;
    }
    uint64_t written;
    encodeValue(
        column,
        *decoded.base(),
        decoded.index(inputRow),
        destination,
        written,
        mapIndices,
        decodedVectors);
    output.advance(row, written);
  }
}

template <RadixSortKeyLayoutKind KIND>
void encodeAndAppendInlineLayout(
    const std::vector<RadixSortKeyColumn>& columns,
    const RowVector& input,
    RadixSortRunStorage& storage,
    std::span<char* const> payloads,
    std::vector<uint64_t>& cursors) {
  using Traits = RadixSortKeyTraits<KIND>;
  static_assert(!Traits::kVariable);

  DecodedKeyVectors decodedInputs;
  storage.appendKeyBlocks(
      input.size(),
      [&](vector_size_t source, vector_size_t count, char* records) {
        std::memset(records, 0, static_cast<uint64_t>(count) * Traits::kWidth);
        cursors.resize(count);
        std::fill(cursors.begin(), cursors.end(), uint64_t{0});
        StridedCursorEncodeOutput output(
            records, Traits::kWidth, cursors.data());
        for (uint32_t column = 0; column < columns.size(); ++column) {
          encodeVariableColumn(
              columns[column],
              *input.childAt(column),
              source,
              count,
              output,
              &decodedInputs);
        }

        for (vector_size_t row = 0; row < count; ++row) {
          BOLT_DCHECK_LE(cursors[row], Traits::kInlineCapacity);
          auto* record = records + static_cast<uint64_t>(row) * Traits::kWidth;
          if constexpr (std::endian::native == std::endian::little) {
            for (uint32_t word = 0; word < Traits::kInlineWords; ++word) {
              auto value = loadUnaligned<uint64_t>(
                  record + static_cast<uint64_t>(word) * sizeof(uint64_t));
              storeUnaligned<uint64_t>(
                  record + static_cast<uint64_t>(word) * sizeof(uint64_t),
                  byteSwap(value));
            }
          }
          if constexpr (Traits::kHasPayload) {
            storeCompactPointer(
                record + Traits::kPayloadOffset, payloads[source + row]);
          }
        }
      });
}

} // namespace

class EncodedKeyReader {
 public:
  EncodedKeyReader(const char* data, uint64_t size)
      : data_(data), size_(size) {}

  static EncodedKeyReader checkedAt(std::string_view bytes, uint64_t position) {
    BOLT_CHECK_LE(position, bytes.size(), "Radix sort key input is truncated");
    EncodedKeyReader reader(bytes.data(), bytes.size());
    reader.position_ = position;
    return reader;
  }

  void readByte(uint8_t& value) {
    value = static_cast<uint8_t>(data_[position_++]);
  }

  void peekByte(uint8_t& value) const {
    value = static_cast<uint8_t>(data_[position_]);
  }

  void checkedReadByte(uint8_t& value) {
    require(1);
    readByte(value);
  }

  void checkedPeekByte(uint8_t& value) const {
    require(1);
    peekByte(value);
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

  void checkedSkip(uint64_t bytes) {
    require(bytes);
    skip(bytes);
  }

  uint64_t position() const {
    return position_;
  }

  const char* currentData() const {
    return data_ + position_;
  }

  uint64_t remaining() const {
    return size_ - position_;
  }

 private:
  void require(uint64_t bytes) const {
    BOLT_CHECK_LE(
        position_, size_, "Radix sort key reader position is out of bounds");
    BOLT_CHECK_LE(
        bytes, size_ - position_, "Radix sort key input is truncated");
  }

  const char* data_;
  uint64_t size_;
  uint64_t position_{0};
};

namespace {

template <typename T>
void decodeUnsigned(EncodedKeyReader& reader, bool descending, T& value) {
  static_assert(std::is_unsigned_v<T>);
  auto encoded = loadUnaligned<T>(reader.currentData());
  reader.skip(sizeof(T));
  if (descending) {
    encoded = static_cast<T>(~encoded);
  }
  value = fromBigEndian(encoded);
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

template <bool HostOrderWords>
uint8_t physicalEncodedByte(
    const char* key,
    uint32_t offset,
    uint32_t inlineWordBytes) {
  if constexpr (!HostOrderWords) {
    return static_cast<uint8_t>(key[offset]);
  }
  if (offset >= inlineWordBytes) {
    return static_cast<uint8_t>(key[offset]);
  }
  const auto word = loadUnaligned<uint64_t>(
      key + (offset / sizeof(uint64_t)) * sizeof(uint64_t));
  const auto shift = (sizeof(uint64_t) - 1 - offset % sizeof(uint64_t)) * 8;
  return static_cast<uint8_t>(word >> shift);
}

template <bool HostOrderWords, typename T>
FOLLY_ALWAYS_INLINE T decodePhysicalUnsigned(
    const char* key,
    bool descending,
    uint32_t offset,
    uint32_t inlineWordBytes) {
  static_assert(std::is_unsigned_v<T>);
  T value;
  if constexpr (HostOrderWords) {
    if (offset + sizeof(T) <= inlineWordBytes) {
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
      value = static_cast<T>(encoded);
    } else {
      BOLT_DCHECK_LT(offset, inlineWordBytes);
      const auto byteOffset = offset % sizeof(uint64_t);
      const auto wordBytes = sizeof(uint64_t) - byteOffset;
      const auto tailBytes = sizeof(T) - wordBytes;
      const auto first = loadUnaligned<uint64_t>(
          key + (offset / sizeof(uint64_t)) * sizeof(uint64_t));
      const auto firstMask =
          std::numeric_limits<uint64_t>::max() >> (byteOffset * 8);
      value = static_cast<T>((first & firstMask) << (tailBytes * 8));
      for (uint32_t byte = 0; byte < tailBytes; ++byte) {
        value = static_cast<T>(
            value |
            static_cast<T>(static_cast<uint8_t>(key[inlineWordBytes + byte]))
                << ((tailBytes - byte - 1) * 8));
      }
    }
  } else {
    value = fromBigEndian(loadUnaligned<T>(key + offset));
  }
  return descending ? static_cast<T>(~value) : value;
}

template <bool HostOrderWords, typename T>
FOLLY_ALWAYS_INLINE T decodePhysicalSigned(
    const char* key,
    bool descending,
    uint32_t offset,
    uint32_t inlineWordBytes) {
  static_assert(std::is_signed_v<T>);
  using Unsigned = std::make_unsigned_t<T>;
  auto bits = decodePhysicalUnsigned<HostOrderWords, Unsigned>(
      key, descending, offset, inlineWordBytes);
  bits ^= Unsigned{1} << (sizeof(T) * 8 - 1);
  T value;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

float decodeFloat(uint32_t input) {
  input =
      (input & (uint32_t{1} << 31)) != 0 ? input ^ (uint32_t{1} << 31) : ~input;
  return std::bit_cast<float>(input);
}

double decodeDouble(uint64_t input) {
  input =
      (input & (uint64_t{1} << 63)) != 0 ? input ^ (uint64_t{1} << 63) : ~input;
  return std::bit_cast<double>(input);
}

template <bool HostOrderWords, typename T, typename Decode>
void decodeSinglePhysicalColumn(
    const RadixSortKeyColumn& column,
    const RadixSortRunStorage& arena,
    uint64_t begin,
    vector_size_t count,
    bool mayHaveNulls,
    const VectorPtr& result,
    uint32_t encodedOffset,
    uint32_t inlineWordBytes,
    Decode decode) {
  auto* flat = result->asUnchecked<FlatVector<T>>();
  auto* values = flat->mutableRawValues();
  if (!mayHaveNulls) {
    result->resetNulls();
    vector_size_t outputRow = 0;
    while (outputRow < count) {
      const auto range = arena.keyRangeAt(begin + outputRow, count - outputRow);
      for (vector_size_t row = 0; row < range.count; ++row) {
        const auto* key =
            range.data + static_cast<uint64_t>(row) * arena.layout().width();
        values[outputRow + row] = decode(
            key, !column.flags.ascending, encodedOffset + 1, inlineWordBytes);
      }
      outputRow += range.count;
    }
    return;
  }
  const auto null = nullMarker(column.flags);
  auto* nulls =
      result->rawNulls() == nullptr ? nullptr : result->mutableRawNulls();
  if (nulls != nullptr) {
    bits::fillBits(nulls, 0, count, bits::kNotNull);
  }
  vector_size_t outputRow = 0;
  while (outputRow < count) {
    const auto range = arena.keyRangeAt(begin + outputRow, count - outputRow);
    for (vector_size_t row = 0; row < range.count; ++row) {
      const auto* key =
          range.data + static_cast<uint64_t>(row) * arena.layout().width();
      const auto marker = physicalEncodedByte<HostOrderWords>(
          key, encodedOffset, inlineWordBytes);
      if (marker == null) {
        if (nulls == nullptr) {
          nulls = result->mutableRawNulls();
        }
        bits::setNull(nulls, outputRow + row, true);
        continue;
      }
      values[outputRow + row] = decode(
          key, !column.flags.ascending, encodedOffset + 1, inlineWordBytes);
    }
    outputRow += range.count;
  }
}

template <bool HostOrderWords, typename T, typename Decode>
void decodeSinglePhysicalColumn(
    const RadixSortKeyColumn& column,
    std::span<const char* const> keys,
    bool mayHaveNulls,
    const VectorPtr& result,
    uint32_t encodedOffset,
    uint32_t inlineWordBytes,
    vector_size_t outputOffset,
    bool offsetWrite,
    Decode decode) {
  auto* flat = result->asUnchecked<FlatVector<T>>();
  auto* values = flat->mutableRawValues();
  values += outputOffset;
  if (!mayHaveNulls) {
    if (offsetWrite) {
      result->clearNulls(outputOffset, outputOffset + keys.size());
    } else {
      result->resetNulls();
    }
    for (vector_size_t row = 0; row < keys.size(); ++row) {
      values[row] = decode(
          keys[row],
          !column.flags.ascending,
          encodedOffset + 1,
          inlineWordBytes);
    }
    return;
  }
  const auto null = nullMarker(column.flags);
  auto* nulls =
      result->rawNulls() == nullptr ? nullptr : result->mutableRawNulls();
  if (nulls != nullptr) {
    bits::fillBits(
        nulls, outputOffset, outputOffset + keys.size(), bits::kNotNull);
  }
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    const auto* key = keys[row];
    const auto marker = physicalEncodedByte<HostOrderWords>(
        key, encodedOffset, inlineWordBytes);
    if (marker == null) {
      if (nulls == nullptr) {
        nulls = result->mutableRawNulls();
      }
      bits::setNull(nulls, outputOffset + row, true);
      continue;
    }
    values[row] = decode(
        key, !column.flags.ascending, encodedOffset + 1, inlineWordBytes);
  }
}

template <bool HostOrderWords>
void decodeSinglePhysicalBooleanColumn(
    const RadixSortKeyColumn& column,
    std::span<const char* const> keys,
    bool mayHaveNulls,
    const VectorPtr& result,
    uint32_t encodedOffset,
    uint32_t inlineWordBytes,
    vector_size_t outputOffset,
    bool offsetWrite) {
  auto* flat = result->asUnchecked<FlatVector<bool>>();
  auto* values = flat->template mutableRawValues<uint64_t>();
  if (!mayHaveNulls) {
    if (offsetWrite) {
      result->clearNulls(outputOffset, outputOffset + keys.size());
    } else {
      result->resetNulls();
    }
    for (vector_size_t row = 0; row < keys.size(); ++row) {
      auto value = physicalEncodedByte<HostOrderWords>(
          keys[row], encodedOffset + 1, inlineWordBytes);
      if (!column.flags.ascending) {
        value = static_cast<uint8_t>(~value);
      }
      bits::setBit(values, outputOffset + row, value != 0);
    }
    return;
  }
  const auto null = nullMarker(column.flags);
  auto* nulls =
      result->rawNulls() == nullptr ? nullptr : result->mutableRawNulls();
  if (nulls != nullptr) {
    bits::fillBits(
        nulls, outputOffset, outputOffset + keys.size(), bits::kNotNull);
  }
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    const auto* key = keys[row];
    const auto marker = physicalEncodedByte<HostOrderWords>(
        key, encodedOffset, inlineWordBytes);
    if (marker == null) {
      if (nulls == nullptr) {
        nulls = result->mutableRawNulls();
      }
      bits::setNull(nulls, outputOffset + row, true);
      continue;
    }
    auto value = physicalEncodedByte<HostOrderWords>(
        key, encodedOffset + 1, inlineWordBytes);
    if (!column.flags.ascending) {
      value = static_cast<uint8_t>(~value);
    }
    bits::setBit(values, outputOffset + row, value != 0);
  }
}

template <bool HostOrderWords>
void decodeSinglePhysicalBooleanColumn(
    const RadixSortKeyColumn& column,
    const RadixSortRunStorage& arena,
    uint64_t begin,
    vector_size_t count,
    bool mayHaveNulls,
    const VectorPtr& result,
    uint32_t encodedOffset,
    uint32_t inlineWordBytes) {
  auto* flat = result->asUnchecked<FlatVector<bool>>();
  auto* values = flat->template mutableRawValues<uint64_t>();
  if (!mayHaveNulls) {
    result->resetNulls();
    vector_size_t outputRow = 0;
    while (outputRow < count) {
      const auto range = arena.keyRangeAt(begin + outputRow, count - outputRow);
      for (vector_size_t row = 0; row < range.count; ++row) {
        const auto* key =
            range.data + static_cast<uint64_t>(row) * arena.layout().width();
        auto value = physicalEncodedByte<HostOrderWords>(
            key, encodedOffset + 1, inlineWordBytes);
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
  auto* nulls =
      result->rawNulls() == nullptr ? nullptr : result->mutableRawNulls();
  if (nulls != nullptr) {
    bits::fillBits(nulls, 0, count, bits::kNotNull);
  }
  vector_size_t outputRow = 0;
  while (outputRow < count) {
    const auto range = arena.keyRangeAt(begin + outputRow, count - outputRow);
    for (vector_size_t row = 0; row < range.count; ++row) {
      const auto* key =
          range.data + static_cast<uint64_t>(row) * arena.layout().width();
      const auto marker = physicalEncodedByte<HostOrderWords>(
          key, encodedOffset, inlineWordBytes);
      if (marker == null) {
        if (nulls == nullptr) {
          nulls = result->mutableRawNulls();
        }
        bits::setNull(nulls, outputRow + row, true);
        continue;
      }
      auto value = physicalEncodedByte<HostOrderWords>(
          key, encodedOffset + 1, inlineWordBytes);
      if (!column.flags.ascending) {
        value = static_cast<uint8_t>(~value);
      }
      bits::setBit(values, outputRow + row, value != 0);
    }
    outputRow += range.count;
  }
}

template <bool HostOrderWords, TypeKind KIND, typename Source, typename... Args>
void decodeSinglePhysicalColumnByKind(
    const RadixSortKeyColumn& column,
    const Source& source,
    bool mayHaveNulls,
    const VectorPtr& result,
    uint32_t encodedOffset,
    uint32_t inlineWordBytes,
    Args... args) {
  using T = typename TypeTraits<KIND>::NativeType;
  using SourceType = std::remove_cv_t<std::remove_reference_t<Source>>;
  constexpr bool kPointerSpan =
      std::is_same_v<SourceType, std::span<const char* const>>;
  const auto decode = [&](auto decodeValue) {
    if constexpr (kPointerSpan) {
      decodeSinglePhysicalColumn<HostOrderWords, T>(
          column,
          source,
          mayHaveNulls,
          result,
          encodedOffset,
          inlineWordBytes,
          args...,
          decodeValue);
    } else {
      decodeSinglePhysicalColumn<HostOrderWords, T>(
          column,
          source,
          args...,
          mayHaveNulls,
          result,
          encodedOffset,
          inlineWordBytes,
          decodeValue);
    }
  };
  if constexpr (KIND == TypeKind::BOOLEAN) {
    if constexpr (kPointerSpan) {
      decodeSinglePhysicalBooleanColumn<HostOrderWords>(
          column,
          source,
          mayHaveNulls,
          result,
          encodedOffset,
          inlineWordBytes,
          args...);
    } else {
      decodeSinglePhysicalBooleanColumn<HostOrderWords>(
          column,
          source,
          args...,
          mayHaveNulls,
          result,
          encodedOffset,
          inlineWordBytes);
    }
  } else if constexpr (
      KIND == TypeKind::TINYINT || KIND == TypeKind::SMALLINT ||
      KIND == TypeKind::INTEGER || KIND == TypeKind::BIGINT) {
    decode(
        [](const char* key, bool descending, uint32_t offset, uint32_t words) {
          return decodePhysicalSigned<HostOrderWords, T>(
              key, descending, offset, words);
        });
  } else if constexpr (KIND == TypeKind::HUGEINT) {
    decode(
        [](const char* key, bool descending, uint32_t offset, uint32_t words) {
          const auto upper = decodePhysicalSigned<HostOrderWords, int64_t>(
              key, descending, offset, words);
          const auto lower = decodePhysicalUnsigned<HostOrderWords, uint64_t>(
              key, descending, offset + sizeof(int64_t), words);
          return HugeInt::build(static_cast<uint64_t>(upper), lower);
        });
  } else if constexpr (KIND == TypeKind::REAL) {
    decode(
        [](const char* key, bool descending, uint32_t offset, uint32_t words) {
          return decodeFloat(decodePhysicalUnsigned<HostOrderWords, uint32_t>(
              key, descending, offset, words));
        });
  } else if constexpr (KIND == TypeKind::DOUBLE) {
    decode(
        [](const char* key, bool descending, uint32_t offset, uint32_t words) {
          return decodeDouble(decodePhysicalUnsigned<HostOrderWords, uint64_t>(
              key, descending, offset, words));
        });
  } else if constexpr (KIND == TypeKind::TIMESTAMP) {
    decode(
        [](const char* key, bool descending, uint32_t offset, uint32_t words) {
          return Timestamp(
              decodePhysicalSigned<HostOrderWords, int64_t>(
                  key, descending, offset, words),
              decodePhysicalUnsigned<HostOrderWords, uint64_t>(
                  key, descending, offset + sizeof(int64_t), words));
        });
  } else {
    BOLT_FAIL(
        "Single fixed radix sort key decode is not implemented for {}",
        column.type->toString());
  }
}

template <bool HostOrderWords>
void decodeSinglePhysicalColumn(
    const RadixSortKeyColumn& column,
    std::span<const char* const> keys,
    bool mayHaveNulls,
    const VectorPtr& result,
    uint32_t inlineWordBytes,
    uint32_t encodedOffset = 0,
    vector_size_t outputOffset = 0,
    bool offsetWrite = false) {
  const auto dispatch = [&]<TypeKind KIND>() {
    decodeSinglePhysicalColumnByKind<HostOrderWords, KIND>(
        column,
        keys,
        mayHaveNulls,
        result,
        encodedOffset,
        inlineWordBytes,
        outputOffset,
        offsetWrite);
  };
  BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      dispatch.template operator(), column.type->kind());
}

template <bool HostOrderWords>
void decodeSinglePhysicalColumn(
    const RadixSortKeyColumn& column,
    const RadixSortRunStorage& arena,
    uint64_t begin,
    vector_size_t count,
    bool mayHaveNulls,
    const VectorPtr& result,
    uint32_t inlineWordBytes,
    uint32_t encodedOffset = 0) {
  const auto dispatch = [&]<TypeKind KIND>() {
    decodeSinglePhysicalColumnByKind<HostOrderWords, KIND>(
        column,
        arena,
        mayHaveNulls,
        result,
        encodedOffset,
        inlineWordBytes,
        begin,
        count);
  };
  BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      dispatch.template operator(), column.type->kind());
}

template <typename DecodeColumn>
void decodeFixedPrefixColumns(
    const std::vector<RadixSortKeyColumn>& columns,
    uint32_t prefixColumnCount,
    std::span<const uint8_t> decodedColumns,
    std::span<const uint8_t> mayHaveNulls,
    DecodeColumn decodeColumn) {
  for (uint32_t column = 0; column < prefixColumnCount; ++column) {
    if (decodedColumns.empty() || decodedColumns[column] != 0) {
      decodeColumn(
          column,
          *columns[column].fixedPrefixOffset,
          mayHaveNulls.empty() || mayHaveNulls[column] != 0);
    }
  }
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

void scanStringBody(
    const char* body,
    uint64_t remaining,
    bool descending,
    uint64_t& decodedSize,
    uint64_t& encodedSize);

template <bool Descending>
void writeDecodedString(
    const char* body,
    uint64_t encodedSize,
    uint64_t decodedSize,
    char* destination);

char* reserveNestedStringBytes(
    FlatVector<StringView>& result,
    uint64_t bytes,
    vector_size_t remainingRows) {
  const auto& buffers = result.stringBuffers();
  auto* buffer = buffers.empty() ? nullptr : buffers.back().get();
  if (buffer == nullptr || !buffer->unique() ||
      bytes > buffer->capacity() - buffer->size()) {
    // Start small for sparse/wide schemas. Grow only when the previous buffer
    // fills, with a bounded tail and no allocator-bucket padding overflow.
    constexpr uint64_t kInitialBytes = 256 - AlignedBuffer::kPaddedSize;
    constexpr uint64_t kMaxBytes = 32 * 1024 - AlignedBuffer::kPaddedSize;
    const auto previous = buffer == nullptr
        ? uint64_t{0}
        : std::min<uint64_t>(buffer->capacity(), kMaxBytes);
    auto growth = std::min(kMaxBytes, std::max(kInitialBytes, 2 * previous));
    if (remainingRows > 0) {
      // ROW children have a known final size. Avoid a large last buffer when
      // only a few rows remain; unknown-length ARRAY/MAP children keep growing.
      const auto estimate = checkedMultiply<uint64_t>(bytes, remainingRows);
      growth = std::min(growth, estimate.value_or(kMaxBytes));
    }
    buffer = result.getBufferWithSpace(std::max(bytes, growth), true);
  }
  auto* data = buffer->asMutable<char>() + buffer->size();
  buffer->setSize(buffer->size() + bytes);
  return data;
}

void decodeString(
    EncodedKeyReader& reader,
    bool descending,
    const VectorPtr& result,
    vector_size_t row,
    vector_size_t endRow) {
  const auto* body = reader.currentData();
  uint64_t decodedSize;
  uint64_t encodedSize;
  scanStringBody(
      body, reader.remaining(), descending, decodedSize, encodedSize);
  auto* flatResult = result->asUnchecked<FlatVector<StringView>>();
  std::array<char, StringView::kInlineSize> inlineData{};
  char* output = decodedSize <= inlineData.size()
      ? inlineData.data()
      : reserveNestedStringBytes(
            *flatResult, decodedSize, endRow == 0 ? 0 : endRow - row);
  if (descending) {
    writeDecodedString<true>(body, encodedSize, decodedSize, output);
  } else {
    writeDecodedString<false>(body, encodedSize, decodedSize, output);
  }
  reader.skip(encodedSize);
  flatResult->setNoCopy(
      row, StringView(output, static_cast<int32_t>(decodedSize)));
}

template <TypeKind KIND>
FOLLY_ALWAYS_INLINE typename TypeTraits<KIND>::NativeType
decodeFixedScalarValue(EncodedKeyReader& input, bool descending) {
  using T = typename TypeTraits<KIND>::NativeType;
  if constexpr (KIND == TypeKind::BOOLEAN) {
    uint8_t value;
    input.readBodyByte(descending, value);
    return value != 0;
  } else if constexpr (
      KIND == TypeKind::TINYINT || KIND == TypeKind::SMALLINT ||
      KIND == TypeKind::INTEGER || KIND == TypeKind::BIGINT) {
    T value;
    decodeSigned(input, descending, value);
    return value;
  } else if constexpr (KIND == TypeKind::HUGEINT) {
    int64_t upper;
    uint64_t lower;
    decodeSigned(input, descending, upper);
    decodeUnsigned(input, descending, lower);
    return HugeInt::build(static_cast<uint64_t>(upper), lower);
  } else if constexpr (KIND == TypeKind::REAL) {
    uint32_t value;
    decodeUnsigned(input, descending, value);
    return decodeFloat(value);
  } else if constexpr (KIND == TypeKind::DOUBLE) {
    uint64_t value;
    decodeUnsigned(input, descending, value);
    return decodeDouble(value);
  } else if constexpr (KIND == TypeKind::TIMESTAMP) {
    int64_t seconds;
    uint64_t nanos;
    decodeSigned(input, descending, seconds);
    decodeUnsigned(input, descending, nanos);
    return Timestamp(seconds, nanos);
  } else {
    BOLT_FAIL(
        "Fixed radix sort key decoding is not implemented for {}",
        TypeTraits<KIND>::name);
  }
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
  const auto bodySize = *fixedBodySize(*column.type);
  auto scan = reader;
  vector_size_t count = 0;
  while (true) {
    uint8_t next;
    scan.checkedPeekByte(next);
    if (next == encodedDelimiter) {
      break;
    }
    scan.checkedReadByte(next);
    if (next != null) {
      scan.checkedSkip(bodySize);
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
  reader.checkedReadByte(delimiter);
}

template <TypeKind KIND>
void decodeFixedScalarArrayElementsByKind(
    const RadixSortKeyColumn& column,
    EncodedKeyReader& reader,
    const VectorPtr& result,
    vector_size_t start) {
  if constexpr (
      KIND == TypeKind::BOOLEAN || KIND == TypeKind::TINYINT ||
      KIND == TypeKind::SMALLINT || KIND == TypeKind::INTEGER ||
      KIND == TypeKind::BIGINT || KIND == TypeKind::HUGEINT ||
      KIND == TypeKind::REAL || KIND == TypeKind::DOUBLE ||
      KIND == TypeKind::TIMESTAMP) {
    using T = typename TypeTraits<KIND>::NativeType;
    decodeFixedScalarArrayElements<T>(
        column, reader, result, start, decodeFixedScalarValue<KIND>);
  } else {
    BOLT_FAIL(
        "Radix sort fixed array element decoding is not implemented for {}",
        column.type->toString());
  }
}

void decodeFixedScalarArrayElements(
    const RadixSortKeyColumn& column,
    EncodedKeyReader& reader,
    const VectorPtr& result,
    vector_size_t start) {
  BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      decodeFixedScalarArrayElementsByKind,
      column.type->kind(),
      column,
      reader,
      result,
      start);
}

template <TypeKind KIND>
void decodeScalarValue(
    EncodedKeyReader& reader,
    bool descending,
    const VectorPtr& result,
    vector_size_t row,
    vector_size_t endRow) {
  if constexpr (
      KIND == TypeKind::BOOLEAN || KIND == TypeKind::TINYINT ||
      KIND == TypeKind::SMALLINT || KIND == TypeKind::INTEGER ||
      KIND == TypeKind::BIGINT || KIND == TypeKind::HUGEINT ||
      KIND == TypeKind::REAL || KIND == TypeKind::DOUBLE ||
      KIND == TypeKind::TIMESTAMP) {
    using T = typename TypeTraits<KIND>::NativeType;
    setValue<T>(result, row, decodeFixedScalarValue<KIND>(reader, descending));
  } else if constexpr (
      KIND == TypeKind::VARCHAR || KIND == TypeKind::VARBINARY) {
    decodeString(reader, descending, result, row, endRow);
  } else {
    BOLT_FAIL(
        "Radix sort key decoding is not implemented for {}",
        TypeTraits<KIND>::name);
  }
}

void decodeValue(
    const RadixSortKeyColumn& column,
    EncodedKeyReader& reader,
    const VectorPtr& result,
    vector_size_t row,
    vector_size_t endRow = 0) {
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
          column.children[child],
          reader,
          rowResult->childAt(child),
          row,
          endRow);
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
    const bool fixedElements = isFixedScalarColumn(column.children[0]);
    while (true) {
      uint8_t next;
      reader.peekByte(next);
      if (next == encodedDelimiter) {
        reader.readByte(next);
        break;
      }
      if (fixedElements) {
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
    if (isFixedScalarColumn(column.children[0])) {
      decodeFixedScalarArrayElements(
          column.children[0], reader, mapResult->mapKeys(), start);
      count = mapResult->mapKeys()->size() - start;
    } else {
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
    }
    if (isFixedScalarColumn(column.children[1])) {
      decodeFixedScalarArrayElements(
          column.children[1], reader, mapResult->mapValues(), start);
      BOLT_CHECK_EQ(
          mapResult->mapValues()->size(),
          start + count,
          "Radix sort encoded map key and value counts differ");
    } else {
      mapResult->mapValues()->resize(start + count);
      for (vector_size_t index = 0; index < count; ++index) {
        decodeValue(
            column.children[1], reader, mapResult->mapValues(), start + index);
      }
      uint8_t delimiter;
      reader.readByte(delimiter);
    }
    mapResult->setOffsetAndSize(row, start, count);
    return;
  }
  if (column.type->isPrimitiveType() &&
      column.type->kind() != TypeKind::UNKNOWN) {
    BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
        decodeScalarValue,
        column.type->kind(),
        reader,
        descending,
        result,
        row,
        endRow);
    return;
  }
  BOLT_FAIL(
      "Radix sort key decoding is not implemented for {}",
      column.type->toString());
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
    return decodedColumns.empty() || decodedColumns[column] != 0;
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

void clearDestinationNulls(
    const VectorPtr& result,
    vector_size_t outputOffset,
    vector_size_t size) {
  if (outputOffset == 0 && size == result->size()) {
    result->resetNulls();
  } else {
    result->clearNulls(outputOffset, outputOffset + size);
  }
}

void prepareDecodeScratch(
    vector_size_t size,
    memory::MemoryPool* pool,
    uint64_t wordsPerRow,
    BufferPtr& cursorScratch,
    DecodeScratch& scratch) {
  const auto wordCount = checkedMultiply<uint64_t>(size, wordsPerRow);
  BOLT_CHECK(
      wordCount.has_value(), "Radix sort decode scratch word count overflows");
  const auto bytes = checkedMultiply<uint64_t>(*wordCount, sizeof(uint64_t));
  BOLT_CHECK(
      bytes.has_value(), "Radix sort decode scratch byte size overflows");
  if (cursorScratch == nullptr || cursorScratch->pool() != pool) {
    cursorScratch.reset();
    cursorScratch = AlignedBuffer::allocate<uint64_t>(*wordCount, pool);
  } else if (cursorScratch->capacity() < *bytes) {
    cursorScratch.reset();
    cursorScratch = AlignedBuffer::allocate<uint64_t>(*wordCount, pool);
  } else {
    cursorScratch->setSize(*bytes);
  }
  scratch.size = size;
  scratch.words = cursorScratch->asMutable<uint64_t>();
  scratch.cursors = scratch.words;
}

void bindPreparedDecodeScratch(
    vector_size_t size,
    memory::MemoryPool* pool,
    uint64_t wordsPerRow,
    const BufferPtr& cursorScratch,
    DecodeScratch& scratch) {
  BOLT_DCHECK_NOT_NULL(pool);
  BOLT_DCHECK_NOT_NULL(cursorScratch);
  BOLT_DCHECK(cursorScratch->pool() == pool);
  BOLT_DCHECK_GT(wordsPerRow, 0);
  BOLT_DCHECK_LE(
      static_cast<uint64_t>(size),
      cursorScratch->size() / sizeof(uint64_t) / wordsPerRow);
  scratch.size = size;
  scratch.words = cursorScratch->asMutable<uint64_t>();
  scratch.cursors = scratch.words;
}

bool isStringColumn(const RadixSortKeyColumn& column) {
  return column.type->kind() == TypeKind::VARCHAR ||
      column.type->kind() == TypeKind::VARBINARY;
}

uint64_t calculateDecodeScratchWordsPerRow(
    const std::vector<RadixSortKeyColumn>& columns,
    std::span<const uint8_t> decodedColumns,
    std::span<const uint8_t> mayHaveNulls,
    uint32_t firstColumn,
    uint32_t endColumn) {
  BOLT_DCHECK_LT(firstColumn, endColumn);
  BOLT_DCHECK_LE(endColumn, columns.size());
  uint64_t extraBlocks = 0;
  for (uint32_t column = firstColumn; column < endColumn; ++column) {
    const bool masked = !decodedColumns.empty() && decodedColumns[column] == 0;
    if (masked) {
      continue;
    }
    const bool columnMayHaveNulls =
        mayHaveNulls.empty() || mayHaveNulls[column] != 0;
    if (isFixedScalarColumn(columns[column])) {
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
    uint64_t*& bodyPointers,
    vector_size_t outputOffset) {
  const auto size = static_cast<vector_size_t>(keys.size());
  auto* cursors = scratch.cursors;
  validRows = MayHaveNulls ? scratch.block(0) : nullptr;
  bodyPointers = scratch.block(MayHaveNulls ? 1 : 0);

  uint64_t validCount = 0;
  if constexpr (!MayHaveNulls) {
    clearDestinationNulls(result, outputOffset, size);
  } else {
    auto* nulls =
        result->rawNulls() == nullptr ? nullptr : result->mutableRawNulls();
    if (nulls != nullptr) {
      bits::fillBits(nulls, outputOffset, outputOffset + size, bits::kNotNull);
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
        bits::setNull(nulls, outputOffset + row, true);
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

template <bool MayHaveNulls>
FOLLY_ALWAYS_INLINE vector_size_t outputRowWithOffset(
    uint64_t index,
    uint64_t* validRows,
    vector_size_t outputOffset) {
  return outputOffset + outputRow<MayHaveNulls>(index, validRows);
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
    const VectorPtr& result,
    vector_size_t outputOffset) {
  using Encoded = typename Decoder::Encoded;
  auto* flat = result->asUnchecked<FlatVector<T>>();
  auto* values = flat->mutableRawValues();
  if (outputOffset != 0) {
    values += outputOffset;
  }
  uint64_t* validRows;
  uint64_t* encoded;
  const auto count = prepareFixedBodyPointers<FirstColumn, MayHaveNulls>(
      column,
      keys,
      sizeof(Encoded),
      result,
      scratch,
      validRows,
      encoded,
      outputOffset);
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
    const VectorPtr& result,
    vector_size_t outputOffset) {
  auto* flat = result->asUnchecked<FlatVector<bool>>();
  auto* values = flat->template mutableRawValues<uint64_t>();
  uint64_t* validRows;
  uint64_t* encoded;
  const auto count = prepareFixedBodyPointers<FirstColumn, MayHaveNulls>(
      column, keys, 1, result, scratch, validRows, encoded, outputOffset);
  compilerMemoryBarrier();
  loadFixedBodyWord<uint8_t>(count, encoded);
  compilerMemoryBarrier();
  for (uint64_t index = 0; index < count; ++index) {
    const auto value = decodeUnsignedEncoded<Descending, uint8_t>(
        static_cast<uint8_t>(encoded[index]));
    bits::setBit(
        values,
        outputRowWithOffset<MayHaveNulls>(index, validRows, outputOffset),
        value != 0);
  }
}

template <bool FirstColumn, bool MayHaveNulls, bool Descending>
__attribute__((noinline)) void decodeInt128Layered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result,
    vector_size_t outputOffset) {
  auto* flat = result->asUnchecked<FlatVector<int128_t>>();
  auto* values = flat->mutableRawValues();
  if (outputOffset != 0) {
    values += outputOffset;
  }
  uint64_t* validRows;
  uint64_t* upperEncoded;
  const auto count = prepareFixedBodyPointers<FirstColumn, MayHaveNulls>(
      column,
      keys,
      sizeof(int128_t),
      result,
      scratch,
      validRows,
      upperEncoded,
      outputOffset);
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
    const VectorPtr& result,
    vector_size_t outputOffset) {
  auto* flat = result->asUnchecked<FlatVector<Timestamp>>();
  auto* values = flat->mutableRawValues();
  if (outputOffset != 0) {
    values += outputOffset;
  }
  uint64_t* validRows;
  uint64_t* secondsEncoded;
  const auto count = prepareFixedBodyPointers<FirstColumn, MayHaveNulls>(
      column,
      keys,
      sizeof(int64_t) + sizeof(uint64_t),
      result,
      scratch,
      validRows,
      secondsEncoded,
      outputOffset);
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
    const VectorPtr& result,
    vector_size_t outputOffset = 0) {
  dispatchDescending(column, [&]<bool Descending>() {
    decodeWordLayered<
        FirstColumn,
        MayHaveNulls,
        Descending,
        T,
        SignedFixedDecoder<T>>(column, keys, scratch, result, outputOffset);
  });
}

template <bool FirstColumn, bool MayHaveNulls>
void decodeBooleanLayered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result,
    vector_size_t outputOffset = 0) {
  dispatchDescending(column, [&]<bool Descending>() {
    decodeBooleanLayered<FirstColumn, MayHaveNulls, Descending>(
        column, keys, scratch, result, outputOffset);
  });
}

template <bool FirstColumn, bool MayHaveNulls>
void decodeFloatLayered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result,
    vector_size_t outputOffset = 0) {
  dispatchDescending(column, [&]<bool Descending>() {
    decodeWordLayered<
        FirstColumn,
        MayHaveNulls,
        Descending,
        float,
        FloatFixedDecoder>(column, keys, scratch, result, outputOffset);
  });
}

template <bool FirstColumn, bool MayHaveNulls>
void decodeDoubleLayered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result,
    vector_size_t outputOffset = 0) {
  dispatchDescending(column, [&]<bool Descending>() {
    decodeWordLayered<
        FirstColumn,
        MayHaveNulls,
        Descending,
        double,
        DoubleFixedDecoder>(column, keys, scratch, result, outputOffset);
  });
}

template <bool FirstColumn, bool MayHaveNulls>
void decodeInt128Layered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result,
    vector_size_t outputOffset = 0) {
  dispatchDescending(column, [&]<bool Descending>() {
    decodeInt128Layered<FirstColumn, MayHaveNulls, Descending>(
        column, keys, scratch, result, outputOffset);
  });
}

template <bool FirstColumn, bool MayHaveNulls>
void decodeTimestampLayered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result,
    vector_size_t outputOffset = 0) {
  dispatchDescending(column, [&]<bool Descending>() {
    decodeTimestampLayered<FirstColumn, MayHaveNulls, Descending>(
        column, keys, scratch, result, outputOffset);
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

template <bool FirstColumn>
void skipCheckedFixedColumn(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    uint64_t* cursors) {
  const auto bodySize = *fixedBodySize(*column.type);
  const auto null = nullMarker(column.flags);
  const auto valid = validMarker(column.flags);
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    const auto cursor = FirstColumn ? uint64_t{0} : cursors[row];
    auto reader = EncodedKeyReader::checkedAt(keys[row].bytes, cursor);
    uint8_t marker;
    reader.checkedReadByte(marker);
    BOLT_CHECK(
        marker == null || marker == valid,
        "Invalid radix sort key marker while skipping fixed column");
    if (marker == valid) {
      reader.checkedSkip(bodySize);
    }
    cursors[row] = reader.position();
  }
}

void skipStringBody(EncodedKeyReader& reader, bool descending) {
  const auto delimiter =
      descending ? static_cast<uint8_t>(~kStringDelimiter) : kStringDelimiter;
  const auto escape =
      descending ? static_cast<uint8_t>(~kBlobEscape) : kBlobEscape;
  while (true) {
    uint8_t byte;
    reader.checkedReadByte(byte);
    if (byte == delimiter) {
      return;
    }
    if (byte == escape) {
      reader.checkedReadByte(byte);
    }
  }
}

void skipValue(const RadixSortKeyColumn& column, EncodedKeyReader& reader) {
  uint8_t marker;
  reader.checkedReadByte(marker);
  const auto null = nullMarker(column.flags);
  const auto valid = validMarker(column.flags);
  BOLT_CHECK(
      marker == null || marker == valid,
      "Invalid radix sort key marker while skipping {}",
      column.type->toString());
  if (marker == null) {
    return;
  }
  BOLT_CHECK_NE(
      column.type->kind(),
      TypeKind::UNKNOWN,
      "UNKNOWN radix sort key must use the null marker");

  const bool descending = !column.flags.ascending;
  if (const auto bodySize = fixedBodySize(*column.type)) {
    reader.checkedSkip(*bodySize);
    return;
  }
  switch (column.type->kind()) {
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      skipStringBody(reader, descending);
      return;
    case TypeKind::ROW:
      for (const auto& child : column.children) {
        skipValue(child, reader);
      }
      return;
    case TypeKind::ARRAY: {
      const auto delimiter = descending
          ? static_cast<uint8_t>(~kStringDelimiter)
          : kStringDelimiter;
      while (true) {
        uint8_t next;
        reader.checkedPeekByte(next);
        if (next == delimiter) {
          reader.checkedReadByte(next);
          return;
        }
        skipValue(column.children[0], reader);
      }
    }
    case TypeKind::MAP: {
      const auto delimiter = descending
          ? static_cast<uint8_t>(~kStringDelimiter)
          : kStringDelimiter;
      uint64_t count = 0;
      while (true) {
        uint8_t next;
        reader.checkedPeekByte(next);
        if (next == delimiter) {
          reader.checkedReadByte(next);
          break;
        }
        skipValue(column.children[0], reader);
        BOLT_CHECK_LT(
            count,
            std::numeric_limits<uint64_t>::max(),
            "Radix sort encoded map entry count overflows");
        ++count;
      }
      for (uint64_t index = 0; index < count; ++index) {
        skipValue(column.children[1], reader);
      }
      uint8_t finalDelimiter;
      reader.checkedReadByte(finalDelimiter);
      BOLT_CHECK_EQ(
          finalDelimiter,
          delimiter,
          "Invalid radix sort encoded map delimiter");
      return;
    }
    case TypeKind::UNKNOWN:
      BOLT_UNREACHABLE();
    default:
      BOLT_FAIL(
          "Radix sort key skip is not implemented for {}",
          column.type->toString());
  }
}

bool encodedSpecialValues(const RadixSortKeyColumn& column) {
  if (!column.containsFloatingPoint) {
    return false;
  }
  return column.hasSpecialValues ||
      std::any_of(
             column.children.begin(),
             column.children.end(),
             encodedSpecialValues);
}

void mergeSpecialValues(
    RadixSortKeyColumn& target,
    std::span<const uint8_t> flags,
    uint32_t& index) {
  BOLT_CHECK_LT(index, flags.size());
  target.hasSpecialValues |= flags[index++] != 0;
  for (auto& child : target.children) {
    mergeSpecialValues(child, flags, index);
  }
}

void appendSpecialValueFlags(
    const RadixSortKeyColumn& column,
    std::vector<uint8_t>& flags) {
  flags.push_back(column.hasSpecialValues);
  for (const auto& child : column.children) {
    appendSpecialValueFlags(child, flags);
  }
}

int32_t compareKeyBytes(std::string_view left, std::string_view right) {
  const auto common = std::min(left.size(), right.size());
  const auto result =
      common == 0 ? 0 : std::memcmp(left.data(), right.data(), common);
  return result == 0
      ? (left.size() > right.size()) - (left.size() < right.size())
      : (result > 0) - (result < 0);
}

template <typename T>
int32_t comparePhysicalUnsigned(
    const char* left,
    const char* right,
    uint32_t offset,
    uint32_t inlineWordBytes) {
  const auto a = loadEncodedUnsigned<true, T>(left, offset, inlineWordBytes);
  const auto b = loadEncodedUnsigned<true, T>(right, offset, inlineWordBytes);
  return (a > b) - (a < b);
}

int32_t comparePhysicalBytes(
    const char* left,
    const char* right,
    uint32_t offset,
    uint32_t size,
    uint32_t inlineWordBytes) {
  while (size >= sizeof(uint64_t)) {
    const auto result =
        comparePhysicalUnsigned<uint64_t>(left, right, offset, inlineWordBytes);
    if (result != 0) {
      return result;
    }
    offset += sizeof(uint64_t);
    size -= sizeof(uint64_t);
  }
  if (size >= sizeof(uint32_t)) {
    const auto result =
        comparePhysicalUnsigned<uint32_t>(left, right, offset, inlineWordBytes);
    if (result != 0) {
      return result;
    }
    offset += sizeof(uint32_t);
    size -= sizeof(uint32_t);
  }
  if (size >= sizeof(uint16_t)) {
    const auto result =
        comparePhysicalUnsigned<uint16_t>(left, right, offset, inlineWordBytes);
    if (result != 0) {
      return result;
    }
    offset += sizeof(uint16_t);
    size -= sizeof(uint16_t);
  }
  return size == 0
      ? 0
      : comparePhysicalUnsigned<uint8_t>(left, right, offset, inlineWordBytes);
}

template <typename T>
int32_t
compareFloatingPointBody(const char* left, const char* right, bool descending) {
  const auto a = normalizeFloatingPointKey(
      fromBigEndian(loadUnaligned<T>(left)), descending);
  const auto b = normalizeFloatingPointKey(
      fromBigEndian(loadUnaligned<T>(right)), descending);
  return (a > b) - (a < b);
}

int32_t compareFixedBytes(
    const RadixSortKeyColumn& column,
    const char* left,
    const char* right,
    uint32_t offset,
    uint32_t inlineWordBytes) {
  return comparePhysicalBytes(
      left,
      right,
      offset,
      static_cast<uint32_t>(*column.maximumEncodedSize - 1),
      inlineWordBytes);
}

int32_t compareVariableBytes(
    const RadixSortKeyColumn& column,
    const char* left,
    const char* right,
    uint32_t offset,
    uint32_t /*inlineWordBytes*/) {
  const auto result = std::memcmp(
      left + offset, right + offset, *column.maximumEncodedSize - 1);
  return (result > 0) - (result < 0);
}

int32_t compareFixedFloat(
    const RadixSortKeyColumn& column,
    const char* left,
    const char* right,
    uint32_t offset,
    uint32_t inlineWordBytes) {
  const auto x = normalizeFloatingPointKey(
      loadEncodedUnsigned<true, uint32_t>(left, offset, inlineWordBytes),
      !column.flags.ascending);
  const auto y = normalizeFloatingPointKey(
      loadEncodedUnsigned<true, uint32_t>(right, offset, inlineWordBytes),
      !column.flags.ascending);
  return (x > y) - (x < y);
}

int32_t compareFixedDouble(
    const RadixSortKeyColumn& column,
    const char* left,
    const char* right,
    uint32_t offset,
    uint32_t inlineWordBytes) {
  const auto x = normalizeFloatingPointKey(
      loadEncodedUnsigned<true, uint64_t>(left, offset, inlineWordBytes),
      !column.flags.ascending);
  const auto y = normalizeFloatingPointKey(
      loadEncodedUnsigned<true, uint64_t>(right, offset, inlineWordBytes),
      !column.flags.ascending);
  return (x > y) - (x < y);
}

int32_t compareVariableFloat(
    const RadixSortKeyColumn& column,
    const char* left,
    const char* right,
    uint32_t offset,
    uint32_t /*inlineWordBytes*/) {
  return compareFloatingPointBody<uint32_t>(
      left + offset, right + offset, !column.flags.ascending);
}

int32_t compareVariableDouble(
    const RadixSortKeyColumn& column,
    const char* left,
    const char* right,
    uint32_t offset,
    uint32_t /*inlineWordBytes*/) {
  return compareFloatingPointBody<uint64_t>(
      left + offset, right + offset, !column.flags.ascending);
}

int32_t compareEncodedBytes(
    const RadixSortKeyColumn& column,
    EncodedKeyReader& left,
    EncodedKeyReader& right) {
  const auto* a = left.currentData();
  const auto* b = right.currentData();
  skipValue(column, left);
  skipValue(column, right);
  return compareKeyBytes(
      {a, static_cast<size_t>(left.currentData() - a)},
      {b, static_cast<size_t>(right.currentData() - b)});
}

template <typename T>
int32_t compareEncodedFloating(
    const RadixSortKeyColumn& column,
    EncodedKeyReader& left,
    EncodedKeyReader& right) {
  uint8_t a;
  uint8_t b;
  left.checkedReadByte(a);
  right.checkedReadByte(b);
  if (a != b) {
    return (a > b) - (a < b);
  }
  if (a == nullMarker(column.flags)) {
    return 0;
  }
  BOLT_CHECK_EQ(a, validMarker(column.flags), "Invalid radix sort key marker");
  const auto* aBody = left.currentData();
  const auto* bBody = right.currentData();
  left.checkedSkip(sizeof(T));
  right.checkedSkip(sizeof(T));
  return compareFloatingPointBody<T>(aBody, bBody, !column.flags.ascending);
}

int32_t compareEncodedRow(
    const RadixSortKeyColumn& column,
    EncodedKeyReader& left,
    EncodedKeyReader& right) {
  uint8_t a;
  uint8_t b;
  left.checkedReadByte(a);
  right.checkedReadByte(b);
  if (a != b) {
    return (a > b) - (a < b);
  }
  if (a == nullMarker(column.flags)) {
    return 0;
  }
  BOLT_CHECK_EQ(a, validMarker(column.flags), "Invalid radix sort key marker");
  for (const auto& child : column.children) {
    const auto result = child.encodedComparator(child, left, right);
    if (result != 0) {
      return result;
    }
  }
  return 0;
}

int32_t compareEncodedArray(
    const RadixSortKeyColumn& column,
    EncodedKeyReader& left,
    EncodedKeyReader& right) {
  uint8_t a;
  uint8_t b;
  left.checkedReadByte(a);
  right.checkedReadByte(b);
  if (a != b) {
    return (a > b) - (a < b);
  }
  if (a == nullMarker(column.flags)) {
    return 0;
  }
  BOLT_CHECK_EQ(a, validMarker(column.flags), "Invalid radix sort key marker");
  const uint8_t delimiter = column.flags.ascending ? uint8_t{0} : uint8_t{255};
  const auto& child = column.children[0];
  while (true) {
    left.checkedPeekByte(a);
    right.checkedPeekByte(b);
    if (a == delimiter || b == delimiter) {
      if (a != b) {
        return (a > b) - (a < b);
      }
      left.skip(1);
      right.skip(1);
      return 0;
    }
    const auto result = child.encodedComparator(child, left, right);
    if (result != 0) {
      return result;
    }
  }
}

int32_t compareEncodedMap(
    const RadixSortKeyColumn& column,
    EncodedKeyReader& left,
    EncodedKeyReader& right) {
  uint8_t a;
  uint8_t b;
  left.checkedReadByte(a);
  right.checkedReadByte(b);
  if (a != b) {
    return (a > b) - (a < b);
  }
  if (a == nullMarker(column.flags)) {
    return 0;
  }
  BOLT_CHECK_EQ(a, validMarker(column.flags), "Invalid radix sort key marker");
  const uint8_t delimiter = column.flags.ascending ? uint8_t{0} : uint8_t{255};
  uint64_t count = 0;
  const auto& key = column.children[0];
  const auto& value = column.children[1];
  while (true) {
    left.checkedPeekByte(a);
    right.checkedPeekByte(b);
    if (a == delimiter || b == delimiter) {
      if (a != b) {
        return (a > b) - (a < b);
      }
      left.skip(1);
      right.skip(1);
      break;
    }
    const auto result = key.encodedComparator(key, left, right);
    if (result != 0) {
      return result;
    }
    ++count;
  }
  for (uint64_t index = 0; index < count; ++index) {
    const auto result = value.encodedComparator(value, left, right);
    if (result != 0) {
      return result;
    }
  }
  left.checkedReadByte(a);
  right.checkedReadByte(b);
  BOLT_CHECK_EQ(a, delimiter, "Invalid radix sort encoded map delimiter");
  BOLT_CHECK_EQ(b, delimiter, "Invalid radix sort encoded map delimiter");
  return 0;
}

void prepareColumnComparators(const RadixSortKeyColumn& column) {
  column.fixedComparator = compareFixedBytes;
  column.variableComparator = compareVariableBytes;
  column.encodedComparator = compareEncodedBytes;
  for (const auto& child : column.children) {
    prepareColumnComparators(child);
  }
  if (!encodedSpecialValues(column)) {
    return;
  }
  switch (column.type->kind()) {
    case TypeKind::REAL:
      column.fixedComparator = compareFixedFloat;
      column.variableComparator = compareVariableFloat;
      column.encodedComparator = compareEncodedFloating<uint32_t>;
      return;
    case TypeKind::DOUBLE:
      column.fixedComparator = compareFixedDouble;
      column.variableComparator = compareVariableDouble;
      column.encodedComparator = compareEncodedFloating<uint64_t>;
      return;
    case TypeKind::ROW:
      column.encodedComparator = compareEncodedRow;
      return;
    case TypeKind::ARRAY:
      column.encodedComparator = compareEncodedArray;
      return;
    case TypeKind::MAP:
      column.encodedComparator = compareEncodedMap;
      return;
    default:
      return;
  }
}

template <bool FirstColumn>
void skipColumn(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    uint64_t* cursors) {
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    const auto cursor = FirstColumn ? uint64_t{0} : cursors[row];
    auto reader = EncodedKeyReader::checkedAt(keys[row].bytes, cursor);
    skipValue(column, reader);
    cursors[row] = reader.position();
  }
}

template <bool FirstColumn>
void decodeCheckedUnknownColumn(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    uint64_t* cursors,
    const VectorPtr& result,
    vector_size_t outputOffset) {
  const auto null = nullMarker(column.flags);
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    const auto cursor = FirstColumn ? uint64_t{0} : cursors[row];
    auto reader = EncodedKeyReader::checkedAt(keys[row].bytes, cursor);
    uint8_t marker;
    reader.checkedReadByte(marker);
    BOLT_CHECK_EQ(marker, null, "UNKNOWN radix sort key must use null marker");
    cursors[row] = reader.position();
    result->setNull(outputOffset + row, true);
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
    const auto* escape = static_cast<const uint8_t*>(
        std::memchr(data, encodedEscape, remaining));
    if (delimiter != nullptr && (escape == nullptr || delimiter < escape)) {
      decodedSize = static_cast<uint64_t>(delimiter - data);
      encodedSize = decodedSize + 1;
      return;
    }
    if (escape != nullptr) {
      cursor = static_cast<uint64_t>(escape - data);
      decodedSize = cursor;
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
      BOLT_CHECK_LT(cursor, remaining, "Radix sort key input is truncated");
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
    uint64_t index = 0;
    for (; index + sizeof(uint64_t) <= decodedSize; index += sizeof(uint64_t)) {
      storeUnaligned<uint64_t>(
          destination + index, ~loadUnaligned<uint64_t>(body + index));
    }
    for (; index < decodedSize; ++index) {
      destination[index] =
          static_cast<char>(~static_cast<uint8_t>(body[index]));
    }
    return;
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
    uint64_t*& remainingSizes,
    vector_size_t outputOffset) {
  const auto size = static_cast<vector_size_t>(keys.size());
  auto* cursors = scratch.cursors;
  validRows = MayHaveNulls ? scratch.block(0) : nullptr;
  bodyPointers = scratch.block(MayHaveNulls ? 1 : 0);
  remainingSizes = scratch.block(MayHaveNulls ? 2 : 1);
  uint64_t validCount = 0;
  if constexpr (!MayHaveNulls) {
    clearDestinationNulls(result, outputOffset, size);
  } else {
    auto* nulls =
        result->rawNulls() == nullptr ? nullptr : result->mutableRawNulls();
    if (nulls != nullptr) {
      bits::fillBits(nulls, outputOffset, outputOffset + size, bits::kNotNull);
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
        bits::setNull(nulls, outputOffset + row, true);
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
    const VectorPtr& result,
    vector_size_t outputOffset) {
  auto* flat = result->asUnchecked<FlatVector<StringView>>();
  auto* values = flat->mutableRawValues();
  if (outputOffset != 0) {
    values += outputOffset;
  }
  uint64_t* validRows;
  uint64_t* bodyPointers;
  uint64_t* decodedSizes;
  const auto count = prepareStringBodies<FirstColumn, MayHaveNulls>(
      column,
      keys,
      result,
      scratch,
      validRows,
      bodyPointers,
      decodedSizes,
      outputOffset);
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
    const VectorPtr& result,
    vector_size_t outputOffset = 0) {
  dispatchDescending(column, [&]<bool Descending>() {
    decodeStringColumnLayered<FirstColumn, MayHaveNulls, Descending>(
        column, keys, scratch, result, outputOffset);
  });
}

template <TypeKind KIND, bool FirstColumn, bool MayHaveNulls>
void decodeScalarColumnLayered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    DecodeScratch& scratch,
    const VectorPtr& result,
    vector_size_t outputOffset) {
  if constexpr (KIND == TypeKind::BOOLEAN) {
    decodeBooleanLayered<FirstColumn, MayHaveNulls>(
        column, keys, scratch, result, outputOffset);
  } else if constexpr (
      KIND == TypeKind::TINYINT || KIND == TypeKind::SMALLINT ||
      KIND == TypeKind::INTEGER || KIND == TypeKind::BIGINT) {
    using T = typename TypeTraits<KIND>::NativeType;
    decodeSignedFixedLayered<FirstColumn, MayHaveNulls, T>(
        column, keys, scratch, result, outputOffset);
  } else if constexpr (KIND == TypeKind::HUGEINT) {
    decodeInt128Layered<FirstColumn, MayHaveNulls>(
        column, keys, scratch, result, outputOffset);
  } else if constexpr (KIND == TypeKind::REAL) {
    decodeFloatLayered<FirstColumn, MayHaveNulls>(
        column, keys, scratch, result, outputOffset);
  } else if constexpr (KIND == TypeKind::DOUBLE) {
    decodeDoubleLayered<FirstColumn, MayHaveNulls>(
        column, keys, scratch, result, outputOffset);
  } else if constexpr (KIND == TypeKind::TIMESTAMP) {
    decodeTimestampLayered<FirstColumn, MayHaveNulls>(
        column, keys, scratch, result, outputOffset);
  } else {
    BOLT_FAIL(
        "Layered radix sort key decoding is not implemented for {}",
        column.type->toString());
  }
}

template <TypeKind KIND, bool FirstColumn, bool MayHaveNulls>
void decodePrimitiveColumnLayered(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    uint64_t* cursors,
    const VectorPtr& result,
    DecodeScratch& scratch,
    vector_size_t outputOffset) {
  if constexpr (
      KIND == TypeKind::BOOLEAN || KIND == TypeKind::TINYINT ||
      KIND == TypeKind::SMALLINT || KIND == TypeKind::INTEGER ||
      KIND == TypeKind::BIGINT || KIND == TypeKind::HUGEINT ||
      KIND == TypeKind::REAL || KIND == TypeKind::DOUBLE ||
      KIND == TypeKind::TIMESTAMP) {
    decodeScalarColumnLayered<KIND, FirstColumn, MayHaveNulls>(
        column, keys, scratch, result, outputOffset);
  } else if constexpr (
      KIND == TypeKind::VARCHAR || KIND == TypeKind::VARBINARY) {
    decodeStringColumnLayered<FirstColumn, MayHaveNulls>(
        column, keys, scratch, result, outputOffset);
  } else if constexpr (KIND == TypeKind::UNKNOWN) {
    const auto null = nullMarker(column.flags);
    for (vector_size_t row = 0; row < keys.size(); ++row) {
      auto cursor = FirstColumn ? uint64_t{0} : cursors[row];
      bool isNull;
      readMarker(keys[row], cursor, null, isNull);
      cursors[row] = cursor;
      result->setNull(outputOffset + row, true);
    }
  } else {
    BOLT_FAIL(
        "Layered radix sort key decoding is not implemented for {}",
        column.type->toString());
  }
}

template <bool FirstColumn, bool MayHaveNulls>
void decodeColumn(
    const RadixSortKeyColumn& column,
    std::span<const EncodedKeyView> keys,
    uint64_t* cursors,
    const VectorPtr& result,
    DecodeScratch& scratch,
    vector_size_t outputOffset = 0) {
  if (column.type->isPrimitiveType()) {
    const auto dispatch = [&]<TypeKind KIND>() {
      decodePrimitiveColumnLayered<KIND, FirstColumn, MayHaveNulls>(
          column, keys, cursors, result, scratch, outputOffset);
    };
    BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
        dispatch.template operator(), column.type->kind());
    return;
  }

  for (vector_size_t row = 0; row < keys.size(); ++row) {
    auto cursor = FirstColumn ? uint64_t{0} : cursors[row];
    EncodedKeyReader reader(
        keys[row].bytes.data() + cursor, keys[row].bytes.size() - cursor);
    decodeValue(
        column, reader, result, outputOffset + row, outputOffset + keys.size());
    cursors[row] = cursor + reader.position();
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
    RowVectorPtr& result,
    uint32_t firstColumn) {
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
      calculateDecodeScratchWordsPerRow(
          columns, decodedColumns, mayHaveNulls, firstColumn, columns.size()),
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

  if (!decodedColumns.empty() && decodedColumns[firstColumn] == 0) {
    if (fixedBodySize(*columns[firstColumn].type).has_value()) {
      skipFixedColumn<true>(columns[firstColumn], keys, cursors);
    } else {
      skipColumn<true>(columns[firstColumn], keys, cursors);
    }
  } else {
    decodeFirstColumn(firstColumn);
  }
  for (uint32_t column = firstColumn + 1; column < columns.size(); ++column) {
    if (!decodedColumns.empty() && decodedColumns[column] == 0) {
      if (fixedBodySize(*columns[column].type).has_value()) {
        skipFixedColumn<false>(columns[column], keys, cursors);
      } else {
        skipColumn<false>(columns[column], keys, cursors);
      }
    } else {
      decodeNextColumn(column);
    }
  }
}

template <typename BindScratch>
void decodeColumnsWithOffsetImpl(
    const std::vector<RadixSortKeyColumn>& columns,
    std::span<const EncodedKeyView> keys,
    std::span<const uint8_t> decodedColumns,
    std::span<const uint8_t> mayHaveNulls,
    vector_size_t outputOffset,
    RowVector& output,
    std::span<const column_index_t> directKeyChannels,
    uint32_t firstColumn,
    uint32_t endColumn,
    BindScratch&& bindScratch) {
  BOLT_DCHECK_LT(firstColumn, endColumn);
  BOLT_DCHECK_LE(endColumn, columns.size());
  DecodeScratch scratch;
  bindScratch(scratch);
  auto* cursors = scratch.cursors;
  const auto decode = [&](uint32_t column, bool first) {
    auto& child = output.childAt(directKeyChannels[column]);
    if (columns[column].type->kind() == TypeKind::UNKNOWN) {
      if (first) {
        decodeCheckedUnknownColumn<true>(
            columns[column], keys, cursors, child, outputOffset);
      } else {
        decodeCheckedUnknownColumn<false>(
            columns[column], keys, cursors, child, outputOffset);
      }
      return;
    }
    if (first) {
      if (mayHaveNulls[column] != 0) {
        decodeColumn<true, true>(
            columns[column], keys, cursors, child, scratch, outputOffset);
      } else {
        decodeColumn<true, false>(
            columns[column], keys, cursors, child, scratch, outputOffset);
      }
    } else if (mayHaveNulls[column] != 0) {
      decodeColumn<false, true>(
          columns[column], keys, cursors, child, scratch, outputOffset);
    } else {
      decodeColumn<false, false>(
          columns[column], keys, cursors, child, scratch, outputOffset);
    }
  };

  if (decodedColumns[firstColumn] != 0) {
    decode(firstColumn, true);
  } else if (isFixedScalarColumn(columns[firstColumn])) {
    skipCheckedFixedColumn<true>(columns[firstColumn], keys, cursors);
  } else {
    skipColumn<true>(columns[firstColumn], keys, cursors);
  }
  for (uint32_t column = firstColumn + 1; column < endColumn; ++column) {
    if (decodedColumns[column] != 0) {
      decode(column, false);
    } else if (isFixedScalarColumn(columns[column])) {
      skipCheckedFixedColumn<false>(columns[column], keys, cursors);
    } else {
      skipColumn<false>(columns[column], keys, cursors);
    }
  }
}

void decodeColumnsWithOffsetAndPreparedScratch(
    const std::vector<RadixSortKeyColumn>& columns,
    std::span<const EncodedKeyView> keys,
    std::span<const uint8_t> decodedColumns,
    std::span<const uint8_t> mayHaveNulls,
    vector_size_t outputOffset,
    memory::MemoryPool* pool,
    const BufferPtr& cursorScratch,
    RowVector& output,
    std::span<const column_index_t> directKeyChannels,
    uint64_t scratchWordsPerRow,
    uint32_t firstColumn,
    uint32_t endColumn) {
  auto bindScratch = [&](DecodeScratch& scratch) {
    bindPreparedDecodeScratch(
        static_cast<vector_size_t>(keys.size()),
        pool,
        scratchWordsPerRow,
        cursorScratch,
        scratch);
  };
  decodeColumnsWithOffsetImpl(
      columns,
      keys,
      decodedColumns,
      mayHaveNulls,
      outputOffset,
      output,
      directKeyChannels,
      firstColumn,
      endColumn,
      bindScratch);
}

template <typename CanSkipColumn>
std::vector<uint32_t> makeLeadingSkippableValidityOffsets(
    const std::vector<RadixSortKeyColumn>& columns,
    uint32_t radixWidth,
    CanSkipColumn canSkipColumn) {
  std::vector<uint32_t> offsets;
  uint64_t encodedOffset = 0;
  for (uint32_t column = 0; column < columns.size(); ++column) {
    const auto& metadata = columns[column];
    if (!canSkipColumn(column) || encodedOffset >= radixWidth) {
      break;
    }
    offsets.push_back(static_cast<uint32_t>(encodedOffset));
    if (!metadata.maximumEncodedSize.has_value()) {
      break;
    }
    encodedOffset += *metadata.maximumEncodedSize;
  }
  return offsets;
}

} // namespace

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
  std::optional<uint64_t> maximumEncodedSize = 0;
  bool maximumEncodedSizeValid = true;
  std::optional<uint64_t> fixedPrefixSize = 0;
  for (uint32_t column = 0; column < types.size(); ++column) {
    RadixSortKeyColumn metadata;
    buildMetadata(types[column], flags[column], metadata);
    BOLT_CHECK(
        supportsType(*types[column]),
        "Radix sort key type is not supported: {}",
        types[column]->toString());
    if (fixedPrefixSize.has_value() && isFixedScalarColumn(metadata)) {
      metadata.fixedPrefixOffset = static_cast<uint32_t>(*fixedPrefixSize);
      fixedPrefixSize =
          checkedAdd(*fixedPrefixSize, *metadata.maximumEncodedSize);
    } else {
      fixedPrefixSize = std::nullopt;
    }
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

  codec = std::unique_ptr<RadixSortKeyCodec>(
      new RadixSortKeyCodec(std::move(columns), maximumEncodedSize));
}

RadixSortKeyCodec::RadixSortKeyCodec(
    std::vector<RadixSortKeyColumn> columns,
    std::optional<uint64_t> maximumEncodedSize)
    : columns_(std::move(columns)),
      rowType_([&]() {
        std::vector<TypePtr> types;
        types.reserve(columns_.size());
        for (const auto& column : columns_) {
          types.push_back(column.type);
        }
        return ROW(std::move(types));
      }()),
      maximumEncodedSize_(maximumEncodedSize),
      allFixedScalarColumns_(
          std::all_of(columns_.begin(), columns_.end(), [](const auto& column) {
            return column.fixedPrefixOffset.has_value();
          })) {
  for (uint32_t column = 0; column < columns_.size(); ++column) {
    if (columns_[column].containsFloatingPoint) {
      floatingPointEnd_ = column + 1;
    }
  }
}

std::vector<uint32_t> RadixSortKeyCodec::leadingSkippableValidityOffsets(
    std::span<const uint8_t> keyMayHaveNulls,
    uint32_t radixWidth) const {
  return makeLeadingSkippableValidityOffsets(
      columns_, radixWidth, [&](uint32_t column) {
        return keyMayHaveNulls[column] == 0;
      });
}

uint32_t RadixSortKeyCodec::heapKeyOffsetForVariableLayout(
    uint32_t inlineCapacity) const {
  uint64_t offset = 0;
  for (const auto& column : columns_) {
    if (!column.fixedPrefixOffset.has_value()) {
      break;
    }
    const auto next = checkedAdd(offset, *column.maximumEncodedSize);
    if (!next.has_value() || *next > inlineCapacity) {
      break;
    }
    offset = *next;
  }
  return static_cast<uint32_t>(offset);
}

uint32_t RadixSortKeyCodec::fixedPrefixColumnCount(
    uint32_t heapKeyOffset) const {
  uint32_t count = 0;
  while (count < columns_.size() &&
         columns_[count].fixedPrefixOffset.has_value() &&
         *columns_[count].fixedPrefixOffset < heapKeyOffset) {
    ++count;
  }
  return count;
}

bool RadixSortKeyCodec::hasSpecialValues() const {
  return std::any_of(
      columns_.begin(),
      columns_.begin() + floatingPointEnd_,
      encodedSpecialValues);
}

std::vector<uint8_t> RadixSortKeyCodec::specialValueFlags() const {
  std::vector<uint8_t> flags;
  for (const auto& column : columns_) {
    appendSpecialValueFlags(column, flags);
  }
  return flags;
}

void RadixSortKeyCodec::mergeSpecialValueFlags(std::span<const uint8_t> flags) {
  uint32_t index = 0;
  for (auto& column : columns_) {
    mergeSpecialValues(column, flags, index);
  }
  BOLT_CHECK_EQ(index, flags.size());
}

void RadixSortKeyCodec::prepareSpecialComparators() const {
  BOLT_DCHECK(hasSpecialValues());
  for (const auto& column : columns_) {
    prepareColumnComparators(column);
  }
  specialComparisonEnd_ = floatingPointEnd_;
}

RadixSortFloatingPointPlan RadixSortKeyCodec::floatingPointPlan(
    const RadixSortKeyLayout& layout,
    std::span<const uint8_t> mayHaveNulls) const {
  RadixSortFloatingPointPlan plan;
  const auto limit = layout.inlineCapacity();
  uint32_t offset = 0;
  for (uint32_t index = 0; index < columns_.size() && offset < limit; ++index) {
    const auto& column = columns_[index];
    const auto size = fixedBodySize(*column.type);
    if (!size.has_value()) {
      break;
    }
    const bool fixedNull =
        layout.isVariable() && offset < layout.heapKeyOffset();
    if ((mayHaveNulls.empty() || mayHaveNulls[index]) && !fixedNull &&
        columns_.size() != 1) {
      ++offset;
      break;
    }
    if (column.hasSpecialValues) {
      if (offset + 1 + *size > limit) {
        ++offset;
        break;
      }
      for (uint32_t byte = offset + 1; byte < offset + 1 + *size; ++byte) {
        plan.digits[byte] = {
            offset + 1, static_cast<uint8_t>(*size), !column.flags.ascending};
      }
    }
    offset += 1 + *size;
  }
  plan.radixWidth = std::min(offset, limit);
  plan.complete = maximumEncodedSize_.has_value() &&
      *maximumEncodedSize_ <= plan.radixWidth;
  return plan;
}

int32_t RadixSortKeyCodec::compareEncoded(
    std::string_view left,
    std::string_view right,
    uint32_t firstColumn) const {
  auto a = EncodedKeyReader::checkedAt(left, 0);
  auto b = EncodedKeyReader::checkedAt(right, 0);
  for (uint32_t column = firstColumn; column < specialComparisonEnd_;
       ++column) {
    const auto& metadata = columns_[column];
    const auto result = metadata.encodedComparator(metadata, a, b);
    if (result != 0) {
      return result;
    }
  }
  return compareKeyBytes(
      {a.currentData(), a.remaining()}, {b.currentData(), b.remaining()});
}

int32_t RadixSortKeyCodec::comparePhysical(
    const RadixSortKeyLayout& layout,
    const char* left,
    const char* right,
    std::string_view leftSuffix,
    std::string_view rightSuffix) const {
  if (!layout.isVariable()) {
    if (allFixedScalarColumns_) {
      const auto wordBytes = layout.inlineWordBytes();
      uint32_t offset = 0;
      for (const auto& column : columns_) {
        const auto a = loadEncodedByte<true>(left, offset, wordBytes);
        const auto b = loadEncodedByte<true>(right, offset, wordBytes);
        if (a != b) {
          return (a > b) - (a < b);
        }
        if (a == nullMarker(column.flags)) {
          ++offset;
          continue;
        }
        const auto result =
            column.fixedComparator(column, left, right, offset + 1, wordBytes);
        if (result != 0) {
          return result;
        }
        offset += static_cast<uint32_t>(*column.maximumEncodedSize);
      }
      return 0;
    }
    RadixSortInlineKeyBuffer a;
    RadixSortInlineKeyBuffer b;
    EncodedKeyView aView;
    EncodedKeyView bView;
    RadixSortKey(layout, left).deconstruct(a, aView);
    RadixSortKey(layout, right).deconstruct(b, bView);
    return compareEncoded(aView.bytes, bView.bytes);
  }
  const auto heapOffset = layout.heapKeyOffset();
  const auto prefixColumns = fixedPrefixColumnCount(heapOffset);
  uint32_t column = 0;
  for (; column < prefixColumns; ++column) {
    const auto& metadata = columns_[column];
    const auto offset = *metadata.fixedPrefixOffset;
    const auto a = static_cast<uint8_t>(left[offset]);
    const auto b = static_cast<uint8_t>(right[offset]);
    if (a != b) {
      return (a > b) - (a < b);
    }
    if (a == nullMarker(metadata.flags)) {
      continue;
    }
    const auto result = metadata.variableComparator(
        metadata, left, right, offset + 1, layout.inlineWordBytes());
    if (result != 0) {
      return (result > 0) - (result < 0);
    }
  }
  if (leftSuffix.data() == nullptr) {
    const auto leftSize =
        loadUnaligned<uint64_t>(left + *layout.sizeOffset()) - heapOffset;
    const auto rightSize =
        loadUnaligned<uint64_t>(right + *layout.sizeOffset()) - heapOffset;
    leftSuffix = {loadCompactPointer(left + *layout.dataOffset()), leftSize};
    rightSuffix = {loadCompactPointer(right + *layout.dataOffset()), rightSize};
  }
  return compareEncoded(leftSuffix, rightSuffix, column);
}

uint64_t RadixSortKeyCodec::appendVariable(
    const RowVector& input,
    RadixSortRunStorage& arena,
    std::span<char* const> payloads,
    uint32_t firstSuffixColumn,
    BufferPtr& sizeScratch) const {
  const auto& layout = arena.layout();
  BOLT_DCHECK(layout.isVariable());
  BOLT_DCHECK_EQ(
      firstSuffixColumn, fixedPrefixColumnCount(layout.heapKeyOffset()));
  BOLT_DCHECK_LT(firstSuffixColumn, columns_.size());

  auto* heapSizes =
      prepareReusableBuffer<uint64_t>(sizeScratch, input.size(), arena.pool());
  initializeVariableKeySizes(columns_, input, firstSuffixColumn, heapSizes);

  DecodedKeyVectors decodedInputs;
  return arena.appendVariableKeyBatch(
      std::span<const uint64_t>(heapSizes, input.size()),
      payloads,
      [&](vector_size_t source, vector_size_t count, char* records) {
        for (uint32_t column = 0; column < firstSuffixColumn; ++column) {
          StridedEncodeOutput output(
              records, layout.width(), *columns_[column].fixedPrefixOffset);
          encodeVariableColumn(
              columns_[column],
              *input.childAt(column),
              source,
              count,
              output,
              &decodedInputs,
              true);
        }

        auto* cursors = heapSizes + source;
        std::fill(cursors, cursors + count, uint64_t{0});
        IndirectEncodeOutput output(
            records, layout.width(), *layout.dataOffset(), cursors);
        for (uint32_t column = firstSuffixColumn; column < columns_.size();
             ++column) {
          encodeVariableColumn(
              columns_[column],
              *input.childAt(column),
              source,
              count,
              output,
              &decodedInputs);
        }

        const auto crossingPrefixSize =
            layout.radixWidth() - layout.heapKeyOffset();
        for (vector_size_t row = 0; row < count; ++row) {
          auto* record = records + static_cast<uint64_t>(row) * layout.width();
          BOLT_DCHECK_EQ(
              cursors[row],
              loadUnaligned<uint64_t>(record + *layout.sizeOffset()) -
                  layout.heapKeyOffset());
          const auto* heap = loadCompactPointer(record + *layout.dataOffset());
          std::memcpy(
              record + layout.heapKeyOffset(),
              heap,
              std::min<uint64_t>(cursors[row], crossingPrefixSize));
        }
      });
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

uint64_t RadixSortKeyCodec::append(
    const RowVector& input,
    RadixSortRunStorage& storage,
    std::span<char* const> payloads,
    BufferPtr& sizeScratch) const {
  BOLT_DCHECK_EQ(input.childrenSize(), columns_.size());
  BOLT_DCHECK(
      storage.layout().hasPayload()
          ? payloads.size() == static_cast<size_t>(input.size())
          : payloads.empty());
  if (storage.layout().isVariable()) {
    return appendVariable(
        input,
        storage,
        payloads,
        fixedPrefixColumnCount(storage.layout().heapKeyOffset()),
        sizeScratch);
  }

  if (input.childrenSize() == 1 &&
      tryAppendSingleFixedFlat(
          *input.childAt(0), input.size(), storage, payloads)) {
    return *maximumEncodedSize_;
  }

  const auto append = [&]<RadixSortKeyLayoutKind KIND>() {
    encodeAndAppendInlineLayout<KIND>(
        columns_, input, storage, payloads, encodeCursorScratch_);
  };
  switch (storage.layout().kind()) {
    case RadixSortKeyLayoutKind::kKeyOnlyFixed8:
      append.template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed8>();
      break;
    case RadixSortKeyLayoutKind::kKeyOnlyFixed16:
      append.template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed16>();
      break;
    case RadixSortKeyLayoutKind::kKeyOnlyFixed24:
      append.template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed24>();
      break;
    case RadixSortKeyLayoutKind::kKeyOnlyFixed32:
      append.template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed32>();
      break;
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed16:
      append.template
      operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed16>();
      break;
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed24:
      append.template
      operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed24>();
      break;
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed32:
      append.template
      operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed32>();
      break;
    case RadixSortKeyLayoutKind::kInvalid:
    case RadixSortKeyLayoutKind::kKeyOnlyVariable32:
    case RadixSortKeyLayoutKind::kKeyWithPayloadVariable32:
      BOLT_FAIL("Unsupported inline radix sort key layout");
  }
  return *maximumEncodedSize_;
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
  decodeSinglePhysicalColumn<true>(
      columns_[0],
      arena,
      begin,
      count,
      mayHaveNulls,
      child,
      arena.layout().inlineWordBytes());
  result = std::make_shared<RowVector>(
      pool, rowType_, nullptr, count, std::vector<VectorPtr>{std::move(child)});
  return true;
}

std::optional<uint32_t> RadixSortKeyCodec::singleFixedWordBytes(
    RadixSortKeyLayoutKind layoutKind) const {
  if (!canDecodeSingleFixedColumn()) {
    return std::nullopt;
  }
  const auto layout = RadixSortKeyLayout::fromKind(layoutKind);
  if (layout.isVariable()) {
    return std::nullopt;
  }
  return layout.inlineWordBytes();
}

void RadixSortKeyCodec::decodeSingleFixedAt(
    std::span<const char* const> keys,
    bool mayHaveNulls,
    vector_size_t outputOffset,
    uint32_t inlineWordBytes,
    const VectorPtr& destination) const {
  BOLT_DCHECK_NOT_NULL(destination);
  BOLT_DCHECK_GE(outputOffset, 0);
  BOLT_DCHECK_LE(outputOffset, destination->size());
  BOLT_DCHECK_LE(
      keys.size(), static_cast<size_t>(destination->size() - outputOffset));
  decodeSinglePhysicalColumn<true>(
      columns_[0],
      keys,
      mayHaveNulls,
      destination,
      inlineWordBytes,
      0,
      outputOffset,
      true);
}

void RadixSortKeyCodec::decode(
    std::span<const EncodedKeyView> keys,
    std::span<const uint8_t> decodedColumns,
    std::span<const uint8_t> mayHaveNulls,
    memory::MemoryPool* pool,
    BufferPtr& cursorScratch,
    RowVectorPtr& result,
    uint32_t firstColumn) const {
  BOLT_CHECK_NOT_NULL(pool, "Radix sort key memory pool must not be null");
  BOLT_CHECK_LT(firstColumn, columns_.size());
  decodeColumns(
      columns_,
      rowType_,
      keys,
      decodedColumns,
      mayHaveNulls,
      pool,
      cursorScratch,
      result,
      firstColumn);
}

uint64_t RadixSortKeyCodec::decodeScratchWordsPerRowWithMask(
    std::span<const uint8_t> decodedColumns,
    std::span<const uint8_t> mayHaveNulls,
    uint32_t firstColumn,
    uint32_t endColumn) const {
  BOLT_DCHECK_EQ(decodedColumns.size(), columns_.size());
  BOLT_DCHECK_EQ(mayHaveNulls.size(), columns_.size());
  BOLT_DCHECK_LT(firstColumn, columns_.size());
  BOLT_DCHECK_LE(endColumn, columns_.size());
  return calculateDecodeScratchWordsPerRow(
      columns_, decodedColumns, mayHaveNulls, firstColumn, endColumn);
}

void RadixSortKeyCodec::decodeSuffixAtWithPreparedScratch(
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
    uint32_t endColumn) const {
  BOLT_DCHECK_NOT_NULL(scratchPool);
  BOLT_DCHECK_NOT_NULL(cursorScratch);
  BOLT_DCHECK_GE(outputOffset, 0);
  BOLT_DCHECK_LE(outputOffset, output.size());
  BOLT_DCHECK_LE(
      keys.size(), static_cast<size_t>(output.size() - outputOffset));
  decodeColumnsWithOffsetAndPreparedScratch(
      columns_,
      keys,
      decodedColumns,
      mayHaveNulls,
      outputOffset,
      scratchPool,
      cursorScratch,
      output,
      directKeyChannels,
      scratchWordsPerRow,
      firstColumn,
      endColumn);
}

void RadixSortKeyCodec::decodeFixedPrefix(
    const RadixSortRunStorage& arena,
    uint64_t begin,
    vector_size_t count,
    std::span<const uint8_t> decodedColumns,
    std::span<const uint8_t> mayHaveNulls,
    RowVectorPtr& result,
    uint32_t prefixColumnCount) const {
  decodeFixedPrefixColumns(
      columns_,
      prefixColumnCount,
      decodedColumns,
      mayHaveNulls,
      [&](uint32_t column, uint32_t encodedOffset, bool mayHaveNulls) {
        decodeSinglePhysicalColumn<false>(
            columns_[column],
            arena,
            begin,
            count,
            mayHaveNulls,
            result->childAt(column),
            0,
            encodedOffset);
      });
}

void RadixSortKeyCodec::decodePrefixAt(
    std::span<const char* const> keys,
    std::span<const uint8_t> decodedColumns,
    std::span<const uint8_t> mayHaveNulls,
    vector_size_t outputOffset,
    RowVector& output,
    std::span<const column_index_t> directKeyChannels,
    uint32_t prefixColumnCount) const {
  BOLT_DCHECK_GE(outputOffset, 0);
  BOLT_DCHECK_LE(outputOffset, output.size());
  BOLT_DCHECK_LE(
      keys.size(), static_cast<size_t>(output.size() - outputOffset));
  decodeFixedPrefixColumns(
      columns_,
      prefixColumnCount,
      decodedColumns,
      mayHaveNulls,
      [&](uint32_t column, uint32_t encodedOffset, bool columnMayHaveNulls) {
        decodeSinglePhysicalColumn<false>(
            columns_[column],
            keys,
            columnMayHaveNulls,
            output.childAt(directKeyChannels[column]),
            0,
            encodedOffset,
            outputOffset,
            true);
      });
}

void RadixSortKeyCodec::finishDecode(
    std::span<const uint8_t> decodedColumns,
    RowVector& output,
    std::span<const column_index_t> directKeyChannels) const {
  BOLT_DCHECK_EQ(decodedColumns.size(), columns_.size());
  BOLT_DCHECK_EQ(directKeyChannels.size(), columns_.size());
  for (uint32_t column = 0; column < columns_.size(); ++column) {
    if (decodedColumns[column] == 0) {
      continue;
    }
    output.childAt(directKeyChannels[column])->resetDataDependentFlags(nullptr);
  }
}

} // namespace bytedance::bolt::exec::radixsort
