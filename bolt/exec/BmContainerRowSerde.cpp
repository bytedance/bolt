/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/exec/BmContainerRowSerde.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/CheckedArithmetic.h"
#include "bolt/exec/VariantSerdeDetail.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/VariantVector.h"

#include <cstring>
#include <limits>
#include <span>
#include <type_traits>

namespace bytedance::bolt::exec {

// This file has two independent traversals of the same recursive
// ARRAY/MAP/ROW structure, grouped into separate namespaces:
//   - serialize: writes the serialized bytes into a caller-provided range.
//   - measure:   computes the exact serialized byte size without writing.
// The measure path is kept distinct (rather than sizing via a counting writer)
// so it can skip MAP key sorting, which is size-invariant but costs a per-value
// allocation on the serialize path.
namespace {

//===----------------------------------------------------------------------===//
// serialize: write the serialized bytes.
//===----------------------------------------------------------------------===//
namespace serialize {

// Serialization sink shared by the exact-size and best-effort write paths.
//
// TolerateOverflow == false: the target range must have the exact serialized
// size. append throws on any capacity mismatch and finish() asserts the range
// was filled exactly. Used by serializeInto().
//
// TolerateOverflow == true: capacity exhaustion is not an error. append copies
// only while the value still fits, then keeps counting so size() is exact after
// a single traversal; overflowed() reports whether the value fit. Used by
// trySerializeInto().
template <bool TolerateOverflow>
class SerializeWriter {
 public:
  explicit SerializeWriter(std::span<char> target)
      : begin_(target.data()), cursor_(target.data()) {
    end_ = target.empty() ? cursor_ : cursor_ + target.size();
  }

  template <typename T>
  void append(folly::Range<const T*> values) {
    appendBytes(values.data(), sizeof(T) * values.size());
  }

  template <typename T>
  FOLLY_ALWAYS_INLINE void appendOne(const T& value) {
    static_assert(std::is_trivially_copyable_v<T>);
    // Fixed-width leaf: sizeof(T) is a compile-time constant, so this inlined
    // appendBytes lowers to a single (possibly unaligned) store with the
    // zero-length branch folded away. Constant-size memcpy avoids the
    // misaligned-store UB of a raw reinterpret_cast assignment.
    appendBytes(&value, sizeof(T));
  }

  void appendStringView(StringView value) {
    appendBytes(value.data(), value.size());
  }

  uint64_t size() const {
    if constexpr (TolerateOverflow) {
      return size_;
    } else {
      return begin_ == nullptr ? 0 : static_cast<uint64_t>(cursor_ - begin_);
    }
  }

  bool overflowed() const {
    return overflowed_;
  }

  void finish() const {
    if constexpr (!TolerateOverflow) {
      BOLT_CHECK(
          cursor_ == end_,
          "ContainerRowSerde target range must have exact serialized size");
    }
  }

 private:
  FOLLY_ALWAYS_INLINE void appendBytes(const void* data, uint64_t bytes) {
    if constexpr (TolerateOverflow) {
      // Always count; copy only while the value fits, then stop.
      size_ += bytes;
      if (FOLLY_LIKELY(!overflowed_ && cursor_ + bytes <= end_)) {
        if (bytes != 0) {
          simd::memcpy(cursor_, data, bytes);
          cursor_ += bytes;
        }
        return;
      }
      overflowed_ = true;
    } else {
      const auto remaining =
          cursor_ == nullptr ? 0 : static_cast<uint64_t>(end_ - cursor_);
      BOLT_CHECK_LE(
          bytes, remaining, "ContainerRowSerde target range is too small");
      if (bytes != 0) {
        simd::memcpy(cursor_, data, bytes);
        cursor_ += bytes;
      }
    }
  }

  char* begin_{nullptr};
  char* cursor_{nullptr};
  char* end_{nullptr};
  uint64_t size_{0}; // TolerateOverflow only.
  bool overflowed_{false}; // TolerateOverflow only.
};

template <typename Writer>
void serializeDirectSwitch(
    const BaseVector& source,
    vector_size_t index,
    Writer& writer,
    const ContainerRowSerdeOptions& options);

// Writes the element null bitmap: one uint64 per 64 elements, with a bit set
// when the element at 'indexOf(i)' is null. 'indexOf' maps a logical position
// to a source row index; it is a gather ('indices[i]') for the sorted map-key
// path and an offset ('offset + i') for the contiguous path. The lambda is
// inlined, so both callers keep their original single-copy code.
template <typename Writer, typename IndexOf>
void writeDirectNulls(
    const BaseVector& values,
    vector_size_t count,
    Writer& writer,
    IndexOf indexOf) {
  for (vector_size_t i = 0; i < count; i += 64) {
    uint64_t flags = 0;
    const auto end = std::min<vector_size_t>(64, count - i);
    for (vector_size_t bit = 0; bit < end; ++bit) {
      if (values.isNullAt(indexOf(i + bit))) {
        bits::setBit(&flags, bit);
      }
    }
    writer.template appendOne<uint64_t>(flags);
  }
}

template <typename Writer>
void serializeDirectArray(
    const BaseVector& elements,
    folly::Range<const vector_size_t*> indices,
    Writer& writer,
    const ContainerRowSerdeOptions& options) {
  BOLT_CHECK_LE(indices.size(), std::numeric_limits<int32_t>::max());
  writer.template appendOne<int32_t>(indices.size());
  writeDirectNulls(elements, indices.size(), writer, [&](vector_size_t i) {
    return indices[i];
  });
  for (auto index : indices) {
    if (!elements.isNullAt(index)) {
      serializeDirectSwitch(elements, index, writer, options);
    }
  }
}

// Appends a single flat fixed-width run [start, start + count) with one copy.
// int128 is copied through an int8 view to avoid a misaligned 16-byte load.
template <typename T, typename Writer>
FOLLY_ALWAYS_INLINE void appendFlatRun(
    const T* rawValues,
    vector_size_t start,
    vector_size_t count,
    Writer& writer) {
  if (count == 0) {
    return;
  }
  const T* values = rawValues + start;
  if constexpr (std::is_same_v<T, __int128>) {
    writer.template append<int8_t>(folly::Range<const int8_t*>(
        reinterpret_cast<const int8_t*>(values),
        static_cast<size_t>(count) * sizeof(T)));
  } else {
    writer.template append<T>(folly::Range<const T*>(values, count));
  }
}

// Copies elements[offset, offset + size) into the writer, batching flat
// fixed-width contiguous non-null runs into single append() calls instead of
// dispatching each element. Returns false when the elements are not a flat
// fixed-width vector, leaving the caller to fall back to per-element writes.
template <TypeKind Kind, typename Writer>
bool serializeDirectFixedWidthRange(
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t size,
    Writer& writer) {
  if constexpr (
      Kind == TypeKind::TINYINT || Kind == TypeKind::SMALLINT ||
      Kind == TypeKind::INTEGER || Kind == TypeKind::BIGINT ||
      Kind == TypeKind::HUGEINT || Kind == TypeKind::REAL ||
      Kind == TypeKind::DOUBLE) {
    using T = typename TypeTraits<Kind>::NativeType;
    if (elements.encoding() != VectorEncoding::Simple::FLAT) {
      return false;
    }
    const auto* flat = elements.asUnchecked<FlatVector<T>>();
    const auto* rawValues = flat->rawValues();
    if (!flat->mayHaveNulls()) {
      appendFlatRun<T>(rawValues, offset, size, writer);
      return true;
    }
    // Batch maximal contiguous non-null runs.
    vector_size_t runStart = offset;
    vector_size_t runCount = 0;
    for (vector_size_t i = 0; i < size; ++i) {
      const auto index = offset + i;
      if (flat->isNullAt(index)) {
        appendFlatRun<T>(rawValues, runStart, runCount, writer);
        runCount = 0;
      } else {
        if (runCount == 0) {
          runStart = index;
        }
        ++runCount;
      }
    }
    appendFlatRun<T>(rawValues, runStart, runCount, writer);
    return true;
  } else {
    return false;
  }
}

template <typename Writer>
void serializeDirectArray(
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t size,
    Writer& writer,
    const ContainerRowSerdeOptions& options) {
  BOLT_CHECK_GE(size, 0);
  writer.template appendOne<int32_t>(size);
  writeDirectNulls(
      elements, size, writer, [&](vector_size_t i) { return offset + i; });
  // Flat fixed-width elements copy in batched runs; other encodings and complex
  // element types fall back to per-element serialization.
  if (BOLT_DYNAMIC_TYPE_DISPATCH(
          serializeDirectFixedWidthRange,
          elements.typeKind(),
          elements,
          offset,
          size,
          writer)) {
    return;
  }
  for (vector_size_t i = 0; i < size; ++i) {
    const auto index = offset + i;
    if (!elements.isNullAt(index)) {
      serializeDirectSwitch(elements, index, writer, options);
    }
  }
}

template <TypeKind Kind, typename Writer>
void serializeOneDirect(
    const BaseVector& source,
    vector_size_t index,
    Writer& writer,
    const ContainerRowSerdeOptions& options) {
  if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
    const auto value =
        source.asUnchecked<SimpleVector<StringView>>()->valueAt(index);
    BOLT_CHECK_LE(value.size(), std::numeric_limits<int32_t>::max());
    writer.template appendOne<int32_t>(value.size());
    writer.appendStringView(value);
  } else if constexpr (Kind == TypeKind::VARIANT) {
    variant_serde::serializeVariant(source, index, writer);
  } else if constexpr (Kind == TypeKind::ROW) {
    const auto* row = source.wrappedVector()->asUnchecked<RowVector>();
    const auto wrappedIndex = source.wrappedIndex(index);
    const auto childrenSize = row->type()->size();
    const auto& children = row->children();
    std::vector<uint64_t> nulls(bits::nwords(childrenSize));
    for (vector_size_t child = 0; child < childrenSize; ++child) {
      if (child >= children.size() || !children[child] ||
          children[child]->isNullAt(wrappedIndex)) {
        bits::setBit(nulls.data(), child);
      }
    }
    writer.template append<uint64_t>(nulls);
    for (vector_size_t child = 0; child < children.size(); ++child) {
      if (!bits::isBitSet(nulls.data(), child)) {
        serializeDirectSwitch(*children[child], wrappedIndex, writer, options);
      }
    }
  } else if constexpr (Kind == TypeKind::ARRAY) {
    const auto* array = source.wrappedVector()->asUnchecked<ArrayVector>();
    const auto wrappedIndex = source.wrappedIndex(index);
    serializeDirectArray(
        *array->elements(),
        array->offsetAt(wrappedIndex),
        array->sizeAt(wrappedIndex),
        writer,
        options);
  } else if constexpr (Kind == TypeKind::MAP) {
    const auto* map = source.wrappedVector()->asUnchecked<MapVector>();
    const auto wrappedIndex = source.wrappedIndex(index);
    if (options.isKey) {
      // TODO: Reuse bounded, recursion-depth-aware thread-local storage for
      // MAP sort indices to avoid per-value allocations without changing the
      // public serialization API.
      const auto indices = map->sortedKeyIndices(wrappedIndex);
      serializeDirectArray(*map->mapKeys(), indices, writer, options);
      serializeDirectArray(*map->mapValues(), indices, writer, options);
    } else {
      const auto offset = map->offsetAt(wrappedIndex);
      const auto size = map->sizeAt(wrappedIndex);
      serializeDirectArray(*map->mapKeys(), offset, size, writer, options);
      serializeDirectArray(*map->mapValues(), offset, size, writer, options);
    }
  } else if constexpr (
      Kind == TypeKind::UNKNOWN || !TypeTraits<Kind>::isPrimitiveType ||
      !TypeTraits<Kind>::isFixedWidth) {
    BOLT_NYI("Unsupported serialize type {}", source.type()->toString());
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    writer.template appendOne<T>(
        source.asUnchecked<SimpleVector<T>>()->valueAt(index));
  }
}

template <typename Writer>
void serializeDirectSwitch(
    const BaseVector& source,
    vector_size_t index,
    Writer& writer,
    const ContainerRowSerdeOptions& options) {
  BOLT_DYNAMIC_TYPE_DISPATCH(
      serializeOneDirect, source.typeKind(), source, index, writer, options);
}

} // namespace serialize

//===----------------------------------------------------------------------===//
// measure: compute the exact serialized byte size without writing.
//===----------------------------------------------------------------------===//
namespace measure {

uint64_t serializedSizeSwitch(
    const BaseVector& source,
    vector_size_t index,
    const ContainerRowSerdeOptions& options);

uint64_t nullBytes(vector_size_t size) {
  BOLT_CHECK_GE(size, 0);
  return checkedMultiply<uint64_t>(
      bits::nwords(static_cast<uint64_t>(size)), sizeof(uint64_t));
}

template <TypeKind Kind>
std::optional<uint64_t> fixedWidthRangeSize(
    const BaseVector& values,
    vector_size_t offset,
    vector_size_t size) {
  if constexpr (
      Kind != TypeKind::UNKNOWN && TypeTraits<Kind>::isPrimitiveType &&
      TypeTraits<Kind>::isFixedWidth) {
    using T = typename TypeTraits<Kind>::NativeType;
    uint64_t nullCount = 0;
    if (values.mayHaveNulls()) {
      for (vector_size_t i = 0; i < size; ++i) {
        nullCount += values.isNullAt(offset + i);
      }
    }
    return checkedMultiply<uint64_t>(
        static_cast<uint64_t>(size) - nullCount, sizeof(T));
  } else {
    return std::nullopt;
  }
}

uint64_t serializedArraySize(
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t size,
    const ContainerRowSerdeOptions& options) {
  auto result = checkedPlus<uint64_t>(sizeof(int32_t), nullBytes(size));
  if (const auto fixedBytes = BOLT_DYNAMIC_TYPE_DISPATCH(
          fixedWidthRangeSize, elements.typeKind(), elements, offset, size)) {
    return checkedPlus<uint64_t>(result, fixedBytes.value());
  }
  for (vector_size_t i = 0; i < size; ++i) {
    const auto index = offset + i;
    if (!elements.isNullAt(index)) {
      result = checkedPlus<uint64_t>(
          result, serializedSizeSwitch(elements, index, options));
    }
  }
  return result;
}

template <TypeKind Kind>
uint64_t serializedSizeOne(
    const BaseVector& source,
    vector_size_t index,
    const ContainerRowSerdeOptions& options) {
  if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
    const auto value =
        source.asUnchecked<SimpleVector<StringView>>()->valueAt(index);
    BOLT_CHECK_LE(value.size(), std::numeric_limits<int32_t>::max());
    return checkedPlus<uint64_t>(sizeof(int32_t), value.size());
  } else if constexpr (Kind == TypeKind::VARIANT) {
    const auto* wrapped = source.wrappedVector();
    BOLT_CHECK_EQ(
        wrapped->encoding(),
        VectorEncoding::Simple::VARIANT,
        "Unexpected encoding for VARIANT vector: {}",
        wrapped->encoding());
    const auto value = wrapped->asUnchecked<VariantVector>()->valueAt(
        source.wrappedIndex(index));
    auto result = checkedPlus<uint64_t>(
        2 * sizeof(int32_t), static_cast<uint64_t>(value.value.size()));
    return checkedPlus<uint64_t>(result, value.metadata.size());
  } else if constexpr (Kind == TypeKind::ROW) {
    const auto* row = source.wrappedVector()->asUnchecked<RowVector>();
    const auto wrappedIndex = source.wrappedIndex(index);
    const auto childrenSize = row->type()->size();
    const auto& children = row->children();
    auto result = nullBytes(childrenSize);
    for (vector_size_t child = 0; child < children.size(); ++child) {
      if (children[child] && !children[child]->isNullAt(wrappedIndex)) {
        result = checkedPlus<uint64_t>(
            result,
            serializedSizeSwitch(*children[child], wrappedIndex, options));
      }
    }
    return result;
  } else if constexpr (Kind == TypeKind::ARRAY) {
    const auto* array = source.wrappedVector()->asUnchecked<ArrayVector>();
    const auto wrappedIndex = source.wrappedIndex(index);
    return serializedArraySize(
        *array->elements(),
        array->offsetAt(wrappedIndex),
        array->sizeAt(wrappedIndex),
        options);
  } else if constexpr (Kind == TypeKind::MAP) {
    const auto* map = source.wrappedVector()->asUnchecked<MapVector>();
    const auto wrappedIndex = source.wrappedIndex(index);
    const auto offset = map->offsetAt(wrappedIndex);
    const auto size = map->sizeAt(wrappedIndex);
    return checkedPlus<uint64_t>(
        serializedArraySize(*map->mapKeys(), offset, size, options),
        serializedArraySize(*map->mapValues(), offset, size, options));
  } else if constexpr (
      Kind == TypeKind::UNKNOWN || !TypeTraits<Kind>::isPrimitiveType ||
      !TypeTraits<Kind>::isFixedWidth) {
    BOLT_NYI("Unsupported serialize type {}", source.type()->toString());
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    return sizeof(T);
  }
}

uint64_t serializedSizeSwitch(
    const BaseVector& source,
    vector_size_t index,
    const ContainerRowSerdeOptions& options) {
  return BOLT_DYNAMIC_TYPE_DISPATCH(
      serializedSizeOne, source.typeKind(), source, index, options);
}

template <TypeKind Kind>
void serializedSizesTyped(
    const DecodedVector& decoded,
    vector_size_t offset,
    vector_size_t size,
    const ContainerRowSerdeOptions& options,
    std::span<uint64_t> result) {
  const auto& source = *decoded.base();
  if constexpr (
      Kind != TypeKind::UNKNOWN && TypeTraits<Kind>::isPrimitiveType &&
      TypeTraits<Kind>::isFixedWidth) {
    using T = typename TypeTraits<Kind>::NativeType;
    for (vector_size_t i = 0; i < size; ++i) {
      result[i] = decoded.isNullAt(offset + i) ? 0 : sizeof(T);
    }
    return;
  }

  if constexpr (Kind == TypeKind::ROW) {
    const auto* row = source.wrappedVector()->asUnchecked<RowVector>();
    const auto childrenSize = row->type()->size();
    const auto& children = row->children();
    for (vector_size_t i = 0; i < size; ++i) {
      result[i] = decoded.isNullAt(offset + i) ? 0 : nullBytes(childrenSize);
    }
    for (vector_size_t child = 0; child < children.size(); ++child) {
      if (!children[child]) {
        continue;
      }
      for (vector_size_t i = 0; i < size; ++i) {
        const auto sourceIndex = offset + i;
        if (result[i] == 0) {
          continue;
        }
        const auto rowIndex = decoded.index(sourceIndex);
        if (!children[child]->isNullAt(rowIndex)) {
          result[i] = checkedPlus<uint64_t>(
              result[i],
              serializedSizeSwitch(*children[child], rowIndex, options));
        }
      }
    }
    return;
  }

  for (vector_size_t i = 0; i < size; ++i) {
    const auto sourceIndex = offset + i;
    if (decoded.isNullAt(sourceIndex)) {
      result[i] = 0;
      continue;
    }
    result[i] =
        serializedSizeOne<Kind>(source, decoded.index(sourceIndex), options);
  }
}

} // namespace measure

} // namespace

// static
uint64_t BmContainerRowSerde::serializedSize(
    const BaseVector& source,
    vector_size_t index,
    const ContainerRowSerdeOptions& options) {
  BOLT_CHECK(
      !source.isNullAt(index), "Null top-level values are not supported");
  return measure::serializedSizeSwitch(source, index, options);
}

// static
void BmContainerRowSerde::serializedSizes(
    const DecodedVector& decoded,
    vector_size_t offset,
    vector_size_t size,
    const ContainerRowSerdeOptions& options,
    std::span<uint64_t> result) {
  BOLT_CHECK_GE(offset, 0);
  BOLT_CHECK_GE(size, 0);
  BOLT_CHECK_LE(offset, decoded.size());
  BOLT_CHECK_LE(size, decoded.size() - offset);
  BOLT_CHECK_EQ(result.size(), size);
  BOLT_DYNAMIC_TYPE_DISPATCH(
      measure::serializedSizesTyped,
      decoded.base()->typeKind(),
      decoded,
      offset,
      size,
      options,
      result);
}

// static
uint64_t BmContainerRowSerde::serializeInto(
    const BaseVector& source,
    vector_size_t index,
    std::span<char> target,
    const ContainerRowSerdeOptions& options) {
  BOLT_CHECK(
      !source.isNullAt(index), "Null top-level values are not supported");
  serialize::SerializeWriter</*TolerateOverflow=*/false> writer(target);
  serialize::serializeDirectSwitch(source, index, writer, options);
  writer.finish();
  return writer.size();
}

namespace detail {

TrySerializeResult trySerializeInto(
    const BaseVector& source,
    vector_size_t index,
    std::span<char> available,
    const ContainerRowSerdeOptions& options) {
  BOLT_CHECK(
      !source.isNullAt(index), "Null top-level values are not supported");
  serialize::SerializeWriter</*TolerateOverflow=*/true> writer(available);
  serialize::serializeDirectSwitch(source, index, writer, options);
  writer.finish();
  return TrySerializeResult{
      .size = writer.size(), .complete = !writer.overflowed()};
}

} // namespace detail

} // namespace bytedance::bolt::exec
