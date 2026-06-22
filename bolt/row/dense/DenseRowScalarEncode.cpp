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

// Scalar-row fast path — ENCODE side (size + write). See
// DenseRowScalar.h. Split from the decode side so the two layout-sensitive
// scalar hot paths do not perturb each other's code layout.

#include "bolt/row/dense/DenseRowScalar.h"

#include <cstring>

#include "bolt/row/dense/DenseRowGeneral.h"
#include "bolt/row/dense/IntVarint.h"

namespace bytedance::bolt::row::dense_row::scalar {

template <typename T>
FOLLY_ALWAYS_INLINE void
addIntColumnSizes(const DecodedVector& dec, vector_size_t N, size_t* rowSizes) {
  const auto* raw = dec.data<T>();
  if (dec.isIdentityMapping()) {
    // Flat column. A null rawNulls() means no nulls (mayHaveNulls() is only a
    // conservative upper bound), so this also covers the "may-null flag set
    // without a backing bitmap" case. All int widths handled: int64 directly,
    // int8/int16/int32 via int32.
    const uint64_t* nulls =
        dec.mayHaveNulls() ? dec.base()->rawNulls() : nullptr;
    if (nulls) {
      // SIMD value sizes + branchless null override via the row-indexed
      // validity bitmap.
      detail::addNullableIntColumnSizes(
          raw, nulls, rowSizes, static_cast<size_t>(N));
    } else {
      // Contiguous, no value-nulls: portable xsimd size kernel scattered
      // per row.
      detail::addNoNullIntColumnSizes<T>(raw, rowSizes, static_cast<size_t>(N));
    }
    return;
  }
  if (dec.isConstantMapping()) {
    // Every row maps to the same base value (or all rows are null), so the
    // serialized size is identical — compute it once and splat across rows.
    const bool isNull = dec.mayHaveNulls() && dec.isNullAt(0);
    const int64_t v = isNull ? 0 : static_cast<int64_t>(raw[dec.index(0)]);
    const size_t sz = detail::nullableInt64SerializedSize(v, isNull);
    for (vector_size_t r = 0; r < N; ++r) {
      rowSizes[r] += sz;
    }
    return;
  }
  // General path: dictionary mappings.
  const bool mayNulls = dec.mayHaveNulls();
  for (vector_size_t r = 0; r < N; ++r) {
    const bool isNull = mayNulls && dec.isNullAt(r);
    const int64_t v = isNull ? 0 : static_cast<int64_t>(raw[dec.index(r)]);
    rowSizes[r] += detail::nullableInt64SerializedSize(v, isNull);
  }
}

template <size_t KBYTES>
FOLLY_ALWAYS_INLINE void addFixedColumnSizes(
    vector_size_t N,
    size_t* rowSizes) {
  for (vector_size_t r = 0; r < N; ++r) {
    rowSizes[r] += KBYTES;
  }
}

void addColumnSizes(
    const Type& type,
    const DecodedVector& dec,
    vector_size_t N,
    size_t* rowSizes) {
  switch (type.kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::UNKNOWN:
      addFixedColumnSizes<1>(N, rowSizes);
      return;
    case TypeKind::TINYINT:
      addIntColumnSizes<int8_t>(dec, N, rowSizes);
      return;
    case TypeKind::SMALLINT:
      addIntColumnSizes<int16_t>(dec, N, rowSizes);
      return;
    case TypeKind::INTEGER:
      addIntColumnSizes<int32_t>(dec, N, rowSizes);
      return;
    case TypeKind::BIGINT:
      addIntColumnSizes<int64_t>(dec, N, rowSizes);
      return;
    case TypeKind::REAL:
      addFixedColumnSizes<sizeof(float)>(N, rowSizes);
      return;
    case TypeKind::DOUBLE:
      addFixedColumnSizes<sizeof(double)>(N, rowSizes);
      return;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
      const bool mayNulls = dec.mayHaveNulls();
      for (vector_size_t r = 0; r < N; ++r) {
        if (mayNulls && dec.isNullAt(r)) {
          ++rowSizes[r];
        } else {
          const auto len = dec.valueAt<StringView>(r).size();
          rowSizes[r] +=
              detail::varintSize(static_cast<uint64_t>(len) + 1) + len;
        }
      }
      return;
    }
    case TypeKind::TIMESTAMP: {
      const bool mayNulls = dec.mayHaveNulls();
      for (vector_size_t r = 0; r < N; ++r) {
        if (mayNulls && dec.isNullAt(r)) {
          ++rowSizes[r];
        } else {
          rowSizes[r] += detail::nullableInt64SerializedSize(
              dec.valueAt<Timestamp>(r).toMicros(), false);
        }
      }
      return;
    }
    case TypeKind::HUGEINT: {
      // null folded into the low slot: nullableInt64(zigzag128 low 64), then
      // varint(high 64) if non-null. See detail::writeNullableInt128.
      const bool mayNulls = dec.mayHaveNulls();
      for (vector_size_t r = 0; r < N; ++r) {
        const bool isNull = mayNulls && dec.isNullAt(r);
        rowSizes[r] += detail::nullableInt128SerializedSize(
            isNull ? int128_t{0} : dec.valueAt<int128_t>(r), isNull);
      }
      return;
    }
    default:
      BOLT_UNREACHABLE();
  }
}

template <typename T>
FOLLY_ALWAYS_INLINE void writeIntColumn(
    const DecodedVector& dec,
    vector_size_t N,
    uint8_t** rowCursors) {
  const bool mayNulls = dec.mayHaveNulls();
  const bool identity = dec.isIdentityMapping();
  const auto* raw = dec.data<T>();
  for (vector_size_t r = 0; r < N; ++r) {
    const bool isNull = mayNulls && dec.isNullAt(r);
    const int64_t v = isNull
        ? 0
        : static_cast<int64_t>(identity ? raw[r] : raw[dec.index(r)]);
    rowCursors[r] = writeNullableInt64(v, isNull, rowCursors[r]);
  }
}

void writeColumn(
    const Type& type,
    const DecodedVector& dec,
    vector_size_t N,
    uint8_t** rowCursors) {
  switch (type.kind()) {
    case TypeKind::BOOLEAN: {
      const bool mayNulls = dec.mayHaveNulls();
      for (vector_size_t r = 0; r < N; ++r) {
        if (mayNulls && dec.isNullAt(r)) {
          *rowCursors[r]++ = uint8_t{0};
        } else {
          *rowCursors[r]++ = dec.valueAt<bool>(r) ? uint8_t{2} : uint8_t{1};
        }
      }
      return;
    }
    case TypeKind::UNKNOWN:
      for (vector_size_t r = 0; r < N; ++r) {
        *rowCursors[r]++ = uint8_t{0};
      }
      return;
    case TypeKind::TINYINT:
      writeIntColumn<int8_t>(dec, N, rowCursors);
      return;
    case TypeKind::SMALLINT:
      writeIntColumn<int16_t>(dec, N, rowCursors);
      return;
    case TypeKind::INTEGER:
      writeIntColumn<int32_t>(dec, N, rowCursors);
      return;
    case TypeKind::BIGINT:
      writeIntColumn<int64_t>(dec, N, rowCursors);
      return;
    case TypeKind::REAL: {
      const bool mayNulls = dec.mayHaveNulls();
      const bool identity = dec.isIdentityMapping();
      const auto* raw = dec.data<float>();
      for (vector_size_t r = 0; r < N; ++r) {
        uint32_t b;
        if (mayNulls && dec.isNullAt(r)) {
          b = kNullFloatBits;
        } else {
          const float value = identity ? raw[r] : raw[dec.index(r)];
          std::memcpy(&b, &value, sizeof(b));
          // kNullFloatBits is the canonical quiet NaN. Flipping the low
          // mantissa bit yields another NaN.
          if (FOLLY_UNLIKELY(b == kNullFloatBits)) {
            b ^= 1u;
          }
        }
        std::memcpy(rowCursors[r], &b, sizeof(b));
        rowCursors[r] += sizeof(b);
      }
      return;
    }
    case TypeKind::DOUBLE: {
      const bool mayNulls = dec.mayHaveNulls();
      const bool identity = dec.isIdentityMapping();
      const auto* raw = dec.data<double>();
      for (vector_size_t r = 0; r < N; ++r) {
        uint64_t b;
        if (mayNulls && dec.isNullAt(r)) {
          b = kNullDoubleBits;
        } else {
          const double value = identity ? raw[r] : raw[dec.index(r)];
          std::memcpy(&b, &value, sizeof(b));
          // kNullDoubleBits is the canonical quiet NaN. Flipping the low
          // mantissa bit yields another NaN.
          if (FOLLY_UNLIKELY(b == kNullDoubleBits)) {
            b ^= 1ull;
          }
        }
        std::memcpy(rowCursors[r], &b, sizeof(b));
        rowCursors[r] += sizeof(b);
      }
      return;
    }
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
      const bool mayNulls = dec.mayHaveNulls();
      for (vector_size_t r = 0; r < N; ++r) {
        uint8_t* out = rowCursors[r];
        if (mayNulls && dec.isNullAt(r)) {
          out = writeVarint(0, out);
        } else {
          const auto sv = dec.valueAt<StringView>(r);
          out = writeVarint(static_cast<uint64_t>(sv.size()) + 1, out);
          std::memcpy(out, sv.data(), sv.size());
          out += sv.size();
        }
        rowCursors[r] = out;
      }
      return;
    }
    case TypeKind::TIMESTAMP: {
      const bool mayNulls = dec.mayHaveNulls();
      for (vector_size_t r = 0; r < N; ++r) {
        const bool isNull = mayNulls && dec.isNullAt(r);
        const int64_t v = isNull ? 0 : dec.valueAt<Timestamp>(r).toMicros();
        rowCursors[r] = writeNullableInt64(v, isNull, rowCursors[r]);
      }
      return;
    }
    case TypeKind::HUGEINT: {
      const bool mayNulls = dec.mayHaveNulls();
      for (vector_size_t r = 0; r < N; ++r) {
        const bool isNull = mayNulls && dec.isNullAt(r);
        rowCursors[r] = writeNullableInt128(
            isNull ? int128_t{0} : dec.valueAt<int128_t>(r),
            isNull,
            rowCursors[r]);
      }
      return;
    }
    default:
      BOLT_UNREACHABLE();
  }
}

} // namespace bytedance::bolt::row::dense_row::scalar
