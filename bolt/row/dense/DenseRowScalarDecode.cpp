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

// Scalar-row fast path — DECODE side (read). See DenseRowScalar.h.
//
// Column-at-a-time, row-major reads: for each scalar field, walk the N per-row
// cursors and decode one value from each. This is already the fast shape — it
// skips the SlotView machinery the general decoder uses — and there is no SIMD
// batch decode because varint parsing is inherently sequential. The varint
// readers (readVarint / readNullableInt64) use the BMI2 short-fast-path. The
// input is always marker-less with no top-level null rows, so there is no
// per-row row-null filtering here.
//
// Split from the encode side so the two layout-sensitive scalar hot paths do
// not perturb each other's code layout.

#include "bolt/row/dense/DenseRowScalar.h"

#include <cstring>
#include <limits>
#include <type_traits>
#include <vector>

#include "bolt/row/dense/DenseRowGeneral.h"
#include "bolt/row/dense/IntVarint.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::row::dense_row::scalar {

template <typename T>
void readIntColumn(
    FlatVector<T>* flat,
    vector_size_t N,
    folly::Range<RowCursor*> cursors) {
  auto* raw = flat->mutableRawValues();
  for (vector_size_t r = 0; r < N; ++r) {
    RowCursor& c = cursors[r];
    bool isNull{false};
    int64_t v{0};
    [[maybe_unused]] const bool ok = readNullableInt64(c.cur, c.end, isNull, v);
    BOLT_DCHECK(ok, "DenseRow: malformed integer value at row {}", r);
    if (isNull) {
      flat->setNull(r, true);
    } else {
      if constexpr (!std::is_same_v<T, int64_t>) {
        BOLT_DCHECK(
            v >= static_cast<int64_t>(std::numeric_limits<T>::min()) &&
                v <= static_cast<int64_t>(std::numeric_limits<T>::max()),
            "DenseRow: integer value out of range at row {}: {}",
            r,
            v);
      }
      flat->setNull(r, false);
      raw[r] = static_cast<T>(v);
    }
  }
}

void readColumn(
    const Type& type,
    BaseVector& dst,
    vector_size_t N,
    folly::Range<RowCursor*> cursors) {
  switch (type.kind()) {
    case TypeKind::BOOLEAN: {
      auto* flat = dst.asUnchecked<FlatVector<bool>>();
      for (vector_size_t r = 0; r < N; ++r) {
        RowCursor& c = cursors[r];
        uint64_t v{0};
        [[maybe_unused]] const bool ok = readVarint(c.cur, c.end, v);
        BOLT_DCHECK(ok, "DenseRow: malformed boolean at row {}", r);
        if (v == 0) {
          flat->setNull(r, true);
        } else {
          BOLT_DCHECK(
              v <= 2, "DenseRow: invalid boolean encoding at row {}: {}", r, v);
          flat->setNull(r, false);
          flat->set(r, v == 2);
        }
      }
      return;
    }
    case TypeKind::UNKNOWN:
      for (vector_size_t r = 0; r < N; ++r) {
        RowCursor& c = cursors[r];
        uint64_t v{0};
        [[maybe_unused]] const bool ok = readVarint(c.cur, c.end, v);
        BOLT_DCHECK(ok, "DenseRow: malformed unknown-type marker at row {}", r);
        BOLT_DCHECK(
            v == 0, "DenseRow: unknown-type expected null marker at row {}", r);
        dst.setNull(r, true);
      }
      return;
    case TypeKind::TINYINT:
      readIntColumn<int8_t>(dst.asUnchecked<FlatVector<int8_t>>(), N, cursors);
      return;
    case TypeKind::SMALLINT:
      readIntColumn<int16_t>(
          dst.asUnchecked<FlatVector<int16_t>>(), N, cursors);
      return;
    case TypeKind::INTEGER:
      readIntColumn<int32_t>(
          dst.asUnchecked<FlatVector<int32_t>>(), N, cursors);
      return;
    case TypeKind::BIGINT:
      readIntColumn<int64_t>(
          dst.asUnchecked<FlatVector<int64_t>>(), N, cursors);
      return;
    case TypeKind::REAL: {
      auto* flat = dst.asUnchecked<FlatVector<float>>();
      auto* raw = flat->mutableRawValues();
      for (vector_size_t r = 0; r < N; ++r) {
        RowCursor& c = cursors[r];
        BOLT_DCHECK(
            static_cast<size_t>(c.end - c.cur) >= sizeof(uint32_t),
            "DenseRow: truncated real at row {}",
            r);
        uint32_t b;
        std::memcpy(&b, c.cur, sizeof(b));
        c.cur += sizeof(b);
        if (b == kNullFloatBits) {
          flat->setNull(r, true);
        } else {
          flat->setNull(r, false);
          if (FOLLY_UNLIKELY(b == (kNullFloatBits ^ 1u))) {
            b ^= 1u;
          }
          std::memcpy(raw + r, &b, sizeof(b));
        }
      }
      return;
    }
    case TypeKind::DOUBLE: {
      auto* flat = dst.asUnchecked<FlatVector<double>>();
      auto* raw = flat->mutableRawValues();
      for (vector_size_t r = 0; r < N; ++r) {
        RowCursor& c = cursors[r];
        BOLT_DCHECK(
            static_cast<size_t>(c.end - c.cur) >= sizeof(uint64_t),
            "DenseRow: truncated double at row {}",
            r);
        uint64_t b;
        std::memcpy(&b, c.cur, sizeof(b));
        c.cur += sizeof(b);
        if (b == kNullDoubleBits) {
          flat->setNull(r, true);
        } else {
          flat->setNull(r, false);
          if (FOLLY_UNLIKELY(b == (kNullDoubleBits ^ 1ull))) {
            b ^= 1ull;
          }
          std::memcpy(raw + r, &b, sizeof(b));
        }
      }
      return;
    }
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
      auto* flat = dst.asUnchecked<FlatVector<StringView>>();
      auto* rawValues = flat->mutableRawValues();
      // Inline values (<= kInlineSize) live in the StringView itself; longer
      // ones are copied into the vector's string buffer, carved from fixed-size
      // chunks (a string larger than a chunk gets its own exact allocation).
      // Fixed chunks avoid both a look-ahead scan of the remaining cursor bytes
      // and a getRawStringBufferWithSpace call per non-inline value.
      constexpr size_t kStringChunk = 32 * 1024;
      char* heap = nullptr;
      size_t heapRemaining = 0;
      for (vector_size_t r = 0; r < N; ++r) {
        RowCursor& c = cursors[r];
        uint64_t lenPlus{0};
        // Always-on: the length and payload-bounds guards gate a memcpy of a
        // wire-controlled length, so a corrupt/truncated value must fail loudly
        // rather than over-read the input buffer (matches the general decoder).
        BOLT_USER_CHECK(
            readVarint(c.cur, c.end, lenPlus),
            "DenseRow: malformed varchar length at row {}",
            r);
        if (lenPlus == 0) {
          flat->setNull(r, true);
          continue;
        }
        const auto len = static_cast<size_t>(lenPlus - 1);
        BOLT_USER_CHECK(
            static_cast<size_t>(c.end - c.cur) >= len,
            "DenseRow: truncated varchar payload at row {}",
            r);
        flat->setNull(r, false);
        if (len <= StringView::kInlineSize) {
          rawValues[r] = StringView(reinterpret_cast<const char*>(c.cur), len);
        } else {
          if (heapRemaining < len) {
            const size_t alloc = len > kStringChunk ? len : kStringChunk;
            heap = flat->getRawStringBufferWithSpace(alloc, /*exactSize=*/true);
            heapRemaining = alloc;
          }
          std::memcpy(heap, c.cur, len);
          rawValues[r] = StringView(heap, len);
          heap += len;
          heapRemaining -= len;
        }
        c.cur += len;
      }
      return;
    }
    case TypeKind::TIMESTAMP: {
      auto* flat = dst.asUnchecked<FlatVector<Timestamp>>();
      for (vector_size_t r = 0; r < N; ++r) {
        RowCursor& c = cursors[r];
        bool isNull{false};
        int64_t micros{0};
        [[maybe_unused]] const bool ok =
            readNullableInt64(c.cur, c.end, isNull, micros);
        BOLT_DCHECK(ok, "DenseRow: malformed timestamp at row {}", r);
        if (isNull) {
          flat->setNull(r, true);
        } else {
          flat->setNull(r, false);
          flat->set(r, Timestamp::fromMicrosNoError(micros));
        }
      }
      return;
    }
    case TypeKind::HUGEINT: {
      auto* flat = dst.asUnchecked<FlatVector<int128_t>>();
      auto* raw = flat->mutableRawValues();
      for (vector_size_t r = 0; r < N; ++r) {
        RowCursor& c = cursors[r];
        bool isNull{false};
        int128_t v{0};
        [[maybe_unused]] const bool ok =
            readNullableInt128(c.cur, c.end, isNull, v);
        BOLT_DCHECK(ok, "DenseRow: malformed hugeint at row {}", r);
        if (isNull) {
          flat->setNull(r, true);
        } else {
          flat->setNull(r, false);
          raw[r] = v;
        }
      }
      return;
    }
    default:
      BOLT_UNREACHABLE();
  }
}

} // namespace bytedance::bolt::row::dense_row::scalar
