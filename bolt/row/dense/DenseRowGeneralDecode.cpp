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

// Decode (deserialize) kernels for the level-hoisted dense row format.
//
// These nested-container decode loops are pathologically sensitive to machine-
// code layout: on byte-identical source, small shifts in surrounding code swing
// individual cases (nested ARRAY/MAP/long-string deserialize) by ~10-14% purely
// from cache-line / branch-predictor aliasing — see the intra-TU layout note in
// DenseRow.cpp. They live in their own translation unit so that unrelated
// edits to the encode/serializer code no longer re-roll that layout lottery;
// the decode layout is now determined solely by this file. Do not merge these
// kernels back into another TU, and re-run dense_row_serialize_benchmark
// (dense_deserialize_*) after any change here.

#include "bolt/row/dense/DenseRowGeneral.h"

#include <cstring>
#include <limits>
#include <type_traits>
#include <vector>

#include <folly/small_vector.h>

#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::row::dense_row {

// =============================================================================
// Decode side
// =============================================================================

template <typename T>
void decodeIntegerBatch(
    BaseVector& dst,
    SlotView out,
    folly::Range<RowCursor*> cursors,
    const uint64_t* rowNulls) {
  auto* flat = dst.asUnchecked<FlatVector<T>>();
  auto* raw = flat->mutableRawValues();
  const auto N = static_cast<vector_size_t>(cursors.size());
  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    RowCursor& c = cursors[r];
    forEachLivePos(out, r, [&](uint32_t p) {
      bool isNull{false};
      int64_t v{0};
      BOLT_USER_CHECK(
          readNullableInt64(c.cur, c.end, isNull, v),
          "DenseRow: malformed integer value at row {}",
          r);
      if (isNull) {
        flat->setNull(static_cast<vector_size_t>(p), true);
      } else {
        if constexpr (!std::is_same_v<T, int64_t>) {
          BOLT_USER_CHECK(
              v >= static_cast<int64_t>(std::numeric_limits<T>::min()) &&
                  v <= static_cast<int64_t>(std::numeric_limits<T>::max()),
              "DenseRow: integer value out of range at row {}: {}",
              r,
              v);
        }
        flat->setNull(static_cast<vector_size_t>(p), false);
        raw[p] = static_cast<T>(v);
      }
    });
  }
}

void decodeBooleanBatch(
    BaseVector& dst,
    SlotView out,
    folly::Range<RowCursor*> cursors,
    const uint64_t* rowNulls) {
  auto* flat = dst.asUnchecked<FlatVector<bool>>();
  const auto N = static_cast<vector_size_t>(cursors.size());
  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    RowCursor& c = cursors[r];
    forEachLivePos(out, r, [&](uint32_t p) {
      uint64_t v{0};
      BOLT_USER_CHECK(
          readVarint(c.cur, c.end, v),
          "DenseRow: malformed boolean at row {}",
          r);
      if (v == 0) {
        flat->setNull(static_cast<vector_size_t>(p), true);
      } else {
        BOLT_USER_CHECK(
            v <= 2, "DenseRow: invalid boolean encoding at row {}: {}", r, v);
        flat->setNull(static_cast<vector_size_t>(p), false);
        flat->set(static_cast<vector_size_t>(p), v == 2);
      }
    });
  }
}

void decodeRealBatch(
    BaseVector& dst,
    SlotView out,
    folly::Range<RowCursor*> cursors,
    const uint64_t* rowNulls) {
  auto* flat = dst.asUnchecked<FlatVector<float>>();
  auto* raw = flat->mutableRawValues();
  const auto N = static_cast<vector_size_t>(cursors.size());
  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    RowCursor& c = cursors[r];
    forEachLivePos(out, r, [&](uint32_t p) {
      BOLT_USER_CHECK(
          static_cast<size_t>(c.end - c.cur) >= sizeof(uint32_t),
          "DenseRow: truncated real at row {}",
          r);
      uint32_t b;
      std::memcpy(&b, c.cur, sizeof(b));
      c.cur += sizeof(b);
      if (b == kNullFloatBits) {
        flat->setNull(static_cast<vector_size_t>(p), true);
      } else {
        flat->setNull(static_cast<vector_size_t>(p), false);
        if (FOLLY_UNLIKELY(b == (kNullFloatBits ^ 1u))) {
          b ^= 1u;
        }
        std::memcpy(raw + p, &b, sizeof(b));
      }
    });
  }
}

void decodeDoubleBatch(
    BaseVector& dst,
    SlotView out,
    folly::Range<RowCursor*> cursors,
    const uint64_t* rowNulls) {
  auto* flat = dst.asUnchecked<FlatVector<double>>();
  auto* raw = flat->mutableRawValues();
  const auto N = static_cast<vector_size_t>(cursors.size());
  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    RowCursor& c = cursors[r];
    forEachLivePos(out, r, [&](uint32_t p) {
      BOLT_USER_CHECK(
          static_cast<size_t>(c.end - c.cur) >= sizeof(uint64_t),
          "DenseRow: truncated double at row {}",
          r);
      uint64_t b;
      std::memcpy(&b, c.cur, sizeof(b));
      c.cur += sizeof(b);
      if (b == kNullDoubleBits) {
        flat->setNull(static_cast<vector_size_t>(p), true);
      } else {
        flat->setNull(static_cast<vector_size_t>(p), false);
        if (FOLLY_UNLIKELY(b == (kNullDoubleBits ^ 1ull))) {
          b ^= 1ull;
        }
        std::memcpy(raw + p, &b, sizeof(b));
      }
    });
  }
}

// Mirror of encodeHugeintBatch: nullableInt64(low 64 of zigzag128) carrying the
// null marker, then (if non-null) varint(high 64). See
// detail::writeNullableInt128.
void decodeHugeintBatch(
    BaseVector& dst,
    SlotView out,
    folly::Range<RowCursor*> cursors,
    const uint64_t* rowNulls) {
  auto* flat = dst.asUnchecked<FlatVector<int128_t>>();
  auto* raw = flat->mutableRawValues();
  const auto N = static_cast<vector_size_t>(cursors.size());
  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    RowCursor& c = cursors[r];
    forEachLivePos(out, r, [&](uint32_t p) {
      bool isNull{false};
      int128_t v{0};
      BOLT_USER_CHECK(
          readNullableInt128(c.cur, c.end, isNull, v),
          "DenseRow: malformed hugeint at row {}",
          r);
      flat->setNull(static_cast<vector_size_t>(p), isNull);
      if (!isNull) {
        raw[p] = v;
      }
    });
  }
}

void decodeVarcharBatch(
    BaseVector& dst,
    SlotView out,
    folly::Range<RowCursor*> cursors,
    const uint64_t* rowNulls) {
  auto* flat = dst.asUnchecked<FlatVector<StringView>>();
  // Write StringViews via raw pointer to avoid flat->set()'s extra memcpy
  // for non-inline strings (the payload already lives in our buffer).
  auto* rawValues = flat->mutableRawValues();
  char* buf{nullptr};
  size_t bufRemaining = 0;
  const auto N = static_cast<vector_size_t>(cursors.size());

  // Wire layout per row segment: length stream then payload stream.
  // Decode must mirror that split. Stash per-slot length (or -1 for null)
  // so the payload pass can do the placement.
  folly::small_vector<int32_t, 32> lengths;
  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    RowCursor& c = cursors[r];
    lengths.clear();
    forEachLivePos(out, r, [&](uint32_t /*p*/) {
      uint64_t v{0};
      BOLT_USER_CHECK(
          readVarint(c.cur, c.end, v),
          "DenseRow: malformed varchar length at row {}",
          r);
      lengths.push_back(v == 0 ? -1 : static_cast<int32_t>(v - 1));
    });

    size_t idx = 0;
    forEachLivePos(out, r, [&](uint32_t p) {
      const int32_t len = lengths[idx++];
      if (len < 0) {
        flat->setNull(static_cast<vector_size_t>(p), true);
        return;
      }
      const auto ulen = static_cast<size_t>(len);
      BOLT_USER_CHECK(
          static_cast<size_t>(c.end - c.cur) >= ulen,
          "DenseRow: truncated varchar payload at row {}",
          r);
      if (ulen <= StringView::kInlineSize) {
        rawValues[p] = StringView(reinterpret_cast<const char*>(c.cur), ulen);
      } else {
        if (bufRemaining < ulen) {
          // Upper bound: bytes remaining in this row + every later non-null
          // row's bytes.
          size_t needed = static_cast<size_t>(c.end - c.cur);
          for (vector_size_t j = r + 1; j < N; ++j) {
            if (rowNulls && bits::isBitNull(rowNulls, j)) {
              continue;
            }
            needed += static_cast<size_t>(cursors[j].end - cursors[j].cur);
          }
          buf = flat->getRawStringBufferWithSpace(needed, true);
          bufRemaining = needed;
        }
        std::memcpy(buf, c.cur, ulen);
        rawValues[p] = StringView(buf, ulen);
        buf += ulen;
        bufRemaining -= ulen;
      }
      c.cur += ulen;
    });
  }
}

void decodeTimestampBatch(
    BaseVector& dst,
    SlotView out,
    folly::Range<RowCursor*> cursors,
    const uint64_t* rowNulls) {
  auto* flat = dst.asUnchecked<FlatVector<Timestamp>>();
  const auto N = static_cast<vector_size_t>(cursors.size());
  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    RowCursor& c = cursors[r];
    forEachLivePos(out, r, [&](uint32_t p) {
      bool isNull{false};
      int64_t micros{0};
      BOLT_USER_CHECK(
          readNullableInt64(c.cur, c.end, isNull, micros),
          "DenseRow: malformed timestamp at row {}",
          r);
      if (isNull) {
        flat->setNull(static_cast<vector_size_t>(p), true);
      } else {
        flat->setNull(static_cast<vector_size_t>(p), false);
        flat->set(
            static_cast<vector_size_t>(p),
            Timestamp::fromMicrosNoError(micros));
      }
    });
  }
}

void decodeNullColumnBatch(
    BaseVector& dst,
    SlotView out,
    folly::Range<RowCursor*> cursors,
    const uint64_t* rowNulls) {
  const auto N = static_cast<vector_size_t>(cursors.size());
  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    RowCursor& c = cursors[r];
    forEachLivePos(out, r, [&](uint32_t p) {
      uint64_t v{0};
      BOLT_USER_CHECK(
          readVarint(c.cur, c.end, v),
          "DenseRow: malformed unknown-type marker at row {}",
          r);
      BOLT_USER_CHECK(
          v == 0, "DenseRow: unknown-type expected null marker at row {}", r);
      dst.setNull(static_cast<vector_size_t>(p), true);
    });
  }
}

// Pass 1 for ARRAY/MAP decode: read cardinality varints in row/parent-slot
// order. Allocates one (off, sz) child-slot entry per parent slot — null
// parent slots get (0, 0) entries as placeholders for index alignment with
// the parent-slot iteration. We also write the parent ArrayVector/MapVector
// offsets/sizes/nulls at this stage since the layout is already known.
// Callback `assign(pos, isNull, off, sz)` is passed as a template parameter so
// it inlines (no std::function indirect call per element in the decode hot
// loop).
template <typename Assign>
void decodeArrayLikePass1(
    SlotView out,
    folly::Range<RowCursor*> cursors,
    const uint64_t* rowNulls,
    vector_size_t childBase,
    folly::small_vector<SlotRange, 32>& childSlots,
    folly::small_vector<uint32_t, 16>& childBoundaries,
    vector_size_t& totalChildren,
    const char* what,
    Assign assign) {
  const auto N = static_cast<vector_size_t>(cursors.size());
  childBoundaries.resize(N + 1);
  childBoundaries[0] = 0;
  vector_size_t writeHead = childBase;
  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      childBoundaries[r + 1] = static_cast<uint32_t>(childSlots.size());
      continue;
    }
    RowCursor& c = cursors[r];
    forEachLivePos(out, r, [&](uint32_t p) {
      uint64_t e{0};
      BOLT_USER_CHECK(
          readVarint(c.cur, c.end, e),
          "DenseRow: malformed {} cardinality at row {}",
          what,
          r);
      if (e == 0) {
        assign(static_cast<vector_size_t>(p), /*isNull=*/true, 0, 0);
      } else {
        // Bound the cardinality before the (narrowing) cast: each element
        // consumes >= 1 byte further in this row's blob, so a cardinality
        // larger than the bytes remaining for this row is corrupt input.
        // Guards against overflowing writeHead/totalChildren and the
        // subsequent child-vector resize on malformed wire.
        const uint64_t card = e - 1;
        BOLT_USER_CHECK_LE(
            card,
            static_cast<uint64_t>(c.end - c.cur),
            "DenseRow: {} cardinality {} exceeds remaining bytes at row {}",
            what,
            card,
            r);
        const auto sz = static_cast<vector_size_t>(card);
        assign(static_cast<vector_size_t>(p), /*isNull=*/false, writeHead, sz);
        if (sz > 0) {
          childSlots.push_back(
              {static_cast<uint32_t>(writeHead), static_cast<uint32_t>(sz)});
        }
        writeHead += sz;
      }
    });
    childBoundaries[r + 1] = static_cast<uint32_t>(childSlots.size());
  }
  totalChildren = writeHead - childBase;
}

void decodeArrayBatch(
    const Type& type,
    BaseVector& dst,
    SlotView out,
    folly::Range<RowCursor*> cursors,
    const uint64_t* rowNulls) {
  auto* arr = dst.asUnchecked<ArrayVector>();
  auto& elements = *arr->elements();
  const vector_size_t childBase = elements.size();

  folly::small_vector<SlotRange, 32> childSlots;
  folly::small_vector<uint32_t, 16> childBoundaries;
  vector_size_t totalChildren = 0;
  decodeArrayLikePass1(
      out,
      cursors,
      rowNulls,
      childBase,
      childSlots,
      childBoundaries,
      totalChildren,
      "array",
      [&](vector_size_t pos, bool isNull, vector_size_t off, vector_size_t sz) {
        arr->setNull(pos, isNull);
        arr->setOffsetAndSize(pos, off, sz);
      });

  elements.resize(childBase + totalChildren);

  SlotView childView{
      {childSlots.data(), childSlots.size()},
      {childBoundaries.data(), childBoundaries.size()},
      nullptr};
  decodeColumnBatch(*type.childAt(0), elements, childView, cursors, rowNulls);
}

void decodeMapBatch(
    const Type& type,
    BaseVector& dst,
    SlotView out,
    folly::Range<RowCursor*> cursors,
    const uint64_t* rowNulls) {
  auto* m = dst.asUnchecked<MapVector>();
  auto& keys = *m->mapKeys();
  auto& values = *m->mapValues();
  const vector_size_t childBase = keys.size();

  folly::small_vector<SlotRange, 32> childSlots;
  folly::small_vector<uint32_t, 16> childBoundaries;
  vector_size_t totalChildren = 0;
  decodeArrayLikePass1(
      out,
      cursors,
      rowNulls,
      childBase,
      childSlots,
      childBoundaries,
      totalChildren,
      "map",
      [&](vector_size_t pos, bool isNull, vector_size_t off, vector_size_t sz) {
        m->setNull(pos, isNull);
        m->setOffsetAndSize(pos, off, sz);
      });

  keys.resize(childBase + totalChildren);
  values.resize(childBase + totalChildren);

  SlotView childView{
      {childSlots.data(), childSlots.size()},
      {childBoundaries.data(), childBoundaries.size()},
      nullptr};
  decodeColumnBatch(*type.childAt(0), keys, childView, cursors, rowNulls);
  decodeColumnBatch(*type.childAt(1), values, childView, cursors, rowNulls);
}

void decodeRowBatch(
    const Type& type,
    BaseVector& dst,
    SlotView out,
    folly::Range<RowCursor*> cursors,
    const uint64_t* rowNulls,
    bool readMarker) {
  auto* row = dst.asUnchecked<RowVector>();
  const auto N = static_cast<vector_size_t>(cursors.size());

  vector_size_t bitmapBits = 0;
  for (const auto& sr : out.slots) {
    const auto endPos = static_cast<vector_size_t>(sr.base + sr.count);
    if (endPos > bitmapBits) {
      bitmapBits = endPos;
    }
  }

  std::vector<uint64_t> childNullsBuf;
  const uint64_t* childParentNulls = out.parentNulls;
  if (bitmapBits > 0) {
    childNullsBuf.assign(bits::nwords(bitmapBits), ~uint64_t{0});
    childParentNulls = childNullsBuf.data();
  }

  if (readMarker) {
    for (vector_size_t r = 0; r < N; ++r) {
      if (rowNulls && bits::isBitNull(rowNulls, r)) {
        continue;
      }
      RowCursor& c = cursors[r];
      const auto lo = out.rowBoundaries[r];
      const auto hi = out.rowBoundaries[r + 1];
      for (uint32_t i = lo; i < hi; ++i) {
        const auto& sr = out.slots[i];
        const uint32_t endPos = sr.base + sr.count;
        for (uint32_t p = sr.base; p < endPos; ++p) {
          const bool parentSaysNull = out.parentNulls &&
              bits::isBitNull(out.parentNulls, static_cast<int32_t>(p));
          if (parentSaysNull) {
            if (!childNullsBuf.empty()) {
              bits::setNull(childNullsBuf.data(), p, true);
            }
            continue;
          }
          uint64_t v{0};
          BOLT_USER_CHECK(
              readVarint(c.cur, c.end, v),
              "DenseRow: malformed row null marker at row {}",
              r);
          if (v == 0) {
            row->setNull(static_cast<vector_size_t>(p), true);
            if (!childNullsBuf.empty()) {
              bits::setNull(childNullsBuf.data(), p, true);
            }
          } else {
            BOLT_USER_CHECK(
                v == 1,
                "DenseRow: invalid row null marker at row {}: {}",
                r,
                v);
            row->setNull(static_cast<vector_size_t>(p), false);
          }
        }
      }
    }
  } else {
    // No marker on wire — caller asserts every position is non-null.
    // Mirror the null-tracking the marker pass would have done for a
    // non-null row so descended children see consistent parentNulls.
    for (vector_size_t r = 0; r < N; ++r) {
      if (rowNulls && bits::isBitNull(rowNulls, r)) {
        continue;
      }
      const auto lo = out.rowBoundaries[r];
      const auto hi = out.rowBoundaries[r + 1];
      for (uint32_t i = lo; i < hi; ++i) {
        const auto& sr = out.slots[i];
        const uint32_t endPos = sr.base + sr.count;
        for (uint32_t p = sr.base; p < endPos; ++p) {
          const bool parentSaysNull = out.parentNulls &&
              bits::isBitNull(out.parentNulls, static_cast<int32_t>(p));
          if (parentSaysNull) {
            if (!childNullsBuf.empty()) {
              bits::setNull(childNullsBuf.data(), p, true);
            }
            continue;
          }
          row->setNull(static_cast<vector_size_t>(p), false);
        }
      }
    }
  }

  SlotView childView{out.slots, out.rowBoundaries, childParentNulls};
  const auto fieldCount = type.size();
  for (size_t f = 0; f < fieldCount; ++f) {
    decodeColumnBatch(
        *type.childAt(f), *row->childAt(f), childView, cursors, rowNulls);
  }
}

void decodeColumnBatch(
    const Type& type,
    BaseVector& dst,
    SlotView out,
    folly::Range<RowCursor*> cursors,
    const uint64_t* rowNulls) {
  switch (type.kind()) {
    case TypeKind::BOOLEAN:
      decodeBooleanBatch(dst, out, cursors, rowNulls);
      return;
    case TypeKind::TINYINT:
      decodeIntegerBatch<int8_t>(dst, out, cursors, rowNulls);
      return;
    case TypeKind::SMALLINT:
      decodeIntegerBatch<int16_t>(dst, out, cursors, rowNulls);
      return;
    case TypeKind::INTEGER:
      decodeIntegerBatch<int32_t>(dst, out, cursors, rowNulls);
      return;
    case TypeKind::BIGINT:
      decodeIntegerBatch<int64_t>(dst, out, cursors, rowNulls);
      return;
    case TypeKind::REAL:
      decodeRealBatch(dst, out, cursors, rowNulls);
      return;
    case TypeKind::DOUBLE:
      decodeDoubleBatch(dst, out, cursors, rowNulls);
      return;
    case TypeKind::HUGEINT:
      decodeHugeintBatch(dst, out, cursors, rowNulls);
      return;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      decodeVarcharBatch(dst, out, cursors, rowNulls);
      return;
    case TypeKind::TIMESTAMP:
      decodeTimestampBatch(dst, out, cursors, rowNulls);
      return;
    case TypeKind::ARRAY:
      decodeArrayBatch(type, dst, out, cursors, rowNulls);
      return;
    case TypeKind::MAP:
      decodeMapBatch(type, dst, out, cursors, rowNulls);
      return;
    case TypeKind::ROW:
      decodeRowBatch(type, dst, out, cursors, rowNulls);
      return;
    case TypeKind::UNKNOWN:
      decodeNullColumnBatch(dst, out, cursors, rowNulls);
      return;
    default:
      BOLT_UNREACHABLE();
  }
}

} // namespace bytedance::bolt::row::dense_row
