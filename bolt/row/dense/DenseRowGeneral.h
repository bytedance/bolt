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

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include <folly/CPortability.h>
#include <folly/Range.h>
#include <folly/small_vector.h>

#include "bolt/common/base/Nulls.h"
#include "bolt/row/dense/IntVarint.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/DecodedVector.h"

// Internal declarations for the GENERAL (non-flat) dense-row codec: the
// column-batch machinery that handles arbitrary nesting (ARRAY/MAP/ROW) and
// dictionary/constant inputs. Shared between its two implementation TUs —
// DenseRowGeneralEncode.cpp and DenseRowGeneralDecode.cpp — and included by the
// DenseRow public API layer (DenseRow.cpp) and the scalar fast path
// (DenseRowScalar*.cpp) for the common varint/slot helpers below.
//
// Encode and decode live in separate TUs so their (cache-alignment-sensitive)
// machine-code layout is not perturbed by unrelated edits to the other side —
// see the note on intra-TU layout sensitivity in DenseRow.cpp.
namespace bytedance::bolt::row::dense_row {

// The BMI2 fast path is selected at compile time inside IntVarint.h (gated by
// the x86_64 `#if`), so these are just the detail helpers under this namespace.
using detail::readNullableInt128;
using detail::readNullableInt64;
using detail::readVarint;
using detail::writeNullableInt128;
using detail::writeNullableInt64;
using detail::writeVarint;

// Null sentinels for REAL/DOUBLE: a non-null value whose raw bits collide with
// the sentinel is bit-flipped on encode and restored on decode.
constexpr uint32_t kNullFloatBits{0x7fc00000U};
constexpr uint64_t kNullDoubleBits{0x7ff8000000000000ULL};

// =============================================================================
// Shared slot machinery for column-batch encode/decode.
// =============================================================================
//
// Each call processes N source rows. At every recursion level, each source row
// contributes zero or more contiguous slot ranges in the level's vector.
// SlotView.slots is a flat array of (base, count) ranges; the entries for
// source row r occupy slots[rowBoundaries[r]..rowBoundaries[r+1]).
//
// POSITION SPACE: slot positions index the CURRENT level's vector — for a
// nested ARRAY/MAP level that is the child elements vector's own position
// space (built from the parent's rawOffsets/rawSizes), NOT the parent's. The
// top-level SlotView indexes the top vector ({r, 1} per row). `parentNulls`, if
// set, is indexed by these same current-level positions. A leaf encoder maps a
// position `p` to the decoded value via `plan.decoded` (identity, or
// `decoded.index(p)` for dictionary/constant inputs) — so `p` is the decoded
// vector's position, and rawOffsets/rawSizes for ARRAY/MAP are read at the
// decoded index.
//
// Multiple ranges per row are necessary because ArrayVector/MapVector input can
// have non-contiguous child layouts (gaps between adjacent parent slots), so
// each non-null parent slot contributes its own child range. Decoded output
// vectors are always packed contiguously, so on decode each source row's child
// ranges may happen to be back-to-back, but the representation stays uniform.
struct SlotRange {
  uint32_t base;
  uint32_t count;
};

struct SlotView {
  folly::Range<const SlotRange*> slots;
  folly::Range<const uint32_t*> rowBoundaries; // size N+1
  // Per-position null bitmap inherited from ancestor ROWs. Indexed by the
  // current level's vector positions. nullptr means no filter.
  const uint64_t* parentNulls = nullptr;
};

struct RowCursor {
  const uint8_t* cur;
  const uint8_t* end;
};

// Iterate over a single source row's live positions. Walks every slot range
// belonging to row r and every position inside it, skipping positions covered
// by parentNulls.
template <typename F>
FOLLY_ALWAYS_INLINE void forEachLivePos(SlotView v, vector_size_t r, F f) {
  const uint64_t* nulls = v.parentNulls;
  const auto* slots = v.slots.data();
  const auto lo = v.rowBoundaries[r];
  const auto hi = v.rowBoundaries[r + 1];
  if (!nulls) {
    for (uint32_t i = lo; i < hi; ++i) {
      const auto& sr = slots[i];
      const uint32_t end = sr.base + sr.count;
      for (uint32_t p = sr.base; p < end; ++p) {
        f(p);
      }
    }
  } else {
    for (uint32_t i = lo; i < hi; ++i) {
      const auto& sr = slots[i];
      const uint32_t end = sr.base + sr.count;
      for (uint32_t p = sr.base; p < end; ++p) {
        if (bits::isBitNull(nulls, static_cast<int32_t>(p))) {
          continue;
        }
        f(p);
      }
    }
  }
}

// Top-level SlotView covering every position [0, rowCount): one {r, 1} slot per
// source row. Used by both serialize and deserialize entry points.
struct TopSlotView {
  std::vector<SlotRange> slots;
  std::vector<uint32_t> boundaries;

  SlotView view() {
    return SlotView{
        {slots.data(), slots.size()},
        {boundaries.data(), boundaries.size()},
        nullptr};
  }
};

// TODO delete TopSlotView
inline TopSlotView makeTopView(vector_size_t rowCount) {
  TopSlotView tv;
  tv.slots.resize(rowCount);
  tv.boundaries.resize(rowCount + 1);
  for (vector_size_t r = 0; r < rowCount; ++r) {
    tv.slots[r] = {static_cast<uint32_t>(r), 1u};
    tv.boundaries[r] = static_cast<uint32_t>(r);
  }
  tv.boundaries[rowCount] = static_cast<uint32_t>(rowCount);
  return tv;
}

// =============================================================================
// ENCODE — column-batch encode kernels (DenseRowGeneralEncode.cpp).
// =============================================================================

// Backing storage for one nested ARRAY/MAP level's child SlotView: the child
// slot ranges + per-source-row boundaries. Built by the SizeSink pass and read
// back by the WriteSink pass (see ColumnPlan::childSlots).
struct SlotTreeNode {
  folly::small_vector<SlotRange, 32> slots;
  folly::small_vector<uint32_t, 16> boundaries;
};

// One node of the per-row encode plan: a column at one nesting level. Caches
// that column's DecodedVector (so reads see through dictionary/constant
// wrapping) and, for nested types, its child plans. A flat tagged struct (no
// inheritance): `kind` selects how `children` is interpreted —
//   ARRAY -> {elements}, MAP -> {keys, values}, ROW -> fields, scalar -> {}.
// `buildPlan` produces the tree once; both the size and write passes reuse it.
// The concrete ArrayVector/MapVector/RowVector base is recovered at the use
// site via decoded.base()->as<...>().
struct ColumnPlan {
  TypeKind kind{TypeKind::UNKNOWN};
  DecodedVector decoded;
  bool mayHaveNulls{false};
  bool isNullColumn{false};
  std::vector<ColumnPlan> children;
  // The vector this node's `decoded` reads, held so the node is self-contained:
  // both `decoded` and the buffers it points into stay valid for the node's
  // lifetime, independent of who else references the input. Null only for an
  // all-null (missing) ROW child.
  VectorPtr source;
  // ARRAY/MAP only: the child SlotView's storage, built into the plan tree by
  // the SizeSink pass and replayed by the WriteSink pass — so each level reads
  // its own slots straight off the tree (no shared cursor/scratch between
  // passes). `mutable` because the (size) pass fills it through a `const`
  // ColumnPlan&; it is a derived cache, not part of the plan's identity.
  mutable SlotTreeNode childSlots;
};

ColumnPlan buildPlan(const TypePtr& type, const VectorPtr& vector);

// A "sink" abstracts the size pass vs the write pass: encodeColumnBatch is
// templated on it so both passes share one implementation (byte counts and
// bytes-written cannot drift). SizeSink accumulates a byte count; WriteSink
// writes bytes through a moving cursor.
//
// The size pass does not naively walk every value: fixed-width leaves
// (BOOLEAN/REAL/DOUBLE) add count*width analytically, and integer leaves use
// the SIMD-batched sumNullableIntSizes; only variable-length leaves (VARCHAR)
// are walked per value, which is unavoidable. The SizeSink pass also builds the
// slot tree that the write pass reuses for nested ARRAY/MAP, so it is not just
// a size computation that could be replaced by a closed-form estimate.
struct SizeSink {
  size_t bytes{0};

  FOLLY_ALWAYS_INLINE void putVarint(uint64_t v) {
    bytes += detail::varintSize(v);
  }
  FOLLY_ALWAYS_INLINE void putNullableInt64(int64_t v, bool isNull) {
    bytes += detail::nullableInt64SerializedSize(v, isNull);
  }
  FOLLY_ALWAYS_INLINE void putRaw(const void* /*p*/, size_t n) {
    bytes += n;
  }
  template <typename T>
  FOLLY_ALWAYS_INLINE void putFixed(const T& /*v*/) {
    bytes += sizeof(T);
  }
};

struct WriteSink {
  uint8_t* out{nullptr};

  FOLLY_ALWAYS_INLINE void putVarint(uint64_t v) {
    out = writeVarint(v, out);
  }
  FOLLY_ALWAYS_INLINE void putNullableInt64(int64_t v, bool isNull) {
    out = writeNullableInt64(v, isNull, out);
  }
  FOLLY_ALWAYS_INLINE void putRaw(const void* p, size_t n) {
    std::memcpy(out, p, n);
    out += n;
  }
  template <typename T>
  FOLLY_ALWAYS_INLINE void putFixed(const T& v) {
    std::memcpy(out, &v, sizeof(T));
    out += sizeof(T);
  }
};

// Encode one column (any type) for N source rows into the per-row sinks. The
// SizeSink pass fills each ARRAY/MAP node's ColumnPlan::childSlots; the
// WriteSink pass reads them back. Instantiated for SizeSink and WriteSink in
// DenseRowGeneralEncode.cpp.
template <typename Sink>
void encodeColumnBatch(
    const Type& type,
    const ColumnPlan& plan,
    SlotView in,
    folly::Range<Sink*> sinks,
    const uint64_t* rowNulls);

// Encode a ROW level directly (entry point for the marker-less serializer).
// emitMarker=false omits the per-position present/null marker (caller asserts
// no nulls at this level).
template <typename Sink>
void encodeRowBatch(
    const Type& type,
    const ColumnPlan& plan,
    SlotView in,
    folly::Range<Sink*> sinks,
    const uint64_t* rowNulls,
    bool emitMarker = true);

// =============================================================================
// DECODE — column-batch decode kernels (DenseRowGeneralDecode.cpp).
// =============================================================================

// Decode entry points. Both are mutually recursive across the type dispatch.
// `readMarker == false` is the marker-less shuffle contract (caller asserts
// every top-level row is non-null).
void decodeColumnBatch(
    const Type& type,
    BaseVector& dst,
    SlotView out,
    folly::Range<RowCursor*> cursors,
    const uint64_t* rowNulls);

void decodeRowBatch(
    const Type& type,
    BaseVector& dst,
    SlotView out,
    folly::Range<RowCursor*> cursors,
    const uint64_t* rowNulls,
    bool readMarker = true);

} // namespace bytedance::bolt::row::dense_row
