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

// General (non-flat) column-batch encode kernels for the dense row format. See
// DenseRowGeneral.h. Kept in its own TU to isolate code layout from the decode
// kernels and the flat path; the two Sink instantiations the serializer needs
// are explicitly instantiated at the bottom.

#include "bolt/row/dense/DenseRowGeneral.h"

#include <cstring>
#include <type_traits>
#include <vector>

#include "bolt/common/base/Nulls.h"
#include "bolt/row/dense/IntVarint.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::row::dense_row {

template <typename Sink>
void encodeColumnBatch(
    const Type& type,
    const ColumnPlan& plan,
    SlotView in,
    folly::Range<Sink*> sinks,
    const uint64_t* rowNulls);

// Build the per-column plan tree by recursively decoding all nested vectors.
// Each node holds `source` — the vector it decoded — so the node is
// self-contained: its `decoded` (a non-owning view) and the held vector live
// together, independent of who else references the input. For nested types
// `children` holds the sub-plans (ARRAY -> {elements}, MAP -> {keys, values},
// ROW -> fields).
// NOLINTNEXTLINE(misc-no-recursion)
ColumnPlan buildPlan(const TypePtr& type, const VectorPtr& vector) {
  ColumnPlan plan;
  plan.source = vector;
  plan.kind = type->kind();
  plan.decoded.decode(*vector);
  plan.mayHaveNulls = plan.decoded.mayHaveNulls();

  switch (plan.kind) {
    case TypeKind::ARRAY: {
      const auto* array = plan.decoded.base()->as<ArrayVector>();
      BOLT_CHECK_NOT_NULL(array, "buildPlan: ARRAY base is not ArrayVector");
      plan.children.push_back(buildPlan(type->childAt(0), array->elements()));
      return plan;
    }
    case TypeKind::MAP: {
      const auto* map = plan.decoded.base()->as<MapVector>();
      BOLT_CHECK_NOT_NULL(map, "buildPlan: MAP base is not MapVector");
      plan.children.push_back(buildPlan(type->childAt(0), map->mapKeys()));
      plan.children.push_back(buildPlan(type->childAt(1), map->mapValues()));
      return plan;
    }
    case TypeKind::ROW: {
      const auto* row = plan.decoded.base()->as<RowVector>();
      BOLT_CHECK_NOT_NULL(row, "buildPlan: ROW base is not RowVector");
      const auto& rowType = type->asRow();
      plan.children.reserve(rowType.size());

      // For a dict/constant-wrapped ROW, push the outer mapping down onto each
      // base child so its DecodedVector reads through the wrap. The wrapped
      // child becomes that child node's `source`. The index buffer is reused
      // from the input's own wrapInfo() for a single-level dictionary (no
      // copy); for constant / multi-level it is materialized once (the resolved
      // indices aren't a standalone input buffer).
      BufferPtr outerIndices;
      const auto outerSize = vector->size();
      if (!plan.decoded.isIdentityMapping()) {
        if (vector->encoding() == VectorEncoding::Simple::DICTIONARY &&
            vector->wrapInfo()->as<vector_size_t>() == plan.decoded.indices()) {
          outerIndices = vector->wrapInfo();
        } else {
          outerIndices =
              AlignedBuffer::allocate<vector_size_t>(outerSize, vector->pool());
          std::memcpy(
              outerIndices->asMutable<vector_size_t>(),
              plan.decoded.indices(),
              outerSize * sizeof(vector_size_t));
        }
      }

      for (size_t i = 0; i < rowType.size(); ++i) {
        const auto& baseChild = row->childAt(i);
        if (!baseChild) {
          ColumnPlan nullPlan;
          nullPlan.kind = TypeKind::UNKNOWN;
          nullPlan.isNullColumn = true;
          plan.children.push_back(std::move(nullPlan));
          continue;
        }
        if (outerIndices) {
          plan.children.push_back(buildPlan(
              rowType.childAt(i),
              BaseVector::wrapInDictionary(
                  /*nulls=*/BufferPtr{nullptr},
                  outerIndices,
                  outerSize,
                  baseChild)));
        } else {
          plan.children.push_back(buildPlan(rowType.childAt(i), baseChild));
        }
      }
      return plan;
    }
    default:
      // Scalar leaves: nothing more to build.
      return plan;
  }
}

// =============================================================================
// Encode side
// =============================================================================

// Dedicated nullable-int encoder for any of int8/int16/int32/int64. Fast
// path (identity-mapped + no nulls + no parentNulls): walks each slot
// range as a contiguous int sequence and uses SIMD-batched varint sizing
// on the size pass, plus a tight scalar loop on the write pass.
template <typename Sink, typename T>
void encodeIntegerBatchT(
    const ColumnPlan& plan,
    SlotView in,
    folly::Range<Sink*> sinks,
    const uint64_t* rowNulls) {
  const auto N = static_cast<vector_size_t>(sinks.size());
  const bool mayNulls = plan.mayHaveNulls;
  const bool identity = plan.decoded.isIdentityMapping();
  const auto* raw = plan.decoded.data<T>();
  const bool fastPath = identity && !mayNulls && in.parentNulls == nullptr;

  if (fastPath) {
    for (vector_size_t r = 0; r < N; ++r) {
      if (rowNulls && bits::isBitNull(rowNulls, r)) {
        continue;
      }
      Sink& s = sinks[r];
      const auto lo = in.rowBoundaries[r];
      const auto hi = in.rowBoundaries[r + 1];
      for (uint32_t i = lo; i < hi; ++i) {
        const auto& sr = in.slots[i];
        if constexpr (std::is_same_v<Sink, SizeSink>) {
          s.bytes += detail::sumNullableIntSizes<T>(raw + sr.base, sr.count);
        } else {
          const uint32_t end = sr.base + sr.count;
          for (uint32_t p = sr.base; p < end; ++p) {
            s.putNullableInt64(static_cast<int64_t>(raw[p]), false);
          }
        }
      }
    }
    return;
  }

  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    Sink& s = sinks[r];
    forEachLivePos(in, r, [&](uint32_t p) {
      const bool isNull = mayNulls && plan.decoded.isNullAt(p);
      const int64_t v = isNull
          ? 0
          : static_cast<int64_t>(
                identity ? raw[p] : raw[plan.decoded.index(p)]);
      s.putNullableInt64(v, isNull);
    });
  }
}

// BOOLEAN: each non-null value emits exactly 1 byte (varint 1 or 2), null
// emits varint(0) = 1 byte. Total bytes per range = sr.count regardless
// of value distribution — size pass collapses to a single add.
template <typename Sink>
void encodeBooleanBatch(
    const ColumnPlan& plan,
    SlotView in,
    folly::Range<Sink*> sinks,
    const uint64_t* rowNulls) {
  const auto N = static_cast<vector_size_t>(sinks.size());
  const bool mayNulls = plan.mayHaveNulls;

  if (in.parentNulls == nullptr) {
    for (vector_size_t r = 0; r < N; ++r) {
      if (rowNulls && bits::isBitNull(rowNulls, r)) {
        continue;
      }
      Sink& s = sinks[r];
      const auto lo = in.rowBoundaries[r];
      const auto hi = in.rowBoundaries[r + 1];
      if constexpr (std::is_same_v<Sink, SizeSink>) {
        size_t total = 0;
        for (uint32_t i = lo; i < hi; ++i) {
          total += in.slots[i].count;
        }
        s.bytes += total;
      } else {
        for (uint32_t i = lo; i < hi; ++i) {
          const auto& sr = in.slots[i];
          const uint32_t end = sr.base + sr.count;
          for (uint32_t p = sr.base; p < end; ++p) {
            if (mayNulls && plan.decoded.isNullAt(p)) {
              s.putVarint(0);
            } else {
              s.putVarint(plan.decoded.valueAt<bool>(p) ? 2 : 1);
            }
          }
        }
      }
    }
    return;
  }

  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    Sink& s = sinks[r];
    forEachLivePos(in, r, [&](uint32_t p) {
      if (mayNulls && plan.decoded.isNullAt(p)) {
        s.putVarint(0);
      } else {
        s.putVarint(plan.decoded.valueAt<bool>(p) ? 2 : 1);
      }
    });
  }
}

template <typename Sink>
void encodeRealBatch(
    const ColumnPlan& plan,
    SlotView in,
    folly::Range<Sink*> sinks,
    const uint64_t* rowNulls) {
  const auto N = static_cast<vector_size_t>(sinks.size());
  const bool mayNulls = plan.mayHaveNulls;
  const bool identity = plan.decoded.isIdentityMapping();
  const auto* raw = plan.decoded.data<float>();
  const bool fastPath = identity && !mayNulls && in.parentNulls == nullptr;

  if (fastPath) {
    for (vector_size_t r = 0; r < N; ++r) {
      if (rowNulls && bits::isBitNull(rowNulls, r)) {
        continue;
      }
      Sink& s = sinks[r];
      const auto lo = in.rowBoundaries[r];
      const auto hi = in.rowBoundaries[r + 1];
      for (uint32_t i = lo; i < hi; ++i) {
        const auto& sr = in.slots[i];
        if constexpr (std::is_same_v<Sink, SizeSink>) {
          s.bytes += static_cast<size_t>(sr.count) * sizeof(uint32_t);
        } else {
          const uint32_t end = sr.base + sr.count;
          for (uint32_t p = sr.base; p < end; ++p) {
            uint32_t b;
            std::memcpy(&b, raw + p, sizeof(b));
            if (FOLLY_UNLIKELY(b == kNullFloatBits)) {
              b ^= 1u;
            }
            s.template putFixed<uint32_t>(b);
          }
        }
      }
    }
    return;
  }

  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    Sink& s = sinks[r];
    forEachLivePos(in, r, [&](uint32_t p) {
      if (mayNulls && plan.decoded.isNullAt(p)) {
        s.template putFixed<uint32_t>(kNullFloatBits);
        return;
      }
      const float value = plan.decoded.valueAt<float>(p);
      uint32_t b;
      std::memcpy(&b, &value, sizeof(b));
      // Match v1 collision policy: bit-flip the rare value that aliases
      // the null sentinel. Inputs whose bits already equal kNullFloatBits^1
      // round-trip through a single-bit corruption — same lossy behavior
      // as the v1 wire format.
      if (FOLLY_UNLIKELY(b == kNullFloatBits)) {
        b ^= 1u;
      }
      s.template putFixed<uint32_t>(b);
    });
  }
}

template <typename Sink>
void encodeDoubleBatch(
    const ColumnPlan& plan,
    SlotView in,
    folly::Range<Sink*> sinks,
    const uint64_t* rowNulls) {
  const auto N = static_cast<vector_size_t>(sinks.size());
  const bool mayNulls = plan.mayHaveNulls;
  const bool identity = plan.decoded.isIdentityMapping();
  const auto* raw = plan.decoded.data<double>();
  const bool fastPath = identity && !mayNulls && in.parentNulls == nullptr;

  if (fastPath) {
    for (vector_size_t r = 0; r < N; ++r) {
      if (rowNulls && bits::isBitNull(rowNulls, r)) {
        continue;
      }
      Sink& s = sinks[r];
      const auto lo = in.rowBoundaries[r];
      const auto hi = in.rowBoundaries[r + 1];
      for (uint32_t i = lo; i < hi; ++i) {
        const auto& sr = in.slots[i];
        if constexpr (std::is_same_v<Sink, SizeSink>) {
          s.bytes += static_cast<size_t>(sr.count) * sizeof(uint64_t);
        } else {
          const uint32_t end = sr.base + sr.count;
          for (uint32_t p = sr.base; p < end; ++p) {
            uint64_t b;
            std::memcpy(&b, raw + p, sizeof(b));
            if (FOLLY_UNLIKELY(b == kNullDoubleBits)) {
              b ^= 1ull;
            }
            s.template putFixed<uint64_t>(b);
          }
        }
      }
    }
    return;
  }

  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    Sink& s = sinks[r];
    forEachLivePos(in, r, [&](uint32_t p) {
      if (mayNulls && plan.decoded.isNullAt(p)) {
        s.template putFixed<uint64_t>(kNullDoubleBits);
        return;
      }
      const double value = plan.decoded.valueAt<double>(p);
      uint64_t b;
      std::memcpy(&b, &value, sizeof(b));
      if (FOLLY_UNLIKELY(b == kNullDoubleBits)) {
        b ^= 1ull;
      }
      s.template putFixed<uint64_t>(b);
    });
  }
}

template <typename Sink>
void encodeVarcharBatch(
    const ColumnPlan& plan,
    SlotView in,
    folly::Range<Sink*> sinks,
    const uint64_t* rowNulls) {
  const auto N = static_cast<vector_size_t>(sinks.size());
  const bool mayNulls = plan.mayHaveNulls;
  // Wire layout per row segment: length stream then payload stream.
  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    Sink& s = sinks[r];
    forEachLivePos(in, r, [&](uint32_t p) {
      if (mayNulls && plan.decoded.isNullAt(p)) {
        s.putVarint(0);
      } else {
        s.putVarint(
            static_cast<uint64_t>(plan.decoded.valueAt<StringView>(p).size()) +
            1);
      }
    });
    forEachLivePos(in, r, [&](uint32_t p) {
      if (mayNulls && plan.decoded.isNullAt(p)) {
        return;
      }
      const auto sv = plan.decoded.valueAt<StringView>(p);
      s.putRaw(sv.data(), sv.size());
    });
  }
}

template <typename Sink>
void encodeTimestampBatch(
    const ColumnPlan& plan,
    SlotView in,
    folly::Range<Sink*> sinks,
    const uint64_t* rowNulls) {
  const auto N = static_cast<vector_size_t>(sinks.size());
  const bool mayNulls = plan.mayHaveNulls;
  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    Sink& s = sinks[r];
    forEachLivePos(in, r, [&](uint32_t p) {
      const bool isNull = mayNulls && plan.decoded.isNullAt(p);
      s.putNullableInt64(
          isNull ? 0 : plan.decoded.valueAt<Timestamp>(p).toMicros(), isNull);
    });
  }
}

// HUGEINT wire format: the null marker is folded into the low int64 slot (no
// separate presence tag). null -> nullableInt64(_, null) (a single 0x00 byte);
// non-null -> nullableInt64(low 64 of zigzag128(value)), varint(high 64).
// Small/ medium DECIMAL unscaled values encode in a few bytes (vs the old fixed
// 16). Mirrors detail::writeNullableInt128.
template <typename Sink>
void encodeHugeintBatch(
    const ColumnPlan& plan,
    SlotView in,
    folly::Range<Sink*> sinks,
    const uint64_t* rowNulls) {
  const auto N = static_cast<vector_size_t>(sinks.size());
  const bool mayNulls = plan.mayHaveNulls;
  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    Sink& s = sinks[r];
    forEachLivePos(in, r, [&](uint32_t p) {
      if (mayNulls && plan.decoded.isNullAt(p)) {
        s.putNullableInt64(0, /*isNull=*/true);
      } else {
        const int128_t v = plan.decoded.valueAt<int128_t>(p);
        const auto zz = detail::zigZagEncode128(v);
        s.putNullableInt64(
            static_cast<int64_t>(static_cast<uint64_t>(zz)), /*isNull=*/false);
        s.putVarint(static_cast<uint64_t>(zz >> 64));
      }
    });
  }
}

template <typename Sink>
void encodeNullColumnBatch(
    SlotView in,
    folly::Range<Sink*> sinks,
    const uint64_t* rowNulls) {
  const auto N = static_cast<vector_size_t>(sinks.size());
  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    Sink& s = sinks[r];
    forEachLivePos(in, r, [&](uint32_t /*p*/) { s.putVarint(0); });
  }
}

// ARRAY/MAP cardinality emission + (conditionally) child slot tree build.
//
// For SizeSink: emits per-slot cardinality varint *sizes* and builds the
//   child SlotRange/boundaries arrays in `node`.
// For WriteSink: emits per-slot cardinality *bytes* and skips the build —
//   `node` was already populated by the prior SizeSink walk.
//
// Walks parent positions identically in both passes (the cardinality stream
// must be byte-for-byte identical between size and write), but the
// push_back work happens only on the size pass. This roughly halves the
// per-call slot-tree overhead vs rebuilding on each pass.
template <typename Sink>
void encodeArrayLikeCardinalities(
    const ColumnPlan& plan,
    const vector_size_t* rawOffsets,
    const vector_size_t* rawSizes,
    SlotView in,
    folly::Range<Sink*> sinks,
    const uint64_t* rowNulls,
    SlotTreeNode& node) {
  const auto N = static_cast<vector_size_t>(sinks.size());
  const bool mayNulls = plan.mayHaveNulls;
  const bool identity = plan.decoded.isIdentityMapping();
  constexpr bool kBuild = std::is_same_v<Sink, SizeSink>;

  if constexpr (kBuild) {
    node.slots.clear();
    node.boundaries.clear();
    node.boundaries.resize(N + 1);
    node.boundaries[0] = 0;
  }

  // Fast path: no nulls on parent vector, no parentNulls bitmap, identity
  // mapping. Inline the hot loop (cardinality emit + maybe push).
  if (!mayNulls && identity && in.parentNulls == nullptr) {
    for (vector_size_t r = 0; r < N; ++r) {
      if (rowNulls && bits::isBitNull(rowNulls, r)) {
        if constexpr (kBuild) {
          node.boundaries[r + 1] = static_cast<uint32_t>(node.slots.size());
        }
        continue;
      }
      Sink& s = sinks[r];
      const auto lo = in.rowBoundaries[r];
      const auto hi = in.rowBoundaries[r + 1];
      for (uint32_t i = lo; i < hi; ++i) {
        const auto& sr = in.slots[i];
        const uint32_t end = sr.base + sr.count;
        for (uint32_t p = sr.base; p < end; ++p) {
          const auto sz = static_cast<uint32_t>(rawSizes[p]);
          s.putVarint(static_cast<uint64_t>(sz) + 1);
          if constexpr (kBuild) {
            node.slots.push_back({static_cast<uint32_t>(rawOffsets[p]), sz});
          }
        }
      }
      if constexpr (kBuild) {
        node.boundaries[r + 1] = static_cast<uint32_t>(node.slots.size());
      }
    }
    return;
  }

  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      if constexpr (kBuild) {
        node.boundaries[r + 1] = static_cast<uint32_t>(node.slots.size());
      }
      continue;
    }
    Sink& s = sinks[r];
    forEachLivePos(in, r, [&](uint32_t p) {
      if (mayNulls && plan.decoded.isNullAt(p)) {
        s.putVarint(0);
        return;
      }
      const auto idx =
          identity ? p : plan.decoded.index(static_cast<vector_size_t>(p));
      const auto sz = static_cast<uint32_t>(rawSizes[idx]);
      s.putVarint(static_cast<uint64_t>(sz) + 1);
      if constexpr (kBuild) {
        node.slots.push_back({static_cast<uint32_t>(rawOffsets[idx]), sz});
      }
    });
    if constexpr (kBuild) {
      node.boundaries[r + 1] = static_cast<uint32_t>(node.slots.size());
    }
  }
}

template <typename Sink>
void encodeArrayBatch(
    const Type& type,
    const ColumnPlan& plan,
    SlotView in,
    folly::Range<Sink*> sinks,
    const uint64_t* rowNulls) {
  const auto* array = plan.decoded.base()->as<ArrayVector>();
  // This level owns its child slots in the plan tree: the SizeSink pass builds
  // them, the WriteSink pass reads them straight back (no cursor/scratch).
  SlotTreeNode& node = plan.childSlots;
  if constexpr (std::is_same_v<Sink, SizeSink>) {
    // Upper bound: total non-null parent slots ≤ elements vector size.
    node.slots.reserve(array->elements()->size());
  }
  encodeArrayLikeCardinalities(
      plan, array->rawOffsets(), array->rawSizes(), in, sinks, rowNulls, node);
  if constexpr (std::is_same_v<Sink, SizeSink>) {
    BOLT_CHECK_EQ(in.sourceRowOffset, 0);
  }
  const auto rowOffset = in.sourceRowOffset;
  // Boundaries can be sliced, but slots must remain full because boundary
  // values are absolute indexes into node.slots.
  SlotView childView{
      {node.slots.data(), node.slots.size()},
      {node.boundaries.data() + rowOffset, sinks.size() + 1},
      nullptr,
      in.sourceRowOffset};
  encodeColumnBatch(
      *type.childAt(0), plan.children[0], childView, sinks, rowNulls);
}

template <typename Sink>
void encodeMapBatch(
    const Type& type,
    const ColumnPlan& plan,
    SlotView in,
    folly::Range<Sink*> sinks,
    const uint64_t* rowNulls) {
  const auto* map = plan.decoded.base()->as<MapVector>();
  // One child slot set (built by the SizeSink pass) drives both keys and
  // values.
  SlotTreeNode& node = plan.childSlots;
  if constexpr (std::is_same_v<Sink, SizeSink>) {
    node.slots.reserve(map->mapKeys()->size());
  }
  encodeArrayLikeCardinalities(
      plan, map->rawOffsets(), map->rawSizes(), in, sinks, rowNulls, node);
  if constexpr (std::is_same_v<Sink, SizeSink>) {
    BOLT_CHECK_EQ(in.sourceRowOffset, 0);
  }
  const auto rowOffset = in.sourceRowOffset;
  // Boundaries can be sliced, but slots must remain full because boundary
  // values are absolute indexes into node.slots.
  SlotView childView{
      {node.slots.data(), node.slots.size()},
      {node.boundaries.data() + rowOffset, sinks.size() + 1},
      nullptr,
      in.sourceRowOffset};
  encodeColumnBatch(
      *type.childAt(0), plan.children[0], childView, sinks, rowNulls);
  encodeColumnBatch(
      *type.childAt(1), plan.children[1], childView, sinks, rowNulls);
}

template <typename Sink>
void encodeRowBatch(
    const Type& type,
    const ColumnPlan& plan,
    SlotView in,
    folly::Range<Sink*> sinks,
    const uint64_t* rowNulls,
    bool emitMarker) {
  const auto N = static_cast<vector_size_t>(sinks.size());
  const bool mayNulls = plan.mayHaveNulls;

  // The subfield slot range equals this ROW's slot range — same positions,
  // filtered by combined ancestor + this-level nulls. Materialize a bitmap
  // only if there's anything to filter.
  const bool needBitmap = mayNulls || in.parentNulls != nullptr;
  vector_size_t bitmapBits = 0;
  if (needBitmap) {
    for (const auto& sr : in.slots) {
      const auto endPos = static_cast<vector_size_t>(sr.base + sr.count);
      if (endPos > bitmapBits) {
        bitmapBits = endPos;
      }
    }
  }

  std::vector<uint64_t> childNullsBuf;
  const uint64_t* childParentNulls = in.parentNulls;
  if (needBitmap && bitmapBits > 0) {
    childNullsBuf.assign(bits::nwords(bitmapBits), ~uint64_t{0});
    childParentNulls = childNullsBuf.data();
  }

  // Fast path: no nulls anywhere AND no parentNulls. Every position emits
  // varint(1) = 1 byte and contributes nothing to childNullsBuf. SizeSink
  // collapses to a bulk count add; WriteSink writes 1 byte per slot.
  // When emitMarker is false (top-level non-null contract from caller via
  // serializeAt), the marker step is skipped entirely; nested ROW levels
  // always call this with the default true.
  if (!mayNulls && in.parentNulls == nullptr) {
    if (emitMarker) {
      for (vector_size_t r = 0; r < N; ++r) {
        if (rowNulls && bits::isBitNull(rowNulls, r)) {
          continue;
        }
        Sink& s = sinks[r];
        const auto lo = in.rowBoundaries[r];
        const auto hi = in.rowBoundaries[r + 1];
        if constexpr (std::is_same_v<Sink, SizeSink>) {
          size_t total = 0;
          for (uint32_t i = lo; i < hi; ++i) {
            total += in.slots[i].count;
          }
          s.bytes += total;
        } else {
          for (uint32_t i = lo; i < hi; ++i) {
            const auto& sr = in.slots[i];
            const uint32_t endPos = sr.base + sr.count;
            for (uint32_t p = sr.base; p < endPos; ++p) {
              (void)p;
              s.putVarint(1);
            }
          }
        }
      }
    }
    SlotView childView{
        in.slots, in.rowBoundaries, childParentNulls, in.sourceRowOffset};
    const auto fieldCount = type.size();
    for (size_t f = 0; f < fieldCount; ++f) {
      encodeColumnBatch(
          *type.childAt(f), plan.children[f], childView, sinks, rowNulls);
    }
    return;
  }

  for (vector_size_t r = 0; r < N; ++r) {
    if (rowNulls && bits::isBitNull(rowNulls, r)) {
      continue;
    }
    Sink& s = sinks[r];
    const auto lo = in.rowBoundaries[r];
    const auto hi = in.rowBoundaries[r + 1];
    for (uint32_t i = lo; i < hi; ++i) {
      const auto& sr = in.slots[i];
      const uint32_t endPos = sr.base + sr.count;
      for (uint32_t p = sr.base; p < endPos; ++p) {
        const bool parentSaysNull = in.parentNulls &&
            bits::isBitNull(in.parentNulls, static_cast<int32_t>(p));
        if (parentSaysNull) {
          if (!childNullsBuf.empty()) {
            bits::setNull(childNullsBuf.data(), p, true);
          }
          continue;
        }
        if (mayNulls && plan.decoded.isNullAt(p)) {
          s.putVarint(0);
          if (!childNullsBuf.empty()) {
            bits::setNull(childNullsBuf.data(), p, true);
          }
        } else {
          s.putVarint(1);
        }
      }
    }
  }

  SlotView childView{
      in.slots, in.rowBoundaries, childParentNulls, in.sourceRowOffset};
  const auto fieldCount = type.size();
  for (size_t f = 0; f < fieldCount; ++f) {
    encodeColumnBatch(
        *type.childAt(f), plan.children[f], childView, sinks, rowNulls);
  }
}

template <typename Sink>
void encodeColumnBatch(
    const Type& type,
    const ColumnPlan& plan,
    SlotView in,
    folly::Range<Sink*> sinks,
    const uint64_t* rowNulls) {
  if (plan.isNullColumn) {
    encodeNullColumnBatch(in, sinks, rowNulls);
    return;
  }
  switch (plan.kind) {
    case TypeKind::BOOLEAN:
      encodeBooleanBatch(plan, in, sinks, rowNulls);
      return;
    case TypeKind::TINYINT:
      encodeIntegerBatchT<Sink, int8_t>(plan, in, sinks, rowNulls);
      return;
    case TypeKind::SMALLINT:
      encodeIntegerBatchT<Sink, int16_t>(plan, in, sinks, rowNulls);
      return;
    case TypeKind::INTEGER:
      encodeIntegerBatchT<Sink, int32_t>(plan, in, sinks, rowNulls);
      return;
    case TypeKind::BIGINT:
      encodeIntegerBatchT<Sink, int64_t>(plan, in, sinks, rowNulls);
      return;
    case TypeKind::REAL:
      encodeRealBatch(plan, in, sinks, rowNulls);
      return;
    case TypeKind::DOUBLE:
      encodeDoubleBatch(plan, in, sinks, rowNulls);
      return;
    case TypeKind::HUGEINT:
      encodeHugeintBatch(plan, in, sinks, rowNulls);
      return;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      encodeVarcharBatch(plan, in, sinks, rowNulls);
      return;
    case TypeKind::TIMESTAMP:
      encodeTimestampBatch(plan, in, sinks, rowNulls);
      return;
    case TypeKind::ARRAY:
      encodeArrayBatch(type, plan, in, sinks, rowNulls);
      return;
    case TypeKind::MAP:
      encodeMapBatch(type, plan, in, sinks, rowNulls);
      return;
    case TypeKind::ROW:
      encodeRowBatch(type, plan, in, sinks, rowNulls);
      return;
    case TypeKind::UNKNOWN:
      encodeNullColumnBatch(in, sinks, rowNulls);
      return;
    default:
      BOLT_UNREACHABLE();
  }
}

// Explicit instantiations for the two passes (size + write). Keeping the kernel
// bodies in this TU (not a header) is what isolates their code layout.
template void encodeColumnBatch<SizeSink>(
    const Type&,
    const ColumnPlan&,
    SlotView,
    folly::Range<SizeSink*>,
    const uint64_t*);
template void encodeColumnBatch<WriteSink>(
    const Type&,
    const ColumnPlan&,
    SlotView,
    folly::Range<WriteSink*>,
    const uint64_t*);
template void encodeRowBatch<SizeSink>(
    const Type&,
    const ColumnPlan&,
    SlotView,
    folly::Range<SizeSink*>,
    const uint64_t*,
    bool);
template void encodeRowBatch<WriteSink>(
    const Type&,
    const ColumnPlan&,
    SlotView,
    folly::Range<WriteSink*>,
    const uint64_t*,
    bool);

} // namespace bytedance::bolt::row::dense_row
