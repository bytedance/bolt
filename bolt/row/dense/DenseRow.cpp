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

#include "bolt/row/dense/DenseRow.h"

// =============================================================================
// Wire format (in-tree spec — frozen, byte-identical across this codebase)
// =============================================================================
//
// Encoding is purely TYPE-DRIVEN: every value is encoded according to its
// vector's concrete type. A row's blob is the concatenation of its fields,
// each encoded by its own type. There is no top-level row marker — the caller
// frames rows and guarantees no top-level null rows.
//
// Core property: LEVEL-HOISTED. At every nesting level the structural bytes for
// all of that level's positions (nested ROW markers / ARRAY|MAP cardinalities /
// VARCHAR lengths) are written first, then the level descends into children.
// Each row's whole blob (all levels) lives in that row's own byte range.
//
//   row_blob := encode(field_0) ... encode(field_{k-1})
//
//   encode(T):
//     TINYINT/SMALLINT/INTEGER/BIGINT/TIMESTAMP:
//         null -> 0x00 | INT64_MIN -> 0x80 0x00
//         else  -> varint(zigzag(adjust(v))), adjust(v) = v > 0 ? v : v - 1
//     BOOLEAN:           varint(0 = null | 1 = false | 2 = true)
//     REAL:              4B LE float bits; sentinel 0x7fc00000 = null
//                        (a non-null value colliding with the sentinel is
//                        bit-flipped on encode and restored on decode)
//     DOUBLE:            8B LE; sentinel 0x7ff8000000000000 = null (as above)
//     VARCHAR/VARBINARY: varint(len + 1) (0 = null), then len payload bytes.
//                        Under a multi-position level, ALL lengths are written
//                        before ALL payloads.
//     HUGEINT:           nullable-int64 of zigzag128(value)'s low 64 bits (its
//                        0x00 sentinel = null, no separate tag); when non-null,
//                        followed by varint(high 64 bits of zigzag128(value))
//     UNKNOWN:           varint(0) (always null)
//     ROW:               per position varint(0 = null | 1 = present), then
//                        recurse each field (null positions emit only the
//                        marker and are filtered from children via parentNulls)
//     ARRAY:             per position varint(0 = null | cardinality + 1), then
//                        recurse the element column over the child positions
//     MAP:               per position varint(0 = null | cardinality + 1), then
//                        recurse the keys column, then the values column
//
// Frozen invariants: the INT64_MIN sentinel, cardinality + 1, MAP's keys-then-
// values segment ordering, and the level-hoisted ordering above. Empty
// array/map (cardinality+1 = 1) is distinct from null (0).
//
// Routing is per top-level field by type: scalar fields take the scalar
// column-at-a-time path, complex (ARRAY/MAP/ROW) fields take the general
// column-batch path; a row whose fields are all scalar uses a dedicated
// fast path that skips the general scaffolding entirely. The codec kernels
// live in sibling TUs, all declared in DenseRowGeneral.h:
//   * DenseRowGeneralEncode.cpp   general column-batch encode
//   * DenseRowGeneralDecode.cpp   general column-batch decode
//   * DenseRowScalar.{h,*.cpp}    scalar column fast path
// This file is the public API layer (the DenseRow class) only.
// =============================================================================

#include <limits>
#include <optional>
#include <variant>
#include <vector>

#include "bolt/row/dense/DenseRowGeneral.h"
#include "bolt/row/dense/DenseRowScalar.h"
#include "bolt/vector/ComplexVector.h"
#include "vector/DecodedVector.h"

namespace bytedance::bolt::row {

using namespace dense_row;

struct DenseRow::State {
  // Keeps the input column data alive for this DenseRow's lifetime.
  RowVectorPtr rowVector;
  vector_size_t numRows{0};
  std::vector<size_t> rowSizes;
  size_t totalSize{0};

  // Routing is per top-level field by type: a scalar field is decoded and
  // sized/written column-at-a-time (DecodecVector); a complex (ARRAY/MAP/ROW)
  // field goes through the general column-batch path (ColumnPlan). Both vectors
  // are sized to fieldCount; for field k exactly one entry is populated.
  std::vector<std::variant<DecodedVector, ColumnPlan>> decodedOrPlans;

  // Top-level slot view ({r, 1} per row) for the complex columns. Only built
  // when the row has a complex field (an all-scalar row leaves it empty). The
  // nested slot trees live in each ARRAY/MAP node's ColumnPlan::childSlots,
  // built by the size pass and replayed by the write pass.
  TopSlotView topView;
};

DenseRow::DenseRow(const RowVectorPtr& rowVector)
    : state_(std::make_unique<State>()) {
  auto& st = *state_;
  st.rowVector = rowVector;
  const auto numRows = rowVector->size();
  st.numRows = numRows;
  st.rowSizes.assign(numRows, 0);
  const auto& rowType = rowVector->type()->asRow();
  const auto fieldCount = rowType.size();

  if (numRows > 0) {
    // Route each top-level field by type: a scalar field is sized
    // column-at-a-time straight into rowSizes; a complex (ARRAY/MAP/ROW) field
    // runs the general SizeSink pass (which also builds the slot tree reused by
    // the write pass) and accumulates into sizeSinks. The general scaffolding
    // (slot view + sink array + plan slots) is allocated only when a complex
    // field is present, so an all-scalar row pays nothing extra. Complex fields
    // are visited in field order so the slot tree replays in that order during
    // serialize().
    bool anyComplex = false;
    for (size_t k = 0; k < fieldCount; ++k) {
      if (!rowType.childAt(k)->isPrimitiveType()) {
        anyComplex = true;
        break;
      }
    }

    st.decodedOrPlans.resize(fieldCount);
    std::vector<SizeSink> sizeSinks;
    if (anyComplex) {
      st.topView = makeTopView(numRows);
      sizeSinks.resize(numRows);
    }

    for (size_t k = 0; k < fieldCount; ++k) {
      const auto& childType = rowType.childAt(k);
      if (childType->isPrimitiveType()) {
        st.decodedOrPlans[k].emplace<DecodedVector>();
        auto* decoded = std::get_if<DecodedVector>(&st.decodedOrPlans[k]);

        decoded->decode(*rowVector->childAt(k));
        scalar::addColumnSizes(
            *childType, *decoded, numRows, st.rowSizes.data());
      } else {
        st.decodedOrPlans[k].emplace<ColumnPlan>(
            buildPlan(childType, rowVector->childAt(k)));
        auto* plan = std::get_if<ColumnPlan>(&st.decodedOrPlans[k]);
        encodeColumnBatch<SizeSink>(
            *childType,
            *plan,
            st.topView.view(),
            folly::Range<SizeSink*>(sizeSinks.data(), numRows),
            /*rowNulls=*/nullptr);
      }
    }
    if (anyComplex) {
      for (vector_size_t r = 0; r < numRows; ++r) {
        st.rowSizes[r] += sizeSinks[r].bytes;
      }
    }
  }

  size_t total = 0;
  for (size_t s : st.rowSizes) {
    total += s;
  }
  st.totalSize = total;
}

DenseRow::DenseRow(DenseRow&&) noexcept = default;
DenseRow& DenseRow::operator=(DenseRow&&) noexcept = default;
DenseRow::~DenseRow() = default;

vector_size_t DenseRow::numRows() const {
  return state_->numRows;
}

const std::vector<size_t>& DenseRow::rowSizes() const {
  return state_->rowSizes;
}

size_t DenseRow::rowSizeAt(vector_size_t index) const {
  return state_->rowSizes[index];
}

size_t DenseRow::totalSize() const {
  return state_->totalSize;
}

void DenseRow::serialize(uint8_t* base, folly::Range<const size_t*> offsets)
    const {
  const auto numRows = state_->numRows;
  BOLT_USER_CHECK_EQ(
      offsets.size(),
      static_cast<size_t>(numRows),
      "DenseRow::serialize offsets size mismatch");
  if (numRows == 0) {
    return;
  }
  const auto& rowType = state_->rowVector->type()->asRow();
  const auto fieldCount = rowType.size();

  // Write fields in declaration order, sharing one per-row write cursor so each
  // field lands at the right offset. Scalar fields advance the cursor directly;
  // complex fields run the general WriteSink pass (replaying the cached slot
  // tree), syncing the cursor across the call. writeSinks is allocated only if
  // a complex field is present (an all-scalar row never touches it).
  std::vector<uint8_t*> cursors(numRows);
  for (vector_size_t r = 0; r < numRows; ++r) {
    cursors[r] = base + offsets[r];
  }
  std::vector<WriteSink> writeSinks;
  for (size_t k = 0; k < fieldCount; ++k) {
    const auto& childType = rowType.childAt(k);
    std::visit(
        [&](auto& decodedOrPlan) {
          using T = std::decay_t<decltype(decodedOrPlan)>;
          if constexpr (std::is_same_v<T, DecodedVector>) {
            BOLT_CHECK(childType->isPrimitiveType());
            scalar::writeColumn(
                *childType, decodedOrPlan, numRows, cursors.data());
          } else {
            static_assert(std::is_same_v<T, ColumnPlan>);
            if (writeSinks.empty()) {
              writeSinks.resize(numRows);
            }
            for (vector_size_t r = 0; r < numRows; ++r) {
              writeSinks[r].out = cursors[r];
            }
            encodeColumnBatch<WriteSink>(
                *childType,
                decodedOrPlan,
                state_->topView.view(),
                folly::Range<WriteSink*>(writeSinks.data(), numRows),
                /*rowNulls=*/nullptr);
            for (vector_size_t r = 0; r < numRows; ++r) {
              cursors[r] = writeSinks[r].out;
            }
          }
        },
        state_->decodedOrPlans[k]);
  }

  for (vector_size_t r = 0; r < numRows; ++r) {
    const auto* rowStart = base + offsets[r];
    const auto actualSize = static_cast<size_t>(cursors[r] - rowStart);
    const auto expectedSize = state_->rowSizes[r];
    BOLT_CHECK_EQ(
        actualSize,
        expectedSize,
        "DenseRow::serialize row size mismatch at row {}, offset {}",
        r,
        offsets[r]);
  }
}

RowVectorPtr DenseRow::deserialize(
    const std::vector<std::string_view>& data,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  const auto rowCount = static_cast<vector_size_t>(data.size());

  // Decode fields in declaration order, sharing one per-row read cursor
  // (marker-less, so no top-level nulls). Scalar fields read column-at-a-time;
  // complex fields run the general decode over the top slot view, which is
  // built only when a complex field is present. Mirrors the per-column
  // serialize path.
  auto out = BaseVector::create(rowType, rowCount, pool);
  auto* rowVec = out->asUnchecked<RowVector>();
  std::vector<RowCursor> cursors(rowCount);
  for (vector_size_t r = 0; r < rowCount; ++r) {
    cursors[r].cur = reinterpret_cast<const uint8_t*>(data[r].data());
    cursors[r].end = cursors[r].cur + data[r].size();
    rowVec->setNull(r, false);
  }

  const auto cursorRange = folly::Range<RowCursor*>(cursors.data(), rowCount);
  const auto fieldCount = rowType->size();
  bool anyComplex = false;
  for (size_t k = 0; k < fieldCount; ++k) {
    if (!rowType->childAt(k)->isPrimitiveType()) {
      anyComplex = true;
      break;
    }
  }
  TopSlotView top;
  if (anyComplex) {
    top = makeTopView(rowCount);
  }
  for (size_t k = 0; k < fieldCount; ++k) {
    const auto& childType = rowType->childAt(k);
    if (childType->isPrimitiveType()) {
      scalar::readColumn(
          *childType, *rowVec->childAt(k), rowCount, cursorRange);
    } else {
      decodeColumnBatch(
          *childType,
          *rowVec->childAt(k),
          top.view(),
          cursorRange,
          /*rowNulls=*/nullptr);
    }
  }

  for (vector_size_t r = 0; r < rowCount; ++r) {
    BOLT_USER_CHECK(
        cursors[r].cur == cursors[r].end,
        "DenseRow: row {} not fully consumed ({} bytes remaining)",
        r,
        cursors[r].end - cursors[r].cur);
  }
  return std::dynamic_pointer_cast<RowVector>(out);
}

} // namespace bytedance::bolt::row
