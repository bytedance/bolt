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
#include "bolt/shuffle/sparksql/LazyBundleEncoder.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/row/CompactRow.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/LazyComplexCodec.h"
#include "bolt/vector/LazyComplexVector.h"

namespace bytedance::bolt::shuffle::sparksql {

namespace {

inline bool isComplexType(const TypePtr& t) {
  return t->isRow() || t->isArray() || t->isMap();
}

RowVectorPtr wrapAsRow(const VectorPtr& input, memory::MemoryPool* pool) {
  return std::make_shared<RowVector>(
      pool,
      ROW({input->type()}),
      input->nulls(),
      input->size(),
      std::vector<VectorPtr>{input});
}

enum class Kind : uint8_t { kLazy, kRaw };

struct ColState {
  Kind kind;
  // kLazy: read straight from the pre-encoded FlatVector<StringView>.
  const StringView* rawViews{nullptr};
  const uint64_t* rawNulls{nullptr};
  // kRaw: CompactRow encodes [wrapper_null_byte][field_bytes] directly into
  // the bundle arena. `compactHolder` keeps the wrapping RowVector alive
  // for the lifetime of `compact`.
  RowVectorPtr compactHolder;
  std::unique_ptr<row::CompactRow> compact;
  bool fixedSize{false};
  int32_t fixedBytes{0};
};

} // namespace

RowVectorPtr encodeAndBundleLazyWireRowVector(
    const RowVectorPtr& input,
    memory::MemoryPool* pool) {
  if (!input || LazyComplexCodec::activeCodec() == nullptr) {
    return input;
  }

  std::vector<VectorPtr> complexChildren;
  std::vector<VectorPtr> nonComplexChildren;
  complexChildren.reserve(input->childrenSize());
  nonComplexChildren.reserve(input->childrenSize());
  for (size_t i = 0; i < input->childrenSize(); ++i) {
    const auto& c = input->childAt(i);
    if (c && isComplexType(c->type())) {
      complexChildren.push_back(c);
    } else {
      nonComplexChildren.push_back(c);
    }
  }
  if (complexChildren.empty()) {
    return input;
  }

  const vector_size_t size = input->size();
  const size_t numComplex = complexChildren.size();
  const size_t nullByteCount = (numComplex + 7) / 8;

  // Per-col classification: already-lazy vs raw-complex-to-encode.
  std::vector<ColState> cols(numComplex);
  for (size_t j = 0; j < numComplex; ++j) {
    const auto& child = complexChildren[j];
    if (child->encoding() == VectorEncoding::Simple::LAZY_COMPLEX) {
      const auto* enc =
          child->asUnchecked<LazyComplexVector>()->encoded().get();
      cols[j].kind = Kind::kLazy;
      cols[j].rawViews = enc->rawValues<StringView>();
      cols[j].rawNulls = enc->rawNulls();
    } else {
      cols[j].kind = Kind::kRaw;
      cols[j].compactHolder = wrapAsRow(child, pool);
      cols[j].compact =
          std::make_unique<row::CompactRow>(cols[j].compactHolder);
      cols[j].rawNulls = child->rawNulls();
      const auto fixed = row::CompactRow::fixedRowSize(
          asRowType(cols[j].compactHolder->type()));
      if (fixed.has_value()) {
        cols[j].fixedSize = true;
        cols[j].fixedBytes = *fixed;
      }
    }
  }

  // Size pass. Matches the serialize-pass per-cell rule: null cells
  // contribute 0 bytes (the bundle bitmap carries null); non-null cells
  // contribute sizeof(uint32_t) length prefix + cell payload.
  const auto perRowBitmap = static_cast<int64_t>(nullByteCount);
  const int64_t perRowLenPrefix =
      static_cast<int64_t>(numComplex) * sizeof(uint32_t);
  int64_t total = static_cast<int64_t>(size) * (perRowBitmap + perRowLenPrefix);
  for (size_t j = 0; j < numComplex; ++j) {
    const auto& pj = cols[j];
    int64_t colBytes = 0;
    if (pj.kind == Kind::kLazy) {
      // The invariant on LazyComplexVector means null rows have size 0,
      // so we can sum unconditionally.
      for (vector_size_t r = 0; r < size; ++r) {
        colBytes += pj.rawViews[r].size();
      }
    } else if (pj.fixedSize) {
      if (pj.rawNulls == nullptr) {
        colBytes = static_cast<int64_t>(pj.fixedBytes) * size;
      } else {
        for (vector_size_t r = 0; r < size; ++r) {
          if (!bits::isBitNull(pj.rawNulls, r)) {
            colBytes += pj.fixedBytes;
          }
        }
      }
    } else {
      for (vector_size_t r = 0; r < size; ++r) {
        if (pj.rawNulls == nullptr || !bits::isBitNull(pj.rawNulls, r)) {
          colBytes += pj.compact->rowSize(r);
        }
      }
    }
    total += colBytes;
  }

  // Allocate arena without zero-init.  The per-cell writes below fully
  // overwrite their slots: kLazy cells via memcpy, kRaw cells via a
  // scoped memset + CompactRow::serialize (CompactRow requires pre-zero
  // on the target region to use setBit on null-flag bytes).  Prefixes
  // (null bitmap + uint32 lens) are written explicitly row-by-row.
  const auto wantBytes = static_cast<size_t>(total > 0 ? total : 1);
  auto arena = AlignedBuffer::allocate<char>(wantBytes, pool);
  auto* base = arena->asMutable<char>();
  auto valuesBuf =
      AlignedBuffer::allocate<StringView>(size > 0 ? size : 1, pool);
  auto* rawViewsOut = valuesBuf->asMutable<StringView>();

  // Fused serialize: one sequential write through the arena. For kLazy
  // cols we memcpy the pre-encoded bytes; for kRaw cols CompactRow
  // writes [null_byte][field_bytes] directly into the bundle arena.
  // The per-row null bitmap is zeroed up-front and null bits are set
  // directly at rowStart[j/8] as we walk columns - no uint64_t
  // accumulator, so there is no 64-column limit.
  char* p = base;
  for (vector_size_t r = 0; r < size; ++r) {
    char* const rowStart = p;
    std::memset(rowStart, 0, nullByteCount);
    p += nullByteCount;
    for (size_t j = 0; j < numComplex; ++j) {
      const auto& pj = cols[j];
      const bool nullHere =
          pj.rawNulls != nullptr && bits::isBitNull(pj.rawNulls, r);
      uint32_t len = 0;
      if (!nullHere) {
        if (pj.kind == Kind::kLazy) {
          len = static_cast<uint32_t>(pj.rawViews[r].size());
        } else if (pj.fixedSize) {
          len = static_cast<uint32_t>(pj.fixedBytes);
        } else {
          len = static_cast<uint32_t>(pj.compact->rowSize(r));
        }
      } else {
        rowStart[j >> 3] |= static_cast<char>(1U << (j & 7));
      }
      *reinterpret_cast<uint32_t*>(p) = len;
      p += sizeof(uint32_t);
      if (!nullHere && len > 0) {
        if (pj.kind == Kind::kLazy) {
          std::memcpy(p, pj.rawViews[r].data(), len);
        } else {
          // CompactRow uses setBit on null-flag bytes, so the cell
          // region must start zeroed before serialize.
          std::memset(p, 0, len);
          pj.compact->serialize(r, p);
        }
        p += len;
      }
    }
    rawViewsOut[r] = StringView(rowStart, static_cast<int32_t>(p - rowStart));
  }
  BOLT_DCHECK_EQ(p - base, total);

  auto bundle = std::make_shared<FlatVector<StringView>>(
      pool,
      VARBINARY(),
      /*nulls=*/nullptr,
      size,
      valuesBuf,
      std::vector<BufferPtr>{arena});

  std::vector<VectorPtr> wireChildren = std::move(nonComplexChildren);
  wireChildren.push_back(bundle);
  auto wireType = lazyBundleWireRowType(asRowType(input->type()));
  return std::make_shared<RowVector>(
      input->pool(), wireType, input->nulls(), size, std::move(wireChildren));
}

} // namespace bytedance::bolt::shuffle::sparksql
