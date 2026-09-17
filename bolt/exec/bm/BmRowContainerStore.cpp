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

#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SimdUtil.h"
#include "bolt/exec/BmContainerRowSerde.h"
#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/type/HugeInt.h"

#include <folly/Portability.h>

#include <limits>
#include <span>

namespace bytedance::bolt::exec::bm {

ColumnStorePlan::StoreValueFn BmRowContainer::storeFnFor(
    TypeKind kind,
    bool nullable) {
  if (nullable) {
    return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(storeWithNullsFn, kind);
  }
  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(storeNoNullsFn, kind);
}

template <TypeKind Kind>
ColumnStorePlan::StoreValueFn BmRowContainer::storeNoNullsFn() {
  return &BmRowContainer::storeNoNullsTyped<Kind>;
}

template <TypeKind Kind>
ColumnStorePlan::StoreValueFn BmRowContainer::storeWithNullsFn() {
  return &BmRowContainer::storeWithNullsTyped<Kind>;
}

void BmRowContainer::storeValue(
    const DecodedVector& decoded,
    vector_size_t sourceIndex,
    RowWriteContext& context,
    int32_t column) {
  BOLT_DCHECK_NOT_NULL(context.row_);
  BOLT_DCHECK_LT(column, layout_.columns().size());
  const auto& plan = layout_.storePlan(column);
  BOLT_DCHECK_NOT_NULL(plan.storeFn);
  (this->*plan.storeFn)(decoded, sourceIndex, context, plan);
}

template <TypeKind Kind>
void BmRowContainer::storeNoNullsTyped(
    const DecodedVector& decoded,
    vector_size_t sourceIndex,
    RowWriteContext& context,
    const ColumnStorePlan& column) {
  BOLT_DCHECK(
      !decoded.isNullAt(sourceIndex),
      "Column {} is not nullable",
      column.type->toString());
  storeNonNullValueTyped<Kind>(decoded, sourceIndex, context, column);
}

template <TypeKind Kind>
void BmRowContainer::storeWithNullsTyped(
    const DecodedVector& decoded,
    vector_size_t sourceIndex,
    RowWriteContext& context,
    const ColumnStorePlan& column) {
  auto& nullByte = context.row_[column.nullByte];
  const auto nullMask = static_cast<char>(column.nullMask);
  if (FOLLY_UNLIKELY(decoded.isNullAt(sourceIndex))) {
    nullByte |= nullMask;
    return;
  }
  nullByte &= ~nullMask;
  storeNonNullValueTyped<Kind>(decoded, sourceIndex, context, column);
}

template <TypeKind Kind>
void BmRowContainer::storeNonNullValueTyped(
    const DecodedVector& decoded,
    vector_size_t sourceIndex,
    RowWriteContext& context,
    const ColumnStorePlan& column) {
  auto* row = context.row_;
  if constexpr (
      Kind == TypeKind::ARRAY || Kind == TypeKind::MAP ||
      Kind == TypeKind::ROW) {
    storeComplexValue(decoded, sourceIndex, context, column);
  } else if constexpr (
      Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
    storeStringValue(decoded, sourceIndex, context, column);
  } else if constexpr (
      Kind == TypeKind::UNKNOWN || !TypeTraits<Kind>::isPrimitiveType ||
      !TypeTraits<Kind>::isFixedWidth) {
    BOLT_NYI("Unsupported store type {}", column.type->toString());
  } else if constexpr (Kind == TypeKind::HUGEINT) {
    HugeInt::serialize(
        decoded.valueAt<int128_t>(sourceIndex), row + column.offset);
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    *reinterpret_cast<T*>(row + column.offset) =
        decoded.valueAt<T>(sourceIndex);
  }
}

void BmRowContainer::storeComplexValue(
    const DecodedVector& decoded,
    vector_size_t sourceIndex,
    RowWriteContext& context,
    const ColumnStorePlan& column) {
  auto* row = context.row_;
  const auto& source = *decoded.base();
  const auto index = decoded.index(sourceIndex);
  const ContainerRowSerdeOptions options{.isKey = column.isKey};
  auto* target = reinterpret_cast<StringView*>(row + column.offset);

  // Speculative write: serialize directly into the uncommitted heap tail
  // while counting bytes, and commit only once the whole value fits. This
  // does a single traversal on the common path instead of a separate
  // serializedSize() pass followed by serializeInto(). trySerializeInto()
  // never grows the heap and reports capacity exhaustion via a result flag
  // rather than throwing.
  //
  // IMPORTANT: the four-way decision tree below (fit+inline / fit+heap /
  // overflow+inline / overflow+new-block) must stay byte-for-byte equivalent to
  // the batch path in BmRowContainer::storeComplexColumnBatchRanges()
  // (BmRowContainerBatch.cpp); the two only differ in how the heap cursor is
  // held (RowWriteContext here vs BatchHeapWriter there). Change both together.
  auto& chunk = *context.chunk_;
  auto* heap = context.currentHeap_;
  const std::span<char> available = heap == nullptr
      ? std::span<char>{}
      : std::span<char>(heap->ptr + heap->used, heap->size - heap->used);

  const auto result =
      detail::trySerializeInto(source, index, available, options);
  BOLT_CHECK_LE(
      result.size,
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max()),
      "Complex value exceeds BmRowContainer StringView size limit");
  const auto bytes = static_cast<uint32_t>(result.size);

  if (FOLLY_LIKELY(result.complete)) {
    if (FOLLY_UNLIKELY(StringView::isInline(result.size))) {
      // Inline-sized value: StringView copies the bytes into itself, so the
      // heap tail stays uncommitted and small values never occupy the heap.
      *target = StringView(available.data(), result.size);
      return;
    }
    // Common path: the value fit the tail; commit it in place.
    heap->used += bytes;
    *target = StringView(available.data(), bytes);
    // Register this block as backing the chunk the first time a row writes into
    // it, so its StringView pointers can be rebased on load. Subsequent columns
    // of the same row reuse the block and skip the redundant record.
    if (context.recordedHeapBlock_ != heap->id) {
      segments_.recordHeapForChunk(chunk, *heap, row);
      context.recordedHeapBlock_ = heap->id;
    }
    return;
  }

  // Overflow: the partial bytes left in the tail are garbage (never
  // committed, so spill and later writes ignore them).
  if (StringView::isInline(result.size)) {
    // Inline-sized value that did not fit the current tail: rewrite into a
    // stack buffer, no heap growth.
    char inlineBytes[StringView::kInlineSize];
    BmContainerRowSerde::serializeInto(
        source, index, std::span<char>(inlineBytes, result.size), options);
    *target = StringView(inlineBytes, result.size);
    return;
  }

  // Value crosses the current block: allocate an exact (possibly oversized)
  // block and serialize once more into it. The exact size is already known
  // from the first traversal, so this fallback is a plain serializeInto().
  heap = &segments_.ensureHeapBlockInChunk(chunk, bytes);
  context.currentHeap_ = heap;
  auto* serializedTarget = heap->ptr + heap->used;
  BmContainerRowSerde::serializeInto(
      source, index, std::span<char>(serializedTarget, bytes), options);
  heap->used += bytes;
  *target = StringView(serializedTarget, bytes);
  // Freshly allocated block: always new to this row, so record it as backing
  // the chunk for on-load StringView rebasing.
  if (context.recordedHeapBlock_ != heap->id) {
    segments_.recordHeapForChunk(chunk, *heap, row);
    context.recordedHeapBlock_ = heap->id;
  }
}

void BmRowContainer::storeStringValue(
    const DecodedVector& decoded,
    vector_size_t sourceIndex,
    RowWriteContext& context,
    const ColumnStorePlan& column) {
  auto* row = context.row_;
  auto* target = reinterpret_cast<StringView*>(row + column.offset);
  const auto value = decoded.valueAt<StringView>(sourceIndex);
  if (value.isInline()) {
    *target = value;
    return;
  }
  auto& chunk = *context.chunk_;
  auto* heap = context.currentHeap_;
  if (heap == nullptr || heap->used + value.size() > heap->size) {
    heap = &segments_.ensureHeapBlockInChunk(chunk, value.size());
    context.currentHeap_ = heap;
  }
  auto* stringTarget = heap->ptr + heap->used;
  simd::memcpy(stringTarget, value.data(), value.size());
  heap->used += value.size();
  *target = StringView(stringTarget, value.size());
  // Register this block as backing the chunk on first write from a row, so its
  // StringView pointers can be rebased on load; reused blocks skip it.
  if (context.recordedHeapBlock_ != heap->id) {
    segments_.recordHeapForChunk(chunk, *heap, row);
    context.recordedHeapBlock_ = heap->id;
  }
}

} // namespace bytedance::bolt::exec::bm
