#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <folly/Portability.h>

#include <cstring>

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
  if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
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
    std::memcpy(stringTarget, value.data(), value.size());
    heap->used += value.size();
    *target = StringView(stringTarget, value.size());
    if (context.recordedHeapBlock_ != heap->id) {
      segments_.recordHeapForChunk(chunk, *heap, row);
      context.recordedHeapBlock_ = heap->id;
    }
  } else if constexpr (
      Kind == TypeKind::UNKNOWN || !TypeTraits<Kind>::isPrimitiveType ||
      !TypeTraits<Kind>::isFixedWidth) {
    BOLT_NYI("Unsupported store type {}", column.type->toString());
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    *reinterpret_cast<T*>(row + column.offset) =
        decoded.valueAt<T>(sourceIndex);
  }
}

} // namespace bytedance::bolt::exec::bm
