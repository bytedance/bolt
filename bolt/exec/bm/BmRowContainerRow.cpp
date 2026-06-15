#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <folly/Portability.h>

#include <chrono>
#include <cstring>

namespace bytedance::bolt::exec::bm {
namespace {

uint64_t metricNowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

} // namespace

void BmRowContainer::storeValue(
    const DecodedVector& decoded,
    vector_size_t sourceIndex,
    RowWriteContext& context,
    int32_t column,
    BmStoreMetrics* metrics) {
  BOLT_DCHECK_NOT_NULL(context.row_);
  BOLT_DCHECK_LT(column, layout_.columns().size());
  const auto& plan = layout_.storePlan(column);
  if (FOLLY_LIKELY(!plan.nullable)) {
    BOLT_DCHECK(
        !decoded.isNullAt(sourceIndex),
        "Column {} is not nullable",
        column);
  } else {
    const bool null = decoded.isNullAt(sourceIndex);
    auto& nullByte = context.row_[plan.nullByte];
    const auto nullMask = static_cast<char>(plan.nullMask);
    if (FOLLY_UNLIKELY(null)) {
      nullByte |= nullMask;
      return;
    }
    nullByte &= ~nullMask;
  }

  const auto start = metrics == nullptr ? 0 : metricNowNs();
  BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      storeValueTyped,
      plan.kind,
      decoded,
      sourceIndex,
      context,
      plan,
      metrics);
  if (metrics != nullptr) {
    const auto elapsed = metricNowNs() - start;
    if (plan.stringKind) {
      metrics->stringStoreNs += elapsed;
    } else {
      metrics->fixedStoreNs += elapsed;
    }
  }
}

template <TypeKind Kind>
void BmRowContainer::storeValueTyped(
    const DecodedVector& decoded,
    vector_size_t sourceIndex,
    RowWriteContext& context,
    const ColumnStorePlan& column,
    BmStoreMetrics* metrics) {
  auto* row = context.row_;
  if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
    auto* target = reinterpret_cast<StringView*>(row + column.offset);
    const auto value = decoded.valueAt<StringView>(sourceIndex);
    if (metrics != nullptr) {
      ++metrics->stringValues;
      metrics->stringBytes += value.size();
    }
    if (value.isInline()) {
      *target = value;
      return;
    }
    auto& segment = *context.segment_;
    auto& chunk = *context.chunk_;
    const auto heapBlocksBefore = chunk.heapBlocks.size();
    const auto ensureStart = metrics == nullptr ? 0 : metricNowNs();
    auto* heap = context.currentHeap_;
    if (heap == nullptr || heap->used + value.size() > heap->size) {
      heap = &segments_.ensureHeapBlockInChunk(chunk, value.size());
      context.currentHeap_ = heap;
    }
    if (metrics != nullptr) {
      metrics->heapEnsureNs += metricNowNs() - ensureStart;
      metrics->heapAllocations += chunk.heapBlocks.size() - heapBlocksBefore;
    }
    auto* stringTarget = heap->ptr + heap->used;
    const auto copyStart = metrics == nullptr ? 0 : metricNowNs();
    std::memcpy(stringTarget, value.data(), value.size());
    if (metrics != nullptr) {
      metrics->stringCopyNs += metricNowNs() - copyStart;
    }
    heap->used += value.size();
    *target = StringView(stringTarget, value.size());
    if (context.recordedHeapBlock_ != heap->id) {
      const auto recordStart = metrics == nullptr ? 0 : metricNowNs();
      segments_.recordHeapForChunk(chunk, *heap, row);
      context.recordedHeapBlock_ = heap->id;
      if (metrics != nullptr) {
        metrics->heapRecordNs += metricNowNs() - recordStart;
      }
    }
  } else if constexpr (
      Kind == TypeKind::UNKNOWN || !TypeTraits<Kind>::isPrimitiveType ||
      !TypeTraits<Kind>::isFixedWidth) {
    BOLT_NYI("Unsupported store type {}", column.type->toString());
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    *reinterpret_cast<T*>(row + column.offset) =
        decoded.valueAt<T>(sourceIndex);
    if (metrics != nullptr) {
      ++metrics->fixedValues;
    }
  }
}

} // namespace bytedance::bolt::exec::bm
