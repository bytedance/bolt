#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <folly/Portability.h>

#include <cstring>

namespace bytedance::bolt::exec::bm {

void BmRowContainer::store(
    RowWriteContext& context,
    const DecodedVector& decoded,
    vector_size_t sourceIndex,
    int32_t column) {
  storeValue(decoded, sourceIndex, context, column);
}

std::vector<char*> BmRowContainer::appendBatch(
    const RowVectorPtr& input,
    PartitionId partition) {
  BOLT_CHECK_EQ(input->childrenSize(), types_.size());
  auto* inputRow = input->as<RowVector>();
  BOLT_CHECK_NOT_NULL(inputRow);

  std::vector<char*> rows;
  rows.reserve(input->size());
  // TODO: Consecutive rows in a batch usually share segment/chunk. Track
  // append ranges and use one shared write context per range for variable-width
  // stores instead of keeping one RowWriteContext per row.
  std::vector<RowWriteContext> contexts;
  contexts.reserve(input->size());
  for (vector_size_t row = 0; row < input->size(); ++row) {
    contexts.push_back(appendRow(partition));
    rows.push_back(contexts.back().row());
  }

  SelectivityVector allRows(input->size());
  for (auto column = 0; column < inputRow->childrenSize(); ++column) {
    BOLT_CHECK_EQ(inputRow->childAt(column)->type(), types_[column]);
    DecodedVector decoded(*inputRow->childAt(column), allRows);
    const auto kind = types_[column]->kind();
    if (kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY) {
      for (vector_size_t row = 0; row < input->size(); ++row) {
        store(contexts[row], decoded, row, column);
      }
    } else {
      BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
          storeFixedColumnTyped,
          kind,
          decoded,
          input->size(),
          rows.data(),
          column);
    }
  }

  return rows;
}

void BmRowContainer::storeValue(
    const DecodedVector& decoded,
    vector_size_t sourceIndex,
    RowWriteContext& context,
    int32_t column) {
  BOLT_DCHECK_NOT_NULL(context.row_);
  BOLT_DCHECK_LT(column, layout_.columns().size());
  const auto& layout = layout_.column(column);
  if (FOLLY_LIKELY(!layout.nullable)) {
    BOLT_DCHECK(
        !decoded.isNullAt(sourceIndex),
        "Column {} is not nullable",
        column);
    BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
        storeValueTyped,
        types_[column]->kind(),
        decoded,
        sourceIndex,
        context,
        layout);
    return;
  }

  const bool null = decoded.isNullAt(sourceIndex);
  layout_.setNull(context.row_, column, null);
  if (FOLLY_UNLIKELY(null)) {
    return;
  }

  BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      storeValueTyped,
      types_[column]->kind(),
      decoded,
      sourceIndex,
      context,
      layout);
}

template <TypeKind Kind>
void BmRowContainer::storeValueTyped(
    const DecodedVector& decoded,
    vector_size_t sourceIndex,
    RowWriteContext& context,
    const ColumnLayout& column) {
  auto* row = context.row_;
  if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
    auto* target = reinterpret_cast<StringView*>(row + column.offset);
    const auto value = decoded.valueAt<StringView>(sourceIndex);
    if (value.isInline()) {
      *target = value;
      return;
    }
    auto& segment = segments_.segmentData(context.segment_);
    auto& heap =
        segments_.ensureHeapBlockForChunk(segment, context.chunk_, value.size());
    auto* stringTarget = heap.ptr + heap.used;
    std::memcpy(stringTarget, value.data(), value.size());
    heap.used += value.size();
    *target = StringView(stringTarget, value.size());
    segments_.recordHeapForChunk(segment, context.chunk_, heap, row);
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

template <TypeKind Kind>
void BmRowContainer::storeFixedColumnTyped(
    const DecodedVector& decoded,
    vector_size_t size,
    char* const* rows,
    int32_t column) {
  const auto& layout = layout_.column(column);
  if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
    BOLT_FAIL(
        "Variable-width columns must be stored through RowWriteContext");
    return;
  } else if constexpr (
      Kind == TypeKind::UNKNOWN || !TypeTraits<Kind>::isPrimitiveType ||
      !TypeTraits<Kind>::isFixedWidth) {
    BOLT_NYI("Unsupported store type {}", layout.type->toString());
    return;
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    if (FOLLY_LIKELY(!layout.nullable)) {
      for (vector_size_t i = 0; i < size; ++i) {
        *reinterpret_cast<T*>(rows[i] + layout.offset) =
            decoded.valueAt<T>(i);
      }
      return;
    }

    const auto mask = static_cast<char>(layout.nullMask);
    for (vector_size_t i = 0; i < size; ++i) {
      auto* row = rows[i];
      if (FOLLY_UNLIKELY(decoded.isNullAt(i))) {
        row[layout.nullByte] |= mask;
        continue;
      }
      row[layout.nullByte] &= ~mask;
      *reinterpret_cast<T*>(row + layout.offset) = decoded.valueAt<T>(i);
    }
  }
}

} // namespace bytedance::bolt::exec::bm
