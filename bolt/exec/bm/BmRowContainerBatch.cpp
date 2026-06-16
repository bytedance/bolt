#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::exec::bm {

void BmRowContainer::appendBatch(
    const RowVectorPtr& input,
    PartitionId partition,
    std::vector<char*>* rows) {
  BOLT_CHECK_NOT_NULL(input);
  BOLT_CHECK_EQ(input->childrenSize(), types_.size());
  auto* inputRow = input->as<RowVector>();
  BOLT_CHECK_NOT_NULL(inputRow);

  if (input->size() == 0) {
    return;
  }

  std::vector<RowWriteContext> contexts;
  std::vector<char*> rowPointers;
  contexts.reserve(input->size());
  rowPointers.reserve(input->size());
  if (rows != nullptr) {
    rows->reserve(rows->size() + input->size());
  }

  for (vector_size_t row = 0; row < input->size(); ++row) {
    auto context = appendRow(partition);
    auto* rowPointer = context.row();
    contexts.push_back(context);
    rowPointers.push_back(context.row());
    if (rows != nullptr) {
      rows->push_back(rowPointer);
    }
  }

  SelectivityVector allRows(input->size());
  for (auto column = 0; column < inputRow->childrenSize(); ++column) {
    const auto& child = inputRow->childAt(column);
    BOLT_CHECK_EQ(child->type(), types_[column]);
    DecodedVector decoded(*child, allRows);
    const auto& plan = layout_.storePlan(column);
    if (plan.stringKind) {
      for (vector_size_t row = 0; row < input->size(); ++row) {
        // Batch append keeps contexts across row and column loops. Heap block
        // vectors can grow while other rows are stored, so do not reuse a
        // cached BlockRef* captured by an earlier variable-width store.
        contexts[row].currentHeap_ = nullptr;
        contexts[row].recordedHeapBlock_ = kNoBlock;
        store(contexts[row], decoded, row, column);
      }
      continue;
    }

    if (plan.nullable) {
      BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
          storeFixedColumnBatchWithNullsTyped,
          plan.kind,
          decoded,
          input->size(),
          rowPointers.data(),
          plan);
    } else {
      BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
          storeFixedColumnBatchNoNullsTyped,
          plan.kind,
          decoded,
          input->size(),
          rowPointers.data(),
          plan);
    }
  }
}

template <TypeKind Kind>
void BmRowContainer::storeFixedColumnBatchNoNullsTyped(
    const DecodedVector& decoded,
    vector_size_t size,
    char* const* rows,
    const ColumnStorePlan& column) {
  BOLT_DCHECK(!column.stringKind);
  BOLT_DCHECK(!column.nullable);
  if constexpr (
      Kind == TypeKind::UNKNOWN || !TypeTraits<Kind>::isPrimitiveType ||
      !TypeTraits<Kind>::isFixedWidth) {
    BOLT_NYI("Unsupported store type {}", column.type->toString());
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    for (vector_size_t row = 0; row < size; ++row) {
      BOLT_DCHECK(
          !decoded.isNullAt(row),
          "Column {} is not nullable",
          column.type->toString());
      *reinterpret_cast<T*>(rows[row] + column.offset) =
          decoded.valueAt<T>(row);
    }
  }
}

template <TypeKind Kind>
void BmRowContainer::storeFixedColumnBatchWithNullsTyped(
    const DecodedVector& decoded,
    vector_size_t size,
    char* const* rows,
    const ColumnStorePlan& column) {
  BOLT_DCHECK(!column.stringKind);
  BOLT_DCHECK(column.nullable);
  if constexpr (
      Kind == TypeKind::UNKNOWN || !TypeTraits<Kind>::isPrimitiveType ||
      !TypeTraits<Kind>::isFixedWidth) {
    BOLT_NYI("Unsupported store type {}", column.type->toString());
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    const auto nullMask = static_cast<char>(column.nullMask);
    for (vector_size_t row = 0; row < size; ++row) {
      auto* targetRow = rows[row];
      auto& nullByte = targetRow[column.nullByte];
      if (decoded.isNullAt(row)) {
        nullByte |= nullMask;
        continue;
      }
      nullByte &= ~nullMask;
      *reinterpret_cast<T*>(targetRow + column.offset) =
          decoded.valueAt<T>(row);
    }
  }
}

} // namespace bytedance::bolt::exec::bm
