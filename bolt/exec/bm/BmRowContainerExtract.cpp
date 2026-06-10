#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"

#include <folly/Portability.h>

namespace bytedance::bolt::exec::bm {

void BmRowContainer::extractColumnResident(
    char* const* rows,
    int32_t numRows,
    int32_t column,
    const VectorPtr& result,
    bool exactSize) {
  BOLT_CHECK_LT(column, layout_.columns().size());
  BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      extractColumnTyped,
      types_[column]->kind(),
      rows,
      numRows,
      layout_.column(column),
      result,
      exactSize);
}

template <TypeKind Kind>
void BmRowContainer::extractColumnTyped(
    char* const* rows,
    int32_t numRows,
    const ColumnLayout& column,
    const VectorPtr& result,
    bool /*exactSize*/) const {
  if constexpr (Kind == TypeKind::UNKNOWN) {
    BOLT_NYI("Unsupported extract type {}", column.type->toString());
    return;
  }

  using T = typename TypeTraits<Kind>::NativeType;
  auto* flatResult = result->asFlatVector<T>();
  BOLT_CHECK_NOT_NULL(flatResult);
  result->resize(numRows);

  if (FOLLY_LIKELY(!column.nullable)) {
    result->clearNulls(0, numRows);
    auto values =
        flatResult->mutableValues(numRows)->template asMutableRange<T>();
    for (vector_size_t i = 0; i < numRows; ++i) {
      if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
        flatResult->set(
            i, *reinterpret_cast<const StringView*>(
                   rows[i] + column.offset));
      } else {
        values[i] = *reinterpret_cast<const T*>(rows[i] + column.offset);
      }
    }
    return;
  }

  auto* nulls = result->mutableNulls(numRows)->asMutable<uint64_t>();
  auto values =
      flatResult->mutableValues(numRows)->template asMutableRange<T>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    const auto* row = rows[i];
    if (FOLLY_UNLIKELY(row[column.nullByte] & column.nullMask)) {
      bits::setNull(nulls, i, true);
      continue;
    }
    bits::setNull(nulls, i, false);
    if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
      flatResult->set(
          i,
          *reinterpret_cast<const StringView*>(row + column.offset));
    } else {
      values[i] = *reinterpret_cast<const T*>(row + column.offset);
    }
  }
}

} // namespace bytedance::bolt::exec::bm
