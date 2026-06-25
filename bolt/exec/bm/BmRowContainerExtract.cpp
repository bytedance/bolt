#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/type/HugeInt.h"

#include <folly/Portability.h>

namespace bytedance::bolt::exec::bm {

void BmRowContainer::extractColumnResident(
    const char* const* rows,
    int32_t numRows,
    int32_t column,
    const VectorPtr& result,
    bool exactSize) {
  BOLT_DCHECK_LT(column, layout_.columns().size());
  BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnTyped,
      types_[column]->kind(),
      rows,
      numRows,
      layout_.column(column),
      result,
      exactSize);
}

void BmRowContainer::extractNullsResident(
    const char* const* rows,
    int32_t numRows,
    int32_t column,
    const BufferPtr& result) {
  BOLT_DCHECK_LT(column, layout_.columns().size());
  BOLT_DCHECK(result->size() >= bits::nbytes(numRows));
  auto* rawNulls = result->asMutable<uint64_t>();
  bits::fillBits(rawNulls, 0, numRows, false);
  const auto& columnLayout = layout_.column(column);
  if (!columnLayout.nullable) {
    return;
  }

  for (vector_size_t i = 0; i < numRows; ++i) {
    bits::setBit(
        rawNulls,
        i,
        rows[i][columnLayout.nullByte] & columnLayout.nullMask);
  }
}

template <TypeKind Kind>
void BmRowContainer::extractColumnTyped(
    const char* const* rows,
    int32_t numRows,
    const ColumnLayout& column,
    const VectorPtr& result,
    bool /*exactSize*/) const {
  if constexpr (
      Kind == TypeKind::UNKNOWN || !TypeTraits<Kind>::isPrimitiveType ||
      (!TypeTraits<Kind>::isFixedWidth && Kind != TypeKind::VARCHAR &&
       Kind != TypeKind::VARBINARY)) {
    BOLT_NYI("Unsupported extract type {}", column.type->toString());
    return;
  } else {
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
        } else if constexpr (Kind == TypeKind::HUGEINT) {
          values[i] = HugeInt::deserialize(rows[i] + column.offset);
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
      } else if constexpr (Kind == TypeKind::HUGEINT) {
        values[i] = HugeInt::deserialize(row + column.offset);
      } else {
        values[i] = *reinterpret_cast<const T*>(row + column.offset);
      }
    }
  }
}

} // namespace bytedance::bolt::exec::bm
