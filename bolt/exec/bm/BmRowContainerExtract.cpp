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
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) {
  BOLT_DCHECK_LT(column, layout_.columns().size());
  BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnTyped,
      types_[column]->kind(),
      rows,
      numRows,
      layout_.column(column),
      resultOffset,
      result,
      exactSize);
}

void BmRowContainer::extractColumnResident(
    const char* const* rows,
    folly::Range<const vector_size_t*> rowNumbers,
    int32_t column,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) {
  BOLT_DCHECK_LT(column, layout_.columns().size());
  BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnByRowNumbersTyped,
      types_[column]->kind(),
      rows,
      rowNumbers,
      layout_.column(column),
      resultOffset,
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
        rawNulls, i, rows[i][columnLayout.nullByte] & columnLayout.nullMask);
  }
}

template <TypeKind Kind>
void BmRowContainer::extractColumnByRowNumbersTyped(
    const char* const* rows,
    folly::Range<const vector_size_t*> rowNumbers,
    const ColumnLayout& column,
    vector_size_t resultOffset,
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
    const auto numRows = static_cast<vector_size_t>(rowNumbers.size());
    const auto resultSize = resultOffset + numRows;
    result->resize(resultSize);
    auto values =
        flatResult->mutableValues(resultSize)->template asMutableRange<T>();

    if (FOLLY_LIKELY(!column.nullable)) {
      result->clearNulls(resultOffset, resultSize);
      for (vector_size_t i = 0; i < numRows; ++i) {
        const auto resultRow = resultOffset + i;
        const auto rowNumber = rowNumbers[i];
        if (FOLLY_UNLIKELY(rowNumber < 0)) {
          result->setNull(resultRow, true);
          continue;
        }
        const auto* row = rows[rowNumber];
        if constexpr (
            Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
          flatResult->set(
              resultRow,
              *reinterpret_cast<const StringView*>(row + column.offset));
        } else if constexpr (Kind == TypeKind::HUGEINT) {
          values[resultRow] = HugeInt::deserialize(row + column.offset);
        } else {
          values[resultRow] = *reinterpret_cast<const T*>(row + column.offset);
        }
      }
      return;
    }

    auto* nulls = result->mutableRawNulls();
    for (vector_size_t i = 0; i < numRows; ++i) {
      const auto resultRow = resultOffset + i;
      const auto rowNumber = rowNumbers[i];
      if (FOLLY_UNLIKELY(rowNumber < 0)) {
        bits::setNull(nulls, resultRow, true);
        continue;
      }

      const auto* row = rows[rowNumber];
      if (FOLLY_UNLIKELY(row[column.nullByte] & column.nullMask)) {
        bits::setNull(nulls, resultRow, true);
        continue;
      }
      bits::setNull(nulls, resultRow, false);
      if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
        flatResult->set(
            resultRow,
            *reinterpret_cast<const StringView*>(row + column.offset));
      } else if constexpr (Kind == TypeKind::HUGEINT) {
        values[resultRow] = HugeInt::deserialize(row + column.offset);
      } else {
        values[resultRow] = *reinterpret_cast<const T*>(row + column.offset);
      }
    }
  }
}

template <TypeKind Kind>
void BmRowContainer::extractColumnTyped(
    const char* const* rows,
    int32_t numRows,
    const ColumnLayout& column,
    vector_size_t resultOffset,
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
    const auto resultSize = resultOffset + numRows;
    result->resize(resultSize);

    if (FOLLY_LIKELY(!column.nullable)) {
      result->clearNulls(resultOffset, resultSize);
      auto values =
          flatResult->mutableValues(resultSize)->template asMutableRange<T>();
      for (vector_size_t i = 0; i < numRows; ++i) {
        const auto resultRow = resultOffset + i;
        if constexpr (
            Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
          flatResult->set(
              resultRow,
              *reinterpret_cast<const StringView*>(rows[i] + column.offset));
        } else if constexpr (Kind == TypeKind::HUGEINT) {
          values[resultRow] = HugeInt::deserialize(rows[i] + column.offset);
        } else {
          values[resultRow] =
              *reinterpret_cast<const T*>(rows[i] + column.offset);
        }
      }
      return;
    }

    auto* nulls = result->mutableRawNulls();
    auto values =
        flatResult->mutableValues(resultSize)->template asMutableRange<T>();
    for (vector_size_t i = 0; i < numRows; ++i) {
      const auto* row = rows[i];
      const auto resultRow = resultOffset + i;
      if (FOLLY_UNLIKELY(row[column.nullByte] & column.nullMask)) {
        bits::setNull(nulls, resultRow, true);
        continue;
      }
      bits::setNull(nulls, resultRow, false);
      if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
        flatResult->set(
            resultRow,
            *reinterpret_cast<const StringView*>(row + column.offset));
      } else if constexpr (Kind == TypeKind::HUGEINT) {
        values[resultRow] = HugeInt::deserialize(row + column.offset);
      } else {
        values[resultRow] = *reinterpret_cast<const T*>(row + column.offset);
      }
    }
  }
}

} // namespace bytedance::bolt::exec::bm
