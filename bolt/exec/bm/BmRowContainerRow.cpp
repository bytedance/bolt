#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <cstring>
#include <string_view>

namespace bytedance::bolt::exec::bm {
namespace {

template <typename T>
int32_t compareValues(const char* left, const char* right) {
  const auto l = *reinterpret_cast<const T*>(left);
  const auto r = *reinterpret_cast<const T*>(right);
  return l < r ? -1 : (l > r ? 1 : 0);
}

template <TypeKind Kind>
void storeFixedWidthValue(
    const DecodedVector& decoded,
    vector_size_t sourceIndex,
    char* target,
    const TypePtr& type) {
  if constexpr (
      Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY ||
      Kind == TypeKind::UNKNOWN) {
    BOLT_NYI("Unsupported store type {}", type->toString());
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    static_assert(TypeTraits<Kind>::isFixedWidth);
    *reinterpret_cast<T*>(target) = decoded.valueAt<T>(sourceIndex);
  }
}

template <TypeKind Kind>
int32_t compareScalarValue(
    const char* left,
    const char* right,
    const TypePtr& type) {
  if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
    const auto leftValue = *reinterpret_cast<const StringView*>(left);
    const auto rightValue = *reinterpret_cast<const StringView*>(right);
    const auto cmp = std::string_view(leftValue.data(), leftValue.size())
                         .compare(std::string_view(
                             rightValue.data(), rightValue.size()));
    return cmp < 0 ? -1 : (cmp > 0 ? 1 : 0);
  } else if constexpr (Kind == TypeKind::UNKNOWN) {
    BOLT_NYI("Unsupported compare type {}", type->toString());
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    static_assert(TypeTraits<Kind>::isFixedWidth);
    return compareValues<T>(left, right);
  }
}

} // namespace

void BmRowContainer::store(
    const DecodedVector& decoded,
    vector_size_t sourceIndex,
    char* row,
    int32_t column) {
  BOLT_CHECK_NOT_NULL(row);
  BOLT_CHECK_LT(column, columns_.size());
  const bool null = decoded.isNullAt(sourceIndex);
  BOLT_CHECK(
      !null || columns_[column].nullable,
      "Column {} is not nullable",
      column);
  setNull(row, column, null);
  if (null) {
    return;
  }

  auto* target = valueAddress(row, column);
  const auto kind = types_[column]->kind();
  if (kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY) {
    const auto value = decoded.valueAt<StringView>(sourceIndex);
    if (value.isInline()) {
      *reinterpret_cast<StringView*>(target) = value;
      return;
    }
    auto& segment = owningActiveSegment(row);
    auto& heap = ensureHeapBlock(segment, value.size());
    auto* stringTarget = heap.ptr + heap.used;
    std::memcpy(stringTarget, value.data(), value.size());
    heap.used += value.size();
    *reinterpret_cast<StringView*>(target) =
        StringView(stringTarget, value.size());
    recordHeapForCurrentPart(segment, heap);
    return;
  }
  BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      storeFixedWidthValue, kind, decoded, sourceIndex, target, types_[column]);
}

int32_t BmRowContainer::compare(
    const char* left,
    const char* right,
    int32_t column,
    CompareFlags flags) {
  const auto leftNull = isNull(left, column);
  const auto rightNull = isNull(right, column);
  if (leftNull || rightNull) {
    if (leftNull && rightNull) {
      return 0;
    }
    const int32_t result = leftNull ? -1 : 1;
    return flags.nullsFirst ? result : -result;
  }

  auto result = compareNonNull(left, right, column);
  if (!flags.ascending) {
    result = -result;
  }
  return result;
}

int32_t BmRowContainer::compareRows(
    const char* left,
    const char* right,
    const std::vector<CompareFlags>& flags) {
  const auto numColumns = types_.size();
  for (int32_t i = 0; i < numColumns; ++i) {
    const auto result =
        compare(left, right, i, i < flags.size() ? flags[i] : CompareFlags{});
    if (result != 0) {
      return result;
    }
  }
  return 0;
}

void BmRowContainer::extractColumnResident(
    char* const* rows,
    int32_t numRows,
    int32_t column,
    const VectorPtr& result,
    bool exactSize) {
  BOLT_CHECK_LT(column, columns_.size());
  BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      extractColumnTyped,
      types_[column]->kind(),
      rows,
      numRows,
      columns_[column],
      result,
      exactSize);
}

bool BmRowContainer::isNull(const char* row, int32_t column) const {
  const auto& layout = columns_[column];
  return layout.nullMask != 0 && (row[layout.nullByte] & layout.nullMask);
}

void BmRowContainer::setNull(char* row, int32_t column, bool null) const {
  const auto& layout = columns_[column];
  if (layout.nullMask == 0) {
    BOLT_CHECK(!null, "Column {} is not nullable", column);
    return;
  }
  auto& byte = row[layout.nullByte];
  const auto mask = static_cast<char>(layout.nullMask);
  if (null) {
    byte |= mask;
  } else {
    byte &= ~mask;
  }
}

char* BmRowContainer::valueAddress(char* row, int32_t column) const {
  return row + columns_[column].offset;
}

const char* BmRowContainer::valueAddress(
    const char* row,
    int32_t column) const {
  return row + columns_[column].offset;
}

int32_t BmRowContainer::compareNonNull(
    const char* left,
    const char* right,
    int32_t column) const {
  const auto* l = valueAddress(left, column);
  const auto* r = valueAddress(right, column);
  return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      compareScalarValue, types_[column]->kind(), l, r, types_[column]);
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

  if (!column.nullable) {
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
    if (row[column.nullByte] & column.nullMask) {
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
