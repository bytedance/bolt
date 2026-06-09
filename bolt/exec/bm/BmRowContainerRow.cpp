#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <algorithm>
#include <bit>
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

int32_t normalizeCompare(int32_t result) {
  return result < 0 ? -1 : (result > 0 ? 1 : 0);
}

int32_t compareStringViewsAsc(StringView left, StringView right) {
  uint32_t leftPrefix = *(reinterpret_cast<const uint32_t*>(&left) + 1);
  uint32_t rightPrefix = *(reinterpret_cast<const uint32_t*>(&right) + 1);
  if constexpr (std::endian::native == std::endian::little) {
    leftPrefix = __builtin_bswap32(leftPrefix);
    rightPrefix = __builtin_bswap32(rightPrefix);
  }
  if (leftPrefix != rightPrefix) {
    return leftPrefix < rightPrefix ? -1 : 1;
  }

  const auto suffixSize =
      static_cast<int32_t>(std::min(left.size(), right.size())) -
      StringView::kPrefixSize;
  if (suffixSize <= 0) {
    return normalizeCompare(
        static_cast<int32_t>(left.size()) -
        static_cast<int32_t>(right.size()));
  }

  if (left.isInline() && right.isInline()) {
    uint64_t leftInlined = reinterpret_cast<const uint64_t*>(&left)[1];
    uint64_t rightInlined = reinterpret_cast<const uint64_t*>(&right)[1];
    if constexpr (std::endian::native == std::endian::little) {
      leftInlined = __builtin_bswap64(leftInlined);
      rightInlined = __builtin_bswap64(rightInlined);
    }
    if (leftInlined != rightInlined) {
      return leftInlined < rightInlined ? -1 : 1;
    }
    return normalizeCompare(
        static_cast<int32_t>(left.size()) -
        static_cast<int32_t>(right.size()));
  }

  return normalizeCompare(
      std::string_view(left.data(), left.size())
          .compare(std::string_view(right.data(), right.size())));
}

template <TypeKind Kind>
int32_t compareScalarValue(
    const char* left,
    const char* right,
    const TypePtr& type) {
  if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
    const auto leftValue = *reinterpret_cast<const StringView*>(left);
    const auto rightValue = *reinterpret_cast<const StringView*>(right);
    return compareStringViewsAsc(leftValue, rightValue);
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
  const auto& layout = columns_[column];
  const bool null = decoded.isNullAt(sourceIndex);
  BOLT_CHECK(
      !null || layout.nullable,
      "Column {} is not nullable",
      column);
  setNull(row, column, null);
  if (null) {
    return;
  }

  BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      storeValueTyped,
      types_[column]->kind(),
      decoded,
      sourceIndex,
      row,
      layout);
}

void BmRowContainer::storeColumn(
    const DecodedVector& decoded,
    vector_size_t size,
    char* const* rows,
    int32_t column) {
  BOLT_CHECK_LT(column, columns_.size());
  BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      storeColumnTyped, types_[column]->kind(), decoded, size, rows, column);
}

int32_t BmRowContainer::compare(
    const char* left,
    const char* right,
    int32_t column,
    CompareFlags flags) {
  BOLT_CHECK_LT(column, columns_.size());
  const auto& layout = columns_[column];
  if (!layout.nullable) {
    auto result = compareNonNull(left, right, column);
    return flags.ascending ? result : -result;
  }

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
void BmRowContainer::storeValueTyped(
    const DecodedVector& decoded,
    vector_size_t sourceIndex,
    char* row,
    const ColumnLayout& column) {
  if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
    auto* target = reinterpret_cast<StringView*>(row + column.offset);
    const auto value = decoded.valueAt<StringView>(sourceIndex);
    if (value.isInline()) {
      *target = value;
      return;
    }
    auto& segment = owningActiveSegment(row);
    auto& heap = ensureHeapBlock(segment, value.size());
    auto* stringTarget = heap.ptr + heap.used;
    std::memcpy(stringTarget, value.data(), value.size());
    heap.used += value.size();
    *target = StringView(stringTarget, value.size());
    recordHeapForCurrentPart(segment, heap);
  } else if constexpr (Kind == TypeKind::UNKNOWN) {
    BOLT_NYI("Unsupported store type {}", column.type->toString());
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    static_assert(TypeTraits<Kind>::isFixedWidth);
    *reinterpret_cast<T*>(row + column.offset) =
        decoded.valueAt<T>(sourceIndex);
  }
}

template <TypeKind Kind>
void BmRowContainer::storeColumnTyped(
    const DecodedVector& decoded,
    vector_size_t size,
    char* const* rows,
    int32_t column) {
  const auto& layout = columns_[column];
  if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
    auto rowBelongsToSegment = [](const SegmentData& segment, const char* row) {
      const auto address = reinterpret_cast<uintptr_t>(row);
      for (const auto& block : segment.rowBlocks) {
        const auto begin = reinterpret_cast<uintptr_t>(block.ptr);
        const auto end = begin + block.used;
        if (begin <= address && address < end) {
          return true;
        }
      }
      return false;
    };

    SegmentData* segment = nullptr;
    ChunkId chunkHint = kNoBlock;
    PartId partHint = kNoBlock;
    auto storeString = [&](vector_size_t i, char* row) {
      auto* target = reinterpret_cast<StringView*>(row + layout.offset);
      const auto value = decoded.valueAt<StringView>(i);
      if (value.isInline()) {
        *target = value;
        return;
      }
      if (segment == nullptr || !rowBelongsToSegment(*segment, row)) {
        segment = &owningActiveSegment(row);
        chunkHint = kNoBlock;
        partHint = kNoBlock;
      }
      auto& heap = ensureHeapBlock(*segment, value.size());
      auto* stringTarget = heap.ptr + heap.used;
      std::memcpy(stringTarget, value.data(), value.size());
      heap.used += value.size();
      *target = StringView(stringTarget, value.size());
      recordHeapForRow(*segment, row, heap, chunkHint, partHint);
    };

    if (!layout.nullable) {
      for (vector_size_t i = 0; i < size; ++i) {
        storeString(i, rows[i]);
      }
      return;
    }

    const auto mask = static_cast<char>(layout.nullMask);
    for (vector_size_t i = 0; i < size; ++i) {
      auto* row = rows[i];
      if (decoded.isNullAt(i)) {
        row[layout.nullByte] |= mask;
        continue;
      }
      row[layout.nullByte] &= ~mask;
      storeString(i, row);
    }
    return;
  }

  if (!layout.nullable) {
    for (vector_size_t i = 0; i < size; ++i) {
      storeValueTyped<Kind>(decoded, i, rows[i], layout);
    }
    return;
  }

  const auto mask = static_cast<char>(layout.nullMask);
  for (vector_size_t i = 0; i < size; ++i) {
    auto* row = rows[i];
    if (decoded.isNullAt(i)) {
      row[layout.nullByte] |= mask;
      continue;
    }
    row[layout.nullByte] &= ~mask;
    storeValueTyped<Kind>(decoded, i, row, layout);
  }
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
