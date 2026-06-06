#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/vector/VectorTypeUtils.h"

#include <cmath>
#include <string_view>

namespace bytedance::bolt::exec {
namespace {

bool isNullAt(const char* row, int32_t nullByte, uint8_t nullMask) {
  return (row[nullByte] & nullMask) != 0;
}

template <typename T>
int32_t comparePrimitiveAsc(const T& left, const T& right) {
  if constexpr (std::is_floating_point_v<T>) {
    const bool leftNan = std::isnan(left);
    const bool rightNan = std::isnan(right);
    if (leftNan) {
      return rightNan ? 0 : 1;
    }
    if (rightNan) {
      return -1;
    }
  }
  return left < right ? -1 : left == right ? 0 : 1;
}

int32_t compareStringAsc(StringView left, StringView right) {
  const auto result = std::string_view(left.data(), left.size())
                          .compare(std::string_view(right.data(), right.size()));
  return result < 0 ? -1 : result > 0 ? 1 : 0;
}

} // namespace

int32_t BmRowContainer::compare(
    RowId left,
    RowId right,
    int32_t column,
    CompareFlags flags) {
  BOLT_CHECK_GE(column, 0);
  BOLT_CHECK_LT(column, layout_.numColumns());

  const auto* leftRow = pinRow(left);
  const auto* rightRow = pinRow(right);
  return compareDispatch(
      layout_.typeKindAt(column),
      leftRow,
      rightRow,
      layout_.columnAt(column),
      flags);
}

int32_t BmRowContainer::compareRows(
    RowId left,
    RowId right,
    const std::vector<CompareFlags>& flags) {
  BOLT_CHECK(flags.empty() || flags.size() == layout_.keyTypes().size());
  for (auto i = 0; i < layout_.keyTypes().size(); ++i) {
    const auto result =
        compare(left, right, i, flags.empty() ? CompareFlags() : flags[i]);
    if (result != 0) {
      return result;
    }
  }
  return 0;
}

int32_t BmRowContainer::compareDispatch(
    TypeKind kind,
    const char* left,
    const char* right,
    BmRowColumn column,
    CompareFlags flags) {
  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      compareTyped, kind, left, right, column, flags);
}

template <TypeKind Kind>
int32_t BmRowContainer::compareTyped(
    const char* left,
    const char* right,
    BmRowColumn column,
    CompareFlags flags) {
  if constexpr (
      Kind == TypeKind::UNKNOWN || Kind == TypeKind::OPAQUE ||
      Kind == TypeKind::ARRAY || Kind == TypeKind::MAP ||
      Kind == TypeKind::ROW || Kind == TypeKind::VARIANT) {
    BOLT_NYI(
        "BmRowContainer compare does not support type {} yet",
        mapTypeKindToName(Kind));
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    const bool leftNull = isNullAt(left, column.nullByte(), column.nullMask());
    const bool rightNull = isNullAt(right, column.nullByte(), column.nullMask());
    if (leftNull) {
      return rightNull ? 0 : flags.nullsFirst ? -1 : 1;
    }
    if (rightNull) {
      return flags.nullsFirst ? 1 : -1;
    }

    int32_t result;
    if constexpr (std::is_same_v<T, StringView>) {
      result = compareStringAsc(stringView(left, column), stringView(right, column));
    } else {
      result = comparePrimitiveAsc(
          *reinterpret_cast<const T*>(left + column.offset()),
          *reinterpret_cast<const T*>(right + column.offset()));
    }
    return flags.ascending ? result : -result;
  }
}

} // namespace bytedance::bolt::exec
