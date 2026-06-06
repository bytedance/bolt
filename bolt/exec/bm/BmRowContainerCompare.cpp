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

  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      compareTyped,
      layout_.typeKindAt(column),
      left,
      right,
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

template <TypeKind Kind>
int32_t BmRowContainer::compareTyped(
    RowId left,
    RowId right,
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
    const auto* leftRow = pinRow(left);
    const bool leftNull =
        isNullAt(leftRow, column.nullByte(), column.nullMask());
    std::conditional_t<std::is_same_v<T, StringView>, std::string, T>
        leftValue{};
    if (!leftNull) {
      if constexpr (std::is_same_v<T, StringView>) {
        auto value = stringView(leftRow, column);
        leftValue.assign(value.data(), value.size());
      } else {
        leftValue = *reinterpret_cast<const T*>(leftRow + column.offset());
      }
    }

    const auto* rightRow = pinRow(right);
    const bool rightNull =
        isNullAt(rightRow, column.nullByte(), column.nullMask());
    if (leftNull) {
      return rightNull ? 0 : flags.nullsFirst ? -1 : 1;
    }
    if (rightNull) {
      return flags.nullsFirst ? 1 : -1;
    }

    int32_t result;
    if constexpr (std::is_same_v<T, StringView>) {
      auto rightValue = stringView(rightRow, column);
      result = compareStringAsc(
          StringView(leftValue), rightValue);
    } else {
      result = comparePrimitiveAsc(
          leftValue,
          *reinterpret_cast<const T*>(rightRow + column.offset()));
    }
    return flags.ascending ? result : -result;
  }
}

} // namespace bytedance::bolt::exec
