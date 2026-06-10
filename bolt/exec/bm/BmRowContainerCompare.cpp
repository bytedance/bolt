#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <folly/Portability.h>

#include <algorithm>
#include <bit>
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
  if (FOLLY_LIKELY(leftPrefix != rightPrefix)) {
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

int32_t BmRowContainer::compare(
    const char* left,
    const char* right,
    int32_t column,
    CompareFlags flags) {
  BOLT_DCHECK_LT(column, layout_.columns().size());
  const auto& layout = layout_.column(column);
  if (FOLLY_LIKELY(!layout.nullable)) {
    auto result = compareNonNull(left, right, column);
    return flags.ascending ? result : -result;
  }

  const auto leftNull = layout_.isNull(left, column);
  const auto rightNull = layout_.isNull(right, column);
  if (FOLLY_UNLIKELY(leftNull || rightNull)) {
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

int32_t BmRowContainer::compareNonNull(
    const char* left,
    const char* right,
    int32_t column) const {
  const auto* l = layout_.valueAddress(left, column);
  const auto* r = layout_.valueAddress(right, column);
  return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      compareScalarValue, types_[column]->kind(), l, r, types_[column]);
}

} // namespace bytedance::bolt::exec::bm
