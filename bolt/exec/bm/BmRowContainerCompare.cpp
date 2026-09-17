/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/type/HugeInt.h"

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
        static_cast<int32_t>(left.size()) - static_cast<int32_t>(right.size()));
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
        static_cast<int32_t>(left.size()) - static_cast<int32_t>(right.size()));
  }

  return normalizeCompare(
      std::string_view(left.data(), left.size())
          .compare(std::string_view(right.data(), right.size())));
}

ByteInputStream inputFor(const StringView& value) {
  return ByteInputStream({ByteRange{
      reinterpret_cast<uint8_t*>(const_cast<char*>(value.data())),
      static_cast<int32_t>(value.size()),
      0}});
}

template <TypeKind Kind>
int32_t compareScalarValue(
    const char* left,
    const char* right,
    const TypePtr& type,
    CompareFlags flags) {
  int32_t result;
  if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
    const auto leftValue = *reinterpret_cast<const StringView*>(left);
    const auto rightValue = *reinterpret_cast<const StringView*>(right);
    result = compareStringViewsAsc(leftValue, rightValue);
  } else if constexpr (Kind == TypeKind::HUGEINT) {
    const auto leftValue = HugeInt::deserialize(left);
    const auto rightValue = HugeInt::deserialize(right);
    result = leftValue < rightValue ? -1 : (leftValue > rightValue ? 1 : 0);
  } else if constexpr (
      Kind == TypeKind::UNKNOWN || !TypeTraits<Kind>::isPrimitiveType ||
      !TypeTraits<Kind>::isFixedWidth) {
    BOLT_NYI("Unsupported compare type {}", type->toString());
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    result = compareValues<T>(left, right);
  }
  return flags.ascending ? result : -result;
}

template <TypeKind Kind>
uint64_t hashScalarValue(const char* value, const TypePtr& type) {
  if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
    return folly::hasher<StringView>()(
        *reinterpret_cast<const StringView*>(value));
  } else if constexpr (
      Kind == TypeKind::ARRAY || Kind == TypeKind::MAP ||
      Kind == TypeKind::ROW) {
    auto stream = inputFor(*reinterpret_cast<const StringView*>(value));
    return ContainerRowSerde::hash(stream, type.get());
  } else if constexpr (
      Kind == TypeKind::UNKNOWN || !TypeTraits<Kind>::isPrimitiveType ||
      !TypeTraits<Kind>::isFixedWidth) {
    BOLT_NYI("Unsupported hash type {}", type->toString());
  } else if constexpr (Kind == TypeKind::HUGEINT) {
    return folly::hasher<int128_t>()(HugeInt::deserialize(value));
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    return folly::hasher<T>()(*reinterpret_cast<const T*>(value));
  }
}

} // namespace

int32_t BmRowContainer::compare(
    const char* left,
    const char* right,
    int32_t column,
    CompareFlags flags) {
  return compare(left, right, column, column, flags);
}

int32_t BmRowContainer::compare(
    const char* left,
    const char* right,
    int32_t leftColumn,
    int32_t rightColumn,
    CompareFlags flags) {
  BOLT_DCHECK_LT(leftColumn, layout_.columns().size());
  BOLT_DCHECK_LT(rightColumn, layout_.columns().size());
  BOLT_DCHECK_EQ(
      types_[leftColumn]->kind(),
      types_[rightColumn]->kind(),
      "Cannot compare BM columns with different physical kinds: {} vs {}",
      types_[leftColumn]->toString(),
      types_[rightColumn]->toString());
  const auto& leftLayout = layout_.column(leftColumn);
  const auto& rightLayout = layout_.column(rightColumn);
  if (FOLLY_LIKELY(!leftLayout.nullable && !rightLayout.nullable)) {
    return compareNonNull(left, right, leftColumn, rightColumn, flags);
  }

  const auto leftNull = layout_.isNull(left, leftColumn);
  const auto rightNull = layout_.isNull(right, rightColumn);
  if (FOLLY_UNLIKELY(leftNull || rightNull)) {
    if (leftNull && rightNull) {
      return 0;
    }
    const int32_t result = leftNull ? -1 : 1;
    return flags.nullsFirst ? result : -result;
  }

  return compareNonNull(left, right, leftColumn, rightColumn, flags);
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
    int32_t leftColumn,
    int32_t rightColumn,
    CompareFlags flags) const {
  const auto* l = layout_.valueAddress(left, leftColumn);
  const auto* r = layout_.valueAddress(right, rightColumn);
  if (layout_.column(leftColumn).variableWidth &&
      !layout_.storePlan(leftColumn).stringKind) {
    auto leftStream = inputFor(*reinterpret_cast<const StringView*>(l));
    auto rightStream = inputFor(*reinterpret_cast<const StringView*>(r));
    return ContainerRowSerde::compare(
        leftStream, rightStream, types_[leftColumn].get(), flags);
  }
  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      compareScalarValue,
      types_[leftColumn]->kind(),
      l,
      r,
      types_[leftColumn],
      flags);
}

uint64_t BmRowContainer::hash(const char* row, int32_t column) const {
  BOLT_DCHECK_NOT_NULL(row);
  BOLT_DCHECK_LT(column, layout_.columns().size());
  if (layout_.isNull(row, column)) {
    return BaseVector::kNullHash;
  }
  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      hashScalarValue,
      types_[column]->kind(),
      layout_.valueAddress(row, column),
      types_[column]);
}

} // namespace bytedance::bolt::exec::bm
