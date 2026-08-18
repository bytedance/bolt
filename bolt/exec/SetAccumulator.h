/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include <folly/container/F14Set.h>
#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/exec/AddressableNonNullValueList.h"
#include "bolt/exec/Strings.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::aggregate::prestosql {

namespace detail {

template <typename T>
struct SetAccumulatorHash : std::hash<T> {};

#if defined(BOLT_CLANG_LIBSTDCXX_INT128_COMPAT)
template <>
struct SetAccumulatorHash<int128_t> {
  size_t operator()(int128_t value) const noexcept {
    return HugeInt::hash(value);
  }
};

template <>
struct SetAccumulatorHash<__uint128_t> {
  size_t operator()(__uint128_t value) const noexcept {
    return HugeInt::hash(value);
  }
};
#endif

/// Maintains a set of unique values. Non-null values are stored in F14FastSet.
/// A separate flag tracks presence of the null value.
template <
    typename T,
#if defined(BOLT_CLANG_LIBSTDCXX_INT128_COMPAT)
    typename Hash = SetAccumulatorHash<T>,
#else
    typename Hash = std::hash<T>,
#endif
    typename EqualTo = std::equal_to<T>>
struct SetAccumulator {
  std::optional<vector_size_t> nullIndex;

  folly::F14FastMap<
      T,
      int32_t,
      Hash,
      EqualTo,
      AlignedStlAllocator<std::pair<const T, vector_size_t>, 16>>
      uniqueValues;

  SetAccumulator(const TypePtr& /*type*/, HashStringAllocator* allocator)
      : uniqueValues{AlignedStlAllocator<std::pair<const T, vector_size_t>, 16>(
            allocator)} {}

  SetAccumulator(Hash hash, EqualTo equalTo, HashStringAllocator* allocator)
      : uniqueValues{
            0,
            hash,
            equalTo,
            AlignedStlAllocator<std::pair<const T, vector_size_t>, 16>(
                allocator)} {}

  /// Adds value if new. No-op if the value was added before.
  void addValue(
      const DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* /*allocator*/) {
    const auto cnt = uniqueValues.size();
    if (decoded.isNullAt(index)) {
      if (!nullIndex.has_value()) {
        nullIndex = cnt;
      }
    } else {
      uniqueValues.insert(
          {decoded.valueAt<T>(index), nullIndex.has_value() ? cnt + 1 : cnt});
    }
  }

  /// Adds new values from an array.
  void addValues(
      const ArrayVector& arrayVector,
      vector_size_t index,
      const DecodedVector& values,
      HashStringAllocator* allocator) {
    BOLT_DCHECK(!arrayVector.isNullAt(index));
    const auto size = arrayVector.sizeAt(index);
    const auto offset = arrayVector.offsetAt(index);

    for (auto i = 0; i < size; ++i) {
      addValue(values, offset + i, allocator);
    }
  }

  /// Adds non-null value if new. No-op if the value is NULL or was added
  /// before.
  void addNonNullValue(
      const DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* /*allocator*/) {
    const auto cnt = uniqueValues.size();
    if (!decoded.isNullAt(index)) {
      uniqueValues.insert({decoded.valueAt<T>(index), cnt});
    }
  }

  /// Adds new non-null values from an array.
  void addNonNullValues(
      const ArrayVector& arrayVector,
      vector_size_t index,
      const DecodedVector& values,
      HashStringAllocator* allocator) {
    BOLT_DCHECK(!arrayVector.isNullAt(index));
    const auto size = arrayVector.sizeAt(index);
    const auto offset = arrayVector.offsetAt(index);

    for (auto i = 0; i < size; ++i) {
      addNonNullValue(values, offset + i, allocator);
    }
  }

  /// Returns number of unique values including null.
  size_t size() const {
    return uniqueValues.size() + (nullIndex.has_value() ? 1 : 0);
  }

  /// Copies the unique values and null into the specified vector starting at
  /// the specified offset.
  vector_size_t extractValues(FlatVector<T>& values, vector_size_t offset) {
    for (auto value : uniqueValues) {
      values.set(offset + value.second, value.first);
    }

    if (nullIndex.has_value()) {
      values.setNull(offset + nullIndex.value(), true);
    }

    return nullIndex.has_value() ? uniqueValues.size() + 1
                                 : uniqueValues.size();
  }

  void free(HashStringAllocator& allocator) {
    using UT = decltype(uniqueValues);
    uniqueValues.~UT();
  }
};

/// Maintains a set of unique strings.
struct StringViewSetAccumulator {
  /// A set of unique StringViews pointing to storage managed by 'strings'.
  SetAccumulator<StringView> base;

  /// Stores unique non-null non-inline strings.
  Strings strings;

  StringViewSetAccumulator(const TypePtr& type, HashStringAllocator* allocator)
      : base{type, allocator} {}

  void addValue(
      const DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* allocator) {
    const auto cnt = base.uniqueValues.size();
    if (decoded.isNullAt(index)) {
      if (!base.nullIndex.has_value()) {
        base.nullIndex = cnt;
      }
    } else {
      auto value = decoded.valueAt<StringView>(index);
      if (!value.isInline()) {
        if (base.uniqueValues.contains(value)) {
          return;
        }
        value = strings.append(value, *allocator);
      }
      base.uniqueValues.insert(
          {value, base.nullIndex.has_value() ? cnt + 1 : cnt});
    }
  }

  void addValues(
      const ArrayVector& arrayVector,
      vector_size_t index,
      const DecodedVector& values,
      HashStringAllocator* allocator) {
    BOLT_DCHECK(!arrayVector.isNullAt(index));
    const auto size = arrayVector.sizeAt(index);
    const auto offset = arrayVector.offsetAt(index);

    for (auto i = 0; i < size; ++i) {
      addValue(values, offset + i, allocator);
    }
  }

  void addNonNullValue(
      const DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* allocator) {
    const auto cnt = base.uniqueValues.size();
    if (!decoded.isNullAt(index)) {
      auto value = decoded.valueAt<StringView>(index);
      if (!value.isInline()) {
        if (base.uniqueValues.contains(value)) {
          return;
        }
        value = strings.append(value, *allocator);
      }
      base.uniqueValues.insert({value, cnt});
    }
  }

  void addNonNullValues(
      const ArrayVector& arrayVector,
      vector_size_t index,
      const DecodedVector& values,
      HashStringAllocator* allocator) {
    BOLT_DCHECK(!arrayVector.isNullAt(index));
    const auto size = arrayVector.sizeAt(index);
    const auto offset = arrayVector.offsetAt(index);

    for (auto i = 0; i < size; ++i) {
      addNonNullValue(values, offset + i, allocator);
    }
  }

  size_t size() const {
    return base.size();
  }

  vector_size_t extractValues(
      FlatVector<StringView>& values,
      vector_size_t offset) {
    return base.extractValues(values, offset);
  }

  void free(HashStringAllocator& allocator) {
    strings.free(allocator);
    using Base = decltype(base);
    base.~Base();
  }
};

/// Maintains a set of unique arrays, maps or structs.
struct ComplexTypeSetAccumulator {
  /// A set of pointers to values stored in AddressableNonNullValueList.
  SetAccumulator<
      AddressableNonNullValueList::Entry,
      AddressableNonNullValueList::Hash,
      AddressableNonNullValueList::EqualTo>
      base;

  /// Stores unique non-null values.
  AddressableNonNullValueList values;

  ComplexTypeSetAccumulator(const TypePtr& type, HashStringAllocator* allocator)
      : base{
            AddressableNonNullValueList::Hash{},
            AddressableNonNullValueList::EqualTo{type},
            allocator} {}

  void addValue(
      const DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* allocator) {
    const auto cnt = base.uniqueValues.size();
    if (decoded.isNullAt(index)) {
      if (!base.nullIndex.has_value()) {
        base.nullIndex = cnt;
      }
    } else {
      auto entry = values.append(decoded, index, allocator);

      if (!base.uniqueValues
               .insert({entry, base.nullIndex.has_value() ? cnt + 1 : cnt})
               .second) {
        values.removeLast(entry);
      }
    }
  }

  void addValues(
      const ArrayVector& arrayVector,
      vector_size_t index,
      const DecodedVector& values,
      HashStringAllocator* allocator) {
    BOLT_DCHECK(!arrayVector.isNullAt(index));
    const auto size = arrayVector.sizeAt(index);
    const auto offset = arrayVector.offsetAt(index);

    for (auto i = 0; i < size; ++i) {
      addValue(values, offset + i, allocator);
    }
  }

  void addNonNullValue(
      const DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* allocator) {
    const auto cnt = base.uniqueValues.size();
    if (!decoded.isNullAt(index)) {
      auto entry = values.append(decoded, index, allocator);

      if (!base.uniqueValues.insert({entry, cnt}).second) {
        values.removeLast(entry);
      }
    }
  }

  void addNonNullValues(
      const ArrayVector& arrayVector,
      vector_size_t index,
      const DecodedVector& values,
      HashStringAllocator* allocator) {
    BOLT_DCHECK(!arrayVector.isNullAt(index));
    const auto size = arrayVector.sizeAt(index);
    const auto offset = arrayVector.offsetAt(index);

    for (auto i = 0; i < size; ++i) {
      addNonNullValue(values, offset + i, allocator);
    }
  }

  size_t size() const {
    return base.size();
  }

  vector_size_t extractValues(BaseVector& values, vector_size_t offset) {
    for (const auto& position : base.uniqueValues) {
      AddressableNonNullValueList::read(
          position.first, values, offset + position.second);
    }

    if (base.nullIndex.has_value()) {
      values.setNull(offset + base.nullIndex.value(), true);
    }

    return base.uniqueValues.size() + (base.nullIndex.has_value() ? 1 : 0);
  }

  void free(HashStringAllocator& allocator) {
    values.free(allocator);
    using Base = decltype(base);
    base.~Base();
  }
};

template <typename T>
struct SetAccumulatorTypeTraits {
  using AccumulatorType = SetAccumulator<T>;
};

template <>
struct SetAccumulatorTypeTraits<StringView> {
  using AccumulatorType = StringViewSetAccumulator;
};

template <>
struct SetAccumulatorTypeTraits<ComplexType> {
  using AccumulatorType = ComplexTypeSetAccumulator;
};
} // namespace detail

template <typename T>
using SetAccumulator =
    typename detail::SetAccumulatorTypeTraits<T>::AccumulatorType;

} // namespace bytedance::bolt::aggregate::prestosql
