/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#pragma once

#include <common/base/CompareFlags.h>
#include <limits>
#pragma once

#include "bolt/exec/RowContainer.h"
#include "bolt/exec/meta/MetaTypeUtils.h"

#include <concepts>
#include <vector>
namespace bytedance::bolt::exec {

/// @brief Comparing keys in a RowContainer with the other RowContainer
///         or a Vector(column-based).
/// Use this templated, recursive and inlined function to generate compare
/// code. this will eliminate overhead of function call & type dispatching &
/// loop of keys at runtime. We use templates to generate code at compiling
/// stage
///
/// Note that:
/// 1. To prevent template code explosion, use this class only when the
/// number of keys is <= 3.
/// 2. for Simplicity, only support native types. Map,Array,Struct are not
/// supported. Luckily, these types are seldom used for sorting
/// 3. In theory, CompareFlags can also be used as template parameters to
/// boost the performance. but, it would be better if we encoding CompareFlags
/// into RowContainer in future
///
/// @tparam Kinds types of the keys in a row (RowContainer)
template <TypeKind... Kinds>
struct MetaRowComparator {
  MetaRowComparator(
      RowContainer& container,
      const std::vector<column_index_t>& sortKeysIndices,
      const std::vector<CompareFlags>& flags)
      : container(container),
        sortKeysIndices(sortKeysIndices),
        compareflags(flags),
        rowColumns(container.getRowColumn()) {}

  static constexpr TypeKind typeKinds[] = {Kinds...};
  static constexpr std::size_t typesSize{sizeof...(Kinds)};

  /// Compare row with row
  bool operator()(const char* l, const char* r) {
    auto result = compare<0, Kinds...>(l, r);

    // If number of keys > 3, partially generate code for the first 3 keys
    // and compare the remaining keys via type dispatching at runtime
    for (auto i = sizeof...(Kinds); result == 0 && i < sortKeysIndices.size();
         ++i) {
      result =
          container.compare(l, r, (int)sortKeysIndices[i], compareflags[i]);
    }

    // Only for debugging
    // #ifndef NDEBUG
    //     decltype(result) origResult{0};
    //     for (auto i = 0; i < sortKeysIndices.size(); ++i) {
    //       origResult =
    //           container.compare(l, r, (int)sortKeysIndices[i],
    //           compareflags[i]);
    //       if (origResult) {
    //         break;
    //       }
    //     }
    //     if (origResult != result) {
    //       result = compare<0, Kinds...>(l, r);

    //       for (auto i = 0; i < sortKeysIndices.size(); ++i) {
    //         origResult =
    //             container.compare(l, r, (int)sortKeysIndices[i],
    //             compareflags[i]);
    //         if (origResult) {
    //           break;
    //         }
    //       }
    //     }
    // #endif

    return result < 0;
  }

  /// TODO: Compare row with column
  // bool operator()(const char *l,  const DecodedVector& decoded, vector_size_t
  // index)

  // Calculating column offset in Row at compiling stage
  template <size_t N>
  constexpr int getColumnOffset() {
    if constexpr (N == 0) {
      return 0;
    } else {
      // sizeof(typename KindToFlatVector<Kind>::HashRowType);
      return getColumnOffset<N - 1>() +
          sizeof(typename TypeTraits<typeKinds[N]>::NativeType);
    }
  }

  /// @brief  Used for comparing rows in RowContainer
  /// @tparam INDEX
  /// @tparam Kind
  /// @tparam ...Others
  /// @param l
  /// @param r
  /// @return
  template <int INDEX, TypeKind Kind, TypeKind... Others>
  int compare(const char* left, const char* right) { // ALWAYS_INLINE
    using T = typename TypeTraits<Kind>::NativeType;
    int res = 0;

    auto&& flags = compareflags[INDEX];

    // get offset at compiling stage
    // constexpr auto offset = getColumnOffset<INDEX>();
    auto offset = rowColumns[sortKeysIndices[INDEX]].offset();
    T leftValue = RowContainer::valueAt<T>((char*)left, offset);
    T rightValue = RowContainer::valueAt<T>((char*)right, offset);

    // Since null values have been already set as limit<T>::max in the Row
    // format, Only check nullity for 'ASC + NULLS FIRST' and 'DESC + NULLS
    // LAST' No need to check nullity for 'ASC + NULLS LAST' and 'DESC + NULLS
    // FIRST'
    // For Spill and Agg, sort flags make no sense, set as 'ASC + NULLS FIRST'
    // so that we can ignore nullity check.
    bool checkNullity =
        (flags.nullsFirst == flags.ascending) || Kind == TypeKind::BOOLEAN;

    if constexpr (
        Kind != TypeKind::VARCHAR && Kind != TypeKind::VARBINARY &&
        Kind != TypeKind::BOOLEAN) {
      if (!checkNullity) {
        // there is a case, we still have to check nullity.
        // For numeric types, if both values are max values.
        // we have to check nullity to avoid 'max' == 'null'
        checkNullity = leftValue == std::numeric_limits<T>::max() &&
            rightValue == std::numeric_limits<T>::max();
      }
    }

    if (checkNullity) {
      bool leftIsNull = RowContainer::isNullAt(
          left,
          rowColumns[sortKeysIndices[INDEX]].nullByte(),
          rowColumns[INDEX].nullMask());
      bool rightIsNull = RowContainer::isNullAt(
          right, rowColumns[INDEX].nullByte(), rowColumns[INDEX].nullMask());

      if (leftIsNull) {
        if (!rightIsNull) {
          return flags.nullsFirst ? -1 : 1;
        }

        // if both are null, continue to check following keys
        if constexpr (sizeof...(Others) > 0) {
          res = compare<INDEX + 1, Others...>(left, right);
        }
        return res;
      }
      if (rightIsNull) {
        return flags.nullsFirst ? 1 : -1;
      }
    }

    if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
      auto result = RowContainer::compareStringAsc(leftValue, rightValue);
      res = flags.ascending ? result : result * -1;
    } else {
      auto result = SimpleVector<T>::comparePrimitiveAsc(leftValue, rightValue);
      res = flags.ascending ? result : result * -1;
    }

    // Compiler will optimize this tail recursion at compiling time.
    if constexpr (sizeof...(Others) > 0) {
      if (res == 0) {
        res = compare<INDEX + 1, Others...>(left, right);
      }
    }
    return res;
  }

  RowContainer& container;
  const std::vector<column_index_t>& sortKeysIndices;
  const std::vector<CompareFlags>& compareflags;
  const std::vector<RowColumn>& rowColumns;
};

} // namespace bytedance::bolt::exec
