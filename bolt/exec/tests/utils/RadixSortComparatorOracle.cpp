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

#include "bolt/exec/tests/utils/RadixSortComparatorOracle.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::exec::radixsort::test {

int32_t SortComparatorOracle::compare(
    const BaseVector& left,
    vector_size_t leftIndex,
    const BaseVector& right,
    vector_size_t rightIndex,
    CompareFlags flags) {
  BOLT_CHECK(left.type()->equivalent(*right.type()));
  BOLT_CHECK_LT(leftIndex, left.size());
  BOLT_CHECK_LT(rightIndex, right.size());
  BOLT_CHECK(!flags.equalsOnly);
  BOLT_CHECK(
      flags.nullHandlingMode == CompareFlags::NullHandlingMode::kNullAsValue);
  auto result = left.compare(&right, leftIndex, rightIndex, flags);
  BOLT_CHECK(result.has_value());
  return (*result > 0) - (*result < 0);
}

int32_t SortComparatorOracle::compareRows(
    const RowVector& left,
    vector_size_t leftIndex,
    const RowVector& right,
    vector_size_t rightIndex,
    const std::vector<column_index_t>& keyChannels,
    const std::vector<CompareFlags>& flags) {
  BOLT_CHECK_EQ(keyChannels.size(), flags.size());
  for (uint32_t key = 0; key < keyChannels.size(); ++key) {
    const auto channel = keyChannels[key];
    BOLT_CHECK_LT(channel, left.childrenSize());
    BOLT_CHECK_LT(channel, right.childrenSize());
    const auto result = compare(
        *left.childAt(channel),
        leftIndex,
        *right.childAt(channel),
        rightIndex,
        flags[key]);
    if (result != 0) {
      return result;
    }
  }
  return 0;
}

} // namespace bytedance::bolt::exec::radixsort::test
