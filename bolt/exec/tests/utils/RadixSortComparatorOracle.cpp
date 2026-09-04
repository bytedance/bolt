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

#include <algorithm>
#include <cstring>

#include <gtest/gtest.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/radixsort/RadixSortKeyCodec.h"
#include "bolt/vector/SimpleVector.h"

namespace bytedance::bolt::exec::radixsort::test {

CompareFlags SortComparatorOracle::makeSortFlags(
    bool ascending,
    bool nullsFirst) {
  return CompareFlags{
      .nullsFirst = nullsFirst,
      .ascending = ascending,
      .nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsValue};
}

std::vector<CompareFlags> SortComparatorOracle::allSortFlags() {
  return {
      makeSortFlags(true, true),
      makeSortFlags(true, false),
      makeSortFlags(false, true),
      makeSortFlags(false, false)};
}

int32_t SortComparatorOracle::compareUnsignedBytes(
    std::string_view left,
    std::string_view right) {
  const auto commonSize = std::min(left.size(), right.size());
  const auto result = std::memcmp(left.data(), right.data(), commonSize);
  if (result != 0) {
    return (result > 0) - (result < 0);
  }
  return (left.size() > right.size()) - (left.size() < right.size());
}

int32_t SortComparatorOracle::compareEncodedKeys(
    const EncodedKeyBatch& keys,
    vector_size_t left,
    vector_size_t right) {
  BOLT_CHECK_GE(left, 0);
  BOLT_CHECK_GE(right, 0);
  BOLT_CHECK_LT(left, keys.size());
  BOLT_CHECK_LT(right, keys.size());
  if (keys.format() == EncodedKeyFormat::kFixed64) {
    const auto leftKey = keys.fixedKeyAt(left);
    const auto rightKey = keys.fixedKeyAt(right);
    return (leftKey > rightKey) - (leftKey < rightKey);
  }
  return compareUnsignedBytes(
      keys.variableKeyAt(left), keys.variableKeyAt(right));
}

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

void SortComparatorOracle::expectSorted(
    const RowVector& output,
    const std::vector<column_index_t>& keyChannels,
    const std::vector<CompareFlags>& flags) {
  ASSERT_EQ(keyChannels.size(), flags.size());
  for (const auto channel : keyChannels) {
    ASSERT_LT(channel, output.childrenSize());
  }
  for (vector_size_t row = 1; row < output.size(); ++row) {
    EXPECT_LE(compareRows(output, row - 1, output, row, keyChannels, flags), 0)
        << "row=" << row;
  }
}

void SortComparatorOracle::expectRowsMatchById(
    const RowVector& input,
    const RowVector& output,
    column_index_t idChannel,
    RowIdMatchOptions options) {
  std::vector<bool> localSeen;
  auto* seen = options.seen;
  if (seen == nullptr) {
    ASSERT_EQ(output.size(), input.size());
    localSeen.resize(input.size(), false);
    seen = &localSeen;
  } else {
    ASSERT_EQ(seen->size(), input.size());
  }

  const auto compareFlags = makeSortFlags(true, true);
  const auto* ids =
      output.childAt(idChannel)->asUnchecked<SimpleVector<int64_t>>();
  for (vector_size_t row = 0; row < output.size(); ++row) {
    const auto id = ids->valueAt(row);
    const auto inputRow = id - options.idBase;
    ASSERT_GE(inputRow, 0);
    ASSERT_LT(inputRow, input.size());
    const auto inputIndex = static_cast<vector_size_t>(inputRow);
    EXPECT_FALSE((*seen)[inputIndex]);
    (*seen)[inputIndex] = true;
    for (uint32_t column = 0; column < input.childrenSize(); ++column) {
      if (options.directlyCheckedColumn == column) {
        continue;
      }
      EXPECT_EQ(
          compare(
              *input.childAt(column),
              inputIndex,
              *output.childAt(column),
              row,
              compareFlags),
          0)
          << "row=" << row << ", id=" << id << ", column=" << column;
    }
  }

  if (options.seen == nullptr) {
    EXPECT_TRUE(std::all_of(
        seen->begin(), seen->end(), [](bool value) { return value; }));
  }
}

} // namespace bytedance::bolt::exec::radixsort::test
