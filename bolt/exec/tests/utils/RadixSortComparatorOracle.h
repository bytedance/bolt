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

#include <optional>
#include <string_view>
#include <vector>

#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::exec::radixsort {

class EncodedKeyBatch;

namespace test {

struct RowIdMatchOptions {
  int64_t idBase{0};
  std::optional<column_index_t> directlyCheckedColumn;
  std::vector<bool>* seen{nullptr};
};

class SortComparatorOracle {
 public:
  static CompareFlags makeSortFlags(bool ascending, bool nullsFirst);

  static std::vector<CompareFlags> allSortFlags();

  static int32_t compareUnsignedBytes(
      std::string_view left,
      std::string_view right);

  static int32_t compareEncodedKeys(
      const EncodedKeyBatch& keys,
      vector_size_t left,
      vector_size_t right);

  static int32_t compare(
      const BaseVector& left,
      vector_size_t leftIndex,
      const BaseVector& right,
      vector_size_t rightIndex,
      CompareFlags flags);

  static int32_t compareRows(
      const RowVector& left,
      vector_size_t leftIndex,
      const RowVector& right,
      vector_size_t rightIndex,
      const std::vector<column_index_t>& keyChannels,
      const std::vector<CompareFlags>& flags);

  static void expectSorted(
      const RowVector& output,
      const std::vector<column_index_t>& keyChannels,
      const std::vector<CompareFlags>& flags);

  static void expectRowsMatchById(
      const RowVector& input,
      const RowVector& output,
      column_index_t idChannel,
      RowIdMatchOptions options = {});
};

} // namespace test
} // namespace bytedance::bolt::exec::radixsort
