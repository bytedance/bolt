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

/// Separated from implementation details to reduce compiling time

#include "bolt/exec/HybridSorter.h"
#include "bolt/exec/RowContainer.h"
#include "common/base/CompareFlags.h"
// #include "bolt/type/StringView.h"

#include <vector>
namespace bytedance::bolt::exec {

/// @brief Sortable concept for both RowContainer & SpillRows
template <typename T>
concept RowsIterable = requires(T x) {
  x.begin();
  x.end();
};

constexpr size_t TMP_CODEGEN_KEYS_THRESHOLD = 3;

/// public APIs wrapper
template <RowsIterable SortedRows>
struct MetaRowsSorterWraper {
  /// Sort by all the keys for OrderBy & Spill sort
  static void MetaCodegenSort(
      SortedRows& sortedRows,
      RowContainer* rowContainer,
      HybridSorter& sortAlgo,
      const std::vector<column_index_t>& sortKeysIndices,
      const std::vector<CompareFlags>& sortCompareFlags);

  /// Sort by the specified  keys
  /// For SortedAggregation
  static void MetaCodegenSort(
      SortedRows& sortedRows,
      RowContainer* rowContainer,
      HybridSorter& sortAlgo,
      const std::vector<std::pair<column_index_t, core::SortOrder>>& sortSpecs);
};

/// For Spill
using SpillRows = std::vector<char*, memory::StlAllocator<char*>>;

/// For SortBuffer
using BufferRows = std::vector<char*>;

#ifdef ENABLE_META_SORT
extern template struct MetaRowsSorterWraper<SpillRows>;
extern template struct MetaRowsSorterWraper<BufferRows>;
#endif

} // namespace bytedance::bolt::exec
