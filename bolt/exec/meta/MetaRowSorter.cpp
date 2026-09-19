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
 */

#ifdef ENABLE_META_SORT

// clang-format off
#include "bolt/type/StringView.h"  // to avoid fmt::format compiling error
#include "bolt/exec/meta/MetaRowSorterApi.h"
#include "bolt/exec/meta/MetaRowSorter.h"
#include "bolt/exec/meta/MetaTypesDispatcher.h"
#include "bolt/type/Type.h"
// clang-format on

#include <vector>
namespace bytedance::bolt::exec {

template <RowsIterable SortedRows>
void MetaRowsSorterWraper<SortedRows>::MetaCodegenSort(
    SortedRows& sortedRows,
    RowContainer* rowContainer,
    HybridSorter& sortAlgo,
    const std::vector<column_index_t>& sortKeysIndices,
    const std::vector<CompareFlags>& sortCompareFlags) {
  SorterDataArgs<SortedRows> dataArgs(
      sortAlgo, sortedRows, *rowContainer, sortKeysIndices, sortCompareFlags);

  auto&& types = rowContainer->keyTypes();

  bool codeGenSupported = std::all_of(
      sortKeysIndices.cbegin(),
      sortKeysIndices.cend(),
      [&types](const auto& i) { return types[i]->kind() < TypeKind::HUGEINT; });

  if (codeGenSupported) {
    auto cmpKeysSize = sortKeysIndices.size();
    if (cmpKeysSize == 1) {
      TypesDispatcher<>(
          dataArgs, TypeKindHolder<>(), types[sortKeysIndices[0]]->kind());
    } else if (cmpKeysSize == 2) {
      TypesDispatcher(
          dataArgs,
          TypeKindHolder<>(),
          types[sortKeysIndices[0]]->kind(),
          types[sortKeysIndices[1]]->kind());
    } else if (cmpKeysSize >= 3) {
      TypesDispatcher(
          dataArgs,
          TypeKindHolder<>(),
          types[sortKeysIndices[0]]->kind(),
          types[sortKeysIndices[1]]->kind(),
          types[sortKeysIndices[2]]->kind());
    }
  } else {
    sortAlgo.sort(
        sortedRows.begin(),
        sortedRows.end(),
        [&rowContainer, &sortCompareFlags](
            const char* leftRow, const char* rightRow) {
          for (vector_size_t index = 0; index < sortCompareFlags.size();
               ++index) {
            if (auto result = rowContainer->compare(
                    leftRow, rightRow, index, sortCompareFlags[index])) {
              return result < 0;
            }
          }
          return false;
        });
  }
}

/// For SortedAggregation
template <RowsIterable SortedRows>
void MetaRowsSorterWraper<SortedRows>::MetaCodegenSort(
    SortedRows& sortedRows,
    RowContainer* rowContainer,
    HybridSorter& sortAlgo,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortSpecs) {
  std::vector<column_index_t> sortKeysIndices;
  std::vector<CompareFlags> sortCompareFlags;
  for (auto&& key : sortSpecs) {
    sortKeysIndices.emplace_back(key.first);
    sortCompareFlags.emplace_back<CompareFlags>(
        {.nullsFirst = key.second.isNullsFirst(),
         .ascending = key.second.isAscending()});
  }

  return MetaCodegenSort(
      sortedRows, rowContainer, sortAlgo, sortKeysIndices, sortCompareFlags);
}

/// Hide template implementation
/// And instantiate here to reduce compiling time
template struct MetaRowsSorterWraper<SpillRows>;
template struct MetaRowsSorterWraper<BufferRows>;
template struct MetaRowsSorterWraper<folly::Range<char**>>;

} // namespace bytedance::bolt::exec

#endif // ENABLE_META_SORT
