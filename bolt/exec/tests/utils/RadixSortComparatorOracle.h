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

#include <vector>

#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::exec::radixsort::test {

class SortComparatorOracle {
 public:
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
};

} // namespace bytedance::bolt::exec::radixsort::test
