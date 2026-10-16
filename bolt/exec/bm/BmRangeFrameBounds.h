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

#pragma once

#include "bolt/exec/WindowPartition.h"

namespace bytedance::bolt::exec::window {

class BmWindowPartition;

class BmRangeFrameBounds {
 public:
  static void compute(
      const BmWindowPartition& partition,
      bool isStartBound,
      bool isPreceding,
      column_index_t frameColumn,
      vector_size_t startRow,
      vector_size_t numRows,
      const vector_size_t* rawPeerBounds,
      vector_size_t* rawFrameBounds);

 private:
  template <typename BoundTest>
  struct SearchParams {
    bool firstMatch;
    int32_t step;
    BoundTest boundTest;
  };

  template <typename BoundTest>
  static vector_size_t searchFrameValue(
      const BmWindowPartition& partition,
      const SearchParams<BoundTest>& params,
      vector_size_t start,
      column_index_t orderByPhysicalColumn,
      column_index_t framePhysicalColumn,
      const char* frameRow);

  template <typename BoundTest>
  static void updateKRangeFrameBounds(
      const BmWindowPartition& partition,
      const SearchParams<BoundTest>& params,
      column_index_t frameColumn,
      vector_size_t startRow,
      vector_size_t numRows,
      const vector_size_t* rawPeerBounds,
      vector_size_t* rawFrameBounds);
};

} // namespace bytedance::bolt::exec::window
