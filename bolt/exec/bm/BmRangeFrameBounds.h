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
      const VectorPtr& frameValues,
      vector_size_t frameValueRow);

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
