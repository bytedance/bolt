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

#include "bolt/exec/HashTable.h"
#include "bolt/exec/Spiller.h"
#include "bolt/exec/WindowBuild.h"
namespace bytedance::bolt::exec {

class TopNComparator {
 public:
  TopNComparator(
      const std::vector<std::pair<column_index_t, core::SortOrder>>& keyInfo,
      RowContainer* rowContainer)
      : keyInfo_(keyInfo), rowContainer_(rowContainer){};

  /// Returns true if lhs < rhs, false otherwise.
  bool operator()(const char* lhs, const char* rhs) {
    if (lhs == rhs) {
      return false;
    }
    for (auto& key : keyInfo_) {
      if (auto result = rowContainer_->compare(
              lhs,
              rhs,
              key.first,
              {key.second.isNullsFirst(), key.second.isAscending(), false})) {
        return result < 0;
      }
    }
    return false;
  };

 private:
  std::vector<std::pair<column_index_t, core::SortOrder>> keyInfo_;
  RowContainer* rowContainer_;
};

// Sorts input data of the Window by {partition keys, sort keys}
// to identify window partitions. This sort fully orders
// rows as needed for window function computation.
class SortWindowBuild : public WindowBuild {
 public:
  RowBasedSpillMergeStream* nextSampleFunc();
  SortWindowBuild(
      const std::shared_ptr<const core::WindowNode>& node,
      bolt::memory::MemoryPool* pool,
      const common::SpillConfig* spillConfig,
      tsan_atomic<bool>* nonReclaimableSection,
      uint64_t spillMemoryThreshold,
      bool enableJit);

  bool needsInput() override {
    // No partitions are available yet, so can consume input rows.
    return partitionStartRows_.size() == 0;
  }

  void addInput(RowVectorPtr input) override;

  void spill() override {
    return;
  };

  void noMoreInput() override;

  void finish() override;

  bool hasNextPartition() override;

  std::shared_ptr<WindowPartition> nextPartition() override;

 private:
  struct TopRows {
    struct Compare {
      TopNComparator& comparator;

      bool operator()(const char* lhs, const char* rhs) {
        return comparator(lhs, rhs);
      }
    };

    std::priority_queue<char*, std::vector<char*, StlAllocator<char*>>, Compare>
        rows;

    TopRows(HashStringAllocator* allocator, TopNComparator& comparator)
        : rows{{comparator}, StlAllocator<char*>(allocator)} {}
  };

  void initializeNewPartitions();

  void clearTopRows();

  void clearStateBeforeSortSpill() override;

  TopRows& partitionAt(char* group) {
    return *reinterpret_cast<TopRows*>(group + partitionOffset_);
  }

  // Adds input row to a partition or discards the row.
  bool processInputRow(vector_size_t index, TopRows& partition);

  void ensureInputFits(const RowVectorPtr& input);

  // Main sorting function loop done after all input rows are received
  // by WindowBuild.

  // Function to compute the partitionStartRows_ structure.
  // partitionStartRows_ is vector of the starting rows index
  // of each partition in the data. This is an auxiliary
  // structure that helps simplify the window function computations.
  void computePartitionStartRows();

  void storeRows();

  // Reads next partition from spilled data into 'data_' and 'sortedRows_'.
  void loadNextPartitionFromSpill();

  // Compare flags for partition and sorting keys. Compare flags for partition
  // keys are set to default values. Compare flags for sorting keys match
  // sorting order specified in the plan node.
  //
  // Used to sort 'data_' while spilling.

  memory::MemoryPool* const pool_;

  bool hasOutputAll() override {
    // No partitions are available or the currentPartition is the last available
    // one, so can consume input rows.
    return (
        (currentPartition_ >= partitionStartRows_.size() - 2) &&
        finishLoadFromSpill_);
  }

  uint64_t* getloadFromSpillTime() override {
    return &loadFromSpillTimeUs_;
  }

  uint64_t* getWindowSpillTime() override {
    return &windowSpillTimeUs_;
  }

  // allKeyInfo_ is a combination of (partitionKeyInfo_ and sortKeyInfo_).
  // It is used to perform a full sorting of the input rows to be able to
  // separate partitions and sort the rows in it. The rows are output in
  // this order by the operator.
  std::vector<std::pair<column_index_t, core::SortOrder>> allKeyInfo_;

  // Vector of pointers to each input row in the data_ RowContainer.
  // The rows are sorted by partitionKeys + sortKeys. This total
  // ordering can be used to split partitions (with the correct
  // order by) for the processing.
  std::vector<char*> sortedRows_;

  // This is a vector that gives the index of the start row
  // (in sortedRows_) of each partition in the RowContainer data_.
  // This auxiliary structure helps demarcate partitions.
  std::vector<vector_size_t> partitionStartRows_;

  // Current partition being output. Used to construct WindowPartitions
  // during resetPartition.
  vector_size_t currentPartition_ = -1;

  int followedTopNum_ = 0;
  // Hash table to keep track of partitions. Not used if there are no
  // partitioning keys. For each partition, stores an instance of TopRows
  // struct.
  std::unique_ptr<BaseHashTable> table_;
  std::unique_ptr<HashLookup> lookup_;
  int32_t partitionOffset_;
  TopNComparator comparator_;

  // TopRows struct to keep track of top rows for a single partition, when
  // there are no partitioning keys.
  std::unique_ptr<HashStringAllocator> allocator_;
  std::unique_ptr<TopRows> singlePartition_;

  // Used to sort-merge spilled data.
  std::unique_ptr<TreeOfLosers<SpillMergeStream>> merge_;
  std::unique_ptr<TreeOfLosers<RowBasedSpillMergeStream>> rowBasedSpillMerger_;

  uint64_t loadFromSpillTimeUs_ = 0;

  uint64_t windowSpillTimeUs_ = 0;

  int64_t lastErasePartition_ = -1;
};

} // namespace bytedance::bolt::exec
