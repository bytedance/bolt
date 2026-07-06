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

#include "bolt/common/base/AggregationStats.h"
#include "bolt/exec/AggregateInfo.h"
#include "bolt/exec/AggregationMasks.h"
#include "bolt/exec/DistinctAggregations.h"
#include "bolt/exec/HashTable.h"
#include "bolt/exec/SortedAggregations.h"
#include "bolt/exec/Spiller.h"
#include "bolt/exec/TreeOfLosers.h"
#include "bolt/exec/VectorHasher.h"
namespace bytedance::bolt::exec {

class GroupingSet {
 public:
  GroupingSet(
      const RowTypePtr& inputType,
      std::vector<std::unique_ptr<VectorHasher>>&& hashers,
      std::vector<column_index_t>&& preGroupedKeys,
      std::vector<AggregateInfo>&& aggregates,
      bool ignoreNullKeys,
      bool isPartial,
      bool isRawInput,
      const std::vector<vector_size_t>& globalGroupingSets,
      const std::optional<column_index_t>& groupIdChannel,
      const common::SpillConfig* spillConfig,
      tsan_atomic<bool>* nonReclaimableSection,
      OperatorCtx* operatorCtx);

  ~GroupingSet();

  // Used by MarkDistinct operator to identify rows with unique values.
  static std::unique_ptr<GroupingSet> createForMarkDistinct(
      const RowTypePtr& inputType,
      std::vector<std::unique_ptr<VectorHasher>>&& hashers,
      OperatorCtx* operatorCtx,
      tsan_atomic<bool>* nonReclaimableSection);

  void addInput(const RowVectorPtr& input, bool mayPushdown);

  void noMoreInput();

  void putDistinctNewGroupsIndices(int64_t offset) {
    if (lookup_ && !hasSpilled()) {
      lookup_->distinctNewGroups.reserve(
          lookup_->distinctNewGroups.size() + lookup_->newGroups.size());
      for (auto index : lookup_->newGroups) {
        lookup_->distinctNewGroups.emplace_back(index + offset);
      }
      lookup_->newGroups.clear();
    }
  }

  /// Typically, the output is not available until all input has been added.
  /// However, in case when input is clustered on some of the grouping keys, the
  /// output becomes available every time one of these grouping keys changes
  /// value. This method returns true if no-more-input message has been received
  /// or if some groups are ready for output because pre-grouped keys values
  /// have changed.
  bool hasOutput();

  /// Called if partial aggregation has reached memory limit or if hasOutput()
  /// returns true. 'maxOutputRows' and 'maxOutputBytes' specify the max number
  /// of rows/bytes to return in 'result' respectively. The function stops
  /// producing output if it exceeds either limit.
  bool getOutput(
      int32_t maxOutputRows,
      int32_t maxOutputBytes,
      RowContainerIterator& iterator,
      RowVectorPtr& result);

  uint64_t allocatedBytes() const;

  /// Resets the hash table inside the grouping set when partial aggregation
  /// is full or reclaims memory from distinct aggregation after it has received
  /// all the inputs.
  void resetTable();

  void resetDistinctNewGroups();

  /// Returns true if 'this' should start producing partial
  /// aggregation results. Checks the memory consumption against
  /// 'maxBytes'. If exceeding 'maxBytes', sees if changing hash mode
  /// can free up space and rehashes and returns false if significant
  /// space was recovered. In specific, changing from an array hash
  /// based on value ranges to one based on value ids can save a lot.
  bool isPartialFull(int64_t maxBytes);

  /// Returns the count of the hash table, if any.
  int64_t numDistinct() const {
    return table_ ? table_->numDistinct() : 0;
  }

  /// Returns number of global grouping sets rows if there is default output.
  std::optional<vector_size_t> numDefaultGlobalGroupingSetRows() const {
    if (hasDefaultGlobalGroupingSetOutput()) {
      return globalGroupingSets_.size();
    }
    return std::nullopt;
  }

  const HashLookup& hashLookup() const;

  /// Spills all the rows in container.
  void spill();

  /// Spills all the rows in container starting from the offset specified by
  /// 'rowIterator'.
  void spill(const RowContainerIterator& rowIterator);

  /// Returns the spiller stats including total bytes and rows spilled so far.
  std::optional<common::SpillStats> spilledStats() const {
    if (spiller_ == nullptr) {
      return std::nullopt;
    }
    return spiller_->stats();
  }

  /// Returns the spill read stats, currently only spillReadTime supported.
  std::optional<common::SpillReadStats> spillReadStats() const {
    common::SpillReadStats spillReadStats;
    if (merge_) {
      spillReadStats.spillReadTimeUs = merge_->getSpillReadTime();
      spillReadStats.spillDecompressTimeUs = merge_->getSpillDecompressTime();
      spillReadStats.spillReadIOTimeUs = merge_->getSpillReadIOTime();
    } else if (rowBasedSpillMerge_) {
      spillReadStats.spillReadTimeUs = rowBasedSpillMerge_->getSpillReadTime();
      spillReadStats.spillDecompressTimeUs =
          rowBasedSpillMerge_->getSpillDecompressTime();
      spillReadStats.spillReadIOTimeUs =
          rowBasedSpillMerge_->getSpillReadIOTime();
    } else {
      return std::nullopt;
    }
    return spillReadStats;
  }

  /// Returns true if spilling has triggered on this grouping set.
  bool hasSpilled() const;

  /// Returns the hashtable stats.
  HashTableStats hashTableStats() const {
    return table_ ? table_->stats() : HashTableStats{};
  }

  /// Return the number of rows kept in memory.
  int64_t numRows() const {
    return table_ ? table_->rows()->numRows() : 0;
  }

  // Frees hash tables and other state when giving up partial aggregation as
  // non-productive. Must be called before toIntermediate() is used.
  void abandonPartialAggregation();

  /// Translates the raw input in input to accumulators initialized from a
  /// single input row. Passes grouping keys through.
  void toIntermediate(const RowVectorPtr& input, RowVectorPtr& result);

  /// Returns default global grouping sets output if there are no input rows.
  /// The default global grouping set output is a single row per global grouping
  /// set with the groupId key and the default aggregate value.
  /// This function can also be used with distinct aggregations.
  bool getDefaultGlobalGroupingSetOutput(
      RowContainerIterator& iterator,
      RowVectorPtr& result);

  memory::MemoryPool& testingPool() const {
    return pool_;
  }

  std::optional<int64_t> estimateOutputRowSize() const;

  /// Returns the number of rows spilled
  uint64_t totalSpillRowCount() {
    return spillSumRowCount_;
  }

  /// Returns the number of used rows in spilled files
  uint64_t outputSpillRowCount() {
    return spillOutputRowCount_;
  }

  common::AggregationStats getRuntimeStats() {
    return stats_;
  }

  void setPreferPartialSpill(bool preferPartialSpill) {
    preferPartialSpill_ = preferPartialSpill;
  }

  void setSupportRowBasedOutput(bool supportRowBasedOutput) {
    supportRowBasedOutput_ = supportRowBasedOutput;
  }

  void setSupportUniqueRowOptimization(bool uniqueRowOpt) {
    supportUniqueRowOpt_ = uniqueRowOpt;
  }

  void populateOutputValidColumns(SelectivityVector* selectedColumns);

  void convertCompositeInput(
      const std::vector<AggregateInfo>& extractAggregates,
      const CompositeRowVectorPtr& input,
      const RowVectorPtr& result);

  void convertRowsFromContainerRows(
      const CompositeRowVectorPtr& compositeResult,
      folly::Range<char**> groups,
      const std::vector<std::pair<int32_t, int32_t>>& resultRanges);

  void convertRowsFromSpilledRows(
      const CompositeRowVectorPtr& compositeResult,
      std::vector<char*>& rows,
      int32_t resultOffset,
      const std::vector<int32_t>& rowSizeVec,
      const int64_t totalUniqueRowSize);

 private:
  using RowSizeType = int32_t;

  bool isDistinct() const {
    return aggregates_.empty();
  }

  void addInputForActiveRows(const RowVectorPtr& input, bool mayPushdown);

  void addRemainingInput();

  void initializeGlobalAggregation();

  void destroyGlobalAggregations();

  void addGlobalAggregationInput(const RowVectorPtr& input, bool mayPushdown);

  bool getGlobalAggregationOutput(
      RowContainerIterator& iterator,
      RowVectorPtr& result);

  // If there are global grouping sets, then returns if they have default
  // output in case no input rows were received.
  bool hasDefaultGlobalGroupingSetOutput() const {
    return noMoreInput_ && numInputRows_ == 0 && !globalGroupingSets_.empty() &&
        isRawInput_;
  }

  void createHashTable();

  void populateTempVectors(int32_t aggregateIndex, const RowVectorPtr& input);

  // If the given aggregation has mask, the method returns reference to the
  // selectivity vector from the maskedActiveRows_ (based on the mask channel
  // index for this aggregation), otherwise it returns reference to activeRows_.
  const SelectivityVector& getSelectivityVector(size_t aggregateIndex) const;

  // Checks if input will fit in the existing memory and increases reservation
  // if not. If reservation cannot be increased, spills enough to make 'input'
  // fit.
  void ensureInputFits(const RowVectorPtr& input);

  // Reserves memory for output processing. If reservation cannot be increased,
  // spills enough to make output fit.
  void ensureOutputFits();

  // Copies the grouping keys and aggregates for 'groups' into 'result' If
  // partial output, extracts the intermediate type for aggregates, final result
  // otherwise.
  void extractGroups(
      folly::Range<char**> groups,
      const RowVectorPtr& result,
      bool excludeKey = false);

  // from row in RowContainer to serialized row by ContainerRow2RowSerde
  void extractGroupsInRowFormat(
      folly::Range<char**> groups,
      RowVectorPtr& result);

  void extractSpilledGroupsInRowFormat(
      folly::Range<char**> groups,
      const RowVectorPtr& result,
      const std::vector<std::pair<int32_t, int32_t>>& resultRanges);

  // Produces output in if spilling has occurred. First produces data
  // from non-spilled partitions, then merges spill runs and unspilled data
  // form spilled partitions. Returns nullptr when at end. 'maxOutputRows' and
  // 'maxOutputBytes' specifies the max number of output rows and bytes in
  // 'result'.
  bool getOutputWithSpill(
      int32_t maxOutputRows,
      int32_t maxOutputBytes,
      const RowVectorPtr& result);

  // Reads from spilled rows until producing a batch of final results in
  // 'result'. Returns false and leaves 'result' empty when the spilled data is
  // fully read. 'maxOutputRows' and 'maxOutputBytes' specify the max number of
  // output rows and bytes in 'result'.
  bool mergeNext(
      int32_t maxOutputRows,
      int32_t maxOutputBytes,
      const RowVectorPtr& result);

  // Reads from spilled rows for group by with aggregates.
  bool mergeNextWithAggregates(
      int32_t maxOutputRows,
      int32_t maxOutputBytes,
      const RowVectorPtr& result);

  // Reads from spilled rows for group by without aggregates.
  bool mergeNextWithoutAggregates(
      int32_t maxOutputRows,
      const RowVectorPtr& result);

  bool mergeNextWithRowBasedSpill(
      int32_t maxOutputRows,
      int32_t maxOutputBytes,
      const RowVectorPtr& result);

  bool mergeNextWithRowBasedSpillInRowFormat(
      int32_t maxOutputRows,
      int32_t maxOutputBytes,
      const RowVectorPtr& result);

  void copyKeyAndInitGroup(
      std::vector<char*>& distinctRows,
      std::vector<char*>& groups,
      size_t& initGroupCount,
      const RowContainer* container,
      const RowVectorPtr& result,
      const vector_size_t resultOffset);

  void copyKeyAndUpdateGroups(
      std::vector<char*>& rows,
      std::vector<char*>& groupOfRows,
      std::vector<char*>& distinctRows,
      std::vector<char*>& groups,
      size_t& initGroupCount,
      const RowContainer* container,
      const RowVectorPtr& result,
      const vector_size_t resultOffset);

  void outputUniqueGroups(
      std::vector<char*>& uniqueRows,
      const RowVectorPtr& uniqueRes,
      size_t& uniqueCount,
      size_t& estimateUniqueBytesPerRow);

  void outputUniqueGroupsInRowFormat(
      std::vector<char*>& uniqueRows,
      const RowVectorPtr& result,
      const int32_t resultOffset,
      size_t& estimateUniqueBytesPerRow,
      const std::vector<int32_t>& rowSizeVec,
      const uint64_t totalUniqueRowSize);

  // ensure groupIndicesOfRows_ contains 0, 1, 2 ... size
  void ensureGroupIndices(vector_size_t size);

  // Initializes rows in 'rows'. Accumulators are left in the initial
  // state with no data accumulated. This is called when enough rows is
  // collected from a merge of spilled data. After this updateRows() can be
  // called on each initialized row. When enough rows have been accumulated and
  // we have a new key, we produce the output and clear 'mergeRows_' with
  // extractSpillResult().
  void initializeRows(std::vector<char*> rows);

  // Updates the accumulators in 'rows' with the intermediate type data from
  // 'input', each input data stores it's row in rows. This is called when we
  // receive enough intermediate from a merge of spilled data.
  void updateRows(const RowVectorPtr& input, std::vector<char*>& rows);

  // Returns a RowType of the spilled data.
  RowTypePtr makeSpillType() const;

  // Copies the finalized state from 'mergeRows' to 'result' and clears
  // 'mergeRows'. Used for producing a batch of results when aggregating spilled
  // groups.
  void extractSpillResult(const RowVectorPtr& result, bool excludeKey = false);

  void extractSpillResultInRowFormat(
      const RowVectorPtr& result,
      const std::vector<std::pair<int32_t, int32_t>>& resultRanges);

  // Return a list of accumulators for 'aggregates_', plus one more accumulator
  // for 'sortedAggregations_', and one for each 'distinctAggregations_'.  When
  // 'excludeToIntermediate' is true, skip the functions that support
  // 'toIntermediate'.
  std::vector<Accumulator> accumulators(bool excludeToIntermediate);

  // Calculates the number of groups to extract from 'rowsWhileReadingSpill_'
  // container with rows starting at 'nonSpilledIndex_' in 'nonSpilledRows_'.
  // 'maxOutputRows' and 'maxOutputBytes' specifies the max number of groups and
  // bytes to extract.
  size_t numNonSpilledGroupsToExtract(
      int32_t maxOutputRows,
      int32_t maxOutputBytes) const;

  void adjustBypassHashTable(common::SpillConfig* spillConf);

  std::vector<column_index_t> keyChannels_;

  /// A subset of grouping keys on which the input is clustered.
  const std::vector<column_index_t> preGroupedKeyChannels_;

  std::vector<std::unique_ptr<VectorHasher>> hashers_;
  const bool isGlobal_;
  const bool isPartial_;
  const bool isRawInput_;

  const core::QueryConfig& queryConfig_;

  std::vector<AggregateInfo> aggregates_;
  AggregationMasks masks_;
  std::unique_ptr<SortedAggregations> sortedAggregations_;
  std::vector<std::unique_ptr<DistinctAggregations>> distinctAggregations_;

  // True if any aggregate accumulator allocates memory outside RowContainer's
  // HashStringAllocator (e.g. directly from MemoryPool). In that case,
  // RowContainer::estimateRowSize() can under-estimate the actual per-group
  // footprint.
  bool hasExternalMemoryAccumulators_{false};

  const bool ignoreNullKeys_;

  uint64_t numInputRows_ = 0;

  // The maximum memory usage that a final aggregation can hold before spilling.
  // If it is zero, then there is no such limit.
  const uint64_t spillMemoryThreshold_;

  // List of global grouping set numbers, if being used with a GROUPING SET.
  const std::vector<vector_size_t> globalGroupingSets_;
  // Column for groupId for a GROUPING SET.
  std::optional<column_index_t> groupIdChannel_;

  const common::SpillConfig* spillConfig_;

  // Indicates if this grouping set and the associated hash aggregation operator
  // is under non-reclaimable execution section or not.
  tsan_atomic<bool>* const nonReclaimableSection_;

  // Boolean indicating whether accumulators for a global aggregation (i.e.
  // aggregation with no grouping keys) have been initialized.
  bool globalAggregationInitialized_{false};

  std::vector<bool> mayPushdown_;

  // Place for the arguments of the aggregate being updated.
  std::vector<VectorPtr> tempVectors_;
  std::unique_ptr<BaseHashTable> table_;
  std::unique_ptr<HashLookup> lookup_;
  SelectivityVector activeRows_;

  // Used to allocate memory for a single row accumulating results of global
  // aggregation
  HashStringAllocator stringAllocator_;
  memory::AllocationPool rows_;
  const bool isAdaptive_;

  bool noMoreInput_{false};

  // In case of partial streaming aggregation, the input vector passed to
  // addInput(). A set of rows that belong to the last group of pre-grouped
  // keys need to be processed after flushing the hash table and accumulators.
  RowVectorPtr remainingInput_;

  // First row in remainingInput_ that needs to be processed.
  vector_size_t firstRemainingRow_;

  // The value of mayPushdown flag specified in addInput() for the
  // 'remainingInput_'.
  bool remainingMayPushdown_;

  std::unique_ptr<Spiller> spiller_;
  // Sets to the number of files stores the spilled distinct hash table which
  // are the files generated by the first spill call. This only applies for
  // distinct hash aggregation.
  size_t numDistinctSpilledFiles_{0};
  std::unique_ptr<TreeOfLosers<SpillMergeStream>> merge_;
  std::unique_ptr<TreeOfLosers<RowBasedSpillMergeStream>> rowBasedSpillMerge_;
  // Indicates the group indices passed to initializeNewGroups in spill merge
  // stage, element should be 0, 1, 2 ... groupIndicesOfRows_.size()
  std::vector<vector_size_t> groupIndicesOfRows_;

  // Container for materializing batches of output from spilling.
  std::unique_ptr<RowContainer> mergeRows_;

  // The row with the current merge state, allocated from 'mergeRow_'.
  char* mergeState_ = nullptr;

  // Intermediate vector for passing arguments to aggregate in merging spill.
  std::vector<VectorPtr> mergeArgs_;

  // Indicates the element in mergeArgs_[0] that corresponds to the accumulator
  // to merge.
  SelectivityVector mergeSelection_;

  // Pool of the OperatorCtx. Used for spilling.
  memory::MemoryPool& pool_;

  OperatorCtx* operatorCtx_;

  // Counts input batches and triggers spilling if folly hash of this % 100 <=
  // 'spillConfig_->testSpillPct'.
  uint64_t spillTestCounter_{0};

  // True if partial aggregation has been given up as non-productive.
  bool abandonedPartialAggregation_{false};

  // True if partial aggregation and all aggregates have a fast path from raw
  // input to intermediate. Initialized in abandonPartialAggregation().
  bool allSupportToIntermediate_;

  // RowContainer for toIntermediate for aggregates that do not have a
  // toIntermediate() fast path
  std::unique_ptr<RowContainer> intermediateRows_;
  std::vector<char*> intermediateGroups_;
  std::vector<vector_size_t> intermediateRowNumbers_;
  // Temporary for case where an aggregate in toIntermediate() outputs post-init
  // state of aggregate for all rows.
  std::vector<char*> firstGroup_;

  // Total row count of spilled files
  uint64_t spillSumRowCount_{0};
  // Total row count of output rows in spilled files
  uint64_t spillOutputRowCount_{0};
  common::AggregationStats stats_;

  bool preferPartialSpill_{false};
  bool bypassProbeHT_{false};
  double bypassHTDistinctRatio_{0.8};

  // for CompositeRowVector
  bool supportRowBasedOutput_{false};
  bool supportUniqueRowOpt_{false};
  std::optional<RowFormatInfo> rowInfo_{std::nullopt};
  bool extractAggregatesInitialized_{false};
};

} // namespace bytedance::bolt::exec
