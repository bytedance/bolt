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

#include "bolt/common/base/SpillConfig.h"
#include "bolt/common/compression/Compression.h"
#include "bolt/exec/HashBitRange.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/Spill.h"

#include "bolt/exec/HybridSorter.h"
namespace bytedance::bolt::exec {

/// Manages spilling data from a RowContainer.
class Spiller {
 public:
  // Define the spiller types.
  enum class Type : int8_t {
    // Used for aggregation input processing stage.
    kAggregateInput = 0,
    // Used for aggregation output processing stage.
    kAggregateOutput = 1,
    // Used for hash join build.
    kHashJoinBuild = 2,
    // Used for hash join probe.
    kHashJoinProbe = 3,
    // Used for order by input processing stage.
    kOrderByInput = 4,
    // Used for order by output processing stage.
    kOrderByOutput = 5,
    // Number of spiller types.
    kNumTypes = 6,
    // Used for hash probe match flags
    kHashJoinProbeMatchFlag = 7,
  };

  static std::string typeName(Type);

  using SpillRows = std::vector<char*, memory::StlAllocator<char*>>;

  enum class Mode : int8_t {
    kRowVector = 0,
    kRowContainer = 1,
  };

  /// The constructor without specifying hash bits which will only use one
  /// partition by default.

  /// type == Type::kOrderByInput || type == Type::kAggregateInput
  Spiller(
      Type type,
      RowContainer* container,
      RowTypePtr rowType,
      int32_t numSortingKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      const common::SpillConfig* spillConfig);

  /// type == Type::kAggregateOutput || type == Type::kOrderByOutput
  Spiller(
      Type type,
      RowContainer* container,
      RowTypePtr rowType,
      const common::SpillConfig* spillConfig);

  /// type == Type::kOrderByOutput for SpillableWindowBuild
  Spiller(
      Type type,
      RowContainer* container,
      RowTypePtr rowType,
      const common::SpillConfig* spillConfig,
      uint32_t maxBatchRows);

  /// type == Type::kHashJoinProbe
  Spiller(
      Type type,
      RowTypePtr rowType,
      HashBitRange bits,
      const common::SpillConfig* spillConfig,
      uint64_t targetFileSize);

  /// type == Type::kHashJoinBuild
  Spiller(
      Type type,
      RowContainer* container,
      RowTypePtr rowType,
      HashBitRange bits,
      const common::SpillConfig* spillConfig,
      uint64_t targetFileSize,
      bool supportSkewPartition);

  /// type == Type::kHashJoinProbeMatchFlag
  Spiller(
      Type type,
      RowTypePtr rowType,
      const common::SpillConfig* spillConfig);

  Type type() const {
    return type_;
  }

  /// Spills all the rows from 'this' to disk. The spilled rows stays in the
  /// row container. The caller needs to erase the spilled rows from the row
  /// container.
  void spill();

  /// Spill all rows starting from 'startRowIter'. This is only used by
  /// 'kAggregateOutput' spiller type to spill during the aggregation output
  /// processing. Similarly, the spilled rows still stays in the row container.
  /// The caller needs to erase them from the row container.
  void spill(const RowContainerIterator& startRowIter);

  /// Invoked to spill all the rows pointed by rows. This is used by
  /// 'kOrderByOutput' spiller type to spill during the order by
  /// output processing. Similarly, the spilled rows still stays in the row
  /// container. The caller needs to erase them from the row container.
  void spill(std::vector<char*>& rows, bool lastRun = true);

  /// Append 'spillVector' into the spill file of given 'partition'. It is now
  /// only used by the spilling operator which doesn't need data sort, such as
  /// hash join build and hash join probe.
  ///
  /// NOTE: the spilling operator should first mark 'partition' as spilling and
  /// spill any data buffered in row container before call this.
  void spill(uint32_t partition, const RowVectorPtr& spillVector);

  Spiller& setSpillConfig(const common::SpillConfig* cfg);

  // set rowInfo to show the format is SerializedRow or RowContainer
  // It decides whether the data should be serialized or not before spill
  void setRowFormatInfo(bool isSerialized);

  /// Extracts up to 'maxRows' or 'maxBytes' from 'rows' into 'spillVector'. The
  /// extract starts at nextBatchIndex and updates nextBatchIndex to be the
  /// index of the first non-extracted element of 'rows'. Returns the byte size
  /// of the extracted rows.
  int64_t extractSpillVector(
      SpillRows& rows,
      int32_t maxRows,
      int64_t maxBytes,
      RowVectorPtr& spillVector,
      size_t& nextBatchIndex);

  /// Finishes spilling and accumulate the spilled partition metadata in
  /// 'partitionSet' indexed by spill partition id.
  void finishSpill(SpillPartitionSet& partitionSet);

  /// Finishes spilling and expects single partition.
  SpillPartition finishSpill();

  void doSkewPartitionSplit(
      SpillPartitionSet& partitionSet,
      uint8_t bitOffset,
      int32_t partitionNumberMax,
      int32_t estimatePartitions);

  void trySplitSkewPartition(SpillPartitionSet& partitionSet);

  void prepareForRangPartitionIfNeeded();

  const SpillState& state() const {
    return state_;
  }

  const HashBitRange& hashBits() const {
    return bits_;
  }

  bool isSpilled(int32_t partition) const {
    return state_.isPartitionSpilled(partition);
  }

  /// Indicates if all the partitions have spilled.
  bool isAllSpilled() const {
    return state_.isAllPartitionSpilled();
  }

  /// Indicates if any one of the partitions has spilled.
  bool isAnySpilled() const {
    return state_.isAnyPartitionSpilled();
  }

  /// Returns the spilled partition number set.
  SpillPartitionNumSet spilledPartitionSet() const {
    return state_.spilledPartitionSet();
  }

  /// Invokes to set a set of 'partitions' as spilling.
  void setPartitionsSpilled(const SpillPartitionNumSet& partitions) {
    BOLT_CHECK(
        type_ == Spiller::Type::kHashJoinProbe ||
            type_ == Spiller::Type::kHashJoinProbeMatchFlag,
        "Unexpected spiller type: ",
        typeName(type_));
    for (const auto& partition : partitions) {
      state_.setPartitionSpilled(partition);
    }
  }

  /// Indicates if this spiller has finalized or not.
  bool finalized() const {
    return finalized_;
  }

  // return total num of rows which has been spilled
  uint64_t maxPartitionRowCount() {
    return state_.maxPartitionRowCount();
  }

  uint64_t sumPartitionRowCount() {
    return state_.sumPartitionRowCount();
  }

  common::SpillStats stats() const;

  std::string toString() const;

  void setSortAlgo(SortAlgo algo) {
    sorter_ = HybridSorter{algo};
  }

  const RowContainer* container() const {
    return container_;
  }

  bool rangePartitioningApplicable() const {
    return rangePartitioningApplicable_;
  }

  void setMemoryUsedTriggered(int64_t used) {
    memoryUsedWhenTriggered_ = used;
  }

  void setSkewThreshold(int32_t fileSizeThreshold, int32_t rowCountThreshold) {
    skewFileSizeRatioThreshold_ = fileSizeThreshold;
    skewRowCountRatioThreshold_ = rowCountThreshold;
  }

  void setMaxRowsInMemory(uint64_t rowsInMemory) {
    maxRowsInMemory_ = rowsInMemory;
  }

  bool filledZeroRows() const {
    return (totalFillRows_.has_value() && *totalFillRows_ == 0);
  }

 private:
  Spiller(
      Type type,
      RowContainer* container,
      RowTypePtr rowType,
      HashBitRange bits,
      int32_t numSortingKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      const common::SpillConfig::SpillIOConfig& ioConfig,
      uint64_t targetFileSize,
      folly::Executor* executor,
      uint64_t maxSpillRunRows,
      common::RowBasedSpillMode rowBasedSpillMode);

  // Invoked to spill. If 'startRowIter' is not null, then we only spill rows
  // from row container starting at the offset pointed by 'startRowIter'.
  void spill(const RowContainerIterator* startRowIter);

  // Extracts the keys, dependents or accumulators for 'rows' into '*result'.
  // Creates '*results' in spillPool() if nullptr. Used from Spiller and
  // RowContainerSpillMergeStream.
  void extractSpill(folly::Range<char**> rows, RowVectorPtr& result);

  // Returns a mergeable stream that goes over unspilled in-memory
  // rows for the spill partition  'partition'. finishSpill()
  // first and 'partition' must specify a partition that has started spilling.
  std::unique_ptr<SpillMergeStream> spillMergeStreamOverRows(int32_t partition);

  // Invoked to finalize the spiller and flush any buffered spill to disk.
  void finalizeSpill();

  Mode spillMode(Type type, bool rowBasedSpill) {
    return (container_->accumulators().empty() && rowBasedSpill)
        ? Mode::kRowContainer
        : Mode::kRowVector;
  }

  // Represents a run of rows from a spillable partition of
  // a RowContainer. Rows that hash to the same partition are accumulated here
  // and sorted in the case of sorted spilling. The run is then
  // spilled into storage as multiple batches. The rows are deleted
  // from this and the RowContainer as they are written. When 'rows'
  // goes empty this is refilled from the RowContainer for the next
  // spill run from the same partition.
  struct SpillRun {
    explicit SpillRun(memory::MemoryPool& pool)
        : rows(0, memory::StlAllocator<char*>(pool)) {}
    // Spillable rows from the RowContainer.
    SpillRows rows;
    // The total byte size of rows referenced from 'rows'.
    uint64_t numBytes{0};
    // True if 'rows' are sorted on their key.
    bool sorted{false};

    void clear() {
      rows.clear();
      // Clears the memory allocated in rows after a spill run finishes.
      rows.shrink_to_fit();
      numBytes = 0;
      sorted = false;
    }

    std::string toString() const {
      return fmt::format(
          "[{} ROWS {} BYTES {}]",
          rows.size(),
          numBytes,
          sorted ? "SORTED" : "UNSORTED");
    }
  };

  struct SpillStatus {
    const int32_t partition;
    const int32_t rowsWritten;
    const std::exception_ptr error;

    SpillStatus(
        int32_t _partition,
        int32_t _numWritten,
        std::exception_ptr _error)
        : partition(_partition), rowsWritten(_numWritten), error(_error) {}
  };

  void checkEmptySpillRuns() const;

  // Marks all the partitions have been spilled as we don't support
  // fine-grained spilling as for now.
  void markAllPartitionsSpilled();

  // Prepares spill runs for the spillable data from all the hash partitions.
  // If 'startRowIter' is not null, we prepare runs starting from the offset
  // pointed by 'startRowIter'.
  // The function returns true if it is the last spill run.
  bool fillSpillRuns(RowContainerIterator* startRowIter = nullptr);

  // Prepares spill run of a single partition for the spillable data from the
  // rows.
  void fillSpillRun(std::vector<char*>& rows);

  // Writes out all the rows collected in spillRuns_.
  void runSpill(bool lastRun);

  // Sorts 'run' if not already sorted.
  void ensureSorted(SpillRun& run);

  // set nextEqual if rows[i] == rows[i+1], return whether equal rows to
  // feedback whether bypass hash table or not
  size_t setNextEqualForAgg(SpillRun& run);

  // Function for writing a spill partition on an executor. Writes to
  // 'partition' until all rows in spillRuns_[partition] are written
  // or spill file size limit is exceededg. Returns the number of rows
  // written.
  std::unique_ptr<SpillStatus> writeSpill(int32_t partition);

  // Indicates if the spill data needs to be sorted before write to file. It is
  // based on the spiller type. As for now, we need to sort spill data for any
  // non hash join types of spilling.
  bool needSort() const;

  void updateSpillFillTime(uint64_t timeUs);

  void updateSpillSortTime(uint64_t timeUs);

  void updateSpillConvertTime(uint64_t timeUs);

  void updateSpillTotalTime(uint64_t timeUs);

  const Type type_;
  // NOTE: for hash join probe type, there is no associated row container for
  // the spiller.
  RowContainer* const container_{nullptr};
  folly::Executor* const executor_;
  const HashBitRange bits_;
  const RowTypePtr rowType_;
  const uint64_t maxSpillRunRows_;

  // True if all rows of spilling partitions are in 'spillRuns_', so
  // that one can start reading these back. This means that the rows
  // that are not written out and deleted will be captured by
  // spillMergeStreamOverRows().
  bool finalized_{false};

  folly::Synchronized<common::SpillStats> stats_;
  SpillState state_;

  Mode spillMode_{Mode::kRowVector};
  std::optional<RowFormatInfo> rowInfo_{std::nullopt};

  // Collects the rows to spill for each partition.
  std::vector<SpillRun> spillRuns_;

  common::SpillConfig* spillConfig_{nullptr};

  HybridSorter sorter_;

  bool supportSkewPartition_{false};
  bool rangePartitioningApplicable_{false};
  int32_t skewedVictim_{-1};
  uint32_t maxRowsPerFile_{0};
  int64_t memoryUsedWhenTriggered_{0};
  int32_t skewFileSizeRatioThreshold_ = 10;
  int32_t skewRowCountRatioThreshold_ = 100;
  static constexpr uint64_t kDefaultSpillRunRows = 12UL << 20;
  static constexpr int32_t minRangePartitionNumber = 6;
  uint64_t maxRowsInMemory_{0};
  std::optional<uint64_t> totalFillRows_;

#ifdef ENABLE_BOLT_JIT
  bytedance::bolt::jit::CompiledModuleSP jitModule_;
#endif
  RowRowCompare cmp_{nullptr};
};
} // namespace bytedance::bolt::exec

template <>
struct fmt::formatter<bytedance::bolt::exec::Spiller::Type> : formatter<int> {
  auto format(bytedance::bolt::exec::Spiller::Type s, format_context& ctx) {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
