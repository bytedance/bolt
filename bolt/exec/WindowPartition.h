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

#include "bolt/exec/RowContainer.h"
#include "bolt/exec/SpillFile.h"
#include "bolt/vector/BaseVector.h"

/// Simple WindowPartition that builds over the RowContainer used for storing
/// the input rows in the Window Operator. This works completely in-memory.
/// TODO: This implementation will be revised for Spill to disk semantics.
namespace bytedance::bolt::exec {

class WindowPartition {
 public:
  /// The WindowPartition is used by the Window operator and WindowFunction
  /// objects to access the underlying data and columns of a partition of rows.
  /// The WindowPartition is constructed by WindowBuild from the input data.
  /// 'data' : Underlying RowContainer of the WindowBuild.
  /// 'rows' : Pointers to rows in the RowContainer belonging to this partition.
  /// 'inputMapping' : Mapping from Window input column to the column position
  /// in 'data' for it. This is required because the WindowBuild re-orders
  /// the columns in 'data' for use with the spiller.
  /// 'sortKeyInfo' : Order by columns used by the the Window operator. Used to
  /// get peer rows from the input partition.
  WindowPartition(
      RowContainer* data,
      const folly::Range<char**>& rows,
      const std::vector<column_index_t>& inputMapping,
      const std::vector<std::pair<column_index_t, core::SortOrder>>&
          sortKeyInfo,
      bool enableJit = false);

  virtual ~WindowPartition() = default;

  /// Returns the number of rows in the current WindowPartition.
  virtual vector_size_t numRows() const {
    return partition_.size();
  }

  // Returns the starting offset of the current partial window partition within
  // the full partition.
  virtual vector_size_t offsetInPartition() const {
    return 0;
  }

  // Indicates support for rows streaming processing.
  virtual bool supportRowsStreaming() const {
    return false;
  }

  // Sets the flag indicating that all input rows have been processed on the
  // producer side.
  virtual void setInputRowsFinished() {
    return;
  }

  virtual void setPartialPartitionFinished(bool patialPartitionFinished) {
    return;
  }

  virtual bool getPartialPartitionFinished() {
    return false;
  }

  virtual bool getInputRowsFinished() {
    return true;
  }

  virtual char* getLastRow() {
    return nullptr;
  }

  // Adds new rows to the partition using a streaming approach on the producer
  // side.
  virtual void addNewRows(std::vector<char*> rows) {
    return;
  }

  virtual void addNewRows(
      bool patialPartitionFinished,
      std::vector<char*> rows,
      // size_t start,
      // size_t end,
      char* allocatedStart,
      unsigned long allocatedSize) {
    return;
  }

  // Builds the next set of available rows on the consumer side.
  virtual bool buildNextRows() {
    return false;
  }

  virtual void erasePartition() {
    return;
  }

  // Determines if the current partition is complete and then proceed to the
  // next partition.
  virtual bool processFinished() const {
    return true;
  }

  /// Copies the values at 'columnIndex' into 'result' (starting at
  /// 'resultOffset') for the rows at positions in the 'rowNumbers'
  /// array from the partition input data.
  virtual void extractColumn(
      int32_t columnIndex,
      folly::Range<const vector_size_t*> rowNumbers,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize = false) const;

  /// Copies the values at 'columnIndex' into 'result' (starting at
  /// 'resultOffset') for 'numRows' starting at positions 'partitionOffset'
  /// in the partition input data.
  virtual void extractColumn(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize = false) const;

  /// Extracts null positions at 'columnIndex' into 'nullsBuffer' for
  /// 'numRows' starting at positions 'partitionOffset' in the partition
  /// input data.
  void extractNulls(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      const BufferPtr& nullsBuffer) const;

  /// Extracts null positions at 'col' into 'nulls'. The null positions
  /// are from the smallest 'frameStarts' value to the greatest 'frameEnds'
  /// value for 'validRows'. Both 'frameStarts' and 'frameEnds' are buffers
  /// of type vector_size_t.
  /// The returned value is an optional pair of vector_size_t.
  /// The pair is returned only if null values are found in the nulls
  /// extracted. The first value of the pair is the smallest frameStart for
  /// nulls. The second is the number of frames extracted.
  std::optional<std::pair<vector_size_t, vector_size_t>> extractNulls(
      column_index_t col,
      const SelectivityVector& validRows,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds,
      BufferPtr* nulls) const;

  /// Sets in 'rawPeerStarts' and in 'rawPeerEnds' the peer start and peer end
  /// offsets of the rows between 'start' and 'end' of the current partition.
  /// 'peer' row are all the rows having the same value of the order by columns
  /// as the current row.
  /// computePeerBuffers is called multiple times for each partition. It is
  /// called in sequential order of start to end rows.
  /// The peerStarts/peerEnds of the startRow could be same as the last row in
  /// the previous call to computePeerBuffers (if they have the same order by
  /// keys). So peerStart and peerEnd of the last row of this call are returned
  /// to be passed as prevPeerStart and prevPeerEnd to the subsequent
  /// call to computePeerBuffers.
  virtual std::pair<vector_size_t, vector_size_t> computePeerBuffers(
      vector_size_t start,
      vector_size_t end,
      vector_size_t prevPeerStart,
      vector_size_t prevPeerEnd,
      vector_size_t* rawPeerStarts,
      vector_size_t* rawPeerEnds) const;

  /// Sets in 'rawFrameBounds' the frame boundary for the k range
  /// preceding/following frame.
  /// @param isStartBound start or end boundary of the frame.
  /// @param isPreceding preceding or following boundary.
  /// @param frameColumn column which has the range boundary for that row.
  /// @param startRow starting row in the partition for this buffer computation.
  /// @param numRows number of rows to compute buffer for.
  /// @param rawPeerStarts buffer of peer row values for each row. If the frame
  /// column is null, then its peer row value is the frame boundary.
  virtual void computeKRangeFrameBounds(
      bool isStartBound,
      bool isPreceding,
      column_index_t frameColumn,
      vector_size_t startRow,
      vector_size_t numRows,
      const vector_size_t* rawPeerStarts,
      vector_size_t* rawFrameBounds) const;

  virtual bool isSpilled() {
    return false;
  }

  virtual bool getBatch(RowVectorPtr& output, bool& isEnd) {
    return false;
  }

  virtual void getWindowFunctionResults(
      VectorPtr& result,
      int32_t numOutputRows,
      int32_t functionIndex,
      bool isAggregateWindowFunc) {
    BOLT_UNREACHABLE();
  }

  template <typename BoundTest>
  struct RangeSearchParams {
    // Determines if the range search should return the first row matching the
    // frame bound or the last row matching it. firstMatch depends on start or
    // end bound search semantics.
    bool firstMatch;

    // Number of rows incremented when searching the range value.
    // It is +1 for searching following rows, and -1 for searching preceding
    // rows.
    int32_t step;

    // Checks if the comparison result (between the order by value and the
    // frame value) satisfies the frame bound. If it returns true, that means
    // the frame bound is crossed. This is required because the order by
    // column for the range search could be ascending or descending. The
    // comparison result is considered as crossing the boundary based on this
    // directionality.
    BoundTest boundTest;
  };

 private:
  virtual bool compareRowsWithSortKeys(const char* lhs, const char* rhs) const;

  // Searches for frameColumn[startRow] in orderByColumn[startRow+-]
  // preceding or following based on the range search params.
  template <typename BoundTest>
  vector_size_t searchFrameValue(
      const RangeSearchParams<BoundTest>& params,
      vector_size_t startRow,
      column_index_t orderByColumn,
      column_index_t frameColumn) const;

  // Iterates over 'numRows' and searches frame value for each row.
  template <typename BoundTest>
  void updateKRangeFrameBounds(
      const RangeSearchParams<BoundTest>& params,
      column_index_t frameColumn,
      vector_size_t startRow,
      vector_size_t numRows,
      const vector_size_t* rawPeerBuffer,
      vector_size_t* rawFrameBounds) const;

 protected:
  // The RowContainer associated with the partition.
  // It is owned by the WindowBuild that creates the partition.
  RowContainer* data_;

  // folly::Range is for the partition rows iterator provided by the
  // Window operator. The pointers are to rows from a RowContainer owned
  // by the operator. We can assume these are valid values for the lifetime
  // of WindowPartition.
  folly::Range<char**> partition_;

  // Mapping from window input column -> index in data_. This is required
  // because the WindowBuild reorders data_ to place partition and sort keys
  // before other columns in data_. But the Window Operator and Function code
  // accesses WindowPartition using the indexes of Window input type.
  const std::vector<column_index_t> inputMapping_;

  // ORDER BY column info for this partition.
  const std::vector<std::pair<column_index_t, core::SortOrder>> sortKeyInfo_;

  // Copy of the input RowColumn objects that are used for
  // accessing the partition row columns. These RowColumn objects
  // index into RowContainer data_ above and can retrieve the column values.
  // The order of these columns is the same as that of the input row
  // of the Window operator. The WindowFunctions know the
  // corresponding indexes of their input arguments into this vector.
  // They will request for column vector values at the respective index.
  std::vector<exec::RowColumn> columns_;

  bool enableJit_{false};

#ifdef ENABLE_BOLT_JIT
  // Cached JIT module for peer row comparison in computePeerBuffers.
  // Must be a member to keep the compiled module alive across calls.
  mutable bolt::jit::CompiledModuleSP jitModuleRow_{nullptr};
  mutable RowRowCompare rowCmpRowFunc_{nullptr};
#endif
};

enum class RowFormat { kRowContainer, kSerializedRows };

template <RowFormat T = RowFormat::kRowContainer>
class WindowPartitionImpl : public WindowPartition {
 public:
  WindowPartitionImpl(
      RowContainer* data,
      const folly::Range<char**>& rows,
      const std::vector<column_index_t>& inputMapping,
      const std::vector<std::pair<column_index_t, core::SortOrder>>&
          sortKeyInfo,
      bool enableJit = false);

  /// Copies the values at 'columnIndex' into 'result' (starting at
  /// 'resultOffset') for the rows at positions in the 'rowNumbers'
  /// array from the partition input data.
  void extractColumn(
      int32_t columnIndex,
      folly::Range<const vector_size_t*> rowNumbers,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize = false) const override;

  void extractColumn(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize = false) const override;

  bool compareRowsWithSortKeys(const char* lhs, const char* rhs) const override;

  /// Sets in 'rawFrameBounds' the frame boundary for the k range
  /// preceding/following frame.
  /// @param isStartBound start or end boundary of the frame.
  /// @param isPreceding preceding or following boundary.
  /// @param frameColumn column which has the range boundary for that row.
  /// @param startRow starting row in the partition for this buffer computation.
  /// @param numRows number of rows to compute buffer for.
  /// @param rawPeerStarts buffer of peer row values for each row. If the frame
  /// column is null, then its peer row value is the frame boundary.
  void computeKRangeFrameBounds(
      bool isStartBound,
      bool isPreceding,
      column_index_t frameColumn,
      vector_size_t startRow,
      vector_size_t numRows,
      const vector_size_t* rawPeerStarts,
      vector_size_t* rawFrameBounds) const override;

  // Iterates over 'numRows' and searches frame value for each row.
  template <typename BoundTest>
  void updateKRangeFrameBounds(
      const RangeSearchParams<BoundTest>& params,
      column_index_t frameColumn,
      vector_size_t startRow,
      vector_size_t numRows,
      const vector_size_t* rawPeerBuffer,
      vector_size_t* rawFrameBounds) const;

  // Searches for frameColumn[startRow] in orderByColumn[startRow+-]
  // preceding or following based on the range search params.
  template <typename BoundTest>
  vector_size_t searchFrameValue(
      const RangeSearchParams<BoundTest>& params,
      vector_size_t startRow,
      column_index_t orderByColumn,
      column_index_t frameColumn) const;
};

template <RowFormat R = RowFormat::kRowContainer>
class SpilledWindowPartition : public WindowPartitionImpl<R> {
 public:
  SpilledWindowPartition(
      RowContainer* data,
      const folly::Range<char**>& rows,
      const std::vector<column_index_t>& inputMapping,
      const std::vector<std::pair<column_index_t, core::SortOrder>>&
          sortKeyInfo,
      std::unique_ptr<UnorderedStreamReader<BatchStream>>&& merge,
      std::vector<std::deque<VectorPtr>>&& aggregateResults,
      uint64_t numRows,
      const RowTypePtr& outputType,
      const std::vector<column_index_t>* outputChannels,
      bolt::memory::MemoryPool* pool,
      bool enableJit = false);

  bool isSpilled() override {
    return true;
  }

  bool getBatch(RowVectorPtr& output, bool& isEnd) override;

  void getWindowFunctionResults(
      VectorPtr& result,
      int32_t numOutputRows,
      int32_t functionIndex,
      bool isAggregateWindowFunc) override;

 private:
  std::unique_ptr<UnorderedStreamReader<BatchStream>> merge_;
  std::vector<std::deque<VectorPtr>> aggregateResults_;
  int32_t sourceIndex_ = 0;
  const uint64_t numRows_;
  uint64_t processedRows_ = 0;
  const RowTypePtr outputType_;
  const std::vector<column_index_t>* outputChannels_;
  bolt::memory::MemoryPool* pool_;
};

} // namespace bytedance::bolt::exec

template <>
struct fmt::formatter<bytedance::bolt::exec::RowFormat> : formatter<int> {
  auto format(bytedance::bolt::exec::RowFormat e, format_context& ctx) {
    return formatter<int>::format(static_cast<int>(e), ctx);
  }
};
