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

#include "bolt/exec/WindowBuild.h"
#include "bolt/exec/bm/BmRowContainer.h"
#include "bolt/exec/bm/BmWindowPartition.h"

#include <deque>
#include <optional>

namespace bytedance::bolt::exec::window {

class BmEntirePartitionAggregation;

class BmStreamingWindowBuild : public exec::WindowBuild {
 public:
  BmStreamingWindowBuild(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      memory::MemoryPool* pool,
      const common::SpillConfig* spillConfig,
      tsan_atomic<bool>* nonReclaimableSection,
      uint64_t spillMemoryThreshold,
      bool enableJit,
      std::shared_ptr<memory::bm::BufferManager> bufferManager);

  bool needsInput() override;

  bool hasOutputAll() override;

  void addInput(RowVectorPtr input) override;

  void spill() override;

  std::optional<common::SpillStats> windowSpilledStats() const override;

  BmWindowAccessStats windowAccessStats() const {
    return *windowAccessStats_;
  }

  void noMoreInput() override;

  bool hasNextPartition() override;

  std::shared_ptr<exec::WindowPartition> nextPartition() override;

  void setIgnorePeer(bool ignorePeer) override {
    ignorePeer_ = ignorePeer;
  }

  void finish() override;

  void setInputAggregation(
      std::shared_ptr<BmEntirePartitionAggregation> aggregation);

 private:
  enum class AdjacentRelation {
    kSamePeer,
    kNewPeer,
    kNewPartition,
  };

  struct PreviousRow {
    const char* row{nullptr};
    std::vector<char> ownedRow;
    std::vector<char> ownedVariableData;
  };

  RowVectorPtr makeReorderedInput(const RowVectorPtr& input) const;

  FOLLY_ALWAYS_INLINE int32_t compareBmRowsWithKeys(
      const char* left,
      const char* right,
      const std::vector<std::pair<column_index_t, core::SortOrder>>& keys)
      const {
    for (const auto& key : keys) {
      const auto result = bmData_->compare(
          left,
          right,
          key.first,
          {.nullsFirst = key.second.isNullsFirst(),
           .ascending = key.second.isAscending(),
           .equalsOnly = false});
      if (result != 0) {
        return result;
      }
    }
    return 0;
  }

  FOLLY_ALWAYS_INLINE int32_t
  comparePartitionRows(const char* left, const char* right) const {
    return bmData_->equalKeys(
               left,
               right,
               {partitionKeyColumns_.data(), partitionKeyColumns_.size()})
        ? 0
        : 1;
  }

  FOLLY_ALWAYS_INLINE int32_t
  compareSortRows(const char* left, const char* right) const {
    return compareBmRowsWithKeys(left, right, sortKeyInfo_);
  }

  FOLLY_ALWAYS_INLINE bool collectPeerBoundaries() const {
    return !ignorePeer_ && !sortKeyInfo_.empty();
  }

  AdjacentRelation classifyAdjacentRows(const char* left, const char* right)
      const;

  void markPeerStart(vector_size_t rowOffset);

  void appendRowsToOpenPartition(
      exec::bm::SegmentId segment,
      exec::bm::RowNumber begin,
      const std::vector<char*>& rows,
      const RowVectorPtr& input,
      vector_size_t offset,
      vector_size_t size);

  void closeOpenPartition();

  void splitRowsIntoPartitions(
      exec::bm::SegmentId segment,
      exec::bm::RowNumber begin,
      const std::vector<char*>& rows,
      const RowVectorPtr& input);

  void prepareMemoryForAppend(const RowVectorPtr& input);

  void recordResidentPreviousRow(const char* row);

  void copyPreviousRowBeforeSpill();

  void clearAllResidentRowPointers();

  void markAllDescriptorsSpilled();

  bool descriptorContainsActiveSegment(
      const BmWindowPartitionDescriptor& descriptor) const;

  void spillActiveRows();

  void flushReleasedRows(bool force);

  void releasePartitionIfConsumed(bool force = false);

  vector_size_t activeRowsInDescriptor(
      const BmWindowPartitionDescriptor& descriptor) const;

  // Require cached results for every window function before releasing input
  // rows.
  const size_t numWindowFunctions_;
  std::shared_ptr<BmEntirePartitionAggregation> inputAggregation_;
  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
  std::unique_ptr<exec::bm::BmRowContainer> bmData_;
  std::shared_ptr<const BmWindowPartitionSchema> partitionSchema_;
  std::shared_ptr<BmWindowAccessStats> windowAccessStats_{
      std::make_shared<BmWindowAccessStats>()};
  std::vector<int32_t> boundaryKeyColumns_;
  std::vector<column_index_t> partitionKeyColumns_;
  std::optional<BmWindowPartitionDescriptor> openPartition_;
  std::deque<BmWindowPartitionDescriptor> readyPartitions_;
  std::weak_ptr<exec::WindowPartition> returnedPartition_;
  vector_size_t returnedPartitionRows_{0};
  vector_size_t returnedActiveRows_{0};
  vector_size_t releasedRowsPendingPop_{0};
  common::SpillStats spillStats_;

  PreviousRow previousRow_;
  bool ignorePeer_{false};
  bool inputFinished_{false};
  vector_size_t activeRows_{0};
};

} // namespace bytedance::bolt::exec::window
