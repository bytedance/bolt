#pragma once

#include "bolt/exec/WindowBuild.h"
#include "bolt/exec/bm/BmRowContainer.h"
#include "bolt/exec/bm/BmWindowPartition.h"

#include <deque>
#include <optional>

namespace bytedance::bolt::exec::window {

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

  void noMoreInput() override;

  bool hasNextPartition() override;

  std::shared_ptr<exec::WindowPartition> nextPartition() override;

  void setIgnorePeer(bool ignorePeer) override {
    ignorePeer_ = ignorePeer;
  }

  void finish() override;

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

  FOLLY_ALWAYS_INLINE int32_t
  compareBmRowsWithKeys(
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
    return compareBmRowsWithKeys(left, right, partitionKeyInfo_);
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
      vector_size_t offset,
      vector_size_t size);

  void closeOpenPartition();

  void splitRowsIntoPartitions(
      exec::bm::SegmentId segment,
      exec::bm::RowNumber begin,
      const std::vector<char*>& rows);

  void prepareMemoryForAppend(const RowVectorPtr& input);

  void recordResidentPreviousRow(const char* row);

  void copyPreviousRowBeforeSpill();

  void clearAllResidentRowPointers();

  void markAllDescriptorsSpilled();

  bool descriptorContainsActiveSegment(
      const BmWindowPartitionDescriptor& descriptor) const;

  void spillActiveRows();

  void releasePartitionIfConsumed();

  vector_size_t activeRowsInDescriptor(
      const BmWindowPartitionDescriptor& descriptor) const;

  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
  std::unique_ptr<exec::bm::BmRowContainer> bmData_;
  std::vector<TypePtr> logicalTypes_;
  std::vector<int32_t> boundaryKeyColumns_;
  std::optional<BmWindowPartitionDescriptor> openPartition_;
  std::deque<BmWindowPartitionDescriptor> readyPartitions_;
  std::weak_ptr<exec::WindowPartition> returnedPartition_;
  vector_size_t returnedPartitionRows_{0};
  vector_size_t returnedActiveRows_{0};
  common::SpillStats spillStats_;

  PreviousRow previousRow_;
  bool ignorePeer_{false};
  bool inputFinished_{false};
  vector_size_t activeRows_{0};
};

} // namespace bytedance::bolt::exec::window
