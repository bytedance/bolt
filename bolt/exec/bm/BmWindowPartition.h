#pragma once

#include "bolt/exec/WindowPartition.h"
#include "bolt/exec/bm/BmRowContainer.h"

#include <optional>

namespace bytedance::bolt::exec::window {

class BmRangeFrameBounds;

enum class BmPeerBoundaryMode {
  kNotNeeded,
  kPrecomputed,
};

struct BmWindowPartitionDescriptor {
  vector_size_t numRows{0};
  std::vector<exec::bm::SegmentRowRange> ranges;
  std::vector<char*> residentRows;
  std::vector<uint64_t> peerStartBits;
  bool hasSpilledRows{false};
  BmPeerBoundaryMode peerBoundaryMode{BmPeerBoundaryMode::kNotNeeded};
};

struct BmWindowPartitionTestStats {
  uint64_t extractNullCalls{0};
  uint64_t extractNullRows{0};
  uint64_t maxExtractNullBatchRows{0};
  uint64_t loadRowsCalls{0};
  uint64_t loadedRows{0};
  uint64_t maxLoadedRows{0};
  uint64_t reclaimReadChunksCalls{0};
};

void resetBmWindowPartitionTestStats();

BmWindowPartitionTestStats bmWindowPartitionTestStats();

class BmWindowPartition : public exec::WindowPartition {
 private:
  enum class RowAccessMode {
    kResidentRows,
    kUndecided,
    kBulkRead,
    kWindowRead,
  };

 public:
  BmWindowPartition(
      exec::bm::BmRowContainer* data,
      memory::MemoryPool* pool,
      std::vector<TypePtr> logicalTypes,
      BmWindowPartitionDescriptor descriptor,
      const std::vector<column_index_t>& inputMapping,
      const std::vector<std::pair<column_index_t, core::SortOrder>>&
          sortKeyInfo);

  vector_size_t numRows() const override;

  bool hasResidentRows() const {
    return rowAccessMode_ == RowAccessMode::kResidentRows;
  }

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

  void extractNulls(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      const BufferPtr& nullsBuffer) const override;

  void extractColumns(
      folly::Range<const column_index_t*> columnIndices,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      folly::Range<const VectorPtr*> results,
      bool exactSize = false) const;

  std::pair<vector_size_t, vector_size_t> computePeerBuffers(
      vector_size_t start,
      vector_size_t end,
      vector_size_t prevPeerStart,
      vector_size_t prevPeerEnd,
      vector_size_t* rawPeerStarts,
      vector_size_t* rawPeerEnds,
      bool enableJit = false) const override;

  void computeKRangeFrameBounds(
      bool isStartBound,
      bool isPreceding,
      column_index_t frameColumn,
      vector_size_t startRow,
      vector_size_t numRows,
      const vector_size_t* rawPeerStarts,
      vector_size_t* rawFrameBounds) const override;

  uint64_t reclaimReadChunks(
      uint64_t targetBytes = exec::bm::kUnlimitedBytes) const;

 private:
  friend class BmRangeFrameBounds;

  FOLLY_ALWAYS_INLINE void initializePhysicalTypes() {
    physicalTypes_.resize(logicalTypes_.size());
    for (auto logical = 0; logical < inputMapping_.size(); ++logical) {
      BOLT_CHECK_LT(inputMapping_[logical], physicalTypes_.size());
      physicalTypes_[inputMapping_[logical]] = logicalTypes_[logical];
    }
  }

  bool canBulkReadPartition() const;

  RowAccessMode ensureAccessMode() const;

  std::vector<const char*> loadRows(
      vector_size_t partitionOffset,
      vector_size_t numRows) const;

  std::vector<const char*> loadResidentRows(
      vector_size_t partitionOffset,
      vector_size_t numRows) const;

  const char* const* residentRowsData(vector_size_t partitionOffset) const;

  std::vector<exec::bm::SegmentRowRange> getSegmentRanges(
      vector_size_t partitionOffset,
      vector_size_t numRows) const;

  VectorPtr extractColumnFromRows(
      const std::vector<const char*>& rows,
      int32_t physicalColumn) const;

  const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo()
      const {
    return sortKeyInfo_;
  }

  vector_size_t peerStartAtOrBeforeFromMetadata(vector_size_t row) const;

  vector_size_t nextPeerStartFromMetadata(vector_size_t row) const;

  void extractRowsToVector(
      const std::vector<const char*>& rows,
      int32_t physicalColumn,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize) const;

  void extractRowsToVector(
      const char* const* rows,
      vector_size_t numRows,
      int32_t physicalColumn,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize) const;

  bool copyCachedNulls(
      int32_t physicalColumn,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      const BufferPtr& nullsBuffer) const;

  void cacheNulls(
      int32_t physicalColumn,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      const BufferPtr& nullsBuffer) const;

  struct CachedNulls {
    bool valid{false};
    vector_size_t partitionOffset{0};
    vector_size_t numRows{0};
    BufferPtr nulls;
  };

  exec::bm::BmRowContainer* data_;
  memory::MemoryPool* pool_;
  std::vector<TypePtr> logicalTypes_;
  std::vector<TypePtr> physicalTypes_;
  mutable std::vector<CachedNulls> nullsCache_;
  mutable std::vector<char*> residentRows_;
  std::vector<exec::bm::SegmentRowRange> rowRanges_;
  std::vector<exec::bm::SegmentId> segmentIds_;
  std::vector<uint64_t> peerStartBits_;
  vector_size_t numRows_{0};
  BmPeerBoundaryMode peerBoundaryMode_{BmPeerBoundaryMode::kNotNeeded};
  mutable RowAccessMode rowAccessMode_{RowAccessMode::kUndecided};
  mutable std::optional<exec::bm::BulkReadSession> bulkSession_;
  mutable std::optional<exec::bm::ReadOnlyWindowReadSession> readSession_;
};

} // namespace bytedance::bolt::exec::window
