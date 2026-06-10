#pragma once

#include "bolt/common/base/CompareFlags.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/MemoryTag.h"
#include "bolt/exec/bm/BmRowBlockLoader.h"
#include "bolt/exec/bm/BmRowContainerRead.h"
#include "bolt/exec/bm/BmRowContainerTypes.h"
#include "bolt/exec/bm/BmRowCopier.h"
#include "bolt/exec/bm/BmRowLayout.h"
#include "bolt/exec/bm/BmRowStorage.h"
#include "bolt/type/Type.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"

#include <folly/Range.h>

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

namespace bytedance::bolt::exec::bm {

class BmRowContainer {
 public:
  class RowWriteContext {
   public:
    RowWriteContext() = default;

    char* row() const {
      return row_;
    }

   private:
    friend class BmRowContainer;

    RowWriteContext(
        SegmentId segment,
        ChunkId chunk,
        PartId part,
        char* row)
        : segment_(segment),
          chunk_(chunk),
          part_(part),
          row_(row) {}

    SegmentId segment_{0};
    ChunkId chunk_{kNoBlock};
    PartId part_{kNoBlock};
    char* row_{nullptr};
  };

  BmRowContainer(
      std::vector<TypePtr> types,
      std::vector<bool> nullable,
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      memory::bm::MemoryTag tag,
      uint32_t rowBlockSize = static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      uint32_t heapBlockSize = static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      uint32_t chunkRowCount = 1024);

  std::vector<char*> appendBatch(
      const RowVectorPtr& input,
      PartitionId partition = kDefaultPartition);

  RowWriteContext appendRow(PartitionId partition = kDefaultPartition);

  void store(
      RowWriteContext& context,
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      int32_t column);

  int32_t compare(
      const char* left,
      const char* right,
      int32_t column,
      CompareFlags flags = {});

  int32_t compareRows(
      const char* left,
      const char* right,
      const std::vector<CompareFlags>& flags = {});

  void extractColumnResident(
      char* const* rows,
      int32_t numRows,
      int32_t column,
      const VectorPtr& result,
      bool exactSize = false);

  SegmentId flushActiveSegment();
  SegmentId flushActivePartitionSegment(PartitionId partition);

  ReorderedRunId finalizeReorderedRun(
      folly::Range<char* const*> sortedRows,
      const ReorderedRunOptions& options);

  BulkReadSession beginBulkReadSegments(
      folly::Range<const SegmentId*> segments,
      ReadSessionOptions options = {});

  MergeReadSession beginMergeReadRuns(
      folly::Range<const ReorderedRunId*> runs,
      ReadSessionOptions options = {});

  void releaseSegment(SegmentId segment, ReleaseReason reason);
  void releaseSegments(
      folly::Range<const SegmentId*> segments,
      ReleaseReason reason);

  SegmentState segmentState(SegmentId segment) const;
  const std::vector<SegmentId>& segmentsForPartition(PartitionId partition)
      const;
  int64_t numRows() const;

 private:
  ReorderedRunMeta& reorderedRunData(ReorderedRunId run);
  const ReorderedRunMeta& reorderedRunData(ReorderedRunId run) const;
  int32_t compareNonNull(
      const char* left,
      const char* right,
      int32_t column) const;
  void storeValue(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      RowWriteContext& context,
      int32_t column);
  template <TypeKind Kind>
  void storeValueTyped(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      RowWriteContext& context,
      const ColumnLayout& column);
  template <TypeKind Kind>
  void storeFixedColumnTyped(
      const DecodedVector& decoded,
      vector_size_t size,
      char* const* rows,
      int32_t column);
  template <TypeKind Kind>
  void extractColumnTyped(
      char* const* rows,
      int32_t numRows,
      const ColumnLayout& column,
      const VectorPtr& result,
      bool exactSize) const;
  friend class BulkReadSession;
  friend class SegmentCursor;
  friend class MergeReadSession;

  std::vector<TypePtr> types_;
  BmRowLayout layout_;
  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
  memory::bm::MemoryTag tag_;
  uint32_t rowBlockSize_;
  uint32_t heapBlockSize_;
  uint32_t chunkRowCount_;
  BmRowStorage storage_;
  BmRowBlockLoader blockLoader_;
  BmRowCopier rowCopier_;
  ReorderedRunId nextReorderedRunId_{1};
  std::unordered_map<ReorderedRunId, ReorderedRunMeta> reorderedRuns_;
};

} // namespace bytedance::bolt::exec::bm
