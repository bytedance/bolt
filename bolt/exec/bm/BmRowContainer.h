#pragma once

#include "bolt/common/base/CompareFlags.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/MemoryTag.h"
#include "bolt/exec/bm/BmRowContainerRead.h"
#include "bolt/exec/bm/BmRowContainerTypes.h"
#include "bolt/type/Type.h"
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
  BmRowContainer(
      std::vector<TypePtr> types,
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      memory::bm::MemoryTag tag,
      uint32_t rowBlockSize = static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      uint32_t heapBlockSize = static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      uint32_t chunkRowCount = 1024);

  RowHandle newRow();
  RowHandle newRow(PartitionId partition);

  void store(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      char* row,
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
      const char* const* rows,
      int32_t numRows,
      int32_t column,
      const VectorPtr& result,
      bool exactSize = false);

  char* resolveRowResident(const RowId& row);

  folly::Range<char* const*> resolveRowsResident(
      folly::Range<const RowId*> rows,
      std::vector<char*>& result);

  SegmentId flushActiveSegment();
  SegmentId flushActivePartitionSegment(PartitionId partition);

  SortedRunId finalizeSortedRun(
      folly::Range<const RowHandle*> sortedRows,
      const SortedRunOptions& options);

  BulkReadSession beginBulkReadSegments(
      folly::Range<const SegmentId*> segments,
      ReadSessionOptions options = {});

  MergeReadSession beginMergeReadSegments(
      folly::Range<const SortedRunId*> runs,
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
  struct ColumnLayout {
    TypePtr type;
    uint32_t offset{0};
    uint32_t width{0};
  };

  struct BlockRef {
    BlockId id{kNoBlock};
    std::shared_ptr<memory::bm::BlockHandle> block;
    memory::bm::BufferHandle handle;
    char* ptr{nullptr};
    uint32_t size{0};
    uint32_t used{0};
  };

  struct SegmentData {
    SegmentMeta meta;
    std::vector<BlockRef> rowBlocks;
    std::vector<BlockRef> heapBlocks;
    std::vector<DataChunkMeta> chunks;
    std::vector<ChunkPartMeta> parts;
    RowNumber nextRowNumber{0};
    uint32_t currentChunkRowCount{0};
    ChunkId currentChunk{kNoBlock};
    PartId currentPart{kNoBlock};
  };

  SegmentData& activeSegment(PartitionId partition);
  SegmentId finalizeAndFlush(PartitionId partition);
  SegmentData& segmentData(SegmentId segment);
  const SegmentData& segmentData(SegmentId segment) const;
  SortedRunMeta& sortedRunData(SortedRunId run);
  const SortedRunMeta& sortedRunData(SortedRunId run) const;
  BlockRef& addBlock(
      SegmentData& segment,
      bool isRowBlock,
      uint32_t blockSize);
  BlockRef& ensureRowBlock(SegmentData& segment);
  BlockRef& ensureHeapBlock(SegmentData& segment, uint32_t minBytes);
  BlockRef& blockRef(SegmentData& segment, BlockId id, bool isRowBlock);
  RowHandle newRowInSegment(SegmentData& segment);
  void updateChunkForRow(SegmentData& segment, const RowId& rowId);
  void recordHeapForCurrentPart(SegmentData& segment, const BlockRef& heap);
  const DataChunkMeta& chunkForRow(
      const SegmentData& segment,
      RowNumber rowNumber) const;
  char* rowPointer(const RowId& id);
  const char* rowPointer(const RowId& id) const;
  bool isNull(const char* row, int32_t column) const;
  void setNull(char* row, int32_t column, bool isNull) const;
  char* valueAddress(char* row, int32_t column) const;
  const char* valueAddress(const char* row, int32_t column) const;
  int32_t compareNonNull(
      const char* left,
      const char* right,
      int32_t column) const;
  void extractOne(
      const char* row,
      int32_t column,
      vector_size_t resultIndex,
      const VectorPtr& result,
      bool exactSize) const;
  std::vector<memory::bm::BufferHandle> pinSegments(
      folly::Range<const SegmentId*> segments);
  std::vector<memory::bm::BufferHandle> pinChunk(
      SegmentData& segment,
      const DataChunkMeta& chunk);
  void rebaseStringViews(
      SegmentData& segment,
      const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
          heapRebases);
  void rebaseChunk(
      SegmentData& segment,
      const DataChunkMeta& chunk,
      const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
          heapRebases);
  uint64_t segmentBytes(const SegmentData& segment) const;
  uint32_t rowStride() const;

  friend class BulkReadSession;
  friend class SegmentCursor;
  friend class MergeReadSession;

  std::vector<TypePtr> types_;
  std::vector<ColumnLayout> columns_;
  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
  memory::bm::MemoryTag tag_;
  uint32_t rowBlockSize_;
  uint32_t heapBlockSize_;
  uint32_t chunkRowCount_;
  uint32_t nullBytes_{0};
  uint32_t fixedRowSize_{0};
  SegmentId nextSegmentId_{1};
  SortedRunId nextSortedRunId_{1};
  BlockId nextBlockId_{1};
  std::unordered_map<SegmentId, SegmentData> segments_;
  std::unordered_map<PartitionId, SegmentId> activeSegments_;
  std::unordered_map<PartitionId, std::vector<SegmentId>> partitionSegments_;
  std::unordered_map<BlockId, std::pair<SegmentId, bool>> blockIndex_;
  std::unordered_map<SortedRunId, SortedRunMeta> sortedRuns_;
  static const std::vector<SegmentId> kEmptySegments_;
};

} // namespace bytedance::bolt::exec::bm
