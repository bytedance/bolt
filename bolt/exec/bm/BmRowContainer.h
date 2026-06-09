#pragma once

#include "bolt/common/base/CompareFlags.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/MemoryTag.h"
#include "bolt/exec/bm/BmRowContainerRead.h"
#include "bolt/exec/bm/BmRowContainerTypes.h"
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
  struct AppendBatchResult {
    std::vector<char*> rows;
  };

  class RowWriter {
   public:
    RowWriter() = default;

    char* row() const {
      return row_;
    }

    void store(
        const DecodedVector& decoded,
        vector_size_t sourceIndex,
        int32_t column);

    void finish();

   private:
    friend class BmRowContainer;

    RowWriter(
        BmRowContainer* container,
        SegmentId segment,
        ChunkId chunk,
        PartId part,
        char* row)
        : container_(container),
          segment_(segment),
          chunk_(chunk),
          part_(part),
          row_(row) {}

    BmRowContainer* container_{nullptr};
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

  AppendBatchResult appendBatch(
      const RowVectorPtr& input,
      PartitionId partition = kDefaultPartition);

  RowWriter appendRow(PartitionId partition = kDefaultPartition);

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

  SortedRunId finalizeSortedRun(
      folly::Range<char* const*> sortedRows,
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
    bool nullable{false};
    uint32_t nullByte{0};
    uint8_t nullMask{0};
  };

  struct BlockRef {
    BlockId id{kNoBlock};
    std::shared_ptr<memory::bm::BlockHandle> block;
    memory::bm::BufferHandle handle;
    char* ptr{nullptr};
    uint32_t size{0};
    uint32_t used{0};
  };

  struct StringColumnLayout {
    uint32_t offset{0};
    bool nullable{false};
    uint32_t nullByte{0};
    uint8_t nullMask{0};
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

  SegmentData& createSegment(std::optional<PartitionId> partition);
  SegmentData& activeSegment(PartitionId partition);
  SegmentId finalizeAndFlush(PartitionId partition);
  SegmentId finalizeAndFlushSegment(SegmentData& segment);
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
  char* newRowInSegment(SegmentData& segment);
  char* copyRowToSegment(SegmentData& segment, const char* source);
  void updateChunkForRow(SegmentData& segment, const RowId& rowId);
  void recordHeapForCurrentPart(SegmentData& segment, const BlockRef& heap);
  void recordHeapForPart(
      SegmentData& segment,
      ChunkId chunk,
      PartId part,
      const BlockRef& heap,
      const char* row);
  const DataChunkMeta& chunkForRow(
      const SegmentData& segment,
      RowNumber rowNumber) const;
  RowId rowIdForRowNumber(
      const SegmentData& segment,
      RowNumber rowNumber) const;
  void appendRowIdsForSegment(
      const SegmentData& segment,
      std::vector<RowId>& rows) const;
  void appendRowPointersForSegment(
      SegmentData& segment,
      std::vector<char*>& rows,
      BulkLoadMetrics* metrics = nullptr);
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
  void storeValue(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      RowWriter& writer,
      int32_t column);
  template <TypeKind Kind>
  void storeValueTyped(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      RowWriter& writer,
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
  std::vector<memory::bm::BufferHandle> pinSegments(
      folly::Range<const SegmentId*> segments,
      BulkLoadMetrics* metrics = nullptr);
  std::vector<memory::bm::BufferHandle> pinChunk(
      SegmentData& segment,
      const DataChunkMeta& chunk);
  void rebaseStringViews(
      SegmentData& segment,
      const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
          heapRebases,
      BulkLoadMetrics* metrics = nullptr);
  void rebaseChunk(
      SegmentData& segment,
      const DataChunkMeta& chunk,
      const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
          heapRebases,
      BulkLoadMetrics* metrics = nullptr);
  uint64_t segmentBytes(const SegmentData& segment) const;
  uint32_t rowStride() const;
  bool isNull(const char* row, const StringColumnLayout& column) const;

  friend class BulkReadSession;
  friend class SegmentCursor;
  friend class MergeReadSession;

  std::vector<TypePtr> types_;
  std::vector<ColumnLayout> columns_;
  std::vector<StringColumnLayout> stringColumns_;
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
