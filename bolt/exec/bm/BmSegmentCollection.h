#pragma once

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/MemoryTag.h"
#include "bolt/exec/bm/BmBatchAppend.h"
#include "bolt/exec/bm/BmRowLayout.h"
#include "bolt/exec/bm/BmSegmentTypes.h"

#include <folly/Range.h>
#include <folly/Portability.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace bytedance::bolt::exec::bm {

// Owns segment/chunk metadata and all BufferManager block handles for one
// BmRowContainer. It does not know column semantics beyond row size; typed
// store/compare/extract logic lives in BmRowContainer and BmRowCopier.
//
// Current physical hierarchy:
//
//   BmSegmentCollection
//     SegmentData
//       ChunkData
//         rowBlock      fixed-width rows for one row block
//         heapBlocks    variable-width payload blocks referenced by those rows
//         heapBases     current heap base metadata for StringView rebasing
//
// A chunk is deliberately anchored to one row block and may own several heap
// blocks. This differs from DuckDB's TupleDataChunk/ChunkPart model where a
// logical chunk can be split into smaller parts that each describe precise row
// and heap slices. Bolt keeps the old RowContainer write shape for now:
// appendRow() allocates row slots before every variable-width payload size is
// known, so one-row-block chunks are simpler and keep window read ownership
// local. The cost is coarser heap pinning/rebasing for variable width data. If
// callers later switch to vector-planned writes, this layer can adopt a finer
// DuckDB-like part model.
class BmSegmentCollection {
 public:
  BmSegmentCollection(
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      memory::bm::MemoryTag tag,
      const BmRowLayout* layout,
      uint32_t rowBlockSize,
      uint32_t heapBlockSize);

  // Segment lifecycle and partition ownership.
  SegmentId spillActiveSegment();
  SegmentId spillActivePartitionSegment(PartitionId partition);
  void releaseSegment(SegmentId segment);
  void releaseSegments(folly::Range<const SegmentId*> segments);
  SegmentState segmentState(SegmentId segment) const;
  const std::vector<SegmentId>& segmentsForPartition(
      PartitionId partition) const;
  std::vector<SegmentId> allSegmentIds() const;
  int64_t numRows() const;

  // Segment creation and finalization.
  SegmentData& activeSegment(PartitionId partition);
  SegmentData& createSegment(std::optional<PartitionId> partition);
  SegmentId finalizeAndFlush(PartitionId partition);
  SegmentId finalizeAndFlushSegment(SegmentData& segment);
  SegmentData& segmentData(SegmentId segment);
  const SegmentData& segmentData(SegmentId segment) const;

  // Write allocation.
  FOLLY_ALWAYS_INLINE BlockRef& ensureHeapBlockInChunk(
      ChunkData& chunk,
      uint32_t minBytes) {
    if (FOLLY_LIKELY(
            !chunk.heapBlocks.empty() &&
            chunk.heapBlocks.back().used + minBytes <=
                chunk.heapBlocks.back().size)) {
      return chunk.heapBlocks.back();
    }
    return ensureHeapBlockSlow(chunk, minBytes);
  }

  char* newRowInSegment(SegmentData& segment);
  void reserveRowsInBatch(
      SegmentData& segment,
      vector_size_t sourceBegin,
      vector_size_t count,
      std::vector<BatchAppendRange>& ranges,
      std::vector<char*>* rows);
  void recordHeapForChunk(
      ChunkData& chunk,
      const BlockRef& heap,
      const char* row);

  // Read addressing.
  ChunkData& currentChunk(SegmentData& segment);
  const ChunkData& currentChunk(const SegmentData& segment) const;
  ChunkData& chunkForRow(SegmentData& segment, RowNumber rowNumber);
  const ChunkData& chunkForRow(
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
      std::vector<char*>& rows);
  char* rowPointer(const RowId& id);
  const char* rowPointer(const RowId& id) const;

  // Release and accounting.
  void releaseChunkBlocks(ChunkData& chunk);
  uint64_t segmentBytes(const SegmentData& segment) const;
  FOLLY_ALWAYS_INLINE uint32_t rowStride() const {
    return rowStride_;
  }

 private:
  BlockRef addBlock(uint32_t blockSize);
  ChunkData& ensureWritableChunk(SegmentData& segment);
  FOLLY_ALWAYS_INLINE ChunkData& chunkForRowUnchecked(
      SegmentData& segment,
      RowNumber rowNumber) const {
    return const_cast<ChunkData&>(
        chunkForRowUnchecked(std::as_const(segment), rowNumber));
  }
  FOLLY_ALWAYS_INLINE const ChunkData& chunkForRowUnchecked(
      const SegmentData& segment,
      RowNumber rowNumber) const {
    const auto chunkIndex = rowNumber / rowsPerChunk_;
    BOLT_DCHECK_LT(chunkIndex, segment.chunks.size());
    const auto& chunk = *segment.chunks[chunkIndex];
    BOLT_DCHECK(
        rowNumber >= chunk.meta.firstRowNumber &&
        rowNumber < chunk.meta.firstRowNumber + chunk.meta.rowCount);
    return chunk;
  }
  BlockRef& ensureHeapBlockSlow(ChunkData& chunk, uint32_t minBytes);

  FOLLY_ALWAYS_INLINE const BmRowLayout& layout() const {
    BOLT_DCHECK_NOT_NULL(layout_);
    return *layout_;
  }

  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
  memory::bm::MemoryTag tag_;
  const BmRowLayout* layout_{nullptr};
  uint32_t rowBlockSize_;
  uint32_t heapBlockSize_;
  uint32_t rowStride_{0};
  uint32_t rowsPerChunk_{0};
  SegmentId nextSegmentId_{1};
  BlockId nextBlockId_{1};
  std::vector<std::unique_ptr<SegmentData>> segments_;
  std::vector<SegmentId> activeSegments_;
  std::vector<std::vector<SegmentId>> partitionSegments_;
};

} // namespace bytedance::bolt::exec::bm
