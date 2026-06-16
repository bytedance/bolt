#pragma once

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/MemoryTag.h"
#include "bolt/exec/bm/BmRowContainerTypes.h"
#include "bolt/exec/bm/BmRowLayout.h"

#include <folly/Range.h>
#include <folly/Portability.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace bytedance::bolt::exec::bm {

struct BlockRef {
  // Container-local block id.
  BlockId id{kNoBlock};
  // BufferManager block ownership handle.
  std::shared_ptr<memory::bm::BlockHandle> block;
  // Pin handle. When empty, ptr must not be dereferenced.
  memory::bm::BufferHandle handle;
  // Raw address for the pinned block.
  char* ptr{nullptr};
  // Block capacity in bytes.
  uint32_t size{0};
  // Bytes already used by this segment.
  uint32_t used{0};
};

struct ChunkData {
  // Logical row range covered by this chunk.
  DataChunkMeta meta;
  // Fixed-width rows for this chunk. Current BM RowContainer deliberately keeps
  // one chunk anchored to one row block; see BmRowContainerTypes.h for the
  // DuckDB comparison.
  BlockRef rowBlock;
  // Variable-width payload blocks referenced by rows in this chunk. Heap blocks
  // never cross chunk boundaries: cutting a new row block/chunk also cuts heap
  // reuse, which keeps chunk ownership and window read pinning local.
  std::vector<BlockRef> heapBlocks;
  // Heap base addresses encoded in spilled row-block StringViews. Read-only
  // window reads keep this as backing metadata, so a chunk's row block and heap
  // blocks must be evicted together.
  std::vector<HeapBaseRef> heapBases;
  // Consuming merge reads can drop blocks after this chunk has been read. The
  // metadata stays in place so rowNumber/chunk indexing is not disturbed, but
  // the chunk can no longer be pinned or read.
  bool consumed{false};
};

struct SegmentWriteCursor {
  ChunkData* chunk{nullptr};
  char* nextRow{nullptr};
  char* rowBlockEnd{nullptr};
};

struct SegmentData {
  // Public lifecycle and block/chunk summary.
  SegmentMeta meta;
  // Chunk data used by bulk/window read. SegmentData deliberately does not keep
  // flat rowBlocks/heapBlocks mirrors; ownership lives in ChunkData so the
  // hierarchy is Segment -> Chunk -> {row block, heap blocks, rebase metadata}.
  // Keep ChunkData addresses stable because RowWriteContext, read sessions and
  // block loaders may temporarily hold raw ChunkData* while the chunk list grows.
  std::vector<std::unique_ptr<ChunkData>> chunks;
  // Next row number to assign inside this segment.
  RowNumber nextRowNumber{0};
  // Active chunk while the segment accepts writes.
  ChunkId currentChunk{kNoBlock};
  // Hot append cursor for the active chunk.
  SegmentWriteCursor writeCursor;
};

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
//         heapBases     spill-backing heap base metadata for StringView rebasing
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

  SegmentId spillActiveSegment(BmSegmentSpillMetrics* metrics = nullptr);
  SegmentId spillActivePartitionSegment(
      PartitionId partition,
      BmSegmentSpillMetrics* metrics = nullptr);
  void releaseSegment(SegmentId segment);
  void releaseSegments(folly::Range<const SegmentId*> segments);
  SegmentState segmentState(SegmentId segment) const;
  const std::vector<SegmentId>& segmentsForPartition(
      PartitionId partition) const;
  std::vector<SegmentId> allSegmentIds() const;
  int64_t numRows() const;

  SegmentData& activeSegment(PartitionId partition);
  SegmentData& createSegment(std::optional<PartitionId> partition);
  SegmentId finalizeAndFlush(
      PartitionId partition,
      BmSegmentSpillMetrics* metrics = nullptr);
  SegmentId finalizeAndFlushSegment(
      SegmentData& segment,
      BmSegmentSpillMetrics* metrics = nullptr);
  SegmentData& segmentData(SegmentId segment);
  const SegmentData& segmentData(SegmentId segment) const;

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
  void recordHeapForChunk(ChunkData& chunk, const BlockRef& heap, const char* row);

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
      std::vector<char*>& rows,
      BulkLoadMetrics* metrics = nullptr);
  char* rowPointer(const RowId& id);
  const char* rowPointer(const RowId& id) const;
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
