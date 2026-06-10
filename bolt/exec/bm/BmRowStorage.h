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
#include <unordered_map>
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
  // Heap base addresses observed by StringView payloads in this chunk. They
  // are used to rebase non-inline StringViews after BufferManager pins blocks
  // at a different address.
  std::vector<HeapBaseRef> heapBases;
};

struct SegmentData {
  // Public lifecycle and block/chunk summary.
  SegmentMeta meta;
  // Chunk data used by bulk/window read. SegmentData deliberately does not keep
  // flat rowBlocks/heapBlocks mirrors; ownership lives in ChunkData so the
  // hierarchy is Segment -> Chunk -> {row block, heap blocks, rebase metadata}.
  std::vector<ChunkData> chunks;
  // Next row number to assign inside this segment.
  RowNumber nextRowNumber{0};
  // Active chunk while the segment accepts writes.
  ChunkId currentChunk{kNoBlock};
};

// Owns segment/block/chunk metadata and all BufferManager block handles for one
// BmRowContainer. It does not know column semantics beyond row size.
class BmRowStorage {
 public:
  BmRowStorage(
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      memory::bm::MemoryTag tag,
      const BmRowLayout* layout,
      uint32_t rowBlockSize,
      uint32_t heapBlockSize);

  SegmentId flushActiveSegment();
  SegmentId flushActivePartitionSegment(PartitionId partition);
  void releaseSegment(SegmentId segment, ReleaseReason reason);
  void releaseSegments(
      folly::Range<const SegmentId*> segments,
      ReleaseReason reason);
  SegmentState segmentState(SegmentId segment) const;
  const std::vector<SegmentId>& segmentsForPartition(
      PartitionId partition) const;
  int64_t numRows() const;

  SegmentData& activeSegment(PartitionId partition);
  SegmentData& createSegment(std::optional<PartitionId> partition);
  SegmentId finalizeAndFlush(PartitionId partition);
  SegmentId finalizeAndFlushSegment(SegmentData& segment);
  SegmentData& segmentData(SegmentId segment);
  const SegmentData& segmentData(SegmentId segment) const;

  FOLLY_ALWAYS_INLINE BlockRef& ensureHeapBlock(
      SegmentData& segment,
      uint32_t minBytes) {
    return ensureHeapBlockInChunk(currentChunk(segment), minBytes);
  }

  FOLLY_ALWAYS_INLINE BlockRef& ensureHeapBlockForChunk(
      SegmentData& segment,
      ChunkId chunkId,
      uint32_t minBytes) {
    BOLT_DCHECK_LT(chunkId, segment.chunks.size());
    return ensureHeapBlockInChunk(segment.chunks[chunkId], minBytes);
  }

  BlockRef& blockRef(SegmentData& segment, BlockId id, bool isRowBlock);

  char* newRowInSegment(SegmentData& segment);
  void updateChunkForRow(SegmentData& segment, const RowId& rowId);
  void recordHeapForCurrentChunk(SegmentData& segment, const BlockRef& heap);
  void recordHeapForChunk(
      SegmentData& segment,
      ChunkId chunk,
      const BlockRef& heap,
      const char* row);

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

  uint64_t segmentBytes(const SegmentData& segment) const;
  FOLLY_ALWAYS_INLINE uint32_t rowStride() const {
    return layout().rowSize();
  }

 private:
  BlockRef addBlock(uint32_t blockSize);
  ChunkData& ensureWritableChunk(SegmentData& segment);
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
  SegmentId nextSegmentId_{1};
  BlockId nextBlockId_{1};
  std::unordered_map<SegmentId, SegmentData> segments_;
  std::unordered_map<PartitionId, SegmentId> activeSegments_;
  std::unordered_map<PartitionId, std::vector<SegmentId>> partitionSegments_;
  static const std::vector<SegmentId> kEmptySegments_;
};

} // namespace bytedance::bolt::exec::bm
