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

class BmRowStorage {
 public:
  BmRowStorage(
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      memory::bm::MemoryTag tag,
      const BmRowLayout* layout,
      uint32_t rowBlockSize,
      uint32_t heapBlockSize,
      uint32_t chunkRowCount);

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

  BlockRef& addBlock(
      SegmentData& segment,
      bool isRowBlock,
      uint32_t blockSize);
  FOLLY_ALWAYS_INLINE BlockRef& ensureRowBlock(SegmentData& segment) {
    if (FOLLY_LIKELY(
            !segment.rowBlocks.empty() &&
            segment.rowBlocks.back().used + layout().rowSize() <=
                segment.rowBlocks.back().size)) {
      return segment.rowBlocks.back();
    }
    return ensureRowBlockSlow(segment);
  }

  FOLLY_ALWAYS_INLINE BlockRef& ensureHeapBlock(
      SegmentData& segment,
      uint32_t minBytes) {
    if (FOLLY_LIKELY(
            !segment.heapBlocks.empty() &&
            segment.heapBlocks.back().used + minBytes <=
                segment.heapBlocks.back().size)) {
      return segment.heapBlocks.back();
    }
    return ensureHeapBlockSlow(segment, minBytes);
  }

  BlockRef& blockRef(SegmentData& segment, BlockId id, bool isRowBlock);

  char* newRowInSegment(SegmentData& segment);
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

  uint64_t segmentBytes(const SegmentData& segment) const;
  FOLLY_ALWAYS_INLINE uint32_t rowStride() const {
    return layout().rowSize();
  }

 private:
  BlockRef& ensureRowBlockSlow(SegmentData& segment);
  BlockRef& ensureHeapBlockSlow(SegmentData& segment, uint32_t minBytes);

  FOLLY_ALWAYS_INLINE const BmRowLayout& layout() const {
    BOLT_CHECK_NOT_NULL(layout_);
    return *layout_;
  }

  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
  memory::bm::MemoryTag tag_;
  const BmRowLayout* layout_{nullptr};
  uint32_t rowBlockSize_;
  uint32_t heapBlockSize_;
  uint32_t chunkRowCount_;
  SegmentId nextSegmentId_{1};
  BlockId nextBlockId_{1};
  std::unordered_map<SegmentId, SegmentData> segments_;
  std::unordered_map<PartitionId, SegmentId> activeSegments_;
  std::unordered_map<PartitionId, std::vector<SegmentId>> partitionSegments_;
  std::unordered_map<BlockId, std::pair<SegmentId, bool>> blockIndex_;
  static const std::vector<SegmentId> kEmptySegments_;
};

} // namespace bytedance::bolt::exec::bm
