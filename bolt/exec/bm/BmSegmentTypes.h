#pragma once

#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/exec/bm/BmRowContainerPublicTypes.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace bytedance::bolt::exec::bm {

// Heap block base metadata used to rebase StringView payload pointers after
// BufferManager pins heap blocks at new addresses. baseAddress tracks the heap
// base currently referenced by the row block's StringViews.
struct HeapBaseRef {
  BlockId heapBlockId{kNoBlock};
  uintptr_t baseAddress{0};
  uint32_t capacity{0};
};

struct DataChunkMeta {
  ChunkId id{0};
  SegmentId segmentId{0};
  RowNumber firstRowNumber{0};
  uint32_t rowCount{0};
};

struct SegmentMeta {
  SegmentId id{0};
  SegmentState state{SegmentState::kActiveResident};
  std::optional<PartitionId> partitionId;
  // Global append ordinal of row 0 in this segment. It is used only for FIFO
  // prefix release; segment-local row numbers remain the public addressing
  // scheme.
  uint64_t firstGlobalRow{0};
  uint64_t numRows{0};
  // True when rows are physically materialized in merge order. MergeReadSession
  // requires this because it only merges already-ordered segments.
  bool orderedForMerge{false};
};

struct BlockRef {
  BlockId id{kNoBlock};
  std::shared_ptr<memory::bm::BlockHandle> block;
  // Pin handle. When empty, ptr must not be dereferenced.
  memory::bm::BufferHandle handle;
  char* ptr{nullptr};
  uint32_t size{0};
  uint32_t used{0};
};

struct ChunkData {
  DataChunkMeta meta;
  // Current BM RowContainer deliberately keeps one chunk anchored to one row
  // block; see BmSegmentCollection.h for the storage model details.
  BlockRef rowBlock;
  // Heap blocks never cross chunk boundaries, so one chunk owns all variable
  // width payload blocks referenced by rows in its row block.
  std::vector<BlockRef> heapBlocks;
  std::vector<HeapBaseRef> heapBases;
  // Consuming merge reads can drop blocks after this chunk has been read. The
  // metadata stays so rowNumber/chunk indexing is not disturbed.
  bool consumed{false};
};

struct SegmentWriteCursor {
  ChunkData* chunk{nullptr};
  char* nextRow{nullptr};
  char* rowBlockEnd{nullptr};
};

struct SegmentData {
  SegmentMeta meta;
  // Keep ChunkData addresses stable because RowWriteContext, read sessions and
  // block loaders may temporarily hold raw ChunkData* while the chunk list grows.
  std::vector<std::unique_ptr<ChunkData>> chunks;
  RowNumber nextRowNumber{0};
  ChunkId currentChunk{kNoBlock};
  SegmentWriteCursor writeCursor;
};

} // namespace bytedance::bolt::exec::bm
