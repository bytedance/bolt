#pragma once

#include <cstdint>
#include <limits>
#include <optional>
#include <vector>

namespace bytedance::bolt::exec::bm {

// A segment is the flush/read/release unit visible to operators. It owns an
// ordered sequence of chunks.
using SegmentId = uint32_t;

// BufferManager-backed row or heap block id. Block ids are unique inside one
// row container and are used to rebuild pointers after pinning.
using BlockId = uint32_t;

// Offset of a row inside its row block.
using RowOffset = uint32_t;

// Row ordinal inside a segment. It is stable after the row is appended.
using RowNumber = uint32_t;

// Operator-defined partition id. The default partition is used by non-partitioned
// operators such as simple sort/hash agg paths.
using PartitionId = uint32_t;

// A chunk is the row-view used by window read.
//
// DuckDB's TupleDataCollection separates the logical scan chunk from physical
// storage slices: one logical TupleDataChunk can be described by multiple
// TupleDataChunkParts, and each part maps a contiguous row slice to the row
// block and heap slice needed to read that slice. This works well for DuckDB
// because append happens from vectors: the writer can inspect a vector first,
// know the fixed row count and variable-width payload sizes, then allocate row
// and heap slices together.
//
// BM RowContainer cannot use that layout yet. Its current write API preserves
// the old RowContainer usage pattern: callers allocate a row with
// appendRow()/appendBatch(), then store columns later through RowWriteContext.
// In appendBatch(), all fixed row slots are allocated first and variable-width
// columns are written afterwards. Therefore the writer does not know the
// VARCHAR/VARBINARY payload sizes when the row slot and chunk are created, and
// different string columns can cross heap block boundaries at different rows.
//
// Because of that, the current design is intentionally simpler: one chunk is
// anchored to one row block and owns the heap blocks referenced by rows in that
// row block. Rebase metadata is tracked at chunk level, not part level.
//
// If upper operators stop using row-at-a-time allocation and all RowContainer
// writes become vector writes, we can switch to a DuckDB-like design: plan row
// and heap slices from the vector up front, then create finer chunk parts that
// describe those slices precisely.
//
// The drawback of the current design is coarser metadata. A chunk can reference
// several heap blocks, so window read pins all heap blocks for the chunk and
// StringView rebasing scans the whole chunk against all relevant heap bases.
// It is simpler and matches today's API, but less precise than a part-based
// layout for large variable-width working sets.
using ChunkId = uint32_t;

constexpr BlockId kNoBlock = std::numeric_limits<BlockId>::max();
constexpr PartitionId kDefaultPartition = 0;

enum class SegmentState {
  // The segment is still accepting writes and all pointers are resident.
  kActiveResident,
  // The segment is closed for writes but its blocks have not been flushed yet.
  kFinalizedResident,
  // The segment is closed and its blocks are managed by BufferManager. Callers
  // must load blocks back through BmRowContainer read APIs before using
  // pointers.
  kFinalizedFlushed,
};

struct RowId {
  // Segment containing the row.
  SegmentId segmentId{0};
  // Ordinal inside the segment. Used to locate chunk metadata.
  RowNumber rowNumber{0};
  // Row block containing the fixed-width row payload.
  BlockId rowBlockId{kNoBlock};
  // Offset inside rowBlockId.
  RowOffset rowOffset{0};
};

struct BulkLoadMetrics {
  // Time spent estimating bytes that must be pinned for listRows().
  uint64_t estimateBytesNs{0};
  // Time spent asking BufferManager to reserve estimated memory.
  uint64_t reserveNs{0};
  // Time spent collecting row/heap blocks before BatchPin.
  uint64_t collectBlocksNs{0};
  // Time spent in BufferManager batch pin.
  uint64_t batchPinNs{0};
  // Time spent refreshing BlockRef raw pointers after pinning.
  uint64_t updateBlockPointersNs{0};
  // Time spent rebasing StringView payload pointers.
  uint64_t rebaseStringViewsNs{0};
  // Time spent materializing output char* rows after full load.
  uint64_t appendRowPointersNs{0};
  // Time spent materializing RowIds after falling back to window read.
  uint64_t appendRowIdsNs{0};
  // Estimated bytes for the full working set.
  uint64_t estimatedBytes{0};
  // Number of blocks pinned by listRows()/window reads.
  uint64_t pinnedBlocks{0};
  // Number of StringViews whose pointer was rebased.
  uint64_t rebasedStringViews{0};
  // Number of row pointers returned to caller.
  uint64_t pointerRows{0};
  // Number of RowIds returned to caller.
  uint64_t rowIdRows{0};
};

struct HeapBaseRef {
  // Heap block referenced by one chunk.
  BlockId heapBlockId{kNoBlock};
  // Last known base address of the heap block. It is refreshed after pinning and
  // used to detect whether existing StringView pointers need rebasing.
  uintptr_t baseAddress{0};
  // Heap block capacity, used to test whether a StringView payload belongs to
  // this heap block.
  uint32_t capacity{0};
};

struct DataChunkMeta {
  // Chunk id inside a segment.
  ChunkId id{0};
  // Owning segment.
  SegmentId segmentId{0};
  // First row number covered by this chunk.
  RowNumber firstRowNumber{0};
  // Number of rows covered by this chunk.
  uint32_t rowCount{0};
};

struct SegmentMeta {
  // Stable segment id returned to operators.
  SegmentId id{0};
  // Lifecycle state of this segment.
  SegmentState state{SegmentState::kActiveResident};
  // Partition that produced this segment. std::nullopt is used only for
  // internally-created segments where partition identity is irrelevant.
  std::optional<PartitionId> partitionId;
  // Number of rows finalized into this segment.
  uint64_t numRows{0};
  // True when rows are physically materialized in merge order. MergeReadSession
  // requires this because it only merges already-ordered segments; it does not
  // sort inside a segment.
  bool orderedForMerge{false};
};

} // namespace bytedance::bolt::exec::bm
