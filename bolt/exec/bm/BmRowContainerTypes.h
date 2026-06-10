#pragma once

#include <cstdint>
#include <limits>
#include <optional>
#include <vector>

namespace bytedance::bolt::exec::bm {

// A segment is the flush/read/release unit visible to operators. It owns a
// sequence of row blocks, heap blocks, and chunk metadata.
using SegmentId = uint32_t;

// A reordered run is a logical read sequence over rows. The current
// implementation materializes that sequence into a dedicated segment.
using ReorderedRunId = uint32_t;

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

// A chunk groups rows for window read. Loading one chunk pins all blocks needed
// by rows in that chunk.
using ChunkId = uint32_t;

// A part is a contiguous row-block range inside a chunk. It is the unit used for
// fast row pointer reconstruction and StringView rebasing.
using PartId = uint32_t;

constexpr BlockId kNoBlock = std::numeric_limits<BlockId>::max();
constexpr PartitionId kDefaultPartition = 0;

enum class SegmentState {
  // The segment is still accepting writes and all pointers are resident.
  kActiveResident,
  // The segment is closed for writes but its blocks have not been flushed yet.
  kFinalizedResident,
  // The segment is closed and its blocks are managed by BufferManager. Callers
  // must read it through BulkReadSession/MergeReadSession before using pointers.
  kFinalizedFlushed,
};

enum class ReadMode {
  // All requested segments were pinned at once. tryLoadAll() returned pointers.
  kFullyResident,
  // The whole working set did not fit. The caller must submit RowIds back to the
  // session through loadRows()/loadRow().
  kWindowRead,
};

enum class LoadAllResult {
  // Output vector contains resident row pointers. RowId output is empty.
  kLoadedPointers,
  // Output vector contains RowIds. Pointer output is empty.
  kNeedWindowRead,
};

enum class ReorderedRunLayout {
  // Rows are physically copied to a new segment in the requested order. Merge
  // cursors can scan this segment sequentially.
  kMaterializedOrder,
};

enum class ReleaseReason {
  // Data has been consumed successfully and does not need to be preserved.
  kConsumed,
  // Data is abandoned before normal consumption, for example after an error or
  // because an upstream branch no longer needs it.
  kDiscarded,
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
  // Heap block that likely contains this row's first variable-width payload.
  // It is a locality hint for read/rebase paths; a row can still reference
  // additional heap blocks through chunk part metadata.
  BlockId primaryHeapBlockId{kNoBlock};
};

struct BulkLoadMetrics {
  // Time spent estimating bytes that must be pinned for tryLoadAll().
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
  // Number of blocks pinned by tryLoadAll().
  uint64_t pinnedBlocks{0};
  // Number of StringViews whose pointer was rebased.
  uint64_t rebasedStringViews{0};
  // Number of row pointers returned to caller.
  uint64_t pointerRows{0};
  // Number of RowIds returned to caller.
  uint64_t rowIdRows{0};
};

struct ReadSessionOptions {
  // Optional hard limit for tryLoadAll(). If non-zero and the estimated pinned
  // bytes exceed this limit, tryLoadAll() immediately returns RowIds.
  uint64_t maxPinnedBytes{0};
  // Reserved for future automatic release-on-consume support. Current callers
  // should still release consumed segments explicitly.
  bool releaseWhenConsumed{false};
  // Optional observer for bulk load timing/counter metrics.
  BulkLoadMetrics* bulkLoadMetrics{nullptr};
};

struct ReorderedRunOptions {
  // Preferred physical representation for the run. Only kMaterializedOrder is
  // currently supported.
  ReorderedRunLayout preferredLayout{ReorderedRunLayout::kMaterializedOrder};
};

struct HeapBaseRef {
  // Heap block referenced by one chunk part.
  BlockId heapBlockId{kNoBlock};
  // Last known base address of the heap block. It is refreshed after pinning and
  // used to detect whether existing StringView pointers need rebasing.
  uintptr_t baseAddress{0};
  // Heap block capacity, used to test whether a StringView payload belongs to
  // this heap block.
  uint32_t capacity{0};
};

struct ChunkPartMeta {
  // Part id inside SegmentData::parts.
  PartId id{0};
  // Owning chunk id.
  ChunkId chunkId{0};
  // Row block backing this contiguous row range.
  BlockId rowBlockId{kNoBlock};
  // First row offset in rowBlockId.
  uint32_t rowBlockOffset{0};
  // Number of rows in this part.
  uint32_t rowCount{0};
  // A part is split by row-block continuity, not by heap-block changes. This
  // deliberately preserves the old RowContainer newRow()+store(...) write
  // model where variable-width sizes are not known when the row is allocated.
  // Therefore one part can reference multiple heap blocks for StringView
  // pointer rebasing.
  std::vector<HeapBaseRef> heapBases;
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
  // Part ids that make up this chunk.
  std::vector<PartId> parts;
  // Row blocks needed to load this chunk.
  std::vector<BlockId> rowBlocks;
  // Heap blocks needed to load this chunk.
  std::vector<BlockId> heapBlocks;
};

struct SegmentMeta {
  // Stable segment id returned to operators.
  SegmentId id{0};
  // Lifecycle state of this segment.
  SegmentState state{SegmentState::kActiveResident};
  // Partition that produced this segment. std::nullopt is used only for
  // internally-created segments where partition identity is irrelevant.
  std::optional<PartitionId> partitionId;
  // All row blocks owned by the segment.
  std::vector<BlockId> rowBlocks;
  // All heap blocks owned by the segment.
  std::vector<BlockId> heapBlocks;
  // Chunk ids in segment order.
  std::vector<ChunkId> chunks;
  // Number of rows finalized into this segment.
  uint64_t numRows{0};
};

struct ReorderedRunMeta {
  // Stable run id returned by finalizeReorderedRun().
  ReorderedRunId id{0};
  // Physical layout used to read this run.
  ReorderedRunLayout layout{ReorderedRunLayout::kMaterializedOrder};
  // Segment containing rows in run order for kMaterializedOrder.
  SegmentId materializedSegment{0};
  // Number of rows in the run.
  uint64_t numRows{0};
};

} // namespace bytedance::bolt::exec::bm
