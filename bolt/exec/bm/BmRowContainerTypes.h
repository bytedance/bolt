#pragma once

#include <cstdint>
#include <limits>
#include <optional>
#include <vector>

namespace bytedance::bolt::exec::bm {

using SegmentId = uint32_t;
using ReorderedRunId = uint32_t;
using BlockId = uint32_t;
using RowOffset = uint32_t;
using RowNumber = uint32_t;
using PartitionId = uint32_t;
using ChunkId = uint32_t;
using PartId = uint32_t;

constexpr BlockId kNoBlock = std::numeric_limits<BlockId>::max();
constexpr PartitionId kDefaultPartition = 0;

enum class SegmentState {
  kActiveResident,
  kFinalizedResident,
  kFinalizedFlushed,
};

enum class ReadMode {
  kFullyResident,
  kWindowRead,
};

enum class LoadAllResult {
  kLoadedPointers,
  kNeedWindowRead,
};

enum class ReorderedRunLayout {
  kMaterializedOrder,
};

enum class ReleaseReason {
  kConsumed,
  kDiscarded,
};

struct RowId {
  SegmentId segmentId{0};
  RowNumber rowNumber{0};
  BlockId rowBlockId{kNoBlock};
  RowOffset rowOffset{0};
  BlockId primaryHeapBlockId{kNoBlock};
};

struct BulkLoadMetrics {
  uint64_t estimateBytesNs{0};
  uint64_t reserveNs{0};
  uint64_t collectBlocksNs{0};
  uint64_t batchPinNs{0};
  uint64_t updateBlockPointersNs{0};
  uint64_t rebaseStringViewsNs{0};
  uint64_t appendRowPointersNs{0};
  uint64_t appendRowIdsNs{0};
  uint64_t estimatedBytes{0};
  uint64_t pinnedBlocks{0};
  uint64_t rebasedStringViews{0};
  uint64_t pointerRows{0};
  uint64_t rowIdRows{0};
};

struct ReadSessionOptions {
  uint64_t maxPinnedBytes{0};
  bool releaseWhenConsumed{false};
  BulkLoadMetrics* bulkLoadMetrics{nullptr};
};

struct ReorderedRunOptions {
  ReorderedRunLayout preferredLayout{ReorderedRunLayout::kMaterializedOrder};
};

struct HeapBaseRef {
  BlockId heapBlockId{kNoBlock};
  uintptr_t baseAddress{0};
  uint32_t capacity{0};
};

struct ChunkPartMeta {
  PartId id{0};
  ChunkId chunkId{0};
  BlockId rowBlockId{kNoBlock};
  uint32_t rowBlockOffset{0};
  uint32_t rowCount{0};
  // A part is split by row-block continuity, not by heap-block changes. This
  // deliberately preserves the old RowContainer newRow()+store(...) write
  // model where variable-width sizes are not known when the row is allocated.
  // Therefore one part can reference multiple heap blocks for StringView
  // pointer rebasing.
  std::vector<HeapBaseRef> heapBases;
};

struct DataChunkMeta {
  ChunkId id{0};
  SegmentId segmentId{0};
  RowNumber firstRowNumber{0};
  uint32_t rowCount{0};
  std::vector<PartId> parts;
  std::vector<BlockId> rowBlocks;
  std::vector<BlockId> heapBlocks;
};

struct SegmentMeta {
  SegmentId id{0};
  SegmentState state{SegmentState::kActiveResident};
  std::optional<PartitionId> partitionId;
  std::vector<BlockId> rowBlocks;
  std::vector<BlockId> heapBlocks;
  std::vector<ChunkId> chunks;
  uint64_t numRows{0};
};

struct ReorderedRunMeta {
  ReorderedRunId id{0};
  ReorderedRunLayout layout{ReorderedRunLayout::kMaterializedOrder};
  SegmentId materializedSegment{0};
  uint64_t numRows{0};
};

} // namespace bytedance::bolt::exec::bm
