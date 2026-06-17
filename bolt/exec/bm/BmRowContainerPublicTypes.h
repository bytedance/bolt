#pragma once

#include <cstdint>
#include <limits>

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

// Operator-defined partition id. The default partition is used by
// non-partitioned operators such as simple sort/hash agg paths.
using PartitionId = uint32_t;

// A chunk is the row-view used by window read. It remains public because RowId
// identifies chunk-backed storage today, but callers should treat it as an
// implementation detail.
using ChunkId = uint32_t;

constexpr BlockId kNoBlock = std::numeric_limits<BlockId>::max();
constexpr SegmentId kNoSegment = 0;
constexpr PartitionId kDefaultPartition = 0;
constexpr uint32_t kMaxPartitions = 256;
constexpr uint64_t kUnlimitedBytes = std::numeric_limits<uint64_t>::max();

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

} // namespace bytedance::bolt::exec::bm
