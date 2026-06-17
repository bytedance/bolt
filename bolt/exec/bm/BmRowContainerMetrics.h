#pragma once

#include <cstdint>

namespace bytedance::bolt::exec::bm {

struct BulkLoadMetrics {
  // Time spent estimating bytes that must be pinned for bulk/window reads.
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
  // Number of blocks pinned by bulk/window reads.
  uint64_t pinnedBlocks{0};
  // Number of StringViews whose pointer was rebased.
  uint64_t rebasedStringViews{0};
  // Number of row pointers returned to caller.
  uint64_t pointerRows{0};
  // Number of RowIds returned to caller.
  uint64_t rowIdRows{0};
};

struct BmStoreMetrics {
  uint64_t appendOnlyNs{0};
  uint64_t appendFixedNs{0};
  uint64_t appendFullNs{0};
  uint64_t rows{0};
};

struct BmSegmentSpillMetrics {
  uint64_t zeroHeapTailNs{0};
  uint64_t collectBlocksNs{0};
  uint64_t spillBlocksNs{0};
  uint64_t chunks{0};
  uint64_t rowBlocks{0};
  uint64_t heapBlocks{0};
  uint64_t totalBlocks{0};
  uint64_t rowBlockBytes{0};
  uint64_t heapBlockBytes{0};
  uint64_t usedRowBytes{0};
  uint64_t usedHeapBytes{0};
  uint64_t unusedHeapTailBytes{0};
};

struct BmBatchAppendMetrics {
  uint64_t batches{0};
  uint64_t rows{0};
  uint64_t fixedColumns{0};
  uint64_t stringColumns{0};
  uint64_t stringRows{0};
  uint64_t stringInlineRows{0};
  uint64_t stringCopiedBytes{0};
  uint64_t stringReferencedBytes{0};
  uint64_t stringHeapAllocCalls{0};
  uint64_t stringFastAllocHits{0};
  uint64_t stringSlowAllocHits{0};
  uint64_t stringHeapBlockSwitches{0};
  uint64_t stringRecordHeapCalls{0};
  uint64_t totalNs{0};
  uint64_t reserveRowsNs{0};
  uint64_t decodeNs{0};
  uint64_t fixedStoreNs{0};
  uint64_t stringStoreNs{0};
};

} // namespace bytedance::bolt::exec::bm
