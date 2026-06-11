#pragma once

#include "bolt/exec/bm/BmSegmentCollection.h"

#include <cstdint>
#include <unordered_map>
#include <utility>

namespace bytedance::bolt::exec::bm {

void rebaseStringViewsInChunkSlow(
    ChunkData& chunk,
    const BmRowLayout& layout,
    uint32_t rowStride,
    const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
        heapRebases,
    BulkLoadMetrics* metrics);

// Rewrites non-inline StringView payload pointers after BufferManager pins heap
// blocks at new virtual addresses. The caller owns block pinning; this helper
// only understands row layout and chunk-local heap base metadata.
FOLLY_ALWAYS_INLINE void rebaseStringViewsInChunk(
    ChunkData& chunk,
    const BmRowLayout& layout,
    uint32_t rowStride,
    const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
        heapRebases,
    BulkLoadMetrics* metrics) {
  if (FOLLY_LIKELY(
          layout.stringColumns().empty() || chunk.heapBases.empty())) {
    return;
  }
  if (FOLLY_UNLIKELY(heapRebases.empty())) {
    return;
  }
  rebaseStringViewsInChunkSlow(
      chunk, layout, rowStride, heapRebases, metrics);
}

} // namespace bytedance::bolt::exec::bm
