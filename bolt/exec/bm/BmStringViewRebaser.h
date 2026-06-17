#pragma once

#include "bolt/exec/bm/BmSegmentCollection.h"

#include <folly/Portability.h>

#include <cstdint>
#include <unordered_map>
#include <utility>
#include <vector>

namespace bytedance::bolt::exec::bm {
namespace detail {

struct HeapRebaseRange {
  HeapBaseRef* heapBase{nullptr};
  uintptr_t oldBase{0};
  uintptr_t newBase{0};
  uintptr_t end{0};
};

FOLLY_ALWAYS_INLINE std::vector<HeapRebaseRange> collectRebaseRanges(
    ChunkData& chunk,
    const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
        heapRebases) {
  std::vector<HeapRebaseRange> ranges;
  ranges.reserve(chunk.heapBases.size());
  for (auto& heapBase : chunk.heapBases) {
    const auto rebase = heapRebases.find(heapBase.heapBlockId);
    if (rebase == heapRebases.end()) {
      continue;
    }
    const auto oldBase = heapBase.baseAddress;
    BOLT_CHECK_NE(
        oldBase,
        0,
        "StringView rebase requires a recorded spill-backing heap base");
    if (oldBase == rebase->second.second) {
      continue;
    }
    ranges.push_back(
        {&heapBase,
         oldBase,
         rebase->second.second,
         oldBase + heapBase.capacity});
  }
  return ranges;
}

FOLLY_ALWAYS_INLINE void rebaseSingleRange(
    ChunkData& chunk,
    const BmRowLayout& layout,
    uint32_t rowStride,
    const HeapRebaseRange& range) {
  const auto oldBase = range.oldBase;
  const auto newBase = range.newBase;
  const auto end = range.end;
  auto* row = chunk.rowBlock.ptr;
  for (uint32_t rowIndex = 0; rowIndex < chunk.meta.rowCount; ++rowIndex) {
    for (const auto& column : layout.stringColumns()) {
      if (layout.isNull(row, column)) {
        continue;
      }
      auto* value = reinterpret_cast<StringView*>(row + column.offset);
      if (value->isInline()) {
        continue;
      }
      const auto oldAddress = reinterpret_cast<uintptr_t>(value->data());
      if (oldAddress >= oldBase && oldAddress < end) {
        *value = StringView(
            reinterpret_cast<const char*>(newBase + oldAddress - oldBase),
            value->size());
      }
    }
    row += rowStride;
  }
}

FOLLY_ALWAYS_INLINE void rebaseMultipleRanges(
    ChunkData& chunk,
    const BmRowLayout& layout,
    uint32_t rowStride,
    const std::vector<HeapRebaseRange>& ranges) {
  // Multi-heap chunks are expected when variable-width payloads cross heap
  // block boundaries. Keep the lookup as a linear scan for now: it is
  // cache-friendly for small vectors and avoids per-chunk index construction.
  // If large heapBases vectors show up in real workloads, add a last-hit cache
  // or sort ranges by oldBase and use upper_bound for interval lookup.
  auto* row = chunk.rowBlock.ptr;
  for (uint32_t rowIndex = 0; rowIndex < chunk.meta.rowCount; ++rowIndex) {
    for (const auto& column : layout.stringColumns()) {
      if (layout.isNull(row, column)) {
        continue;
      }
      auto* value = reinterpret_cast<StringView*>(row + column.offset);
      if (value->isInline()) {
        continue;
      }
      const auto oldAddress = reinterpret_cast<uintptr_t>(value->data());
      for (const auto& range : ranges) {
        if (oldAddress >= range.oldBase && oldAddress < range.end) {
          *value = StringView(
              reinterpret_cast<const char*>(
                  range.newBase + oldAddress - range.oldBase),
              value->size());
          break;
        }
      }
    }
    row += rowStride;
  }
}

FOLLY_ALWAYS_INLINE void rebaseStringViewsInChunkSlow(
    ChunkData& chunk,
    const BmRowLayout& layout,
    uint32_t rowStride,
    const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
        heapRebases) {
  auto ranges = collectRebaseRanges(chunk, heapRebases);
  if (ranges.empty()) {
    return;
  }

  BOLT_DCHECK_NOT_NULL(chunk.rowBlock.ptr);
  if (FOLLY_LIKELY(ranges.size() == 1)) {
    rebaseSingleRange(chunk, layout, rowStride, ranges[0]);
  } else {
    rebaseMultipleRanges(chunk, layout, rowStride, ranges);
  }
}

} // namespace detail

// Rewrites non-inline StringView payload pointers after BufferManager pins heap
// blocks at new virtual addresses. heapBases deliberately stays at the spill
// backing base, not the resident base. ReadOnlyWindow eviction must therefore
// evict the row block and all heap blocks of a chunk together.
void rebaseStringViewsInChunk(
    ChunkData& chunk,
    const BmRowLayout& layout,
    uint32_t rowStride,
    const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
        heapRebases) {
  if (FOLLY_LIKELY(
          layout.stringColumns().empty() || chunk.heapBases.empty())) {
    return;
  }
  if (FOLLY_UNLIKELY(heapRebases.empty())) {
    return;
  }
  detail::rebaseStringViewsInChunkSlow(
      chunk, layout, rowStride, heapRebases);
}

} // namespace bytedance::bolt::exec::bm
