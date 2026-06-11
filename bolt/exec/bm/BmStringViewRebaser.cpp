#include "bolt/exec/bm/BmStringViewRebaser.h"

#include <folly/Portability.h>

#include <vector>

namespace bytedance::bolt::exec::bm {
namespace {

struct HeapRebaseRange {
  HeapBaseRef* heapBase{nullptr};
  uintptr_t oldBase{0};
  uintptr_t newBase{0};
  uintptr_t end{0};
};

std::vector<HeapRebaseRange> collectRebaseRanges(
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
    if (oldBase == 0) {
      heapBase.baseAddress = rebase->second.second;
      continue;
    }
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

void rebaseSingleRange(
    ChunkData& chunk,
    const BmRowLayout& layout,
    uint32_t rowStride,
    const HeapRebaseRange& range,
    BulkLoadMetrics* metrics) {
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
      if (oldAddress >= range.oldBase && oldAddress < range.end) {
        *value = StringView(
            reinterpret_cast<const char*>(
                range.newBase + oldAddress - range.oldBase),
            value->size());
        if (metrics != nullptr) {
          ++metrics->rebasedStringViews;
        }
      }
    }
    row += rowStride;
  }
}

void rebaseMultipleRanges(
    ChunkData& chunk,
    const BmRowLayout& layout,
    uint32_t rowStride,
    const std::vector<HeapRebaseRange>& ranges,
    BulkLoadMetrics* metrics) {
  // Multi-heap chunks are expected when variable-width payloads cross heap
  // block boundaries. Keep the lookup as a linear scan for now: it is
  // cache-friendly for small vectors and avoids per-chunk index construction.
  // If metrics show large heapBases vectors in real workloads, add a last-hit
  // cache or sort ranges by oldBase and use upper_bound for interval lookup.
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
          if (metrics != nullptr) {
            ++metrics->rebasedStringViews;
          }
          break;
        }
      }
    }
    row += rowStride;
  }
}

void refreshHeapBases(const std::vector<HeapRebaseRange>& ranges) {
  for (auto& range : ranges) {
    range.heapBase->baseAddress = range.newBase;
  }
}

} // namespace

void rebaseStringViewsInChunkSlow(
    ChunkData& chunk,
    const BmRowLayout& layout,
    uint32_t rowStride,
    const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
        heapRebases,
    BulkLoadMetrics* metrics) {
  auto ranges = collectRebaseRanges(chunk, heapRebases);
  if (ranges.empty()) {
    return;
  }

  BOLT_DCHECK_NOT_NULL(chunk.rowBlock.ptr);
  if (FOLLY_LIKELY(ranges.size() == 1)) {
    rebaseSingleRange(chunk, layout, rowStride, ranges[0], metrics);
  } else {
    rebaseMultipleRanges(chunk, layout, rowStride, ranges, metrics);
  }
  refreshHeapBases(ranges);
}

} // namespace bytedance::bolt::exec::bm
