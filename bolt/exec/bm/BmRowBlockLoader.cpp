#include "bolt/exec/bm/BmRowBlockLoader.h"

#include "bolt/common/base/Exceptions.h"

#include <folly/Portability.h>

#include <chrono>
#include <span>
#include <unordered_map>

namespace bytedance::bolt::exec::bm {
namespace {

uint64_t nowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

struct HeapRebaseRange {
  HeapBaseRef* heapBase{nullptr};
  uintptr_t oldBase{0};
  uintptr_t newBase{0};
  uintptr_t end{0};
};

} // namespace

BmRowBlockLoader::BmRowBlockLoader(
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    const BmRowLayout* layout,
    BmRowStorage* storage)
    : bufferManager_(std::move(bufferManager)),
      layout_(layout),
      storage_(storage) {
  BOLT_CHECK_NOT_NULL(bufferManager_);
  BOLT_CHECK_NOT_NULL(layout_);
  BOLT_CHECK_NOT_NULL(storage_);
}

std::vector<memory::bm::BufferHandle> BmRowBlockLoader::pinSegments(
    folly::Range<const SegmentId*> segments,
    BulkLoadMetrics* metrics) {
  const auto collectStart = metrics == nullptr ? 0 : nowNs();
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  std::vector<BlockRef*> blockRefs;
  std::vector<bool> isHeapBlock;
  for (const auto segmentId : segments) {
    auto& segment = storage().segmentData(segmentId);
    size_t blockCount = 0;
    for (const auto& chunk : segment.chunks) {
      BOLT_CHECK(
          !chunk.consumed,
          "Cannot pin consumed chunk {} in segment {}",
          chunk.meta.id,
          segment.meta.id);
      blockCount += 1 + chunk.heapBlocks.size();
    }
    blocks.reserve(blocks.size() + blockCount);
    blockRefs.reserve(blockRefs.size() + blockCount);
    isHeapBlock.reserve(isHeapBlock.size() + blockCount);
    for (auto& chunk : segment.chunks) {
      blocks.push_back(chunk.rowBlock.block);
      blockRefs.push_back(&chunk.rowBlock);
      isHeapBlock.push_back(false);
      for (auto& block : chunk.heapBlocks) {
        blocks.push_back(block.block);
        blockRefs.push_back(&block);
        isHeapBlock.push_back(true);
      }
    }
  }
  if (metrics != nullptr) {
    metrics->collectBlocksNs += nowNs() - collectStart;
    metrics->pinnedBlocks += blocks.size();
  }

  const auto batchPinStart = metrics == nullptr ? 0 : nowNs();
  auto pins = bufferManager_->BatchPin(
      std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
          blocks.data(), blocks.size()));
  if (metrics != nullptr) {
    metrics->batchPinNs += nowNs() - batchPinStart;
  }
  BOLT_DCHECK_EQ(pins.size(), blockRefs.size());

  const auto updateStart = metrics == nullptr ? 0 : nowNs();
  std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>> heapRebases;
  for (size_t i = 0; i < pins.size(); ++i) {
    auto& block = *blockRefs[i];
    auto* const newPtr = pins[i].Ptr();
    if (isHeapBlock[i]) {
      // Heap payloads can be read back at a different address. Keep the old
      // and new bases so StringView values stored in row blocks can be rebased.
      const auto oldBase = reinterpret_cast<uintptr_t>(block.ptr);
      const auto newBase = reinterpret_cast<uintptr_t>(newPtr);
      if (oldBase != 0 && oldBase != newBase) {
        heapRebases[block.id] = {oldBase, newBase};
      }
    }
    block.ptr = newPtr;
  }
  if (metrics != nullptr) {
    metrics->updateBlockPointersNs += nowNs() - updateStart;
  }
  if (!heapRebases.empty()) {
    for (const auto segmentId : segments) {
      rebaseStringViews(storage().segmentData(segmentId), heapRebases, metrics);
    }
  }
  return pins;
}

std::vector<memory::bm::BufferHandle> BmRowBlockLoader::pinChunk(
    ChunkData& chunk) {
  BOLT_CHECK(
      !chunk.consumed,
      "Cannot pin consumed chunk {} in segment {}",
      chunk.meta.id,
      chunk.meta.segmentId);
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  std::vector<BlockRef*> blockRefs;
  std::vector<bool> isHeapBlock;
  blocks.reserve(1 + chunk.heapBlocks.size());
  blockRefs.reserve(1 + chunk.heapBlocks.size());
  isHeapBlock.reserve(1 + chunk.heapBlocks.size());
  blocks.push_back(chunk.rowBlock.block);
  blockRefs.push_back(&chunk.rowBlock);
  isHeapBlock.push_back(false);
  for (auto& block : chunk.heapBlocks) {
    blocks.push_back(block.block);
    blockRefs.push_back(&block);
    isHeapBlock.push_back(true);
  }

  auto pins = bufferManager_->BatchPin(
      std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
          blocks.data(), blocks.size()));
  BOLT_DCHECK_EQ(pins.size(), blockRefs.size());

  std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>> heapRebases;
  for (size_t i = 0; i < pins.size(); ++i) {
    auto& block = *blockRefs[i];
    auto* const newPtr = pins[i].Ptr();
    if (isHeapBlock[i]) {
      // Window reads pin one chunk at a time. StringViews in the row block may
      // still point at the heap base recorded for this chunk before the block was
      // unpinned. Always pass the pinned heap base to rebaseChunk(); it decides
      // from chunk-level HeapBaseRef metadata whether row values need rebasing.
      const auto oldBase = reinterpret_cast<uintptr_t>(block.ptr);
      const auto newBase = reinterpret_cast<uintptr_t>(newPtr);
      heapRebases[block.id] = {oldBase, newBase};
    }
    block.ptr = newPtr;
  }
  if (!heapRebases.empty()) {
    rebaseChunk(chunk, heapRebases, nullptr);
  }
  return pins;
}

void BmRowBlockLoader::rebaseStringViews(
    SegmentData& segment,
    const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
        heapRebases,
    BulkLoadMetrics* metrics) {
  const auto rebaseStart = metrics == nullptr ? 0 : nowNs();
  for (auto& chunk : segment.chunks) {
    rebaseChunk(chunk, heapRebases, metrics);
  }
  if (metrics != nullptr) {
    metrics->rebaseStringViewsNs += nowNs() - rebaseStart;
  }
}

void BmRowBlockLoader::rebaseChunk(
    ChunkData& chunk,
    const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
        heapRebases,
    BulkLoadMetrics* metrics) {
  if (FOLLY_LIKELY(layout().stringColumns().empty())) {
    return;
  }
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
  if (ranges.empty()) {
    return;
  }

  auto& rowBlock = chunk.rowBlock;
  BOLT_DCHECK_NOT_NULL(rowBlock.ptr);

  if (FOLLY_LIKELY(ranges.size() == 1)) {
    const auto range = ranges[0];
    for (uint32_t rowIndex = 0; rowIndex < chunk.meta.rowCount; ++rowIndex) {
      auto* row = rowBlock.ptr + rowIndex * storage().rowStride();
      for (const auto& column : layout().stringColumns()) {
        if (layout().isNull(row, column)) {
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
    }
  } else {
    // Multi-heap chunks are expected when variable-width payloads cross heap
    // block boundaries. Keep the lookup as a linear scan for now: it is
    // cache-friendly for small vectors and avoids per-chunk index construction.
    // If metrics show large heapBases vectors in real workloads, add a last-hit
    // cache or sort ranges by oldBase and use upper_bound for interval lookup.
    for (uint32_t rowIndex = 0; rowIndex < chunk.meta.rowCount; ++rowIndex) {
      auto* row = rowBlock.ptr + rowIndex * storage().rowStride();
      for (const auto& column : layout().stringColumns()) {
        if (layout().isNull(row, column)) {
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
    }
  }

  for (auto& range : ranges) {
    range.heapBase->baseAddress = range.newBase;
  }
}

} // namespace bytedance::bolt::exec::bm
