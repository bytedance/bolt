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

void BmRowBlockLoader::loadSegments(
    folly::Range<const SegmentId*> segments,
    BulkLoadMetrics* metrics) {
  std::vector<ChunkData*> chunks;
  for (const auto segmentId : segments) {
    auto& segment = storage().segmentData(segmentId);
    chunks.reserve(chunks.size() + segment.chunks.size());
    for (auto& chunk : segment.chunks) {
      chunks.push_back(&chunk);
    }
  }
  loadChunks({chunks.data(), chunks.size()}, metrics);
}

void BmRowBlockLoader::loadChunks(
    folly::Range<ChunkData* const*> chunks,
    BulkLoadMetrics* metrics) {
  const auto collectStart = metrics == nullptr ? 0 : nowNs();
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  std::vector<BlockRef*> blockRefs;
  std::vector<bool> isHeapBlock;
  std::vector<ChunkData*> touchedChunks;
  for (auto* chunkPtr : chunks) {
    BOLT_CHECK_NOT_NULL(chunkPtr);
    auto& chunk = *chunkPtr;
    BOLT_CHECK(
        !chunk.consumed,
        "Cannot pin consumed chunk {} in segment {}",
        chunk.meta.id,
        chunk.meta.segmentId);
    size_t blockCount = 0;
    if (!chunk.rowBlock.handle.valid()) {
      ++blockCount;
    }
    for (const auto& block : chunk.heapBlocks) {
      if (!block.handle.valid()) {
        ++blockCount;
      }
    }
    if (blockCount == 0) {
      continue;
    }
    touchedChunks.push_back(&chunk);
    blocks.reserve(blocks.size() + blockCount);
    blockRefs.reserve(blockRefs.size() + blockCount);
    isHeapBlock.reserve(isHeapBlock.size() + blockCount);
    if (!chunk.rowBlock.handle.valid()) {
      BOLT_CHECK_NOT_NULL(chunk.rowBlock.block);
      blocks.push_back(chunk.rowBlock.block);
      blockRefs.push_back(&chunk.rowBlock);
      isHeapBlock.push_back(false);
    }
    for (auto& block : chunk.heapBlocks) {
      if (!block.handle.valid()) {
        BOLT_CHECK_NOT_NULL(block.block);
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
  if (blocks.empty()) {
    return;
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
    block.handle = std::move(pins[i]);
    auto* const newPtr = block.handle.Ptr();
    if (isHeapBlock[i]) {
      // Heap payloads can be read back at a different address. The reliable
      // old address lives in chunk.heapBases because flushed BlockRef::ptr is
      // cleared when handles are released.
      const auto newBase = reinterpret_cast<uintptr_t>(newPtr);
      heapRebases[block.id] = {0, newBase};
    }
    block.ptr = newPtr;
  }
  if (metrics != nullptr) {
    metrics->updateBlockPointersNs += nowNs() - updateStart;
  }
  if (!heapRebases.empty()) {
    const auto rebaseStart = metrics == nullptr ? 0 : nowNs();
    for (auto* chunk : touchedChunks) {
      rebaseChunk(*chunk, heapRebases, metrics);
    }
    if (metrics != nullptr) {
      metrics->rebaseStringViewsNs += nowNs() - rebaseStart;
    }
  }
}

void BmRowBlockLoader::loadChunk(ChunkData& chunk) {
  ChunkData* chunkPtr = &chunk;
  loadChunks({&chunkPtr, 1}, nullptr);
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
  const auto rowWidth = storage().rowStride();

  if (FOLLY_LIKELY(ranges.size() == 1)) {
    const auto range = ranges[0];
    auto* row = rowBlock.ptr;
    for (uint32_t rowIndex = 0; rowIndex < chunk.meta.rowCount; ++rowIndex) {
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
      row += rowWidth;
    }
  } else {
    // Multi-heap chunks are expected when variable-width payloads cross heap
    // block boundaries. Keep the lookup as a linear scan for now: it is
    // cache-friendly for small vectors and avoids per-chunk index construction.
    // If metrics show large heapBases vectors in real workloads, add a last-hit
    // cache or sort ranges by oldBase and use upper_bound for interval lookup.
    auto* row = rowBlock.ptr;
    for (uint32_t rowIndex = 0; rowIndex < chunk.meta.rowCount; ++rowIndex) {
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
      row += rowWidth;
    }
  }

  for (auto& range : ranges) {
    range.heapBase->baseAddress = range.newBase;
  }
}

} // namespace bytedance::bolt::exec::bm
