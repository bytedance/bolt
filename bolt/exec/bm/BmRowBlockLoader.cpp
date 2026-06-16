#include "bolt/exec/bm/BmRowBlockLoader.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/bm/BmStringViewRebaser.h"

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

} // namespace

BmRowBlockLoader::BmRowBlockLoader(
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    const BmRowLayout* layout,
    BmSegmentCollection* segments)
    : bufferManager_(std::move(bufferManager)),
      layout_(layout),
      segments_(segments) {
  BOLT_CHECK_NOT_NULL(bufferManager_);
  BOLT_CHECK_NOT_NULL(layout_);
  BOLT_CHECK_NOT_NULL(segments_);
}

void BmRowBlockLoader::loadSegments(
    folly::Range<const SegmentId*> segments,
    BulkLoadMetrics* metrics) {
  std::vector<ChunkData*> chunks;
  for (const auto segmentId : segments) {
    auto& segment = this->segments().segmentData(segmentId);
    chunks.reserve(chunks.size() + segment.chunks.size());
    for (auto& chunkPtr : segment.chunks) {
      auto& chunk = *chunkPtr;
      chunks.push_back(&chunk);
    }
  }
  loadChunks({chunks.data(), chunks.size()}, metrics);
}

void BmRowBlockLoader::loadChunks(
    folly::Range<ChunkData* const*> chunks,
    BulkLoadMetrics* metrics) {
  const auto collectStart = metrics == nullptr ? 0 : nowNs();
  const bool mayNeedRebase = !layout().stringColumns().empty();
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
    if (mayNeedRebase) {
      touchedChunks.push_back(&chunk);
      isHeapBlock.reserve(isHeapBlock.size() + blockCount);
    }
    blocks.reserve(blocks.size() + blockCount);
    blockRefs.reserve(blockRefs.size() + blockCount);
    if (!chunk.rowBlock.handle.valid()) {
      BOLT_CHECK_NOT_NULL(chunk.rowBlock.block);
      blocks.push_back(chunk.rowBlock.block);
      blockRefs.push_back(&chunk.rowBlock);
      if (mayNeedRebase) {
        isHeapBlock.push_back(false);
      }
    }
    for (auto& block : chunk.heapBlocks) {
      if (!block.handle.valid()) {
        BOLT_CHECK_NOT_NULL(block.block);
        blocks.push_back(block.block);
        blockRefs.push_back(&block);
        if (mayNeedRebase) {
          isHeapBlock.push_back(true);
        }
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
  if (FOLLY_LIKELY(!mayNeedRebase)) {
    for (size_t i = 0; i < pins.size(); ++i) {
      auto& block = *blockRefs[i];
      block.handle = std::move(pins[i]);
      block.ptr = block.handle.Ptr();
    }
    if (metrics != nullptr) {
      metrics->updateBlockPointersNs += nowNs() - updateStart;
    }
    return;
  }

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
      rebaseStringViewsInChunk(
          *chunk, layout(), segments().rowStride(), heapRebases, metrics);
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

} // namespace bytedance::bolt::exec::bm
