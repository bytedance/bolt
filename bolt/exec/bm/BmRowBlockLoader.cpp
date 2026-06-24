#include "bolt/exec/bm/BmRowBlockLoader.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/bm/BmStringViewRebaser.h"

#include <folly/Portability.h>

#include <span>
#include <unordered_map>

namespace bytedance::bolt::exec::bm {
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
    folly::Range<const SegmentId*> segments) {
  std::vector<ChunkData*> chunks;
  for (const auto segmentId : segments) {
    auto& segment = this->segments().segmentData(segmentId);
    chunks.reserve(chunks.size() + segment.chunks.size());
    for (auto& chunkPtr : segment.chunks) {
      auto& chunk = *chunkPtr;
      chunks.push_back(&chunk);
    }
  }
  loadChunks({chunks.data(), chunks.size()});
}

void BmRowBlockLoader::loadChunks(
    folly::Range<ChunkData* const*> chunks) {
  const bool mayNeedRebase = !layout().stringColumns().empty();
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  std::vector<BlockRef*> blockRefs;
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
    }
    blocks.reserve(blocks.size() + blockCount);
    blockRefs.reserve(blockRefs.size() + blockCount);
    if (!chunk.rowBlock.handle.valid()) {
      BOLT_CHECK_NOT_NULL(chunk.rowBlock.block);
      blocks.push_back(chunk.rowBlock.block);
      blockRefs.push_back(&chunk.rowBlock);
    }
    for (auto& block : chunk.heapBlocks) {
      if (!block.handle.valid()) {
        BOLT_CHECK_NOT_NULL(block.block);
        blocks.push_back(block.block);
        blockRefs.push_back(&block);
      }
    }
  }
  if (blocks.empty()) {
    return;
  }

  auto pins = bufferManager_->BatchPin(
      std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
          blocks.data(), blocks.size()));
  BOLT_DCHECK_EQ(pins.size(), blockRefs.size());

  if (FOLLY_LIKELY(!mayNeedRebase)) {
    for (size_t i = 0; i < pins.size(); ++i) {
      auto& block = *blockRefs[i];
      block.handle = std::move(pins[i]);
      block.ptr = block.handle.Ptr();
    }
    return;
  }

  for (size_t i = 0; i < pins.size(); ++i) {
    auto& block = *blockRefs[i];
    block.handle = std::move(pins[i]);
    auto* const newPtr = block.handle.Ptr();
    block.ptr = newPtr;
  }
  for (auto* chunk : touchedChunks) {
    std::unordered_map<BlockId, uintptr_t> heapBases;
    heapBases.reserve(chunk->heapBlocks.size());
    for (const auto& heapBlock : chunk->heapBlocks) {
      if (!heapBlock.handle.valid()) {
        continue;
      }
      BOLT_CHECK_NOT_NULL(heapBlock.ptr);
      heapBases.emplace(
          heapBlock.id, reinterpret_cast<uintptr_t>(heapBlock.ptr));
    }
    if (rebaseStringViewsInChunk(
            *chunk, layout(), segments().rowStride(), heapBases)) {
      bufferManager_->MarkDirty(chunk->rowBlock.block);
    }
  }
}

void BmRowBlockLoader::loadChunk(ChunkData& chunk) {
  ChunkData* chunkPtr = &chunk;
  loadChunks({&chunkPtr, 1});
}

} // namespace bytedance::bolt::exec::bm
