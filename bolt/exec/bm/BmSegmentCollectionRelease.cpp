#include "bolt/exec/bm/BmSegmentCollection.h"

namespace bytedance::bolt::exec::bm {

void BmSegmentCollection::releaseChunkBlocks(ChunkData& chunk) {
  if (chunk.consumed) {
    return;
  }
  chunk.rowBlock.handle = memory::bm::BufferHandle{};
  chunk.rowBlock.ptr = nullptr;
  chunk.rowBlock.block.reset();
  for (auto& block : chunk.heapBlocks) {
    block.handle = memory::bm::BufferHandle{};
    block.ptr = nullptr;
    block.block.reset();
  }
  chunk.heapBlocks.clear();
  chunk.heapBases.clear();
  chunk.consumed = true;
}

uint64_t BmSegmentCollection::segmentBytes(const SegmentData& segment) const {
  uint64_t bytes = 0;
  for (const auto& chunkPtr : segment.chunks) {
    const auto& chunk = *chunkPtr;
    if (chunk.consumed) {
      continue;
    }
    bytes += chunk.rowBlock.size;
    for (const auto& block : chunk.heapBlocks) {
      bytes += block.size;
    }
  }
  return bytes;
}

} // namespace bytedance::bolt::exec::bm
