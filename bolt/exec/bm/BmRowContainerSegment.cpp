#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <span>
#include <vector>

namespace bytedance::bolt::exec::bm {
namespace {

void collectLoadedBlock(
    BlockRef& block,
    std::vector<std::shared_ptr<memory::bm::BlockHandle>>& blocks) {
  if (!block.handle.valid()) {
    return;
  }
  BOLT_CHECK_NOT_NULL(block.block);
  blocks.push_back(block.block);
  block.handle = memory::bm::BufferHandle{};
  block.ptr = nullptr;
}

void collectLoadedChunkBlocks(
    ChunkData& chunk,
    std::vector<std::shared_ptr<memory::bm::BlockHandle>>& blocks) {
  blocks.reserve(blocks.size() + 1 + chunk.heapBlocks.size());
  collectLoadedBlock(chunk.rowBlock, blocks);
  for (auto& block : chunk.heapBlocks) {
    collectLoadedBlock(block, blocks);
  }
}

} // namespace

SegmentId BmRowContainer::flushActiveSegment() {
  return segments_.flushActiveSegment();
}

SegmentId BmRowContainer::flushActivePartitionSegment(PartitionId partition) {
  return segments_.flushActivePartitionSegment(partition);
}

void BmRowContainer::releaseSegment(SegmentId segment) {
  segments_.releaseSegment(segment);
}

void BmRowContainer::releaseSegments(folly::Range<const SegmentId*> segments) {
  for (auto segment : segments) {
    releaseSegment(segment);
  }
}

void BmRowContainer::releaseChunk(SegmentId segment, ChunkId chunk) {
  auto& segmentData = segments_.segmentData(segment);
  BOLT_CHECK_LT(chunk, segmentData.chunks.size());
  segments_.releaseChunkBlocks(segmentData.chunks[chunk]);
}

void BmRowContainer::spillLoadedChunk(SegmentId segment, ChunkId chunk) {
  auto& segmentData = segments_.segmentData(segment);
  BOLT_CHECK(
      segmentData.meta.state != SegmentState::kActiveResident,
      "Cannot spill loaded blocks from active segment {}",
      segment);
  BOLT_CHECK_LT(chunk, segmentData.chunks.size());
  auto& chunkData = segmentData.chunks[chunk];
  BOLT_CHECK(
      !chunkData.consumed,
      "Cannot spill consumed chunk {} in segment {}",
      chunk,
      segment);

  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  collectLoadedChunkBlocks(chunkData, blocks);
  if (blocks.empty()) {
    return;
  }
  bufferManager_->SpillBlocks(
      std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
          blocks.data(), blocks.size()));
}

void BmRowContainer::spillLoadedSegments(
    folly::Range<const SegmentId*> segments) {
  validateSegments(segments);
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  for (auto segment : segments) {
    auto& segmentData = segments_.segmentData(segment);
    BOLT_CHECK(
        segmentData.meta.state != SegmentState::kActiveResident,
        "Cannot spill loaded blocks from active segment {}",
        segment);
    for (auto& chunk : segmentData.chunks) {
      if (!chunk.consumed) {
        collectLoadedChunkBlocks(chunk, blocks);
      }
    }
  }
  if (blocks.empty()) {
    return;
  }
  bufferManager_->SpillBlocks(
      std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
          blocks.data(), blocks.size()));
}

void BmRowContainer::spillAllLoadedBlocks() {
  auto segments = segments_.allSegmentIds();
  spillLoadedSegments({segments.data(), segments.size()});
}

SegmentState BmRowContainer::segmentState(SegmentId segment) const {
  return segments_.segmentState(segment);
}

const std::vector<SegmentId>& BmRowContainer::segmentsForPartition(
    PartitionId partition) const {
  return segments_.segmentsForPartition(partition);
}

int64_t BmRowContainer::numRows() const {
  return segments_.numRows();
}

} // namespace bytedance::bolt::exec::bm
