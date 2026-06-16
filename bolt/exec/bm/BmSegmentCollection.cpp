#include "bolt/exec/bm/BmSegmentCollection.h"

#include "bolt/common/base/Exceptions.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <span>
#include <utility>

namespace bytedance::bolt::exec::bm {
namespace {

void zeroUnusedHeapTail(BlockRef& block) {
  BOLT_DCHECK_LE(block.used, block.size);
  if (block.ptr != nullptr && block.used < block.size) {
    std::memset(block.ptr + block.used, 0, block.size - block.used);
  }
}

void zeroUnusedHeapTail(ChunkData& chunk) {
  for (auto& block : chunk.heapBlocks) {
    zeroUnusedHeapTail(block);
  }
}

uint64_t metricNowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

void checkPartition(PartitionId partition) {
  BOLT_CHECK_LT(
      partition,
      kMaxPartitions,
      "BmRowContainer partition {} exceeds max partition count {}",
      partition,
      kMaxPartitions);
}

} // namespace

BmSegmentCollection::BmSegmentCollection(
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    memory::bm::MemoryTag tag,
    const BmRowLayout* layout,
    uint32_t rowBlockSize,
    uint32_t heapBlockSize)
    : bufferManager_(std::move(bufferManager)),
      tag_(tag),
      layout_(layout),
      rowBlockSize_(rowBlockSize),
      heapBlockSize_(heapBlockSize),
      activeSegments_(kMaxPartitions, kNoSegment),
      partitionSegments_(kMaxPartitions) {
  BOLT_CHECK_NOT_NULL(bufferManager_);
  BOLT_CHECK_NOT_NULL(layout_);
  rowStride_ = layout_->rowSize();
  BOLT_CHECK_GT(rowStride_, 0);
  rowsPerChunk_ = rowBlockSize_ / rowStride_;
  BOLT_CHECK_GT(rowsPerChunk_, 0);
}

SegmentId BmSegmentCollection::spillActiveSegment(
    BmSegmentSpillMetrics* metrics) {
  return spillActivePartitionSegment(kDefaultPartition, metrics);
}

SegmentId BmSegmentCollection::spillActivePartitionSegment(
    PartitionId partition,
    BmSegmentSpillMetrics* metrics) {
  return finalizeAndFlush(partition, metrics);
}

void BmSegmentCollection::releaseSegment(SegmentId segment) {
  if (segment >= segments_.size() || segments_[segment] == nullptr) {
    return;
  }
  auto& data = *segments_[segment];
  if (data.meta.partitionId.has_value()) {
    const auto partition = *data.meta.partitionId;
    checkPartition(partition);
    if (activeSegments_[partition] == segment) {
      activeSegments_[partition] = kNoSegment;
    }
    auto& segments = partitionSegments_[partition];
    segments.erase(
        std::remove(segments.begin(), segments.end(), segment),
        segments.end());
  }
  segments_[segment].reset();
}

void BmSegmentCollection::releaseSegments(
    folly::Range<const SegmentId*> segments) {
  for (auto segment : segments) {
    releaseSegment(segment);
  }
}

SegmentState BmSegmentCollection::segmentState(SegmentId segment) const {
  return segmentData(segment).meta.state;
}

const std::vector<SegmentId>& BmSegmentCollection::segmentsForPartition(
    PartitionId partition) const {
  checkPartition(partition);
  return partitionSegments_[partition];
}

std::vector<SegmentId> BmSegmentCollection::allSegmentIds() const {
  std::vector<SegmentId> ids;
  ids.reserve(segments_.size());
  for (SegmentId id = 1; id < segments_.size(); ++id) {
    if (segments_[id] != nullptr) {
      ids.push_back(id);
    }
  }
  return ids;
}

int64_t BmSegmentCollection::numRows() const {
  int64_t rows = 0;
  for (const auto& segment : segments_) {
    if (segment != nullptr) {
      rows += segment->meta.numRows;
    }
  }
  return rows;
}

SegmentData& BmSegmentCollection::activeSegment(PartitionId partition) {
  checkPartition(partition);
  auto id = activeSegments_[partition];
  if (id != kNoSegment) {
    return segmentData(id);
  }

  auto& segment = createSegment(partition);
  activeSegments_[partition] = segment.meta.id;
  return segment;
}

SegmentData& BmSegmentCollection::createSegment(
    std::optional<PartitionId> partition) {
  auto segment = std::make_unique<SegmentData>();
  segment->meta.id = nextSegmentId_++;
  segment->meta.state = SegmentState::kActiveResident;
  segment->meta.partitionId = std::move(partition);
  const auto id = segment->meta.id;
  if (segments_.size() <= id) {
    segments_.resize(id + 1);
  }
  segments_[id] = std::move(segment);
  return *segments_[id];
}

SegmentId BmSegmentCollection::finalizeAndFlush(
    PartitionId partition,
    BmSegmentSpillMetrics* metrics) {
  checkPartition(partition);
  auto active = activeSegments_[partition];
  BOLT_CHECK_NE(active, kNoSegment);
  auto& segment = segmentData(active);
  const auto id = finalizeAndFlushSegment(segment, metrics);
  partitionSegments_[partition].push_back(id);
  activeSegments_[partition] = kNoSegment;
  return id;
}

SegmentId BmSegmentCollection::finalizeAndFlushSegment(
    SegmentData& segment,
    BmSegmentSpillMetrics* metrics) {
  BOLT_DCHECK(segment.meta.state == SegmentState::kActiveResident);
  segment.meta.state = SegmentState::kFinalizedResident;

  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  for (auto& chunkPtr : segment.chunks) {
    auto& chunk = *chunkPtr;
    BOLT_DCHECK(!chunk.consumed);
    blocks.reserve(blocks.size() + 1 + chunk.heapBlocks.size());
    if (metrics != nullptr) {
      ++metrics->chunks;
      ++metrics->rowBlocks;
      ++metrics->totalBlocks;
      metrics->rowBlockBytes += chunk.rowBlock.size;
      metrics->usedRowBytes += chunk.rowBlock.used;
    }
    const auto zeroStart = metrics == nullptr ? 0 : metricNowNs();
    zeroUnusedHeapTail(chunk);
    if (metrics != nullptr) {
      metrics->zeroHeapTailNs += metricNowNs() - zeroStart;
    }
    const auto collectStart = metrics == nullptr ? 0 : metricNowNs();
    chunk.rowBlock.handle = memory::bm::BufferHandle{};
    chunk.rowBlock.ptr = nullptr;
    blocks.push_back(chunk.rowBlock.block);
    for (auto& block : chunk.heapBlocks) {
      if (metrics != nullptr) {
        ++metrics->heapBlocks;
        ++metrics->totalBlocks;
        metrics->heapBlockBytes += block.size;
        metrics->usedHeapBytes += block.used;
        metrics->unusedHeapTailBytes += block.size - block.used;
      }
      block.handle = memory::bm::BufferHandle{};
      block.ptr = nullptr;
      blocks.push_back(block.block);
    }
    if (metrics != nullptr) {
      metrics->collectBlocksNs += metricNowNs() - collectStart;
    }
  }
  const auto spillStart = metrics == nullptr ? 0 : metricNowNs();
  bufferManager_->SpillBlocks(
      std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
          blocks.data(), blocks.size()));
  if (metrics != nullptr) {
    metrics->spillBlocksNs += metricNowNs() - spillStart;
  }
  segment.meta.state = SegmentState::kFinalizedFlushed;
  return segment.meta.id;
}

SegmentData& BmSegmentCollection::segmentData(SegmentId segment) {
  BOLT_CHECK(
      segment < segments_.size() && segments_[segment] != nullptr,
      "Unknown segment {}",
      segment);
  return *segments_[segment];
}

const SegmentData& BmSegmentCollection::segmentData(SegmentId segment) const {
  BOLT_CHECK(
      segment < segments_.size() && segments_[segment] != nullptr,
      "Unknown segment {}",
      segment);
  return *segments_[segment];
}

} // namespace bytedance::bolt::exec::bm
