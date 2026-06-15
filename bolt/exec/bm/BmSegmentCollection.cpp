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

} // namespace

const std::vector<SegmentId> BmSegmentCollection::kEmptySegments_{};

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
      heapBlockSize_(heapBlockSize) {
  BOLT_CHECK_NOT_NULL(bufferManager_);
  BOLT_CHECK_NOT_NULL(layout_);
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
  auto it = segments_.find(segment);
  if (it == segments_.end()) {
    return;
  }
  if (it->second.meta.partitionId.has_value()) {
    const auto partition = *it->second.meta.partitionId;
    auto activeIt = activeSegments_.find(partition);
    if (activeIt != activeSegments_.end() && activeIt->second == segment) {
      activeSegments_.erase(activeIt);
    }
    auto partitionIt = partitionSegments_.find(partition);
    if (partitionIt != partitionSegments_.end()) {
      auto& segments = partitionIt->second;
      segments.erase(
          std::remove(segments.begin(), segments.end(), segment),
          segments.end());
    }
  }
  segments_.erase(it);
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
  auto it = partitionSegments_.find(partition);
  if (it == partitionSegments_.end()) {
    return kEmptySegments_;
  }
  return it->second;
}

std::vector<SegmentId> BmSegmentCollection::allSegmentIds() const {
  std::vector<SegmentId> ids;
  ids.reserve(segments_.size());
  for (const auto& [id, _] : segments_) {
    ids.push_back(id);
  }
  return ids;
}

int64_t BmSegmentCollection::numRows() const {
  int64_t rows = 0;
  for (const auto& [_, segment] : segments_) {
    rows += segment.meta.numRows;
  }
  return rows;
}

SegmentData& BmSegmentCollection::activeSegment(PartitionId partition) {
  auto it = activeSegments_.find(partition);
  if (it != activeSegments_.end()) {
    return segmentData(it->second);
  }

  auto& segment = createSegment(partition);
  activeSegments_[partition] = segment.meta.id;
  return segment;
}

SegmentData& BmSegmentCollection::createSegment(
    std::optional<PartitionId> partition) {
  SegmentData segment;
  segment.meta.id = nextSegmentId_++;
  segment.meta.state = SegmentState::kActiveResident;
  segment.meta.partitionId = std::move(partition);
  const auto id = segment.meta.id;
  auto [inserted, _] = segments_.emplace(id, std::move(segment));
  return inserted->second;
}

SegmentId BmSegmentCollection::finalizeAndFlush(
    PartitionId partition,
    BmSegmentSpillMetrics* metrics) {
  auto active = activeSegments_.find(partition);
  BOLT_CHECK(active != activeSegments_.end());
  auto& segment = segmentData(active->second);
  const auto id = finalizeAndFlushSegment(segment, metrics);
  partitionSegments_[partition].push_back(id);
  activeSegments_.erase(active);
  return id;
}

SegmentId BmSegmentCollection::finalizeAndFlushSegment(
    SegmentData& segment,
    BmSegmentSpillMetrics* metrics) {
  BOLT_DCHECK(segment.meta.state == SegmentState::kActiveResident);
  segment.meta.state = SegmentState::kFinalizedResident;

  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  for (auto& chunk : segment.chunks) {
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
  auto it = segments_.find(segment);
  BOLT_CHECK(it != segments_.end(), "Unknown segment {}", segment);
  return it->second;
}

const SegmentData& BmSegmentCollection::segmentData(SegmentId segment) const {
  auto it = segments_.find(segment);
  BOLT_CHECK(it != segments_.end(), "Unknown segment {}", segment);
  return it->second;
}

} // namespace bytedance::bolt::exec::bm
