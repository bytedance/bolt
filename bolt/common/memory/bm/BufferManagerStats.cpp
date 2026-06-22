#include "bolt/common/memory/bm/BufferManagerStats.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BlockMemory.h"
#include "bolt/common/memory/bm/SpillStore.h"

#include <glog/logging.h>

#include <sstream>

namespace bytedance::bolt::memory::bm {
namespace {

constexpr std::array<MemoryTag, kMemoryTagCount> kMemoryTags{
    MemoryTag::kUnknown,
    MemoryTag::kHashBuild,
    MemoryTag::kAggregation,
    MemoryTag::kSort,
    MemoryTag::kWindow,
    MemoryTag::kExchange,
    MemoryTag::kTesting};

void SubtractOrSaturate(
    uint64_t& value,
    uint64_t delta,
    const char* field,
    const BlockMemory& memory) noexcept {
#ifndef NDEBUG
  BOLT_DCHECK_GE(
      value,
      delta,
      "BM observability counter underflow, field={}, value={}, delta={}, block_id={}, tag={}, size={}, state={}, pin_count={}",
      field,
      value,
      delta,
      memory.id,
      toString(memory.tag),
      memory.size,
      static_cast<int>(memory.state),
      memory.pinCount);
#endif
  if (value < delta) {
    BOLT_MEM_LOG_EVERY_MS(WARNING, 1000)
        << "[BM] observability counter underflow"
        << " field=" << field << " value=" << value << " delta=" << delta
        << " block_id=" << memory.id << " tag=" << toString(memory.tag)
        << " size=" << memory.size
        << " state=" << static_cast<int>(memory.state)
        << " pin_count=" << memory.pinCount;
    value = 0;
    return;
  }
  value -= delta;
}

} // namespace

std::vector<BufferManagerTagStats> nonEmptyTagStats(
    const std::vector<BufferManagerTagStats>& tagStats) {
  std::vector<BufferManagerTagStats> result;
  for (const auto& stats : tagStats) {
    if (stats.allocatedBlocks > 0 || stats.liveBlocks > 0 ||
        stats.residentBytes > 0 || stats.spilledBytes > 0 ||
        stats.reclaimedBytes > 0 || stats.pinCount > 0 ||
        stats.spillWriteCount > 0 || stats.spillReadCount > 0) {
      result.push_back(stats);
    }
  }
  return result;
}

std::string toDebugString(
    const BufferManagerStats& stats,
    const std::vector<BufferManagerTagStats>& tagStats) {
  std::ostringstream out;
  out << "BufferManagerStats{"
      << "allocated_blocks=" << stats.allocatedBlocks
      << ", live_blocks=" << stats.liveBlocks
      << ", pinned_resident_bytes=" << stats.pinnedResidentBytes
      << ", unpinned_resident_bytes=" << stats.unpinnedResidentBytes
      << ", spilled_bytes=" << stats.spilledBytes
      << ", prefetching_bytes=" << stats.prefetchingBytes
      << ", spilling_bytes=" << stats.spillingBytes
      << ", reclaimed_bytes=" << stats.reclaimedBytes
      << ", pin_count=" << stats.pinCount
      << ", pin_in_memory_count=" << stats.pinInMemoryCount
      << ", pin_read_count=" << stats.pinReadCount
      << ", batch_pin_count=" << stats.batchPinCount
      << ", prefetch_count=" << stats.prefetchCount
      << ", reclaim_count=" << stats.reclaimCount
      << ", reclaim_attempted_blocks=" << stats.reclaimAttemptedBlocks
      << ", reclaim_skipped_blocks=" << stats.reclaimSkippedBlocks
      << ", spill_write_count=" << stats.spillWriteCount
      << ", spill_read_count=" << stats.spillReadCount
      << ", spill_write_bytes=" << stats.spillWriteBytes
      << ", spill_read_bytes=" << stats.spillReadBytes
      << ", spill_physical_write_bytes=" << stats.spillPhysicalWriteBytes
      << ", spill_physical_read_bytes=" << stats.spillPhysicalReadBytes
      << ", spill_io_write_bytes=" << stats.spillIoWriteBytes
      << ", spill_io_read_bytes=" << stats.spillIoReadBytes
      << ", spill_io_padding_write_bytes=" << stats.spillIoPaddingWriteBytes
      << ", spill_compressed_blocks=" << stats.spillCompressedBlocks
      << ", spill_compression_time_us=" << stats.spillCompressionTimeUs
      << ", spill_decompression_time_us=" << stats.spillDecompressionTimeUs
      << ", file_allocate_failures=" << stats.fileAllocateFailures
      << ", file_free_failures=" << stats.fileFreeFailures
      << ", read_io_failures=" << stats.readIoFailures
      << ", write_io_failures=" << stats.writeIoFailures
      << ", prefetch_submit_failures=" << stats.prefetchSubmitFailures
      << ", prefetch_io_failures=" << stats.prefetchIoFailures
      << ", eviction_queue_size=" << stats.evictionQueueSize
      << ", eviction_queue_stale_entries=" << stats.evictionQueueStaleEntries
      << "}";

  const auto nonEmpty = nonEmptyTagStats(tagStats);
  if (!nonEmpty.empty()) {
    out << " tags=[";
    for (size_t i = 0; i < nonEmpty.size(); ++i) {
      const auto& tag = nonEmpty[i];
      if (i > 0) {
        out << ", ";
      }
      out << "{tag=" << toString(tag.tag)
          << ", allocated_blocks=" << tag.allocatedBlocks
          << ", live_blocks=" << tag.liveBlocks
          << ", resident_bytes=" << tag.residentBytes
          << ", pinned_resident_bytes=" << tag.pinnedResidentBytes
          << ", unpinned_resident_bytes=" << tag.unpinnedResidentBytes
          << ", spilled_bytes=" << tag.spilledBytes
          << ", reclaimed_bytes=" << tag.reclaimedBytes
          << ", pin_count=" << tag.pinCount
          << ", spill_write_count=" << tag.spillWriteCount
          << ", spill_read_count=" << tag.spillReadCount << "}";
    }
    out << "]";
  }
  return out.str();
}

BufferManagerStatsCollector::BufferManagerStatsCollector() {
  for (size_t i = 0; i < kMemoryTags.size(); ++i) {
    tagStats_[i].tag = kMemoryTags[i];
  }
}

uint64_t BufferManagerStatsCollector::reclaimableBytes() const {
  // The current BM threading contract serializes reclaimer calls with API
  // calls. If that changes, this field must become atomic or be protected.
  return stats_.unpinnedResidentBytes;
}

BufferManagerStats BufferManagerStatsCollector::stats() const {
  return stats_;
}

std::vector<BufferManagerTagStats> BufferManagerStatsCollector::tagStats()
    const {
  return nonEmptyTagStats(allTagStats());
}

std::vector<BufferManagerTagStats> BufferManagerStatsCollector::allTagStats()
    const {
  return std::vector<BufferManagerTagStats>{tagStats_.begin(), tagStats_.end()};
}

void BufferManagerStatsCollector::RecordAllocate(const BlockMemory& memory) {
  ++stats_.allocatedBlocks;
  ++stats_.liveBlocks;
  stats_.pinnedResidentBytes += memory.size;

  auto& tagStats = MutableTagStats(memory.tag);
  ++tagStats.allocatedBlocks;
  ++tagStats.liveBlocks;
  tagStats.residentBytes += memory.size;
  tagStats.pinnedResidentBytes += memory.size;
}

void BufferManagerStatsCollector::RecordPinRequest(MemoryTag tag) {
  ++stats_.pinCount;
  ++MutableTagStats(tag).pinCount;
}

void BufferManagerStatsCollector::RecordPinInMemory() {
  ++stats_.pinInMemoryCount;
}

void BufferManagerStatsCollector::RecordBatchPin() {
  ++stats_.batchPinCount;
}

void BufferManagerStatsCollector::RecordPrefetch() {
  ++stats_.prefetchCount;
}

void BufferManagerStatsCollector::RecordPrefetchSubmitFailure() {
  ++stats_.prefetchSubmitFailures;
}

void BufferManagerStatsCollector::RecordReclaim() {
  ++stats_.reclaimCount;
}

void BufferManagerStatsCollector::RecordReclaimAttemptedBlock() {
  ++stats_.reclaimAttemptedBlocks;
}

void BufferManagerStatsCollector::RecordReclaimedBytes(uint64_t bytes) {
  stats_.reclaimedBytes += bytes;
}

void BufferManagerStatsCollector::RecordWriteIoFailure() {
  ++stats_.writeIoFailures;
}

void BufferManagerStatsCollector::RecordReadIoFailure() {
  ++stats_.prefetchIoFailures;
  ++stats_.readIoFailures;
}

void BufferManagerStatsCollector::OnResidentPinned(const BlockMemory& memory) {
  if (memory.pinCount != 0) {
    return;
  }
  SubtractOrSaturate(
      stats_.unpinnedResidentBytes,
      memory.size,
      "unpinnedResidentBytes",
      memory);
  stats_.pinnedResidentBytes += memory.size;

  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrSaturate(
      tagStats.unpinnedResidentBytes,
      memory.size,
      "tag.unpinnedResidentBytes",
      memory);
  tagStats.pinnedResidentBytes += memory.size;
}

void BufferManagerStatsCollector::OnResidentUnpinned(
    const BlockMemory& memory) noexcept {
  SubtractOrSaturate(
      stats_.pinnedResidentBytes, memory.size, "pinnedResidentBytes", memory);
  stats_.unpinnedResidentBytes += memory.size;

  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrSaturate(
      tagStats.pinnedResidentBytes,
      memory.size,
      "tag.pinnedResidentBytes",
      memory);
  tagStats.unpinnedResidentBytes += memory.size;
}

void BufferManagerStatsCollector::OnReadSubmitted(const BlockMemory& memory) {
  stats_.prefetchingBytes += memory.size;
  MutableTagStats(memory.tag).prefetchingBytes += memory.size;
}

void BufferManagerStatsCollector::OnReadFutureConsumed(
    const BlockMemory& memory) {
  SubtractOrSaturate(
      stats_.prefetchingBytes, memory.size, "prefetchingBytes", memory);
  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrSaturate(
      tagStats.prefetchingBytes, memory.size, "tag.prefetchingBytes", memory);
}

void BufferManagerStatsCollector::OnReadCompleted(
    const BlockMemory& memory,
    const SpillReadResult& read) {
  SubtractOrSaturate(stats_.spilledBytes, memory.size, "spilledBytes", memory);
  stats_.pinnedResidentBytes += memory.size;
  ++stats_.pinReadCount;
  ++stats_.spillReadCount;
  stats_.spillReadBytes += memory.size;
  stats_.spillPhysicalReadBytes += read.physicalBytes;
  stats_.spillIoReadBytes +=
      read.ioBytes == 0 ? read.physicalBytes : read.ioBytes;
  stats_.spillDecompressionTimeUs += read.decompressionTimeUs;

  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrSaturate(
      tagStats.spilledBytes, memory.size, "tag.spilledBytes", memory);
  tagStats.residentBytes += memory.size;
  tagStats.pinnedResidentBytes += memory.size;
  ++tagStats.spillReadCount;
}

void BufferManagerStatsCollector::OnSpillStarted(const BlockMemory& memory) {
  SubtractOrSaturate(
      stats_.unpinnedResidentBytes,
      memory.size,
      "unpinnedResidentBytes",
      memory);
  stats_.spillingBytes += memory.size;

  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrSaturate(
      tagStats.residentBytes, memory.size, "tag.residentBytes", memory);
  SubtractOrSaturate(
      tagStats.unpinnedResidentBytes,
      memory.size,
      "tag.unpinnedResidentBytes",
      memory);
  tagStats.spillingBytes += memory.size;
}

void BufferManagerStatsCollector::OnSpillRolledBack(const BlockMemory& memory) {
  SubtractOrSaturate(stats_.spillingBytes, memory.size, "spillingBytes", memory);
  stats_.unpinnedResidentBytes += memory.size;

  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrSaturate(
      tagStats.spillingBytes, memory.size, "tag.spillingBytes", memory);
  tagStats.residentBytes += memory.size;
  tagStats.unpinnedResidentBytes += memory.size;
}

void BufferManagerStatsCollector::OnSpillCompleted(
    const BlockMemory& memory,
    const SpillWriteResult& write) {
  const auto ioBytes = write.ioBytes == 0 ? write.physicalBytes : write.ioBytes;
  stats_.spillPhysicalWriteBytes += write.physicalBytes;
  stats_.spillIoWriteBytes += ioBytes;
  if (ioBytes > write.physicalBytes) {
    stats_.spillIoPaddingWriteBytes += ioBytes - write.physicalBytes;
  }
  stats_.spillCompressionTimeUs += write.compressionTimeUs;
  if (write.compressed) {
    ++stats_.spillCompressedBlocks;
  }

  SubtractOrSaturate(stats_.spillingBytes, memory.size, "spillingBytes", memory);
  stats_.spilledBytes += memory.size;
  ++stats_.spillWriteCount;
  stats_.spillWriteBytes += memory.size;

  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrSaturate(
      tagStats.spillingBytes, memory.size, "tag.spillingBytes", memory);
  tagStats.spilledBytes += memory.size;
  tagStats.reclaimedBytes += memory.size;
  ++tagStats.spillWriteCount;
}

void BufferManagerStatsCollector::OnCleanResidentDiscarded(
    const BlockMemory& memory) {
  SubtractOrSaturate(
      stats_.unpinnedResidentBytes,
      memory.size,
      "unpinnedResidentBytes",
      memory);
  stats_.spilledBytes += memory.size;

  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrSaturate(
      tagStats.residentBytes, memory.size, "tag.residentBytes", memory);
  SubtractOrSaturate(
      tagStats.unpinnedResidentBytes,
      memory.size,
      "tag.unpinnedResidentBytes",
      memory);
  tagStats.spilledBytes += memory.size;
  tagStats.reclaimedBytes += memory.size;
}

void BufferManagerStatsCollector::OnBlockMemoryDestroy(
    const BlockMemory& memory) noexcept {
  SubtractOrSaturate(stats_.liveBlocks, 1, "liveBlocks", memory);
  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrSaturate(tagStats.liveBlocks, 1, "tag.liveBlocks", memory);

  switch (memory.state) {
    case BlockMemoryState::kInMemory:
      if (memory.pinCount == 0) {
        SubtractOrSaturate(
            stats_.unpinnedResidentBytes,
            memory.size,
            "unpinnedResidentBytes",
            memory);
        SubtractOrSaturate(
            tagStats.unpinnedResidentBytes,
            memory.size,
            "tag.unpinnedResidentBytes",
            memory);
      } else {
        SubtractOrSaturate(
            stats_.pinnedResidentBytes,
            memory.size,
            "pinnedResidentBytes",
            memory);
        SubtractOrSaturate(
            tagStats.pinnedResidentBytes,
            memory.size,
            "tag.pinnedResidentBytes",
            memory);
      }
      SubtractOrSaturate(
          tagStats.residentBytes, memory.size, "tag.residentBytes", memory);
      break;
    case BlockMemoryState::kSpilled:
      SubtractOrSaturate(stats_.spilledBytes, memory.size, "spilledBytes", memory);
      SubtractOrSaturate(
          tagStats.spilledBytes, memory.size, "tag.spilledBytes", memory);
      break;
    case BlockMemoryState::kPrefetching:
      SubtractOrSaturate(stats_.spilledBytes, memory.size, "spilledBytes", memory);
      SubtractOrSaturate(
          stats_.prefetchingBytes, memory.size, "prefetchingBytes", memory);
      SubtractOrSaturate(
          tagStats.spilledBytes, memory.size, "tag.spilledBytes", memory);
      SubtractOrSaturate(
          tagStats.prefetchingBytes,
          memory.size,
          "tag.prefetchingBytes",
          memory);
      break;
    case BlockMemoryState::kSpilling:
      SubtractOrSaturate(
          stats_.spillingBytes, memory.size, "spillingBytes", memory);
      SubtractOrSaturate(
          tagStats.spillingBytes, memory.size, "tag.spillingBytes", memory);
      break;
  }
}

BufferManagerTagStats& BufferManagerStatsCollector::MutableTagStats(
    MemoryTag tag) {
  const auto index = static_cast<size_t>(tag);
  if (index < tagStats_.size() && tagStats_[index].tag == tag) {
    return tagStats_[index];
  }
  return tagStats_[static_cast<size_t>(MemoryTag::kUnknown)];
}

} // namespace bytedance::bolt::memory::bm
