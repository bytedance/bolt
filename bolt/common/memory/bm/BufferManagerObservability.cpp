#include "bolt/common/memory/bm/BufferManagerObservability.h"

#include <sstream>

namespace bytedance::bolt::memory::bm {

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

} // namespace bytedance::bolt::memory::bm
