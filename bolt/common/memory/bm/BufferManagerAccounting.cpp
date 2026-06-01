#include "bolt/common/memory/bm/BufferManagerAccounting.h"

#include "bolt/common/memory/bm/BlockMemory.h"
#include "bolt/common/memory/bm/SpillStore.h"

#include <glog/logging.h>

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

void SubtractOrFatal(
    uint64_t& value,
    uint64_t delta,
    const char* field,
    const BlockMemory& memory) noexcept {
  if (value < delta) {
    LOG(FATAL) << "BM observability counter underflow, field=" << field
               << ", value=" << value << ", delta=" << delta
               << ", block_id=" << memory.id
               << ", tag=" << toString(memory.tag)
               << ", size=" << memory.size
               << ", state=" << static_cast<int>(memory.state)
               << ", pin_count=" << memory.pinCount;
  }
  value -= delta;
}

} // namespace

BufferManagerAccounting::BufferManagerAccounting() {
  for (size_t i = 0; i < kMemoryTags.size(); ++i) {
    tagStats_[i].tag = kMemoryTags[i];
  }
}

uint64_t BufferManagerAccounting::reclaimableBytes() const {
  // The current BM threading contract serializes reclaimer calls with API
  // calls. If that changes, this field must become atomic or be protected.
  return stats_.unpinnedResidentBytes;
}

BufferManagerStats BufferManagerAccounting::stats() const {
  return stats_;
}

std::vector<BufferManagerTagStats> BufferManagerAccounting::tagStats() const {
  return nonEmptyTagStats(allTagStats());
}

std::vector<BufferManagerTagStats> BufferManagerAccounting::allTagStats() const {
  return std::vector<BufferManagerTagStats>{tagStats_.begin(), tagStats_.end()};
}

void BufferManagerAccounting::RecordAllocate(const BlockMemory& memory) {
  ++stats_.allocatedBlocks;
  ++stats_.liveBlocks;
  stats_.pinnedResidentBytes += memory.size;

  auto& tagStats = MutableTagStats(memory.tag);
  ++tagStats.allocatedBlocks;
  ++tagStats.liveBlocks;
  tagStats.residentBytes += memory.size;
  tagStats.pinnedResidentBytes += memory.size;
}

void BufferManagerAccounting::RecordPinRequest(MemoryTag tag) {
  ++stats_.pinCount;
  ++MutableTagStats(tag).pinCount;
}

void BufferManagerAccounting::RecordPinInMemory() {
  ++stats_.pinInMemoryCount;
}

void BufferManagerAccounting::RecordBatchPin() {
  ++stats_.batchPinCount;
}

void BufferManagerAccounting::RecordPrefetch() {
  ++stats_.prefetchCount;
}

void BufferManagerAccounting::RecordPrefetchSubmitFailure() {
  ++stats_.prefetchSubmitFailures;
}

void BufferManagerAccounting::RecordReclaim() {
  ++stats_.reclaimCount;
}

void BufferManagerAccounting::RecordReclaimAttemptedBlock() {
  ++stats_.reclaimAttemptedBlocks;
}

void BufferManagerAccounting::RecordReclaimedBytes(uint64_t bytes) {
  stats_.reclaimedBytes += bytes;
}

void BufferManagerAccounting::RecordWriteIoFailure() {
  ++stats_.writeIoFailures;
}

void BufferManagerAccounting::RecordReadIoFailure() {
  ++stats_.prefetchIoFailures;
  ++stats_.readIoFailures;
}

void BufferManagerAccounting::OnResidentPinned(const BlockMemory& memory) {
  if (memory.pinCount != 0) {
    return;
  }
  SubtractOrFatal(
      stats_.unpinnedResidentBytes, memory.size, "unpinnedResidentBytes", memory);
  stats_.pinnedResidentBytes += memory.size;

  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrFatal(
      tagStats.unpinnedResidentBytes,
      memory.size,
      "tag.unpinnedResidentBytes",
      memory);
  tagStats.pinnedResidentBytes += memory.size;
}

void BufferManagerAccounting::OnResidentUnpinned(
    const BlockMemory& memory) noexcept {
  SubtractOrFatal(
      stats_.pinnedResidentBytes, memory.size, "pinnedResidentBytes", memory);
  stats_.unpinnedResidentBytes += memory.size;

  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrFatal(
      tagStats.pinnedResidentBytes,
      memory.size,
      "tag.pinnedResidentBytes",
      memory);
  tagStats.unpinnedResidentBytes += memory.size;
}

void BufferManagerAccounting::OnReadSubmitted(const BlockMemory& memory) {
  stats_.prefetchingBytes += memory.size;
  MutableTagStats(memory.tag).prefetchingBytes += memory.size;
}

void BufferManagerAccounting::OnReadFutureConsumed(const BlockMemory& memory) {
  SubtractOrFatal(
      stats_.prefetchingBytes, memory.size, "prefetchingBytes", memory);
  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrFatal(
      tagStats.prefetchingBytes,
      memory.size,
      "tag.prefetchingBytes",
      memory);
}

void BufferManagerAccounting::OnReadCompleted(
    const BlockMemory& memory,
    const SpillReadResult& read) {
  SubtractOrFatal(stats_.spilledBytes, memory.size, "spilledBytes", memory);
  stats_.pinnedResidentBytes += memory.size;
  ++stats_.pinReadCount;
  ++stats_.spillReadCount;
  stats_.spillReadBytes += memory.size;
  stats_.spillPhysicalReadBytes += read.physicalBytes;
  stats_.spillDecompressionTimeUs += read.decompressionTimeUs;

  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrFatal(
      tagStats.spilledBytes, memory.size, "tag.spilledBytes", memory);
  tagStats.residentBytes += memory.size;
  tagStats.pinnedResidentBytes += memory.size;
  ++tagStats.spillReadCount;
}

void BufferManagerAccounting::OnSpillStarted(const BlockMemory& memory) {
  SubtractOrFatal(
      stats_.unpinnedResidentBytes, memory.size, "unpinnedResidentBytes", memory);
  stats_.spillingBytes += memory.size;

  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrFatal(
      tagStats.residentBytes, memory.size, "tag.residentBytes", memory);
  SubtractOrFatal(
      tagStats.unpinnedResidentBytes,
      memory.size,
      "tag.unpinnedResidentBytes",
      memory);
  tagStats.spillingBytes += memory.size;
}

void BufferManagerAccounting::OnSpillRolledBack(const BlockMemory& memory) {
  SubtractOrFatal(stats_.spillingBytes, memory.size, "spillingBytes", memory);
  stats_.unpinnedResidentBytes += memory.size;

  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrFatal(
      tagStats.spillingBytes, memory.size, "tag.spillingBytes", memory);
  tagStats.residentBytes += memory.size;
  tagStats.unpinnedResidentBytes += memory.size;
}

void BufferManagerAccounting::OnSpillCompleted(
    const BlockMemory& memory,
    const SpillWriteResult& write) {
  stats_.spillPhysicalWriteBytes += write.physicalBytes;
  stats_.spillCompressionTimeUs += write.compressionTimeUs;
  if (write.compressed) {
    ++stats_.spillCompressedBlocks;
  }

  SubtractOrFatal(stats_.spillingBytes, memory.size, "spillingBytes", memory);
  stats_.spilledBytes += memory.size;
  ++stats_.spillWriteCount;
  stats_.spillWriteBytes += memory.size;

  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrFatal(
      tagStats.spillingBytes, memory.size, "tag.spillingBytes", memory);
  tagStats.spilledBytes += memory.size;
  tagStats.reclaimedBytes += memory.size;
  ++tagStats.spillWriteCount;
}

void BufferManagerAccounting::OnBlockMemoryDestroy(
    const BlockMemory& memory) noexcept {
  SubtractOrFatal(stats_.liveBlocks, 1, "liveBlocks", memory);
  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrFatal(tagStats.liveBlocks, 1, "tag.liveBlocks", memory);

  switch (memory.state) {
    case BlockMemoryState::kInMemory:
      if (memory.pinCount == 0) {
        SubtractOrFatal(
            stats_.unpinnedResidentBytes,
            memory.size,
            "unpinnedResidentBytes",
            memory);
        SubtractOrFatal(
            tagStats.unpinnedResidentBytes,
            memory.size,
            "tag.unpinnedResidentBytes",
            memory);
      } else {
        SubtractOrFatal(
            stats_.pinnedResidentBytes,
            memory.size,
            "pinnedResidentBytes",
            memory);
        SubtractOrFatal(
            tagStats.pinnedResidentBytes,
            memory.size,
            "tag.pinnedResidentBytes",
            memory);
      }
      SubtractOrFatal(
          tagStats.residentBytes, memory.size, "tag.residentBytes", memory);
      break;
    case BlockMemoryState::kSpilled:
      SubtractOrFatal(stats_.spilledBytes, memory.size, "spilledBytes", memory);
      SubtractOrFatal(
          tagStats.spilledBytes, memory.size, "tag.spilledBytes", memory);
      break;
    case BlockMemoryState::kPrefetching:
      SubtractOrFatal(stats_.spilledBytes, memory.size, "spilledBytes", memory);
      SubtractOrFatal(
          stats_.prefetchingBytes, memory.size, "prefetchingBytes", memory);
      SubtractOrFatal(
          tagStats.spilledBytes, memory.size, "tag.spilledBytes", memory);
      SubtractOrFatal(
          tagStats.prefetchingBytes,
          memory.size,
          "tag.prefetchingBytes",
          memory);
      break;
    case BlockMemoryState::kSpilling:
      SubtractOrFatal(
          stats_.spillingBytes, memory.size, "spillingBytes", memory);
      SubtractOrFatal(
          tagStats.spillingBytes, memory.size, "tag.spillingBytes", memory);
      break;
  }
}

BufferManagerTagStats& BufferManagerAccounting::MutableTagStats(MemoryTag tag) {
  const auto index = static_cast<size_t>(tag);
  if (index < tagStats_.size() && tagStats_[index].tag == tag) {
    return tagStats_[index];
  }
  return tagStats_[static_cast<size_t>(MemoryTag::kUnknown)];
}

} // namespace bytedance::bolt::memory::bm
