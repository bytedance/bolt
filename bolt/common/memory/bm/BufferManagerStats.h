#pragma once

#include "bolt/common/memory/bm/MemoryTag.h"

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace bytedance::bolt::memory::bm {

struct BufferManagerStats {
  uint64_t allocatedBlocks{0};
  uint64_t liveBlocks{0};

  uint64_t pinnedResidentBytes{0};
  uint64_t unpinnedResidentBytes{0};
  uint64_t spilledBytes{0};
  uint64_t prefetchingBytes{0};
  uint64_t spillingBytes{0};
  uint64_t reclaimedBytes{0};

  uint64_t pinCount{0};
  uint64_t pinInMemoryCount{0};
  uint64_t pinReadCount{0};
  uint64_t batchPinCount{0};
  uint64_t prefetchCount{0};

  uint64_t reclaimCount{0};
  uint64_t reclaimAttemptedBlocks{0};
  uint64_t reclaimSkippedBlocks{0};

  uint64_t spillWriteCount{0};
  uint64_t spillReadCount{0};
  uint64_t spillWriteBytes{0};
  uint64_t spillReadBytes{0};
  uint64_t spillPhysicalWriteBytes{0};
  uint64_t spillPhysicalReadBytes{0};
  uint64_t spillCompressedBlocks{0};
  uint64_t spillCompressionTimeUs{0};
  uint64_t spillDecompressionTimeUs{0};

  uint64_t fileAllocateFailures{0};
  uint64_t fileFreeFailures{0};
  uint64_t readIoFailures{0};
  uint64_t writeIoFailures{0};
  uint64_t prefetchSubmitFailures{0};
  uint64_t prefetchIoFailures{0};

  uint64_t evictionQueueSize{0};
  uint64_t evictionQueueStaleEntries{0};
};

struct BufferManagerTagStats {
  MemoryTag tag{MemoryTag::kUnknown};
  uint64_t allocatedBlocks{0};
  uint64_t liveBlocks{0};

  uint64_t residentBytes{0};
  uint64_t pinnedResidentBytes{0};
  uint64_t unpinnedResidentBytes{0};
  uint64_t spilledBytes{0};
  uint64_t prefetchingBytes{0};
  uint64_t spillingBytes{0};
  uint64_t reclaimedBytes{0};

  uint64_t pinCount{0};
  uint64_t spillWriteCount{0};
  uint64_t spillReadCount{0};
};

struct BlockMemory;
struct SpillReadResult;
struct SpillWriteResult;

std::vector<BufferManagerTagStats> nonEmptyTagStats(
    const std::vector<BufferManagerTagStats>& tagStats);

std::string toDebugString(
    const BufferManagerStats& stats,
    const std::vector<BufferManagerTagStats>& tagStats);

class BufferManagerStatsCollector {
 public:
  BufferManagerStatsCollector();

  uint64_t reclaimableBytes() const;
  BufferManagerStats stats() const;
  std::vector<BufferManagerTagStats> tagStats() const;
  std::vector<BufferManagerTagStats> allTagStats() const;

  void RecordAllocate(const BlockMemory& memory);
  void RecordPinRequest(MemoryTag tag);
  void RecordPinInMemory();
  void RecordBatchPin();
  void RecordPrefetch();
  void RecordPrefetchSubmitFailure();
  void RecordReclaim();
  void RecordReclaimAttemptedBlock();
  void RecordReclaimedBytes(uint64_t bytes);
  void RecordWriteIoFailure();
  void RecordReadIoFailure();

  void OnResidentPinned(const BlockMemory& memory);
  void OnResidentUnpinned(const BlockMemory& memory) noexcept;
  void OnReadSubmitted(const BlockMemory& memory);
  void OnReadFutureConsumed(const BlockMemory& memory);
  void OnReadCompleted(const BlockMemory& memory, const SpillReadResult& read);
  void OnSpillStarted(const BlockMemory& memory);
  void OnSpillRolledBack(const BlockMemory& memory);
  void OnSpillCompleted(
      const BlockMemory& memory,
      const SpillWriteResult& write);
  void OnBlockMemoryDestroy(const BlockMemory& memory) noexcept;

 private:
  BufferManagerTagStats& MutableTagStats(MemoryTag tag);

  BufferManagerStats stats_;
  std::array<BufferManagerTagStats, kMemoryTagCount> tagStats_;
};

} // namespace bytedance::bolt::memory::bm
