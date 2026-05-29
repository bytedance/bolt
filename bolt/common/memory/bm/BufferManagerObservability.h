#pragma once

#include "bolt/common/memory/bm/MemoryTag.h"

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

std::vector<BufferManagerTagStats> nonEmptyTagStats(
    const std::vector<BufferManagerTagStats>& tagStats);

std::string toDebugString(
    const BufferManagerStats& stats,
    const std::vector<BufferManagerTagStats>& tagStats);

} // namespace bytedance::bolt::memory::bm
