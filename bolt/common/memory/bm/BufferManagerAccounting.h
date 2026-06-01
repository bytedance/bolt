#pragma once

#include "bolt/common/memory/bm/BufferManagerObservability.h"

#include <array>
#include <vector>

namespace bytedance::bolt::memory::bm {

struct BlockMemory;
struct SpillReadResult;
struct SpillWriteResult;

class BufferManagerAccounting {
 public:
  BufferManagerAccounting();

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
  void OnSpillCompleted(const BlockMemory& memory, const SpillWriteResult& write);
  void OnBlockMemoryDestroy(const BlockMemory& memory) noexcept;

 private:
  BufferManagerTagStats& MutableTagStats(MemoryTag tag);

  BufferManagerStats stats_;
  std::array<BufferManagerTagStats, kMemoryTagCount> tagStats_;
};

} // namespace bytedance::bolt::memory::bm
