#pragma once

#include "bolt/common/memory/MemoryArbitrator.h"

#include <memory>

namespace bytedance::bolt::memory::bm {

class BufferManager;

class BufferManagerReclaimer final : public memory::MemoryReclaimer {
 public:
  explicit BufferManagerReclaimer(std::weak_ptr<BufferManager> manager);

  bool reclaimableBytes(const MemoryPool& pool, uint64_t& reclaimableBytes)
      const override;

  uint64_t reclaim(
      MemoryPool* pool,
      uint64_t targetBytes,
      uint64_t maxWaitMs,
      Stats& stats) override;

 private:
  std::weak_ptr<BufferManager> manager_;
};

} // namespace bytedance::bolt::memory::bm
