#include "bolt/common/memory/bm/BufferManagerReclaimer.h"

#include "bolt/common/memory/bm/BufferManager.h"

#include <utility>

namespace bytedance::bolt::memory::bm {

BufferManagerReclaimer::BufferManagerReclaimer(
    std::weak_ptr<BufferManager> manager)
    : memory::MemoryReclaimer(0), manager_(std::move(manager)) {}

bool BufferManagerReclaimer::reclaimableBytes(
    const MemoryPool& /*pool*/,
    uint64_t& reclaimableBytes) const {
  auto manager = manager_.lock();
  if (!manager) {
    reclaimableBytes = 0;
    return false;
  }
  reclaimableBytes = manager->reclaimableBytes();
  return reclaimableBytes > 0;
}

uint64_t BufferManagerReclaimer::reclaim(
    MemoryPool* pool,
    uint64_t targetBytes,
    uint64_t maxWaitMs,
    Stats& stats) {
  // v1 intentionally ignores maxWaitMs. Reclaim writes are synchronous and
  // timeout budgeting will be added with the production arbitration policy.
  (void)maxWaitMs;
  BOLT_CHECK_NOT_NULL(pool);
  auto manager = manager_.lock();
  if (!manager) {
    return 0;
  }
  return memory::MemoryReclaimer::run(
      [&]() {
        int64_t reclaimedBytes{0};
        {
          memory::ScopedReclaimedBytesRecorder recorder(pool, &reclaimedBytes);
          manager->Reclaim(targetBytes);
        }
        return reclaimedBytes;
      },
      stats);
}

} // namespace bytedance::bolt::memory::bm
