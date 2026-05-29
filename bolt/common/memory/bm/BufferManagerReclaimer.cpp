#include "bolt/common/memory/bm/BufferManagerReclaimer.h"

#include "bolt/common/memory/bm/BufferManager.h"

#include <glog/logging.h>

#include <utility>

namespace bytedance::bolt::memory::bm {

BufferManagerReclaimer::BufferManagerReclaimer(
    std::weak_ptr<BufferManager> manager)
    : memory::MemoryReclaimer(0), manager_(std::move(manager)) {}

bool BufferManagerReclaimer::reclaimableBytes(
    const MemoryPool& pool,
    uint64_t& reclaimableBytes) const {
  auto manager = manager_.lock();
  if (!manager) {
    reclaimableBytes = 0;
    VLOG(1) << "BM reclaimer reclaimableBytes manager expired"
            << " pool=" << pool.name();
    return false;
  }
  reclaimableBytes = manager->reclaimableBytes();
  VLOG(1) << "BM reclaimer reclaimableBytes"
          << " pool=" << pool.name()
          << " reclaimable_bytes=" << reclaimableBytes
          << " pool_used=" << pool.usedBytes()
          << " pool_current=" << pool.currentBytes()
          << " pool_reserved=" << pool.reservedBytes()
          << " pool_available_reservation=" << pool.availableReservation()
          << " pool_releasable_reservation=" << pool.releasableReservation()
          << " bm=" << manager->debugString();
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
    VLOG(1) << "BM reclaimer reclaim skipped, manager expired"
            << " pool=" << pool->name()
            << " target_bytes=" << targetBytes
            << " max_wait_ms=" << maxWaitMs;
    return 0;
  }
  VLOG(1) << "BM reclaimer reclaim begin"
          << " pool=" << pool->name()
          << " target_bytes=" << targetBytes
          << " max_wait_ms=" << maxWaitMs
          << " pool_used=" << pool->usedBytes()
          << " pool_current=" << pool->currentBytes()
          << " pool_reserved=" << pool->reservedBytes()
          << " pool_available_reservation=" << pool->availableReservation()
          << " pool_releasable_reservation=" << pool->releasableReservation()
          << " bm=" << manager->debugString();
  const auto reclaimedByRecorder = memory::MemoryReclaimer::run(
      [&]() {
        int64_t reclaimedBytes{0};
        uint64_t managerReclaimedBytes{0};
        {
          memory::ScopedReclaimedBytesRecorder recorder(pool, &reclaimedBytes);
          managerReclaimedBytes = manager->Reclaim(targetBytes);
          pool->release();
        }
        VLOG(1) << "BM reclaimer reclaim body end"
                << " pool=" << pool->name()
                << " target_bytes=" << targetBytes
                << " manager_reclaimed_bytes=" << managerReclaimedBytes
                << " recorder_reclaimed_bytes=" << reclaimedBytes
                << " pool_used=" << pool->usedBytes()
                << " pool_current=" << pool->currentBytes()
                << " pool_reserved=" << pool->reservedBytes()
                << " pool_available_reservation=" << pool->availableReservation()
                << " pool_releasable_reservation="
                << pool->releasableReservation()
                << " bm=" << manager->debugString();
        return reclaimedBytes;
      },
      stats);
  VLOG(1) << "BM reclaimer reclaim end"
          << " pool=" << pool->name()
          << " target_bytes=" << targetBytes
          << " returned_bytes=" << reclaimedByRecorder
          << " pool_used=" << pool->usedBytes()
          << " pool_current=" << pool->currentBytes()
          << " pool_reserved=" << pool->reservedBytes()
          << " pool_available_reservation=" << pool->availableReservation()
          << " pool_releasable_reservation=" << pool->releasableReservation()
          << " bm=" << manager->debugString();
  return reclaimedByRecorder;
}

} // namespace bytedance::bolt::memory::bm
