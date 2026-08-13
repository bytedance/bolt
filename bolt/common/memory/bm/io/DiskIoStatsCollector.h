#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "bolt/common/memory/bm/io/DiskIoSchedulerStats.h"
#include "bolt/common/memory/bm/io/IoPriority.h"
#include "bolt/common/memory/bm/io/IoResult.h"

namespace bytedance::bolt::memory::bm {

class DiskIoStatsCollector {
 public:
  static void recordQueuedSnapshot(
      DiskIoSchedulerStats& stats,
      const std::array<uint64_t, kIoPriorityCount>& queuedCounts);
  static void recordMaxQueueDepth(
      DiskIoSchedulerStats& stats,
      uint64_t queueDepth);
  static void recordRejected(DiskIoSchedulerStats& stats);
  static void recordShutdownRejected(DiskIoSchedulerStats& stats);
  static void recordAccepted(DiskIoSchedulerStats& stats);
  static void recordSubmitBatch(DiskIoSchedulerStats& stats, size_t batchSize);
  static void recordSubmitted(
      DiskIoSchedulerStats& stats,
      IoPriority priority,
      uint64_t queueWaitUs,
      size_t inflightSize);
  static void recordBackendSubmitFailed(
      DiskIoSchedulerStats& stats,
      IoPriority priority);
  static void recordCompletionBatch(
      DiskIoSchedulerStats& stats,
      size_t batchSize);
  static void recordCompletion(
      DiskIoSchedulerStats& stats,
      IoPriority priority,
      const IoResult& result,
      uint64_t deviceLatencyUs,
      uint64_t endToEndLatencyUs,
      size_t inflightSize);
  static void recordQueuedShutdown(
      DiskIoSchedulerStats& stats,
      IoPriority priority);
  static void recordBackendReap(
      DiskIoSchedulerStats& stats,
      uint64_t durationUs);
  static void recordBackendSubmit(
      DiskIoSchedulerStats& stats,
      uint64_t durationUs);
  static void recordWorkerWait(
      DiskIoSchedulerStats& stats,
      uint64_t durationUs);
  static void recordFutureFulfill(
      DiskIoSchedulerStats& stats,
      uint64_t durationUs,
      size_t batchSize);
};

} // namespace bytedance::bolt::memory::bm
