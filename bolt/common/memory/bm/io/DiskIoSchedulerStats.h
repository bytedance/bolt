#pragma once

#include <array>
#include <cstdint>
#include <string>

#include "bolt/common/memory/bm/io/DepthControlStats.h"
#include "bolt/common/memory/bm/io/IoPriority.h"

namespace bytedance::bolt::memory::bm {

struct DiskIoSchedulerStats {
  std::array<uint64_t, kIoPriorityCount> queuedRequests{};
  std::array<uint64_t, kIoPriorityCount> submittedRequests{};
  std::array<uint64_t, kIoPriorityCount> completedRequestsByPriority{};
  std::array<uint64_t, kIoPriorityCount> completedBytesByPriority{};
  std::array<uint64_t, kIoPriorityCount> successfulRequestsByPriority{};
  std::array<uint64_t, kIoPriorityCount> failedRequestsByPriority{};
  uint64_t inflightRequests{0};
  uint64_t acceptedRequests{0};
  uint64_t rejectedRequests{0};
  uint64_t shutdownRejectedRequests{0};
  uint64_t completedRequests{0};
  uint64_t completedBytes{0};
  uint64_t successfulRequests{0};
  uint64_t failedRequests{0};
  uint64_t backendSubmitFailedRequests{0};
  uint64_t backendIoErrorRequests{0};
  uint64_t maxObservedQueueDepth{0};
  uint64_t maxObservedInflightRequests{0};
  uint64_t cumulativeDeviceLatencyUs{0};
  uint64_t latencySamples{0};
  double averageDeviceLatencyUs{0};
  uint64_t minLatencyUs{0};
  uint64_t maxLatencyUs{0};
  uint64_t cumulativeQueueWaitUs{0};
  uint64_t queueWaitSamples{0};
  double averageQueueWaitUs{0};
  uint64_t maxQueueWaitUs{0};
  uint64_t cumulativeEndToEndLatencyUs{0};
  double averageEndToEndLatencyUs{0};
  uint64_t maxEndToEndLatencyUs{0};
  uint64_t submitBatches{0};
  uint64_t submittedRequestsInBatches{0};
  double averageSubmitBatchSize{0};
  uint64_t maxSubmitBatchSize{0};
  uint64_t completionBatches{0};
  uint64_t completedRequestsInBatches{0};
  double averageCompletionBatchSize{0};
  uint64_t maxCompletionBatchSize{0};
  uint64_t backendReapCalls{0};
  uint64_t cumulativeBackendReapUs{0};
  double averageBackendReapUs{0};
  uint64_t maxBackendReapUs{0};
  uint64_t backendSubmitCalls{0};
  uint64_t cumulativeBackendSubmitUs{0};
  double averageBackendSubmitUs{0};
  uint64_t maxBackendSubmitUs{0};
  uint64_t workerWaitCalls{0};
  uint64_t cumulativeWorkerWaitUs{0};
  double averageWorkerWaitUs{0};
  uint64_t maxWorkerWaitUs{0};
  uint64_t futureFulfillBatches{0};
  uint64_t fulfilledResults{0};
  uint64_t cumulativeFutureFulfillUs{0};
  double averageFutureFulfillUs{0};
  double averageFutureFulfillBatchSize{0};
  uint64_t maxFutureFulfillUs{0};
  uint64_t maxFutureFulfillBatchSize{0};
  DepthControlStatsPtr depthControl;

  void deriveMetrics();
  std::string toString() const;
};

} // namespace bytedance::bolt::memory::bm
