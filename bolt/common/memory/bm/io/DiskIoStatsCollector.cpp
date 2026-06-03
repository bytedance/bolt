#include "bolt/common/memory/bm/io/DiskIoStatsCollector.h"

#include <algorithm>

namespace bytedance::bolt::memory::bm {

void DiskIoStatsCollector::recordQueuedSnapshot(
    DiskIoSchedulerStats& stats,
    const std::array<uint64_t, kIoPriorityCount>& queuedCounts) {
  stats.queuedRequests = queuedCounts;
}

void DiskIoStatsCollector::recordMaxQueueDepth(
    DiskIoSchedulerStats& stats,
    uint64_t queueDepth) {
  stats.maxObservedQueueDepth =
      std::max(stats.maxObservedQueueDepth, queueDepth);
}

void DiskIoStatsCollector::recordRejected(DiskIoSchedulerStats& stats) {
  ++stats.rejectedRequests;
}

void DiskIoStatsCollector::recordShutdownRejected(DiskIoSchedulerStats& stats) {
  ++stats.shutdownRejectedRequests;
}

void DiskIoStatsCollector::recordAccepted(DiskIoSchedulerStats& stats) {
  ++stats.acceptedRequests;
}

void DiskIoStatsCollector::recordSubmitBatch(
    DiskIoSchedulerStats& stats,
    size_t batchSize) {
  if (batchSize == 0) {
    return;
  }
  ++stats.submitBatches;
  stats.submittedRequestsInBatches += batchSize;
  stats.maxSubmitBatchSize =
      std::max<uint64_t>(stats.maxSubmitBatchSize, batchSize);
  stats.averageSubmitBatchSize =
      static_cast<double>(stats.submittedRequestsInBatches) /
      static_cast<double>(stats.submitBatches);
}

void DiskIoStatsCollector::recordSubmitted(
    DiskIoSchedulerStats& stats,
    IoPriority priority,
    uint64_t queueWaitUs,
    size_t inflightSize) {
  ++stats.submittedRequests[priorityIndex(priority)];
  stats.cumulativeQueueWaitUs += queueWaitUs;
  ++stats.queueWaitSamples;
  stats.averageQueueWaitUs = static_cast<double>(stats.cumulativeQueueWaitUs) /
      static_cast<double>(stats.queueWaitSamples);
  stats.maxQueueWaitUs = std::max(stats.maxQueueWaitUs, queueWaitUs);
  stats.inflightRequests = inflightSize;
  stats.maxObservedInflightRequests =
      std::max<uint64_t>(stats.maxObservedInflightRequests, inflightSize);
}

void DiskIoStatsCollector::recordBackendSubmitFailed(
    DiskIoSchedulerStats& stats,
    IoPriority priority) {
  const auto priorityIdx = priorityIndex(priority);
  ++stats.completedRequests;
  ++stats.completedRequestsByPriority[priorityIdx];
  ++stats.failedRequests;
  ++stats.failedRequestsByPriority[priorityIdx];
  ++stats.backendSubmitFailedRequests;
}

void DiskIoStatsCollector::recordCompletionBatch(
    DiskIoSchedulerStats& stats,
    size_t batchSize) {
  if (batchSize == 0) {
    return;
  }
  ++stats.completionBatches;
  stats.completedRequestsInBatches += batchSize;
  stats.maxCompletionBatchSize =
      std::max<uint64_t>(stats.maxCompletionBatchSize, batchSize);
  stats.averageCompletionBatchSize =
      static_cast<double>(stats.completedRequestsInBatches) /
      static_cast<double>(stats.completionBatches);
}

void DiskIoStatsCollector::recordCompletion(
    DiskIoSchedulerStats& stats,
    IoPriority priority,
    const IoResult& result,
    uint64_t deviceLatencyUs,
    uint64_t endToEndLatencyUs,
    size_t inflightSize) {
  const auto priorityIdx = priorityIndex(priority);
  const auto firstLatencySample = stats.latencySamples == 0;

  stats.cumulativeDeviceLatencyUs += deviceLatencyUs;
  stats.cumulativeEndToEndLatencyUs += endToEndLatencyUs;
  ++stats.latencySamples;
  ++stats.completedRequests;
  ++stats.completedRequestsByPriority[priorityIdx];
  stats.completedBytes += result.bytes;
  stats.completedBytesByPriority[priorityIdx] += result.bytes;
  if (result.ok()) {
    ++stats.successfulRequests;
    ++stats.successfulRequestsByPriority[priorityIdx];
  } else {
    ++stats.failedRequests;
    ++stats.failedRequestsByPriority[priorityIdx];
    if (result.error == IoErrorCode::BackendIoError) {
      ++stats.backendIoErrorRequests;
    }
  }

  stats.inflightRequests = inflightSize;
  stats.averageDeviceLatencyUs =
      static_cast<double>(stats.cumulativeDeviceLatencyUs) /
      static_cast<double>(stats.latencySamples);
  stats.averageEndToEndLatencyUs =
      static_cast<double>(stats.cumulativeEndToEndLatencyUs) /
      static_cast<double>(stats.latencySamples);
  if (firstLatencySample || deviceLatencyUs < stats.minLatencyUs) {
    stats.minLatencyUs = deviceLatencyUs;
  }
  stats.maxLatencyUs = std::max(stats.maxLatencyUs, deviceLatencyUs);
  stats.maxEndToEndLatencyUs =
      std::max(stats.maxEndToEndLatencyUs, endToEndLatencyUs);
}

void DiskIoStatsCollector::recordQueuedShutdown(
    DiskIoSchedulerStats& stats,
    IoPriority priority) {
  const auto priorityIdx = priorityIndex(priority);
  ++stats.completedRequests;
  ++stats.completedRequestsByPriority[priorityIdx];
  ++stats.failedRequests;
  ++stats.failedRequestsByPriority[priorityIdx];
}

void DiskIoStatsCollector::recordBackendReap(
    DiskIoSchedulerStats& stats,
    uint64_t durationUs) {
  ++stats.backendReapCalls;
  stats.cumulativeBackendReapUs += durationUs;
  stats.averageBackendReapUs =
      static_cast<double>(stats.cumulativeBackendReapUs) /
      static_cast<double>(stats.backendReapCalls);
  stats.maxBackendReapUs = std::max(stats.maxBackendReapUs, durationUs);
}

void DiskIoStatsCollector::recordBackendSubmit(
    DiskIoSchedulerStats& stats,
    uint64_t durationUs) {
  ++stats.backendSubmitCalls;
  stats.cumulativeBackendSubmitUs += durationUs;
  stats.averageBackendSubmitUs =
      static_cast<double>(stats.cumulativeBackendSubmitUs) /
      static_cast<double>(stats.backendSubmitCalls);
  stats.maxBackendSubmitUs = std::max(stats.maxBackendSubmitUs, durationUs);
}

void DiskIoStatsCollector::recordWorkerWait(
    DiskIoSchedulerStats& stats,
    uint64_t durationUs) {
  ++stats.workerWaitCalls;
  stats.cumulativeWorkerWaitUs += durationUs;
  stats.averageWorkerWaitUs =
      static_cast<double>(stats.cumulativeWorkerWaitUs) /
      static_cast<double>(stats.workerWaitCalls);
  stats.maxWorkerWaitUs = std::max(stats.maxWorkerWaitUs, durationUs);
}

void DiskIoStatsCollector::recordFutureFulfill(
    DiskIoSchedulerStats& stats,
    uint64_t durationUs,
    size_t batchSize) {
  if (batchSize == 0) {
    return;
  }
  ++stats.futureFulfillBatches;
  stats.fulfilledResults += batchSize;
  stats.cumulativeFutureFulfillUs += durationUs;
  stats.averageFutureFulfillUs =
      static_cast<double>(stats.cumulativeFutureFulfillUs) /
      static_cast<double>(stats.futureFulfillBatches);
  stats.averageFutureFulfillBatchSize =
      static_cast<double>(stats.fulfilledResults) /
      static_cast<double>(stats.futureFulfillBatches);
  stats.maxFutureFulfillUs = std::max(stats.maxFutureFulfillUs, durationUs);
  stats.maxFutureFulfillBatchSize =
      std::max<uint64_t>(stats.maxFutureFulfillBatchSize, batchSize);
}

} // namespace bytedance::bolt::memory::bm
