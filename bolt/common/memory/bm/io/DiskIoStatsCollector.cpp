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

void DiskIoStatsCollector::recordShutdownRejected(
    DiskIoSchedulerStats& stats) {
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
  stats.averageQueueWaitUs =
      static_cast<double>(stats.cumulativeQueueWaitUs) /
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

} // namespace bytedance::bolt::memory::bm
