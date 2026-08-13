#include "bolt/common/memory/bm/io/DiskIoSchedulerStats.h"

#include <sstream>

namespace bytedance::bolt::memory::bm {
namespace {

std::string formatArray(const std::array<uint64_t, kIoPriorityCount>& values) {
  std::ostringstream out;
  out << "[high=" << values[priorityIndex(IoPriority::High)]
      << ",medium=" << values[priorityIndex(IoPriority::Medium)]
      << ",low=" << values[priorityIndex(IoPriority::Low)] << "]";
  return out.str();
}

double average(uint64_t total, uint64_t count) {
  if (count == 0) {
    return 0;
  }
  return static_cast<double>(total) / static_cast<double>(count);
}

} // namespace

void DiskIoSchedulerStats::deriveMetrics() {
  averageDeviceLatencyUs =
      average(cumulativeDeviceLatencyUs, latencySamples);
  averageQueueWaitUs = average(cumulativeQueueWaitUs, queueWaitSamples);
  averageEndToEndLatencyUs =
      average(cumulativeEndToEndLatencyUs, latencySamples);
  averageSubmitBatchSize =
      average(submittedRequestsInBatches, submitBatches);
  averageCompletionBatchSize =
      average(completedRequestsInBatches, completionBatches);
  averageBackendReapUs =
      average(cumulativeBackendReapUs, backendReapCalls);
  averageBackendSubmitUs =
      average(cumulativeBackendSubmitUs, backendSubmitCalls);
  averageWorkerWaitUs = average(cumulativeWorkerWaitUs, workerWaitCalls);
  averageFutureFulfillUs =
      average(cumulativeFutureFulfillUs, futureFulfillBatches);
  averageFutureFulfillBatchSize =
      average(fulfilledResults, futureFulfillBatches);
}

std::string DiskIoSchedulerStats::toString() const {
  auto derived = *this;
  derived.deriveMetrics();
  const auto& stats = derived;

  std::ostringstream out;
  out << "bm_disk_io_stats"
      << " queued_requests=" << formatArray(stats.queuedRequests)
      << " submitted_requests=" << formatArray(stats.submittedRequests)
      << " completed_requests_by_priority="
      << formatArray(stats.completedRequestsByPriority)
      << " completed_bytes_by_priority="
      << formatArray(stats.completedBytesByPriority)
      << " successful_requests_by_priority="
      << formatArray(stats.successfulRequestsByPriority)
      << " failed_requests_by_priority="
      << formatArray(stats.failedRequestsByPriority)
      << " inflight_requests=" << stats.inflightRequests
      << " accepted_requests=" << stats.acceptedRequests
      << " rejected_requests=" << stats.rejectedRequests
      << " shutdown_rejected_requests=" << stats.shutdownRejectedRequests
      << " completed_requests=" << stats.completedRequests
      << " completed_bytes=" << stats.completedBytes
      << " successful_requests=" << stats.successfulRequests
      << " failed_requests=" << stats.failedRequests
      << " backend_submit_failed_requests="
      << stats.backendSubmitFailedRequests
      << " backend_io_error_requests=" << stats.backendIoErrorRequests
      << " max_observed_queue_depth=" << stats.maxObservedQueueDepth
      << " max_observed_inflight_requests="
      << stats.maxObservedInflightRequests
      << " latency_samples=" << stats.latencySamples
      << " cumulative_device_latency_us="
      << stats.cumulativeDeviceLatencyUs
      << " average_device_latency_us=" << stats.averageDeviceLatencyUs
      << " min_latency_us=" << stats.minLatencyUs
      << " max_latency_us=" << stats.maxLatencyUs
      << " cumulative_queue_wait_us=" << stats.cumulativeQueueWaitUs
      << " queue_wait_samples=" << stats.queueWaitSamples
      << " average_queue_wait_us=" << stats.averageQueueWaitUs
      << " max_queue_wait_us=" << stats.maxQueueWaitUs
      << " cumulative_end_to_end_latency_us="
      << stats.cumulativeEndToEndLatencyUs
      << " average_end_to_end_latency_us="
      << stats.averageEndToEndLatencyUs
      << " max_end_to_end_latency_us=" << stats.maxEndToEndLatencyUs
      << " submit_batches=" << stats.submitBatches
      << " submitted_requests_in_batches="
      << stats.submittedRequestsInBatches
      << " average_submit_batch_size=" << stats.averageSubmitBatchSize
      << " max_submit_batch_size=" << stats.maxSubmitBatchSize
      << " completion_batches=" << stats.completionBatches
      << " completed_requests_in_batches="
      << stats.completedRequestsInBatches
      << " average_completion_batch_size="
      << stats.averageCompletionBatchSize
      << " max_completion_batch_size=" << stats.maxCompletionBatchSize
      << " backend_reap_calls=" << stats.backendReapCalls
      << " cumulative_backend_reap_us=" << stats.cumulativeBackendReapUs
      << " average_backend_reap_us=" << stats.averageBackendReapUs
      << " max_backend_reap_us=" << stats.maxBackendReapUs
      << " backend_submit_calls=" << stats.backendSubmitCalls
      << " cumulative_backend_submit_us="
      << stats.cumulativeBackendSubmitUs
      << " average_backend_submit_us=" << stats.averageBackendSubmitUs
      << " max_backend_submit_us=" << stats.maxBackendSubmitUs
      << " worker_wait_calls=" << stats.workerWaitCalls
      << " cumulative_worker_wait_us=" << stats.cumulativeWorkerWaitUs
      << " average_worker_wait_us=" << stats.averageWorkerWaitUs
      << " max_worker_wait_us=" << stats.maxWorkerWaitUs
      << " future_fulfill_batches=" << stats.futureFulfillBatches
      << " fulfilled_results=" << stats.fulfilledResults
      << " cumulative_future_fulfill_us="
      << stats.cumulativeFutureFulfillUs
      << " average_future_fulfill_us=" << stats.averageFutureFulfillUs
      << " average_future_fulfill_batch_size="
      << stats.averageFutureFulfillBatchSize
      << " max_future_fulfill_us=" << stats.maxFutureFulfillUs
      << " max_future_fulfill_batch_size="
      << stats.maxFutureFulfillBatchSize;
  if (stats.depthControl != nullptr) {
    out << stats.depthControl->toString();
  }
  return out.str();
}

} // namespace bytedance::bolt::memory::bm
