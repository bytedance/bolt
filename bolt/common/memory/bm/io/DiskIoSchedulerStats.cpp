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

} // namespace

std::string DiskIoSchedulerStats::toString() const {
  std::ostringstream out;
  out << "bm_disk_io_stats"
      << " queued_requests=" << formatArray(queuedRequests)
      << " submitted_requests=" << formatArray(submittedRequests)
      << " completed_requests_by_priority="
      << formatArray(completedRequestsByPriority)
      << " completed_bytes_by_priority="
      << formatArray(completedBytesByPriority)
      << " successful_requests_by_priority="
      << formatArray(successfulRequestsByPriority)
      << " failed_requests_by_priority="
      << formatArray(failedRequestsByPriority)
      << " inflight_requests=" << inflightRequests
      << " accepted_requests=" << acceptedRequests
      << " rejected_requests=" << rejectedRequests
      << " shutdown_rejected_requests=" << shutdownRejectedRequests
      << " completed_requests=" << completedRequests
      << " completed_bytes=" << completedBytes
      << " successful_requests=" << successfulRequests
      << " failed_requests=" << failedRequests
      << " backend_submit_failed_requests=" << backendSubmitFailedRequests
      << " backend_io_error_requests=" << backendIoErrorRequests
      << " max_observed_queue_depth=" << maxObservedQueueDepth
      << " max_observed_inflight_requests=" << maxObservedInflightRequests
      << " latency_samples=" << latencySamples
      << " cumulative_device_latency_us=" << cumulativeDeviceLatencyUs
      << " average_device_latency_us=" << averageDeviceLatencyUs
      << " min_latency_us=" << minLatencyUs
      << " max_latency_us=" << maxLatencyUs
      << " cumulative_queue_wait_us=" << cumulativeQueueWaitUs
      << " queue_wait_samples=" << queueWaitSamples
      << " average_queue_wait_us=" << averageQueueWaitUs
      << " max_queue_wait_us=" << maxQueueWaitUs
      << " cumulative_end_to_end_latency_us=" << cumulativeEndToEndLatencyUs
      << " average_end_to_end_latency_us=" << averageEndToEndLatencyUs
      << " max_end_to_end_latency_us=" << maxEndToEndLatencyUs
      << " submit_batches=" << submitBatches
      << " submitted_requests_in_batches=" << submittedRequestsInBatches
      << " average_submit_batch_size=" << averageSubmitBatchSize
      << " max_submit_batch_size=" << maxSubmitBatchSize
      << " completion_batches=" << completionBatches
      << " completed_requests_in_batches=" << completedRequestsInBatches
      << " average_completion_batch_size=" << averageCompletionBatchSize
      << " max_completion_batch_size=" << maxCompletionBatchSize
      << " backend_reap_calls=" << backendReapCalls
      << " cumulative_backend_reap_us=" << cumulativeBackendReapUs
      << " average_backend_reap_us=" << averageBackendReapUs
      << " max_backend_reap_us=" << maxBackendReapUs
      << " backend_submit_calls=" << backendSubmitCalls
      << " cumulative_backend_submit_us=" << cumulativeBackendSubmitUs
      << " average_backend_submit_us=" << averageBackendSubmitUs
      << " max_backend_submit_us=" << maxBackendSubmitUs
      << " worker_wait_calls=" << workerWaitCalls
      << " cumulative_worker_wait_us=" << cumulativeWorkerWaitUs
      << " average_worker_wait_us=" << averageWorkerWaitUs
      << " max_worker_wait_us=" << maxWorkerWaitUs
      << " future_fulfill_batches=" << futureFulfillBatches
      << " fulfilled_results=" << fulfilledResults
      << " cumulative_future_fulfill_us=" << cumulativeFutureFulfillUs
      << " average_future_fulfill_us=" << averageFutureFulfillUs
      << " average_future_fulfill_batch_size=" << averageFutureFulfillBatchSize
      << " max_future_fulfill_us=" << maxFutureFulfillUs
      << " max_future_fulfill_batch_size=" << maxFutureFulfillBatchSize;
  if (depthControl != nullptr) {
    out << depthControl->toString();
  }
  return out.str();
}

} // namespace bytedance::bolt::memory::bm
