#pragma once

#include <array>
#include <cstdint>
#include <sstream>
#include <string>

#include "bolt/common/memory/bm/io/AdaptiveDepthStats.h"
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
  uint32_t currentDepth{0};
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
  double recentThroughputBytesPerSecond{0};
  uint64_t cumulativeLatencyUs{0};
  uint64_t latencySamples{0};
  double averageLatencyUs{0};
  uint64_t minLatencyUs{0};
  uint64_t maxLatencyUs{0};
  AdaptiveDepthStats adaptive;

  std::string toString() const {
    auto formatArray = [](const std::array<uint64_t, kIoPriorityCount>& values) {
      std::ostringstream out;
      out << "[high=" << values[priorityIndex(IoPriority::High)]
          << ",medium=" << values[priorityIndex(IoPriority::Medium)]
          << ",low=" << values[priorityIndex(IoPriority::Low)] << "]";
      return out.str();
    };

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
        << " current_depth=" << currentDepth
        << " accepted_requests=" << acceptedRequests
        << " rejected_requests=" << rejectedRequests
        << " shutdown_rejected_requests=" << shutdownRejectedRequests
        << " completed_requests=" << completedRequests
        << " completed_bytes=" << completedBytes
        << " successful_requests=" << successfulRequests
        << " failed_requests=" << failedRequests
        << " backend_submit_failed_requests="
        << backendSubmitFailedRequests
        << " backend_io_error_requests=" << backendIoErrorRequests
        << " max_observed_queue_depth=" << maxObservedQueueDepth
        << " max_observed_inflight_requests=" << maxObservedInflightRequests
        << " recent_throughput_bytes_per_second="
        << recentThroughputBytesPerSecond
        << " cumulative_latency_us=" << cumulativeLatencyUs
        << " latency_samples=" << latencySamples
        << " average_latency_us=" << averageLatencyUs
        << " min_latency_us=" << minLatencyUs
        << " max_latency_us=" << maxLatencyUs
        << " adaptive_enabled=" << adaptive.enabled
        << " adaptive_current_depth=" << adaptive.currentDepth
        << " adaptive_best_depth=" << adaptive.bestDepth
        << " adaptive_recent_throughput_bytes_per_second="
        << adaptive.recentThroughputBytesPerSecond
        << " adaptive_best_throughput_bytes_per_second="
        << adaptive.bestThroughputBytesPerSecond
        << " adaptive_measuring_probe_depth="
        << adaptive.measuringProbeDepth
        << " adaptive_completed_windows=" << adaptive.completedWindows
        << " adaptive_last_window_throughput_bytes_per_second="
        << adaptive.lastWindowThroughputBytesPerSecond;
    return out.str();
  }
};

} // namespace bytedance::bolt::memory::bm
