#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

#include "bolt/common/memory/bm/io/AdaptiveDepthController.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerConfig.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerStats.h"
#include "bolt/common/memory/bm/io/EventFd.h"
#include "bolt/common/memory/bm/io/IoBackend.h"
#include "bolt/common/memory/bm/io/IoPriority.h"
#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoResult.h"

namespace bytedance::bolt::memory::bm {

class DiskIoScheduler {
 public:
  explicit DiskIoScheduler(DiskIoSchedulerConfig config);
  DiskIoScheduler(
      DiskIoSchedulerConfig config,
      std::unique_ptr<IoBackend> backend);
  ~DiskIoScheduler();

  DiskIoScheduler(const DiskIoScheduler&) = delete;
  DiskIoScheduler& operator=(const DiskIoScheduler&) = delete;

  std::future<IoResult> submit(IoRequest request);
  void stopAndDrain();
  DiskIoSchedulerStats stats() const;

 private:
  struct QueuedRequest {
    uint64_t requestId{0};
    IoRequest request;
    std::promise<IoResult> promise;
    std::chrono::steady_clock::time_point enqueueTime;
  };

  struct InflightRequest {
    IoRequest request;
    std::promise<IoResult> promise;
    std::chrono::steady_clock::time_point enqueueTime;
    std::chrono::steady_clock::time_point submitTime;
  };

  struct ReadyResult {
    std::promise<IoResult> promise;
    IoResult result;
  };

  struct DispatchResult {
    QueuedRequest queued;
    BackendSubmitStatus status{BackendSubmitStatus::Failed};
    std::chrono::steady_clock::time_point submitTime;
  };

  static std::future<IoResult> completedFuture(IoResult result);
  static void fulfillReadyResults(std::vector<ReadyResult>& readyResults);

  void run();
  bool hasQueuedRequestsLocked() const;
  bool drainedLocked() const;
  std::optional<size_t> pickQueueLocked();
  std::vector<QueuedRequest> collectDispatchBatchLocked();
  void applyDispatchResultsLocked(
      std::vector<DispatchResult>& results,
      std::vector<ReadyResult>& readyResults);
  void applyCompletionsLocked(
      std::vector<BackendCompletion>& completions,
      std::vector<ReadyResult>& readyResults);
  void failQueuedLocked(std::vector<ReadyResult>& readyResults);
  DiskIoSchedulerStats snapshotStatsLocked() const;
  void logStatsIfDueLocked(std::chrono::steady_clock::time_point now);
  int computeWaitTimeoutMsLocked(std::chrono::steady_clock::time_point now);
  void waitForWorkerEvent(int timeoutMs);
  void notifyWorker() const;

  const DiskIoSchedulerConfig config_;
  AdaptiveDepthController adaptiveDepth_;
  std::unique_ptr<IoBackend> backend_;
  EventFd wakeupEvent_;
  int epollFd_{-1};

  mutable std::mutex mutex_;
  bool stopping_{false};
  uint64_t nextRequestId_{1};
  std::array<std::deque<QueuedRequest>, kIoPriorityCount> queues_;
  uint64_t totalQueued_{0};
  std::array<int64_t, kIoPriorityCount> deficits_{};
  size_t nextPriorityCursor_{0};
  std::unordered_map<uint64_t, InflightRequest> inflight_;
  DiskIoSchedulerStats stats_;
  std::chrono::steady_clock::time_point lastStatsLogTime_;
  std::thread worker_;
  std::mutex joinMutex_;
};

} // namespace bytedance::bolt::memory::bm
