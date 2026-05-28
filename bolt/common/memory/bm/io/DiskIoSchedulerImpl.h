#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "bolt/common/memory/bm/io/DepthController.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerConfig.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerStats.h"
#include "bolt/common/memory/bm/io/EventFd.h"
#include "bolt/common/memory/bm/io/IoBackend.h"
#include "bolt/common/memory/bm/io/IoPriority.h"
#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoRequestQueue.h"
#include "bolt/common/memory/bm/io/IoResult.h"
#include "bolt/common/memory/bm/io/ScopedFd.h"

namespace bytedance::bolt::memory::bm {

class DiskIoSchedulerImpl {
 public:
  explicit DiskIoSchedulerImpl(DiskIoSchedulerConfig config);
  DiskIoSchedulerImpl(
      DiskIoSchedulerConfig config,
      std::unique_ptr<IoBackend> backend);
  ~DiskIoSchedulerImpl();

  DiskIoSchedulerImpl(const DiskIoSchedulerImpl&) = delete;
  DiskIoSchedulerImpl& operator=(const DiskIoSchedulerImpl&) = delete;

  std::future<IoResult> submit(IoRequest request);
  DiskIoSchedulerStats stats() const;

 private:
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
    QueuedIoRequest queued;
    BackendSubmitStatus status{BackendSubmitStatus::Failed};
    std::chrono::steady_clock::time_point submitTime;
  };

  static std::future<IoResult> completedFuture(IoResult result);
  static void fulfillReadyResults(std::vector<ReadyResult>& readyResults);

  void stopAndDrain();
  void run();
  bool hasQueuedRequestsLocked() const;
  bool drainedLocked() const;
  std::vector<QueuedIoRequest> collectDispatchBatchLocked();
  bool applyDispatchResultsLocked(
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
  DepthController depthController_;
  std::unique_ptr<IoBackend> backend_;
  EventFd wakeupEvent_;
  ScopedFd epollFd_;

  mutable std::mutex mutex_;
  bool stopping_{false};
  uint64_t nextRequestId_{1};
  IoRequestQueue requestQueue_;
  std::unordered_map<uint64_t, InflightRequest> inflight_;
  DiskIoSchedulerStats stats_;
  std::chrono::steady_clock::time_point lastStatsLogTime_;
  std::thread worker_;
  std::mutex joinMutex_;
};

} // namespace bytedance::bolt::memory::bm
