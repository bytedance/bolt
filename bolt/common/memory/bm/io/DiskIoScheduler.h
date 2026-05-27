#pragma once

#include <array>
#include <condition_variable>
#include <chrono>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>

#include "bolt/common/memory/bm/io/AdaptiveDepthController.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerConfig.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerStats.h"
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
  };

  struct InflightRequest {
    IoPriority priority{IoPriority::Medium};
    std::promise<IoResult> promise;
    std::chrono::steady_clock::time_point submitTime;
  };

  static std::future<IoResult> completedFuture(IoResult result);

  void run();
  bool hasQueuedRequestsLocked() const;
  bool drainedLocked() const;
  std::optional<size_t> pickQueueLocked();
  bool dispatchOneLocked();
  void reapCompletionsLocked();

  const DiskIoSchedulerConfig config_;
  AdaptiveDepthController adaptiveDepth_;
  std::unique_ptr<IoBackend> backend_;

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  bool stopping_{false};
  uint64_t nextRequestId_{1};
  std::array<std::deque<QueuedRequest>, kIoPriorityCount> queues_;
  std::array<int64_t, kIoPriorityCount> deficits_{{0, 0, 0}};
  size_t nextPriorityCursor_{0};
  std::unordered_map<uint64_t, InflightRequest> inflight_;
  DiskIoSchedulerStats stats_;
  std::thread worker_;
  std::mutex joinMutex_;
};

} // namespace bytedance::bolt::memory::bm
