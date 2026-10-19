/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "bolt/common/memory/bm/base/ScopedFd.h"
#include "bolt/common/memory/bm/io/DepthController.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerConfig.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerStats.h"
#include "bolt/common/memory/bm/io/EventFd.h"
#include "bolt/common/memory/bm/io/InflightRegistry.h"
#include "bolt/common/memory/bm/io/IoBackend.h"
#include "bolt/common/memory/bm/io/IoPriority.h"
#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoRequestQueue.h"
#include "bolt/common/memory/bm/io/IoResult.h"

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
  struct ReadyResult {
    std::promise<IoResult> promise;
    IoResult result;
  };

  struct DispatchResult {
    QueuedIoRequest queued;
    BackendSubmitStatus status{BackendSubmitStatus::Failed};
    std::chrono::steady_clock::time_point submitTime;
    uint64_t submitDurationUs{0};
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
  uint64_t waitForWorkerEvent(int timeoutMs);
  void notifyWorker() const;

  const DiskIoSchedulerConfig config_;
  std::unique_ptr<DepthController> depthController_;
  std::unique_ptr<IoBackend> backend_;
  EventFd wakeupEvent_;
  ScopedFd epollFd_;

  mutable std::mutex mutex_;
  bool stopping_{false};
  uint64_t nextRequestId_{1};
  IoRequestQueue requestQueue_;
  InflightRegistry inflight_;
  DiskIoSchedulerStats stats_;
  std::thread worker_;
  std::mutex joinMutex_;
};

} // namespace bytedance::bolt::memory::bm
