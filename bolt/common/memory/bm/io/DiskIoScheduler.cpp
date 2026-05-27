#include "bolt/common/memory/bm/io/DiskIoScheduler.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerConfigValidator.h"
#include "bolt/common/memory/bm/io/IoRequestValidator.h"
#include "bolt/common/memory/bm/io/IoUringBackend.h"

#include <algorithm>
#include <chrono>
#include <optional>
#include <utility>

#include <glog/logging.h>

namespace bytedance::bolt::memory::bm {
namespace {

constexpr auto kIdlePollInterval = std::chrono::milliseconds(1);

} // namespace

DiskIoScheduler::DiskIoScheduler(DiskIoSchedulerConfig config)
    : DiskIoScheduler(
          config,
          std::make_unique<IoUringBackend>(config.ringDepth)) {}

DiskIoScheduler::DiskIoScheduler(
    DiskIoSchedulerConfig config,
    std::unique_ptr<IoBackend> backend)
    : config_(config),
      adaptiveDepth_(config_.adaptiveDepth),
      backend_(std::move(backend)),
      lastStatsLogTime_(std::chrono::steady_clock::now()) {
  BOLT_CHECK(
      validateDiskIoSchedulerConfig(config_) == IoErrorCode::Ok,
      "invalid DiskIoSchedulerConfig");
  BOLT_CHECK(backend_ != nullptr, "DiskIoScheduler requires an IoBackend");
  stats_.currentDepth = adaptiveDepth_.currentDepth();
  stats_.adaptive = adaptiveDepth_.stats();
  worker_ = std::thread([this] { run(); });
}

DiskIoScheduler::~DiskIoScheduler() {
  stopAndDrain();
}

std::future<IoResult> DiskIoScheduler::completedFuture(IoResult result) {
  std::promise<IoResult> promise;
  auto future = promise.get_future();
  promise.set_value(std::move(result));
  return future;
}

void DiskIoScheduler::fulfillReadyResults(
    std::vector<ReadyResult>& readyResults) {
  for (auto& ready : readyResults) {
    ready.promise.set_value(std::move(ready.result));
  }
  readyResults.clear();
}

std::future<IoResult> DiskIoScheduler::submit(IoRequest request) {
  const auto validationError = validateIoRequest(request);
  if (validationError != IoErrorCode::Ok) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ++stats_.rejectedRequests;
      ++stats_.completedRequests;
      ++stats_.failedRequests;
      if (validPriority(request.priority)) {
        ++stats_.completedRequestsByPriority[priorityIndex(request.priority)];
        ++stats_.failedRequestsByPriority[priorityIndex(request.priority)];
      }
    }
    IoResult result{0, validationError};
    result.buffer = std::move(request.buffer);
    return completedFuture(std::move(result));
  }

  std::promise<IoResult> promise;
  auto future = promise.get_future();
  std::optional<IoResult> rejectedResult;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) {
      ++stats_.shutdownRejectedRequests;
      ++stats_.completedRequests;
      ++stats_.completedRequestsByPriority[priorityIndex(request.priority)];
      ++stats_.failedRequests;
      ++stats_.failedRequestsByPriority[priorityIndex(request.priority)];
      IoResult result{0, IoErrorCode::Shutdown};
      result.buffer = std::move(request.buffer);
      rejectedResult = std::move(result);
    } else {
      const auto priority = priorityIndex(request.priority);
      queues_[priority].push_back(QueuedRequest{
          nextRequestId_++, std::move(request), std::move(promise)});
      stats_.queuedRequests[priority] = queues_[priority].size();
      ++stats_.acceptedRequests;
      uint64_t queued = 0;
      for (const auto& queue : queues_) {
        queued += queue.size();
      }
      stats_.maxObservedQueueDepth =
          std::max(stats_.maxObservedQueueDepth, queued);
    }
  }

  if (rejectedResult.has_value()) {
    promise.set_value(std::move(*rejectedResult));
    return future;
  }

  cv_.notify_one();

  return future;
}

void DiskIoScheduler::stopAndDrain() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
  }
  cv_.notify_one();

  std::lock_guard<std::mutex> joinLock(joinMutex_);
  if (worker_.joinable()) {
    worker_.join();
  }
}

DiskIoSchedulerStats DiskIoScheduler::stats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return snapshotStatsLocked();
}

DiskIoSchedulerStats DiskIoScheduler::snapshotStatsLocked() const {
  auto snapshot = stats_;
  for (size_t i = 0; i < kIoPriorityCount; ++i) {
    snapshot.queuedRequests[i] = queues_[i].size();
  }
  snapshot.inflightRequests = inflight_.size();
  snapshot.currentDepth = adaptiveDepth_.currentDepth();
  snapshot.recentThroughputBytesPerSecond =
      adaptiveDepth_.recentThroughputBytesPerSecond();
  snapshot.adaptive = adaptiveDepth_.stats();
  return snapshot;
}

void DiskIoScheduler::logStatsIfDueLocked(
    std::chrono::steady_clock::time_point now) {
  if (!config_.enableStatsLogging) {
    return;
  }
  if (now - lastStatsLogTime_ < config_.statsLogInterval) {
    return;
  }
  lastStatsLogTime_ = now;
  LOG(INFO) << snapshotStatsLocked().toString();
}

void DiskIoScheduler::run() {
  while (true) {
    std::vector<ReadyResult> readyResults;
    auto completions = backend_->reap();
    std::vector<QueuedRequest> dispatchBatch;
    bool madeProgress = !completions.empty();
    bool shouldExit = false;

    {
      std::lock_guard<std::mutex> lock(mutex_);
      applyCompletionsLocked(completions, readyResults);
      dispatchBatch = collectDispatchBatchLocked();
      madeProgress = madeProgress || !dispatchBatch.empty();
      shouldExit = stopping_ && drainedLocked() && dispatchBatch.empty();
    }

    std::vector<DispatchResult> dispatchResults;
    dispatchResults.reserve(dispatchBatch.size());
    for (auto& queued : dispatchBatch) {
      // Backend calls are deliberately made without mutex_. submit() should
      // remain a lightweight enqueue operation even if the backend enters the
      // kernel or spends time scanning completions.
      const auto submitTime = std::chrono::steady_clock::now();
      const bool submitted = backend_->submit(queued.requestId, queued.request);
      dispatchResults.push_back(
          DispatchResult{std::move(queued), submitted, submitTime});
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      applyDispatchResultsLocked(dispatchResults, readyResults);
      shouldExit = shouldExit || (stopping_ && drainedLocked());
    }

    // Fulfill futures after releasing mutex_. A waiting caller may immediately
    // call back into submit()/stats(); doing set_value() under the scheduler
    // lock would make that wake-up contend with the worker and can deadlock
    // with future implementations that run continuations inline.
    fulfillReadyResults(readyResults);

    if (shouldExit) {
      return;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    if (madeProgress || !inflight_.empty()) {
      cv_.wait_for(lock, kIdlePollInterval);
      logStatsIfDueLocked(std::chrono::steady_clock::now());
    } else {
      if (config_.enableStatsLogging) {
        cv_.wait_for(lock, config_.statsLogInterval, [this] {
          return stopping_ || hasQueuedRequestsLocked();
        });
        logStatsIfDueLocked(std::chrono::steady_clock::now());
      } else {
        cv_.wait(
            lock, [this] { return stopping_ || hasQueuedRequestsLocked(); });
      }
    }
  }
}

bool DiskIoScheduler::hasQueuedRequestsLocked() const {
  for (const auto& queue : queues_) {
    if (!queue.empty()) {
      return true;
    }
  }
  return false;
}

bool DiskIoScheduler::drainedLocked() const {
  return inflight_.empty() && !hasQueuedRequestsLocked();
}

std::optional<size_t> DiskIoScheduler::pickQueueLocked() {
  auto hasDispatchableDeficit = [this] {
    for (size_t i = 0; i < kIoPriorityCount; ++i) {
      if (!queues_[i].empty() && deficits_[i] > 0) {
        return true;
      }
    }
    return false;
  };

  if (!hasDispatchableDeficit()) {
    for (size_t i = 0; i < kIoPriorityCount; ++i) {
      if (!queues_[i].empty()) {
        deficits_[i] += config_.priorityWeights[i];
      } else {
        deficits_[i] = 0;
      }
    }
  }

  for (size_t i = 0; i < kIoPriorityCount; ++i) {
    const auto priority = nextPriorityCursor_;
    nextPriorityCursor_ = (nextPriorityCursor_ + 1) % kIoPriorityCount;

    if (queues_[priority].empty()) {
      deficits_[priority] = 0;
      continue;
    }
    if (deficits_[priority] <= 0) {
      continue;
    }

    --deficits_[priority];
    return priority;
  }

  return std::nullopt;
}

std::vector<DiskIoScheduler::QueuedRequest>
DiskIoScheduler::collectDispatchBatchLocked() {
  std::vector<QueuedRequest> batch;
  while (inflight_.size() + batch.size() < adaptiveDepth_.currentDepth() &&
         hasQueuedRequestsLocked()) {
    const auto priority = pickQueueLocked();
    if (!priority.has_value()) {
      break;
    }

    auto& queue = queues_[*priority];
    auto queued = std::move(queue.front());
    queue.pop_front();
    stats_.queuedRequests[*priority] = queue.size();
    if (queue.empty()) {
      deficits_[*priority] = 0;
    }
    batch.push_back(std::move(queued));
  }
  return batch;
}

void DiskIoScheduler::applyDispatchResultsLocked(
    std::vector<DispatchResult>& results,
    std::vector<ReadyResult>& readyResults) {
  for (auto& dispatch : results) {
    const auto priority = priorityIndex(dispatch.queued.request.priority);
    if (!dispatch.submitted) {
      IoResult result{0, IoErrorCode::BackendSubmitFailed};
      result.buffer = std::move(dispatch.queued.request.buffer);
      readyResults.push_back(
          ReadyResult{std::move(dispatch.queued.promise), std::move(result)});
      ++stats_.completedRequests;
      ++stats_.completedRequestsByPriority[priority];
      ++stats_.failedRequests;
      ++stats_.failedRequestsByPriority[priority];
      ++stats_.backendSubmitFailedRequests;
      continue;
    }

    ++stats_.submittedRequests[priority];
    inflight_.emplace(
        dispatch.queued.requestId,
        InflightRequest{
            std::move(dispatch.queued.request),
            std::move(dispatch.queued.promise),
            dispatch.submitTime});
    stats_.inflightRequests = inflight_.size();
    stats_.maxObservedInflightRequests = std::max<uint64_t>(
        stats_.maxObservedInflightRequests, inflight_.size());
  }
}

void DiskIoScheduler::applyCompletionsLocked(
    std::vector<BackendCompletion>& completions,
    std::vector<ReadyResult>& readyResults) {
  for (auto& completion : completions) {
    auto it = inflight_.find(completion.requestId);
    if (it == inflight_.end()) {
      continue;
    }

    auto inflight = std::move(it->second);
    inflight_.erase(it);

    const auto priority = priorityIndex(inflight.request.priority);
    const auto now = std::chrono::steady_clock::now();
    const auto latencyUs =
        std::chrono::duration_cast<std::chrono::microseconds>(
            now - inflight.submitTime)
            .count();
    stats_.cumulativeLatencyUs +=
        latencyUs > 0 ? static_cast<uint64_t>(latencyUs) : 0;
    ++stats_.completedRequests;
    ++stats_.completedRequestsByPriority[priority];
    stats_.completedBytes += completion.result.bytes;
    stats_.completedBytesByPriority[priority] += completion.result.bytes;
    if (completion.result.ok()) {
      ++stats_.successfulRequests;
      ++stats_.successfulRequestsByPriority[priority];
    } else {
      ++stats_.failedRequests;
      ++stats_.failedRequestsByPriority[priority];
      if (completion.result.error == IoErrorCode::BackendIoError) {
        ++stats_.backendIoErrorRequests;
      }
    }
    stats_.inflightRequests = inflight_.size();
    stats_.averageLatencyUs =
        stats_.completedRequests == 0
            ? 0
            : static_cast<double>(stats_.cumulativeLatencyUs) /
                static_cast<double>(stats_.completedRequests);
    if (latencyUs > 0) {
      const auto positiveLatencyUs = static_cast<uint64_t>(latencyUs);
      if (stats_.minLatencyUs == 0 || positiveLatencyUs < stats_.minLatencyUs) {
        stats_.minLatencyUs = positiveLatencyUs;
      }
      stats_.maxLatencyUs =
          std::max(stats_.maxLatencyUs, positiveLatencyUs);
    }
    auto result = std::move(completion.result);
    result.buffer = std::move(inflight.request.buffer);
    adaptiveDepth_.onCompletion(result.bytes, hasQueuedRequestsLocked(), now);
    stats_.currentDepth = adaptiveDepth_.currentDepth();
    stats_.recentThroughputBytesPerSecond =
        adaptiveDepth_.recentThroughputBytesPerSecond();
    stats_.adaptive = adaptiveDepth_.stats();

    readyResults.push_back(
        ReadyResult{std::move(inflight.promise), std::move(result)});
  }
}

} // namespace bytedance::bolt::memory::bm
