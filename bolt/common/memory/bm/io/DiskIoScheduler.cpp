#include "bolt/common/memory/bm/io/DiskIoScheduler.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerConfigValidator.h"
#include "bolt/common/memory/bm/io/IoRequestValidator.h"
#include "bolt/common/memory/bm/io/IoUringBackend.h"

#include <chrono>
#include <utility>

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
      backend_(std::move(backend)) {
  BOLT_CHECK(
      validateDiskIoSchedulerConfig(config_) == IoErrorCode::Ok,
      "invalid DiskIoSchedulerConfig");
  BOLT_CHECK(backend_ != nullptr, "DiskIoScheduler requires an IoBackend");
  stats_.currentDepth = adaptiveDepth_.currentDepth();
  worker_ = std::thread([this] { run(); });
}

DiskIoScheduler::~DiskIoScheduler() {
  stopAndDrain();
}

std::future<IoResult> DiskIoScheduler::completedFuture(IoResult result) {
  std::promise<IoResult> promise;
  auto future = promise.get_future();
  promise.set_value(result);
  return future;
}

std::future<IoResult> DiskIoScheduler::submit(IoRequest request) {
  const auto validationError = validateIoRequest(request);
  if (validationError != IoErrorCode::Ok) {
    return completedFuture(IoResult{0, validationError});
  }

  std::promise<IoResult> promise;
  auto future = promise.get_future();

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) {
      promise.set_value(IoResult{0, IoErrorCode::Shutdown});
      return future;
    }

    const auto priority = priorityIndex(request.priority);
    queues_[priority].push_back(QueuedRequest{
        nextRequestId_++, std::move(request), std::move(promise)});
    stats_.queuedRequests[priority] = queues_[priority].size();
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
  auto snapshot = stats_;
  for (size_t i = 0; i < kIoPriorityCount; ++i) {
    snapshot.queuedRequests[i] = queues_[i].size();
  }
  snapshot.inflightRequests = inflight_.size();
  snapshot.currentDepth = adaptiveDepth_.currentDepth();
  snapshot.recentThroughputBytesPerSecond =
      adaptiveDepth_.recentThroughputBytesPerSecond();
  return snapshot;
}

void DiskIoScheduler::run() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (true) {
    // The scheduler thread expects backend submit/reap to be nonblocking and
    // callback-free; completions are observed by polling reap().
    reapCompletionsLocked();

    bool dispatched = false;
    while (inflight_.size() < adaptiveDepth_.currentDepth() &&
           hasQueuedRequestsLocked()) {
      dispatched = dispatchOneLocked() || dispatched;
    }

    if (stopping_ && drainedLocked()) {
      return;
    }

    if (dispatched || !inflight_.empty()) {
      cv_.wait_for(lock, kIdlePollInterval);
    } else {
      cv_.wait(lock, [this] { return stopping_ || hasQueuedRequestsLocked(); });
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

bool DiskIoScheduler::dispatchOneLocked() {
  const auto priority = pickQueueLocked();
  if (!priority.has_value()) {
    return false;
  }

  auto& queue = queues_[*priority];
  auto queued = std::move(queue.front());
  queue.pop_front();
  stats_.queuedRequests[*priority] = queue.size();
  if (queue.empty()) {
    deficits_[*priority] = 0;
  }

  if (!backend_->submit(queued.requestId, queued.request)) {
    queued.promise.set_value(IoResult{0, IoErrorCode::BackendSubmitFailed});
    ++stats_.completedRequests;
    ++stats_.completedRequestsByPriority[*priority];
    ++stats_.failedRequests;
    return true;
  }

  ++stats_.submittedRequests[*priority];
  inflight_.emplace(
      queued.requestId,
      InflightRequest{
          queued.request.priority,
          std::move(queued.promise),
          std::chrono::steady_clock::now()});
  stats_.inflightRequests = inflight_.size();
  return true;
}

void DiskIoScheduler::reapCompletionsLocked() {
  auto completions = backend_->reap();
  for (const auto& completion : completions) {
    auto it = inflight_.find(completion.requestId);
    if (it == inflight_.end()) {
      continue;
    }

    auto inflight = std::move(it->second);
    inflight_.erase(it);

    const auto priority = priorityIndex(inflight.priority);
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
    if (completion.result.ok()) {
      ++stats_.successfulRequests;
    } else {
      ++stats_.failedRequests;
    }
    stats_.inflightRequests = inflight_.size();
    stats_.averageLatencyUs =
        stats_.completedRequests == 0
            ? 0
            : static_cast<double>(stats_.cumulativeLatencyUs) /
                static_cast<double>(stats_.completedRequests);
    adaptiveDepth_.onCompletion(
        completion.result.bytes, hasQueuedRequestsLocked(), now);
    stats_.currentDepth = adaptiveDepth_.currentDepth();
    stats_.recentThroughputBytesPerSecond =
        adaptiveDepth_.recentThroughputBytesPerSecond();

    inflight.promise.set_value(completion.result);
  }
}

} // namespace bytedance::bolt::memory::bm
