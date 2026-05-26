#include "bolt/common/memory/bm/DiskIoScheduler.h"

#include "bolt/common/base/Exceptions.h"

#include <cerrno>
#include <chrono>
#include <stdexcept>
#include <utility>

namespace bytedance::bolt::memory::bm {
namespace {

constexpr auto kIdlePollInterval = std::chrono::milliseconds(1);

} // namespace

DiskIoScheduler::DiskIoScheduler(DiskIoSchedulerConfig config)
    : config_(config), currentDepth_(config.adaptiveDepth.initialDepth) {
  BOLT_FAIL("DiskIoScheduler default constructor requires IoUringBackend");
}

DiskIoScheduler::DiskIoScheduler(
    DiskIoSchedulerConfig config,
    std::unique_ptr<IoBackend> backend)
    : config_(config), currentDepth_(config.adaptiveDepth.initialDepth),
      backend_(std::move(backend)) {
  if (validateDiskIoSchedulerConfig(config_) != 0) {
    throw std::invalid_argument("invalid DiskIoSchedulerConfig");
  }
  if (!backend_) {
    throw std::invalid_argument("DiskIoScheduler requires an IoBackend");
  }
  stats_.currentDepth = currentDepth_;
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
  if (validationError != 0) {
    return completedFuture(IoResult{0, validationError});
  }

  std::promise<IoResult> promise;
  auto future = promise.get_future();

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) {
      promise.set_value(IoResult{0, ESHUTDOWN});
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
  snapshot.currentDepth = currentDepth_;
  return snapshot;
}

void DiskIoScheduler::run() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (true) {
    // The scheduler thread expects backend submit/reap to be nonblocking and
    // callback-free; completions are observed by polling reap().
    reapCompletionsLocked();

    bool dispatched = false;
    while (inflight_.size() < currentDepth_ && hasQueuedRequestsLocked()) {
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

  if (!backend_->submit(queued.requestId, queued.request)) {
    queued.promise.set_value(IoResult{0, EIO});
    ++stats_.completedRequests;
    ++stats_.completedRequestsByPriority[*priority];
    ++stats_.failedRequests;
    return true;
  }

  ++stats_.submittedRequests[*priority];
  inflight_.emplace(
      queued.requestId,
      InflightRequest{queued.request.priority, std::move(queued.promise)});
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
    ++stats_.completedRequests;
    ++stats_.completedRequestsByPriority[priority];
    stats_.completedBytes += completion.result.bytes;
    if (completion.result.errorCode == 0) {
      ++stats_.successfulRequests;
    } else {
      ++stats_.failedRequests;
    }
    stats_.inflightRequests = inflight_.size();

    inflight.promise.set_value(completion.result);
  }
}

} // namespace bytedance::bolt::memory::bm
