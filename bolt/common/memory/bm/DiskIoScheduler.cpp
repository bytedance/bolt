#include "bolt/common/memory/bm/DiskIoScheduler.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/IoUringBackend.h"

#include <cerrno>
#include <chrono>
#include <stdexcept>
#include <utility>

namespace bytedance::bolt::memory::bm {
namespace {

constexpr auto kIdlePollInterval = std::chrono::milliseconds(1);

} // namespace

#ifdef IO_URING_SUPPORTED
DiskIoScheduler::DiskIoScheduler(DiskIoSchedulerConfig config)
    : DiskIoScheduler(
          config,
          std::make_unique<IoUringBackend>(config.ringDepth)) {}
#else
DiskIoScheduler::DiskIoScheduler(DiskIoSchedulerConfig config)
    : config_(config),
      adaptiveDepth_(config_.adaptiveDepth),
      currentDepth_(adaptiveDepth_.currentDepth()),
      windowStart_(std::chrono::steady_clock::now()) {
  BOLT_FAIL("DiskIoScheduler default constructor requires IoUringBackend");
}
#endif

DiskIoScheduler::DiskIoScheduler(
    DiskIoSchedulerConfig config,
    std::unique_ptr<IoBackend> backend)
    : config_(config),
      adaptiveDepth_(config_.adaptiveDepth),
      currentDepth_(adaptiveDepth_.currentDepth()),
      backend_(std::move(backend)),
      windowStart_(std::chrono::steady_clock::now()) {
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
  if (queue.empty()) {
    deficits_[*priority] = 0;
  }

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
    cumulativeLatencyUs_ += latencyUs > 0 ? static_cast<uint64_t>(latencyUs) : 0;
    ++stats_.completedRequests;
    ++stats_.completedRequestsByPriority[priority];
    stats_.completedBytes += completion.result.bytes;
    windowCompletedBytes_ += completion.result.bytes;
    if (completion.result.errorCode == 0) {
      ++stats_.successfulRequests;
    } else {
      ++stats_.failedRequests;
    }
    stats_.inflightRequests = inflight_.size();
    stats_.averageLatencyUs =
        stats_.completedRequests == 0
            ? 0
            : static_cast<double>(cumulativeLatencyUs_) /
                static_cast<double>(stats_.completedRequests);
    updateAdaptiveDepthLocked(now);

    inflight.promise.set_value(completion.result);
  }
}

void DiskIoScheduler::updateAdaptiveDepthLocked(
    std::chrono::steady_clock::time_point now) {
  const auto elapsed = now - windowStart_;
  if (elapsed < config_.adaptiveDepth.controlInterval) {
    return;
  }

  const auto seconds = std::chrono::duration<double>(elapsed).count();
  const auto throughput =
      seconds > 0 ? static_cast<double>(windowCompletedBytes_) / seconds : 0;
  adaptiveDepth_.onWindow(throughput, hasQueuedRequestsLocked());
  currentDepth_ = adaptiveDepth_.currentDepth();
  stats_.currentDepth = currentDepth_;
  stats_.recentThroughputBytesPerSecond =
      adaptiveDepth_.recentThroughputBytesPerSecond();
  windowCompletedBytes_ = 0;
  windowStart_ = now;
}

} // namespace bytedance::bolt::memory::bm
