#include "bolt/common/memory/bm/io/DiskIoSchedulerImpl.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerConfigValidator.h"
#include "bolt/common/memory/bm/io/IoRequestValidator.h"
#include "bolt/common/memory/bm/io/IoUringBackend.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <optional>
#include <utility>

#include <glog/logging.h>
#include <sys/epoll.h>
#include <unistd.h>

namespace bytedance::bolt::memory::bm {
namespace {

constexpr uint32_t kWakeupEvent = 1;
constexpr uint32_t kCompletionEvent = 2;

int createEpollFd() {
  const int fd = ::epoll_create1(EPOLL_CLOEXEC);
  BOLT_CHECK_GE(fd, 0, "epoll_create1 failed: {}", std::strerror(errno));
  return fd;
}

void registerEpollFd(int epollFd, int fd, uint32_t eventId) {
  epoll_event event{};
  event.events = EPOLLIN;
  event.data.u32 = eventId;
  BOLT_CHECK_EQ(
      ::epoll_ctl(epollFd, EPOLL_CTL_ADD, fd, &event),
      0,
      "epoll_ctl add failed: {}",
      std::strerror(errno));
}

int toEpollTimeoutMs(std::chrono::steady_clock::duration duration) {
  if (duration <= std::chrono::steady_clock::duration::zero()) {
    return 0;
  }
  const auto timeoutMs =
      std::chrono::duration_cast<std::chrono::milliseconds>(duration);
  return static_cast<int>(std::max<int64_t>(1, timeoutMs.count()));
}

} // namespace

DiskIoSchedulerImpl::ScopedFd::ScopedFd(int fd) : fd_(fd) {}

DiskIoSchedulerImpl::ScopedFd::~ScopedFd() {
  reset();
}

DiskIoSchedulerImpl::ScopedFd::ScopedFd(ScopedFd&& other) noexcept
    : fd_(other.fd_) {
  other.fd_ = -1;
}

DiskIoSchedulerImpl::ScopedFd& DiskIoSchedulerImpl::ScopedFd::operator=(
    ScopedFd&& other) noexcept {
  if (this != &other) {
    reset(other.fd_);
    other.fd_ = -1;
  }
  return *this;
}

int DiskIoSchedulerImpl::ScopedFd::get() const {
  return fd_;
}

void DiskIoSchedulerImpl::ScopedFd::reset(int fd) {
  if (fd_ >= 0) {
    ::close(fd_);
  }
  fd_ = fd;
}

DiskIoSchedulerImpl::DiskIoSchedulerImpl(DiskIoSchedulerConfig config)
    : DiskIoSchedulerImpl(
          config,
          std::make_unique<IoUringBackend>(config.ringDepth)) {}

DiskIoSchedulerImpl::DiskIoSchedulerImpl(
    DiskIoSchedulerConfig config,
    std::unique_ptr<IoBackend> backend)
    : config_(config),
      adaptiveDepth_(config_.adaptiveDepth),
      backend_(std::move(backend)),
      epollFd_(createEpollFd()),
      lastStatsLogTime_(std::chrono::steady_clock::now()) {
  BOLT_CHECK(
      validateDiskIoSchedulerConfig(config_) == IoErrorCode::Ok,
      "invalid DiskIoSchedulerConfig");
  BOLT_CHECK(backend_ != nullptr, "DiskIoScheduler requires an IoBackend");
  registerEpollFd(epollFd_.get(), wakeupEvent_.fd(), kWakeupEvent);
  registerEpollFd(epollFd_.get(), backend_->completionFd(), kCompletionEvent);
  stats_.currentDepth = adaptiveDepth_.currentDepth();
  stats_.adaptive = adaptiveDepth_.stats();
  worker_ = std::thread([this] { run(); });
}

DiskIoSchedulerImpl::~DiskIoSchedulerImpl() {
  stopAndDrain();
}

std::future<IoResult> DiskIoSchedulerImpl::completedFuture(IoResult result) {
  std::promise<IoResult> promise;
  auto future = promise.get_future();
  promise.set_value(std::move(result));
  return future;
}

void DiskIoSchedulerImpl::fulfillReadyResults(
    std::vector<ReadyResult>& readyResults) {
  for (auto& ready : readyResults) {
    ready.promise.set_value(std::move(ready.result));
  }
  readyResults.clear();
}

std::future<IoResult> DiskIoSchedulerImpl::submit(IoRequest request) {
  const auto validationError = validateIoRequest(request);
  if (validationError != IoErrorCode::Ok) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ++stats_.rejectedRequests;
    }
    IoResult result{0, validationError};
    result.buffer = std::move(request.buffer);
    return completedFuture(std::move(result));
  }

  std::promise<IoResult> promise;
  auto future = promise.get_future();
  std::optional<IoResult> rejectedResult;
  const auto enqueueTime = std::chrono::steady_clock::now();

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) {
      ++stats_.shutdownRejectedRequests;
      IoResult result{0, IoErrorCode::Shutdown};
      result.buffer = std::move(request.buffer);
      rejectedResult = std::move(result);
    } else {
      const auto priority = priorityIndex(request.priority);
      queues_[priority].push_back(QueuedRequest{
          nextRequestId_++,
          std::move(request),
          std::move(promise),
          enqueueTime});
      ++totalQueued_;
      stats_.queuedRequests[priority] = queues_[priority].size();
      ++stats_.acceptedRequests;
      stats_.maxObservedQueueDepth =
          std::max(stats_.maxObservedQueueDepth, totalQueued_);
    }
  }

  if (rejectedResult.has_value()) {
    promise.set_value(std::move(*rejectedResult));
    return future;
  }

  notifyWorker();

  return future;
}

void DiskIoSchedulerImpl::stopAndDrain() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
  }
  notifyWorker();

  std::lock_guard<std::mutex> joinLock(joinMutex_);
  if (worker_.joinable()) {
    worker_.join();
  }
}

DiskIoSchedulerStats DiskIoSchedulerImpl::stats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return snapshotStatsLocked();
}

DiskIoSchedulerStats DiskIoSchedulerImpl::snapshotStatsLocked() const {
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

void DiskIoSchedulerImpl::logStatsIfDueLocked(
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

void DiskIoSchedulerImpl::run() {
  while (true) {
    std::vector<ReadyResult> readyResults;
    wakeupEvent_.drain();
    auto completions = backend_->reap();
    std::vector<QueuedRequest> dispatchBatch;
    bool madeProgress = !completions.empty();
    bool shouldExit = false;
    bool shouldContinue = false;
    int waitTimeoutMs = -1;

    {
      std::lock_guard<std::mutex> lock(mutex_);
      applyCompletionsLocked(completions, readyResults);
      if (stopping_) {
        // Requests that have not reached the backend are owned only by the
        // scheduler, so their buffers can be safely returned as Shutdown. Once
        // a request is inflight, io_uring may still hold a pointer to its
        // buffer; that buffer must remain owned here until the real CQE is
        // reaped.
        failQueuedLocked(readyResults);
      }
      if (!stopping_) {
        dispatchBatch = collectDispatchBatchLocked();
      }
      shouldExit = stopping_ && drainedLocked() && dispatchBatch.empty();
    }

    std::vector<DispatchResult> dispatchResults;
    dispatchResults.reserve(dispatchBatch.size());
    for (auto& queued : dispatchBatch) {
      // Backend calls are deliberately made without mutex_. submit() should
      // remain a lightweight enqueue operation even if the backend enters the
      // kernel or spends time scanning completions.
      const auto submitTime = std::chrono::steady_clock::now();
      const auto status = backend_->submit(queued.requestId, queued.request);
      dispatchResults.push_back(
          DispatchResult{std::move(queued), status, submitTime});
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto dispatchMadeProgress =
          applyDispatchResultsLocked(dispatchResults, readyResults);
      shouldExit = shouldExit || (stopping_ && drainedLocked());
      shouldContinue =
          madeProgress || dispatchMadeProgress ||
          (hasQueuedRequestsLocked() &&
           inflight_.size() < adaptiveDepth_.currentDepth() &&
           !(!dispatchResults.empty() && !dispatchMadeProgress));
      logStatsIfDueLocked(std::chrono::steady_clock::now());
      waitTimeoutMs =
          computeWaitTimeoutMsLocked(std::chrono::steady_clock::now());
    }

    // Fulfill futures after releasing mutex_. A waiting caller may immediately
    // call back into submit()/stats(); doing set_value() under the scheduler
    // lock would make that wake-up contend with the worker and can deadlock
    // with future implementations that run continuations inline.
    fulfillReadyResults(readyResults);

    if (shouldExit) {
      return;
    }

    if (!shouldContinue) {
      // New work and backend completions are both fd-driven, so the worker can
      // sleep here without periodic polling. Timeouts are reserved for real
      // deadlines such as stats logging.
      waitForWorkerEvent(waitTimeoutMs);
    }
  }
}

int DiskIoSchedulerImpl::computeWaitTimeoutMsLocked(
    std::chrono::steady_clock::time_point now) {
  std::optional<std::chrono::steady_clock::duration> waitTime;
  auto includeDeadline =
      [&waitTime, now](std::chrono::steady_clock::time_point deadline) {
        const auto duration = deadline - now;
        waitTime = waitTime.has_value() ? std::min(*waitTime, duration)
                                        : std::optional(duration);
      };

  if (config_.enableStatsLogging) {
    includeDeadline(lastStatsLogTime_ + config_.statsLogInterval);
  }
  if (!waitTime.has_value()) {
    return -1;
  }
  return toEpollTimeoutMs(*waitTime);
}

void DiskIoSchedulerImpl::waitForWorkerEvent(int timeoutMs) {
  epoll_event events[2];
  while (true) {
    const int ready = ::epoll_wait(epollFd_.get(), events, 2, timeoutMs);
    if (ready >= 0) {
      return;
    }
    if (errno == EINTR) {
      continue;
    }
    BOLT_CHECK(false, "epoll_wait failed: {}", std::strerror(errno));
  }
}

void DiskIoSchedulerImpl::notifyWorker() const {
  wakeupEvent_.notify();
}

bool DiskIoSchedulerImpl::hasQueuedRequestsLocked() const {
  for (const auto& queue : queues_) {
    if (!queue.empty()) {
      return true;
    }
  }
  return false;
}

bool DiskIoSchedulerImpl::drainedLocked() const {
  return inflight_.empty() && !hasQueuedRequestsLocked();
}

std::optional<size_t> DiskIoSchedulerImpl::pickQueueLocked() {
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

std::vector<DiskIoSchedulerImpl::QueuedRequest>
DiskIoSchedulerImpl::collectDispatchBatchLocked() {
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
    --totalQueued_;
    stats_.queuedRequests[*priority] = queue.size();
    if (queue.empty()) {
      deficits_[*priority] = 0;
    }
    batch.push_back(std::move(queued));
  }
  return batch;
}

bool DiskIoSchedulerImpl::applyDispatchResultsLocked(
    std::vector<DispatchResult>& results,
    std::vector<ReadyResult>& readyResults) {
  bool madeProgress = false;
  if (!results.empty()) {
    ++stats_.submitBatches;
    stats_.submittedRequestsInBatches += results.size();
    stats_.maxSubmitBatchSize =
        std::max<uint64_t>(stats_.maxSubmitBatchSize, results.size());
    stats_.averageSubmitBatchSize =
        static_cast<double>(stats_.submittedRequestsInBatches) /
        static_cast<double>(stats_.submitBatches);
  }

  for (auto& dispatch : results) {
    const auto priority = priorityIndex(dispatch.queued.request.priority);
    if (dispatch.status == BackendSubmitStatus::RetryableBusy) {
      // A full SQ is backpressure, not user-visible IO failure. Put the request
      // back at the head so the next worker iteration can retry after reaping.
      auto& queue = queues_[priority];
      queue.push_front(std::move(dispatch.queued));
      ++totalQueued_;
      stats_.queuedRequests[priority] = queue.size();
      continue;
    }
    if (dispatch.status == BackendSubmitStatus::Failed) {
      madeProgress = true;
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

    madeProgress = true;
    ++stats_.submittedRequests[priority];
    const auto queueWaitUs =
        std::chrono::duration_cast<std::chrono::microseconds>(
            dispatch.submitTime - dispatch.queued.enqueueTime)
            .count();
    const auto measuredQueueWaitUs =
        queueWaitUs > 0 ? static_cast<uint64_t>(queueWaitUs) : 0;
    stats_.cumulativeQueueWaitUs += measuredQueueWaitUs;
    ++stats_.queueWaitSamples;
    stats_.averageQueueWaitUs =
        static_cast<double>(stats_.cumulativeQueueWaitUs) /
        static_cast<double>(stats_.queueWaitSamples);
    stats_.maxQueueWaitUs =
        std::max(stats_.maxQueueWaitUs, measuredQueueWaitUs);
    inflight_.emplace(
        dispatch.queued.requestId,
        InflightRequest{
            std::move(dispatch.queued.request),
            std::move(dispatch.queued.promise),
            dispatch.queued.enqueueTime,
            dispatch.submitTime});
    stats_.inflightRequests = inflight_.size();
    stats_.maxObservedInflightRequests = std::max<uint64_t>(
        stats_.maxObservedInflightRequests, inflight_.size());
  }
  return madeProgress;
}

void DiskIoSchedulerImpl::applyCompletionsLocked(
    std::vector<BackendCompletion>& completions,
    std::vector<ReadyResult>& readyResults) {
  if (!completions.empty()) {
    ++stats_.completionBatches;
    stats_.completedRequestsInBatches += completions.size();
    stats_.maxCompletionBatchSize =
        std::max<uint64_t>(stats_.maxCompletionBatchSize, completions.size());
    stats_.averageCompletionBatchSize =
        static_cast<double>(stats_.completedRequestsInBatches) /
        static_cast<double>(stats_.completionBatches);
  }

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
    const auto measuredLatencyUs =
        latencyUs > 0 ? static_cast<uint64_t>(latencyUs) : 0;
    const auto endToEndLatencyUs =
        std::chrono::duration_cast<std::chrono::microseconds>(
            now - inflight.enqueueTime)
            .count();
    const auto measuredEndToEndLatencyUs =
        endToEndLatencyUs > 0 ? static_cast<uint64_t>(endToEndLatencyUs) : 0;
    const auto firstLatencySample = stats_.latencySamples == 0;
    stats_.cumulativeLatencyUs += measuredLatencyUs;
    stats_.cumulativeEndToEndLatencyUs += measuredEndToEndLatencyUs;
    ++stats_.latencySamples;
    ++stats_.completedRequests;
    ++stats_.completedRequestsByPriority[priority];
    if (completion.result.ok() &&
        completion.result.bytes != inflight.request.buffer.length) {
      completion.result.error = IoErrorCode::ShortIo;
    }
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
        stats_.latencySamples == 0
            ? 0
            : static_cast<double>(stats_.cumulativeLatencyUs) /
                static_cast<double>(stats_.latencySamples);
    stats_.averageDeviceLatencyUs = stats_.averageLatencyUs;
    stats_.averageEndToEndLatencyUs =
        static_cast<double>(stats_.cumulativeEndToEndLatencyUs) /
        static_cast<double>(stats_.latencySamples);
    if (firstLatencySample || measuredLatencyUs < stats_.minLatencyUs) {
      stats_.minLatencyUs = measuredLatencyUs;
    }
    stats_.maxLatencyUs = std::max(stats_.maxLatencyUs, measuredLatencyUs);
    stats_.maxEndToEndLatencyUs =
        std::max(stats_.maxEndToEndLatencyUs, measuredEndToEndLatencyUs);
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

void DiskIoSchedulerImpl::failQueuedLocked(
    std::vector<ReadyResult>& readyResults) {
  for (size_t priority = 0; priority < kIoPriorityCount; ++priority) {
    auto& queue = queues_[priority];
    while (!queue.empty()) {
      auto queued = std::move(queue.front());
      queue.pop_front();
      --totalQueued_;
      IoResult result{0, IoErrorCode::Shutdown};
      result.buffer = std::move(queued.request.buffer);
      readyResults.push_back(
          ReadyResult{std::move(queued.promise), std::move(result)});
      ++stats_.completedRequests;
      ++stats_.completedRequestsByPriority[priority];
      ++stats_.failedRequests;
      ++stats_.failedRequestsByPriority[priority];
    }
    stats_.queuedRequests[priority] = 0;
    deficits_[priority] = 0;
  }

  totalQueued_ = 0;
}

} // namespace bytedance::bolt::memory::bm
