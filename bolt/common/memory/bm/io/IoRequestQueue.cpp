#include "bolt/common/memory/bm/io/IoRequestQueue.h"

#include <algorithm>
#include <utility>

namespace bytedance::bolt::memory::bm {

IoRequestQueue::IoRequestQueue(
    std::array<uint32_t, kIoPriorityCount> weights)
    : dispatcher_(weights) {}

void IoRequestQueue::enqueue(QueuedIoRequest request) {
  const auto priority = priorityIndex(request.request.priority);
  queues_[priority].push_back(std::move(request));
  ++totalQueued_;
}

void IoRequestQueue::returnToFront(QueuedIoRequest request) {
  const auto priority = priorityIndex(request.request.priority);
  queues_[priority].push_front(std::move(request));
  ++totalQueued_;
  // The previous pick consumed one DRR credit, but the backend did not accept
  // the request. Restoring that credit prevents RetryableBusy from changing
  // priority fairness merely because the ring was temporarily full.
  dispatcher_.restore(priority);
}

std::vector<QueuedIoRequest> IoRequestQueue::collect(size_t maxCount) {
  std::vector<QueuedIoRequest> batch;
  batch.reserve(std::min<uint64_t>(maxCount, totalQueued_));

  while (batch.size() < maxCount && hasRequests()) {
    const auto priority = dispatcher_.pick(queueSizes());
    if (!priority.has_value()) {
      break;
    }

    auto& queue = queues_[*priority];
    auto request = std::move(queue.front());
    queue.pop_front();
    --totalQueued_;
    if (queue.empty()) {
      dispatcher_.reset(*priority);
    }
    batch.push_back(std::move(request));
  }

  return batch;
}

std::vector<QueuedIoRequest> IoRequestQueue::drainAll() {
  std::vector<QueuedIoRequest> drained;
  drained.reserve(totalQueued_);

  for (size_t priority = 0; priority < kIoPriorityCount; ++priority) {
    auto& queue = queues_[priority];
    while (!queue.empty()) {
      drained.push_back(std::move(queue.front()));
      queue.pop_front();
    }
    dispatcher_.reset(priority);
  }

  totalQueued_ = 0;
  return drained;
}

bool IoRequestQueue::hasRequests() const {
  return totalQueued_ > 0;
}

uint64_t IoRequestQueue::totalQueued() const {
  return totalQueued_;
}

std::array<uint64_t, kIoPriorityCount> IoRequestQueue::queuedCounts() const {
  std::array<uint64_t, kIoPriorityCount> counts{};
  for (size_t i = 0; i < kIoPriorityCount; ++i) {
    counts[i] = queues_[i].size();
  }
  return counts;
}

std::array<size_t, kIoPriorityCount> IoRequestQueue::queueSizes() const {
  std::array<size_t, kIoPriorityCount> sizes{};
  for (size_t i = 0; i < kIoPriorityCount; ++i) {
    sizes[i] = queues_[i].size();
  }
  return sizes;
}

} // namespace bytedance::bolt::memory::bm
