#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <deque>
#include <future>
#include <vector>

#include "bolt/common/memory/bm/io/DrrDispatcher.h"
#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoResult.h"

namespace bytedance::bolt::memory::bm {

struct QueuedIoRequest {
  uint64_t requestId{0};
  IoRequest request;
  std::promise<IoResult> promise;
  std::chrono::steady_clock::time_point enqueueTime;
};

class IoRequestQueue {
 public:
  explicit IoRequestQueue(std::array<uint32_t, kIoPriorityCount> weights);

  void enqueue(QueuedIoRequest request);
  void returnToFront(QueuedIoRequest request);
  std::vector<QueuedIoRequest> collect(size_t maxCount);
  std::vector<QueuedIoRequest> drainAll();

  bool hasRequests() const;
  uint64_t totalQueued() const;
  std::array<uint64_t, kIoPriorityCount> queuedCounts() const;

 private:
  std::array<size_t, kIoPriorityCount> queueSizes() const;

  std::array<std::deque<QueuedIoRequest>, kIoPriorityCount> queues_;
  uint64_t totalQueued_{0};
  DrrDispatcher dispatcher_;
};

} // namespace bytedance::bolt::memory::bm
