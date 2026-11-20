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
