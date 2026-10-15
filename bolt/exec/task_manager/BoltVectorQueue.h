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

#include "bolt/common/future/BoltPromise.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/exec/Operator.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::exec {
class BoltVectorQueue {
 public:
  struct VectorQueueEntry {
    RowVectorPtr vector;
    uint64_t bytes;
  };

  explicit BoltVectorQueue(
      std::shared_ptr<memory::MemoryPool> outputBufferPool,
      uint64_t maxBytes)
      : outputBufferPool_(std::move(outputBufferPool)), maxBytes_(maxBytes) {}

  void setNumProducers(int32_t n);

  // Adds a batch of rows to the queue and returns kNotBlocked if the
  // producer may continue. Returns kWaitForConsumer if the queue is
  // full after the addition and sets '*future' to a future that is
  // realized when the producer may continue.
  BlockingReason enqueue(RowVectorPtr vector, ContinueFuture* future);

  // Returns nullptr when all producers are at end. Otherwise blocks.
  RowVectorPtr dequeue();

  void close(bool isCanceledOrFailed = false);

 private:
  // Owns the vectors in 'queue_', hence must be declared first.
  std::shared_ptr<memory::MemoryPool> outputBufferPool_;
  const uint64_t maxBytes_;
  std::deque<VectorQueueEntry> queue_;
  std::optional<int32_t> numProducers_;
  int32_t producersFinished_ = 0;
  uint64_t totalBytes_ = 0;
  // Blocks the producer if 'totalBytes' exceeds 'maxBytes' after
  // adding the result.
  std::mutex mutex_;
  std::vector<ContinuePromise> producerUnblockPromises_;
  bool consumerBlocked_ = false;
  ContinuePromise consumerPromise_;
  ContinueFuture consumerFuture_;
  bool closed_ = false;
};
} // namespace bytedance::bolt::exec
