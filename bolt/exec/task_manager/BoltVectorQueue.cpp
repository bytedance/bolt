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

#include "bolt/exec/task_manager/BoltVectorQueue.h"

namespace bytedance::bolt::exec {

void BoltVectorQueue::setNumProducers(int32_t n) {
  std::lock_guard<std::mutex> lock(mutex_);
  numProducers_ = n;
  if (consumerBlocked_) {
    consumerBlocked_ = false;
    consumerPromise_.setValue();
  }
}

BlockingReason BoltVectorQueue::enqueue(
    RowVectorPtr vector,
    bolt::ContinueFuture* future) {
  if (!vector) {
    std::lock_guard<std::mutex> l(mutex_);
    ++producersFinished_;
    if (consumerBlocked_) {
      consumerBlocked_ = false;
      consumerPromise_.setValue();
    }
    return BlockingReason::kNotBlocked;
  }

  std::lock_guard<std::mutex> l(mutex_);
  // Check inside 'mutex_'
  if (closed_) {
    throw std::runtime_error("Consumer cursor is closed");
  }
  auto copy = BaseVector::create<RowVector>(
      vector->type(), vector->size(), outputBufferPool_.get());
  copy->copy(vector.get(), 0, 0, vector->size());
  // The alias keeps both the pool and its vector alive for callers that retain
  // a result after the queue and task manager have been destroyed.
  auto* result = copy.get();
  vector = RowVectorPtr(
      result,
      [pool = outputBufferPool_, copy = std::move(copy)](RowVector*) mutable {
        copy.reset();
        pool.reset();
      });
  const auto bytes = vector->retainedSize();
  VectorQueueEntry entry{std::move(vector), bytes};
  queue_.push_back(std::move(entry));
  totalBytes_ += entry.bytes;
  if (consumerBlocked_) {
    consumerBlocked_ = false;
    consumerPromise_.setValue();
  }

  if (totalBytes_ > maxBytes_) {
    auto [unblockPromise, unblockFuture] = makeBoltContinuePromiseContract();
    producerUnblockPromises_.emplace_back(std::move(unblockPromise));
    *future = std::move(unblockFuture);
    return BlockingReason::kWaitForConsumer;
  }
  return BlockingReason::kNotBlocked;
}

RowVectorPtr BoltVectorQueue::dequeue() {
  for (;;) {
    RowVectorPtr result;
    bool hasValue = false;
    std::vector<ContinuePromise> mayContinue;
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (!queue_.empty()) {
        hasValue = true;
        auto entry = std::move(queue_.front());
        queue_.pop_front();
        totalBytes_ -= entry.bytes;
        result = std::move(entry.vector);
        if (totalBytes_ <= maxBytes_ / 2) {
          mayContinue = std::move(producerUnblockPromises_);
        }
      } else if (
          closed_ ||
          (numProducers_.has_value() && producersFinished_ == numProducers_)) {
        return nullptr;
      }

      if (!hasValue) {
        consumerBlocked_ = true;
        consumerPromise_ = ContinuePromise();
        consumerFuture_ = consumerPromise_.getFuture();
      }
    }
    // outside of 'mutex_'
    for (auto& promise : mayContinue) {
      promise.setValue();
    }
    if (hasValue) {
      return result;
    }
    consumerFuture_.wait();
  }
}

void BoltVectorQueue::close(bool isCanceledOrFailed) {
  std::lock_guard<std::mutex> l(mutex_);
  closed_ = true;
  // Close a warning while the consumer destructed.
  if (consumerBlocked_) {
    consumerBlocked_ = false;
    consumerPromise_.setValue();
  }
  for (auto& promise : producerUnblockPromises_) {
    promise.setValue();
  }
  producerUnblockPromises_.clear();

  if (isCanceledOrFailed) {
    queue_.clear();
    totalBytes_ = 0;
  }
}

} // namespace bytedance::bolt::exec
