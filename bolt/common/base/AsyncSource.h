/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include <folly/Unit.h>
#include <folly/executors/QueuedImmediateExecutor.h>
#include <folly/futures/Future.h>
#include <folly/system/ThreadName.h>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/GlobalParameters.h"
#include "bolt/common/base/Portability.h"
#include "bolt/common/future/BoltPromise.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/common/time/CpuWallTimer.h"
namespace bytedance::bolt {

// A future-like object that prefabricates Items on an executor and
// allows consumer threads to pick items as they are ready. If the
// consumer needs the item before the executor started making it, the
// consumer will make it instead. If multiple consumers request the
// same item, exactly one gets it. Propagates exceptions to the
// consumer.
template <typename Item>
class AsyncSource {
 public:
  explicit AsyncSource(
      std::function<std::unique_ptr<Item>()> make,
      bool allowException = false)
      : make_(make), allowException_(allowException) {}

  ~AsyncSource() {
    BOLT_CHECK(
        moved_ || closed_,
        "AsyncSource should be properly consumed or closed.");
  }

  // Makes an item if it is not already made. To be called on a background
  // executor.
  void prepare() {
    BOLT_TEST_ADJUST("bytedance::bolt::AsyncSource::prepare", this);
    std::function<std::unique_ptr<Item>()> make = nullptr;
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (!make_) {
        return;
      }
      making_ = true;
      std::swap(make, make_);
    }
    bool isAsyncThread = isAsyncPreloadThread();
    std::unique_ptr<Item> item;
    std::unique_ptr<ContinuePromise> promise;
    try {
      CpuWallTimer timer(timing_);
      item = make();
    } catch (std::exception& e) {
      std::unique_lock<std::mutex> l(mutex_);
      exception_ = std::current_exception();
      if (isAsyncThread && allowException_) {
        LOG(WARNING) << reinterpret_cast<uint64_t>(this)
                     << " AsyncSource failed and will retry, exception: "
                     << e.what();
        exception_ = nullptr;
        making_ = false;
        std::swap(make_, make);
        promise.swap(promise_);
        BOLT_CHECK_NULL(item_);
        BOLT_CHECK_NOT_NULL(make_);
        l.unlock();
        if (promise != nullptr) {
          promise->setValue();
        }
        return;
      }
    }
    {
      std::lock_guard<std::mutex> l(mutex_);
      BOLT_CHECK_NULL(item_);
      if (FOLLY_LIKELY(exception_ == nullptr)) {
        item_ = std::move(item);
      }
      making_ = false;
      promise.swap(promise_);
    }
    if (promise != nullptr) {
      promise->setValue();
    }
  }

  // Returns the item to the first caller and nullptr to subsequent callers. If
  // the item is preparing on the executor, waits for the item and otherwise
  // makes it on the caller thread.
  std::unique_ptr<Item> move() {
    BOLT_TEST_ADJUST("bytedance::bolt::AsyncSource::move", this);
    std::function<std::unique_ptr<Item>()> make = nullptr;
    ContinueFuture wait;
    {
      std::lock_guard<std::mutex> l(mutex_);
      moved_ = true;
      // 'making_' can be read atomically, 'exception' maybe not. So test
      // 'making' so as not to read half-assigned 'exception_'.
      if (!making_ && exception_) {
        std::rethrow_exception(exception_);
      }
      if (item_) {
        return std::move(item_);
      }
      if (promise_) {
        // Somebody else is now waiting for the item to be made.
        return nullptr;
      }
      if (making_) {
        promise_ = std::make_unique<ContinuePromise>();
        wait = promise_->getSemiFuture();
      } else {
        if (!make_) {
          return nullptr;
        }
        std::swap(make, make_);
      }
    }
    // Outside of mutex_.
    if (make) {
      try {
        return make();
      } catch (const std::exception&) {
        exception_ = std::current_exception();
        throw;
      }
    }
    auto& exec = folly::QueuedImmediateExecutor::instance();
    std::move(wait).via(&exec).wait();
    std::lock_guard<std::mutex> l(mutex_);
    if (exception_) {
      std::rethrow_exception(exception_);
    }
    if (!item_ && make_ && allowException_) {
      BOLT_CHECK_NULL(make);
      std::swap(make, make_);
      LOG(WARNING) << reinterpret_cast<uint64_t>(this)
                   << " AsyncSource failed after wait, begin retry";
      try {
        return make();
      } catch (const std::exception&) {
        exception_ = std::current_exception();
        throw;
      }
    }

    return std::move(item_);
  }

  // If true, move() will not block. But there is no guarantee that somebody
  // else will not get the item first.
  bool hasValue() const {
    tsan_lock_guard<std::mutex> l(mutex_);
    return item_ != nullptr || exception_ != nullptr;
  }

  /// Returns the timing of prepare(). If the item was made on the calling
  /// thread, the timing is 0 since only off-thread activity needs to be added
  /// to the caller's timing.
  const CpuWallTiming& prepareTiming() {
    return timing_;
  }

  /// This function assists the caller in ensuring that resources allocated in
  /// AsyncSource are promptly released:
  /// 1. Waits for the completion of the 'make_' function if it is executing in
  /// the thread pool.
  /// 2. Resets the 'make_' function if it has not started yet.
  /// 3. Cleans up the 'item_' if 'make_' has completed, but the result 'item_'
  /// has not been returned to the caller.
  void close() {
    if (closed_ || moved_) {
      return;
    }
    ContinueFuture wait;
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (making_) {
        promise_ = std::make_unique<ContinuePromise>();
        wait = promise_->getSemiFuture();
      } else if (make_) {
        make_ = nullptr;
      }
    }

    auto& exec = folly::QueuedImmediateExecutor::instance();
    std::move(wait).via(&exec).wait();
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (item_) {
        item_ = nullptr;
      }
      closed_ = true;
    }
  }

 private:
  mutable std::mutex mutex_;
  // True if 'prepare() is making the item.
  bool making_{false};
  std::unique_ptr<ContinuePromise> promise_;
  std::unique_ptr<Item> item_;
  std::function<std::unique_ptr<Item>()> make_;
  std::exception_ptr exception_;
  CpuWallTiming timing_;
  bool closed_{false};
  bool moved_{false};
  bool allowException_{false};
};
} // namespace bytedance::bolt
