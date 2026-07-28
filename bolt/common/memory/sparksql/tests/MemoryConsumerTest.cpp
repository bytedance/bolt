/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include <folly/Random.h>
#include <folly/system/ThreadName.h>
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/GlobalParameters.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
#include "bolt/common/memory/sparksql/MemoryConsumer.h"
#include "bolt/common/memory/sparksql/TaskMemoryManager.h"
#include "bolt/common/memory/sparksql/WeakPtrHelper.h"

using namespace ::testing;
using namespace bytedance::bolt::memory::sparksql;
namespace bytedance::bolt::memory::sparksql {

class TestMemoryConsumer
    : public MemoryConsumer,
      public std::enable_shared_from_this<TestMemoryConsumer> {
 public:
  explicit TestMemoryConsumer(TaskMemoryManagerWeakPtr taskMemoryManager)
      : MemoryConsumer(taskMemoryManager) {}

  ~TestMemoryConsumer() override = default;

  int64_t spill(int64_t size) override {
    return 0;
  }

  int64_t acquireMemory(int64_t size) override {
    auto tmm = lock_or_throw(taskMemoryManager_);
    int64_t granted = tmm->acquireExecutionMemory(size, weak_from_this());
    used_ += granted;
    return granted;
  }

  void freeMemory(int64_t size) override {
    BOLT_CHECK(size <= used_);
    auto tmm = lock_or_throw(taskMemoryManager_);
    int64_t released = tmm->releaseExecutionMemory(size, weak_from_this());
    used_ -= released;
  }
};

class MemoryConsumerTest : public testing::Test {
 public:
 protected:
  folly::Random::DefaultGenerator rng_;
};

TEST_F(MemoryConsumerTest, basic) {
  const int64_t taskAttemptId = 7;
  const int64_t capacity = 1 * 1024 * 1024 * 1024;

  auto memoryPool = std::make_shared<ExecutionMemoryPool>();
  memoryPool->setPoolSize(capacity);
  auto taskMemoryManager =
      std::make_shared<TaskMemoryManager>(memoryPool, taskAttemptId);

  std::shared_ptr<MemoryConsumer> consumer =
      std::make_shared<TestMemoryConsumer>(taskMemoryManager);
  // consume
  for (int i = 0; i < 1024; ++i) {
    consumer->acquireMemory(100);
  }
  BOLT_CHECK(
      memoryPool->memoryUsed() == 1024 * 100,
      "pool expect use 1024 * 100 bytes");
  BOLT_CHECK(memoryPool->poolSize() == capacity, "expect poolSize == capacity");
  consumer->freeMemory(100 * 1024);
  BOLT_CHECK(memoryPool->memoryUsed() == 0, "Expect free all memory");
}

TEST_F(MemoryConsumerTest, debugStringDoesNotRequireGlobalInitialization) {
  EXPECT_FALSE(ExecutionMemoryPool::inited());
  EXPECT_NO_THROW({
    const auto detail = ExecutionMemoryPool::debugString();
    EXPECT_FALSE(detail.empty());
  });
}

TEST_F(MemoryConsumerTest, multiTask) {
  const int runs = 10;
  for (int run = 0; run < runs; ++run) {
    int64_t capacity = 1 * 1024 * 1024 * 1024;
    int64_t taskAttemptId = 7;

    auto memoryPool = std::make_shared<ExecutionMemoryPool>();
    memoryPool->setPoolSize(capacity);

    const int64_t threadNum = 100;
    const int64_t requestPeerThread = 10000;

    std::vector<std::thread> workers(threadNum);
    std::vector<MemoryConsumerPtr> consumers(threadNum, nullptr);
    std::vector<int64_t> memoryOccupy(threadNum, 0);
    std::vector<TaskMemoryManagerPtr> managers(threadNum, nullptr);

    for (int i = 0; i < threadNum; ++i) {
      managers[i] = std::make_shared<TaskMemoryManager>(memoryPool, i);
      consumers[i] = std::make_shared<TestMemoryConsumer>(managers[i]);
    }

    for (int i = 0; i < threadNum; ++i) {
      workers[i] = std::thread([i, &consumers, &memoryOccupy, this]() {
        for (int j = 0; j < requestPeerThread; ++j) {
          int64_t need = folly::Random::rand32(1, 1000, rng_);
          consumers[i]->acquireMemory(need);
          memoryOccupy[i] += need;
        }
      });
    }

    for (int i = 0; i < threadNum; ++i) {
      workers[i].join();
    }

    BOLT_CHECK(
        std::accumulate(memoryOccupy.begin(), memoryOccupy.end(), 0) ==
            memoryPool->memoryUsed(),
        "Expect allocated == memoryPool->memoryUsed()");

    for (int i = 0; i < threadNum; ++i) {
      BOLT_CHECK(consumers[i]->getUsed() == memoryOccupy[i]);
      consumers[i]->freeMemory(memoryOccupy[i]);
    }
  }
}

class SpillMemoryConsumer
    : public MemoryConsumer,
      public std::enable_shared_from_this<SpillMemoryConsumer> {
 public:
  explicit SpillMemoryConsumer(TaskMemoryManagerWeakPtr taskMemoryManager)
      : MemoryConsumer(taskMemoryManager) {}

  ~SpillMemoryConsumer() override = default;

  bool hasSpilled() {
    return hasSpilled_;
  }

  int64_t spill(int64_t size) override {
    if (hasSpilled_) {
      return 0;
    }
    const int64_t spillReleased = 100;
    // In reality, bolt will call MemoryConsumer::freeMemory after spill some
    // data
    this->freeMemory(spillReleased);
    hasSpilled_ = true;
    return spillReleased;
  }

  int64_t acquireMemory(int64_t size) override {
    auto tmm = lock_or_throw(taskMemoryManager_);
    int64_t granted = tmm->acquireExecutionMemory(size, weak_from_this());
    used_ += granted;
    return granted;
  }

  void freeMemory(int64_t size) override {
    BOLT_CHECK(size <= used_, "size is {}, used_ is {}", size, used_);
    auto tmm = lock_or_throw(taskMemoryManager_);
    int64_t released = tmm->releaseExecutionMemory(size, weak_from_this());
    used_ -= released;
  }

 private:
  bool hasSpilled_{false};
};

TEST_F(MemoryConsumerTest, expectSpill) {
  int64_t capacity = 1 * 1024 * 1024 * 1024;
  int64_t taskAttemptId = 7;

  auto memoryPool = std::make_shared<ExecutionMemoryPool>();
  memoryPool->setPoolSize(capacity);

  auto taskMemoryManager =
      std::make_shared<TaskMemoryManager>(memoryPool, taskAttemptId);
  std::shared_ptr<MemoryConsumer> consumer =
      std::make_shared<SpillMemoryConsumer>(taskMemoryManager);
  // consumer1 requests half mem, consumer2 requests (half + can Spilled=100 +
  // can't Spilled=1)
  const int64_t firstReq = capacity / 2;
  const int64_t secondReq = capacity / 2 + 100 + 1;
  // got mem success
  const int64_t firstAcq = consumer->acquireMemory(firstReq);
  BOLT_CHECK(
      firstAcq == firstReq,
      "expect firstAcq == firstReq, but firstAcq={}, firstReq={}",
      firstAcq,
      firstReq);
  EXPECT_TRUE(consumer->getUsed() == firstAcq);
  // can't get enough mem for extra 1 byte
  const int64_t secondAcq = consumer->acquireMemory(secondReq);
  BOLT_CHECK(
      secondAcq == secondReq - 1,
      "expect secondAcq == secondReq - 1, but secondAcq={}, secondReq={}",
      secondAcq,
      secondReq);
  EXPECT_TRUE(consumer->getUsed() == capacity);
  // repay mem
  consumer->freeMemory(capacity);
  EXPECT_TRUE(consumer->getUsed() == 0);
  auto testConsumer = std::dynamic_pointer_cast<SpillMemoryConsumer>(consumer);
  EXPECT_TRUE(testConsumer->hasSpilled() == true);
}

TEST_F(MemoryConsumerTest, expectWait) {
  int64_t capacity = 1 * 1024 * 1024 * 1024;

  auto memoryPool = std::make_shared<ExecutionMemoryPool>();
  memoryPool->setPoolSize(capacity);

  std::atomic_bool threadBarrier = false;

  int64_t threadSleepFor = folly::Random::rand32(10, 30, rng_);

  std::thread overAccuqireThread(
      [&memoryPool, &capacity, &threadBarrier, &threadSleepFor]() {
        auto taskMemoryManager =
            std::make_shared<TaskMemoryManager>(memoryPool, 10);
        std::shared_ptr<MemoryConsumer> consumer =
            std::make_shared<TestMemoryConsumer>(taskMemoryManager);
        // 1. acquire most of pool's memory
        int64_t acquire = consumer->acquireMemory(capacity / 10 * 9);
        // 2. expect acquire success, because only 1 task now
        BOLT_CHECK(acquire == capacity / 10 * 9);
        // 3. set flag, make waitForFreeThread begin
        threadBarrier.store(true, std::memory_order_seq_cst);
        std::this_thread::sleep_for(std::chrono::seconds(threadSleepFor));
        // 5. free memory
        consumer->freeMemory(capacity / 10 * 9);
      });

  std::thread waitForFreeThread(
      [&memoryPool, &capacity, &threadBarrier, &threadSleepFor]() {
        auto start = std::chrono::high_resolution_clock::now();
        // wait for flag be true
        while (!threadBarrier.load(std::memory_order_seq_cst)) {
          ;
        }
        auto taskMemoryManager =
            std::make_shared<TaskMemoryManager>(memoryPool, 99);
        std::shared_ptr<MemoryConsumer> consumer =
            std::make_shared<TestMemoryConsumer>(taskMemoryManager);
        // 4. begin acquire memory, will wait (because expect at least 1/2N
        // memory)
        int64_t acquire = consumer->acquireMemory(capacity / 2);
        // 6. will be notified, acquire success
        BOLT_CHECK(acquire == capacity / 2);
        auto finish = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed = finish - start;
        // 7. expect will waiting for threadSleepFor seconds
        BOLT_CHECK(elapsed.count() >= threadSleepFor);
        // clean
        consumer->freeMemory(acquire);
      });

  overAccuqireThread.join();
  waitForFreeThread.join();
}

TEST_F(MemoryConsumerTest, multiConsumer) {
  int64_t capacity = 1 * 1024 * 1024 * 1024;

  auto memoryPool = std::make_shared<ExecutionMemoryPool>();
  memoryPool->setPoolSize(capacity);
  auto taskMemoryManager = std::make_shared<TaskMemoryManager>(memoryPool, 1);

  const int64_t totalConsumers = 1000;

  std::vector<std::thread> workers(totalConsumers);
  std::vector<MemoryConsumerPtr> consumers(totalConsumers);
  for (int64_t j = 0; j < totalConsumers; ++j) {
    consumers[j] = std::make_shared<TestMemoryConsumer>(taskMemoryManager);
  }
  std::vector<int64_t> consumeMem(totalConsumers, 0);

  for (int64_t i = 0; i < totalConsumers; ++i) {
    workers[i] = std::thread([&memoryPool,
                              &i,
                              &consumer = consumers[i],
                              memRecord = &consumeMem[i],
                              this]() {
      for (int64_t j = 0; j < 1000; ++j) {
        int64_t randomConsume = folly::Random::rand32(1, 100, rng_);
        int64_t acquire = consumer->acquireMemory(randomConsume);
        *memRecord = *memRecord + acquire;
      }
    });
  }

  for (int64_t i = 0; i < totalConsumers; ++i) {
    workers[i].join();
  }

  for (int64_t i = 0; i < totalConsumers; ++i) {
    BOLT_CHECK(
        consumers[i]->getUsed() == consumeMem[i],
        "consumers[i]->getUsed()={}, consumeMem[i]={}",
        consumers[i]->getUsed(),
        consumeMem[i]);
    consumers[i]->freeMemory(consumeMem[i]);
  }
}

TEST_F(MemoryConsumerTest, scopedDisableDynamicMemoryQuotaManagerForTask) {
  DynamicMemoryQuotaManagerOption option;
  option.enable = true;
  option.quotaTriggerRatio = 0.0;
  option.rssMinRatio = 0.0;
  option.rssMaxRatio = 0.0;
  option.extendMinRatio = 2.0;
  option.extendMaxRatio = 2.0;
  option.extendScaleRatio = 1.0;
  option.sampleRatio = 1.0;
  option.sampleSize = 1;
  option.changeThresholdRatio = 0.0;
  option.logPrintFreq = 0.0;

  constexpr int64_t kTaskAttemptId = 7;
  auto memoryPool = std::make_shared<ExecutionMemoryPool>(1, option);
  memoryPool->setPoolSize(100);

  EXPECT_EQ(memoryPool->acquireMemory(100, kTaskAttemptId), 100);

  {
    auto scopedDisable =
        memoryPool->scopedDisableDynamicMemoryQuotaManagerForTask(
            kTaskAttemptId);
    EXPECT_EQ(memoryPool->acquireMemory(1, kTaskAttemptId), 0);
  }

  EXPECT_EQ(memoryPool->acquireMemory(1, kTaskAttemptId), 1);
}

TEST_F(
    MemoryConsumerTest,
    scopedDisableDynamicMemoryQuotaManagerForTaskSupportsNesting) {
  DynamicMemoryQuotaManagerOption option;
  option.enable = true;
  option.quotaTriggerRatio = 0.0;
  option.rssMinRatio = 0.0;
  option.rssMaxRatio = 0.0;
  option.extendMinRatio = 2.0;
  option.extendMaxRatio = 2.0;
  option.extendScaleRatio = 1.0;
  option.sampleRatio = 1.0;
  option.sampleSize = 1;
  option.changeThresholdRatio = 0.0;
  option.logPrintFreq = 0.0;

  constexpr int64_t kTaskAttemptId = 7;
  auto memoryPool = std::make_shared<ExecutionMemoryPool>(1, option);
  memoryPool->setPoolSize(100);

  EXPECT_EQ(memoryPool->acquireMemory(100, kTaskAttemptId), 100);

  auto outerScopedDisable =
      memoryPool->scopedDisableDynamicMemoryQuotaManagerForTask(kTaskAttemptId);
  {
    auto innerScopedDisable =
        memoryPool->scopedDisableDynamicMemoryQuotaManagerForTask(
            kTaskAttemptId);
    EXPECT_EQ(memoryPool->acquireMemory(1, kTaskAttemptId), 0);
  }

  EXPECT_EQ(memoryPool->acquireMemory(1, kTaskAttemptId), 0);
}

TEST_F(
    MemoryConsumerTest,
    scopedDisableDynamicMemoryQuotaManagerForTaskSurvivesReleaseAllMemory) {
  DynamicMemoryQuotaManagerOption option;
  option.enable = true;
  option.quotaTriggerRatio = 0.0;
  option.rssMinRatio = 0.0;
  option.rssMaxRatio = 0.0;
  option.extendMinRatio = 2.0;
  option.extendMaxRatio = 2.0;
  option.extendScaleRatio = 1.0;
  option.sampleRatio = 1.0;
  option.sampleSize = 1;
  option.changeThresholdRatio = 0.0;
  option.logPrintFreq = 0.0;

  constexpr int64_t kTaskAttemptId = 7;
  auto memoryPool = std::make_shared<ExecutionMemoryPool>(1, option);
  memoryPool->setPoolSize(100);

  EXPECT_EQ(memoryPool->acquireMemory(100, kTaskAttemptId), 100);

  {
    auto scopedDisable =
        memoryPool->scopedDisableDynamicMemoryQuotaManagerForTask(
            kTaskAttemptId);
    EXPECT_EQ(memoryPool->releaseMemory(100, kTaskAttemptId), 100);

    EXPECT_EQ(memoryPool->acquireMemory(1, kTaskAttemptId), 0);
  }

  EXPECT_EQ(memoryPool->acquireMemory(1, kTaskAttemptId), 1);
}

TEST_F(MemoryConsumerTest, toStringRacesWithConcurrentMemoryUpdates) {
  constexpr int64_t kCapacity = 1 << 20;
  constexpr int64_t kWorkerThreads = 8;
  constexpr int64_t kWriterIterations = 20'000;
  constexpr int64_t kStringifierThreads = 4;
  constexpr int64_t kStringifyIterations = 20'000;

  auto memoryPool = std::make_shared<ExecutionMemoryPool>();
  memoryPool->setPoolSize(kCapacity);

  std::vector<TaskMemoryManagerPtr> managers(kWorkerThreads);
  std::vector<std::shared_ptr<TestMemoryConsumer>> consumers(kWorkerThreads);
  for (int64_t i = 0; i < kWorkerThreads; ++i) {
    managers[i] = std::make_shared<TaskMemoryManager>(memoryPool, i);
    consumers[i] = std::make_shared<TestMemoryConsumer>(managers[i]);
  }

  std::atomic_bool start{false};
  std::atomic_bool stop{false};
  std::atomic<int64_t> stringifyCalls{0};
  std::exception_ptr backgroundFailure{nullptr};
  std::mutex failureLock;

  auto recordFailure = [&](std::exception_ptr error) {
    std::lock_guard<std::mutex> guard(failureLock);
    if (backgroundFailure == nullptr) {
      backgroundFailure = error;
    }
    stop.store(true, std::memory_order_release);
  };

  std::vector<std::thread> writers;
  writers.reserve(kWorkerThreads);
  for (int64_t i = 0; i < kWorkerThreads; ++i) {
    writers.emplace_back([&, i]() {
      try {
        while (!start.load(std::memory_order_acquire)) {
        }

        auto& consumer = consumers[i];
        for (int64_t iter = 0;
             iter < kWriterIterations && !stop.load(std::memory_order_acquire);
             ++iter) {
          const auto request = 1 + ((iter + i * 17) % 127);
          if ((iter & 1) == 0) {
            consumer->acquireMemory(request);
          } else {
            const auto toFree = std::min<int64_t>(consumer->getUsed(), request);
            if (toFree > 0) {
              consumer->freeMemory(toFree);
            }
          }
        }

        const auto remaining = consumer->getUsed();
        if (remaining > 0) {
          consumer->freeMemory(remaining);
        }
      } catch (...) {
        recordFailure(std::current_exception());
      }
    });
  }

  std::vector<std::thread> stringifiers;
  stringifiers.reserve(kStringifierThreads);
  for (int64_t i = 0; i < kStringifierThreads; ++i) {
    stringifiers.emplace_back([&]() {
      try {
        while (!start.load(std::memory_order_acquire)) {
        }

        for (int64_t iter = 0; iter < kStringifyIterations &&
             !stop.load(std::memory_order_acquire);
             ++iter) {
          const auto detail = memoryPool->toString();
          if (detail.empty()) {
            throw std::runtime_error(
                "ExecutionMemoryPool::toString returned empty");
          }
          stringifyCalls.fetch_add(1, std::memory_order_relaxed);
        }
      } catch (...) {
        recordFailure(std::current_exception());
      }
    });
  }

  start.store(true, std::memory_order_release);

  for (auto& writer : writers) {
    writer.join();
  }
  stop.store(true, std::memory_order_release);
  for (auto& stringifier : stringifiers) {
    stringifier.join();
  }

  if (backgroundFailure != nullptr) {
    std::rethrow_exception(backgroundFailure);
  }

  EXPECT_GT(stringifyCalls.load(std::memory_order_relaxed), 0);
  EXPECT_EQ(memoryPool->memoryUsed(), 0);
}

// Regression test for the deadlock guard at
// ExecutionMemoryPool::acquireMemory (see comment "if async threads unreserve
// non-enough memory, there may be a dead lock ..."). Reproduces the scenario
// described in https://github.com/bytedance/bolt/issues/781:
//
// 1. Main task holds most of the pool. It calls acquireMemory for more bytes
//    than are free and, per Spark's 1/2N rule, blocks inside cv_.wait_for
//    while still holding a share of the pool.
// 2. An async preload thread (thread name starts with "AsyncLoadThr") also
//    calls acquireMemory. Without the guard, it would see the same
//    "toGrant < numBytes && curMem + toGrant < minMemoryPerTask" condition
//    and enter cv_.wait_for too. Both threads then wait for the other side
//    to release memory that nobody is going to release, so both hang until
//    minMemoryMaxWaitMs_ expires (300s in prod) -> effectively a deadlock.
// 3. With the guard, the async thread returns 0 immediately so its caller
//    can skip the unreserve(bytes) path and let the main thread progress.
TEST_F(MemoryConsumerTest, asyncPreloadThreadDoesNotDeadlockOnPartialGrant) {
  constexpr int64_t kCapacity = 1024;
  constexpr int64_t kMainTaskAttemptId = 1;
  constexpr int64_t kAsyncTaskAttemptId = 2;

  auto memoryPool = std::make_shared<ExecutionMemoryPool>();
  memoryPool->setPoolSize(kCapacity);

  // Step 1: register the main task and grab almost everything so any further
  // request is guaranteed to be a partial grant.
  ASSERT_EQ(
      memoryPool->acquireMemory(kCapacity - 1, kMainTaskAttemptId),
      kCapacity - 1);

  std::atomic_bool mainEnteredWait{false};
  std::atomic_bool mainDone{false};
  std::atomic_bool asyncDone{false};
  int64_t asyncGranted = -1;

  // Step 2: main task now asks for the full pool. Only 1 byte is free, and
  // the 1/2N floor (1024 / (2 * 1) = 512) is not met, so the main thread
  // blocks inside cv_.wait_for holding its share of the pool.
  std::thread mainThread([&]() {
    mainEnteredWait.store(true, std::memory_order_release);
    // Blocks until the async thread creates the second task and then
    // releases its memory below.
    int64_t got = memoryPool->acquireMemory(kCapacity, kMainTaskAttemptId);
    // Note: after the async task appears, numActiveTasks == 2, so the
    // 1/2N floor drops to 256 and the main task can be served with the
    // remaining free budget (>= 256) once the cv fires.
    EXPECT_GT(got, 0);
    memoryPool->releaseMemory(got, kMainTaskAttemptId);
    mainDone.store(true, std::memory_order_release);
  });

  // Wait until the main thread has entered acquireMemory. A short sleep is
  // enough because the second acquireMemory call blocks on cv_.wait_for.
  while (!mainEnteredWait.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Step 3: async preload thread requests memory. The guard must make it
  // return 0 promptly instead of waiting on cv_.
  std::thread asyncThread([&]() {
    folly::setThreadName(std::string(kAsyncPreloadThreadName) + "1");
    ASSERT_TRUE(isAsyncPreloadThread());
    const auto start = std::chrono::steady_clock::now();
    asyncGranted = memoryPool->acquireMemory(kCapacity, kAsyncTaskAttemptId);
    const auto elapsed = std::chrono::steady_clock::now() - start;
    // The whole point of the guard: don't wait on cv_.
    EXPECT_LT(
        std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(), 5);
    asyncDone.store(true, std::memory_order_release);
  });

  asyncThread.join();
  EXPECT_TRUE(asyncDone.load(std::memory_order_acquire));
  EXPECT_EQ(asyncGranted, 0);

  // Registering the async task made numActiveTasks == 2, which lowered the
  // 1/2N floor and notified cv_. The main thread should now be able to
  // finish. Give it a bounded window; a real deadlock would exceed it.
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (!mainDone.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(mainDone.load(std::memory_order_acquire))
      << "main thread appears to be deadlocked with the async preload thread";

  mainThread.join();
  memoryPool->releaseMemory(kCapacity - 1, kMainTaskAttemptId);
  EXPECT_EQ(memoryPool->memoryUsed(), 0);
}

// Sanity check: a regular (non-preload) thread must still take the cv_.wait_for
// path on a partial grant, so we don't accidentally regress the Spark
// semantics for main-execution threads.
TEST_F(MemoryConsumerTest, nonAsyncPreloadThreadStillWaitsOnPartialGrant) {
  constexpr int64_t kCapacity = 1024;
  constexpr int64_t kTaskAOwnerId = 1;
  constexpr int64_t kTaskBOwnerId = 2;

  auto memoryPool = std::make_shared<ExecutionMemoryPool>();
  memoryPool->setPoolSize(kCapacity);

  // Task A holds most of the pool.
  ASSERT_EQ(
      memoryPool->acquireMemory(kCapacity - 1, kTaskAOwnerId), kCapacity - 1);

  std::atomic_bool started{false};
  int64_t granted = -1;

  std::thread waiter([&]() {
    // Ensure this thread is NOT recognised as an async preload thread.
    ASSERT_FALSE(isAsyncPreloadThread());
    started.store(true, std::memory_order_release);
    granted = memoryPool->acquireMemory(kCapacity, kTaskBOwnerId);
  });

  while (!started.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Freeing memory now should unblock the waiter and let it get a full
  // grant (proving it really was waiting on cv_ rather than returning 0).
  memoryPool->releaseMemory(kCapacity - 1, kTaskAOwnerId);
  waiter.join();

  EXPECT_EQ(granted, kCapacity);
  memoryPool->releaseMemory(granted, kTaskBOwnerId);
  EXPECT_EQ(memoryPool->memoryUsed(), 0);
}

} // namespace bytedance::bolt::memory::sparksql
