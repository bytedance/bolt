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
#include <gtest/gtest.h>
#include <atomic>
#include <cstdint>
#include <exception>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/Exceptions.h"
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

} // namespace bytedance::bolt::memory::sparksql
