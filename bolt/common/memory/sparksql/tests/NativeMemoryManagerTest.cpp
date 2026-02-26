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

#include <arrow/memory_pool.h>
#include <folly/Random.h>
#include <gtest/gtest.h>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <exception>
#include <list>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SuccinctPrinter.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/memory/MemoryPoolForGluten.h"
#include "bolt/common/memory/sparksql/AllocationListener.h"
#include "bolt/common/memory/sparksql/MemoryConsumer.h"
#include "bolt/common/memory/sparksql/MemoryTarget.h"
#include "bolt/common/memory/sparksql/NativeMemoryManagerFactory.h"
#include "bolt/common/memory/sparksql/Spiller.h"
#include "bolt/common/memory/sparksql/SpillerPhase.h"
#include "bolt/common/memory/sparksql/TaskMemoryManager.h"

using namespace ::testing;
using namespace bytedance::bolt::memory::sparksql;
namespace bytedance::bolt::memory::sparksql {

class NativeMemoryManagerTest : public testing::Test {
 protected:
  folly::Random::DefaultGenerator rng_;
};

TEST_F(NativeMemoryManagerTest, basic) {
  const int64_t capacity = 1 * 1024 * 1024 * 1024;
  const int64_t taskAttemptId = 996;

  auto memoryPool = std::make_shared<ExecutionMemoryPool>();
  memoryPool->setPoolSize(capacity);

  auto tmm = std::make_shared<TaskMemoryManager>(memoryPool, taskAttemptId);

  NativeMemoryManagerFactoryParam param{
      .name = "TEST",
      .memoryIsolation = false,
      .conservativeTaskOffHeapMemorySize = 0,
      .overAcquiredRatio = 0.3,
      .taskMemoryManager = tmm,
      .sessionConf = {}};

  auto ans = NativeMemoryManagerFactory::contextInstance(param);
  auto boltMemoryManager = ans->getManager();
  auto pool = boltMemoryManager->getLeafMemoryPool();
  arrow::MemoryPool* arrowPool = boltMemoryManager->getArrowMemoryPool();

  int64_t allocTimes = 100000;

  std::vector<std::pair<void*, int64_t>> allocMem(allocTimes);
  std::vector<std::pair<uint8_t*, int64_t>> arrowAllocMem(allocTimes);

  for (int i = 0; i < allocTimes; ++i) {
    int64_t allocSize = folly::Random::rand32(1, 10000, rng_);
    allocMem[i] = std::make_pair(pool->allocate(allocSize), allocSize);

    uint8_t* out;
    arrowPool->Allocate(allocSize, &out);
    arrowAllocMem[i] = std::make_pair(out, allocSize);

    std::memset(allocMem[i].first, '1', allocSize);
    std::memset(out, '0', allocSize);
  }
  LOG(INFO) << "memoryUsed=" << memoryPool->memoryUsed();

  for (int i = 0; i < allocTimes; ++i) {
    pool->free(allocMem[i].first, allocMem[i].second);
    arrowPool->Free(arrowAllocMem[i].first, arrowAllocMem[i].second);
  }

  boltMemoryManager->shrink(memoryPool->memoryUsed());
  LOG(INFO) << "memoryUsed=" << memoryPool->memoryUsed();
}

TEST_F(NativeMemoryManagerTest, mustUseMemoryPoolForGluten) {
  class NoopListener final : public AllocationListener {
   public:
    ~NoopListener() override {}

    int64_t allocationChanged(int64_t size) override {
      return size;
    }

    int64_t getUsedBytes() override {
      return 0;
    }
  };

  AllocationListenerPtr listener = std::make_shared<NoopListener>();
  ArbitratorFactoryRegister afr(listener);
  bolt::memory::MemoryManager::Options mmOptions;
  mmOptions.alignment = bolt::memory::MemoryAllocator::kMaxAlignment;
  mmOptions.trackDefaultUsage = true; // memory usage tracking
  mmOptions.checkUsageLeak = true; // leak check
  mmOptions.coreOnAllocationFailureEnabled = false;
  mmOptions.allocatorCapacity = bolt::memory::kMaxMemory;
  mmOptions.arbitratorKind = afr.getKind();
  mmOptions.useMemoryPoolForGluten = true; /* This code only for gluten */
  auto boltMemoryManager =
      std::make_unique<bolt::memory::MemoryManager>(mmOptions);

  auto boltAggregatePool = boltMemoryManager->addRootPool(
      "TEST_ONLY_root",
      bolt::memory::kMaxMemory, // the 3rd capacity
      bytedance::bolt::memory::MemoryReclaimer::create());

  auto boltLeafPool = boltAggregatePool->addLeafChild("TEST_ONLY_default_leaf");

  EXPECT_TRUE(
      std::dynamic_pointer_cast<MemoryPoolForGluten>(boltAggregatePool) !=
      nullptr);
  EXPECT_TRUE(
      std::dynamic_pointer_cast<MemoryPoolForGluten>(boltLeafPool) != nullptr);
}

} // namespace bytedance::bolt::memory::sparksql
