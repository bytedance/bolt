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
#include <list>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SuccinctPrinter.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/memory/sparksql/AllocationListener.h"
#include "bolt/common/memory/sparksql/MemoryConsumer.h"
#include "bolt/common/memory/sparksql/MemoryTarget.h"
#include "bolt/common/memory/sparksql/Spiller.h"
#include "bolt/common/memory/sparksql/SpillerPhase.h"
#include "bolt/common/memory/sparksql/TaskMemoryManager.h"
#include "bolt/common/memory/sparksql/WeakPtrHelper.h"

using namespace ::testing;
using namespace bytedance::bolt::memory::sparksql;
namespace bytedance::bolt::memory::sparksql {

class MemoryTargetTest : public testing::Test {};

class TestSpiller final : public Spiller {
 public:
  int64_t spill(MemoryTargetWeakPtr self, int64_t size) override {
    auto target = lock_or_throw(self);
    LOG(INFO) << target->toString() << " TestSpiller::spill wants to spill "
              << succinctBytes(size);
    return 0;
  }

  const std::set<SpillerPhase>& applicablePhases() override {
    return SpillerHelper::phaseSetShrinkOnly();
  }
};

class TestMemoryUsageRecorder final : public MemoryUsageRecorder {
 public:
  void inc(int64_t bytes) override {
    // no op
  }

  // peak used bytes
  int64_t peak() override {
    BOLT_FAIL("Not implemented");
  }

  // current used bytes
  int64_t current() override {
    BOLT_FAIL("Not implemented");
  }

  MemoryUsageStatsPtr toStats() override {
    BOLT_FAIL("Not implemented");
  }
};

TEST_F(MemoryTargetTest, basic) {
  const int64_t capacity = 1 * 1024 * 1024 * 1024;
  const int64_t taskAttemptId = 996;
  bool memoryIsolation = false;
  int64_t conservativeTaskOffHeapMemorySize = 0;
  const double overAccquireRatio = 0.3;

  auto memoryPool = std::make_shared<ExecutionMemoryPool>();
  memoryPool->setPoolSize(capacity);

  auto tmm = std::make_shared<TaskMemoryManager>(memoryPool, taskAttemptId);

  SpillerPtr spiller1 = std::make_shared<TestSpiller>();
  auto spillers1 = std::list<SpillerPtr>{spiller1};
  MemoryUsageRecorderPtr recorder1 =
      std::make_shared<TestMemoryUsageRecorder>();
  auto children1 =
      std::map<std::string, MemoryUsageStatsBuilderPtr>{{"single", recorder1}};

  MemoryTargetPtr target1 = MemoryTargetBuilder::newConsumer(
      tmm,
      "WholeStageCodeGen",
      spillers1,
      children1,
      memoryIsolation,
      conservativeTaskOffHeapMemorySize);

  SpillerPtr spiller2 = std::make_shared<OverAccquireSpiller>();
  auto spillers2 = std::list<SpillerPtr>{spiller2};
  std::map<std::string, MemoryUsageStatsBuilderPtr> children2;
  MemoryTargetPtr target2 = MemoryTargetBuilder::newConsumer(
      tmm,
      "OverAcquire.DummyTarget",
      spillers2,
      children2,
      memoryIsolation,
      conservativeTaskOffHeapMemorySize);
  MemoryTargetPtr overAccquireTarget =
      MemoryTargetBuilder::overAcquire(target1, target2, overAccquireRatio);
  std::string name = "test";
  SimpleMemoryUsageRecorderPtr recorder2 =
      std::make_shared<SimpleMemoryUsageRecorder>();
  AllocationListenerPtr listener = std::make_shared<ManagedAllocationListener>(
      name, overAccquireTarget, recorder2);

  const int64_t allocationNums = 1;
  const int64_t singleAllocSize = 1;
  for (int64_t i = 0; i < allocationNums; ++i) {
    listener->allocationChanged(singleAllocSize);
  }
  std::cout << "MemoryPool used=" << memoryPool->memoryUsed() << std::endl;
  BOLT_CHECK(
      memoryPool->memoryUsed() ==
      allocationNums *
          (singleAllocSize + int64_t(singleAllocSize * overAccquireRatio)));
  for (int64_t i = 0; i < allocationNums; ++i) {
    listener->allocationChanged(-singleAllocSize);
  }
}

TEST_F(MemoryTargetTest, toString) {
  const int64_t capacity = 1 * 1024 * 1024 * 1024;
  const int64_t taskAttemptId = 996;
  bool memoryIsolation = false;
  int64_t conservativeTaskOffHeapMemorySize = 0;
  const double overAccquireRatio = 0.3;

  auto memoryPool = std::make_shared<ExecutionMemoryPool>();
  memoryPool->setPoolSize(capacity);
  auto tmm = std::make_shared<TaskMemoryManager>(memoryPool, taskAttemptId);

  MemoryTargetPtr target = MemoryTargetBuilder::newConsumer(
      tmm,
      "WholeStageCodeGen",
      std::list<SpillerPtr>{},
      std::map<std::string, MemoryUsageStatsBuilderPtr>{},
      memoryIsolation,
      conservativeTaskOffHeapMemorySize);

  MemoryTargetPtr overAccquireTarget = MemoryTargetBuilder::newConsumer(
      tmm,
      "OverAcquire.DummyTarget",
      std::list<SpillerPtr>{},
      std::map<std::string, MemoryUsageStatsBuilderPtr>{},
      memoryIsolation,
      conservativeTaskOffHeapMemorySize);
  MemoryTargetPtr ans = MemoryTargetBuilder::overAcquire(
      target, overAccquireTarget, overAccquireRatio);
  auto recorder = std::make_shared<SimpleMemoryUsageRecorder>();
  AllocationListenerPtr listener = std::make_shared<ManagedAllocationListener>(
      "test", overAccquireTarget, recorder);
  std::string expect =
      "OverAcquireMemoryTarget(target=TreeMemoryTargetNode(name=WholeStageCodeGen.1, capacity=9223372036854775807, childrenNum=0, children=(), parent=TreeMemoryTargetNode(name=SpillTriggerLayer.root.1, capacity=9223372036854775807, childrenNum=2, children=([seq=0, childName=OverAcquire.DummyTarget.1],[seq=1, childName=WholeStageCodeGen.1],), parent=ConsumerTargetBridge(name=Gluten.Tree.1, taskMemoryManager=TaskMemoryManager(taskAttemptId=996, pool=ExecutionMemoryPool(poolSize=1073741824, poolExtendSize=0, memIncreaseSize=0 memoryForTask={, DynamicMemoryQuotaManagerOption=enable=false, quotaTriggerRatio=0, rssMinRatio=0, rssMaxRatio=0, extendMaxRatio=0, extendMinRatio=0, extendScaleRatio=0, sampleRatio=0, sampleSize=0B, changeThresholdRatio=0, logPrintFreq=0}}), overTarget=TreeMemoryTargetNode(name=OverAcquire.DummyTarget.1, capacity=9223372036854775807, childrenNum=0, children=(), parent=TreeMemoryTargetNode(name=SpillTriggerLayer.root.1, capacity=9223372036854775807, childrenNum=2, children=([seq=0, childName=OverAcquire.DummyTarget.1],[seq=1, childName=WholeStageCodeGen.1],), parent=ConsumerTargetBridge(name=Gluten.Tree.1, taskMemoryManager=TaskMemoryManager(taskAttemptId=996, pool=ExecutionMemoryPool(poolSize=1073741824, poolExtendSize=0, memIncreaseSize=0 memoryForTask={, DynamicMemoryQuotaManagerOption=enable=false, quotaTriggerRatio=0, rssMinRatio=0, rssMaxRatio=0, extendMaxRatio=0, extendMinRatio=0, extendScaleRatio=0, sampleRatio=0, sampleSize=0B, changeThresholdRatio=0, logPrintFreq=0}}))";
  BOLT_CHECK(
      expect == ans->toString(),
      "Expect {}, but got {}",
      expect,
      ans->toString());
}

TEST_F(MemoryTargetTest, expectSpill) {
  const int64_t capacity = 1 * 1024 * 1024 * 1024;
  const int64_t taskAttemptId = 996;
  bool memoryIsolation = false;
  int64_t conservativeTaskOffHeapMemorySize = 0;
  const double overAccquireRatio = 0.3;

  auto memoryPool = std::make_shared<ExecutionMemoryPool>();
  memoryPool->setPoolSize(capacity);
  auto tmm = std::make_shared<TaskMemoryManager>(memoryPool, taskAttemptId);

  SpillerPtr spiller1 = std::make_shared<TestSpiller>();
  auto spillers1 = std::list<SpillerPtr>{spiller1};
  MemoryUsageRecorderPtr recorder1 =
      std::make_shared<TestMemoryUsageRecorder>();
  auto children1 =
      std::map<std::string, MemoryUsageStatsBuilderPtr>{{"single", recorder1}};

  MemoryTargetPtr target1 = MemoryTargetBuilder::newConsumer(
      tmm,
      "WholeStageCodeGen",
      spillers1,
      children1,
      memoryIsolation,
      conservativeTaskOffHeapMemorySize);

  SpillerPtr spiller2 = std::make_shared<OverAccquireSpiller>();
  auto spillers2 = std::list<SpillerPtr>{spiller2};
  std::map<std::string, MemoryUsageStatsBuilderPtr> children2;
  MemoryTargetPtr target2 = MemoryTargetBuilder::newConsumer(
      tmm,
      "OverAcquire.DummyTarget",
      spillers2,
      children2,
      memoryIsolation,
      conservativeTaskOffHeapMemorySize);
  MemoryTargetPtr overAccquireTarget =
      MemoryTargetBuilder::overAcquire(target1, target2, overAccquireRatio);
  std::string name = "test";
  SimpleMemoryUsageRecorderPtr recorder2 =
      std::make_shared<SimpleMemoryUsageRecorder>();
  AllocationListenerPtr listener = std::make_unique<ManagedAllocationListener>(
      name, overAccquireTarget, recorder2);

  int64_t first = listener->allocationChanged(capacity / 2);
  BOLT_CHECK(first == capacity / 2);
  int64_t second = listener->allocationChanged(capacity / 2 + 1);
  BOLT_CHECK(second == capacity / 2);
}

} // namespace bytedance::bolt::memory::sparksql
