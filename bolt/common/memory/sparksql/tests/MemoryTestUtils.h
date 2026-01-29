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

#pragma once

#include "bolt/common/memory/sparksql/NativeMemoryManagerFactory.h"
#include "bolt/common/memory/sparksql/Spiller.h"

namespace bytedance::bolt::memory::sparksql::test {

class TestSpiller final : public memory::sparksql::Spiller {
 public:
  explicit TestSpiller(const BoltMemoryManagerPtr& memoryManager)
      : memoryManager_(memoryManager) {}

  int64_t spill(MemoryTargetWeakPtr self, int64_t size) override {
    auto manager = memoryManager_.lock();
    if (!manager) {
      return 0;
    }
    return manager->shrink(size);
  }

  const std::set<SpillerPhase>& applicablePhases() override {
    return SpillerHelper::phaseSetAll();
  }

 private:
  // use weak_ptr to avoid cyclic reference
  std::weak_ptr<BoltMemoryManager> memoryManager_;
};

// A helper to create MemoryManager for tests, hard memory limit is set to
// memoryLimit. Note: there should be only one TestMemoryManagerHolder in
// certain scope.
//
// Usage:
//    auto holder = TestMemoryManagerHolder::create(memoryLimit);
//    auto pool = holder->rootPool();
//    // test operators or task with 'pool'
//    ...
class TestMemoryManagerHolder {
 public:
  static std::shared_ptr<TestMemoryManagerHolder> create(
      int64_t memoryLimit = memory::kMaxMemory) {
    if (!ExecutionMemoryPool::inited()) {
      ExecutionMemoryPool::init(true, memoryLimit, 1, {}, 10000);
    } else {
      ExecutionMemoryPool::testingResetPoolSize(memoryLimit);
    }
    // create TaskMemoryMenager to ensure memory limit is applied correctly
    auto tmm = std::make_shared<TaskMemoryManager>(
        ExecutionMemoryPool::instance(), globalTaskId_++);
    // disable dynamic memory quota manager for test stability
    std::unordered_map<std::string, std::string> conf = {
        {ConfigurationResolver::kDynamicMemoryQuotaManager, "false"}};
    NativeMemoryManagerFactoryParam param{
        .name = "ShuffleTest",
        .memoryIsolation = kIsolateMemory, // isolate memory to set memory limit
        .conservativeTaskOffHeapMemorySize = memoryLimit,
        .overAcquiredRatio = 0,
        .taskMemoryManager = tmm,
        .sessionConf = conf};
    auto instance = NativeMemoryManagerFactory::contextInstance(param);
    std::shared_ptr<TestSpiller> spiller =
        std::make_shared<TestSpiller>(instance->getManager());
    auto genericSpiller =
        std::static_pointer_cast<memory::sparksql::Spiller>(spiller);
    instance->appendSpiller(genericSpiller);
    return std::shared_ptr<TestMemoryManagerHolder>(
        new TestMemoryManagerHolder(instance, tmm, memoryLimit));
  }

  std::shared_ptr<memory::MemoryPool> rootPool() {
    auto ptr = memoryManagerHolder_->getManager()->getAggregateMemoryPool();
    return ptr->shared_from_this();
  }

  int64_t taskAttemptId() {
    return tmm_->getTaskAttemptId();
  }

  ~TestMemoryManagerHolder() {
    delete memoryManagerHolder_;
    MemoryTargetBuilder::invalidate(tmm_, kIsolateMemory, memoryLimit_);
    ExecutionMemoryPool::instance()->releaseAllMemoryForTask(
        tmm_->getTaskAttemptId());
  }

 private:
  TestMemoryManagerHolder(
      BoltMemoryManagerHolder* holder,
      TaskMemoryManagerPtr tmm,
      int64_t memoryLimit)
      : memoryManagerHolder_(holder), tmm_(tmm), memoryLimit_(memoryLimit) {}

  BoltMemoryManagerHolder* memoryManagerHolder_ = nullptr;
  TaskMemoryManagerPtr tmm_ = nullptr;
  int64_t memoryLimit_ = memory::kMaxMemory;
  // disable memory isolation to align with gluten default behavior
  static inline constexpr bool kIsolateMemory = false;

  static inline std::atomic<int64_t> globalTaskId_{0};
};

} // namespace bytedance::bolt::memory::sparksql::test
