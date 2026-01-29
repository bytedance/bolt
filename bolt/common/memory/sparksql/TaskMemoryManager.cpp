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

#include <cstdint>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/sparksql/MemoryConsumer.h"
#include "bolt/common/memory/sparksql/TaskMemoryManager.h"
#include "bolt/common/memory/sparksql/WeakPtrHelper.h"
namespace bytedance::bolt::memory::sparksql {

TaskMemoryManager::TaskMemoryManager(
    ExecutionMemoryPoolWeakPtr memoryPool,
    int64_t taskAttemptId)
    : taskAttemptId_(taskAttemptId), offHeapExecutionMemoryPool_(memoryPool) {}

TaskMemoryManager::~TaskMemoryManager() {
  // can't use lock_or_throw function
  auto pool = offHeapExecutionMemoryPool_.lock();
  if (pool) {
    int64_t released = pool->releaseAllMemoryForTask(taskAttemptId_);
    if (released != 0) {
      LOG(ERROR)
          << "Expecting all consumer which belongs to TaskMemoryManager repay all memory by themselves, but task "
          << taskAttemptId_ << " still have " << released
          << " bytes. TaskMemoryManager detail is" << toString();
    }
  } else {
    LOG(ERROR) << "ExecutionMemoryPool is expired early than TaskMemoryManager";
  }
}

namespace {
bool isInTestEnv() {
  return std::getenv("BOLT_IN_GTEST") != nullptr;
}
} // namespace

int64_t TaskMemoryManager::acquireExecutionMemory(
    int64_t required,
    MemoryConsumerWeakPtr requestingConsumer) {
  BOLT_CHECK(required >= 0);

  MemoryRecursiveMutexGuard guard(lock_);

  auto pool = lock_or_throw(offHeapExecutionMemoryPool_);
  int64_t got = pool->acquireMemory(required, taskAttemptId_);
  auto rc = lock_or_throw(requestingConsumer);
  if (historyRequestConsumer_ == nullptr) {
    historyRequestConsumer_ = rc.get();
  } else {
    // expect only 1 consumer
    BOLT_CHECK(
        historyRequestConsumer_ == rc.get() || isInTestEnv(),
        "Expect only 1 consumer in TaskMemoryManager");
  }

  // When our spill handler releases memory,
  // `ExecutionMemoryPool#releaseMemory()` will immediately notify other
  // tasks that memory has been freed, and they may acquire the
  // newly-freed memory before we have a chance to do so (SPARK-35486).
  // Therefore we may not be able to acquire all the memory that was just
  // spilled. In that case, we will try again in the next loop iteration.
  while (got < required) {
    auto stillNeed = required - got;
    // When DynamicMemoryQuotaManager is enabled, the total quota may become
    // smaller, and the memory quota of each task will also become smaller.
    // If only the `stillNeed` amount of memory is recovered, the memory
    // recovered by spill may be used immediately to make up for the quota
    // deficit and will not be used by the task, which will lead to frequent
    // spills.
    auto mustReturn = pool->getOveragedMemoryForTask(taskAttemptId_);
    int64_t released = rc->spill(stillNeed + mustReturn);
    if (released > 0) {
      got += pool->acquireMemory(stillNeed, taskAttemptId_);
    } else {
      break;
    }
  }
  return got;
}

int64_t TaskMemoryManager::releaseExecutionMemory(
    int64_t size,
    MemoryConsumerWeakPtr consumer) {
  MemoryRecursiveMutexGuard guard(lock_);

  auto pool = lock_or_throw(offHeapExecutionMemoryPool_);
  auto released = pool->releaseMemory(size, taskAttemptId_);
  return released;
}

void TaskMemoryManager::registerConsumer(MemoryConsumerPtr& consumer) {
  ownershipHolder_.emplace(consumer);
}

std::ostream& operator<<(
    std::ostream& os,
    const TaskMemoryManager& taskMemoryManager) {
  auto pool = taskMemoryManager.offHeapExecutionMemoryPool_.lock();
  os << "TaskMemoryManager(taskAttemptId=" << taskMemoryManager.taskAttemptId_
     << ", pool=" << (pool == nullptr ? "nullptr" : pool->toString());
  return os << "}";
}

std::ostream& operator<<(
    std::ostream& os,
    const TaskMemoryManager* taskMemoryManager) {
  if (taskMemoryManager == nullptr) {
    return os << "TaskMemoryManager(nullptr)";
  }
  return os << (*taskMemoryManager);
}

std::string TaskMemoryManager::toString() const {
  std::stringstream ss;
  ss << this;
  return ss.str();
}

} // namespace bytedance::bolt::memory::sparksql
