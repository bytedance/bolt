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

#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "bolt/common/memory/sparksql/DynamicMemoryQuotaManager.h"
namespace bytedance::bolt::memory::sparksql {

using MemoryMutex = std::mutex;
using MemoryMutexGuard = std::unique_lock<MemoryMutex>;

class ExecutionMemoryPool;
using ExecutionMemoryPoolPtr = std::shared_ptr<ExecutionMemoryPool>;
using ExecutionMemoryPoolWeakPtr = std::weak_ptr<ExecutionMemoryPool>;

class ExecutionMemoryPool final
    : public std::enable_shared_from_this<ExecutionMemoryPool> {
 public:
  explicit ExecutionMemoryPool(
      int32_t maxTaskNumber = 1,
      const DynamicMemoryQuotaManagerOption& option = {});

  ~ExecutionMemoryPool();

  void setPoolSize(int64_t poolSize);

  int64_t memoryUsed();

  int64_t poolSize();

  int64_t memoryFree();

  int64_t getMemoryUsageForTask(int64_t taskAttemptId);

  int64_t acquireMemory(int64_t numBytes, int64_t taskAttemptId);

  int64_t releaseMemory(int64_t numBytes, int64_t taskAttemptId);

  int64_t releaseAllMemoryForTask(int64_t taskAttemptId);

  int64_t numActiveTasks() const;

  int64_t maxTaskNumber() const {
    return maxTaskNumber_;
  }

  int64_t getOveragedMemoryForTask(int64_t taskAttemptId);

  friend std::ostream& operator<<(
      std::ostream& os,
      const ExecutionMemoryPool& pool);

  friend std::ostream& operator<<(
      std::ostream& os,
      const ExecutionMemoryPool* pool);

  std::string toString() const;

  static void init(
      bool enable,
      int64_t poolSize,
      int32_t maxTaskNumber,
      const DynamicMemoryQuotaManagerOption& option,
      int64_t maxWaitTimeMs);

  static ExecutionMemoryPoolPtr instance();

  static bool inited() {
    return instance_ != nullptr;
  }

  static std::optional<int64_t> getAvailableMemoryPerTask();

  static std::optional<int64_t> getAvailableMemoryPerTaskRealtime();

  static std::optional<int64_t> getFreeMemoryForTask(int64_t taskAttemptId);

  // Return the minimum free memory for the current task suppose
  // the active task count reaches the maximum. This helps prevent
  // abrupt changes in memory limits when a new task starts.
  // TODO remove this API when SparkShuffleWriter eliminate memLimit as
  // threshold
  static std::optional<int64_t> getMinimumFreeMemoryForTask(
      int64_t taskAttemptId);

  static bool dynamicMemoryManagementTriggeredUnsafe() {
    return inited() ? instance()->poolExtendSize_.has_value() : false;
  }

 private:
  int64_t internalMemoryUsed() const;

  int64_t internalPoolSize() const;

  int64_t internalMemoryFree() const;

  bool triggerDynamicMemoryQuotaManager(bool notEnough, bool reachThreshold);

  MemoryMutex lock_;
  std::condition_variable cv_;
  std::unordered_map<int64_t, int64_t> memoryForTask_;
  std::optional<int64_t> poolSize_{};

  int32_t maxTaskNumber_{1};
  inline static ExecutionMemoryPoolPtr instance_{nullptr};
  inline static bool enable_{false};
  inline static int64_t minMemoryMaxWaitMs_{300000};

  // dynamic memory quota management
  const DynamicMemoryQuotaManagerOption option_;
  int64_t memIncreaseSize_{0};
  std::optional<int64_t> poolExtendSize_;
  DynamicMemoryQuotaManagerStatistics statistics_;
};

} // namespace bytedance::bolt::memory::sparksql
