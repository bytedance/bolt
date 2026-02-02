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
#include <folly/ScopeGuard.h>
#include <chrono>
#include <sstream>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SuccinctPrinter.h"
#include "bolt/common/memory/MemoryUtils.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
#include "bolt/common/time/Timer.h"
namespace bytedance::bolt::memory::sparksql {

ExecutionMemoryPoolPtr ExecutionMemoryPool::instance() {
  BOLT_CHECK(
      inited(),
      "ExecutionMemoryPool is not initialized, please call ExecutionMemoryPool::init() first");
  return instance_;
}

// Call ExecutionMemoryPool::init does not mean that BoltMemoryManager is
// enabled, but enabling BoltMemoryManager definitely means that
// ExecutionMemoryPool::init has been executed.
void ExecutionMemoryPool::init(
    bool enable,
    int64_t poolSize,
    int32_t maxTaskNumber,
    const DynamicMemoryQuotaManagerOption& option,
    int64_t minMemoryMaxWaitMs) {
  BOLT_CHECK(!inited(), "ExecutionMemoryPool is already initialized");
  instance_ = std::make_shared<ExecutionMemoryPool>(maxTaskNumber, option);
  instance_->setPoolSize(poolSize);
  enable_ = enable;
  minMemoryMaxWaitMs_ = minMemoryMaxWaitMs;
}

ExecutionMemoryPool::ExecutionMemoryPool(
    int32_t maxTaskNumber,
    const DynamicMemoryQuotaManagerOption& option)
    : maxTaskNumber_(maxTaskNumber), option_(option) {
  LOG(INFO) << "maxTaskNumber is: " << maxTaskNumber
            << ", BorrowFromRssOption detail is: " << option_.toString();
}

ExecutionMemoryPool::~ExecutionMemoryPool() {
  if (!memoryForTask_.empty()) {
    LOG(ERROR)
        << "If there exists killed task, please ignore this warning! "
        << "Expect release all memory for task before destroy, but still exists "
        << toString();
  }
  if (statistics_.extendCount > 0) {
    LOG(INFO) << statistics_.toString();
  }
}

void ExecutionMemoryPool::setPoolSize(int64_t poolSize) {
  MemoryMutexGuard guard(lock_);
  BOLT_CHECK(
      !poolSize_.has_value(), "Only allow once poolSize_ initialization!");
  BOLT_CHECK(
      poolSize >= 0,
      "illegal argument, require delta >= 0, but got {}",
      poolSize);
  poolSize_ = poolSize;
}

int64_t ExecutionMemoryPool::memoryUsed() {
  MemoryMutexGuard guard(lock_);
  return internalMemoryUsed();
}

int64_t ExecutionMemoryPool::poolSize() {
  MemoryMutexGuard guard(lock_);
  return internalPoolSize();
}

int64_t ExecutionMemoryPool::memoryFree() {
  MemoryMutexGuard guard(lock_);
  return internalMemoryFree();
}

int64_t ExecutionMemoryPool::getMemoryUsageForTask(int64_t taskAttemptId) {
  MemoryMutexGuard guard(lock_);
  auto ans = memoryForTask_.find(taskAttemptId);
  return (ans == memoryForTask_.end()) ? 0L : ans->second;
}

int64_t ExecutionMemoryPool::acquireMemory(
    int64_t numBytes,
    int64_t taskAttemptId) {
  BOLT_CHECK(numBytes > 0, "invalid number of bytes requested:{}", numBytes);

  MemoryMutexGuard guard(lock_);

  auto target = memoryForTask_.find(taskAttemptId);
  if (target == memoryForTask_.end()) {
    memoryForTask_.insert({taskAttemptId, 0L});
    cv_.notify_all();
  }

  bool thisRunHasBorrowed = false;
  while (true) {
    int64_t numActiveTasks = memoryForTask_.size();
    int64_t curMem = memoryForTask_[taskAttemptId];

    int64_t maxPoolSize = internalPoolSize();
    int64_t maxMemoryPerTask = maxPoolSize / numActiveTasks;
    int64_t minMemoryPerTask = internalPoolSize() / (2 * numActiveTasks);

    int64_t maxToGrant =
        std::min(numBytes, std::max<int64_t>(0L, maxMemoryPerTask - curMem));
    int64_t toGrant = std::min(maxToGrant, internalMemoryFree());

    if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
      LOG(INFO) << "TID " << taskAttemptId
                << " waiting for at least 1/2N to be free. "
                << "numActiveTasks=" << numActiveTasks
                << ", curMem=" << succinctBytes(curMem) << "(" << curMem << ")"
                << ", maxPoolSize=" << succinctBytes(maxPoolSize) << "("
                << maxPoolSize << ")"
                << ", maxMemoryPerTask=" << succinctBytes(maxMemoryPerTask)
                << "(" << maxMemoryPerTask << ")"
                << ", minMemoryPerTask=" << succinctBytes(minMemoryPerTask)
                << "(" << minMemoryPerTask << ")"
                << ", maxToGrant=" << succinctBytes(maxToGrant) << "("
                << maxToGrant << ")"
                << ", toGrant=" << succinctBytes(toGrant) << "(" << toGrant
                << ")"
                << ", numBytes=" << succinctBytes(numBytes) << "(" << numBytes
                << ")"
                << ", internalMemoryUsed=" << internalMemoryUsed()
                << ", internalMemoryFree=" << internalMemoryFree()
                << ", ExecutionPool detail is: " << this;
      // In Spark's implementation, there is no timeout, but in Gluten's
      // practice, we found that some deadlock issues occurred, so we added a
      // timeout here. deadlock scenario:
      // 1. task A accquire RESOURCE_REGISTRIES.synchronized.
      // 2. task A waiting for 1/2N to be satisfied.
      // 3. task B wants to release memory, but RESOURCE_REGISTRIES is hold by
      // task A.
      if (cv_.wait_for(guard, std::chrono::milliseconds(minMemoryMaxWaitMs_)) ==
          std::cv_status::timeout) {
        LOG(ERROR) << "TID " << taskAttemptId
                   << " waiting for minimum memory failed after "
                   << minMemoryMaxWaitMs_ << "ms";
        BOLT_CHECK(
            minMemoryMaxWaitMs_ > 0,
            "Expect minMemoryMaxWaitMs is greater than 0, but got {}",
            minMemoryMaxWaitMs_);
        return 0;
      }
    } else {
      bool enable = option_.enable && !thisRunHasBorrowed;
      if (enable) {
        bool notEnough = toGrant < numBytes;
        bool reachThreshold = poolExtendSize_.has_value() &&
            memIncreaseSize_ >= option_.sampleSize;
        if (notEnough || reachThreshold) {
          auto varGuard = folly::makeGuard([&] {
            thisRunHasBorrowed = true;
            memIncreaseSize_ = 0;
          });

          if (triggerDynamicMemoryQuotaManager(notEnough, reachThreshold)) {
            continue;
          }
        }
      }

      memoryForTask_[taskAttemptId] += toGrant;
      memIncreaseSize_ += toGrant;

      return toGrant;
    }
  }
}

int64_t ExecutionMemoryPool::releaseMemory(
    int64_t numBytes,
    int64_t taskAttemptId) {
  MemoryMutexGuard guard(lock_);
  auto it = memoryForTask_.find(taskAttemptId);
  const bool contains = (it != memoryForTask_.end());

  int64_t curMem = (contains ? it->second : 0L);

  int64_t memoryToFree;
  if (curMem < numBytes) {
    LOG(WARNING) << "Internal error: release called on " << numBytes
                 << " bytes but task only has " << curMem
                 << " bytes of memory from pool";
    memoryToFree = curMem;
  } else {
    memoryToFree = numBytes;
  }

  if (contains) {
    memoryForTask_[taskAttemptId] -= memoryToFree;
    if (memoryForTask_[taskAttemptId] == 0) {
      memoryForTask_.erase(taskAttemptId);
    }
  }
  cv_.notify_all();
  return memoryToFree;
}

int64_t ExecutionMemoryPool::releaseAllMemoryForTask(int64_t taskAttemptId) {
  int64_t numBytesToFree = getMemoryUsageForTask(taskAttemptId);
  releaseMemory(numBytesToFree, taskAttemptId);
  return numBytesToFree;
}

int64_t ExecutionMemoryPool::numActiveTasks() const {
  return memoryForTask_.size();
}

int64_t ExecutionMemoryPool::getOveragedMemoryForTask(int64_t taskAttemptId) {
  MemoryMutexGuard guard(lock_);

  if (!poolExtendSize_.has_value()) {
    return 0L;
  }

  int64_t numActiveTasks = memoryForTask_.size();
  int64_t curMem = memoryForTask_[taskAttemptId];

  int64_t maxPoolSize = internalPoolSize();
  int64_t maxMemoryPerTask = maxPoolSize / numActiveTasks;

  return std::max<int64_t>(0L, curMem - maxMemoryPerTask);
}

std::optional<int64_t> ExecutionMemoryPool::getAvailableMemoryPerTask() {
  if (!inited()) {
    return std::nullopt;
  }
  auto pool = instance();
  int64_t numActiveTasks = pool->memoryForTask_.size();
  return pool->poolSize() / std::max(pool->maxTaskNumber(), numActiveTasks);
}

std::optional<int64_t>
ExecutionMemoryPool::getAvailableMemoryPerTaskRealtime() {
  if (!inited() || !enable_) {
    return std::nullopt;
  }
  auto pool = instance();
  int64_t numActiveTasks = pool->memoryForTask_.size();
  // if no running tasks, use task number by conf
  int64_t taskCnt = numActiveTasks ? numActiveTasks : pool->maxTaskNumber();
  return pool->poolSize() / taskCnt;
}

std::optional<int64_t> ExecutionMemoryPool::getFreeMemoryForTask(
    int64_t taskAttemptId) {
  if (!inited() || !enable_) {
    return std::nullopt;
  }
  auto pool = instance();
  int64_t numActiveTasks = pool->memoryForTask_.size();
  int64_t curMem = pool->memoryForTask_[taskAttemptId];

  int64_t maxPoolSize = pool->internalPoolSize();
  int64_t maxMemoryPerTask = maxPoolSize / numActiveTasks;

  return maxMemoryPerTask - curMem;
}

std::optional<int64_t> ExecutionMemoryPool::getMinimumFreeMemoryForTask(
    int64_t taskAttemptId) {
  if (!inited() || !enable_) {
    return std::nullopt;
  }
  int64_t available = 0;
  {
    MemoryMutexGuard guard(instance()->lock_);
    // do not consider extened pool to avoid descrease pool size
    available = instance()->poolSize_.value_or(0) / instance()->maxTaskNumber();
  }
  auto usage = instance()->getMemoryUsageForTask(taskAttemptId);
  return std::max<int64_t>(0L, available - usage);
}

std::ostream& operator<<(std::ostream& os, const ExecutionMemoryPool& pool) {
  os << "ExecutionMemoryPool(poolSize=" << pool.poolSize_.value_or(0)
     << ", poolExtendSize=" << pool.poolExtendSize_.value_or(0)
     << ", memIncreaseSize=" << pool.memIncreaseSize_ << " memoryForTask={";
  for (const auto& pair : pool.memoryForTask_) {
    os << "[taskAttemptId=" << pair.first << ", memoryUsed=" << pair.second
       << "]";
  }
  if (pool.statistics_.extendCount > 0) {
    os << ", DynamicMemoryQuotaManagerStatistics="
       << pool.statistics_.toString();
  }
  os << ", DynamicMemoryQuotaManagerOption=" << pool.option_.toString();
  return os << "}";
}

std::ostream& operator<<(std::ostream& os, const ExecutionMemoryPool* pool) {
  if (pool == nullptr) {
    os << "ExecutionMemoryPool(nullptr)";
    return os;
  }
  return os << (*pool);
}

std::string ExecutionMemoryPool::toString() const {
  std::stringstream ss;
  ss << this;
  return ss.str();
}

/* Internal API doesn't need get lock */
int64_t ExecutionMemoryPool::internalMemoryUsed() const {
  int64_t memory = 0;
  for (const auto& pair : memoryForTask_) {
    memory += pair.second;
  }
  return memory;
}

int64_t ExecutionMemoryPool::internalPoolSize() const {
  return poolSize_.value_or(0) + poolExtendSize_.value_or(0);
}

int64_t ExecutionMemoryPool::internalMemoryFree() const {
  return std::max<int64_t>(0L, internalPoolSize() - internalMemoryUsed());
}

bool ExecutionMemoryPool::triggerDynamicMemoryQuotaManager(
    bool notEnough,
    bool reachThreshold) {
  NanosecondTimer totalTimer(&statistics_.extendTotalTimeCost);

  const int64_t countUsed = internalMemoryUsed();
  const int64_t oldPoolSize = internalPoolSize(),
                originalPoolSize = poolSize_.value_or(0);

  if (countUsed >= oldPoolSize * option_.quotaTriggerRatio || reachThreshold) {
    int64_t processRss, onHeapRss;
    {
      NanosecondTimer apiTimer(&statistics_.extendApiTimeCost);
      processRss = MemoryUtils::getProcessRss();
      onHeapRss = MemoryUtils::getOnHeapMemUsed();
    }
    if (processRss == MemoryUtils::kInvalidRssSize) {
      LOG(ERROR)
          << "Get RSS from OS API failed! DynamicMemoryQuotaManager feature will retry next time.";
      return false;
    }
    // in some cases, onHeapRss may be larger than processRss, in that case, we
    // should use a minimum offHeapRss value to avoid negative rss value.
    const int64_t kMinOffHeapRss = 32 * 1024 * 1024;
    const int64_t rss =
        std::max<int64_t>(kMinOffHeapRss, processRss - onHeapRss);

    auto rssTarget = std::min(countUsed, originalPoolSize);
    if (rss < rssTarget * option_.rssMinRatio ||
        rss > originalPoolSize * option_.rssMaxRatio) {
      const double originalRatio = countUsed / static_cast<double>(rss);
      const double ratio = std::max(
          std::min(originalRatio, option_.extendMaxRatio),
          option_.extendMinRatio);

      const int64_t oldPoolExtendSize = poolExtendSize_.value_or(0);
      const int64_t newPoolExtendSize =
          originalPoolSize * (ratio - 1) * option_.extendScaleRatio;

      if (!poolExtendSize_.has_value() ||
          std::abs(poolExtendSize_.value() - newPoolExtendSize) >=
              option_.changeThresholdRatio * originalPoolSize) {
        poolExtendSize_ = newPoolExtendSize;
        statistics_.updateExtendSize(newPoolExtendSize);

        if (folly::Random::randDouble01() < option_.logPrintFreq) {
          NanosecondTimer logTimer(&statistics_.extendLogTimeCost);
          LOG(INFO) << "Trigger borrowFromRSS feature, processRss is "
                    << succinctBytes(processRss) << ", onHeapRss is "
                    << succinctBytes(onHeapRss) << ", offHeapRss is "
                    << succinctBytes(rss) << ", countUsed is "
                    << succinctBytes(countUsed)
                    << ", originalRatio=" << originalRatio
                    << ", finalRatio=" << ratio
                    << ", originalPoolSize=" << succinctBytes(originalPoolSize)
                    << ", poolSize changed (" << succinctBytes(oldPoolSize)
                    << " -> " << succinctBytes(internalPoolSize()) << ")"
                    << ", extend pool size changed ("
                    << succinctBytesPrinter(oldPoolExtendSize) << " -> "
                    << succinctBytesPrinter(newPoolExtendSize) << ")"
                    << ", notEnough=" << (notEnough ? "true" : "false")
                    << ", reachThreshold="
                    << (reachThreshold ? "true" : "false") << ", statistics is "
                    << statistics_.toString() << ", pool details is "
                    << this->toString();
        }
      }
      return true;
    }
  }

  return false;
}

void ExecutionMemoryPool::testingResetPoolSize(int64_t newSize) {
  MemoryMutexGuard guard(instance()->lock_);
  instance()->poolSize_ = newSize;
}

} // namespace bytedance::bolt::memory::sparksql
