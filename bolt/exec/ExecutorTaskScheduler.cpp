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

#include "bolt/exec/ExecutorTaskScheduler.h"
#include "bolt/common/memory/MemoryUtils.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
namespace bytedance::bolt::exec {

namespace {
constexpr const char* TASKID_PREFIX = "Gluten_Stage_";
constexpr int32_t TASKID_PREFIX_LEN = 13;
constexpr const char* TASKID_PID_PREFIX = "TID_";
constexpr int32_t TASKID_PID_PREFIX_LEN = 4;

int32_t extractStageId(const std::string& taskId) {
  BOLT_CHECK(
      taskId.length() > TASKID_PREFIX_LEN &&
      taskId.compare(0, TASKID_PREFIX_LEN, TASKID_PREFIX) == 0);
  int32_t pos = TASKID_PREFIX_LEN;
  int number = 0;
  bool hasDigit = false;
  while (pos < taskId.length() && taskId[pos] != '_') {
    if (FOLLY_UNLIKELY(!isdigit(static_cast<unsigned char>(taskId[pos])))) {
      BOLT_FAIL("taskid should be digit, taskId {}", taskId);
    }
    number = number * 10 + (taskId[pos] - '0');
    hasDigit = true;
    ++pos;
  }
  BOLT_CHECK(hasDigit);

  return number;
}

int64_t extractTaskTid(const std::string& taskId) {
  const size_t pos = taskId.find("TID_");
  BOLT_CHECK(
      pos != std::string::npos, "taskId {} should contains TID_", taskId);

  const size_t numStart = pos + TASKID_PID_PREFIX_LEN;
  const size_t numEnd = taskId.find('_', numStart);
  BOLT_CHECK(
      numEnd != std::string::npos,
      "taskId {} should contains TID_tid_",
      taskId);
  auto tid = taskId.substr(numStart, numEnd - numStart);
  int64_t value;
  const auto [ptr, ec] =
      std::from_chars(tid.data(), tid.data() + tid.size(), value);
  BOLT_CHECK(
      ec == std::errc() && ptr == tid.data() + tid.size(),
      "convert tid failed, tid {} taskId {}",
      tid,
      taskId);
  return value;
}

} // namespace

std::string mapSchedulerStateToString(const SchedulerState& state) {
  static std::unordered_map<SchedulerState, std::string> stateEnumMap{
      {SchedulerState::kInit, "kInit"},
      {SchedulerState::kSampling, "kSampling"},
      {SchedulerState::kRevising, "kRevising"},
      {SchedulerState::kCompleted, "kCompleted"}};
  auto found = stateEnumMap.find(state);
  if (found == stateEnumMap.end()) {
    BOLT_FAIL("SchedulerState is not found : {}", (int32_t)state);
  }
  return found->second;
}

// static
ExecutorTaskScheduler& ExecutorTaskScheduler::instance() {
  static folly::Indestructible<ExecutorTaskScheduler> taskSchedulerInstance_;
  return *taskSchedulerInstance_;
}

bool ExecutorTaskScheduler::waitForScheduling(
    std::shared_ptr<Task>& task,
    bool doSchedule) {
  bool waitInQueue = false;

  // taskid deduplication
  bool isInserted = tasksIds_.wlock()->emplace(task->taskId()).second;
  (*allTasks_.wlock())[task->taskId()].emplace_back(task);
  if (isInserted) {
    task->setSourceFragmentUnlocked();
  } else {
    return waitInQueue;
  }

  std::lock_guard<std::mutex> l(mutex_);
  ++numRunningTask_;
  if (doSchedule && numRunningTask_ > currentConcurrency_) {
    taskQueue_.push(task);
    waitInQueue = true;
    --numRunningTask_;
  }

  if (!waitInQueue) {
    startTaskTracking(task->taskId());
  }

  VLOG(1) << __FUNCTION__ << ": task " << task->taskId()
          << (waitInQueue ? " in waiting queue" : " not in waiting queue")
          << ", numRunningTask_ = " << numRunningTask_
          << ", taskQueue_.size() = " << taskQueue_.size();
  return waitInQueue;
}

void ExecutorTaskScheduler::collectTaskStatsUnlocked(
    const TaskStats& taskStats,
    SimplifiedTaskStats& stats) {
  stats.memoryReclaimCount =
      std::max(stats.memoryReclaimCount, taskStats.memoryReclaimCount);
  stats.executionTimeMs = std::max(
      stats.executionTimeMs,
      taskStats.executionEndTimeMs - taskStats.executionStartTimeMs);
  if (taskStats.isSourceFragment_) {
    for (const auto& pipelineStats : taskStats.pipelineStats) {
      auto const& opStats = pipelineStats.operatorStats[0];
      if (opStats.operatorType.compare("ValueStream") == 0) {
        stats.numSourceRows = opStats.outputPositions;
      } else if (opStats.operatorType.compare("TableScan") == 0) {
        stats.numSourceRows = opStats.rawInputPositions;
      } else {
        LOG(ERROR)
            << __FUNCTION__
            << " : Source Operator name is neither TableScan or ValueStream, got "
            << opStats.operatorType;
        stats.numSourceRows = 0;
      }
    }
  }

  uint64_t peakMemoryBytes = 0;
  for (const auto& pipelineStats : taskStats.pipelineStats) {
    for (const auto& opStats : pipelineStats.operatorStats) {
      peakMemoryBytes = std::max(
          peakMemoryBytes, opStats.memoryStats.peakTotalMemoryReservation);
    }
  }
  stats.peakMemoryBytes = std::max(peakMemoryBytes, stats.peakMemoryBytes);
  return;
}

void ExecutorTaskScheduler::reportRuntimeStatsLocked(
    SimplifiedTaskStats& stats,
    const MemoryUsageStats& memoryStats) {
  auto maxMemoryForPerTask = memory::sparksql::ExecutionMemoryPool::
      getAvailableMemoryPerTaskRealtime();
  double peakMemoryRatio =
      (maxMemoryForPerTask.has_value() && stats.peakMemoryBytes)
      ? (1.0 * maxMemoryForPerTask.value() / stats.peakMemoryBytes)
      : 1.0;
  double overUsedRatio = 1.0;
  if (!memory::sparksql::ExecutionMemoryPool::
          dynamicMemoryManagementTriggeredUnsafe() &&
      memoryStats.rssBytes) {
    overUsedRatio =
        std::max(1.0, 1.0 * memoryStats.vssBytes / memoryStats.rssBytes);
  }
  stats.memoryUsedRatio = peakMemoryRatio * overUsedRatio;
  LOG(INFO) << __FUNCTION__ << ": maxMemoryForPerTask = "
            << succinctBytes(
                   maxMemoryForPerTask.has_value() ? maxMemoryForPerTask.value()
                                                   : 1024)
            << ", peakMemoryBytes = " << succinctBytes(stats.peakMemoryBytes)
            << ", peakMemoryRatio = " << peakMemoryRatio
            << ", overUsedRatio = " << overUsedRatio
            << ", final memoryUsedRatio = " << stats.memoryUsedRatio
            << ", numRunningTask_ = " << numRunningTask_;
  statsCollector_.addRuntimeStats(stats);
  return;
}

double ExecutorTaskScheduler::estimatedMulOnResource() {
  // cpu usage
  float uUsage = 0.0, sUsage = 0.0, totalUsage = targetCpuUsage_;
  auto err = cpuTracker_.getCurrent(&uUsage, &sUsage);
  if (FOLLY_UNLIKELY(err || uUsage + sUsage == 0)) {
    LOG(ERROR) << __FUNCTION__ << " err = " << err << ", uUsage = " << uUsage
               << ", sUsage = " << sUsage;
  } else {
    totalUsage = uUsage + sUsage;
  }
  // memory usage
  double memoryRatio =
      statsCollector_.accumulatedMemoryUsedRatio_ / numFinishedTask_;
  LOG(INFO) << __FUNCTION__ << ": cpuUsage = " << totalUsage
            << ", targetCpuUsage = " << targetCpuUsage_
            << ", memoryRatio = " << memoryRatio;

  return std::min(targetCpuUsage_ / totalUsage, memoryRatio);
}

void ExecutorTaskScheduler::decideConcurrencyLocked() {
  switch (state_) {
    case SchedulerState::kSampling: {
      if (statsCollector_.accumulatedMemoryReclaimCount_) {
        // memory pressure, use default concurrency
        // todo : reduce concurrency
        currentConcurrency_ = defaultConcurrency_;
        if (ioExecutor_) {
          ioExecutor_->setNumThreads(defaultIOThreadNum_);
        }
        ++concurrencyVersion_;
        // if first sampling tasks from more than 1 stages
        // give it a second try
        if (!retrySampling_ && trackingStageIds_.size() > 1) {
          state_ = SchedulerState::kSampling;
          cpuTracker_.updateLastUsage();
          numTrackingTaskThreshold_ = currentConcurrency_;
          retrySampling_ = true;
        } else {
          state_ = SchedulerState::kCompleted;
          retrySampling_ = false;
        }
        LOG(INFO) << __FUNCTION__ << ": state_ from kSampling to "
                  << mapSchedulerStateToString(state_)
                  << ", accumulatedMemoryReclaimCount_ = "
                  << statsCollector_.accumulatedMemoryReclaimCount_
                  << ", currentConcurrency_ = " << currentConcurrency_
                  << ", ioThreadPool = "
                  << (ioExecutor_ ? ioExecutor_->numThreads() : 0)
                  << ", trackingStageIds_.size() = " << trackingStageIds_.size()
                  << ", retrySampling_ = " << retrySampling_
                  << ", numRunningTask_ = " << numRunningTask_;
        return;
      }
      double estimatedResourceMul = estimatedMulOnResource();
      int32_t newConcurrency = std::min(
          (int32_t)std::round(estimatedResourceMul * defaultConcurrency_),
          3 * defaultConcurrency_);
      size_t threadNum = 0;
      if (newConcurrency > defaultConcurrency_) {
        // finished tasks from more than 1 stage, no need to calculate
        // throughput
        if (trackingStageIds_.size() == 1) {
          inputThroughput_ = statsCollector_.accumulatedExecutionTimeMs_
              ? (1.0 * statsCollector_.accumulatedNumSourceRows_ /
                 statsCollector_.accumulatedExecutionTimeMs_)
              : 0.0;
        }
        currentConcurrency_ = newConcurrency;
        if (ioExecutor_) {
          threadNum = std::min(
              maxIoThreadNum_,
              (int32_t)std::round(ioThreadPerTask_ * newConcurrency * 1.5));
          ioExecutor_->setNumThreads(threadNum);
        }
        ++concurrencyVersion_;
        state_ = SchedulerState::kRevising;
        //        numTrackingTaskThreshold_ = defaultConcurrency_;
      } else {
        // keep original concurrency
        state_ = SchedulerState::kCompleted;
      }
      LOG(INFO) << __FUNCTION__ << ": state_ from kSampling to "
                << mapSchedulerStateToString(state_)
                << ", accumulatedNumSourceRows_ = "
                << statsCollector_.accumulatedNumSourceRows_
                << ", accumulatedExecutionTimeMs_ = "
                << statsCollector_.accumulatedExecutionTimeMs_
                << ", inputThroughput_ = " << inputThroughput_
                << ", estimatedResourceMul = " << estimatedResourceMul
                << ", newConcurrency = " << newConcurrency
                << ", currentConcurrency_ = " << currentConcurrency_
                << ", ioThreadPool = "
                << (ioExecutor_ ? ioExecutor_->numThreads() : 0)
                << ", trackingStageIds_.size() = " << trackingStageIds_.size()
                << ", retrySampling_ = " << retrySampling_
                << ", calculated threadNum = " << threadNum
                << ", numRunningTask_ = " << numRunningTask_;
      retrySampling_ = false;
      break;
    }
    case SchedulerState::kRevising: {
      // check throughput if possible
      double newThroughput = 0.0;
      if (inputThroughput_ && trackingStageIds_.size() == 1 &&
          statsCollector_.accumulatedExecutionTimeMs_) {
        newThroughput = 1.0 * statsCollector_.accumulatedNumSourceRows_ /
            statsCollector_.accumulatedExecutionTimeMs_;
      }
      bool throughputComparable = (newThroughput && inputThroughput_);
      // if throughput decreased, or throughput is not comparable but increased
      // concurrency caused memory pressure go back to default concurrency
      if (statsCollector_.accumulatedMemoryReclaimCount_) {
        // 1.5 is not accuracy
        if (!throughputComparable || newThroughput * 1.5 < inputThroughput_) {
          currentConcurrency_ = defaultConcurrency_;
          if (ioExecutor_) {
            ioExecutor_->setNumThreads(defaultIOThreadNum_);
          }
          ++concurrencyVersion_;
        }
      }
      retrySampling_ = false;
      state_ = SchedulerState::kCompleted;
      LOG(INFO) << __FUNCTION__ << ": state_ from kRevising to "
                << mapSchedulerStateToString(state_)
                << ", statsCollector_.accumulatedNumSourceRows_ = "
                << statsCollector_.accumulatedNumSourceRows_
                << ", currentConcurrency_ = " << currentConcurrency_
                << ", ioThreadPool = "
                << (ioExecutor_ ? ioExecutor_->numThreads() : 0)
                << ", trackingStageIds_.size() = " << trackingStageIds_.size()
                << ", inputThroughput_ = " << inputThroughput_
                << ", newThroughput = " << newThroughput
                << ", accumulatedMemoryReclaimCount_ = "
                << statsCollector_.accumulatedMemoryReclaimCount_
                << ", retrySampling_ = " << retrySampling_;
      break;
    }
    default:
      BOLT_UNSUPPORTED(
          "unsupported ExecutorTaskScheduler's state {}",
          mapSchedulerStateToString(state_));
      break;
  }
}

void ExecutorTaskScheduler::scheduleNewTasksIfAny(
    const std::shared_ptr<Task>& task,
    bool terminateFinished,
    bool doSchedule) {
  auto erasedCount = tasksIds_.wlock()->erase(task->taskId());
  if (!erasedCount) {
    return;
  }

  MemoryUsageStats memoryUsage;
  // collect task stats unlocked
  SimplifiedTaskStats stats;
  if (terminateFinished) {
    // get memory usage
    if (memory::sparksql::ExecutionMemoryPool::inited()) {
      memoryUsage.rssBytes = memory::MemoryUtils::getProcessRss();
      memoryUsage.vssBytes =
          memory::sparksql::ExecutionMemoryPool::instance()->memoryUsed();
      VLOG(1) << __FUNCTION__ << ": memoryUsage.rssBytes = "
              << succinctBytes(memoryUsage.rssBytes)
              << ", memoryUsage.vssBytes = "
              << succinctBytes(memoryUsage.vssBytes);
    }
    const auto& tasks = (*allTasks_.wlock())[task->taskId()];
    BOLT_CHECK(
        tasks.size() >= 1,
        "number tasks {} should >= 1, taskId {}",
        tasks.size(),
        task->taskId());
    for (const auto& savedTask : tasks) {
      auto temp = savedTask.lock();
      BOLT_CHECK(
          temp,
          "Task is destroyed, task id {}, tasks count {}",
          task->taskId(),
          tasks.size());
      collectTaskStatsUnlocked(temp->taskStatsImmutable(), stats);
    }
    BOLT_DCHECK(stats.peakMemoryBytes != 0 || stats.numSourceRows == 0);
  }
  // always erase taskId
  allTasks_.wlock()->erase(task->taskId());

  std::lock_guard<std::mutex> l(mutex_);
  if ((state_ == SchedulerState::kSampling ||
       state_ == SchedulerState::kRevising) &&
      terminateFinished) {
    auto stageId = extractStageId(task->taskId());
    trackingStageIds_.emplace(stageId);
    // record task stats during kSampling phase
    reportRuntimeStatsLocked(stats, memoryUsage);

    // recalculate concurrency if finished task count reaches
    // numTrackingTaskThreshold_
    if (++numFinishedTask_ >= numTrackingTaskThreshold_) {
      decideConcurrencyLocked();
      statsCollector_.clear();
      trackingStageIds_.clear();
      numFinishedTask_ = 0;
    }
  }
  BOLT_CHECK_GE(
      --numRunningTask_,
      0,
      "numRunningTask_ must >= 0, but now it is {}",
      numRunningTask_);
  while (doSchedule && !taskQueue_.empty() &&
         numRunningTask_ < currentConcurrency_) {
    VLOG(1) << __FUNCTION__ << ": task " << taskQueue_.front()->taskId()
            << " is scheduled to run"
            << ", taskQueue_.size() = " << taskQueue_.size() - 1
            << ", numRunningTask_ = " << numRunningTask_ + 1;
    taskQueue_.front()->notifyReadyToRun();
    startTaskTracking(taskQueue_.front()->taskId());
    taskQueue_.pop();
    ++numRunningTask_;
  }
}

void ExecutorTaskScheduler::startTaskTracking(const std::string& taskId) {
  auto stageId = extractStageId(taskId);
  bool newStageId = stageIds_.wlock()->emplace(stageId).second;
  // start running tasks of a new stage
  if (newStageId &&
      (state_ == SchedulerState::kInit ||
       state_ == SchedulerState::kCompleted)) {
    state_ = SchedulerState::kSampling;
    cpuTracker_.updateLastUsage();
    numTrackingTaskThreshold_ = currentConcurrency_;
  }
  VLOG(1) << __FUNCTION__ << ": stageId = " << stageId
          << ", is newStageId = " << newStageId
          << ", state_ = " << mapSchedulerStateToString(state_)
          << ", currentConcurrency_ = " << currentConcurrency_
          << ", ioThreadPool = "
          << (ioExecutor_ ? ioExecutor_->numThreads() : 0);
}

void ExecutorTaskScheduler::ExecutorRuntimeStatsCollector::addRuntimeStats(
    const SimplifiedTaskStats& stats) {
  accumulatedMemoryReclaimCount_ += stats.memoryReclaimCount;
  accumulatedMemoryUsedRatio_ += stats.memoryUsedRatio;
  accumulatedNumSourceRows_ += stats.numSourceRows;
  accumulatedExecutionTimeMs_ += stats.executionTimeMs;
}

} // namespace bytedance::bolt::exec
