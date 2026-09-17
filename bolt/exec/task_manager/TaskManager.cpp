/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/exec/task_manager/TaskManager.h"

#include <folly/ScopeGuard.h>

#include "bolt/connectors/hive/HiveConnector.h"
#ifdef BOLT_ENABLE_HDFS
#include "bolt/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#endif
#include "bolt/core/PlanNode.h"
#ifdef BOLT_ENABLE_PARQUET
#include "bolt/dwio/parquet/RegisterParquetReader.h"
#endif
#include "bolt/exec/Exchange.h"
#include "bolt/exec/Task.h"

namespace bytedance::bolt::exec {

namespace {
struct ZombieTaskStats {
  const std::string taskId;
  const std::string taskInfo;

  explicit ZombieTaskStats(const std::shared_ptr<Task>& task)
      : taskId(task->taskId()), taskInfo(task->toString()) {}

  std::string toString() const {
    return taskInfo;
  }
};

// Helper structure holding stats for 'zombie' tasks.
struct ZombieTaskStatsSet {
  size_t numRunning{0};
  size_t numFinished{0};
  size_t numCanceled{0};
  size_t numAborted{0};
  size_t numFailed{0};
  size_t numTotal{0};
  const size_t numSampleTasks;
  std::vector<ZombieTaskStats> tasks;
  ZombieTaskStatsSet() : numSampleTasks(10) {
    tasks.reserve(numSampleTasks);
  }

  void updateCounts(const std::shared_ptr<Task>& task) {
    switch (task->state()) {
      case TaskState::kRunning:
        ++numRunning;
        break;
      case TaskState::kFinished:
        ++numFinished;
        break;
      case TaskState::kCanceled:
        ++numCanceled;
        break;
      case TaskState::kAborted:
        ++numAborted;
        break;
      case TaskState::kFailed:
        ++numFailed;
        break;
      default:
        break;
    }
    if (tasks.size() < numSampleTasks) {
      tasks.emplace_back(task);
    }
  }

  void logZombieTaskStatus(const std::string& hangingClassName) {
    LOG(ERROR) << "There are " << numTotal << " zombie " << hangingClassName
               << " that satisfy cleanup conditions but could not be "
                  "cleaned up, because the "
               << hangingClassName
               << " are referenced by more than 1 owners. RUNNING["
               << numRunning << "] FINISHED[" << numFinished << "] CANCELED["
               << numCanceled << "] ABORTED[" << numAborted << "] FAILED["
               << numFailed << "]  Sample task IDs (shows only "
               << numSampleTasks << " IDs): " << std::endl;
  }
};
} // namespace

TaskManager::TaskManager(
    const TaskManagerOptions& options,
    const memory::MemoryManager::Options& memoryOptions)
    : driverExecutor_(std::make_shared<folly::CPUThreadPoolExecutor>(
          options.cpuThreadNumber == 0 ? std::thread::hardware_concurrency()
                                       : options.cpuThreadNumber,
          std::make_shared<folly::NamedThreadFactory>("Driver"))),
      connectorIoExecutor_(std::make_shared<folly::IOThreadPoolExecutor>(
          options.ioThreadNumber == 0 ? std::thread::hardware_concurrency()
                                      : options.ioThreadNumber,
          std::make_shared<folly::NamedThreadFactory>("Connector"))) {
  // Registration is process-wide, and multiple managers may coexist.
  static std::mutex initializationMutex;
  std::lock_guard<std::mutex> initializationLock(initializationMutex);
  if (!memory::MemoryManager::testInstance()) {
    memory::MemoryManager::initialize(memoryOptions);
  }
  oldTaskCleanUpMs_ = options.periodCleanOldTasksMs;
  rootPool_ = memory::memoryManager()->addRootPool();
  outputBufferPool_ = rootPool_->addLeafChild("OutputBufferPool");

  if (options.useHiveConnector) {
    if (!connector::isConnectorRegistered(
            std::string(WrappedTask::kHiveConnectorId))) {
      auto hiveConnector =
          connector::getConnectorFactory(connector::kHiveConnectorName)
              ->newConnector(
                  std::string(WrappedTask::kHiveConnectorId),
                  std::make_shared<config::ConfigBase>(
                      std::unordered_map<std::string, std::string>()),
                  connectorIoExecutor_.get());
      auto* connectorPtr = hiveConnector.get();
      connector::registerConnector(std::shared_ptr<connector::Connector>(
          connectorPtr,
          [executor = connectorIoExecutor_,
           hiveConnector](connector::Connector*) mutable {
            hiveConnector.reset();
            executor.reset();
          }));
    }
  }

  if (options.useParquet) {
#ifdef BOLT_ENABLE_PARQUET
    parquet::registerParquetReaderFactory();
#else
    BOLT_FAIL("Parquet support is disabled in this build");
#endif
  }
  if (options.useHdfsFilesystem) {
#ifdef BOLT_ENABLE_HDFS
    filesystems::registerHdfsFileSystem();
#else
    BOLT_FAIL("HDFS support is disabled in this build");
#endif
  }
  if (options.useLocalFilesystem) {
    filesystems::registerLocalFileSystem();
  }
  if (options.enableUserExceptionStacktrace) {
    FLAGS_bolt_exception_user_stacktrace_enabled = true;
  }

  if (!options.spillDir.empty()) {
    filesystems::getFileSystem(options.spillDir, nullptr)
        ->mkdir(options.spillDir);
    spillDir_ = options.spillDir;
  }

  if (oldTaskCleanUpMs_ > 0) {
    LOG(INFO) << "Enable clean old tasks every " << oldTaskCleanUpMs_ << " ms";
    addTask(
        [this]() { cleanOldTasks(); }, oldTaskCleanUpMs_, "clean_old_tasks");
  }
}

TaskManager::~TaskManager() {
  repeatedRunner_.stop();
  shutdown();
  driverExecutor_->join();
  // A registered Hive connector retains its I/O executor.
}

void TaskManager::setOldTaskCleanUpMs(int32_t oldTaskCleanUpMs) {
  BOLT_CHECK_GE(oldTaskCleanUpMs, 0);
  oldTaskCleanUpMs_ = oldTaskCleanUpMs;
}

TaskMap TaskManager::tasks() const {
  return taskMap_.withRLock([](const auto& tasks) { return tasks; });
}

std::shared_ptr<WrappedTask> TaskManager::createTask(
    const std::string& taskId,
    const core::PlanFragment& planFragment,
    const ArrowOptions& arrowOptions,
    const std::vector<std::string>& fieldNames,
    const std::unordered_map<core::PlanNodeId, std::vector<Split>>& splits,
    int32_t maxDrivers,
    const std::unordered_map<std::string, std::string>& queryConfigs,
    uint64_t bufferedBytes,
    int64_t startProcessCpuTime) {
  if (!planFragment.planNode) {
    BOLT_FAIL("no planNode input");
  }

  {
    auto taskMap = taskMap_.wlock();
    BOLT_CHECK(!isShutdown_, "Task manager is shut down");
    BOLT_CHECK(
        taskMap->find(taskId) == taskMap->end() &&
            creatingTasks_.find(taskId) == creatingTasks_.end(),
        "Task {} already exists",
        taskId);
    // Reserve the ID without publishing a partially initialized task.
    creatingTasks_.emplace(taskId, nullptr);
  }
  auto creationGuard = folly::makeGuard([&] {
    std::shared_ptr<WrappedTask> failedTask;
    taskMap_.withWLock([&](auto&) {
      auto it = creatingTasks_.find(taskId);
      if (it != creatingTasks_.end()) {
        failedTask = std::move(it->second);
        creatingTasks_.erase(it);
      }
    });
    // Destroy failed tasks (including cancellation and spill cleanup) unlocked.
  });

  auto boltTask = std::make_shared<WrappedTask>(
      outputBufferPool_,
      arrowOptions,
      fieldNames,
      bufferedBytes,
      startProcessCpuTime);
  boltTask->driverExecutor_ = driverExecutor_;

  std::shared_ptr<Task> execTask;
  {
    std::lock_guard<std::mutex> l(boltTask->mutex_);
    if (boltTask->internalTask_) {
      BOLT_FAIL(
          "Task {} has been created ",
          taskId,
          boltTask->taskStarted ? "and started" : "but not started");
    }

    auto queryCtx = core::QueryCtx::create(
        driverExecutor_.get(),
        core::QueryConfig{queryConfigs},
        std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>{},
        nullptr,
        rootPool_,
        nullptr,
        taskId);

    boltTask->internalTask_ = Task::create(
        taskId,
        planFragment,
        0,
        std::move(queryCtx),
        Task::ExecutionMode::kParallel,
        // consumer
        [queue = boltTask->queue_](
            RowVectorPtr vector,
            bolt::ContinueFuture* future,
            uint32_t partitionId) { return queue->enqueue(vector, future); },
        0,
        std::nullopt,
        [queue = boltTask->queue_](std::exception_ptr) { queue->close(true); });
    BOLT_CHECK_NOT_NULL(
        boltTask->internalTask_, "Task create failed with {}", taskId);
    execTask = boltTask->internalTask_;
    if (!spillDir_.empty()) {
      const auto taskSpillDir = spillDir_ + "/" + execTask->uuid();
      filesystems::getFileSystem(taskSpillDir, nullptr)->mkdir(taskSpillDir);
      execTask->setSpillDirectory(taskSpillDir, /*alreadyCreated=*/true);
    }
  }

  // Shutdown may now find this task, but must wait for startup to finish
  // before canceling it. No filesystem operation or startup holds taskMap_.
  std::lock_guard<std::mutex> lock(boltTask->mutex_);
  {
    auto taskMap = taskMap_.wlock();
    BOLT_CHECK(!isShutdown_, "Task manager is shut down");
    creatingTasks_.at(taskId) = boltTask;
  }

  execTask->start(maxDrivers);
  boltTask->queue_->setNumProducers(execTask->numOutputDrivers());
  boltTask->taskStarted = true;

  for (auto& [nodeId, nodeSplits] : splits) {
    for (const auto& split : nodeSplits) {
      execTask->addSplit(nodeId, exec::Split(split));
    }
    execTask->noMoreSplits(nodeId);
  }

  {
    auto taskMap = taskMap_.wlock();
    BOLT_CHECK(!isShutdown_, "Task manager is shut down");
    taskMap->emplace(taskId, boltTask);
    creatingTasks_.erase(taskId);
    creationGuard.dismiss();
  }
  return boltTask;
}

void TaskManager::addSplit(
    const std::string& taskId,
    const core::PlanNodeId& nodeId,
    Split&& split) {
  auto boltTask = getTask(taskId);
  if (boltTask == nullptr) {
    BOLT_FAIL("Task {} not found", taskId);
  }

  std::lock_guard<std::mutex> l(boltTask->mutex_);
  boltTask->internalTask_->addSplit(nodeId, std::move(split));
}

void TaskManager::noMoreSplits(
    const std::string& taskId,
    const core::PlanNodeId& nodeId) {
  auto boltTask = getTask(taskId);
  if (boltTask == nullptr) {
    BOLT_FAIL("Task {} not found", taskId);
  }

  std::lock_guard<std::mutex> l(boltTask->mutex_);
  boltTask->noMoreSplits(nodeId);
}

std::shared_ptr<WrappedTask> TaskManager::getTask(const std::string& taskId) {
  std::shared_ptr<WrappedTask> boltTask = nullptr;
  taskMap_.withRLock([&](const auto& taskMap) {
    auto it = taskMap.find(taskId);
    if (it != taskMap.end()) {
      boltTask = it->second;
    }
  });

  if (boltTask != nullptr) {
    std::lock_guard<std::mutex> l(boltTask->mutex_);
    boltTask->updateHeartbeatLocked();
    return boltTask;
  }

  return boltTask;
}

int32_t TaskManager::deleteTask(const std::string& taskId, bool cleanTask) {
  LOG(INFO) << "Deleting task " << taskId;
  // Fast. non-blocking delete and cancel serialized on 'taskMap'.
  std::shared_ptr<WrappedTask> boltTask = nullptr;

  taskMap_.withRLock([&](const auto& taskMap) {
    auto it = taskMap.find(taskId);
    if (it != taskMap.cend()) {
      boltTask = it->second;
    }
  });

  if (boltTask == nullptr) {
    return -1;
  }

  {
    std::lock_guard<std::mutex> l(boltTask->mutex_);
    boltTask->updateHeartbeatLocked();
    const auto& execTask = boltTask->internalTask_;
    if (execTask) {
      auto state = execTask->state();
      if (state == kRunning) {
        auto canceled = execTask->requestCancel();
        boltTask->queue_->close(true);
        canceled.wait();
      }
    } else {
      BOLT_FAIL("Task {} is not exist", taskId);
    }
  }

  if (cleanTask) {
    taskMap_.withWLock([&](auto& taskMap) {
      if (boltTask.use_count() > 2 || boltTask->internalTask_.use_count() > 1) {
        BOLT_FAIL(
            "Task {} is used by other components, wrapped use count = {}, internal use count = {}",
            taskId,
            boltTask.use_count(),
            boltTask->internalTask_.use_count());
      } else {
        taskMap.erase(taskId);
      }
    });
  }

  return 0;
}

size_t TaskManager::cleanOldTasks() {
  const auto startTimeMs = getCurrentTimeMs();

  folly::F14FastSet<std::string> taskIdsToClean;

  ZombieTaskStatsSet zombieInternalTaskCounts;
  ZombieTaskStatsSet zombieBoltTaskCounts;
  taskMap_.withRLock([&](const auto& taskMap) {
    for (const auto& [id, boltTask] : taskMap) {
      bool eraseTask{false};
      if (boltTask->internalTask_ != nullptr) {
        if (boltTask->internalTask_->state() != TaskState::kRunning) {
          // Since the state is not running, we know the task has been
          // terminated. We use termination time instead of end time as the
          // former does not include time waiting for results to be consumed.
          if (boltTask->internalTask_->timeSinceTerminationMs() >=
              oldTaskCleanUpMs_) {
            // Not running and old.
            eraseTask = true;
          }
        }
      } else {
        // Use heartbeat to determine the task's age.
        if (boltTask->timeSinceLastHeartbeatMs() >= oldTaskCleanUpMs_) {
          eraseTask = true;
        }
      }

      // We assume 'not erase' is the 'most common' case.
      if (!eraseTask) {
        continue;
      }

      const auto boltTaskRefCount = boltTask.use_count();
      const auto taskRefCount = boltTask->internalTask_.use_count();

      // Do not remove 'zombie' tasks (with outstanding references) from the
      // map. The map owns one reference to each wrapper.
      if (boltTaskRefCount > 1 || taskRefCount > 1) {
        auto& task = boltTask->internalTask_;
        if (boltTaskRefCount > 1) {
          ++zombieBoltTaskCounts.numTotal;
          if (task != nullptr) {
            zombieBoltTaskCounts.updateCounts(task);
          }
        }
        if (taskRefCount > 1) {
          ++zombieInternalTaskCounts.numTotal;
          zombieInternalTaskCounts.updateCounts(task);
        }
      } else {
        taskIdsToClean.emplace(id);
      }
    }
  });

  size_t numCleaned = 0;
  const auto elapsedMs = (getCurrentTimeMs() - startTimeMs);
  if (not taskIdsToClean.empty()) {
    std::vector<std::shared_ptr<WrappedTask>> tasksToDelete;
    tasksToDelete.reserve(taskIdsToClean.size());
    taskMap_.withWLock([&](auto& taskMap) {
      // Remove tasks from the task map. We briefly lock for write here.
      for (const auto& taskId : taskIdsToClean) {
        auto it = taskMap.find(taskId);
        if (it == taskMap.end() || it->second.use_count() > 1 ||
            it->second->internalTask_.use_count() > 1) {
          continue;
        }
        const auto& task = it->second->internalTask_;
        if (task &&
            (task->state() == TaskState::kRunning ||
             task->timeSinceTerminationMs() < oldTaskCleanUpMs_)) {
          continue;
        }
        tasksToDelete.push_back(std::move(it->second));
        taskMap.erase(it);
      }
    });
    numCleaned = tasksToDelete.size();
    LOG(INFO) << "cleanOldTasks: Cleaned " << numCleaned << " old task(s) in "
              << elapsedMs << " ms";
  } else if (elapsedMs > 1000) {
    // If we took more than 1 second to run this, something might be wrong.
    LOG(INFO) << "cleanOldTasks: Didn't clean any old task(s). Took "
              << elapsedMs << "ms";
  }

  if (zombieInternalTaskCounts.numTotal > 0) {
    zombieInternalTaskCounts.logZombieTaskStatus("Task");
  }
  if (zombieBoltTaskCounts.numTotal > 0) {
    zombieBoltTaskCounts.logZombieTaskStatus("WrappedTask");
  }
  return numCleaned;
}

std::string TaskManager::toString() const {
  std::stringstream out;
  auto taskMap = taskMap_.rlock();
  for (const auto& pair : *taskMap) {
    if (pair.second->internalTask_) {
      out << pair.second->internalTask_->toString() << std::endl;
    } else {
      out << Task::shortId(pair.first) << " no task (" << pair.first << ")"
          << std::endl;
    }
  }
  return out.str();
}

std::string TaskManager::treeMemoryUsage() const {
  return rootPool_->treeMemoryUsage();
}

DriverCountStats TaskManager::getDriverCountStats() const {
  auto taskMap = taskMap_.rlock();
  DriverCountStats driverCountStats;
  for (const auto& pair : *taskMap) {
    if (pair.second->internalTask_ != nullptr) {
      driverCountStats.numRunningDrivers +=
          pair.second->internalTask_->numRunningDrivers();
    }
  }
  driverCountStats.numBlockedDrivers = BlockingState::numBlockedDrivers();
  return driverCountStats;
}

std::array<size_t, 5> TaskManager::getTaskNumbers(size_t& numTasks) const {
  std::array<size_t, 5> res{0};
  auto taskMap = taskMap_.rlock();
  numTasks = 0;
  for (const auto& pair : *taskMap) {
    if (pair.second->internalTask_ != nullptr) {
      ++res[pair.second->internalTask_->state()];
      ++numTasks;
    }
  }
  return res;
}

void TaskManager::shutdown() {
  TaskMap tasksToStop;
  taskMap_.withWLock([&](auto& taskMap) {
    if (isShutdown_) {
      return;
    }
    isShutdown_ = true;
    tasksToStop.swap(taskMap);
    for (auto& [id, task] : creatingTasks_) {
      if (task) {
        tasksToStop.emplace(id, std::move(task));
      }
    }
    creatingTasks_.clear();
  });
  std::vector<ContinueFuture> canceled;
  for (const auto& [id, task] : tasksToStop) {
    std::lock_guard<std::mutex> lock(task->mutex_);
    if (task->internalTask_) {
      canceled.push_back(task->internalTask_->requestCancel());
    }
    task->queue_->close(
        !task->internalTask_ ||
        task->internalTask_->state() != TaskState::kFinished);
  }
  for (auto& future : canceled) {
    future.wait();
  }
}

} // namespace bytedance::bolt::exec
