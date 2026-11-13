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

#pragma once

#include <folly/Synchronized.h>
#include <folly/experimental/ThreadedRepeatingFunctionRunner.h>
#include <memory>

#include "bolt/core/PlanNode.h"
#include "bolt/exec/task_manager/WrappedTask.h"

namespace bytedance::bolt::exec {

struct TaskManagerOptions {
  size_t cpuThreadNumber{0};
  size_t ioThreadNumber{0};
  bool useHiveConnector{true};
  bool useParquet{true};
  bool useHdfsFilesystem{true};
  bool useLocalFilesystem{false};
  size_t periodCleanOldTasksMs{30 * 1000}; // 30 seconds
  bool enableUserExceptionStacktrace{false};
  std::string spillDir{""};
};

struct DriverCountStats {
  size_t numBlockedDrivers{0};
  size_t numRunningDrivers{0};
};

using TaskMap = std::unordered_map<std::string, std::shared_ptr<WrappedTask>>;
class TaskManager {
  static constexpr folly::StringPiece kMaxDriversPerTask{
      "driver_count_per_task"};

  static constexpr folly::StringPiece kConcurrentLifespansPerTask{
      "concurrent_lifespans_per_task"};

 public:
  TaskManager() = delete;

  TaskManager(
      const TaskManagerOptions& options,
      const memory::MemoryManager::Options& memoryOptions);

  ~TaskManager();

  /// Cancels outstanding tasks, wakes blocked consumers and waits for drivers.
  void shutdown();

  /// Sets the time (ms) that a task is considered to be old for cleanup since
  /// its completion.
  void setOldTaskCleanUpMs(int32_t oldTaskCleanUpMs);

  TaskMap tasks() const;

  std::shared_ptr<WrappedTask> createTask(
      const std::string& taskId,
      const core::PlanFragment& planFragment,
      const ArrowOptions& arrowOptions = {},
      const std::vector<std::string>& fieldNames = {},
      const std::unordered_map<core::PlanNodeId, std::vector<Split>>& splits =
          {},
      int32_t maxDrivers = 1,
      const std::unordered_map<std::string, std::string>& queryConfigs = {},
      uint64_t bufferedBytes = 512 * 1024,
      long startProcessCpuTime = 0);

  void addSplit(
      const std::string& taskId,
      const core::PlanNodeId& nodeId,
      Split&& split);

  void noMoreSplits(const std::string& taskId, const core::PlanNodeId& nodeId);

  std::shared_ptr<WrappedTask> getTask(const std::string& taskId);

  int32_t deleteTask(const std::string& taskId, bool cleanTask = false);

  std::string toString() const;

  std::string treeMemoryUsage() const;

  inline size_t getNumTasks() const {
    return taskMap_.rlock()->size();
  }

 private:
  /// Remove old Finished, Cancelled, Failed and Aborted tasks.
  /// Old is being defined by the lifetime of the task.
  size_t cleanOldTasks();

  template <typename TFunc>
  void addTask(TFunc&& func, size_t periodMicros, const std::string& taskName) {
    repeatedRunner_.add(
        taskName,
        [taskName,
         periodMicros,
         func = std::forward<TFunc>(func)]() mutable noexcept {
          try {
            func();
          } catch (const std::exception& e) {
            LOG(ERROR) << "Error running periodic task " << taskName << ": "
                       << e.what();
          }
          return std::chrono::milliseconds(periodMicros);
        });
  }

  // Returns the number of running drivers in all tasks.
  DriverCountStats getDriverCountStats() const;

  // Returns array with number of tasks for each of five TaskState (enum defined
  // in exec/Task.h).
  std::array<size_t, 5> getTaskNumbers(size_t& numTasks) const;

 private:
  int32_t oldTaskCleanUpMs_{30 * 1000}; // 30 seconds
  folly::Synchronized<TaskMap> taskMap_;
  // Guarded by taskMap_. A null entry reserves an ID during initialization;
  // a non-null entry lets shutdown find a task during startup.
  TaskMap creatingTasks_;

  std::shared_ptr<folly::CPUThreadPoolExecutor> driverExecutor_;
  std::shared_ptr<folly::IOThreadPoolExecutor> connectorIoExecutor_;
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> outputBufferPool_;
  std::string spillDir_{""};

  // Guarded by taskMap_.
  bool isShutdown_{false};
  folly::ThreadedRepeatingFunctionRunner repeatedRunner_;
};

} // namespace bytedance::bolt::exec
