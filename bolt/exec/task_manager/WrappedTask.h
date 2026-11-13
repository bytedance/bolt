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

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <limits>

#include "bolt/dwio/common/Options.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/task_manager/BoltVectorQueue.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"

namespace bytedance::bolt::exec {

class WrappedTask {
 public:
  static inline const std::string kHiveConnectorId = "test-hive";

  /// Error before task is created or when task is being created.
  std::exception_ptr error{nullptr};

  /// @param taskId Task ID.
  /// @param startCpuTime CPU time in nanoseconds recorded when request to
  /// create this task arrived.
  WrappedTask(
      std::shared_ptr<memory::MemoryPool> pool,
      const ArrowOptions& options = {},
      const std::vector<std::string>& fieldNames = {},
      uint64_t maxBytes = 512 * 1024,
      int64_t startProcessCpuTime = 0)
      : bufferedPool_(pool),
        options_(options),
        fieldNames_(fieldNames),
        queue_(std::make_shared<BoltVectorQueue>(pool, maxBytes)),
        startProcessCpuTime_(startProcessCpuTime) {}

  ~WrappedTask();

  /// Updates when this task was touched last time.
  inline void updateHeartbeatLocked() {
    lastHeartbeatMs_ = getCurrentTimeMs();
  };

  /// Returns time (ms) since the task was touched last time (last heartbeat).
  /// Returns zero, if never (shouldn't happen).
  uint64_t timeSinceLastHeartbeatMs() const;

  /// Turns the task numbers (per state) into a string.
  static std::string taskNumbersToString(
      const std::array<size_t, 5>& taskNumbers);

  inline const std::shared_ptr<Task> getTask() const {
    return internalTask_;
  }

  inline std::string getTaskId() const {
    if (!internalTask_) {
      return "";
    }
    return internalTask_->taskId();
  }

  bool moveNext();

  inline RowVectorPtr currentResult() const {
    return current_;
  }

  std::shared_ptr<arrow::RecordBatch> currentArrowResult();

  bool convertCurrentToArrow(ArrowSchema& schema, ArrowArray& array);

  inline void addSplit(Split&& split) {
    // "0" as scan node id usually
    this->addSplit("0", std::move(split));
  }

  inline void addSplit(const core::PlanNodeId& nodeId, Split&& split) {
    // "0" as scan node id usually
    internalTask_->addSplit(nodeId, std::move(split));
  }

  inline void noMoreSplits(const core::PlanNodeId& nodeId = "0") {
    // "0" as scan node id usually
    internalTask_->noMoreSplits(nodeId);
  }

  void addHiveFile(
      const core::PlanNodeId& nodeId,
      const std::string& filePath,
      dwio::common::FileFormat fileFormat,
      size_t offset = 0,
      size_t length = std::numeric_limits<size_t>::max());

  void setCustomContext(std::any customContext) {
    internalTask_->queryCtx()->customContext() = std::move(customContext);
  }

  const std::any& getCustomContext() {
    return internalTask_->queryCtx()->customContext();
  }

  friend class TaskManager;

 private:
  void recordProcessCpuTime();

  std::shared_ptr<memory::MemoryPool> bufferedPool_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> driverExecutor_;
  const ArrowOptions options_;
  const std::vector<std::string> fieldNames_;

  std::shared_ptr<BoltVectorQueue> queue_;
  RowVectorPtr current_;
  const long startProcessCpuTime_;
  long processCpuTime_{0};

  // Has the task been normally created and started.
  // When you create task with error - it has never been started.
  // When you create task from 'delete task' - it has never been started.
  // When you create task from any other endpoint, such as 'get result' - it has
  // not been started, until the actual 'create task' message comes.
  bool taskStarted{false};

  uint64_t lastHeartbeatMs_{0};
  mutable std::mutex mutex_;

  std::shared_ptr<Task> internalTask_;
};

} // namespace bytedance::bolt::exec
