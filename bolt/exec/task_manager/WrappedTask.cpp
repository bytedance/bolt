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

#include "bolt/exec/task_manager/WrappedTask.h"
#include "bolt/exec/task_manager/Utils.h"

namespace bytedance::bolt::exec {

namespace {
// Arrow owns the exported buffers independently of WrappedTask. Retain their
// pool until the original Arrow release callback has freed those buffers.
struct ArrowResultOwner {
  std::shared_ptr<memory::MemoryPool> pool;
  ArrowArray original;
};

void retainPool(ArrowArray& array, std::shared_ptr<memory::MemoryPool> pool) {
  auto owner = std::make_unique<ArrowResultOwner>();
  owner->pool = std::move(pool);
  owner->original = array;
  array.private_data = owner.release();
  array.release = [](ArrowArray* array) {
    auto owner = std::unique_ptr<ArrowResultOwner>(
        static_cast<ArrowResultOwner*>(array->private_data));
    owner->original.release(&owner->original);
    array->release = nullptr;
    array->private_data = nullptr;
  };
}
} // namespace

WrappedTask::~WrappedTask() {
  if (internalTask_) {
    auto canceled = internalTask_->requestCancel();
    queue_->close(true);
    canceled.wait();
  } else {
    queue_->close(true);
  }
}

std::shared_ptr<arrow::RecordBatch> WrappedTask::currentArrowResult() {
  if (!current_) {
    return nullptr;
  }

  for (auto& child : current_->children()) {
    if (child) {
      child->loadedVector();
    }
  }

  ArrowSchema schema;
  ArrowArray array;
  memset(&schema, 0, sizeof(ArrowSchema));
  memset(&array, 0, sizeof(ArrowArray));

  exportToArrow(current_, schema, options_, fieldNames_);
  exportToArrow(current_, array, bufferedPool_.get(), options_);
  retainPool(array, bufferedPool_);

  auto resultImportVectorSchemaRoot = arrow::ImportRecordBatch(&array, &schema);
  if (!resultImportVectorSchemaRoot.ok()) {
    if (array.release) {
      array.release(&array);
    }
    if (schema.release) {
      schema.release(&schema);
    }
    BOLT_FAIL(
        "Arrow import failed: {}",
        resultImportVectorSchemaRoot.status().ToString());
  }
  std::shared_ptr<arrow::RecordBatch> recordBatch =
      *resultImportVectorSchemaRoot;
  return recordBatch;
}

bool WrappedTask::convertCurrentToArrow(
    ArrowSchema& schema,
    ArrowArray& array) {
  if (!current_) {
    return false;
  }

  for (auto& child : current_->children()) {
    if (child) {
      child->loadedVector();
    }
  }
  VectorPtr flatternVector = current_;
  BaseVector::flattenVector(flatternVector);

  exportToArrow(flatternVector, schema, options_, fieldNames_);
  exportToArrow(flatternVector, array, bufferedPool_.get(), options_);
  retainPool(array, bufferedPool_);

  return true;
}

std::string WrappedTask::taskNumbersToString(
    const std::array<size_t, 5>& taskNumbers) {
  // Names of five TaskState (enum defined in exec/Task.h).
  static constexpr std::array<folly::StringPiece, 5> taskStateNames{
      "Running",
      "Finished",
      "Canceled",
      "Aborted",
      "Failed",
  };

  std::string str;
  for (size_t i = 0; i < taskNumbers.size(); ++i) {
    if (taskNumbers[i] != 0) {
      folly::toAppend(
          fmt::format("{}={} ", taskStateNames[i], taskNumbers[i]), &str);
    }
  }
  return str;
}

bool WrappedTask::moveNext() {
  current_ = queue_->dequeue();

  if (internalTask_->error()) {
    current_ = nullptr;
    std::rethrow_exception(internalTask_->error());
  }

  return current_ != nullptr;
}

uint64_t WrappedTask::timeSinceLastHeartbeatMs() const {
  std::lock_guard<std::mutex> l(mutex_);
  if (lastHeartbeatMs_ == 0UL) {
    return 0UL;
  }
  return getCurrentTimeMs() - lastHeartbeatMs_;
}

void WrappedTask::addHiveFile(
    const core::PlanNodeId& nodeId,
    const std::string& filePath,
    dwio::common::FileFormat fileFormat,
    size_t offset,
    size_t length) {
  addSplit(
      nodeId,
      makeSplit(
          WrappedTask::kHiveConnectorId, filePath, fileFormat, offset, length));
}
} // namespace bytedance::bolt::exec
