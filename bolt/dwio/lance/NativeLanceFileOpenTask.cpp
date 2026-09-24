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

#include "bolt/dwio/lance/NativeLanceFileOpenTask.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::lance::reader {

NativeLanceFileOpenTask::NativeLanceFileOpenTask(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options,
    std::shared_ptr<const NativeLanceBlobResolver> blobResolver,
    std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter)
    : input_(std::move(input)),
      options_(options),
      blobResolver_(std::move(blobResolver)),
      typeAdapter_(std::move(typeAdapter)) {
  BOLT_CHECK_NOT_NULL(input_);
  BOLT_CHECK_NOT_NULL(typeAdapter_);
}

bool NativeLanceFileOpenTask::executeStep() {
  if (state_ == NativeLanceFileOpenState::kFailed) {
    rethrowFailure();
  }
  BOLT_CHECK(
      state_ != NativeLanceFileOpenState::kReady,
      "Cannot execute a completed Lance file-open task");
  try {
    return executeImpl();
  } catch (...) {
    failure_ = std::current_exception();
    state_ = NativeLanceFileOpenState::kFailed;
    throw;
  }
}

bool NativeLanceFileOpenTask::executeImpl() {
  switch (state_) {
    case NativeLanceFileOpenState::kNeedContext:
      context_ = std::make_shared<NativeLanceFileContext>(
          std::move(input_), options_, blobResolver_, typeAdapter_);
      state_ = NativeLanceFileOpenState::kNeedValidation;
      return true;
    case NativeLanceFileOpenState::kNeedValidation:
      context_->validate();
      state_ = NativeLanceFileOpenState::kReady;
      return false;
    case NativeLanceFileOpenState::kReady:
      BOLT_UNREACHABLE();
    case NativeLanceFileOpenState::kFailed:
      rethrowFailure();
  }
  BOLT_UNREACHABLE();
}

std::shared_ptr<const NativeLanceFileContext> NativeLanceFileOpenTask::finish()
    const {
  if (state_ == NativeLanceFileOpenState::kFailed) {
    rethrowFailure();
  }
  BOLT_CHECK(
      state_ == NativeLanceFileOpenState::kReady,
      "Cannot finish an incomplete Lance file-open task");
  BOLT_CHECK_NOT_NULL(context_);
  return context_;
}

void NativeLanceFileOpenTask::rethrowFailure() const {
  BOLT_CHECK_NOT_NULL(failure_);
  std::rethrow_exception(failure_);
}

std::shared_ptr<const NativeLanceFileContext> openNativeLanceFile(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options,
    std::shared_ptr<const NativeLanceBlobResolver> blobResolver,
    std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter) {
  NativeLanceFileOpenTask task(
      std::move(input),
      options,
      std::move(blobResolver),
      std::move(typeAdapter));
  while (task.executeStep()) {
  }
  return task.finish();
}

} // namespace bytedance::bolt::lance::reader
