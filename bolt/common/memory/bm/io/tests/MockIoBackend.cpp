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

#include "bolt/common/memory/bm/io/tests/MockIoBackend.h"

#include "bolt/common/base/Exceptions.h"

#include <utility>

namespace bytedance::bolt::memory::bm {

int MockIoBackend::completionFd() const {
  return completionEvent_.fd();
}

BackendSubmitStatus MockIoBackend::submit(
    uint64_t requestId,
    const IoRequest& request) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (inflight_.count(requestId) != 0) {
    return BackendSubmitStatus::Failed;
  }
  submitted_.push_back(MockSubmittedRequest{requestId, request.priority});
  inflight_.insert(requestId);
  return BackendSubmitStatus::Submitted;
}

std::vector<BackendCompletion> MockIoBackend::reap() {
  std::lock_guard<std::mutex> lock(mutex_);
  completionEvent_.drainNonBlocking();
  auto completions = std::move(completions_);
  completions_.clear();
  return completions;
}

void MockIoBackend::complete(uint64_t requestId, IoResult result) {
  std::lock_guard<std::mutex> lock(mutex_);
  BOLT_CHECK(inflight_.erase(requestId) == 1, "unknown requestId");
  completions_.push_back(BackendCompletion{requestId, std::move(result)});
  completionEvent_.notify();
}

std::vector<MockSubmittedRequest> MockIoBackend::submitted() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return submitted_;
}

size_t MockIoBackend::inflight() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return inflight_.size();
}

} // namespace bytedance::bolt::memory::bm
