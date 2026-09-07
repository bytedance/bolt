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

#include <cstddef>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "bolt/common/memory/bm/io/EventFd.h"
#include "bolt/common/memory/bm/io/IoBackend.h"

namespace bytedance::bolt::memory::bm {

struct MockSubmittedRequest {
  uint64_t requestId{0};
  IoPriority priority{IoPriority::Medium};
};

class MockIoBackend : public IoBackend {
 public:
  int completionFd() const override;
  BackendSubmitStatus submit(uint64_t requestId, const IoRequest& request)
      override;
  std::vector<BackendCompletion> reap() override;

  void complete(uint64_t requestId, IoResult result);

  std::vector<MockSubmittedRequest> submitted() const;
  size_t inflight() const;

 private:
  mutable std::mutex mutex_;
  std::vector<MockSubmittedRequest> submitted_;
  std::vector<BackendCompletion> completions_;
  std::unordered_set<uint64_t> inflight_;
  EventFd completionEvent_;
};

} // namespace bytedance::bolt::memory::bm
