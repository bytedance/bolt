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

#include <cstdint>
#include <vector>

#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoResult.h"

namespace bytedance::bolt::memory::bm {

struct BackendCompletion {
  uint64_t requestId{0};
  IoResult result;
};

enum class BackendSubmitStatus : uint8_t {
  Submitted,
  // Temporary backend backpressure, for example an io_uring SQ with no free
  // SQE. The scheduler should retry later instead of failing the request.
  RetryableBusy,
  Failed,
};

class IoBackend {
 public:
  virtual ~IoBackend() = default;

  // DiskIoScheduler calls submit() and reap() from its worker thread serially.
  // Implementations do not need to be internally thread-safe for those calls,
  // and they must not depend on the scheduler mutex being held: backend work
  // may enter the kernel or scan many completions, so the scheduler keeps its
  // own lock out of this interface to preserve lightweight enqueue semantics.
  //
  // completionFd() must return an fd that becomes readable when completions may
  // be available. The signal is only a hint: reap() must drain both the backend
  // completion queue and the fd notification counter before returning.
  virtual int completionFd() const = 0;
  virtual BackendSubmitStatus submit(
      uint64_t requestId,
      const IoRequest& request) = 0;
  virtual std::vector<BackendCompletion> reap() = 0;
};

} // namespace bytedance::bolt::memory::bm
