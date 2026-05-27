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
  virtual BackendSubmitStatus submit(
      uint64_t requestId,
      const IoRequest& request) = 0;
  virtual std::vector<BackendCompletion> reap() = 0;
};

} // namespace bytedance::bolt::memory::bm
