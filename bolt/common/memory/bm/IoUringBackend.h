#pragma once

#include <cstdint>
#include <memory>

#include "bolt/common/memory/bm/IoBackend.h"

#ifdef IO_URING_SUPPORTED
struct io_uring;
#endif

namespace bytedance::bolt::memory::bm {

class IoUringBackend : public IoBackend {
 public:
  explicit IoUringBackend(uint32_t ringDepth);
  ~IoUringBackend() override;

  bool submit(uint64_t requestId, const IoRequest& request) override;
  std::vector<BackendCompletion> reap() override;

 private:
#ifdef IO_URING_SUPPORTED
  std::unique_ptr<io_uring> ring_;
#endif
};

} // namespace bytedance::bolt::memory::bm
