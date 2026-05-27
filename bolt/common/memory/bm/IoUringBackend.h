#pragma once

#include <cstdint>
#include <memory>

#include "bolt/common/memory/bm/IoBackend.h"

namespace bytedance::bolt::memory::bm {

struct IoUringState;

class IoUringBackend : public IoBackend {
 public:
  explicit IoUringBackend(uint32_t ringDepth);
  ~IoUringBackend() override;

  bool submit(uint64_t requestId, const IoRequest& request) override;
  std::vector<BackendCompletion> reap() override;

 private:
  std::unique_ptr<IoUringState> state_;
};

} // namespace bytedance::bolt::memory::bm
