#pragma once

#include <mutex>
#include <unordered_set>
#include <vector>

#include "bolt/common/memory/bm/IoBackend.h"

namespace bytedance::bolt::memory::bm {

struct MockSubmittedRequest {
  uint64_t requestId{0};
  IoRequest request;
};

class MockIoBackend : public IoBackend {
 public:
  bool submit(uint64_t requestId, const IoRequest& request) override;
  std::vector<BackendCompletion> reap() override;

  void complete(uint64_t requestId, IoResult result);

  std::vector<MockSubmittedRequest> submitted() const;
  size_t inflight() const;

 private:
  mutable std::mutex mutex_;
  std::vector<MockSubmittedRequest> submitted_;
  std::vector<BackendCompletion> completions_;
  std::unordered_set<uint64_t> inflight_;
};

} // namespace bytedance::bolt::memory::bm
