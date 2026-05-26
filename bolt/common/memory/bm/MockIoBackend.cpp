#include "bolt/common/memory/bm/MockIoBackend.h"

namespace bytedance::bolt::memory::bm {

bool MockIoBackend::submit(uint64_t requestId, const IoRequest& request) {
  std::lock_guard<std::mutex> lock(mutex_);
  submitted_.push_back(MockSubmittedRequest{requestId, request});
  inflight_.insert(requestId);
  return true;
}

std::vector<BackendCompletion> MockIoBackend::reap() {
  std::lock_guard<std::mutex> lock(mutex_);
  auto completions = std::move(completions_);
  completions_.clear();
  return completions;
}

void MockIoBackend::complete(uint64_t requestId, IoResult result) {
  std::lock_guard<std::mutex> lock(mutex_);
  inflight_.erase(requestId);
  completions_.push_back(BackendCompletion{requestId, result});
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
