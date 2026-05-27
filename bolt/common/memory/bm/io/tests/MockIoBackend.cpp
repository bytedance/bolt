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
  completionEvent_.drain();
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
