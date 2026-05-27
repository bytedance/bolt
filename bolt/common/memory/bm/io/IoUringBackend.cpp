#include "bolt/common/memory/bm/io/IoUringBackend.h"

#ifdef IO_URING_SUPPORTED
#include <liburing.h>
#include <liburing/io_uring.h>
#endif

#include <cstring>
#include <utility>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/io/EventFd.h"

namespace bytedance::bolt::memory::bm {

#ifdef IO_URING_SUPPORTED
struct IoUringState {
  io_uring ring;
  EventFd completionEvent;
  bool ringInitialized{false};
  bool eventRegistered{false};
};

IoUringBackend::IoUringBackend(uint32_t ringDepth) {
  state_ = std::make_unique<IoUringState>();
  const int ret = io_uring_queue_init(ringDepth, &state_->ring, 0);
  BOLT_CHECK_GE(ret, 0, "io_uring_queue_init failed: {}", std::strerror(-ret));
  state_->ringInitialized = true;
  const int registerRet =
      io_uring_register_eventfd(&state_->ring, state_->completionEvent.fd());
  if (registerRet < 0) {
    io_uring_queue_exit(&state_->ring);
    state_->ringInitialized = false;
  }
  BOLT_CHECK_GE(
      registerRet,
      0,
      "io_uring_register_eventfd failed: {}",
      std::strerror(-registerRet));
  state_->eventRegistered = true;
}

IoUringBackend::~IoUringBackend() {
  if (state_) {
    if (state_->eventRegistered) {
      io_uring_unregister_eventfd(&state_->ring);
    }
    if (state_->ringInitialized) {
      io_uring_queue_exit(&state_->ring);
    }
  }
}

int IoUringBackend::completionFd() const {
  return state_->completionEvent.fd();
}

BackendSubmitStatus IoUringBackend::submit(
    uint64_t requestId,
    const IoRequest& request) {
  auto* sqe = io_uring_get_sqe(&state_->ring);
  if (sqe == nullptr) {
    return BackendSubmitStatus::RetryableBusy;
  }

  auto* base =
      static_cast<char*>(request.buffer.data.get()) + request.buffer.offset;
  if (request.opcode == IoOpcode::Read) {
    io_uring_prep_read(
        sqe, request.fd, base, request.buffer.length, request.fileOffset);
  } else {
    io_uring_prep_write(
        sqe, request.fd, base, request.buffer.length, request.fileOffset);
  }
  sqe->user_data = requestId;

  const int ret = io_uring_submit(&state_->ring);
  return ret >= 0 ? BackendSubmitStatus::Submitted
                  : BackendSubmitStatus::Failed;
}

std::vector<BackendCompletion> IoUringBackend::reap() {
  state_->completionEvent.drain();
  std::vector<BackendCompletion> completions;
  io_uring_cqe* cqe = nullptr;
  while (io_uring_peek_cqe(&state_->ring, &cqe) == 0 && cqe != nullptr) {
    IoResult result;
    if (cqe->res >= 0) {
      result.bytes = static_cast<uint64_t>(cqe->res);
      result.error = IoErrorCode::Ok;
    } else {
      result.bytes = 0;
      result.error = IoErrorCode::BackendIoError;
      result.nativeErrorCode = -cqe->res;
    }
    completions.push_back(BackendCompletion{cqe->user_data, std::move(result)});
    io_uring_cqe_seen(&state_->ring, cqe);
    cqe = nullptr;
  }
  return completions;
}

#else

struct IoUringState {};

IoUringBackend::IoUringBackend(uint32_t ringDepth) {
  (void)ringDepth;
  BOLT_FAIL("IoUringBackend requires IO_URING_SUPPORTED");
}

IoUringBackend::~IoUringBackend() = default;

int IoUringBackend::completionFd() const {
  BOLT_FAIL("IoUringBackend requires IO_URING_SUPPORTED");
}

BackendSubmitStatus IoUringBackend::submit(
    uint64_t requestId,
    const IoRequest& request) {
  (void)requestId;
  (void)request;
  return BackendSubmitStatus::Failed;
}

std::vector<BackendCompletion> IoUringBackend::reap() {
  return {};
}

#endif

} // namespace bytedance::bolt::memory::bm
