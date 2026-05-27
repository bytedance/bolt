#include "bolt/common/memory/bm/IoUringBackend.h"

#ifdef IO_URING_SUPPORTED
#include <liburing.h>
#include <liburing/io_uring.h>
#endif

#include <cerrno>
#include <cstring>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::memory::bm {

IoUringBackend::IoUringBackend(uint32_t ringDepth) {
#ifdef IO_URING_SUPPORTED
  ring_ = std::make_unique<io_uring>();
  const int ret = io_uring_queue_init(ringDepth, ring_.get(), 0);
  BOLT_CHECK_GE(ret, 0, "io_uring_queue_init failed: {}", std::strerror(-ret));
#else
  (void)ringDepth;
  BOLT_FAIL("IoUringBackend requires IO_URING_SUPPORTED");
#endif
}

IoUringBackend::~IoUringBackend() {
#ifdef IO_URING_SUPPORTED
  if (ring_) {
    io_uring_queue_exit(ring_.get());
  }
#endif
}

bool IoUringBackend::submit(uint64_t requestId, const IoRequest& request) {
#ifdef IO_URING_SUPPORTED
  auto* sqe = io_uring_get_sqe(ring_.get());
  if (sqe == nullptr) {
    return false;
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

  const int ret = io_uring_submit(ring_.get());
  return ret >= 0;
#else
  (void)requestId;
  (void)request;
  return false;
#endif
}

std::vector<BackendCompletion> IoUringBackend::reap() {
  std::vector<BackendCompletion> completions;
#ifdef IO_URING_SUPPORTED
  io_uring_cqe* cqe = nullptr;
  while (io_uring_peek_cqe(ring_.get(), &cqe) == 0 && cqe != nullptr) {
    IoResult result;
    if (cqe->res >= 0) {
      result.bytes = static_cast<uint64_t>(cqe->res);
      result.errorCode = 0;
    } else {
      result.bytes = 0;
      result.errorCode = -cqe->res;
    }
    completions.push_back(BackendCompletion{cqe->user_data, result});
    io_uring_cqe_seen(ring_.get(), cqe);
    cqe = nullptr;
  }
#endif
  return completions;
}

} // namespace bytedance::bolt::memory::bm
