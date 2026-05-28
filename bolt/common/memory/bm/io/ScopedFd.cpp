#include "bolt/common/memory/bm/io/ScopedFd.h"

#include <unistd.h>

namespace bytedance::bolt::memory::bm {

ScopedFd::ScopedFd(int fd) : fd_(fd) {}

ScopedFd::~ScopedFd() {
  reset();
}

ScopedFd::ScopedFd(ScopedFd&& other) noexcept : fd_(other.fd_) {
  other.fd_ = -1;
}

ScopedFd& ScopedFd::operator=(ScopedFd&& other) noexcept {
  if (this != &other) {
    reset(other.fd_);
    other.fd_ = -1;
  }
  return *this;
}

int ScopedFd::get() const {
  return fd_;
}

void ScopedFd::reset(int fd) {
  if (fd_ >= 0) {
    ::close(fd_);
  }
  fd_ = fd;
}

} // namespace bytedance::bolt::memory::bm
