#pragma once

#include <unistd.h>

namespace bytedance::bolt::memory::bm {

class ScopedFd {
 public:
  ScopedFd() = default;
  explicit ScopedFd(int fd) : fd_(fd) {}

  ~ScopedFd() {
    reset();
  }

  ScopedFd(const ScopedFd&) = delete;
  ScopedFd& operator=(const ScopedFd&) = delete;

  ScopedFd(ScopedFd&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
  }

  ScopedFd& operator=(ScopedFd&& other) noexcept {
    if (this != &other) {
      reset(other.fd_);
      other.fd_ = -1;
    }
    return *this;
  }

  int get() const {
    return fd_;
  }

  void reset(int fd = -1) {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    fd_ = fd;
  }

 private:
  int fd_{-1};
};

} // namespace bytedance::bolt::memory::bm
