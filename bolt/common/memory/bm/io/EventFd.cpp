#include "bolt/common/memory/bm/io/EventFd.h"

#include "bolt/common/base/Exceptions.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <sys/eventfd.h>
#include <unistd.h>

namespace bytedance::bolt::memory::bm {

EventFd::EventFd() {
  fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  BOLT_CHECK_GE(fd_, 0, "eventfd failed: {}", std::strerror(errno));
}

EventFd::~EventFd() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

EventFd::EventFd(EventFd&& other) noexcept : fd_(other.fd_) {
  other.fd_ = -1;
}

EventFd& EventFd::operator=(EventFd&& other) noexcept {
  if (this != &other) {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    fd_ = other.fd_;
    other.fd_ = -1;
  }
  return *this;
}

int EventFd::fd() const {
  return fd_;
}

void EventFd::notify() const {
  uint64_t value = 1;
  while (true) {
    const auto written = ::write(fd_, &value, sizeof(value));
    if (written == sizeof(value)) {
      return;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    // EAGAIN means the counter is already saturated. The fd remains readable,
    // so the waiting worker will still observe the pending wakeup.
    if (written < 0 && errno == EAGAIN) {
      return;
    }
    BOLT_CHECK(false, "eventfd write failed: {}", std::strerror(errno));
  }
}

void EventFd::drain() const {
  uint64_t value = 0;
  while (true) {
    const auto bytes = ::read(fd_, &value, sizeof(value));
    if (bytes == sizeof(value)) {
      continue;
    }
    if (bytes < 0 && errno == EINTR) {
      continue;
    }
    if (bytes < 0 && errno == EAGAIN) {
      return;
    }
    BOLT_CHECK(false, "eventfd read failed: {}", std::strerror(errno));
  }
}

} // namespace bytedance::bolt::memory::bm
