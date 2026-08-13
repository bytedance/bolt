#pragma once

#include "bolt/common/memory/bm/base/ScopedFd.h"

namespace bytedance::bolt::memory::bm {

class EventFd {
 public:
  EventFd();
  ~EventFd() = default;

  EventFd(const EventFd&) = delete;
  EventFd& operator=(const EventFd&) = delete;

  EventFd(EventFd&& other) noexcept = default;
  EventFd& operator=(EventFd&& other) noexcept = default;

  int fd() const;
  void notify() const;
  // Clears all currently pending notifications without waiting for a new one.
  void drainNonBlocking() const;

 private:
  ScopedFd fd_;
};

} // namespace bytedance::bolt::memory::bm
