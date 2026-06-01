#pragma once

#include "bolt/common/memory/bm/ScopedFd.h"

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
  void drain() const;

 private:
  ScopedFd fd_;
};

} // namespace bytedance::bolt::memory::bm
