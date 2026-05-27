#pragma once

namespace bytedance::bolt::memory::bm {

class EventFd {
 public:
  EventFd();
  ~EventFd();

  EventFd(const EventFd&) = delete;
  EventFd& operator=(const EventFd&) = delete;

  EventFd(EventFd&& other) noexcept;
  EventFd& operator=(EventFd&& other) noexcept;

  int fd() const;
  void notify() const;
  void drain() const;

 private:
  int fd_{-1};
};

} // namespace bytedance::bolt::memory::bm
