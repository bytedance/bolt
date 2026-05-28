#pragma once

namespace bytedance::bolt::memory::bm {

class ScopedFd {
 public:
  ScopedFd() = default;
  explicit ScopedFd(int fd);
  ~ScopedFd();

  ScopedFd(const ScopedFd&) = delete;
  ScopedFd& operator=(const ScopedFd&) = delete;
  ScopedFd(ScopedFd&& other) noexcept;
  ScopedFd& operator=(ScopedFd&& other) noexcept;

  int get() const;
  void reset(int fd = -1);

 private:
  int fd_{-1};
};

} // namespace bytedance::bolt::memory::bm
