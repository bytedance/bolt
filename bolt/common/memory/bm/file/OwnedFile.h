#pragma once

#include "bolt/common/memory/bm/ScopedFd.h"

#include <string>

namespace bytedance::bolt::memory::bm {

class OwnedFile {
 public:
  OwnedFile() = default;
  OwnedFile(std::string path, int fd);
  ~OwnedFile();

  OwnedFile(const OwnedFile&) = delete;
  OwnedFile& operator=(const OwnedFile&) = delete;
  OwnedFile(OwnedFile&& other) noexcept;
  OwnedFile& operator=(OwnedFile&& other) noexcept;

  int fd() const {
    return fd_.get();
  }

  const std::string& path() const {
    return path_;
  }

  bool valid() const {
    return fd_.get() >= 0;
  }

  void Close();
  void CloseAndRemove();

 private:
  std::string path_;
  ScopedFd fd_;
};

} // namespace bytedance::bolt::memory::bm
