#pragma once

#include "bolt/common/memory/bm/ScopedFd.h"

#include <string>

namespace bytedance::bolt::memory::bm {

class ManagedOpenFile {
 public:
  ManagedOpenFile() = default;
  ManagedOpenFile(std::string path, int fd);
  ~ManagedOpenFile();

  ManagedOpenFile(const ManagedOpenFile&) = delete;
  ManagedOpenFile& operator=(const ManagedOpenFile&) = delete;
  ManagedOpenFile(ManagedOpenFile&& other) noexcept;
  ManagedOpenFile& operator=(ManagedOpenFile&& other) noexcept;

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
