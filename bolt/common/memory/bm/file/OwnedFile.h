#pragma once

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
    return fd_;
  }

  const std::string& path() const {
    return path_;
  }

  bool valid() const {
    return fd_ >= 0;
  }

  void Close();
  void CloseAndRemove();

 private:
  std::string path_;
  int fd_{-1};
};

} // namespace bytedance::bolt::memory::bm
