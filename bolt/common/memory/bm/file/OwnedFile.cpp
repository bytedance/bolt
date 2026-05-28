#include "bolt/common/memory/bm/file/OwnedFile.h"

#include <filesystem>
#include <utility>

#include <unistd.h>

namespace bytedance::bolt::memory::bm {

OwnedFile::OwnedFile(std::string path, int fd)
    : path_(std::move(path)), fd_(fd) {}

OwnedFile::~OwnedFile() {
  Close();
}

OwnedFile::OwnedFile(OwnedFile&& other) noexcept
    : path_(std::move(other.path_)), fd_(other.fd_) {
  other.fd_ = -1;
}

OwnedFile& OwnedFile::operator=(OwnedFile&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  Close();
  path_ = std::move(other.path_);
  fd_ = other.fd_;
  other.fd_ = -1;
  return *this;
}

void OwnedFile::Close() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

void OwnedFile::CloseAndRemove() {
  Close();
  if (!path_.empty()) {
    std::filesystem::remove(path_);
  }
}

} // namespace bytedance::bolt::memory::bm
