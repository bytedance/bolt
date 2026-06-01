#include "bolt/common/memory/bm/file/OwnedFile.h"

#include <filesystem>
#include <utility>

namespace bytedance::bolt::memory::bm {

OwnedFile::OwnedFile(std::string path, int fd)
    : path_(std::move(path)), fd_(fd) {}

OwnedFile::~OwnedFile() {
  Close();
}

OwnedFile::OwnedFile(OwnedFile&& other) noexcept
    : path_(std::move(other.path_)), fd_(std::move(other.fd_)) {}

OwnedFile& OwnedFile::operator=(OwnedFile&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  Close();
  path_ = std::move(other.path_);
  fd_ = std::move(other.fd_);
  return *this;
}

void OwnedFile::Close() {
  fd_.reset();
}

void OwnedFile::CloseAndRemove() {
  Close();
  if (!path_.empty()) {
    std::filesystem::remove(path_);
  }
}

} // namespace bytedance::bolt::memory::bm
