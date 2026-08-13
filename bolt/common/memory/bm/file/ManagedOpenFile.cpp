#include "bolt/common/memory/bm/file/ManagedOpenFile.h"

#include <filesystem>
#include <utility>

namespace bytedance::bolt::memory::bm {

ManagedOpenFile::ManagedOpenFile(std::string path, int fd)
    : path_(std::move(path)), fd_(fd) {}

ManagedOpenFile::~ManagedOpenFile() {
  Close();
}

ManagedOpenFile::ManagedOpenFile(ManagedOpenFile&& other) noexcept
    : path_(std::move(other.path_)), fd_(std::move(other.fd_)) {}

ManagedOpenFile& ManagedOpenFile::operator=(ManagedOpenFile&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  Close();
  path_ = std::move(other.path_);
  fd_ = std::move(other.fd_);
  return *this;
}

void ManagedOpenFile::Close() {
  fd_.reset();
}

void ManagedOpenFile::CloseAndRemove() {
  Close();
  if (!path_.empty()) {
    std::filesystem::remove(path_);
  }
}

} // namespace bytedance::bolt::memory::bm
