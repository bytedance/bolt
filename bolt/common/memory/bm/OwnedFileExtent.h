#pragma once

#include "bolt/common/memory/bm/file/FileBlockAllocator.h"

#include <memory>
#include <string_view>

namespace bytedance::bolt::memory::bm {

class OwnedFileExtent {
 public:
  OwnedFileExtent() = default;
  OwnedFileExtent(FileExtent extent, std::weak_ptr<FileBlockAllocator> allocator);
  ~OwnedFileExtent() noexcept;

  OwnedFileExtent(OwnedFileExtent&& other) noexcept;
  OwnedFileExtent& operator=(OwnedFileExtent&& other) noexcept;

  OwnedFileExtent(const OwnedFileExtent&) = delete;
  OwnedFileExtent& operator=(const OwnedFileExtent&) = delete;

  const FileExtent& extent() const;
  bool valid() const;
  void FreeOrFatal(std::string_view context) noexcept;

 private:
  void ResetNoexcept(std::string_view context) noexcept;

  FileExtent extent_;
  std::weak_ptr<FileBlockAllocator> allocator_;
  bool valid_{false};
};

} // namespace bytedance::bolt::memory::bm
