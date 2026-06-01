#pragma once

#include "bolt/common/memory/bm/file/FileSegmentAllocator.h"

#include <memory>
#include <string_view>

namespace bytedance::bolt::memory::bm {

class OwnedFileSegment {
 public:
  OwnedFileSegment() = default;
  OwnedFileSegment(FileSegment segment, std::weak_ptr<FileSegmentAllocator> allocator);
  ~OwnedFileSegment() noexcept;

  OwnedFileSegment(OwnedFileSegment&& other) noexcept;
  OwnedFileSegment& operator=(OwnedFileSegment&& other) noexcept;

  OwnedFileSegment(const OwnedFileSegment&) = delete;
  OwnedFileSegment& operator=(const OwnedFileSegment&) = delete;

  const FileSegment& segment() const;
  bool valid() const;
  void FreeOrFatal(std::string_view context) noexcept;

 private:
  void ResetNoexcept(std::string_view context) noexcept;

  FileSegment segment_;
  std::weak_ptr<FileSegmentAllocator> allocator_;
  bool valid_{false};
};

} // namespace bytedance::bolt::memory::bm
