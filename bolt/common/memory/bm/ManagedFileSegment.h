#pragma once

#include "bolt/common/memory/bm/file/FileSegmentAllocator.h"

#include <memory>
#include <string_view>

namespace bytedance::bolt::memory::bm {

class ManagedFileSegment {
 public:
  ManagedFileSegment() = default;
  ManagedFileSegment(FileSegment segment, std::weak_ptr<FileSegmentAllocator> allocator);
  ~ManagedFileSegment() noexcept;

  ManagedFileSegment(ManagedFileSegment&& other) noexcept;
  ManagedFileSegment& operator=(ManagedFileSegment&& other) noexcept;

  ManagedFileSegment(const ManagedFileSegment&) = delete;
  ManagedFileSegment& operator=(const ManagedFileSegment&) = delete;

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
