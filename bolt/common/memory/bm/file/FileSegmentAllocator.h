#pragma once

#include "bolt/common/memory/bm/file/FileSegmentAllocatorConfig.h"

#include <memory>

namespace bytedance::bolt::memory::bm {

class FileSegmentAllocator {
 public:
  virtual ~FileSegmentAllocator() = default;

  // Not thread-safe. Callers must serialize Allocate() and Free() on the same
  // allocator instance.
  virtual FileAllocateResult Allocate(int64_t size) = 0;
  virtual FileFreeResult Free(const FileSegment& segment) = 0;
};

std::shared_ptr<FileSegmentAllocator> CreateFileSegmentAllocator(
    FileSegmentAllocatorConfig config);

} // namespace bytedance::bolt::memory::bm
