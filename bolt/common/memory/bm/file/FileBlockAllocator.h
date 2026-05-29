#pragma once

#include "bolt/common/memory/bm/file/FileBlockAllocatorConfig.h"

#include <memory>

namespace bytedance::bolt::memory::bm {

class FileBlockAllocator {
 public:
  virtual ~FileBlockAllocator() = default;

  // Not thread-safe. Callers must serialize Allocate() and Free() on the same
  // allocator instance.
  virtual FileAllocateResult Allocate(int64_t size) = 0;
  virtual FileFreeResult Free(const FileExtent& extent) = 0;
};

std::unique_ptr<FileBlockAllocator> CreateFileBlockAllocator(
    FileBlockAllocatorConfig config);

} // namespace bytedance::bolt::memory::bm
